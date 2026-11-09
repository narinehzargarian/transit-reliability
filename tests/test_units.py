import gzip
import json
from datetime import date, datetime, timezone

from google.transit import gtfs_realtime_pb2

from transit_reliability.collect import LocalSink, NewPositions, collect
from transit_reliability.feed import decode_vehicle_positions
from transit_reliability.gtfs_time import parse_gtfs_time, service_day_start
from transit_reliability.publish import route_items
from transit_reliability.report import render


def test_parses_times_past_midnight():
    assert parse_gtfs_time("08:05:30") == 8 * 3600 + 330
    assert parse_gtfs_time("25:10:00") == 25 * 3600 + 600
    assert parse_gtfs_time("") is None
    assert parse_gtfs_time(None) is None


def test_service_day_starts_at_noon_minus_12_hours():
    # A normal day: the same as local midnight (05:00 UTC in winter in New York).
    assert service_day_start(date(2026, 3, 2), "America/New_York") == int(datetime(2026, 3, 2, 5, tzinfo=timezone.utc).timestamp())
    # The day clocks spring forward: noon EDT is 16:00 UTC, minus 12 hours is 04:00 UTC,
    # which is 23:00 the evening before in local time, not midnight.
    assert service_day_start(date(2026, 3, 8), "America/New_York") == int(datetime(2026, 3, 8, 4, tzinfo=timezone.utc).timestamp())


def feed_bytes(entities):
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = 1_000_000
    for build in entities:
        build(message.entity.add())
    return message.SerializeToString()


def test_decodes_vehicle_positions_and_keeps_missing_fields_empty():
    def full(entity):
        entity.id = "e1"
        v = entity.vehicle
        v.trip.trip_id, v.trip.route_id, v.trip.start_date, v.trip.direction_id = "T1", "R1", "20260302", 1
        v.vehicle.id = "bus-1"
        v.position.latitude, v.position.longitude = 42.36, -71.06
        v.current_stop_sequence = 0  # Zero is a real value, not "missing".
        v.current_status = gtfs_realtime_pb2.VehiclePosition.STOPPED_AT
        v.timestamp = 1_000_050

    def sparse(entity):
        entity.id = "e2"
        entity.vehicle.trip.trip_id = "T2"

    def no_trip(entity):
        entity.id = "e3"
        entity.vehicle.vehicle.id = "deadheading"

    rows = decode_vehicle_positions(feed_bytes([full, sparse, no_trip]), "test")
    assert len(rows) == 2
    first, second = rows
    assert first["vehicle_id"] == "bus-1" and first["stop_sequence"] == 0 and first["status"] == "STOPPED_AT"
    assert first["direction_id"] == 1 and first["timestamp"] == 1_000_050
    # Missing fields stay None, and the header time stands in for a missing vehicle time.
    assert second["vehicle_id"] == "e2" and second["timestamp"] == 1_000_000
    assert second["stop_sequence"] is None and second["status"] is None and second["lat"] is None


def test_keeps_only_new_reports_per_vehicle():
    fresh = NewPositions()
    assert len(fresh.filter([{"vehicle_id": "a", "timestamp": 10}, {"vehicle_id": "b", "timestamp": 10}])) == 2
    assert fresh.filter([{"vehicle_id": "a", "timestamp": 10}, {"vehicle_id": "b", "timestamp": 15}]) == [{"vehicle_id": "b", "timestamp": 15}]


def test_collect_writes_files_and_survives_a_failed_poll(tmp_path):
    clock = {"t": 1_700_000_000.0}
    responses = iter(
        [
            feed_bytes([lambda e: (setattr(e, "id", "x"), e.vehicle.trip.__setattr__("trip_id", "T1"), setattr(e.vehicle, "timestamp", 1_700_000_000))]),
            RuntimeError("network down"),
            feed_bytes([lambda e: (setattr(e, "id", "x"), e.vehicle.trip.__setattr__("trip_id", "T1"), setattr(e.vehicle, "timestamp", 1_700_000_060))]),
        ]
    )

    def fetcher(url, headers):
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return value

    def sleep(seconds):
        clock["t"] += seconds

    stats = collect("http://feed", LocalSink(str(tmp_path)), "test", interval_s=30, duration_s=80, fetcher=fetcher, now=lambda: clock["t"], sleep=sleep, log=lambda _: None)
    assert stats == {"polls": 3, "positions": 2, "errors": 1}
    files = sorted(tmp_path.rglob("*.jsonl.gz"))
    assert len(files) == 2 and all("date=2023-11-14" in str(f) for f in files)
    rows = [json.loads(line) for f in files for line in gzip.open(f, "rt")]
    assert [r["timestamp"] for r in rows] == [1_700_000_000, 1_700_000_060]


SUMMARY = {
    "timezone": "America/New_York",
    "method": "sequence",
    "config": {"early_s": 60, "late_s": 300, "max_gap_s": 150, "bunching_ratio": 0.25, "bunching_max_headway_s": 900},
    "observed": {"positions": 10, "vehicles": 2, "start": "2026-03-02T13:00:00+00:00", "end": "2026-03-02T14:00:00+00:00"},
    "overall": {"arrivals": 4, "on_time_pct": 50.0, "early_pct": 25.0, "late_pct": 25.0, "median_delay_s": 30},
    "routes": [{"route_id": "R1", "route_name": "<b>1</b>", "arrivals": 4, "trips": 1, "on_time_pct": 50.0, "early_pct": 25.0, "late_pct": 25.0, "median_delay_s": 30, "p90_delay_s": 400, "pairs": 4, "bunched_pct": 25.0}],
    "hours": [{"hour": 8, "arrivals": 4, "on_time_pct": 50.0}],
    "worst_stops": [],
    "route_days": [{"route_id": "R1", "service_date": "20260302", "arrivals": 4, "trips": 1, "on_time_pct": 50.0, "early_pct": 25.0, "late_pct": 25.0, "median_delay_s": 30, "p90_delay_s": 400}],
}


def test_report_escapes_names_and_shows_results():
    page = render(SUMMARY, "Test & Co")
    assert "&lt;b&gt;1&lt;/b&gt;" in page and "<b>1</b>" not in page
    assert "Test &amp; Co" in page and "50.0%" in page and "+0.5 min" in page


def test_dynamodb_items_cover_the_whole_period_and_each_day():
    items = route_items(SUMMARY, "mbta")
    assert [(i["route"], i["period"]) for i in items] == [("mbta#R1", "all"), ("mbta#R1", "20260302")]
    assert str(items[0]["on_time_pct"]) == "50.0"
