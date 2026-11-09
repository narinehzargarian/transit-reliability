import json
import os

import pytest
from sim import day_start, simulate, write_gtfs, write_positions

from transit_reliability.spark_job import Config, assign_service_dates, load_gtfs, load_observations, run

DAY = "20260302"  # A Monday.
H = 3600
# Polls start 5 s off the minute, so arrivals fall between polls as they do in real feeds (and
# using the later poll instead of the midpoint shows up as a ~25 s error).

# Scheduled times (seconds after service-day start) at the four stops.
T1 = [8 * H, 8 * H + 295, 8 * H + 600, 8 * H + 900]
T2 = [8 * H + 600, 8 * H + 900, 8 * H + 1200, 8 * H + 1500]
NIGHT = [24 * H + 1800, 24 * H + 2100, 24 * H + 2400, 24 * H + 2700]  # 00:30 to 00:45 the next morning.


def dataset(tmp_path, positions, trips=None):
    gtfs, raw, out = (str(tmp_path / name) for name in ("gtfs", "raw", "out"))
    write_gtfs(gtfs, trips or {"T1": T1, "T2": T2, "NIGHT": NIGHT})
    write_positions(raw, positions)
    return gtfs, raw, out


def actual(schedule, delays, service_date=DAY):
    start = day_start(service_date)
    return [start + s + d for s, d in zip(schedule, delays)]


def events_by_stop(spark, out, trip):
    rows = spark.read.parquet(os.path.join(out, "stop_events")).where(f"trip_id = '{trip}'").collect()
    return {row["stop_sequence"]: row for row in rows}


def test_measures_on_time_late_and_early(spark, tmp_path):
    delays = [-120, 120, 420, 240]  # Early, on time, late, on time; stops still reached in order.
    positions = simulate("T1", "bus-1", actual(T1, delays), first_poll=day_start(DAY) + 8 * H - 305, start_date=DAY)
    gtfs, raw, out = dataset(tmp_path, positions)
    summary = run(spark, gtfs, raw, out)

    events = events_by_stop(spark, out, "T1")
    assert sorted(events) == [10, 20, 30, 40]
    for sequence, delay in zip([10, 20, 30, 40], delays):
        event = events[sequence]
        # Polls are 30 s apart, so the inferred arrival is within 15 s of the real one.
        assert abs(event["delay_s"] - delay) <= 15, (sequence, event["delay_s"], delay)
        assert event["uncertainty_s"] <= 15
        assert event["method"] == "sequence"
    assert [events[s]["category"] for s in (10, 20, 30, 40)] == ["early", "on_time", "late", "on_time"]
    assert summary["method"] == "sequence"
    assert summary["overall"]["arrivals"] == 4
    assert summary["routes"][0]["route_name"] == "1"
    with open(os.path.join(out, "summary.json")) as saved:
        assert json.load(saved)["overall"]["late_pct"] == 25.0


def test_coordinates_only_feed_uses_distance(spark, tmp_path):
    delays = [30, 200, 300, 600]
    positions = simulate(
        "T1", "bus-1", actual(T1, delays), first_poll=day_start(DAY) + 8 * H - 305, start_date=DAY, progress=False
    )
    gtfs, raw, out = dataset(tmp_path, positions)
    summary = run(spark, gtfs, raw, out)

    assert summary["method"] == "proximity"
    events = events_by_stop(spark, out, "T1")
    assert sorted(events) == [10, 20, 30, 40]
    for sequence, delay in zip([10, 20, 30, 40], delays):
        # Distance-based arrivals read early: the bus counts as arrived once it's within 40 m.
        # The slowest leg here moves at about 1 m/s, so that's up to 40 s, plus half a polling
        # interval for the midpoint.
        assert -60 <= events[sequence]["delay_s"] - delay <= 15, (sequence, events[sequence]["delay_s"], delay)


def test_trip_after_midnight_belongs_to_the_previous_service_day(spark, tmp_path):
    delays = [60, 60, 60, 60]
    # No start_date in the feed, and the positions are timestamped on the next calendar day.
    positions = simulate("NIGHT", "bus-9", actual(NIGHT, delays), first_poll=day_start(DAY) + 24 * H + 1495)
    gtfs, raw, out = dataset(tmp_path, positions)

    observations = assign_service_dates(spark, load_observations(spark, raw), load_gtfs(spark, gtfs))
    assert {row["service_date"] for row in observations.collect()} == {DAY}

    run(spark, gtfs, raw, out)
    events = events_by_stop(spark, out, "NIGHT")
    assert len(events) == 4
    assert all(abs(e["delay_s"] - 60) <= 15 for e in events.values())


def test_skips_arrivals_when_the_feed_went_quiet(spark, tmp_path):
    arrivals = actual(T2, [0, 0, 0, 0])
    # No polls for four minutes around the third stop: its arrival can't be placed reliably.
    positions = simulate("T2", "bus-2", arrivals, first_poll=day_start(DAY) + 8 * H + 295, start_date=DAY, skip=(arrivals[2] - 120, arrivals[2] + 120))
    gtfs, raw, out = dataset(tmp_path, positions)
    run(spark, gtfs, raw, out, Config(max_gap_s=150))

    assert sorted(events_by_stop(spark, out, "T2")) == [10, 20, 40]


def test_detects_bunching(spark, tmp_path):
    # T1 runs 8 minutes late at the last stop and arrives 2 minutes before T2, which is on time:
    # less than 25% of the scheduled 10-minute gap, so the pair is bunched.
    first = simulate("T1", "bus-1", actual(T1, [0, 0, 0, 480]), first_poll=day_start(DAY) + 8 * H - 305, start_date=DAY)
    second = simulate("T2", "bus-2", actual(T2, [0, 0, 0, 0]), first_poll=day_start(DAY) + 8 * H + 295, start_date=DAY)
    gtfs, raw, out = dataset(tmp_path, first + second)
    summary = run(spark, gtfs, raw, out)

    route = summary["routes"][0]
    assert route["pairs"] == 4  # The two trips meet at all four stops.
    assert route["bunched_pct"] == 25.0  # Bunched at one of them.


def test_ignores_positions_for_unknown_trips(spark, tmp_path):
    good = simulate("T1", "bus-1", actual(T1, [0, 0, 0, 0]), first_poll=day_start(DAY) + 8 * H - 305, start_date=DAY)
    stray = [dict(row, trip_id="NOT-IN-SCHEDULE", vehicle_id="bus-x") for row in good]
    gtfs, raw, out = dataset(tmp_path, good + stray)
    summary = run(spark, gtfs, raw, out)
    assert summary["overall"]["arrivals"] == 4


def test_explains_a_schedule_that_does_not_match(spark, tmp_path):
    positions = simulate("T1", "bus-1", actual(T1, [0, 0, 0, 0]), first_poll=day_start(DAY) + 8 * H - 305, start_date=DAY)
    gtfs, raw, out = dataset(tmp_path, positions, trips={"OTHER": T1})
    with pytest.raises(ValueError, match="No saved positions matched"):
        run(spark, gtfs, raw, out)
