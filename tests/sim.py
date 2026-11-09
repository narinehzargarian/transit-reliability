"""Synthetic schedules and feed polls with known delays, for testing the Spark job."""

from __future__ import annotations

import csv
import gzip
import json
import os
from datetime import date

from transit_reliability.gtfs_time import service_day_start

TZ = "America/New_York"
# Four stops due north, about 555 m apart. Stop sequences skip numbers, as real feeds often do.
STOPS = [("S1", 42.3600), ("S2", 42.3650), ("S3", 42.3700), ("S4", 42.3750)]
LON = -71.06
SEQUENCES = [10, 20, 30, 40]


def hhmmss(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def write_gtfs(folder: str, trips: dict[str, list[int]]) -> None:
    """trips: trip_id -> scheduled seconds after service-day start at each of the four stops."""
    os.makedirs(folder, exist_ok=True)

    def write(name, header, rows):
        with open(os.path.join(folder, f"{name}.txt"), "w", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(rows)

    write("agency", ["agency_id", "agency_name", "agency_url", "agency_timezone"], [["A", "Test Transit", "https://example.com", TZ]])
    write("stops", ["stop_id", "stop_name", "stop_lat", "stop_lon"], [[s, f"Stop {s}", lat, LON] for s, lat in STOPS])
    write("routes", ["route_id", "route_short_name", "route_long_name", "route_type"], [["R1", "1", "Main Street", "3"]])
    write("trips", ["route_id", "service_id", "trip_id", "direction_id"], [["R1", "WK", t, "0"] for t in trips])
    write(
        "stop_times",
        ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        [[t, hhmmss(s), hhmmss(s), STOPS[i][0], SEQUENCES[i]] for t, times in trips.items() for i, s in enumerate(times)],
    )


def day_start(service_date: str) -> int:
    return service_day_start(date(int(service_date[:4]), int(service_date[4:6]), int(service_date[6:])), TZ)


def simulate(
    trip_id: str,
    vehicle_id: str,
    arrivals: list[int],
    first_poll: int,
    poll_s: int = 30,
    dwell_s: int = 40,
    start_date: str | None = None,
    progress: bool = True,
    coords: bool = True,
    skip: tuple[int, int] | None = None,
) -> list[dict]:
    """Feed polls for one vehicle that really reaches the stops at `arrivals` (Unix times).

    progress=False leaves out stop sequence and status (a coordinates-only feed);
    skip=(start, end) drops the polls in that time range, as if the feed went quiet.
    """
    rows = []
    poll = first_poll
    while True:
        reached = [i for i, t in enumerate(arrivals) if t <= poll]
        if reached and reached[-1] == len(arrivals) - 1 and poll >= arrivals[-1] + dwell_s:
            break  # Trip finished.
        if not reached:
            sequence, status, lat = SEQUENCES[0], "IN_TRANSIT_TO", STOPS[0][1] - 0.004
        else:
            i = reached[-1]
            if poll < arrivals[i] + dwell_s:
                sequence, status, lat = SEQUENCES[i], "STOPPED_AT", STOPS[i][1]
            else:
                departed = arrivals[i] + dwell_s
                fraction = (poll - departed) / (arrivals[i + 1] - departed)
                sequence, status = SEQUENCES[i + 1], "IN_TRANSIT_TO"
                lat = STOPS[i][1] + fraction * (STOPS[i + 1][1] - STOPS[i][1])
        if not (skip and skip[0] <= poll <= skip[1]):
            rows.append(
                {
                    "feed": "test",
                    "vehicle_id": vehicle_id,
                    "trip_id": trip_id,
                    "route_id": "R1",
                    "direction_id": 0,
                    "start_date": start_date,
                    "timestamp": poll,
                    "lat": lat if coords else None,
                    "lon": LON if coords else None,
                    "stop_sequence": sequence if progress else None,
                    "stop_id": None,
                    "status": status if progress else None,
                }
            )
        poll += poll_s
    return rows


def write_positions(folder: str, rows: list[dict]) -> None:
    os.makedirs(folder, exist_ok=True)
    with gzip.open(os.path.join(folder, "part-0.jsonl.gz"), "wt") as out:
        for row in rows:
            out.write(json.dumps(row) + "\n")
