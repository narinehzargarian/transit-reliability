"""GTFS clock times and service days.

GTFS writes times like "25:10:00" for trips that run past midnight, measured from the start of
the *service day*. The spec defines that start as noon minus 12 hours in the agency's time zone,
not midnight: on daylight-saving days the two differ by an hour.
"""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo


def parse_gtfs_time(value: str | None) -> int | None:
    """"25:10:00" -> 90600 seconds. Returns None for blank values (GTFS allows them)."""
    if value is None or not value.strip():
        return None
    hours, minutes, seconds = (int(part) for part in value.strip().split(":"))
    return hours * 3600 + minutes * 60 + seconds


def parse_service_date(value: str) -> date:
    """"20260302" -> date(2026, 3, 2)."""
    return datetime.strptime(value, "%Y%m%d").date()


def service_day_start(service_date: date, tz: str) -> int:
    """Unix time that GTFS times on this service date count from: local noon minus 12 hours."""
    noon = datetime.combine(service_date, time(12, 0), tzinfo=ZoneInfo(tz))
    # Subtract in absolute time. Subtracting a timedelta from an aware datetime works on the wall
    # clock, which would give local midnight and be an hour off on daylight-saving days.
    return int(noon.timestamp()) - 12 * 3600
