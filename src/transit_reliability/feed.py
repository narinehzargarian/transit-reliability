"""Reading GTFS-Realtime VehiclePositions feeds into plain rows."""

from __future__ import annotations

import urllib.request

from google.transit import gtfs_realtime_pb2

STATUS_NAMES = {
    gtfs_realtime_pb2.VehiclePosition.INCOMING_AT: "INCOMING_AT",
    gtfs_realtime_pb2.VehiclePosition.STOPPED_AT: "STOPPED_AT",
    gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO: "IN_TRANSIT_TO",
}


def fetch(url: str, headers: dict[str, str] | None = None, timeout: float = 20) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "transit-reliability", **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def decode_vehicle_positions(data: bytes, feed: str) -> list[dict]:
    """One row per vehicle that's on a trip. Optional fields stay None when the feed omits them."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(data)
    header_time = message.header.timestamp if message.header.HasField("timestamp") else None
    rows = []
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle = entity.vehicle
        trip = vehicle.trip
        if not trip.trip_id:
            continue  # Without a trip there's no schedule to compare against.
        timestamp = vehicle.timestamp if vehicle.HasField("timestamp") else header_time
        if timestamp is None:
            continue
        rows.append(
            {
                "feed": feed,
                "vehicle_id": (vehicle.vehicle.id if vehicle.HasField("vehicle") and vehicle.vehicle.id else entity.id),
                "trip_id": trip.trip_id,
                "route_id": trip.route_id or None,
                "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                "start_date": trip.start_date or None,
                "timestamp": int(timestamp),
                "lat": vehicle.position.latitude if vehicle.HasField("position") else None,
                "lon": vehicle.position.longitude if vehicle.HasField("position") else None,
                "stop_sequence": vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None,
                "stop_id": vehicle.stop_id or None,
                "status": STATUS_NAMES.get(vehicle.current_status) if vehicle.HasField("current_status") else None,
            }
        )
    return rows
