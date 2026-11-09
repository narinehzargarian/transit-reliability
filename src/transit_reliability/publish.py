"""Writes route results to DynamoDB, so an app can look up a route's reliability quickly."""

from __future__ import annotations

from decimal import Decimal


def route_items(summary: dict, feed: str) -> list[dict]:
    """One item per route for the whole period, plus one per route and service day.

    Key: route ("<feed>#<route_id>") and period ("all" or "YYYYMMDD").
    """
    items = []

    def item(row: dict, period: str) -> dict:
        values = {
            "route": f"{feed}#{row['route_id']}",
            "period": period,
            "route_id": row["route_id"],
            "arrivals": int(row["arrivals"]),
            "trips": int(row["trips"]),
        }
        for key in ("on_time_pct", "early_pct", "late_pct", "median_delay_s", "p90_delay_s", "bunched_pct"):
            if row.get(key) is not None:
                values[key] = Decimal(str(row[key]))
        if row.get("route_name"):
            values["route_name"] = row["route_name"]
        return values

    for row in summary.get("routes", []):
        items.append(item(row, "all"))
    for row in summary.get("route_days", []):
        items.append(item(row, row["service_date"]))
    return items


def publish(summary: dict, table: str, feed: str, resource=None) -> int:
    import boto3

    dynamodb = resource or boto3.resource("dynamodb")
    items = route_items(summary, feed)
    with dynamodb.Table(table).batch_writer(overwrite_by_pkeys=["route", "period"]) as batch:
        for item in items:
            batch.put_item(Item=item)
    return len(items)
