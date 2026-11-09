"""The Spark job: from saved vehicle positions and the static schedule to reliability numbers.

1. Load the schedule (GTFS) and the saved GTFS-Realtime vehicle positions.
2. Work out each position's service date, so trips that run past midnight compare against the
   right day's schedule.
3. Infer when each vehicle reached each stop of its trip, from either
   - stop progress ("stopped at stop 20", "in transit to stop 30"), or
   - distance, for feeds that only report coordinates.
   A vehicle is only seen when the feed is polled, so the arrival is estimated as the midpoint
   between the last poll before it reached the stop and the first poll after. Half that gap is
   kept as the arrival's uncertainty, and arrivals with gaps over max_gap_s are dropped.
4. Compare each arrival with the scheduled time, then summarize by route, stop, and hour, and
   measure bunching: buses arriving much closer together than scheduled.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from .gtfs_time import parse_service_date, service_day_start


@dataclass(frozen=True)
class Config:
    early_s: int = 60  # Arriving more than 1 minute early counts as early.
    late_s: int = 300  # More than 5 minutes late counts as late. Many agencies use these two.
    max_gap_s: int = 150  # Polls further apart than this can't place an arrival well enough.
    proximity_m: float = 40.0  # For coordinate-only feeds: "at the stop" means within this distance.
    bunching_ratio: float = 0.25  # Bunched: the gap behind the previous vehicle is under 25% of scheduled.
    bunching_max_headway_s: int = 900  # Only check bunching where service runs at least every 15 minutes.
    max_abs_delay_s: int = 3 * 3600  # Larger differences mean a mismatched trip, not a delay.
    method: str = "auto"  # "sequence", "proximity", or "auto".


@dataclass
class Gtfs:
    tz: str
    stop_times: DataFrame
    trips: DataFrame
    stops: DataFrame
    routes: DataFrame


OBSERVATION_SCHEMA = StructType(
    [
        StructField("feed", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("timestamp", LongType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("stop_sequence", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("status", StringType()),
    ]
)

TRIP_KEY = ["trip_id", "service_date", "vehicle_id"]


def spark_session(master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("transit-reliability")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def _seconds(column: str):
    """GTFS "HH:MM:SS" (hours may exceed 23) to seconds; blank to null."""
    text = F.trim(F.col(column))
    parts = F.split(text, ":")
    return F.when(F.col(column).isNull() | (text == ""), None).otherwise(
        parts.getItem(0).cast("int") * 3600 + parts.getItem(1).cast("int") * 60 + parts.getItem(2).cast("int")
    )


def load_gtfs(spark: SparkSession, folder: str) -> Gtfs:
    def read(name: str) -> DataFrame:
        return spark.read.option("header", True).csv(os.path.join(folder, f"{name}.txt"))

    tz = read("agency").select("agency_timezone").first()["agency_timezone"]
    raw_stop_times = read("stop_times")
    stop_times = raw_stop_times.select(
        "trip_id",
        "stop_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        # Some stops only list a departure time; use it when the arrival time is blank.
        F.coalesce(_seconds("arrival_time"), _seconds("departure_time")).alias("arrival_s"),
    )
    trips_raw = read("trips")
    trips = trips_raw.select(
        "trip_id",
        "route_id",
        (F.col("direction_id").cast("int") if "direction_id" in trips_raw.columns else F.lit(None).cast("int")).alias("direction_id"),
    )
    stops = read("stops").select(
        "stop_id",
        "stop_name",
        F.col("stop_lat").cast("double").alias("stop_lat"),
        F.col("stop_lon").cast("double").alias("stop_lon"),
    )
    routes_raw = read("routes")
    name_parts = [F.col(c) for c in ("route_short_name", "route_long_name") if c in routes_raw.columns]
    routes = routes_raw.select(
        "route_id",
        F.coalesce(*[F.when(F.trim(c) != "", c) for c in name_parts], F.col("route_id")).alias("route_name"),
    )
    return Gtfs(tz, stop_times, trips, stops, routes)


def load_observations(spark: SparkSession, folder: str) -> DataFrame:
    """Saved positions (gzipped JSON lines, any folder layout), with repeats removed."""
    return (
        spark.read.schema(OBSERVATION_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(folder)
        .where(F.col("trip_id").isNotNull() & F.col("timestamp").isNotNull() & F.col("vehicle_id").isNotNull())
        .dropDuplicates(["vehicle_id", "trip_id", "timestamp"])
    )


def _day_starts(spark: SparkSession, dates: list[str], tz: str) -> DataFrame:
    rows = [(d, service_day_start(parse_service_date(d), tz)) for d in dates if d and len(d) == 8 and d.isdigit()]
    return spark.createDataFrame(rows, "service_date string, day_start long")


def assign_service_dates(spark: SparkSession, observations: DataFrame, gtfs: Gtfs) -> DataFrame:
    """Adds service_date (YYYYMMDD) and day_start (Unix time GTFS times count from).

    Uses the trip's start_date when the feed sends one. Otherwise a position belongs to today's or
    yesterday's service day, whichever trip window contains it: a bus at 00:35 on a trip
    scheduled for 24:30 belongs to yesterday's service.
    """
    spans = gtfs.stop_times.groupBy("trip_id").agg(F.min("arrival_s").alias("first_s"), F.max("arrival_s").alias("last_s"))
    local_day = F.to_date(F.from_utc_timestamp(F.timestamp_seconds("timestamp"), gtfs.tz))
    today = F.date_format(local_day, "yyyyMMdd")
    yesterday = F.date_format(F.date_sub(local_day, 1), "yyyyMMdd")
    candidates = F.when(
        F.col("start_date").rlike("^[0-9]{8}$"),
        F.array(F.struct(F.lit(0).alias("priority"), F.col("start_date").alias("day"))),
    ).otherwise(F.array(F.struct(F.lit(0).alias("priority"), today.alias("day")), F.struct(F.lit(1).alias("priority"), yesterday.alias("day"))))
    expanded = observations.withColumn("candidate", F.explode(candidates)).select(
        "*", F.col("candidate.priority").alias("priority"), F.col("candidate.day").alias("service_date")
    ).drop("candidate")
    dates = [row["service_date"] for row in expanded.select("service_date").distinct().collect()]
    matched = (
        expanded.join(_day_starts(spark, dates, gtfs.tz), "service_date")
        .join(spans, "trip_id")
        .where(
            (F.col("timestamp") >= F.col("day_start") + F.col("first_s") - 3600)
            & (F.col("timestamp") <= F.col("day_start") + F.col("last_s") + 3 * 3600)
        )
    )
    best = Window.partitionBy("vehicle_id", "trip_id", "timestamp").orderBy("priority")
    return matched.withColumn("rank", F.row_number().over(best)).where("rank = 1").drop("rank", "priority", "first_s", "last_s")


def arrivals_from_sequence(observations: DataFrame, gtfs: Gtfs, config: Config) -> DataFrame:
    """Arrivals from stop progress.

    Each position says how far along its trip the vehicle is. As a number: 2 x sequence, plus 1 when
    stopped at that stop, so stop s has been reached once the number is greater than 2 x s
    ("in transit to 30" means 10 and 20 are done; "stopped at 30" means 30 is done too).
    """
    sequence_by_stop = gtfs.stop_times.groupBy("trip_id", "stop_id").agg(F.min("stop_sequence").alias("sequence_from_stop"))
    positions = (
        observations.join(sequence_by_stop, ["trip_id", "stop_id"], "left")
        .withColumn("sequence", F.coalesce("stop_sequence", "sequence_from_stop"))
        .where(F.col("sequence").isNotNull())
    )
    ordered = Window.partitionBy(*TRIP_KEY).orderBy("timestamp")
    progress = positions.withColumn(
        "progress", F.col("sequence") * 2 + F.when(F.col("status") == "STOPPED_AT", 1).otherwise(0)
    ).withColumn(
        # Progress never goes backwards; a stale or noisy report can't undo a reached stop.
        "progress",
        F.max("progress").over(ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )
    steps = (
        progress.withColumn("previous_progress", F.lag("progress").over(ordered))
        .withColumn("previous_time", F.lag("timestamp").over(ordered))
        .where(F.col("previous_progress").isNotNull() & (F.col("progress") > F.col("previous_progress")))
        .drop("stop_sequence", "stop_id", "sequence_from_stop")
    )
    reached = steps.join(gtfs.stop_times.select("trip_id", "stop_sequence"), "trip_id").where(
        (F.col("stop_sequence") * 2 >= F.col("previous_progress")) & (F.col("stop_sequence") * 2 < F.col("progress"))
    )
    return _between_polls(reached, config, "sequence")


def arrivals_from_proximity(observations: DataFrame, gtfs: Gtfs, config: Config) -> DataFrame:
    """Arrivals from distance: the first poll within proximity_m of a stop on the vehicle's trip."""
    ordered = Window.partitionBy(*TRIP_KEY).orderBy("timestamp")
    positions = observations.where(F.col("lat").isNotNull() & F.col("lon").isNotNull()).withColumn(
        "previous_time", F.lag("timestamp").over(ordered)
    )
    stop_points = gtfs.stop_times.select("trip_id", "stop_sequence", "stop_id").join(
        gtfs.stops.select("stop_id", "stop_lat", "stop_lon"), "stop_id"
    )
    near = positions.drop("stop_id", "stop_sequence").join(stop_points, "trip_id")
    near = near.withColumn("distance_m", _haversine_m("lat", "lon", "stop_lat", "stop_lon")).where(
        F.col("distance_m") <= config.proximity_m
    )
    first = Window.partitionBy(*TRIP_KEY, "stop_sequence").orderBy("timestamp")
    reached = near.withColumn("rank", F.row_number().over(first)).where("rank = 1").where(F.col("previous_time").isNotNull())
    return _between_polls(reached, config, "proximity")


def _between_polls(reached: DataFrame, config: Config, method: str) -> DataFrame:
    gap = F.col("timestamp") - F.col("previous_time")
    return reached.where(gap <= config.max_gap_s).select(
        *TRIP_KEY,
        "day_start",
        "stop_sequence",
        ((F.col("previous_time") + F.col("timestamp")) / 2).alias("actual"),
        (gap / 2).alias("uncertainty_s"),
        F.lit(method).alias("method"),
    )


def _haversine_m(lat1: str, lon1: str, lat2: str, lon2: str):
    to_rad = F.lit(3.141592653589793 / 180)
    d_lat = (F.col(lat2) - F.col(lat1)) * to_rad
    d_lon = (F.col(lon2) - F.col(lon1)) * to_rad
    a = F.sin(d_lat / 2) ** 2 + F.cos(F.col(lat1) * to_rad) * F.cos(F.col(lat2) * to_rad) * F.sin(d_lon / 2) ** 2
    return F.lit(2 * 6_371_000.0) * F.asin(F.sqrt(a))


def choose_method(observations: DataFrame, config: Config) -> str:
    if config.method != "auto":
        return config.method
    counts = observations.agg(
        F.count("*").alias("all"),
        F.sum(F.when(F.col("stop_sequence").isNotNull() | F.col("stop_id").isNotNull(), 1).otherwise(0)).alias("with_stop"),
    ).first()
    return "sequence" if counts["all"] and counts["with_stop"] / counts["all"] >= 0.5 else "proximity"


def score(arrivals: DataFrame, gtfs: Gtfs, config: Config) -> DataFrame:
    """One row per trip and stop: scheduled vs. actual arrival, delay, and on-time category."""
    scheduled = arrivals.join(gtfs.stop_times, ["trip_id", "stop_sequence"]).where(F.col("arrival_s").isNotNull())
    scheduled = scheduled.join(gtfs.trips, "trip_id", "left").withColumn("scheduled", F.col("day_start") + F.col("arrival_s"))
    delays = scheduled.withColumn("delay_s", F.round(F.col("actual") - F.col("scheduled")).cast("long")).where(
        F.abs(F.col("delay_s")) <= config.max_abs_delay_s
    )
    # Two vehicles reported on one trip (it happens): keep the better-placed arrival.
    best = Window.partitionBy("trip_id", "service_date", "stop_sequence").orderBy("uncertainty_s", "vehicle_id")
    deduped = delays.withColumn("rank", F.row_number().over(best)).where("rank = 1").drop("rank")
    category = (
        F.when(F.col("delay_s") < -config.early_s, "early").when(F.col("delay_s") > config.late_s, "late").otherwise("on_time")
    )
    local_hour = F.hour(F.from_utc_timestamp(F.timestamp_seconds("scheduled"), gtfs.tz))
    return deduped.select(
        "service_date",
        "route_id",
        "direction_id",
        "trip_id",
        "vehicle_id",
        "stop_id",
        "stop_sequence",
        F.col("scheduled").cast("long").alias("scheduled"),
        F.round("actual").cast("long").alias("actual"),
        "delay_s",
        F.round("uncertainty_s").cast("long").alias("uncertainty_s"),
        "method",
        category.alias("category"),
        local_hour.alias("hour"),
    )


def bunching(events: DataFrame, config: Config) -> DataFrame:
    """Pairs of consecutive vehicles at a stop on frequent routes, flagged when bunched."""
    order = Window.partitionBy("route_id", "direction_id", "stop_id", "service_date").orderBy("scheduled")
    pairs = (
        events.withColumn("scheduled_gap", F.col("scheduled") - F.lag("scheduled").over(order))
        .withColumn("actual_gap", F.col("actual") - F.lag("actual").over(order))
        .where(F.col("scheduled_gap").isNotNull() & (F.col("scheduled_gap") > 0) & (F.col("scheduled_gap") <= config.bunching_max_headway_s))
    )
    return pairs.withColumn("bunched", F.col("actual_gap") < F.col("scheduled_gap") * config.bunching_ratio)


def _rates(frame: DataFrame, keys: list[str]) -> DataFrame:
    total = F.count("*")
    pct = lambda name: F.round(F.sum(F.when(F.col("category") == name, 1).otherwise(0)) * 100.0 / total, 1)
    return frame.groupBy(*keys).agg(
        total.alias("arrivals"),
        F.countDistinct("trip_id", "service_date").alias("trips"),
        pct("on_time").alias("on_time_pct"),
        pct("early").alias("early_pct"),
        pct("late").alias("late_pct"),
        F.percentile_approx("delay_s", 0.5).alias("median_delay_s"),
        F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
    )


def summarize(events: DataFrame, gtfs: Gtfs, config: Config, observations: DataFrame, method: str) -> dict:
    routes = _rates(events, ["route_id"]).join(gtfs.routes, "route_id", "left")
    bunched = bunching(events, config)
    route_bunching = bunched.groupBy("route_id").agg(
        F.count("*").alias("pairs"), F.round(F.avg(F.col("bunched").cast("double")) * 100, 1).alias("bunched_pct")
    )
    routes = routes.join(route_bunching, "route_id", "left").orderBy(F.desc("arrivals"))
    stops = (
        _rates(events, ["route_id", "stop_id"])
        .where(F.col("arrivals") >= 5)
        .join(gtfs.stops.select("stop_id", "stop_name"), "stop_id", "left")
        .orderBy("on_time_pct", F.desc("arrivals"))
        .limit(25)
    )
    hours = _rates(events, ["hour"]).orderBy("hour")
    days = _rates(events, ["route_id", "service_date"]).orderBy("route_id", "service_date")
    overall = _rates(events.withColumn("all", F.lit(1)), ["all"]).drop("all").first()
    window = observations.agg(F.min("timestamp").alias("start"), F.max("timestamp").alias("end"), F.countDistinct("vehicle_id").alias("vehicles"), F.count("*").alias("positions")).first()
    rows = lambda frame: [row.asDict() for row in frame.collect()]
    as_iso = lambda t: datetime.fromtimestamp(t, tz=timezone.utc).isoformat() if t is not None else None
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "timezone": gtfs.tz,
        "method": method,
        "config": asdict(config),
        "observed": {
            "positions": window["positions"],
            "vehicles": window["vehicles"],
            "start": as_iso(window["start"]),
            "end": as_iso(window["end"]),
        },
        "overall": overall.asDict() if overall else {},
        "routes": rows(routes),
        "hours": rows(hours),
        "worst_stops": rows(stops),
        "route_days": rows(days),
    }


def run(spark: SparkSession, gtfs_folder: str, raw_folder: str, out_folder: str, config: Config = Config()) -> dict:
    gtfs = load_gtfs(spark, gtfs_folder)
    observations = assign_service_dates(spark, load_observations(spark, raw_folder), gtfs).cache()
    if observations.limit(1).count() == 0:
        raise ValueError("No saved positions matched trips in this schedule. Is the schedule for the same agency and dates?")
    method = choose_method(observations, config)
    arrivals = arrivals_from_sequence(observations, gtfs, config) if method == "sequence" else arrivals_from_proximity(observations, gtfs, config)
    events = score(arrivals, gtfs, config).cache()
    events.write.mode("overwrite").partitionBy("service_date").parquet(os.path.join(out_folder, "stop_events"))
    summary = summarize(events, gtfs, config, observations, method)
    os.makedirs(out_folder, exist_ok=True)
    with open(os.path.join(out_folder, "summary.json"), "w") as out:
        json.dump(summary, out, indent=2, default=str)
    return summary
