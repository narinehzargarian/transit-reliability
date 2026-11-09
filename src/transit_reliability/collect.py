"""Saving a live GTFS-Realtime feed over time, to local files, S3, or a Kinesis stream."""

from __future__ import annotations

import gzip
import io
import json
import os
import time
from datetime import datetime, timezone
from typing import Callable, Iterable, Protocol

from .feed import decode_vehicle_positions, fetch
from .storage import is_s3, split_s3


def _partition(poll_time: float) -> str:
    moment = datetime.fromtimestamp(poll_time, tz=timezone.utc)
    return f"date={moment:%Y-%m-%d}/hour={moment:%H}"


def _jsonl_gz(rows: Iterable[dict]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        for row in rows:
            gz.write((json.dumps(row, separators=(",", ":")) + "\n").encode())
    return buffer.getvalue()


class Sink(Protocol):
    def write(self, rows: list[dict], poll_time: float) -> None: ...


class LocalSink:
    """Gzipped JSON lines, one file per poll, in date=/hour= folders (UTC)."""

    def __init__(self, folder: str):
        self.folder = folder

    def write(self, rows: list[dict], poll_time: float) -> None:
        if not rows:
            return
        folder = os.path.join(self.folder, _partition(poll_time))
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"part-{int(poll_time * 1000)}.jsonl.gz")
        with open(path, "wb") as out:
            out.write(_jsonl_gz(rows))


class S3Sink:
    def __init__(self, uri: str, client=None):
        import boto3

        self.bucket, self.prefix = split_s3(uri)
        self.client = client or boto3.client("s3")

    def write(self, rows: list[dict], poll_time: float) -> None:
        if not rows:
            return
        key = "/".join(filter(None, [self.prefix, _partition(poll_time), f"part-{int(poll_time * 1000)}.jsonl.gz"]))
        self.client.put_object(Bucket=self.bucket, Key=key, Body=_jsonl_gz(rows))


class KinesisSink:
    """Sends each vehicle position as one record, keyed by vehicle so a vehicle's positions stay in order."""

    def __init__(self, stream: str, client=None):
        import boto3

        self.stream = stream
        self.client = client or boto3.client("kinesis")

    def write(self, rows: list[dict], poll_time: float) -> None:
        for start in range(0, len(rows), 500):  # Kinesis takes at most 500 records per call.
            batch = [{"Data": json.dumps(row).encode(), "PartitionKey": row["vehicle_id"]} for row in rows[start:start + 500]]
            response = self.client.put_records(StreamName=self.stream, Records=batch)
            if response.get("FailedRecordCount"):
                retry = [record for record, result in zip(batch, response["Records"]) if "ErrorCode" in result]
                time.sleep(1)
                again = self.client.put_records(StreamName=self.stream, Records=retry)
                if again.get("FailedRecordCount"):
                    raise RuntimeError(f"{again['FailedRecordCount']} records could not be sent to Kinesis")


def make_sink(target: str | None, kinesis_stream: str | None = None) -> Sink:
    if kinesis_stream:
        return KinesisSink(kinesis_stream)
    if target is None:
        raise ValueError("Give an output folder, an s3:// location, or a Kinesis stream")
    return S3Sink(target) if is_s3(target) else LocalSink(target)


class NewPositions:
    """Feeds repeat a vehicle's last position until it reports again; keep only new reports."""

    def __init__(self) -> None:
        self.last_seen: dict[str, int] = {}

    def filter(self, rows: list[dict]) -> list[dict]:
        fresh = []
        for row in rows:
            if row["timestamp"] > self.last_seen.get(row["vehicle_id"], -1):
                self.last_seen[row["vehicle_id"]] = row["timestamp"]
                fresh.append(row)
        return fresh


def collect(
    url: str,
    sink: Sink,
    feed: str,
    interval_s: float = 30,
    duration_s: float | None = None,
    headers: dict[str, str] | None = None,
    fetcher: Callable[[str, dict[str, str] | None], bytes] = fetch,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> dict:
    """Polls the feed every interval_s seconds until duration_s has passed (forever if None).

    A failed poll is logged and skipped, so a network blip doesn't end a long collection.
    """
    started = now()
    fresh_filter = NewPositions()
    stats = {"polls": 0, "positions": 0, "errors": 0}
    while duration_s is None or now() - started < duration_s:
        poll_time = now()
        try:
            rows = fresh_filter.filter(decode_vehicle_positions(fetcher(url, headers), feed))
            sink.write(rows, poll_time)
            stats["positions"] += len(rows)
        except Exception as error:  # noqa: BLE001 - one bad poll shouldn't stop collection
            stats["errors"] += 1
            log(f"poll failed: {error}")
        stats["polls"] += 1
        if stats["polls"] % 10 == 0:
            log(f"{stats['polls']} polls, {stats['positions']} new positions, {stats['errors']} errors")
        sleep(max(0.0, interval_s - (now() - poll_time)))
    return stats


def drain_kinesis(stream: str, sink: Sink, client=None, idle_reads: int = 3, sleep: Callable[[float], None] = time.sleep) -> int:
    """Reads every record in the stream from the start and writes them through sink. Returns the count."""
    import boto3

    client = client or boto3.client("kinesis")
    total = 0
    shards = client.list_shards(StreamName=stream)["Shards"]
    for shard in shards:
        iterator = client.get_shard_iterator(StreamName=stream, ShardId=shard["ShardId"], ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        idle = 0
        while iterator and idle < idle_reads:
            response = client.get_records(ShardIterator=iterator, Limit=10000)
            rows = [json.loads(record["Data"]) for record in response["Records"]]
            if rows:
                sink.write(rows, time.time())
                total += len(rows)
                idle = 0
            elif response.get("MillisBehindLatest", 0) == 0:
                idle += 1
                sleep(0.2)
            iterator = response.get("NextShardIterator")
    return total
