"""Command line: collect, drain, compute, report, publish."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile

from . import __version__


def _headers(values: list[str]) -> dict[str, str]:
    headers = {}
    for value in values:
        name, _, content = value.partition(":")
        if not content:
            raise SystemExit(f"Header must look like 'Name: value', got {value!r}")
        headers[name.strip()] = content.strip()
    return headers


def cmd_collect(args) -> None:
    from .collect import collect, make_sink

    sink = make_sink(args.out, args.kinesis_stream)
    stats = collect(args.feed_url, sink, feed=args.feed_name, interval_s=args.interval, duration_s=args.duration, headers=_headers(args.header))
    print(json.dumps(stats))


def cmd_drain(args) -> None:
    from .collect import drain_kinesis, make_sink

    count = drain_kinesis(args.kinesis_stream, make_sink(args.out))
    print(f"Wrote {count} positions from {args.kinesis_stream} to {args.out}")


def cmd_compute(args) -> None:
    from .spark_job import Config, run, spark_session
    from .storage import gtfs_folder, is_s3, local_input, upload_dir

    config = Config(early_s=args.early, late_s=args.late, max_gap_s=args.max_gap, method=args.method)
    with tempfile.TemporaryDirectory() as workdir:
        gtfs = gtfs_folder(args.gtfs, workdir)
        raw = local_input(args.raw, workdir, "raw")
        out = os.path.join(workdir, "out") if is_s3(args.out) else args.out
        summary = run(spark_session(args.master), gtfs, raw, out, config)
        if is_s3(args.out):
            upload_dir(out, args.out)
    overall = summary["overall"]
    print(
        f"{overall.get('arrivals', 0)} stop arrivals: {overall.get('on_time_pct')}% on time, "
        f"{overall.get('late_pct')}% late, {overall.get('early_pct')}% early ({summary['method']} method). Results in {args.out}"
    )


def cmd_report(args) -> None:
    from .report import write_report

    write_report(os.path.join(args.results, "summary.json"), args.out, args.title)
    print(f"Wrote {args.out}")


def cmd_publish(args) -> None:
    from .publish import publish

    with open(os.path.join(args.results, "summary.json")) as source:
        count = publish(json.load(source), args.table, args.feed_name)
    print(f"Wrote {count} items to DynamoDB table {args.table}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="transit-reliability", description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect", help="Save a GTFS-Realtime VehiclePositions feed over time")
    collect.add_argument("feed_url")
    collect.add_argument("--out", help="Folder or s3://bucket/prefix for saved positions")
    collect.add_argument("--kinesis-stream", help="Send positions to this Kinesis stream instead")
    collect.add_argument("--feed-name", default="feed")
    collect.add_argument("--interval", type=float, default=30, help="Seconds between polls (default 30)")
    collect.add_argument("--duration", type=float, help="Stop after this many seconds (default: run until stopped)")
    collect.add_argument("--header", action="append", default=[], help="Extra request header, e.g. 'x-api-key: KEY'")
    collect.set_defaults(func=cmd_collect)

    drain = commands.add_parser("drain", help="Copy everything in a Kinesis stream to a folder or S3")
    drain.add_argument("kinesis_stream")
    drain.add_argument("--out", required=True)
    drain.set_defaults(func=cmd_drain)

    compute = commands.add_parser("compute", help="Measure reliability with Spark")
    compute.add_argument("--gtfs", required=True, help="Static GTFS: folder, .zip, URL, or s3:// zip")
    compute.add_argument("--raw", required=True, help="Saved positions: folder or s3:// prefix")
    compute.add_argument("--out", required=True, help="Results: folder or s3:// prefix")
    compute.add_argument("--early", type=int, default=60, help="Seconds early before an arrival counts as early (default 60)")
    compute.add_argument("--late", type=int, default=300, help="Seconds late before an arrival counts as late (default 300)")
    compute.add_argument("--max-gap", type=int, default=150, help="Skip arrivals between polls further apart than this (default 150)")
    compute.add_argument("--method", choices=["auto", "sequence", "proximity"], default="auto")
    compute.add_argument("--master", help="Spark master, e.g. local[4] (default: Spark's own)")
    compute.set_defaults(func=cmd_compute)

    report = commands.add_parser("report", help="Write an HTML report from results")
    report.add_argument("--results", required=True)
    report.add_argument("--out", default="report.html")
    report.add_argument("--title", default="Transit reliability")
    report.set_defaults(func=cmd_report)

    publish = commands.add_parser("publish", help="Write route results to a DynamoDB table")
    publish.add_argument("--results", required=True)
    publish.add_argument("--table", required=True)
    publish.add_argument("--feed-name", default="feed")
    publish.set_defaults(func=cmd_publish)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
