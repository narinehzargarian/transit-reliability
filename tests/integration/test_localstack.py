"""The AWS path end to end, against LocalStack: Kinesis -> S3 -> Spark -> S3 -> DynamoDB."""

import json
import os
import uuid

import pytest
from sim import day_start, simulate, write_gtfs

pytestmark = [pytest.mark.integration, pytest.mark.skipif(not os.environ.get("LOCALSTACK"), reason="set LOCALSTACK=1 with LocalStack running")]

H = 3600
DAY = "20260302"
T1 = [8 * H, 8 * H + 300, 8 * H + 600, 8 * H + 900]


@pytest.fixture(autouse=True)
def localstack_env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def test_kinesis_to_s3_to_spark_to_dynamodb(tmp_path):
    import boto3

    from transit_reliability.cli import main
    from transit_reliability.collect import KinesisSink, S3Sink, drain_kinesis

    run_id = uuid.uuid4().hex[:8]
    bucket, stream, table = f"transit-{run_id}", f"positions-{run_id}", f"routes-{run_id}"
    s3, kinesis, dynamodb = boto3.client("s3"), boto3.client("kinesis"), boto3.client("dynamodb")
    s3.create_bucket(Bucket=bucket)
    kinesis.create_stream(StreamName=stream, ShardCount=1)
    kinesis.get_waiter("stream_exists").wait(StreamName=stream)
    dynamodb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "route", "KeyType": "HASH"}, {"AttributeName": "period", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "route", "AttributeType": "S"}, {"AttributeName": "period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # 1. The collector sends positions to Kinesis (a bus 2 minutes late everywhere).
    start = day_start(DAY)
    positions = simulate("T1", "bus-1", [start + s + 120 for s in T1], first_poll=start + 8 * H - 305, start_date=DAY)
    KinesisSink(stream).write(positions, poll_time=start)

    # 2. Drain the stream into S3.
    assert drain_kinesis(stream, S3Sink(f"s3://{bucket}/raw")) == len(positions)

    # 3. Spark reads the schedule and positions from S3 and writes results back to S3.
    gtfs = tmp_path / "gtfs"
    write_gtfs(str(gtfs), {"T1": T1})
    for name in os.listdir(gtfs):
        s3.upload_file(str(gtfs / name), bucket, f"gtfs/{name}")
    main(["compute", "--gtfs", str(gtfs), "--raw", f"s3://{bucket}/raw", "--out", f"s3://{bucket}/results", "--master", "local[2]"])
    summary = json.loads(s3.get_object(Bucket=bucket, Key="results/summary.json")["Body"].read())
    assert summary["overall"]["arrivals"] == 4
    assert summary["overall"]["on_time_pct"] == 100.0
    keys = [item["Key"] for item in s3.list_objects_v2(Bucket=bucket, Prefix="results/stop_events/")["Contents"]]
    assert any(key.endswith(".parquet") for key in keys)

    # 4. Route results go to DynamoDB.
    results = tmp_path / "results"
    results.mkdir()
    (results / "summary.json").write_text(json.dumps(summary))
    main(["publish", "--results", str(results), "--table", table, "--feed-name", "test"])
    item = boto3.resource("dynamodb").Table(table).get_item(Key={"route": "test#R1", "period": "all"})["Item"]
    assert item["arrivals"] == 4 and float(item["on_time_pct"]) == 100.0
