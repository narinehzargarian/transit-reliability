# transit-reliability

Measure how reliably buses and trains run, from any city's public GTFS-Realtime feed.

Most transit agencies publish live vehicle positions but not how often their service is actually on time. This tool saves those positions, matches them to the published schedule with Apache Spark, and reports:

- on-time, early, and late rates, overall and per route
- which hours of the day are worst
- the stops with the lowest on-time rate
- bunching: vehicles arriving close together instead of evenly spaced

Output is a Parquet table of every inferred stop arrival, a `summary.json`, and a self-contained HTML report.

It runs on a laptop with local files, or on AWS with Kinesis, S3, and DynamoDB. The AWS path can be run free on your own machine with [LocalStack](https://github.com/localstack/localstack).

## Install

Needs Python 3.10+ and Java 17 or 21 (for Spark).

```sh
git clone https://github.com/narinehzargarian/transit-reliability
cd transit-reliability
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Quick start (Boston MBTA, no API key)

```sh
# 1. Save live vehicle positions every 20 seconds for an hour
transit-reliability collect https://cdn.mbta.com/realtime/VehiclePositions.pb \
  --out raw/ --feed-name mbta --interval 20 --duration 3600

# 2. Match them to the schedule
transit-reliability compute --gtfs https://cdn.mbta.com/MBTA_GTFS.zip --raw raw/ --out results/

# 3. Build the report
transit-reliability report --results results/ --out report.html --title "MBTA"
```

Any agency with a GTFS-Realtime VehiclePositions feed and a static GTFS works the same way. Feeds that need a key take a header:

```sh
transit-reliability collect https://example.com/vehicle-positions --header "x-api-key: YOUR_KEY" --out raw/
```

The longer you collect, the more useful the numbers. An hour gives a rough picture; a few weekdays gives per-hour and per-stop results you can trust.

## Example: 48 minutes of Boston MBTA

A short run on a Sunday night, 10:37 to 11:25 pm Eastern, polling every 20 seconds:

| | |
|---|---|
| Positions saved | 31,833 from 260 vehicles, 0 failed polls |
| Stop arrivals inferred | 7,712 across 453 trips and 90 routes |
| On time / late / early | 67.1% / 18.7% / 14.3% |
| Median delay | 1.5 min late (90th percentile 7.5 min) |
| Arrival uncertainty | about +/-11 s (half the poll gap) |

Route 66 was late at 62.6% of its stops (median 6.9 min). The Orange Line was on time 82.0% of the time. 10.4% of route 109's arrivals were bunched. One bus on route 134 ran 5 to 7 minutes early for 28 stops in a row, which is why a short window says little about a single route. Collect for several days before comparing routes.

## Commands

| Command | What it does |
|---|---|
| `collect FEED_URL --out DIR\|s3://...` | Polls the feed and saves new positions as gzipped JSON lines, partitioned by date and hour |
| `collect FEED_URL --kinesis-stream NAME` | Sends positions to a Kinesis stream instead |
| `drain STREAM --out DIR\|s3://...` | Reads a Kinesis stream and saves the positions the same way `collect --out` does |
| `compute --gtfs ... --raw ... --out ...` | Spark job: infers stop arrivals, scores them against the schedule, writes Parquet and `summary.json` |
| `report --results DIR --out report.html` | Renders the summary as one HTML file |
| `publish --results DIR --table NAME` | Writes per-route, per-day results to DynamoDB |

`compute` options:

| Option | Default | Meaning |
|---|---|---|
| `--early` | 60 | Seconds ahead of schedule before an arrival counts as early |
| `--late` | 300 | Seconds behind schedule before an arrival counts as late |
| `--max-gap` | 150 | Skip arrivals that fall between polls further apart than this |
| `--method` | auto | `sequence`, `proximity`, or `auto` (see below) |
| `--master` | Spark's default | Spark master, e.g. `local[4]` |

The 60 s early / 5 min late window is the common US agency definition. Change it to match your agency's.

## How arrivals are inferred

Feeds report where each vehicle is every so often, not when it reached each stop, so arrivals have to be inferred.

**Sequence method** (used when the feed reports `current_stop_sequence`). Each position is turned into a progress number along the trip. Between two polls where progress passes a stop, the arrival is placed at the midpoint, and its uncertainty is half the gap between polls. With 20 s polling that is about ±10 s.

**Proximity method** (used when a feed only has coordinates). The first poll where the vehicle is within 40 m of the stop counts as the arrival. This reads slightly early, by up to the time it takes to cover 40 m.

`auto` uses the sequence method when most positions carry a stop sequence, otherwise proximity.

Other details:

- Trips are matched to their service day using GTFS rules, so trips after midnight (times like `25:10:00`) and daylight-saving days are handled.
- If the feed went quiet for longer than `--max-gap`, arrivals in that window are skipped rather than guessed.
- Bunching: two vehicles on the same route, direction, and stop whose actual gap is under 25% of the scheduled gap, only counted where the scheduled headway is 15 minutes or less.

## Running on AWS

```
GTFS-Realtime feed -> collect -> Kinesis -> drain -> S3 (raw)
                                                       |
                         static GTFS (S3 or URL) -> compute (Spark) -> S3 (results)
                                                                          |
                                                          report (HTML)   publish -> DynamoDB
```

`infra/terraform` creates the bucket (private, encrypted, raw data expires after 30 days), an on-demand Kinesis stream, a pay-per-request DynamoDB table, and an optional budget alert:

```sh
cd infra/terraform
terraform init
terraform apply -var budget_email=you@example.com
```

Then use the outputs:

```sh
transit-reliability collect FEED_URL --kinesis-stream <kinesis_stream>
transit-reliability drain <kinesis_stream> --out s3://<bucket>/raw/
transit-reliability compute --gtfs GTFS_URL --raw s3://<bucket>/raw/ --out s3://<bucket>/results/
transit-reliability publish --results s3://<bucket>/results/ --table <dynamodb_table>
```

An on-demand Kinesis stream bills per hour while it exists, so destroy it (`terraform destroy`) when you are done. For long collection runs, `collect --out s3://...` skips Kinesis entirely.

### Free, on your machine, with LocalStack

```sh
docker compose up -d
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
```

Every command above then talks to LocalStack instead of AWS. The integration test runs the full Kinesis -> S3 -> Spark -> S3 -> DynamoDB path this way:

```sh
pip install -e ".[dev]"
LOCALSTACK=1 pytest tests/integration
```

## Tests

```sh
pytest
```

The Spark tests build a small schedule, simulate vehicles running it with known delays (early, late, after midnight, a feed outage, bunched buses), and check that the inferred arrivals and summaries match.

## Limits

- Results are only as good as the feed. Vehicles with no trip ID are ignored, and arrivals more than 3 hours off schedule are dropped as bad matches.
- The proximity method reads early, by up to 40 m of travel time.
- Arrivals are scored against the static schedule, not against real-time trip updates or added trips.
- The Terraform has been applied against LocalStack and checked with `terraform validate`, but not yet against a real AWS account.

## License

MIT
