# Changelog

## 0.1.0

- `collect`: save GTFS-Realtime vehicle positions to a folder, S3, or Kinesis
- `drain`: copy positions from Kinesis to a folder or S3
- `compute`: Spark job that infers stop arrivals (stop sequence or proximity), scores them against the schedule, and finds bunching
- `report`: self-contained HTML report
- `publish`: per-route, per-day results to DynamoDB
- Terraform for S3, Kinesis, DynamoDB, and a budget alert
- LocalStack setup and integration test
