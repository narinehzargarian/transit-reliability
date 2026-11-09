output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "kinesis_stream" {
  value = aws_kinesis_stream.positions.name
}

output "dynamodb_table" {
  value = aws_dynamodb_table.routes.name
}
