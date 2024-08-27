#!/bin/bash

# verifying localstack is up, this needs to be improved to avoid eternal loops
until awslocal s3api list-buckets > /dev/null 2>&1; do
  echo "Waiting for LocalStack to be ready..."
  sleep 5
done

awslocal s3api create-bucket --bucket raw-climate-data
echo "raw-climate-data bucket created successfully"
awslocal s3api create-bucket --bucket staging-climate-data
echo "staging-climate-data bucket created successfully"