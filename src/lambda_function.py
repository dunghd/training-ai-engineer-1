import os
import json
import boto3
from botocore.exceptions import NoCredentialsError
import csv
from io import BytesIO, StringIO
from src.opensearch_client import bulk_index

# Prefer pandas/numpy if available for richer CSV parsing, otherwise fallback to stdlib csv
USE_PANDAS = False
try:
    import pandas as pd
    import numpy as np
    USE_PANDAS = True
except Exception:
    USE_PANDAS = False


def make_s3_client():
    # Build an S3 client that can point to a local S3-compatible endpoint (MinIO / LocalStack)
    endpoint = os.environ.get('S3_ENDPOINT')
    access = os.environ.get('S3_ACCESS_KEY')
    secret = os.environ.get('S3_SECRET_KEY')
    region = os.environ.get('AWS_REGION')
    kwargs = {}
    if region:
        kwargs['region_name'] = region
    if endpoint:
        kwargs['endpoint_url'] = endpoint
    if access and secret:
        kwargs['aws_access_key_id'] = access
        kwargs['aws_secret_access_key'] = secret
    return boto3.client('s3', **kwargs)


s3 = make_s3_client()


def csv_to_records(content: bytes):
    if USE_PANDAS:
        # Use pandas for robust parsing and NaN handling
        df = pd.read_csv(BytesIO(content))
        df = df.replace({np.nan: None})
        return df.to_dict(orient='records')
    # Fallback to stdlib csv
    text = content.decode('utf-8')
    reader = csv.DictReader(StringIO(text))
    records = []
    for row in reader:
        cleaned = {k: (v if v != '' else None) for k, v in row.items()}
        records.append(cleaned)
    return records


def lambda_handler(event, context):
    # expects S3 put event
    index_name = os.environ.get('INDEX_NAME', 'records')
    processed_bucket = os.environ.get('PROCESSED_BUCKET')

    for rec in event.get('Records', []):
        # defensively extract bucket/key to avoid None being passed to boto3
        s3_bucket = rec.get('s3', {}).get('bucket', {}).get('name')
        s3_key = rec.get('s3', {}).get('object', {}).get('key')
        if not s3_bucket or not s3_key:
            # Provide a clear, actionable error instead of the botocore TypeError
            raise ValueError(
                f"S3 bucket or key missing in event record. "
                f"Got bucket={s3_bucket!r}, key={s3_key!r}. "
                f"Ensure the event or RAW_BUCKET env var is set when testing locally."
            )
        try:
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        except NoCredentialsError:
            raise RuntimeError(
                "AWS credentials not found for S3. "
                "Set S3_ACCESS_KEY and S3_SECRET_KEY (or configure AWS credentials), "
                "or export AWS_PROFILE / AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY."
            )
        body = obj['Body'].read()
        try:
            docs = csv_to_records(body)
            # import opensearch client lazily to avoid import-time failures when deps/env missing
            from src.opensearch_client import bulk_index
            # bulk index
            res = bulk_index(index_name, docs)
            print('Indexed', res)
            # optionally move processed file
            if processed_bucket:
                copy_source = {'Bucket': s3_bucket, 'Key': s3_key}
                s3.copy_object(CopySource=copy_source, Bucket=processed_bucket, Key=s3_key)
                s3.delete_object(Bucket=s3_bucket, Key=s3_key)
        except Exception as e:
            print('Error processing', s3_bucket, s3_key, e)
            raise
