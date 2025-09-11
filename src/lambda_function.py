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


_s3_client = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = make_s3_client()
    return _s3_client


def csv_to_records(content: bytes):
    # Configurable safety: if CSV contains more than CSV_FIELD_LIMIT columns
    # we either 'collapse' the extra columns into a single `extra_columns` dict
    # field per-record, or 'prune' them (drop). Set via env vars:
    #   CSV_FIELD_LIMIT (int, default 1000)
    #   CSV_OVERFLOW_STRATEGY ('collapse' or 'prune', default 'collapse')
    limit = int(os.environ.get('CSV_FIELD_LIMIT', '1000'))
    strategy = os.environ.get('CSV_OVERFLOW_STRATEGY', 'collapse').lower()

    if USE_PANDAS:
        # Use pandas for robust parsing and NaN handling
        df = pd.read_csv(BytesIO(content))
        df = df.replace({np.nan: None})
        cols = list(df.columns)
        if len(cols) > limit:
            print(f'CSV has {len(cols)} columns, exceeding limit {limit}. Using strategy={strategy}')
            keep = cols[:limit]
            extra = cols[limit:]
            if strategy == 'prune':
                df = df[keep]
                return df.to_dict(orient='records')
            # collapse: produce records where extra columns are nested under 'extra_columns'
            records = []
            for _, row in df.iterrows():
                base = {k: row[k] for k in keep}
                extras = {k: row[k] for k in extra if row[k] is not None}
                base['extra_columns'] = extras or None
                records.append(base)
            return records

    # Fallback to stdlib csv
    text = content.decode('utf-8')
    reader = csv.DictReader(StringIO(text))
    records = []
    header = reader.fieldnames or []
    if header and len(header) > limit:
        print(f'CSV has {len(header)} columns, exceeding limit {limit}. Using strategy={strategy}')
        allowed = header[:limit]
        extra_cols = header[limit:]
        if strategy == 'prune':
            for row in reader:
                cleaned = {k: (v if v != '' else None) for k, v in row.items() if k in allowed}
                records.append(cleaned)
            return records
        # collapse
        for row in reader:
            base = {k: (v if v != '' else None) for k, v in row.items() if k in allowed}
            extras = {k: v for k, v in row.items() if k in extra_cols and v != ''}
            base['extra_columns'] = extras or None
            records.append(base)
        return records

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
            s3 = get_s3_client()
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
