"""Download a public CSV and upload it to an S3 raw data bucket.

Example:
python src/ingest_raw.py --bucket my-raw-bucket --key sample.csv --url https://example.com/data.csv
"""
import argparse
import requests
import boto3
from botocore.exceptions import ClientError


def download_file(url: str) -> bytes:
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content


def upload_to_s3(
    bucket: str,
    key: str,
    data: bytes,
    region: str = None,
    endpoint_url: str = None,
    access_key: str = None,
    secret_key: str = None,
    use_ssl: bool = True,
) -> None:
    # Create S3 client that can point to a local S3-compatible endpoint (eg. MinIO)
    kwargs = {}
    if region:
        kwargs['region_name'] = region
    if endpoint_url:
        kwargs['endpoint_url'] = endpoint_url
    if access_key:
        kwargs['aws_access_key_id'] = access_key
    if secret_key:
        kwargs['aws_secret_access_key'] = secret_key
    # boto3 accepts use_ssl for some endpoints via config; most S3-compatible servers work with endpoint_url
    s3 = boto3.client('s3', **kwargs)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=data)
        print(f"Uploaded to s3://{bucket}/{key}")
    except ClientError as e:
        print("Failed to upload to S3:", e)
        raise


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--bucket', required=True)
    p.add_argument('--key', required=True)
    p.add_argument('--url', required=True)
    p.add_argument('--region', default=None)
    p.add_argument('--s3-endpoint', default=None, help='S3 endpoint URL (eg http://localhost:9000)')
    p.add_argument('--s3-access-key', default=None)
    p.add_argument('--s3-secret-key', default=None)
    p.add_argument('--s3-use-ssl', default='true', help='Whether to use SSL for S3 endpoint')
    args = p.parse_args()

    data = download_file(args.url)
    use_ssl = str(args.s3_use_ssl).lower() in ('1', 'true', 'yes')
    upload_to_s3(
        args.bucket,
        args.key,
        data,
        region=args.region,
        endpoint_url=args.s3_endpoint,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        use_ssl=use_ssl,
    )


if __name__ == '__main__':
    main()
