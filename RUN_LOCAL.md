Run the project locally (MinIO + OpenSearch)

This document explains the minimal steps to run the project locally using Docker Compose (MinIO + OpenSearch), upload a sample CSV, run the Lambda handler locally and verify that documents are indexed.

Checklist

- Create `.env.local` with environment variables for MinIO/OpenSearch.
- Start services with Docker Compose.
- Source `.env.local` into your shell.
- Create MinIO buckets (raw + processed).
- Install Python dependencies (Poetry or pip).
- Upload a sample CSV with `src/ingest_raw.py`.
- Run the Lambda handler locally (simulate S3 put event).
- Verify documents in OpenSearch.
- Optional: run tests.

1. Example `.env.local`

Create `./.env.local` in the project root with values like:

```bash
# .env.local (example)
S3_ENDPOINT="http://localhost:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"
RAW_BUCKET="raw-bucket"
PROCESSED_BUCKET="processed-bucket"
OPENSEARCH_ENDPOINT="http://localhost:9200"
INDEX_NAME="records"
AWS_REGION="us-east-1"
# If your OpenSearch uses auth, set these:
# OPENSEARCH_USER=""
# OPENSEARCH_PASS=""
```

2. Start services

```bash
docker compose up -d
```

3. Source the env file (zsh / bash)

```bash
source .env.local
```

4. Create buckets in MinIO (using aws CLI configured to talk to MinIO endpoint)

```bash
aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$RAW_BUCKET" --region "$AWS_REGION"
aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$PROCESSED_BUCKET" --region "$AWS_REGION"
```

Or use the MinIO web console at http://localhost:9001 (default creds: `minioadmin`/`minioadmin`).

5. Install Python dependencies

Preferred (Poetry):

```bash
poetry install
```

Or with pip:

```bash
pip install -r requirements.txt
```

6. Upload a sample CSV to the raw bucket

```bash
python src/ingest_raw.py \
  --bucket "$RAW_BUCKET" \
  --key sample.csv \
  --url "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY" \
  --s3-use-ssl false
```

7. Run the Lambda handler locally (simulate S3 Put event)

```bash
python3 - <<'PY'
import os
from src.lambda_function import lambda_handler

event = {
  "Records": [
    {
      "s3": {
        "bucket": {"name": os.environ.get('RAW_BUCKET')},
        "object": {"key": "sample.csv"}
      }
    }
  ]
}

lambda_handler(event, None)
print("Lambda run finished")
PY
```

8. Verify documents in OpenSearch

```bash
curl -sS "$OPENSEARCH_ENDPOINT/_cat/indices?v"
curl -sS "$OPENSEARCH_ENDPOINT/${INDEX_NAME}/_search?pretty" | jq .
```

(If you don't have `jq`, omit the pipe.)

9. Run tests

```bash
# Using Makefile
make test
# Or directly
python -m pytest -q
```

Troubleshooting

- If OpenSearch is not ready, wait 30-60s and retry (container healthchecks may still be running).
- Ensure `.env.local` is sourced before running the Python snippets so `OPENSEARCH_ENDPOINT` and `RAW_BUCKET` are set.
- If MinIO S3 calls fail, confirm the `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` values match MinIO's settings.

That's it — follow these steps to run everything locally and verify indexed records.
