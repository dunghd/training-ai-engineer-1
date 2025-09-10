# Automated S3 -> Lambda -> OpenSearch pipeline

This project contains a starter Python implementation for a fully automated pipeline that:

- Ingests a public dataset and uploads it to a raw S3 bucket.
- Triggers an AWS Lambda function on new S3 objects.
- Processes the file (Pandas), converts records to JSON, and indexes them into OpenSearch.

What's included

- `src/ingest_raw.py` — CLI script: downloads a CSV and uploads it to the configured S3 bucket.
- `src/lambda_function.py` — AWS Lambda handler: reads S3 object, processes with Pandas, indexes to OpenSearch via `opensearch_client`.
- `src/opensearch_client.py` — small wrapper around OpenSearch bulk indexing (uses environment variables for configuration).
- `template.yaml` — AWS SAM template to create the raw S3 bucket and Lambda with S3 event trigger.
- `requirements.txt` — Python dependencies.
- `tests/test_opensearch_client.py` — minimal test for the conversion helper.

Quick start

1. Install Poetry, AWS SAM CLI and configure your AWS credentials (profile or env vars).
2. Install dependencies with Poetry and build/deploy with SAM (example):

````bash
Run locally (OpenSearch + MinIO, no AWS)

You can run a full local environment that mimics S3 + OpenSearch using Docker Compose and the included `.env` file. Follow these steps:

1. Create a local `.env` from the example and review values:

# Automated S3 -> Lambda -> OpenSearch pipeline

This project is a starter Python implementation for a pipeline that:

- Ingests a public dataset and uploads it to a raw S3 bucket.
- Triggers an AWS Lambda function on new S3 objects.
- Processes the file (Pandas or stdlib csv), converts records to JSON, and indexes them into OpenSearch.

What's included

- `src/ingest_raw.py` — CLI script: downloads a CSV and uploads it to the configured S3 bucket.
- `src/lambda_function.py` — AWS Lambda handler: reads S3 object, processes with Pandas (if available) or csv, indexes to OpenSearch via `opensearch_client`.
- `src/opensearch_client.py` — small wrapper around OpenSearch bulk indexing (uses environment variables for configuration).
- `template.yaml` — AWS SAM template to create the raw S3 bucket and Lambda with S3 event trigger.
- `requirements.txt` — Python dependencies.
- `tests/test_opensearch_client.py` — minimal test for the conversion helper.

Quick start (AWS)

1. Install Poetry, AWS SAM CLI and configure your AWS credentials (profile or env vars).
1. Install dependencies with Poetry and build/deploy with SAM (example):

```bash
# from project root
poetry install
sam build
sam deploy --guided
````

Run locally (OpenSearch + MinIO, no AWS)

You can run a full local environment that mimics S3 + OpenSearch using Docker Compose and the included `.env` file. Follow these steps.

1. Create a local `.env` copy and review values:

```bash
cp .env .env.local
# Edit .env.local if you want different bucket names or credentials
```

1. Start the services with Docker Compose:

```bash
docker compose up -d
```

1. Source the env file so the example commands pick up the variables (zsh/bash):

```bash
source .env.local
```

1. Create buckets in MinIO. Use the MinIO console at <http://localhost:9001> or the `aws` CLI pointed at the MinIO endpoint:

```bash
# using aws-cli against MinIO
aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$RAW_BUCKET"
aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$PROCESSED_BUCKET"
```

1. Install Python dependencies (use Poetry or pip):

```bash
# Using Poetry (preferred)
poetry install
# OR using pip
pip install -r requirements.txt
```

1. Upload a sample CSV to the raw bucket with the included CLI (it will fetch a public CSV and upload to MinIO):

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

1. Run the Lambda handler locally (simulate the S3 put event). Example using the env vars from `.env.local`:

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

1. Verify documents in OpenSearch:

```bash
curl -sS "$OPENSEARCH_ENDPOINT/_cat/indices?v"
curl -sS "$OPENSEARCH_ENDPOINT/${INDEX_NAME}/_search?pretty" | jq .
```

Notes

- MinIO console is available on <http://localhost:9001> (default credentials in `.env` are `minioadmin`/`minioadmin`).
- If your local OpenSearch requires auth, set `OPENSEARCH_USER` and `OPENSEARCH_PASS` in `.env.local` before sourcing.
- If you prefer LocalStack or AWS SAM Local, you can substitute those for MinIO.

## Development (Poetry)

This project uses Poetry to manage dependencies and the virtual environment. Recommended commands for development:

- Install runtime and development dependencies (creates/uses the project's venv):

```bash
poetry install --with dev
```

- Run tests in the Poetry-managed environment:

```bash
poetry run pytest
```

- Add a new dev dependency:

```bash
poetry add --group dev <package>
```

- (Optional) Export a pip-style requirements file for CI that expects pip:

```bash
poetry export --dev --format requirements.txt --output requirements-dev.txt
```

License: MIT
