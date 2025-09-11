#!/usr/bin/env python3
"""
Temporary script to test the lambda function locally.
This simulates the code from README.md lines 136-153.
"""
import os
from src.lambda_function import lambda_handler

event = {
    "Records": [
        {
            "s3": {
                "bucket": {"name": os.environ.get('RAW_BUCKET')},
                "object": {"key": "covid_global.csv"}
            }
        }
    ]
}

if __name__ == "__main__":
    print(f"Running lambda with RAW_BUCKET: {os.environ.get('RAW_BUCKET')}")
    print(f"PROCESSED_BUCKET: {os.environ.get('PROCESSED_BUCKET')}")
    print(f"INDEX_NAME: {os.environ.get('INDEX_NAME')}")
    print(f"S3_ENDPOINT: {os.environ.get('S3_ENDPOINT')}")
    print(f"Event: {event}")
    try:
        lambda_handler(event, None)
        print("Lambda run finished successfully")
    except Exception as e:
        print(f"Lambda run failed with error: {e}")
        import traceback
        traceback.print_exc()
        
