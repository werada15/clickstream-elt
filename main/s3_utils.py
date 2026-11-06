import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError, EndpointConnectionError


def create_s3_client():
    access_key = os.getenv("KEY")
    secret_key = os.getenv("SECRET")
    if not access_key or not secret_key:
        raise RuntimeError("Задайте KEY и SECRET в файле .env")

    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:3900"),
        region_name=os.getenv("S3_REGION", "garage"),
        config=Config(s3={"addressing_style": "path"}),
    )


def wait_for_s3(client, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client.list_buckets()
            return
        except (EndpointConnectionError, ConnectionClosedError):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Garage S3 API не стал доступен за {timeout_seconds} секунд"
                )
            time.sleep(2)


def ensure_bucket(client, bucket_name: str) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)
        return
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    try:
        client.create_bucket(Bucket=bucket_name)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
            raise
