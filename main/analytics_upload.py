import os
from pathlib import Path

from s3_utils import create_s3_client, ensure_bucket, wait_for_s3


def main() -> None:
    analytics_dir = Path(os.getenv("ANALYTICS_DIR", "/data/analytics/sessions"))
    parquet_files = sorted(analytics_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Parquet-файлы не найдены в {analytics_dir}")

    client = create_s3_client()
    wait_for_s3(client)

    bucket_name = os.getenv("ANALYTICS_BUCKET", "clickstream-analytics")
    prefix = os.getenv("ANALYTICS_PREFIX", "curated/sessions").strip("/")
    if not prefix:
        raise ValueError("ANALYTICS_PREFIX не должен быть пустым")
    ensure_bucket(client, bucket_name)

    object_prefix = f"{prefix}/"
    old_keys = [
        {"Key": item["Key"]}
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket_name, Prefix=object_prefix
        )
        for item in page.get("Contents", [])
    ]
    for start in range(0, len(old_keys), 1000):
        response = client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": old_keys[start:start + 1000], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(
                f"Не удалось удалить старые Parquet-объекты: {errors[:5]}"
            )
    if old_keys:
        print(f"Удалены старые объекты из {object_prefix}: {len(old_keys)}")

    for local_path in parquet_files:
        relative_path = local_path.relative_to(analytics_dir).as_posix()
        key = f"{prefix}/{relative_path}"
        print(f"Загрузка {local_path} -> s3://{bucket_name}/{key}")
        client.upload_file(str(local_path), bucket_name, key)

    print(f"Загружено Parquet-файлов: {len(parquet_files)}")


if __name__ == "__main__":
    main()
