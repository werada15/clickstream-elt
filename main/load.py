import os
from pathlib import Path

from s3_utils import create_s3_client, ensure_bucket, wait_for_s3


def main() -> None:
    batch_dir = Path(os.getenv("BATCH_DIR", "/data/batches"))
    files = sorted(batch_dir.glob("*.csv.zst"))
    if not files:
        raise FileNotFoundError(f"Сжатые партии не найдены в {batch_dir}")

    client = create_s3_client()
    wait_for_s3(client)

    bucket_name = os.getenv("RAW_BUCKET", "clickstream-raw")
    prefix = os.getenv("RAW_PREFIX", "raw").strip("/")
    ensure_bucket(client, bucket_name)

    for local_path in files:
        key = f"{prefix}/{local_path.name}" if prefix else local_path.name
        print(f"Загрузка {local_path.name} -> s3://{bucket_name}/{key}")
        client.upload_file(str(local_path), bucket_name, key)

    print(f"Загружено сжатых партий: {len(files)}")


if __name__ == "__main__":
    main()
