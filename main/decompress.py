import os
from pathlib import Path

import zstandard

from s3_utils import create_s3_client, wait_for_s3


def main() -> None:
    bucket_name = os.getenv("RAW_BUCKET", "clickstream-raw")
    prefix = os.getenv("RAW_PREFIX", "raw").strip("/")
    object_prefix = f"{prefix}/" if prefix else ""
    target_dir = Path(os.getenv("RAW_DIR", "/data/tmp"))
    target_dir.mkdir(parents=True, exist_ok=True)

    client = create_s3_client()
    wait_for_s3(client)

    paginator = client.get_paginator("list_objects_v2")
    keys = [
        item["Key"]
        for page in paginator.paginate(Bucket=bucket_name, Prefix=object_prefix)
        for item in page.get("Contents", [])
        if item["Key"].endswith(".csv.zst")
    ]
    if not keys:
        raise FileNotFoundError(
            f"Сжатые партии не найдены в s3://{bucket_name}/{object_prefix}"
        )

    decompressor = zstandard.ZstdDecompressor()
    for key in sorted(keys):
        compressed_path = target_dir / f"{Path(key).name}.part"
        csv_path = target_dir / Path(key).name.removesuffix(".zst")
        csv_temp_path = target_dir / f"{csv_path.name}.part"

        try:
            client.download_file(bucket_name, key, str(compressed_path))
            with compressed_path.open("rb") as compressed, csv_temp_path.open("wb") as output:
                decompressor.copy_stream(compressed, output)
            csv_temp_path.replace(csv_path)
        finally:
            compressed_path.unlink(missing_ok=True)
            csv_temp_path.unlink(missing_ok=True)

        print(f"Распакован s3://{bucket_name}/{key} -> {csv_path}")

    print(f"Распаковано партий: {len(keys)}")


if __name__ == "__main__":
    main()
