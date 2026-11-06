import sys

from botocore.exceptions import ClientError

from s3_utils import create_s3_client


def main():
    if len(sys.argv) != 2:
        print("Использование: python clear_bucket.py <bucket_name>")
        sys.exit(1)

    bucket = sys.argv[1]
    s3 = create_s3_client()

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        print(f"Бакет '{bucket}' недоступен или не существует: {e}")
        sys.exit(1)

    # Постранично собираем все объекты (list_objects_v2 отдаёт до 1000 за раз)
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket)

    total_deleted = 0
    for page in pages:
        if "Contents" not in page:
            continue
        objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
        total_deleted += len(objects)
        print(f"  удалено {len(objects)} объектов...")

    if total_deleted == 0:
        print(f"Бакет '{bucket}' уже пуст.")
    else:
        print(f"Готово: из '{bucket}' удалено {total_deleted} объектов.")


if __name__ == "__main__":
    main()