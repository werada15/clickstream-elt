import os
from pathlib import Path

import duckdb


def main() -> None:
    source_path = Path(os.getenv("SOURCE_FILE", "/data/source/clickstream_full.csv"))
    batch_dir = Path(os.getenv("BATCH_DIR", "/data/batches"))
    batch_dir.mkdir(parents=True, exist_ok=True)

    existing_batches = sorted(batch_dir.glob("*.csv.zst"))
    if existing_batches:
        print(f"Используются существующие партии: {len(existing_batches)}")
        return

    if not source_path.is_file():
        raise FileNotFoundError(f"Файл источника не найден: {source_path}")

    source_sql = str(source_path).replace("'", "''")
    batch_dir_sql = str(batch_dir).replace("'", "''")
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            COPY (
                SELECT * FROM read_csv_auto('{source_sql}')
            )
            TO '{batch_dir_sql}'
            (
                FILE_SIZE_BYTES 104857600,
                DELIMITER ',',
                HEADER,
                NULL 'NULL',
                COMPRESSION 'zstd'
            )
            """
        )
    finally:
        connection.close()

    batches = sorted(batch_dir.glob("*.csv.zst"))
    if not batches:
        raise RuntimeError("DuckDB не создал ни одной сжатой партии")
    print(f"Созданы сжатые партии: {len(batches)}")


if __name__ == "__main__":
    main()
