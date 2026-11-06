from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    dayofweek,
    when
)

spark = (
    SparkSession.builder
    .appName("ClickstreamTransform")
    .config("spark.sql.shuffle.partitions", "128")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_path = "/opt/raw/"
analytics_path = "/opt/analytics/sessions"

# загрузка raw-данных
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "false")
    .csv(raw_path)
)

# удаление дубликатов

df = df.dropDuplicates(["session_id"])

# приведенеие типов данных

df = (
    df
    .withColumn("session_start", to_timestamp(col("session_start")))
    .withColumn("session_end", to_timestamp(col("session_end")))

    .withColumn(
        "session_duration_seconds",
        col("session_duration_seconds").cast("long")
    )

    .withColumn(
        "total_events",
        col("total_events").cast("long")
    )

    .withColumn(
        "session_depth",
        col("session_depth").cast("long")
    )

    .withColumn(
        "session_revenue_usd",
        col("session_revenue_usd").cast("double")
    )

    .withColumn(
        "avg_price_viewed_usd",
        col("avg_price_viewed_usd").cast("double")
    )
)

# удаление некорректных записей 

df = df.filter(
    col("session_id").isNotNull()
    & col("session_start").isNotNull()
    & (col("session_duration_seconds") >= 0)
    & (col("total_events") >= 0)
    & (col("session_depth") >= 0)
    & (
        col("session_revenue_usd").isNull()
        | (col("session_revenue_usd") >= 0)
    )
    & (
        col("avg_price_viewed_usd").isNull()
        | (col("avg_price_viewed_usd") >= 0)
    )
)

# создание искусственных аналитических полей  

df = (
    df
    .withColumn(
        "session_date",
        to_date(col("session_start"))
    )
    .withColumn(
        "session_hour",
        hour(col("session_start"))
    )
    .withColumn(
        "session_weekday",
        dayofweek(col("session_start"))
    )
    .withColumn(
        "session_duration_minutes",
        col("session_duration_seconds") / 60
    )
    .withColumn(
        "engaged_session",
        when(
            (col("session_depth") >= 5)
            | (col("session_duration_seconds") >= 300),
            1
        ).otherwise(0)
    )
)

# упаковка в parquet

(
    df
    .repartition(64, "session_date")
    .write
    .mode("overwrite")
    .partitionBy("session_date")
    .parquet(analytics_path)
)

print("Пример обработанных данных:")
df.select(
    "session_id",
    "session_start",
    "platform",
    "device_type",
    "had_search",
    "session_depth",
    "session_duration_seconds",
    "session_hour",
    "engaged_session"
).show(10, truncate=False)

print("TRANSFORM завершён.")

spark.stop()