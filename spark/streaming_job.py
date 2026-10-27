"""
Spark Structured Streaming microservice.

Consumes the raw event stream from Kafka, drops cancelled invoices, casts
types, then aggregates revenue and item counts per country in 5-minute
tumbling windows with a 2-minute watermark for late events. Aggregates are
upserted into PostgreSQL, the serving table for the (out-of-scope) report.
"""
import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "retail-transactions")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "retail")
POSTGRES_APP_USER = os.environ.get("POSTGRES_APP_USER", "retail_app")
POSTGRES_APP_PASSWORD = os.environ.get("POSTGRES_APP_PASSWORD", "")

WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "5 minutes")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/opt/spark-app/checkpoints/agg")

EVENT_SCHEMA = StructType([
    StructField("invoice_no", StringType()),
    StructField("stock_code", StringType()),
    StructField("description", StringType()),
    StructField("quantity", IntegerType()),
    StructField("invoice_date", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", StringType()),
    StructField("country", StringType()),
])

UPSERT_SQL = """
    INSERT INTO country_revenue_5min (window_start, window_end, country, revenue, items)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (window_start, window_end, country)
    DO UPDATE SET revenue = EXCLUDED.revenue, items = EXCLUDED.items, updated_at = now()
"""


def upsert_batch(batch_df, batch_id: int) -> None:
    """foreachBatch sink: upsert this micro-batch's aggregates into Postgres.

    The aggregated rows per batch are small (windows x countries), so a
    plain collect() + executemany() is simple and fast enough here; a
    higher-throughput deployment would write via the Spark JDBC connector
    or a connection pool instead.
    """
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_APP_USER,
        password=POSTGRES_APP_PASSWORD,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    UPSERT_SQL,
                    [
                        (r["window_start"], r["window_end"], r["country"], r["revenue"], r["items"])
                        for r in rows
                    ],
                )
        print(f"[batch {batch_id}] upserted {len(rows)} rows into country_revenue_5min")
    finally:
        conn.close()


def main() -> None:
    spark = SparkSession.builder.appName("retail-realtime-aggregation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    events = (
        raw.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        # cast types, per the conception-phase design
        .withColumn("event_time", to_timestamp(col("invoice_date")))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
    )

    cleaned = (
        events
        # drop cancelled invoices (their invoice number starts with "C")
        .filter(~col("invoice_no").startswith("C"))
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
        .filter(col("event_time").isNotNull())
        .filter(col("country").isNotNull())
    )

    aggregated = (
        cleaned
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(window(col("event_time"), WINDOW_DURATION), col("country"))
        .agg(
            spark_sum(col("quantity") * col("unit_price")).alias("revenue"),
            spark_sum(col("quantity")).alias("items"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("revenue"),
            col("items"),
        )
    )

    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
