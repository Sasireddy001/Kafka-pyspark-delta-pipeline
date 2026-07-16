"""Streaming transformations."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    from_json,
    hour,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("value", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


def parse_events(df: DataFrame, schema: StructType = EVENT_SCHEMA) -> DataFrame:
    """Parse Kafka JSON payloads into a typed DataFrame."""
    return (
        df.selectExpr("CAST(value AS STRING) as json")
        .withColumn("parsed", from_json(col("json"), schema))
        .select("parsed.*")
        .withColumn("ingested_at", current_timestamp())
    )


def add_partitions(df: DataFrame, timestamp_col: str = "event_timestamp") -> DataFrame:
    """Add date/hour partition columns for Delta Lake partitioning."""
    return (
        df.withColumn("event_date", col(timestamp_col).cast("date"))
        .withColumn("event_hour", hour(col(timestamp_col)))
    )


def deduplicate_events(df: DataFrame, watermark_seconds: int = 120) -> DataFrame:
    """Deduplicate events by event_id using a watermark on event_timestamp."""
    if df.isStreaming:
        return (
            df.withWatermark("event_timestamp", f"{watermark_seconds} seconds")
            .dropDuplicates(["event_id"])
        )
    return df.dropDuplicates(["event_id"])


def aggregate_events(
    df: DataFrame,
    window_seconds: int = 60,
    watermark_seconds: int = 120,
) -> DataFrame:
    """Aggregate event counts by time window and status."""
    grouped = df.groupBy(
        window(col("event_timestamp"), f"{window_seconds} seconds"),
        col("status"),
    ).agg(count("*").alias("event_count"))

    if df.isStreaming:
        return (
            df.withWatermark("event_timestamp", f"{watermark_seconds} seconds")
            .groupBy(
                window(col("event_timestamp"), f"{window_seconds} seconds"),
                col("status"),
            )
            .agg(count("*").alias("event_count"))
        )
    return grouped


def write_stream_to_delta(
    df: DataFrame,
    path: str,
    checkpoint: str,
    trigger: str = "10 seconds",
    output_mode: str = "append",
):
    """Return a streaming query writing a DataFrame to Delta Lake."""
    return (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .start(path)
    )
