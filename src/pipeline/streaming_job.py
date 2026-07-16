"""Main streaming entrypoint: Kafka -> PySpark -> Delta Lake."""
from pipeline.config import PipelineConfig
from pipeline.spark_session import get_spark_session
from pipeline.transform import (
    add_partitions,
    deduplicate_events,
    parse_events,
    write_stream_to_delta,
)
from pipeline.utils import get_logger

logger = get_logger(__name__)


def run():
    """Run the streaming pipeline."""
    config = PipelineConfig.from_env()
    spark = get_spark_session(config.app_name, is_databricks=config.is_databricks)
    logger.info("Starting streaming job %s", config.app_name)

    try:
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", config.kafka_starting_offsets)
            .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = parse_events(raw_stream)
        parsed = add_partitions(parsed)
        deduped = deduplicate_events(
            parsed, watermark_seconds=config.watermark_seconds
        )

        query = write_stream_to_delta(
            deduped,
            path=config.delta_path,
            checkpoint=config.checkpoint_path,
            trigger=config.trigger_interval,
        )

        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
