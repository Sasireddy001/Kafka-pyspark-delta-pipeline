"""Pipeline configuration."""
import os
from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Runtime configuration populated from environment variables."""

    app_name: str = "kafka-pyspark-delta-pipeline"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "events"
    kafka_starting_offsets: str = "latest"
    delta_path: str = "/tmp/delta/events"
    checkpoint_path: str = "/tmp/delta/checkpoints/events"
    trigger_interval: str = "10 seconds"
    max_offsets_per_trigger: int = 10000
    watermark_seconds: int = 120
    window_seconds: int = 60
    is_databricks: bool = False

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Build a configuration instance from environment variables."""
        return cls(
            app_name=os.getenv("PIPELINE_APP_NAME", cls.app_name),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            kafka_starting_offsets=os.getenv(
                "KAFKA_STARTING_OFFSETS", cls.kafka_starting_offsets
            ),
            delta_path=os.getenv("DELTA_PATH", cls.delta_path),
            checkpoint_path=os.getenv("CHECKPOINT_PATH", cls.checkpoint_path),
            trigger_interval=os.getenv("TRIGGER_INTERVAL", cls.trigger_interval),
            max_offsets_per_trigger=int(
                os.getenv("MAX_OFFSETS_PER_TRIGGER", cls.max_offsets_per_trigger)
            ),
            watermark_seconds=int(
                os.getenv("WATERMARK_SECONDS", cls.watermark_seconds)
            ),
            window_seconds=int(os.getenv("WINDOW_SECONDS", cls.window_seconds)),
            is_databricks=os.getenv("DATABRICKS_RUNTIME_VERSION") is not None,
        )
