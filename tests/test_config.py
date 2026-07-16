"""Tests for PipelineConfig."""
from pipeline.config import PipelineConfig


def test_default_config():
    config = PipelineConfig.from_env()
    assert config.kafka_topic == "events"
    assert config.is_databricks is False


def test_env_override(monkeypatch):
    monkeypatch.setenv("KAFKA_TOPIC", "orders")
    monkeypatch.setenv("DELTA_PATH", "dbfs:/mnt/delta/orders")
    config = PipelineConfig.from_env()
    assert config.kafka_topic == "orders"
    assert config.delta_path == "dbfs:/mnt/delta/orders"


def test_databricks_detection(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    config = PipelineConfig.from_env()
    assert config.is_databricks is True
