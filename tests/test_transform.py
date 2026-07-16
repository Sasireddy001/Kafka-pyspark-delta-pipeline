"""Tests for streaming transformations."""
from datetime import datetime

from pipeline.transform import (
    add_partitions,
    aggregate_events,
    deduplicate_events,
    parse_events,
)


def test_parse_events(spark):
    raw = [(
        b'{"event_id":"evt-1","user_id":"u1","event_type":"click",'
        b'"event_timestamp":"2024-01-01T00:00:00Z","value":10.5,"status":"success"}',
    )]
    df = spark.createDataFrame(raw, ["value"])
    result = parse_events(df).collect()
    assert len(result) == 1
    row = result[0]
    assert row["event_id"] == "evt-1"
    assert row["event_type"] == "click"
    assert row["value"] == 10.5
    assert row["event_timestamp"] == datetime(2024, 1, 1, 0, 0, 0)


def test_parse_events_with_missing_optional(spark):
    raw = [(
        b'{"event_id":"evt-2","event_type":"view","event_timestamp":"2024-01-01T01:00:00Z"}',
    )]
    df = spark.createDataFrame(raw, ["value"])
    result = parse_events(df).collect()
    row = result[0]
    assert row["event_id"] == "evt-2"
    assert row["user_id"] is None
    assert row["value"] is None


def test_add_partitions(spark):
    data = [("evt-1", datetime(2024, 1, 1, 14, 30, 0))]
    df = spark.createDataFrame(data, ["event_id", "event_timestamp"])
    result = add_partitions(df).collect()[0]
    assert result["event_date"].year == 2024
    assert result["event_hour"] == 14


def test_deduplicate_events(spark):
    data = [
        ("1", "u1", "click", datetime(2024, 1, 1, 0, 0, 0), 1.0, "success"),
        ("1", "u2", "click", datetime(2024, 1, 1, 0, 0, 30), 2.0, "success"),
    ]
    df = spark.createDataFrame(
        data,
        ["event_id", "user_id", "event_type", "event_timestamp", "value", "status"],
    )
    result = deduplicate_events(df, watermark_seconds=1).collect()
    assert len(result) == 1
    assert result[0]["user_id"] == "u1"


def test_aggregate_events(spark):
    data = [
        ("1", "u1", "click", datetime(2024, 1, 1, 0, 0, 0), 1.0, "success"),
        ("2", "u2", "click", datetime(2024, 1, 1, 0, 0, 30), 2.0, "success"),
        ("3", "u3", "view", datetime(2024, 1, 1, 0, 0, 45), 3.0, "failure"),
    ]
    df = spark.createDataFrame(
        data,
        ["event_id", "user_id", "event_type", "event_timestamp", "value", "status"],
    )
    result = aggregate_events(df, window_seconds=60, watermark_seconds=120).collect()
    statuses = {row["status"]: row["event_count"] for row in result}
    assert statuses["success"] == 2
    assert statuses["failure"] == 1
