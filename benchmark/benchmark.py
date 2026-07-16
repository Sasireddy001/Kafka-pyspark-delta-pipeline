"""Batch benchmark for the transformation pipeline."""
import argparse
import json
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta

# Allow the benchmark to be executed before the package is installed.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from pipeline.transform import add_partitions, parse_events


def generate_jsonl(path: str, n: int):
    """Generate n sample JSON events to a JSONL file."""
    with open(path, "w") as f:
        for i in range(n):
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": f"user_{i % 10000}",
                "event_type": "click",
                "event_timestamp": (
                    datetime.utcnow() - timedelta(seconds=i % 300)
                ).isoformat()
                + "Z",
                "value": float(i % 1000),
                "status": "success" if i % 2 == 0 else "failure",
            }
            f.write(json.dumps(event, default=str) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Kafka-style JSON ingestion to Delta Lake."
    )
    parser.add_argument(
        "--rows", type=int, default=100_000, help="Number of events to generate"
    )
    parser.add_argument(
        "--output", default=None, help="Optional output Delta table path"
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        input_path = os.path.join(tmp, "events.jsonl")
        delta_path = args.output or os.path.join(tmp, "delta")
        generate_jsonl(input_path, args.rows)

        builder = (
            SparkSession.builder.appName("benchmark")
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.ui.showConsoleProgress", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        raw = spark.read.text(input_path).withColumnRenamed("value", "value")
        df = parse_events(raw)
        df = add_partitions(df)

        start = time.perf_counter()
        df.write.format("delta").mode("overwrite").save(delta_path)
        duration = time.perf_counter() - start

        spark.stop()

    throughput = args.rows / duration if duration else 0
    print(
        json.dumps(
            {
                "rows": args.rows,
                "delta_path": delta_path,
                "duration_seconds": round(duration, 2),
                "throughput_rows_per_sec": round(throughput, 2),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
