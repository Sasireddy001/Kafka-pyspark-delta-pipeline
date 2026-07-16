"""SparkSession factory with Delta Lake support."""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(
    app_name: str,
    is_databricks: bool = False,
    extra_conf: dict = None,
) -> SparkSession:
    """Return a SparkSession configured for Delta Lake and Kafka."""
    builder = SparkSession.builder.appName(app_name)

    if not is_databricks:
        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    if not is_databricks:
        builder = configure_spark_with_delta_pip(builder)

    return builder.getOrCreate()
