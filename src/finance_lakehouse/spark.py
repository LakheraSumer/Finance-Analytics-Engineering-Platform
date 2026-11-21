"""SparkSession: the Databricks session when running there, otherwise a local Delta Lake-enabled session."""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import is_databricks


def get_spark(app_name: str = "finance-lakehouse", warehouse_dir: str | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.enabled", "false")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return configure_spark_with_delta_pip(builder).getOrCreate()
