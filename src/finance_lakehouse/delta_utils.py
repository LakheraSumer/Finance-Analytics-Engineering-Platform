"""Delta Lake helpers: table existence, idempotent MERGE upserts and per-step watermarks for incremental loads."""
from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import LakehouseConfig


def table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def merge_upsert(spark: SparkSession, target: str, source: DataFrame, key: str, version_col: str = "updated_at") -> None:
    """Insert new keys; update existing keys only when the source version is newer. Re-running is a no-op."""
    if not table_exists(spark, target):
        source.write.format("delta").saveAsTable(target)
        return
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(source.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition=f"s.{version_col} > t.{version_col}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _watermark_table(cfg: LakehouseConfig) -> str:
    return cfg.table("ops", "pipeline_watermarks")


def get_watermark(spark: SparkSession, cfg: LakehouseConfig, step: str) -> datetime | None:
    table = _watermark_table(cfg)
    if not table_exists(spark, table):
        return None
    row = spark.table(table).where(F.col("step") == step).select("watermark").first()
    return row[0] if row else None


def set_watermark(spark: SparkSession, cfg: LakehouseConfig, step: str, value: datetime) -> None:
    table = _watermark_table(cfg)
    update = spark.createDataFrame([(step, value)], "step string, watermark timestamp").withColumn(
        "updated_at", F.current_timestamp()
    )
    if not table_exists(spark, table):
        update.write.format("delta").saveAsTable(table)
        return
    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(update.alias("s"), "t.step = s.step")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
