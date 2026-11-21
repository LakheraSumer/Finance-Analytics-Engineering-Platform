"""End-to-end orchestration (used by tests and local runs; on Databricks each layer is its own job task)."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from .bronze import run_bronze
from .config import LAYERS, LakehouseConfig, is_databricks
from .gold import run_gold
from .quality import enforce, run_quality_checks
from .silver import run_silver


def setup(spark: SparkSession, cfg: LakehouseConfig) -> None:
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    if is_databricks() and cfg.catalog:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('ops')}.landing")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('ops')}.checkpoints")


def run_pipeline(
    spark: SparkSession, cfg: LakehouseConfig, run_id: str, as_of: datetime | None = None, use_autoloader: bool | None = None
) -> dict:
    setup(spark, cfg)
    summary = {"run_id": run_id, "bronze": run_bronze(spark, cfg, run_id, use_autoloader)}
    summary["silver"] = run_silver(spark, cfg, as_of)
    summary["gold"] = run_gold(spark, cfg)
    results = run_quality_checks(spark, cfg, run_id, as_of)
    summary["quality"] = [(r.check_name, r.severity, r.passed, r.failed_rows) for r in results]
    enforce(results)
    return summary
