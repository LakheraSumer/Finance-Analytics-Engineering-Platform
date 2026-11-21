"""Bronze: land raw files as-is (every field a string) with ingestion metadata. No business logic here.

On Databricks, Auto Loader (cloudFiles, availableNow trigger + checkpoint) picks up only new files. Locally / in CI
the same contract is kept with a batch read that skips files already recorded in `_source_file`, so re-runs never
duplicate data.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import LakehouseConfig, is_databricks
from .delta_utils import table_exists

RAW_COLUMNS = {
    "customers": ["customer_id", "first_name", "last_name", "email", "phone", "city", "segment", "updated_at"],
    "accounts": ["account_id", "customer_id", "account_type", "status", "opened_date", "credit_limit", "currency", "updated_at"],
    "merchants": ["merchant_id", "merchant_name", "category", "mcc_code", "city", "updated_at"],
    "transactions": ["transaction_id", "account_id", "merchant_id", "txn_ts", "amount", "currency", "txn_type",
                     "channel", "status", "updated_at"],
}
ENTITIES = tuple(RAW_COLUMNS)


def raw_schema(entity: str) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS[entity]])


def with_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_run_id", F.lit(run_id))
    )


def ingest_batch(spark: SparkSession, cfg: LakehouseConfig, entity: str, run_id: str) -> int:
    source = f"{cfg.landing_path}/{entity}"
    target = cfg.table("bronze", entity)
    if not os.path.isdir(source):
        return 0
    df = with_ingestion_metadata(spark.read.schema(raw_schema(entity)).json(source), run_id)
    if table_exists(spark, target):
        already_loaded = spark.table(target).select("_source_file").distinct()
        df = df.join(already_loaded, "_source_file", "left_anti")
    df = df.cache()
    rows = df.count()
    if rows:
        df.write.format("delta").mode("append").saveAsTable(target)
    df.unpersist()
    return rows


def ingest_autoloader(spark: SparkSession, cfg: LakehouseConfig, entity: str, run_id: str) -> None:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(raw_schema(entity))
        .load(f"{cfg.landing_path}/{entity}")
    )
    query = (
        with_ingestion_metadata(stream, run_id)
        .writeStream.option("checkpointLocation", f"{cfg.checkpoint_path}/bronze_{entity}")
        .trigger(availableNow=True)
        .toTable(cfg.table("bronze", entity))
    )
    query.awaitTermination()


def run_bronze(spark: SparkSession, cfg: LakehouseConfig, run_id: str, use_autoloader: bool | None = None) -> dict:
    use_autoloader = is_databricks() if use_autoloader is None else use_autoloader
    loaded = {}
    for entity in ENTITIES:
        if use_autoloader:
            before = spark.table(cfg.table("bronze", entity)).count() if table_exists(spark, cfg.table("bronze", entity)) else 0
            ingest_autoloader(spark, cfg, entity, run_id)
            loaded[entity] = spark.table(cfg.table("bronze", entity)).count() - before
        else:
            loaded[entity] = ingest_batch(spark, cfg, entity, run_id)
    return loaded
