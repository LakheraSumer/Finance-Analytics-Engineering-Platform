"""Silver: typed, standardised, validated and de-duplicated entities.

* Incremental: only bronze rows ingested after the step's watermark are processed.
* Invalid rows go to `<entity>_quarantine` with a `reject_reason` instead of being dropped silently.
* Latest version per business key wins (ordered by `updated_at`, then ingestion time).
* Delta MERGE upsert keyed on the business key; older versions never overwrite newer ones, so re-runs are no-ops.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import LakehouseConfig
from .delta_utils import get_watermark, merge_upsert, set_watermark, table_exists

TXN_TYPES = ("debit", "credit")
CHANNELS = ("pos", "online", "upi", "atm", "netbanking")
STATUSES = ("posted", "reversed", "pending")
SEGMENTS = ("retail", "premium", "business")
ACCOUNT_TYPES = ("savings", "current", "credit_card")
TIMESTAMP_FORMATS = ("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm:ss")
KEYS = {"customers": "customer_id", "accounts": "account_id", "merchants": "merchant_id", "transactions": "transaction_id"}


def clean(col: str) -> Column:
    """Trimmed string, empty -> null."""
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def parse_ts(col: str) -> Column:
    return F.coalesce(*[F.try_to_timestamp(clean(col), F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def standardize_transactions(df: DataFrame, as_of: datetime) -> DataFrame:
    out = df.select(
        clean("transaction_id").alias("transaction_id"),
        F.upper(clean("account_id")).alias("account_id"),
        F.upper(clean("merchant_id")).alias("merchant_id"),
        parse_ts("txn_ts").alias("txn_ts"),
        F.expr("try_cast(regexp_replace(trim(amount), ',', '') AS DECIMAL(18,2))").alias("amount"),
        F.coalesce(F.upper(clean("currency")), F.lit("INR")).alias("currency"),
        F.lower(clean("txn_type")).alias("txn_type"),
        F.lower(clean("channel")).alias("channel"),
        F.coalesce(F.lower(clean("status")), F.lit("posted")).alias("status"),
        parse_ts("updated_at").alias("updated_at"),
        F.col("amount").alias("amount_raw"),
        F.col("txn_ts").alias("txn_ts_raw"),
        "_source_file",
        "_ingested_at",
    ).withColumn("updated_at", F.coalesce("updated_at", "txn_ts"))
    reason = (
        F.when(F.col("transaction_id").isNull(), "missing transaction_id")
        .when(F.col("account_id").isNull(), "missing account_id")
        .when(F.col("amount").isNull(), "invalid amount")
        .when(F.col("amount") <= 0, "non-positive amount")
        .when(F.col("txn_ts").isNull(), "invalid timestamp")
        .when(F.col("txn_ts") > F.lit(as_of), "future timestamp")
        .when(F.col("txn_type").isNull() | ~F.col("txn_type").isin(*TXN_TYPES), "invalid txn_type")
        .when(F.col("channel").isNull() | ~F.col("channel").isin(*CHANNELS), "invalid channel")
        .when(~F.col("status").isin(*STATUSES), "invalid status")
    )
    return out.withColumn("reject_reason", reason)


def standardize_customers(df: DataFrame, as_of: datetime) -> DataFrame:
    email = F.lower(clean("email"))
    out = df.select(
        F.upper(clean("customer_id")).alias("customer_id"),
        F.initcap(F.lower(clean("first_name"))).alias("first_name"),
        F.initcap(F.lower(clean("last_name"))).alias("last_name"),
        F.when(email.rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), email).alias("email"),
        F.expr("right(regexp_replace(phone, '[^0-9]', ''), 10)").alias("phone"),
        F.initcap(F.lower(clean("city"))).alias("city"),
        F.lower(clean("segment")).alias("segment"),
        parse_ts("updated_at").alias("updated_at"),
        "_source_file",
        "_ingested_at",
    )
    reason = (
        F.when(F.col("customer_id").isNull(), "missing customer_id")
        .when(F.col("updated_at").isNull(), "invalid updated_at")
        .when(F.col("segment").isNull() | ~F.col("segment").isin(*SEGMENTS), "invalid segment")
    )
    return out.withColumn("reject_reason", reason)


def standardize_accounts(df: DataFrame, as_of: datetime) -> DataFrame:
    out = df.select(
        F.upper(clean("account_id")).alias("account_id"),
        F.upper(clean("customer_id")).alias("customer_id"),
        F.lower(clean("account_type")).alias("account_type"),
        F.lower(clean("status")).alias("status"),
        F.expr("try_cast(opened_date AS DATE)").alias("opened_date"),
        F.expr("try_cast(credit_limit AS DECIMAL(18,2))").alias("credit_limit"),
        F.coalesce(F.upper(clean("currency")), F.lit("INR")).alias("currency"),
        parse_ts("updated_at").alias("updated_at"),
        "_source_file",
        "_ingested_at",
    )
    reason = (
        F.when(F.col("account_id").isNull(), "missing account_id")
        .when(F.col("customer_id").isNull(), "missing customer_id")
        .when(F.col("account_type").isNull() | ~F.col("account_type").isin(*ACCOUNT_TYPES), "invalid account_type")
        .when(F.col("updated_at").isNull(), "invalid updated_at")
    )
    return out.withColumn("reject_reason", reason)


def standardize_merchants(df: DataFrame, as_of: datetime) -> DataFrame:
    out = df.select(
        F.upper(clean("merchant_id")).alias("merchant_id"),
        clean("merchant_name").alias("merchant_name"),
        F.coalesce(F.lower(clean("category")), F.lit("other")).alias("category"),
        clean("mcc_code").alias("mcc_code"),
        F.initcap(F.lower(clean("city"))).alias("city"),
        parse_ts("updated_at").alias("updated_at"),
        "_source_file",
        "_ingested_at",
    )
    reason = F.when(F.col("merchant_id").isNull(), "missing merchant_id").when(
        F.col("updated_at").isNull(), "invalid updated_at"
    )
    return out.withColumn("reject_reason", reason)


STANDARDIZERS = {
    "customers": standardize_customers,
    "accounts": standardize_accounts,
    "merchants": standardize_merchants,
    "transactions": standardize_transactions,
}


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(
        F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def process_entity(spark: SparkSession, cfg: LakehouseConfig, entity: str, as_of: datetime) -> dict:
    step = f"silver_{entity}"
    bronze_table = cfg.table("bronze", entity)
    if not table_exists(spark, bronze_table):
        return {"entity": entity, "rows_in": 0}
    bronze = spark.table(bronze_table)
    watermark = get_watermark(spark, cfg, step)
    if watermark is not None:
        bronze = bronze.where(F.col("_ingested_at") > F.lit(watermark))
    new_watermark = bronze.agg(F.max("_ingested_at")).first()[0]
    if new_watermark is None:
        return {"entity": entity, "rows_in": 0}

    standardized = STANDARDIZERS[entity](bronze, as_of).cache()
    rows_in = standardized.count()
    quarantine = standardized.where(F.col("reject_reason").isNotNull()).withColumn("_quarantined_at", F.current_timestamp())
    valid = standardized.where(F.col("reject_reason").isNull()).drop("reject_reason", "amount_raw", "txn_ts_raw")
    valid_rows = valid.count()
    deduped = latest_per_key(valid, KEYS[entity]).withColumn("_silver_processed_at", F.current_timestamp())
    deduped_rows = deduped.count()

    merge_upsert(spark, cfg.table("silver", entity), deduped, KEYS[entity])
    quarantined = quarantine.count()
    if quarantined:
        quarantine.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            cfg.table("silver", f"{entity}_quarantine")
        )
    set_watermark(spark, cfg, step, new_watermark)
    standardized.unpersist()
    return {
        "entity": entity,
        "rows_in": rows_in,
        "quarantined": quarantined,
        "duplicates_removed": valid_rows - deduped_rows,
        "upserted": deduped_rows,
    }


def run_silver(spark: SparkSession, cfg: LakehouseConfig, as_of: datetime | None = None) -> list[dict]:
    as_of = as_of or datetime.utcnow() + timedelta(days=1)
    return [process_entity(spark, cfg, entity, as_of) for entity in ("customers", "accounts", "merchants", "transactions")]
