"""Gold: Kimball star schema for spend and cash-flow analytics.

* dim_customer - SCD Type 2 (full history of city/segment/contact changes; PII masked)
* dim_account, dim_merchant (with an "Unknown" member, key -1), dim_date
* fact_transactions - grain: one row per transaction (latest status), incremental MERGE, point-in-time customer key
* agg_monthly_category_spend, agg_customer_monthly_cashflow - BI-ready aggregates
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import LakehouseConfig
from .delta_utils import get_watermark, merge_upsert, set_watermark, table_exists

LOW_DATE = "1900-01-01 00:00:00"
HIGH_DATE = "9999-12-31 00:00:00"
SCD2_TRACKED = ["full_name", "email_masked", "phone_last4", "city", "segment"]


def _mask_email(col: str) -> F.Column:
    return F.when(
        F.col(col).isNotNull(),
        F.concat(F.substring(F.col(col), 1, 1), F.lit("***@"), F.substring_index(F.col(col), "@", -1)),
    )


def customer_source(silver_customers: DataFrame) -> DataFrame:
    src = silver_customers.select(
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        _mask_email("email").alias("email_masked"),
        F.when(F.col("phone").isNotNull(), F.substring(F.col("phone"), -4, 4)).alias("phone_last4"),
        "city",
        "segment",
        "updated_at",
    )
    hashed = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in SCD2_TRACKED]), 256)
    return src.withColumn("attr_hash", hashed)


def merge_dim_customer(spark: SparkSession, cfg: LakehouseConfig, source: DataFrame) -> None:
    """SCD Type 2 in a single MERGE: changed customers are staged twice - once to expire the current row (matched
    on the business key) and once with a NULL merge key so the new version is inserted."""
    target = cfg.table("gold", "dim_customer")
    if not table_exists(spark, target):
        (
            source.withColumn("customer_sk", F.xxhash64("customer_id", F.lit(LOW_DATE)))
            .withColumn("effective_from", F.lit(LOW_DATE).cast("timestamp"))
            .withColumn("effective_to", F.lit(HIGH_DATE).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta")
            .saveAsTable(target)
        )
        return

    current = spark.table(target).where("is_current").select(
        "customer_id", F.col("attr_hash").alias("current_hash"), F.col("effective_from").alias("current_from")
    )
    changed = (
        source.join(current, "customer_id")
        .where((F.col("attr_hash") != F.col("current_hash")) & (F.col("updated_at") > F.col("current_from")))
        .drop("current_hash", "current_from")
    )
    staged = changed.withColumn("merge_key", F.lit(None).cast("string")).unionByName(
        source.withColumn("merge_key", F.col("customer_id"))
    )
    new_version = "s.merge_key IS NULL"
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.attr_hash <> s.attr_hash AND s.updated_at > t.effective_from",
            set={"is_current": "false", "effective_to": "s.updated_at"},
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "s.customer_id",
                "full_name": "s.full_name",
                "email_masked": "s.email_masked",
                "phone_last4": "s.phone_last4",
                "city": "s.city",
                "segment": "s.segment",
                "updated_at": "s.updated_at",
                "attr_hash": "s.attr_hash",
                "customer_sk": f"xxhash64(s.customer_id, CASE WHEN {new_version} THEN CAST(s.updated_at AS STRING) "
                f"ELSE '{LOW_DATE}' END)",
                "effective_from": f"CASE WHEN {new_version} THEN s.updated_at ELSE CAST('{LOW_DATE}' AS TIMESTAMP) END",
                "effective_to": f"CAST('{HIGH_DATE}' AS TIMESTAMP)",
                "is_current": "true",
            }
        )
        .execute()
    )


def build_dim_account(silver_accounts: DataFrame) -> DataFrame:
    return silver_accounts.select(
        F.xxhash64("account_id").alias("account_sk"),
        "account_id", "customer_id", "account_type", "status", "opened_date", "credit_limit", "currency", "updated_at",
    )


def build_dim_merchant(spark: SparkSession, silver_merchants: DataFrame) -> DataFrame:
    unknown = spark.createDataFrame(
        [(-1, "UNKNOWN", "Unknown / not applicable", "unknown", None, None)],
        "merchant_sk long, merchant_id string, merchant_name string, category string, mcc_code string, city string",
    )
    known = silver_merchants.select(
        F.xxhash64("merchant_id").alias("merchant_sk"), "merchant_id", "merchant_name", "category", "mcc_code", "city"
    )
    return known.unionByName(unknown)


def build_dim_date(spark: SparkSession, silver_transactions: DataFrame) -> DataFrame:
    bounds = silver_transactions.agg(F.min("txn_ts").alias("lo"), F.max("txn_ts").alias("hi")).first()
    start = f"{bounds['lo'].year}-01-01"
    end = f"{bounds['hi'].year}-12-31"
    days = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_fact_rows(spark: SparkSession, cfg: LakehouseConfig, transactions: DataFrame) -> DataFrame:
    t = transactions.alias("t")
    a = spark.table(cfg.table("gold", "dim_account")).select("account_id", "account_sk", "customer_id").alias("a")
    c = spark.table(cfg.table("gold", "dim_customer")).select(
        "customer_id", "customer_sk", "effective_from", "effective_to"
    ).alias("c")
    m = spark.table(cfg.table("gold", "dim_merchant")).select("merchant_id", "merchant_sk").alias("m")
    joined = (
        t.join(a, F.col("t.account_id") == F.col("a.account_id"), "left")
        .join(
            c,
            (F.col("a.customer_id") == F.col("c.customer_id"))
            & (F.col("t.txn_ts") >= F.col("c.effective_from"))
            & (F.col("t.txn_ts") < F.col("c.effective_to")),
            "left",
        )
        .join(m, F.col("t.merchant_id") == F.col("m.merchant_id"), "left")
    )
    return joined.select(
        F.col("t.transaction_id").alias("transaction_id"),
        F.date_format("t.txn_ts", "yyyyMMdd").cast("int").alias("date_key"),
        F.coalesce("a.account_sk", F.lit(-1)).alias("account_sk"),
        F.coalesce("c.customer_sk", F.lit(-1)).alias("customer_sk"),
        F.coalesce("m.merchant_sk", F.lit(-1)).alias("merchant_sk"),
        F.col("t.txn_ts").alias("txn_ts"),
        F.col("t.amount").alias("amount"),
        F.when(F.col("t.txn_type") == "debit", -F.col("t.amount")).otherwise(F.col("t.amount")).alias("signed_amount"),
        F.col("t.currency").alias("currency"),
        F.col("t.txn_type").alias("txn_type"),
        F.col("t.channel").alias("channel"),
        F.col("t.status").alias("status"),
        (F.col("t.status") == "reversed").alias("is_reversed"),
        F.col("t.updated_at").alias("updated_at"),
    )


def build_aggregates(spark: SparkSession, cfg: LakehouseConfig) -> dict[str, DataFrame]:
    fact = spark.table(cfg.table("gold", "fact_transactions"))
    dates = spark.table(cfg.table("gold", "dim_date")).select("date_key", "year_month")
    merchants = spark.table(cfg.table("gold", "dim_merchant")).select("merchant_sk", "category")
    customers = spark.table(cfg.table("gold", "dim_customer")).select("customer_sk", "customer_id", "segment")
    enriched = fact.join(dates, "date_key").join(merchants, "merchant_sk", "left").join(customers, "customer_sk", "left")
    posted_debits = enriched.where((F.col("txn_type") == "debit") & (F.col("status") == "posted"))
    category_spend = posted_debits.groupBy("year_month", "category", "segment").agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_ticket"),
    )
    cashflow = enriched.where(F.col("status") == "posted").groupBy("customer_id", "year_month").agg(
        F.sum(F.when(F.col("txn_type") == "debit", F.col("amount")).otherwise(0)).alias("total_debits"),
        F.sum(F.when(F.col("txn_type") == "credit", F.col("amount")).otherwise(0)).alias("total_credits"),
        F.sum("signed_amount").alias("net_cashflow"),
        F.count("*").alias("txn_count"),
    )
    return {"agg_monthly_category_spend": category_spend, "agg_customer_monthly_cashflow": cashflow}


def _overwrite(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def run_gold(spark: SparkSession, cfg: LakehouseConfig) -> dict:
    silver = {e: spark.table(cfg.table("silver", e)) for e in ("customers", "accounts", "merchants", "transactions")}

    merge_dim_customer(spark, cfg, customer_source(silver["customers"]))
    merge_upsert(spark, cfg.table("gold", "dim_account"), build_dim_account(silver["accounts"]), "account_id")
    _overwrite(build_dim_merchant(spark, silver["merchants"]), cfg.table("gold", "dim_merchant"))
    _overwrite(build_dim_date(spark, silver["transactions"]), cfg.table("gold", "dim_date"))

    step = "gold_fact_transactions"
    watermark = get_watermark(spark, cfg, step)
    changed = silver["transactions"]
    if watermark is not None:
        changed = changed.where(F.col("_silver_processed_at") > F.lit(watermark))
    new_watermark = changed.agg(F.max("_silver_processed_at")).first()[0]
    fact_rows = 0
    if new_watermark is not None:
        rows = build_fact_rows(spark, cfg, changed).cache()
        fact_rows = rows.count()
        merge_upsert(spark, cfg.table("gold", "fact_transactions"), rows, "transaction_id")
        rows.unpersist()
        set_watermark(spark, cfg, step, new_watermark)

    for name, df in build_aggregates(spark, cfg).items():
        _overwrite(df, cfg.table("gold", name))
    return {"fact_rows_upserted": fact_rows}
