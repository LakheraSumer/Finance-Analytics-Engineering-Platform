"""Data-quality gate for the gold layer. Every result is stored in `finance_ops.dq_results`; any failed
"error" check raises DataQualityError so the job stops before bad numbers reach dashboards."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .config import LakehouseConfig
from .silver import CHANNELS, TXN_TYPES


class DataQualityError(Exception):
    pass


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    severity: str  # "error" blocks the pipeline, "warn" is recorded only
    passed: bool
    failed_rows: int
    details: str = ""


def run_quality_checks(spark: SparkSession, cfg: LakehouseConfig, run_id: str, as_of: datetime | None = None) -> list[CheckResult]:
    g = lambda name: spark.table(cfg.table("gold", name))  # noqa: E731
    fact, dim_customer, dim_account, dim_date = g("fact_transactions"), g("dim_customer"), g("dim_account"), g("dim_date")
    silver_txn = spark.table(cfg.table("silver", "transactions"))
    results: list[CheckResult] = []

    def check(name: str, table: str, failed: int, severity: str = "error", details: str = "") -> None:
        results.append(CheckResult(name, table, severity, failed == 0, int(failed), details))

    check("not_null_keys", "fact_transactions",
          fact.where(F.col("transaction_id").isNull() | F.col("date_key").isNull() | F.col("amount").isNull()).count())
    check("unique_transaction_id", "fact_transactions",
          fact.groupBy("transaction_id").count().where("count > 1").count())
    check("fk_account", "fact_transactions",
          fact.join(dim_account.select("account_sk"), "account_sk", "left_anti").count())
    check("fk_customer_point_in_time", "fact_transactions",
          fact.join(dim_customer.select("customer_sk"), "customer_sk", "left_anti").count())
    check("fk_date", "fact_transactions", fact.join(dim_date.select("date_key"), "date_key", "left_anti").count())
    check("accepted_values", "fact_transactions",
          fact.where(~F.col("txn_type").isin(*TXN_TYPES) | ~F.col("channel").isin(*CHANNELS)).count())
    check("unique_account_id", "dim_account", dim_account.groupBy("account_id").count().where("count > 1").count())
    check("scd2_single_current_version", "dim_customer",
          dim_customer.where("is_current").groupBy("customer_id").count().where("count > 1").count())
    ordered = Window.partitionBy("customer_id").orderBy("effective_from")
    check("scd2_no_overlapping_versions", "dim_customer",
          dim_customer.withColumn("prev_to", F.lag("effective_to").over(ordered))
          .where(F.col("prev_to").isNotNull() & (F.col("effective_from") < F.col("prev_to"))).count())

    s = silver_txn.agg(F.count("*").alias("n"), F.sum("amount").alias("amt")).first()
    f = fact.agg(F.count("*").alias("n"), F.sum("amount").alias("amt")).first()
    check("reconcile_row_count_silver_vs_gold", "fact_transactions", abs(s["n"] - f["n"]),
          details=f"silver={s['n']} gold={f['n']}")
    check("reconcile_amount_silver_vs_gold", "fact_transactions", int((s["amt"] or 0) != (f["amt"] or 0)),
          details=f"silver={s['amt']} gold={f['amt']}")

    unknown = fact.where((F.col("merchant_sk") == -1) & (F.col("txn_type") == "debit")).count()
    check("unknown_merchant_debits", "fact_transactions", unknown, "warn", "debits whose merchant is not in dim_merchant")

    quarantine_table = cfg.table("silver", "transactions_quarantine")
    quarantined = spark.table(quarantine_table).count() if spark.catalog.tableExists(quarantine_table) else 0
    rate = quarantined / max(quarantined + s["n"], 1)
    check("quarantine_rate_below_5pct", "transactions_quarantine", int(rate > 0.05), "warn", f"rate={rate:.2%}")

    as_of = as_of or datetime.utcnow()
    latest = fact.agg(F.max("txn_ts")).first()[0]
    stale = latest is None or latest < as_of - timedelta(days=2)
    check("freshness_2_days", "fact_transactions", int(stale), "warn", f"latest txn_ts={latest}, as_of={as_of:%Y-%m-%d}")

    rows = [(run_id, *asdict(r).values()) for r in results]
    (
        spark.createDataFrame(
            rows, "run_id string, check_name string, table_name string, severity string, passed boolean, failed_rows long, details string"
        )
        .withColumn("checked_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(cfg.table("ops", "dq_results"))
    )
    return results


def enforce(results: list[CheckResult]) -> None:
    failures = [r for r in results if r.severity == "error" and not r.passed]
    if failures:
        raise DataQualityError("; ".join(f"{r.check_name} on {r.table_name}: {r.failed_rows} failed ({r.details})" for r in failures))
