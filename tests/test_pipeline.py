"""End-to-end medallion run on local Spark + Delta Lake: initial load, idempotent re-run, incremental batch."""
import uuid
from datetime import datetime

import os

import pytest

if os.getenv("CI"):  # on CI the Spark tests must run, never silently skip
    import delta  # noqa: F401
    import pyspark  # noqa: F401
else:
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from conftest import SMALL  # noqa: E402
from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.data_generator import generate_landing_data
from finance_lakehouse.pipeline import run_pipeline

AS_OF = datetime(2025, 8, 1)


def _snapshot(spark, cfg):
    tables = [cfg.table("silver", t) for t in ("customers", "accounts", "merchants", "transactions", "transactions_quarantine")]
    tables += [cfg.table("gold", t) for t in ("dim_customer", "dim_account", "dim_merchant", "fact_transactions")]
    snap = {t: spark.table(t).count() for t in tables}
    snap["fact_amount"] = spark.table(cfg.table("gold", "fact_transactions")).agg(F.sum("amount")).first()[0]
    return snap


def _silver(summary, entity):
    return next(r for r in summary["silver"] if r["entity"] == entity)


def test_medallion_pipeline(spark, tmp_path):
    cfg = LakehouseConfig.for_local(str(tmp_path), prefix=f"fin_{uuid.uuid4().hex[:6]}")

    # ---- batch 1: initial load
    s1 = generate_landing_data(cfg.landing_path, 1, scale=SMALL)
    r1 = run_pipeline(spark, cfg, "batch-1", as_of=AS_OF)
    fact = spark.table(cfg.table("gold", "fact_transactions"))
    assert fact.count() == len(s1.valid_transaction_ids)
    assert _silver(r1, "transactions")["quarantined"] == s1.invalid_transactions
    assert _silver(r1, "transactions")["duplicates_removed"] == s1.duplicate_transactions
    assert _silver(r1, "customers")["quarantined"] == s1.invalid_customers
    assert fact.where("merchant_sk = -1 AND txn_type = 'debit'").count() == s1.unknown_merchant_transactions
    dim_customer = spark.table(cfg.table("gold", "dim_customer"))
    assert dim_customer.count() == dim_customer.where("is_current").count() == SMALL.customers

    # ---- re-running with no new files changes nothing
    before = _snapshot(spark, cfg)
    r1_again = run_pipeline(spark, cfg, "batch-1-rerun", as_of=AS_OF)
    assert set(r1_again["bronze"].values()) == {0}
    assert _snapshot(spark, cfg) == before

    # ---- batch 2: incremental load
    s2 = generate_landing_data(cfg.landing_path, 2, scale=SMALL)
    run_pipeline(spark, cfg, "batch-2", as_of=AS_OF)
    fact = spark.table(cfg.table("gold", "fact_transactions"))
    assert fact.count() == len(s1.valid_transaction_ids | s2.valid_transaction_ids)
    reversed_ids = {r.transaction_id for r in fact.where("is_reversed").select("transaction_id").collect()}
    assert reversed_ids == s2.reversed_transactions

    # SCD Type 2: one extra version per changed customer, exactly one current version each
    dim_customer = spark.table(cfg.table("gold", "dim_customer"))
    total_customers = SMALL.customers + SMALL.new_customers
    assert dim_customer.where("is_current").count() == total_customers
    assert dim_customer.count() == total_customers + len(s2.updated_customers)

    # point-in-time: transactions before the change keep the old customer version, later ones get the new one
    customer_id, changed_at = sorted(s2.updated_customers.items())[0]
    changed_at = datetime.fromisoformat(changed_at)
    old, new = dim_customer.where(F.col("customer_id") == customer_id).orderBy("effective_from").collect()
    assert old.effective_to == new.effective_from == changed_at and not old.is_current and new.is_current
    accounts = spark.table(cfg.table("gold", "dim_account")).where(F.col("customer_id") == customer_id).select("account_sk")
    rows = fact.join(accounts, "account_sk").select("txn_ts", "customer_sk").collect()
    assert rows
    for row in rows:
        assert row.customer_sk == (old.customer_sk if row.txn_ts < changed_at else new.customer_sk)

    # quality gate: every blocking check passed in the batch-2 run
    dq = spark.table(cfg.table("ops", "dq_results")).where("run_id = 'batch-2'")
    assert dq.count() > 0
    assert dq.where("severity = 'error' AND NOT passed").count() == 0
