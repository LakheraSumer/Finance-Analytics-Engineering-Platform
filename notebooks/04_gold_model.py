# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: star schema
# MAGIC * `dim_customer` - SCD Type 2 via a single Delta MERGE (history of city/segment changes, PII masked)
# MAGIC * `dim_account`, `dim_merchant` (unknown member = -1), `dim_date`
# MAGIC * `fact_transactions` - incremental MERGE, customer key resolved point-in-time on `txn_ts`
# MAGIC * `agg_monthly_category_spend`, `agg_customer_monthly_cashflow`

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.gold import run_gold

cfg = LakehouseConfig.for_databricks(dbutils.widgets.get("catalog"))
print(run_gold(spark, cfg))

# COMMAND ----------

display(spark.table(cfg.table("gold", "agg_monthly_category_spend")).orderBy("year_month", "total_spend", ascending=[True, False]))
