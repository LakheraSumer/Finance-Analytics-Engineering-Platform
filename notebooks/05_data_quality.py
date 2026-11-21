# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data-quality gate
# MAGIC Keys, uniqueness, referential integrity, SCD2 integrity, silver-to-gold reconciliation (rows and amounts),
# MAGIC quarantine rate and freshness. Results are appended to `finance_ops.dq_results`; a failed `error` check fails
# MAGIC the job.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_id", "manual")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.quality import enforce, run_quality_checks

cfg = LakehouseConfig.for_databricks(dbutils.widgets.get("catalog"))
results = run_quality_checks(spark, cfg, run_id=dbutils.widgets.get("run_id"))
display(spark.createDataFrame([r.__dict__ for r in results]))
enforce(results)
