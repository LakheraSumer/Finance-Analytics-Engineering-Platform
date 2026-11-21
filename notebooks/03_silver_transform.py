# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: cleanse, validate, de-duplicate, MERGE
# MAGIC * Processes only bronze rows newer than the step watermark (`finance_ops.pipeline_watermarks`)
# MAGIC * Casts and standardises fields; invalid rows go to `<entity>_quarantine` with a `reject_reason`
# MAGIC * Keeps the latest version per business key and upserts with Delta `MERGE` (re-runs change nothing)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.silver import run_silver

cfg = LakehouseConfig.for_databricks(dbutils.widgets.get("catalog"))
for result in run_silver(spark, cfg):
    print(result)

# COMMAND ----------

display(spark.sql(f"""
    SELECT reject_reason, count(*) AS rows
    FROM {cfg.table('silver', 'transactions_quarantine')}
    GROUP BY reject_reason ORDER BY rows DESC
"""))
