# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: raw ingestion with Auto Loader
# MAGIC Auto Loader (`cloudFiles`, `availableNow` trigger) loads only files it has not seen before (tracked in the
# MAGIC checkpoint) into `finance_bronze.<entity>` Delta tables. Every column is kept as a raw string, plus
# MAGIC `_source_file`, `_ingested_at` and `_ingest_run_id` for lineage.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_id", "manual")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from finance_lakehouse.bronze import run_bronze
from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.pipeline import setup

cfg = LakehouseConfig.for_databricks(dbutils.widgets.get("catalog"))
setup(spark, cfg)
print(run_bronze(spark, cfg, run_id=dbutils.widgets.get("run_id"), use_autoloader=True))
