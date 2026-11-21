# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate landing data
# MAGIC Writes one batch of synthetic retail-banking source files (customers, accounts, merchants, transactions) as
# MAGIC JSON lines into the Unity Catalog volume `<catalog>.finance_ops.landing`.
# MAGIC
# MAGIC * `batch = 1` - initial load (Jan-Jun 2025)
# MAGIC * `batch = 2` - incremental load (Jul 2025) with customer changes, late arrivals, reversals and duplicates

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("batch", "1", ["1", "2"])

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from finance_lakehouse.config import LakehouseConfig
from finance_lakehouse.data_generator import generate_landing_data
from finance_lakehouse.pipeline import setup

cfg = LakehouseConfig.for_databricks(dbutils.widgets.get("catalog"))
setup(spark, cfg)
stats = generate_landing_data(cfg.landing_path, int(dbutils.widgets.get("batch")))
print(stats)
