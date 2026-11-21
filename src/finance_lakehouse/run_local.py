"""Run the whole medallion pipeline locally (needs Java 17 + `pip install -r requirements-dev.txt`).

    python -m finance_lakehouse.run_local --base-dir .lakehouse
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime

from .config import LakehouseConfig
from .data_generator import generate_landing_data
from .pipeline import run_pipeline
from .spark import get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default=".lakehouse")
    parser.add_argument("--as-of", default="2025-08-01", help="run date used for future-timestamp and freshness checks")
    args = parser.parse_args()

    cfg = LakehouseConfig.for_local(args.base_dir)
    spark = get_spark(warehouse_dir=f"{args.base_dir}/warehouse")
    as_of = datetime.fromisoformat(args.as_of)
    for batch in (1, 2):
        stats = generate_landing_data(cfg.landing_path, batch)
        print(f"batch {batch} generated: {stats.records}")
        summary = run_pipeline(spark, cfg, run_id=f"local-batch-{batch}", as_of=as_of)
        print(json.dumps(summary, indent=2, default=str))
    spark.sql(f"SELECT * FROM {cfg.table('gold', 'agg_monthly_category_spend')} ORDER BY year_month, total_spend DESC").show(10)


if __name__ == "__main__":
    main()
