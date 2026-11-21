import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from finance_lakehouse.data_generator import Scale  # noqa: E402

SMALL = Scale(customers=40, new_customers=8, customer_updates=6, merchants=15, transactions=1500, new_transactions=600,
              invalid_rate=0.03, duplicate_rate=0.02, reversal_rate=0.02, late_rate=0.05, unknown_merchant_rate=0.02)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from finance_lakehouse.spark import get_spark

    session = get_spark("finance-lakehouse-tests", warehouse_dir=str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()
