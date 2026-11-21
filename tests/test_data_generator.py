import json

from conftest import SMALL
from finance_lakehouse.data_generator import generate_landing_data


def _read(folder, entity, batch):
    with open(folder / entity / f"batch_{batch:03d}.json", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def test_generation_is_deterministic(tmp_path):
    generate_landing_data(str(tmp_path / "a"), 1, scale=SMALL)
    generate_landing_data(str(tmp_path / "b"), 1, scale=SMALL)
    for entity in ("customers", "accounts", "merchants", "transactions"):
        assert _read(tmp_path / "a", entity, 1) == _read(tmp_path / "b", entity, 1)


def test_batch_one_counts(tmp_path):
    stats = generate_landing_data(str(tmp_path), 1, scale=SMALL)
    rows = _read(tmp_path, "transactions", 1)
    assert len(rows) == SMALL.transactions + stats.duplicate_transactions == stats.records["transactions"]
    assert len(stats.valid_transaction_ids) == SMALL.transactions - stats.invalid_transactions
    assert stats.invalid_transactions > 0 and stats.duplicate_transactions > 0
    assert len(_read(tmp_path, "customers", 1)) == SMALL.customers + stats.invalid_customers


def test_batch_two_has_incremental_scenarios(tmp_path):
    first = generate_landing_data(str(tmp_path), 1, scale=SMALL)
    second = generate_landing_data(str(tmp_path), 2, scale=SMALL)
    assert len(second.updated_customers) == SMALL.customer_updates
    assert second.reversed_transactions and second.reversed_transactions <= first.valid_transaction_ids
    assert second.late_transactions > 0
    assert not (second.valid_transaction_ids & first.valid_transaction_ids)
    reversed_rows = [r for r in _read(tmp_path, "transactions", 2) if r["status"] == "reversed"]
    assert {r["transaction_id"] for r in reversed_rows} == second.reversed_transactions
