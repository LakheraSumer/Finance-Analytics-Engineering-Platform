"""Synthetic retail-banking source data, written as JSON-lines files to the landing zone.

Batch 1 is the initial load (Jan-Jun 2025). Batch 2 is an incremental load (Jul 2025) that also contains customer
attribute changes (for SCD Type 2), late-arriving June transactions, status updates (reversals) of batch-1
transactions and re-sent duplicates. Both batches include ~2% invalid records and messy-but-valid formatting
(thousand separators, mixed case, alternative timestamp formats) so the silver layer has real work to do.

Pure Python (no Spark), deterministic for a given seed, so tests can assert exact expected counts.
"""
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

ENTITIES = ("customers", "accounts", "merchants", "transactions")
CATEGORIES = {
    "groceries": 5411, "dining": 5812, "fuel": 5541, "travel": 4511,
    "utilities": 4900, "shopping": 5311, "healthcare": 8062, "entertainment": 7832,
}
BRANDS = {
    "groceries": ["FreshMart", "Daily Basket", "GreenGrocer"], "dining": ["Spice Route", "Chai Point", "Urban Tadka"],
    "fuel": ["FuelStop", "HighwayFuels"], "travel": ["SkyJet", "RailYatra", "CityCabs"],
    "utilities": ["PowerGrid Bill", "AquaSupply", "FiberNet"], "shopping": ["StyleHub", "ElectroWorld", "HomeKart"],
    "healthcare": ["CarePlus Pharmacy", "MediLab"], "entertainment": ["CinePlex", "StreamMax"],
}
CITIES = ["Pune", "Mumbai", "Bengaluru", "Hyderabad", "Delhi", "Chennai", "Kolkata", "Ahmedabad"]
SEGMENTS = ["retail", "premium", "business"]
CHANNELS = ["pos", "online", "upi", "atm", "netbanking"]
FIRST_NAMES = ["Aarav", "Diya", "Kabir", "Ananya", "Vihaan", "Isha", "Arjun", "Meera", "Rohan", "Sara",
               "Aditya", "Neha", "Karan", "Pooja", "Rahul", "Sneha", "Vikram", "Priya", "Nikhil", "Tara"]
LAST_NAMES = ["Sharma", "Patel", "Iyer", "Reddy", "Khan", "Das", "Mehta", "Nair", "Joshi", "Gupta", "Rao", "Singh"]
CORRUPTIONS = ("missing transaction_id", "missing account_id", "invalid amount", "non-positive amount",
               "invalid timestamp", "future timestamp", "invalid txn_type")

BATCH_WINDOWS = {
    1: (datetime(2025, 1, 1), datetime(2025, 6, 30, 23, 59, 59)),
    2: (datetime(2025, 7, 1), datetime(2025, 7, 31, 23, 59, 59)),
}
ISO = "%Y-%m-%dT%H:%M:%S"


@dataclass
class Scale:
    customers: int = 200
    new_customers: int = 30
    customer_updates: int = 20
    merchants: int = 60
    transactions: int = 8000
    new_transactions: int = 3000
    invalid_rate: float = 0.02
    duplicate_rate: float = 0.01
    reversal_rate: float = 0.01
    late_rate: float = 0.03
    unknown_merchant_rate: float = 0.01


@dataclass
class BatchStats:
    batch: int
    records: dict[str, int] = field(default_factory=dict)
    invalid_customers: int = 0
    invalid_transactions: int = 0
    duplicate_transactions: int = 0
    late_transactions: int = 0
    unknown_merchant_transactions: int = 0
    valid_transaction_ids: set[str] = field(default_factory=set, repr=False)
    updated_customers: dict[str, str] = field(default_factory=dict, repr=False)  # customer_id -> change time (ISO)
    reversed_transactions: set[str] = field(default_factory=set, repr=False)


def _ts(rng: random.Random, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))


def _messy_ts(rng: random.Random, ts: datetime) -> str:
    roll = rng.random()
    if roll < 0.05:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if roll < 0.10:
        return ts.strftime("%d/%m/%Y %H:%M:%S")
    return ts.strftime(ISO)


def _messy_amount(rng: random.Random, amount: float) -> str:
    roll = rng.random()
    if roll < 0.10:
        return f"{amount:,.2f}"
    if roll < 0.15:
        return f" {amount:.2f} "
    return f"{amount:.2f}"


def _messy_case(rng: random.Random, value: str) -> str:
    roll = rng.random()
    if roll < 0.05:
        return f" {value.upper()} "
    if roll < 0.10:
        return value.title()
    return value


class _Universe:
    """Builds both batches in memory so batch 2 can reference (update / reverse / duplicate) batch-1 records."""

    def __init__(self, seed: int, scale: Scale) -> None:
        self.rng = random.Random(seed)
        self.scale = scale
        self.records: dict[int, dict[str, list[dict]]] = {b: {e: [] for e in ENTITIES} for b in (1, 2)}
        self.stats = {b: BatchStats(b) for b in (1, 2)}
        self.accounts: list[dict] = []
        self.txn_by_id: dict[str, dict] = {}
        self._account_seq = 0
        self._build_merchants()
        self._build_customers(1, range(1, scale.customers + 1), "2025-01-01T00:00:00")
        self._build_transactions(1, scale.transactions)
        self._build_customer_updates()
        self._build_customers(2, range(scale.customers + 1, scale.customers + scale.new_customers + 1), "2025-07-01T00:00:00")
        self._build_transactions(2, scale.new_transactions)
        self._build_reversals()
        for b in (1, 2):
            self.stats[b].records = {e: len(self.records[b][e]) for e in ENTITIES}

    def _build_merchants(self) -> None:
        rng = self.rng
        for i in range(1, self.scale.merchants + 1):
            category = rng.choice(list(CATEGORIES))
            self.records[1]["merchants"].append({
                "merchant_id": f"M{i:04d}",
                "merchant_name": f"{rng.choice(BRANDS[category])} {rng.choice(CITIES)}",
                "category": _messy_case(rng, category),
                "mcc_code": str(CATEGORIES[category]),
                "city": rng.choice(CITIES),
                "updated_at": "2025-01-01T00:00:00",
            })
        self.merchant_ids = [m["merchant_id"] for m in self.records[1]["merchants"]]

    def _customer(self, n: int, updated_at: str, city: str | None = None, segment: str | None = None) -> dict:
        rng = self.rng
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        return {
            "customer_id": f"C{n:05d}",
            "first_name": first if rng.random() > 0.05 else f" {first.lower()} ",
            "last_name": last,
            "email": f"{first}.{last}{n}@example.com".lower() if rng.random() > 0.05 else f"{first}.{last}{n}@EXAMPLE.COM",
            "phone": f"+91 9{rng.randint(100000000, 999999999)}",
            "city": city or rng.choice(CITIES),
            "segment": _messy_case(rng, segment or rng.choice(SEGMENTS)),
            "updated_at": updated_at,
        }

    def _build_customers(self, batch: int, numbers: range, updated_at: str) -> None:
        rng = self.rng
        opened_from, _ = BATCH_WINDOWS[batch]
        for n in numbers:
            self.records[batch]["customers"].append(self._customer(n, updated_at))
            for _ in range(rng.choice([1, 1, 2])):
                self._account_seq += 1
                account_type = rng.choice(["savings", "savings", "current", "credit_card"])
                account = {
                    "account_id": f"A{self._account_seq:06d}",
                    "customer_id": f"C{n:05d}",
                    "account_type": _messy_case(rng, account_type),
                    "status": "active",
                    "opened_date": (opened_from - timedelta(days=rng.randint(0, 900) if batch == 1 else 0)).strftime("%Y-%m-%d"),
                    "credit_limit": str(rng.choice([50000, 100000, 200000])) if account_type == "credit_card" else None,
                    "currency": "INR",
                    "updated_at": updated_at,
                }
                self.records[batch]["accounts"].append(account)
                self.accounts.append({**account, "_batch": batch})
        for _ in range(2 if batch == 1 else 1):  # records without a business key -> quarantine
            bad = self._customer(0, updated_at)
            bad["customer_id"] = None
            self.records[batch]["customers"].append(bad)
            self.stats[batch].invalid_customers += 1

    def _build_customer_updates(self) -> None:
        rng = self.rng
        start, _ = BATCH_WINDOWS[2]
        for n in rng.sample(range(1, self.scale.customers + 1), self.scale.customer_updates):
            original = next(c for c in self.records[1]["customers"] if c["customer_id"] == f"C{n:05d}")
            changed_at = (start + timedelta(days=rng.randint(3, 20), hours=rng.randint(0, 23))).strftime(ISO)
            new_city = rng.choice([c for c in CITIES if c != original["city"]])
            updated = {**original, "city": new_city, "segment": rng.choice(SEGMENTS), "updated_at": changed_at}
            self.records[2]["customers"].append(updated)
            self.stats[2].updated_customers[original["customer_id"]] = changed_at

    def _transaction(self, tid: str, ts: datetime, batch: int) -> dict:
        rng = self.rng
        eligible = [a for a in self.accounts if a["_batch"] <= batch and (a["_batch"] == 1 or ts >= BATCH_WINDOWS[2][0])]
        account = rng.choice(eligible)
        txn_type = "credit" if rng.random() < 0.15 else "debit"
        if txn_type == "credit":
            merchant_id, amount = None, round(rng.uniform(5000, 150000), 2)
        else:
            unknown = rng.random() < self.scale.unknown_merchant_rate
            merchant_id = "M9999" if unknown else rng.choice(self.merchant_ids)
            amount = round(min(200000.0, max(10.0, rng.lognormvariate(6.5, 1.1))), 2)
        return {
            "transaction_id": tid,
            "account_id": account["account_id"],
            "merchant_id": merchant_id,
            "txn_ts": _messy_ts(rng, ts),
            "amount": _messy_amount(rng, amount),
            "currency": "INR",
            "txn_type": _messy_case(rng, txn_type),
            "channel": _messy_case(rng, rng.choice(CHANNELS)),
            "status": "posted",
            "updated_at": ts.strftime(ISO),
        }

    @staticmethod
    def _corrupt(record: dict, kind: str) -> None:
        if kind == "missing transaction_id":
            record["transaction_id"] = None
        elif kind == "missing account_id":
            record["account_id"] = None
        elif kind == "invalid amount":
            record["amount"] = "abc"
        elif kind == "non-positive amount":
            record["amount"] = "-120.00"
        elif kind == "invalid timestamp":
            record["txn_ts"] = "not-a-timestamp"
        elif kind == "future timestamp":
            record["txn_ts"] = "2030-01-01T10:00:00"
            record["updated_at"] = "2030-01-01T10:00:00"
        elif kind == "invalid txn_type":
            record["txn_type"] = "refundx"

    def _build_transactions(self, batch: int, count: int) -> None:
        rng, s, stats = self.rng, self.scale, self.stats[batch]
        start, end = BATCH_WINDOWS[batch]
        out = self.records[batch]["transactions"]
        for seq in range(1, count + 1):
            late = batch == 2 and rng.random() < s.late_rate
            ts = _ts(rng, datetime(2025, 6, 1), datetime(2025, 6, 30, 23, 59, 59)) if late else _ts(rng, start, end)
            record = self._transaction(f"T{batch}{seq:07d}", ts, batch)
            if rng.random() < s.invalid_rate:
                self._corrupt(record, rng.choice(CORRUPTIONS))
                stats.invalid_transactions += 1
                out.append(record)
                continue
            stats.late_transactions += late
            stats.unknown_merchant_transactions += record["merchant_id"] == "M9999"
            stats.valid_transaction_ids.add(record["transaction_id"])
            self.txn_by_id[record["transaction_id"]] = record
            out.append(record)
            if rng.random() < s.duplicate_rate:  # the source system re-sends the same event
                out.append(dict(record))
                stats.duplicate_transactions += 1

    def _build_reversals(self) -> None:
        rng = self.rng
        candidates = sorted(self.stats[1].valid_transaction_ids)
        for tid in rng.sample(candidates, int(len(candidates) * self.scale.reversal_rate)):
            original = self.txn_by_id[tid]
            reversed_at = datetime.strptime(original["updated_at"], ISO) + timedelta(days=3)
            self.records[2]["transactions"].append({**original, "status": "reversed", "updated_at": reversed_at.strftime(ISO)})
            self.stats[2].reversed_transactions.add(tid)


def generate_landing_data(landing_dir: str, batch: int, seed: int = 7, scale: Scale | None = None) -> BatchStats:
    """Write one batch (1 = initial load, 2 = incremental) as `<landing_dir>/<entity>/batch_00N.json`."""
    if batch not in (1, 2):
        raise ValueError("batch must be 1 or 2")
    universe = _Universe(seed, scale or Scale())
    for entity, rows in universe.records[batch].items():
        if not rows:
            continue
        folder = os.path.join(landing_dir, entity)
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, f"batch_{batch:03d}.json"), "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
    return universe.stats[batch]
