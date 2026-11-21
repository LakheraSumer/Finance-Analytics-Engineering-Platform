"""Where every table and file lives. On Databricks: Unity Catalog `<catalog>.finance_<layer>` schemas and a Volume
for landing files. Locally / in CI: plain schemas and a local folder."""
from __future__ import annotations

import os
from dataclasses import dataclass

LAYERS = ("bronze", "silver", "gold", "ops")


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str | None = None
    prefix: str = "finance"
    landing_path: str = "/Volumes/workspace/finance_ops/landing"
    checkpoint_path: str = "/Volumes/workspace/finance_ops/checkpoints"

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}")
        name = f"{self.prefix}_{layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    @classmethod
    def for_databricks(cls, catalog: str = "workspace", prefix: str = "finance") -> "LakehouseConfig":
        base = f"/Volumes/{catalog}/{prefix}_ops"
        return cls(catalog, prefix, f"{base}/landing", f"{base}/checkpoints")

    @classmethod
    def for_local(cls, base_dir: str, prefix: str = "finance") -> "LakehouseConfig":
        base = os.path.abspath(base_dir)
        return cls(None, prefix, os.path.join(base, "landing"), os.path.join(base, "checkpoints"))
