"""Typed configuration: one YAML file in, one immutable handle out.

Nothing else in the package reads an environment variable or concatenates a
path, so "where does that file come from" has exactly one answer.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

#: Statuses that produce revenue.
REVENUE_STATUSES: List[str] = ["PLACED", "PAID", "SHIPPED", "DELIVERED"]
#: Statuses kept in the fact table for audit but zeroed for revenue.
VOID_STATUSES: List[str] = ["CANCELLED", "REFUNDED"]
VALID_STATUSES: List[str] = REVENUE_STATUSES + VOID_STATUSES
VALID_CURRENCIES: List[str] = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY"]

DEFAULT_CONFIG_PATH = "conf/pipeline.yaml"


@dataclass(frozen=True)
class Config:
    """Immutable run configuration, loaded once per process."""

    data_root: Path
    tables: Dict[str, str]
    validation: Dict[str, Any]
    write: Dict[str, Any]
    spark: Dict[str, Any]
    benchmark: Dict[str, Any]

    # --- locations --------------------------------------------------------
    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def curated_dir(self) -> Path:
        return self.data_root / "curated"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def runs_dir(self) -> Path:
        return self.data_root / "runs"

    @property
    def reference_dir(self) -> Path:
        return self.data_root / "reference"

    def raw_source(self, table: str) -> Path:
        """Location of one source extract as produced by the generator."""
        return self.raw_dir / table / (table + ".csv")

    @property
    def fact_path(self) -> Path:
        return self.curated_dir / self.tables["fact"]

    @property
    def expected_labels_path(self) -> Path:
        return self.reference_dir / "expected_labels.csv"

    # --- knobs ------------------------------------------------------------
    def rules(self) -> Dict[str, Any]:
        return self.validation["rules"]

    def partition_by(self) -> List[str]:
        return list(self.write.get("partition_by") or [])

    def spark_conf(self) -> Dict[str, str]:
        configured = self.spark.get("conf") or {}
        return {str(key): str(value) for key, value in configured.items()}

    @property
    def master(self) -> Optional[str]:
        return self.spark.get("master")

    # --- construction -----------------------------------------------------
    @classmethod
    def load(
        cls,
        path: str = DEFAULT_CONFIG_PATH,
        data_root: Optional[str] = None,
        shuffle_partitions: Optional[int] = None,
    ) -> "Config":
        with open(path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)

        root = Path(data_root) if data_root else Path((payload.get("paths") or {}).get("data_root", "data"))
        spark_section = dict(payload.get("spark") or {})
        conf = dict(spark_section.get("conf") or {})
        if shuffle_partitions is not None:
            conf["spark.sql.shuffle.partitions"] = shuffle_partitions
        spark_section["conf"] = conf

        return cls(
            data_root=root,
            tables=payload["tables"],
            validation=payload["validation"],
            write=payload.get("write") or {},
            spark=spark_section,
            benchmark=payload.get("benchmark") or {},
        )
