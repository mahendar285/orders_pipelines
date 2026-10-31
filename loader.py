"""Writers.

Rerun safety lives here, in three explicit rules:

1. the fact table is written with partitionOverwriteMode=dynamic, so a rerun
   replaces exactly the order_date partitions it produced;
2. quarantine is written under a per-run directory which is deleted before the
   rewrite, so a retried run cannot leave two generations of rejects behind;
3. the fact table is never dropped implicitly. full_refresh is an explicit flag,
   because "just rebuild everything" is a decision, not a clean-up.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame


def _delete(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def purge_run_outputs(config, run_id: str) -> None:
    """Remove the per-run quarantine directory before writing it again."""
    _delete(Path(config.quarantine_dir) / ("run_id=" + run_id))


def purge_fact(config) -> None:
    """Drop the curated fact table. Only called for an explicit full refresh."""
    _delete(Path(config.fact_path))


def write_fact(frame: DataFrame, config, partition_by: Optional[List[str]] = None) -> str:
    """Write the curated fact table. Returns the written path."""
    columns = config.partition_by() if partition_by is None else partition_by
    writer = frame.write.mode("overwrite")
    if columns:
        writer = writer.partitionBy(*columns)
    writer.parquet(str(config.fact_path))
    return str(config.fact_path)


def write_quarantine(frame: DataFrame, config, run_id: str) -> str:
    """Write rejects for this run: quarantine/run_id=<id>/reject_reason=<reason>/."""
    purge_run_outputs(config, run_id)
    target = Path(config.quarantine_dir) / ("run_id=" + run_id)
    (frame.write.mode("overwrite")
        .partitionBy("reject_reason")
        .parquet(str(target)))
    return str(target)
