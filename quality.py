"""Quality gates.

Two different questions, deliberately answered in two places:

* reconcile_counts - does source arithmetic balance? (raw = rejects + valid)
* post_write_checks - is the published table internally consistent?

The second reads the table back from disk. Checking the in-memory frame would
prove what the job intended, not what a consumer will actually read.
"""
from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import functions as F

from . import config as cfg


class QualityError(Exception):
    """A published invariant does not hold."""


def reconcile_counts(counts: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, Any]]:
    """Source-to-target identity per table, as arithmetic rather than prose."""
    report: Dict[str, Dict[str, Any]] = {}
    for table, values in counts.items():
        rejected = (
            values.get("malformed", 0)
            + values.get("missing", 0)
            + values.get("cast", 0)
            + values.get("rules", 0)
            + values.get("duplicates", 0)
            + values.get("orphans", 0)
        )
        report[table] = {
            "raw": values.get("raw", 0),
            "rejected": rejected,
            "valid": values.get("valid", 0),
            "balanced": values.get("raw", 0) == rejected + values.get("valid", 0),
        }
    return report


def assert_reconciled(report: Dict[str, Dict[str, Any]]) -> None:
    """Raise naming every unbalanced table, not just the first one."""
    broken = [table for table, values in report.items() if not values["balanced"]]
    if broken:
        detail = ", ".join(
            table + " (raw=" + str(report[table]["raw"])
            + ", rejected=" + str(report[table]["rejected"])
            + ", valid=" + str(report[table]["valid"]) + ")"
            for table in broken
        )
        raise QualityError("source-to-target counts do not balance for: " + detail)


def post_write_checks(spark, config, run_id: str) -> Dict[str, Any]:
    """Invariants verified against the bytes that were just written."""
    fact = spark.read.parquet(str(config.fact_path))

    duplicate_keys = (
        fact.groupBy("order_id", "line_no").count().filter(F.col("count") > 1).count()
    )
    missing_required = fact.filter(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("sku").isNull()
        | F.col("net_amount").isNull()
        | F.col("order_date").isNull()
    ).count()
    money_broken = fact.filter(
        (F.col("discount_amount") > F.col("gross_amount")) | (F.col("net_amount") < 0)
    ).count()
    void_with_revenue = fact.filter(
        F.col("status").isin(*cfg.VOID_STATUSES) & (F.col("net_revenue_amount") != 0)
    ).count()
    partition_mismatch = fact.filter(F.to_date(F.col("order_ts")) != F.col("order_date")).count()
    rows_for_run = fact.filter(F.col("run_id") == run_id).count()
    late_rows = fact.filter(F.col("is_late_arriving")).count()

    boolean_checks = {
        "grain_unique": duplicate_keys == 0,
        "required_not_null": missing_required == 0,
        "money_consistent": money_broken == 0,
        "void_status_carries_no_revenue": void_with_revenue == 0,
        "partition_matches_event_date": partition_mismatch == 0,
        "run_id_present": rows_for_run > 0,
    }

    return {
        "checks": boolean_checks,
        "passed": all(boolean_checks.values()),
        "details": {
            "duplicate_keys": duplicate_keys,
            "missing_required": missing_required,
            "money_broken": money_broken,
            "void_with_revenue": void_with_revenue,
            "partition_mismatch": partition_mismatch,
            "rows_for_run": rows_for_run,
            "late_arriving_rows": late_rows,
        },
    }
