"""Baseline vs optimized.

The rule this module enforces: a timing is only reported if both strategies
produced the same business rows. The checksum check runs first and a mismatch
raises, so a "faster" run that changed a number can never be quoted.
"""
from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .generator import generate_dataset
from .metrics import get_logger
from .runner import run_pipeline
from .session import get_spark

def _checksum(spark, fact_path: Path) -> Dict[str, Any]:
    fact = spark.read.parquet(str(fact_path))
    row = fact.agg(
        F.count(F.lit(1)).alias("rows"),
        F.round(F.coalesce(F.sum("gross_amount"), F.lit(0.0)), 2).alias("gross"),
        F.round(F.coalesce(F.sum("net_amount"), F.lit(0.0)), 2).alias("net"),
        F.round(F.coalesce(F.sum("net_revenue_amount"), F.lit(0.0)), 2).alias("revenue"),
    ).first()
    return {
        "rows": int(row["rows"]),
        "gross_amount": float(row["gross"]),
        "net_amount": float(row["net"]),
        "net_revenue": float(row["revenue"]),
    }


def run_benchmark(config, rows: Optional[int] = None, repeats: Optional[int] = None, seed: int = 42) -> Dict[str, Any]:
    """Generate data, run both strategies, compare rows, then report timings."""
    log = get_logger()
    line_rows = int(rows or config.benchmark.get("rows", 100000))
    attempts = int(repeats or config.benchmark.get("repeats", 1))

    bench_config = replace(config, data_root=Path(config.data_root) / "benchmark")
    generated = generate_dataset(bench_config, rows=line_rows, seed=seed)
    log.info("benchmark data: %s order lines, %s orders", generated["order_items"], generated["orders"])

    spark = get_spark(bench_config, strategy="optimized", app_name="orders-pipeline:benchmark")

    results: Dict[str, Any] = {"rows_requested": line_rows, "generated": generated, "runs": {}}

    for strategy in ("baseline", "optimized"):
        durations = []
        summary = None
        for attempt in range(attempts):
            run_id = "bench-" + strategy + "-" + str(attempt)
            started = time.perf_counter()
            summary = run_pipeline(
                bench_config,
                strategy=strategy,
                run_id=run_id,
                force=True,
                full_refresh=(attempt == 0),
            )
            durations.append(round(time.perf_counter() - started, 3))

            snapshot = _checksum(spark, Path(bench_config.fact_path))
            if attempt == 0:
                results["runs"][strategy] = {
                    "run_id": run_id,
                    "duration_seconds": durations,
                    "best_seconds": min(durations),
                    "checksum": snapshot,
                    "stage_timings": summary.get("timings", {}),
                    "plan_shape": summary.get("plan_shape", {}),
                    "shuffle_partitions": 200 if strategy == "baseline" else bench_config.spark_conf().get(
                        "spark.sql.shuffle.partitions", "8"
                    ),
                }
            else:
                results["runs"][strategy]["duration_seconds"] = durations
                results["runs"][strategy]["best_seconds"] = min(durations)

    baseline = results["runs"]["baseline"]
    optimized = results["runs"]["optimized"]

    results["checksums_match"] = baseline["checksum"] == optimized["checksum"]
    results["speedup"] = round(baseline["best_seconds"] / optimized["best_seconds"], 2) if optimized["best_seconds"] else None
    results["reconciliation"] = summary.get("reconciliation", {}) if summary else {}

    if not results["checksums_match"]:
        raise AssertionError(
            "baseline and optimized produced different rows: "
            + str(baseline["checksum"]) + " vs " + str(optimized["checksum"])
        )

    log.info(
        "baseline %.1fs -> optimized %.1fs (%sx), plan shape baseline=%s optimized=%s",
        baseline["best_seconds"],
        optimized["best_seconds"],
        results["speedup"],
        baseline["plan_shape"],
        optimized["plan_shape"],
    )
    return results
