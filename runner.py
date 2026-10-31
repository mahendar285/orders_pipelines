"""Pipeline orchestration.

This module is the single place that knows the order of operations, the failure
policy, and what a run is allowed to leave behind when it dies.
"""
from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame

from . import loader, quality, transform, validate
from .config import Config
from .metrics import RunContext, new_run_id, read_summary, utc_now
from .session import get_spark

SUMMARY_NAME = "summary.json"


def _already_succeeded(config: Config, run_id: str) -> Optional[Dict]:
    """A run id that already succeeded is not run again by accident."""
    path = Path(config.runs_dir) / ("run_id=" + run_id) / SUMMARY_NAME
    if not path.exists():
        return None
    summary = read_summary(path)
    if summary.get("status") == "SUCCEEDED":
        return summary
    return None


def _union(frames) -> DataFrame:
    usable = [frame for frame in frames if frame is not None]
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), usable)


def run_pipeline(
    config: Config,
    strategy: str = "optimized",
    run_id: Optional[str] = None,
    force: bool = False,
    full_refresh: bool = False,
) -> Dict:
    """Run the whole pipeline once and return the run summary."""
    run_id = run_id or new_run_id()

    if not force:
        previous = _already_succeeded(config, run_id)
        if previous is not None:
            previous["skipped"] = True
            return previous

    context = RunContext(run_id=run_id, strategy=strategy, config=config)
    spark = get_spark(config, strategy=strategy)

    try:
        if full_refresh:
            loader.purge_fact(config)
            context.note("full refresh requested: curated table deleted before the run")

        # ---- read + validate the four extracts --------------------------
        results = {}
        with context.stage("validate"):
            for table in ("customers", "products", "orders", "order_items"):
                path = config.raw_source(table)
                if not path.exists():
                    raise FileNotFoundError(
                        "missing source extract " + str(path) + " - run 'make sample' first"
                    )
                result = validate.validate_source(spark, path, table, config, run_id, context.as_of)
                for warning in result.warnings:
                    context.note(warning)
                results[table] = result

        # ---- referential integrity -------------------------------------
        with context.stage("references"):
            customers = results["customers"].frame
            products = results["products"].frame
            orders = results["orders"].frame
            items = results["order_items"].frame

            orders, orphan_orders = validate.check_reference(orders, customers, "customer_id")
            items, orphan_orders_of_item = validate.check_reference(items, orders, "order_id")
            items, orphan_skus = validate.check_reference(items, products, "sku")

        # ---- quarantine: one dataset, every reject ----------------------
        reject_frames = [
            validate.to_quarantine(results[table].rejects, table, run_id)
            for table in results
        ]
        reject_frames.extend([
            validate.to_quarantine(orphan_orders, "orders", run_id),
            validate.to_quarantine(orphan_orders_of_item, "order_items", run_id),
            validate.to_quarantine(orphan_skus, "order_items", run_id),
        ])
        quarantine = _union(reject_frames)

        with context.stage("quarantine"):
            loader.write_quarantine(quarantine, config, run_id)

        # ---- counts and the reconciliation identity ---------------------
        with context.stage("counts"):
            per_table = {}
            for table, result in results.items():
                reason_counts = validate.reason_counts(result.rejects)
                buckets = validate.categorise(reason_counts)
                per_table[table] = {
                    "raw": result.counts["raw"],
                    "valid": result.counts["valid"],
                    **buckets,
                }
            # orphans discovered after the per-table pass belong to their table
            orphan_reasons = {
                "orders": validate.reason_counts(orphan_orders),
                "order_items": validate.reason_counts(
                    _union([orphan_orders_of_item, orphan_skus])
                ),
            }
            for table, counts in orphan_reasons.items():
                per_table[table]["orphans"] += sum(counts.values())
                per_table[table]["valid"] -= sum(counts.values())

            context.counts = per_table
            context.rejects = validate.reason_counts(quarantine)
            context.reconciliation = quality.reconcile_counts(per_table)

        # ---- transform --------------------------------------------------
        with context.stage("transform"):
            strategy_fn = transform.STRATEGIES[strategy]
            fact = strategy_fn(
                orders=orders,
                items=items,
                customers=customers,
                products=products,
                as_of=context.as_of,
                run_id=run_id,
                late_arrival_days=int(config.rules()["late_arrival_days"]),
            )
            # The plan is evidence: it shows which joins were broadcast and how many
            # exchanges the engine needed. Captured before the write, because that is
            # the frame the benchmark compares.
            plan = fact._jdf.queryExecution().optimizedPlan().toString()
            context.plan_shape = {
                "broadcast_joins": plan.count("BroadcastHashJoin"),
                "sort_merge_joins": plan.count("SortMergeJoin"),
                "exchanges": plan.count("Exchange"),
                "python_udfs": plan.count("BatchEvalPython"),
            }

        with context.stage("publish"):
            loader.write_fact(fact, config)

        # ---- publish-time verification ---------------------------------
        with context.stage("checks"):
            checks = quality.post_write_checks(spark, config, run_id)
            context.checks = checks
            quality.assert_reconciled(context.reconciliation)
            if not checks["passed"]:
                broken = [name for name, ok in checks["checks"].items() if not ok]
                raise quality.QualityError("post-write checks failed: " + ", ".join(broken))

        context.counts["fact_order_line"] = {"rows": checks["details"]["rows_for_run"]}
        context.status = "SUCCEEDED"

    except Exception as error:  # noqa: BLE001 - the failure must be recorded before it propagates
        context.fail(type(error).__name__ + ": " + str(error))
        context.persist()
        context.log.error("run " + run_id + " failed: " + context.failure_reason)
        raise

    finally:
        if context.status == "RUNNING":
            context.fail("run did not complete")

    summary_path = context.persist()
    context.log.info(
        "run %s %s strategy=%s rows=%s %.1fs summary=%s",
        run_id,
        context.status,
        strategy,
        context.counts.get("fact_order_line", {}).get("rows", 0),
        context.summary()["duration_seconds"] or 0.0,
        summary_path,
    )
    return context.summary()
