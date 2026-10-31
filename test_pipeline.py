"""End-to-end tests against a real local Spark session."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from pipeline import quality, validate
from pipeline.generator import generate_dataset
from pipeline.runner import run_pipeline


def _fact_rows(spark, config):
    frame = spark.read.parquet(str(config.fact_path))
    return sorted(
        (row["order_id"], row["line_no"], row["net_amount"], row["net_revenue_amount"])
        for row in frame.select("order_id", "line_no", "net_amount", "net_revenue_amount").collect()
    )


def test_end_to_end_reconciles_all_tables(spark, config):
    generate_dataset(config, rows=300, seed=7)
    summary = run_pipeline(config, strategy="optimized", run_id="e2e-1")

    assert summary["status"] == "SUCCEEDED"
    for table, values in summary["reconciliation"].items():
        assert values["balanced"], table
    assert summary["checks"]["passed"] is True
    assert summary["counts"]["fact_order_line"]["rows"] > 0


def test_generator_labels_match_the_quarantine(spark, config):
    """The generator says what it broke; the pipeline must have found exactly that."""
    generate_dataset(config, rows=300, seed=11)
    summary = run_pipeline(config, strategy="optimized", run_id="labels-1")

    labels = spark.read.option("header", True).csv(str(config.expected_labels_path))
    expected = {
        row["label"]: int(row["count"])
        for row in labels.filter(F.col("expected_outcome") == "rejected")
        .groupBy("label").count().collect()
    }

    observed = summary["rejects"]
    for label, count in expected.items():
        assert observed.get(label, 0) >= count, label + " was planted but not reported"
    assert "malformed_row" not in observed


def test_rerun_of_the_same_data_is_idempotent(spark, config):
    generate_dataset(config, rows=300, seed=13)
    first = run_pipeline(config, strategy="optimized", run_id="idem-1")
    rows_before = _fact_rows(spark, config)

    run_pipeline(config, strategy="optimized", run_id="idem-2", force=True)

    assert first["counts"]["fact_order_line"]["rows"] == len(rows_before)
    assert _fact_rows(spark, config) == rows_before


def test_a_successful_run_id_is_not_repeated(spark, config):
    generate_dataset(config, rows=120, seed=17)
    run_pipeline(config, strategy="optimized", run_id="once")

    second = run_pipeline(config, strategy="optimized", run_id="once")

    assert second.get("skipped") is True
    assert second["status"] == "SUCCEEDED"


def test_full_refresh_rebuilds_from_scratch(spark, config):
    generate_dataset(config, rows=150, seed=19)
    run_pipeline(config, strategy="optimized", run_id="refresh-1")
    rows_before = len(_fact_rows(spark, config))

    summary = run_pipeline(config, strategy="optimized", run_id="refresh-2", force=True, full_refresh=True)

    assert summary["status"] == "SUCCEEDED"
    assert len(_fact_rows(spark, config)) == rows_before


def test_missing_source_file_fails_loudly(spark, config):
    with pytest.raises(FileNotFoundError):
        run_pipeline(config, strategy="optimized", run_id="missing")


def test_partial_failure_is_observable_and_the_target_survives(spark, config):
    """Simulates an upstream schema change mid-life: the run dies, the table does not."""
    generate_dataset(config, rows=120, seed=23)
    run_pipeline(config, strategy="optimized", run_id="good")
    rows_before = _fact_rows(spark, config)

    orders_path = config.raw_source("orders")
    orders_path.write_text(
        "order_id,customer_id,order_ts\nO0000001,C00001,2026-09-01T00:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(validate.SchemaContractError):
        run_pipeline(config, strategy="optimized", run_id="bad", force=True)

    summary_path = Path(config.runs_dir) / "run_id=bad" / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["status"] == "FAILED"
    assert "channel" in payload["failure_reason"]
    assert _fact_rows(spark, config) == rows_before


def test_baseline_and_optimized_produce_identical_rows(spark, config):
    generate_dataset(config, rows=300, seed=29)
    run_pipeline(config, strategy="baseline", run_id="compare-base")
    baseline_rows = _fact_rows(spark, config)

    run_pipeline(config, strategy="optimized", run_id="compare-opt")
    optimized_rows = _fact_rows(spark, config)

    assert baseline_rows == optimized_rows


def test_reconciliation_identity_maths():
    counts = {
        "orders": {"raw": 10, "valid": 6, "malformed": 1, "missing": 1, "cast": 1, "rules": 1, "duplicates": 0, "orphans": 0},
        "order_items": {"raw": 5, "valid": 5, "malformed": 0, "missing": 0, "cast": 0, "rules": 0, "duplicates": 0, "orphans": 0},
    }

    report = quality.reconcile_counts(counts)

    assert report["orders"]["balanced"] is True
    assert report["order_items"]["balanced"] is True


def test_unbalanced_counts_raise_with_the_table_named():
    counts = {"orders": {"raw": 10, "valid": 9, "malformed": 0, "missing": 0, "cast": 0, "rules": 0, "duplicates": 0, "orphans": 0}}

    with pytest.raises(quality.QualityError) as error:
        quality.assert_reconciled(quality.reconcile_counts(counts))

    assert "orders" in str(error.value)
