"""Shared fixtures.

One Spark session for the whole session (JVM start-up dominates otherwise), one
temporary data root per test (so tests cannot see each other's files), and two
helpers that turn a list of dicts into a validated frame.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline import validate  # noqa: E402
from pipeline.config import Config  # noqa: E402
from pipeline.schemas import SOURCE_COLUMNS, string_schema  # noqa: E402

FIXED_AS_OF = datetime(2026, 10, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("orders-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config(tmp_path):
    return Config.load(data_root=str(tmp_path))


def _frame(spark, table, rows):
    frame = spark.createDataFrame(rows, schema=string_schema(SOURCE_COLUMNS[table]))
    payload = F.to_json(F.struct(*[F.col(name).alias(name) for name in SOURCE_COLUMNS[table]]))
    return frame.withColumn("_source_file", F.lit(table + ".csv")).withColumn("_raw_json", payload)


def _prepare(spark, table, rows, config, as_of=FIXED_AS_OF):
    """Read -> cast -> rules for a hand-built frame, returning (valid, rejected)."""
    typed = validate.cast_source(_frame(spark, table, rows), table)
    ruled = validate.apply_rules(typed, table, config.rules(), as_of)
    rejected = ruled.filter(F.col("_reject_reason").isNotNull())
    valid = ruled.filter(F.col("_reject_reason").isNull())
    return valid, rejected


@pytest.fixture
def prepare_orders(spark, config):
    def _run(rows, as_of=FIXED_AS_OF):
        return _prepare(spark, "orders", rows, config, as_of)

    return _run


@pytest.fixture
def prepare_items(spark, config):
    def _run(rows, as_of=FIXED_AS_OF):
        return _prepare(spark, "order_items", rows, config, as_of)

    return _run


@pytest.fixture
def prepare_customers(spark, config):
    def _run(rows, as_of=FIXED_AS_OF):
        return _prepare(spark, "customers", rows, config, as_of)

    return _run


def order_row(**overrides):
    row = {
        "order_id": "O0000001",
        "customer_id": "C00001",
        "order_ts": "2026-09-05T10:00:00Z",
        "status": "DELIVERED",
        "channel": "WEB",
        "currency": "USD",
    }
    row.update(overrides)
    return row


def item_row(**overrides):
    row = {
        "order_id": "O0000001",
        "line_no": "1",
        "sku": "SKU0001",
        "quantity": "2",
        "unit_price": "19.99",
        "discount_pct": "0.00",
        "updated_at": "2026-09-05T10:00:00Z",
    }
    row.update(overrides)
    return row


def customer_row(**overrides):
    row = {
        "customer_id": "C00001",
        "email": "user@example.com",
        "country_code": "US",
        "segment": "SMB",
        "signup_ts": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def product_row(**overrides):
    row = {
        "sku": "SKU0001",
        "category": "home",
        "unit_cost": "10.00",
        "is_active": "true",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row
