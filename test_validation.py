"""Validation tests: the contract layer and the rule layer."""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from pipeline import validate
from pipeline.schemas import SOURCE_COLUMNS, string_schema

from conftest import order_row, item_row, customer_row


def _reasons(rejected):
    return [row["reject_reason"] for row in rejected.select("reject_reason").collect()]


def test_missing_required_column_is_a_contract_error(spark, tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,customer_id,order_ts,status,currency\nO1,C1,2026-09-01T00:00:00Z,PAID,USD\n", encoding="utf-8")

    with pytest.raises(validate.SchemaContractError) as error:
        validate.read_source(spark, path, "orders")

    assert "channel" in str(error.value)


def test_unexpected_extra_column_is_ignored_with_a_warning(spark, tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text(
        "order_id,customer_id,order_ts,status,channel,currency,loyalty_tier\n"
        "O1,C1,2026-09-01T00:00:00Z,PAID,WEB,USD,gold\n",
        encoding="utf-8",
    )

    frame, warnings = validate.read_source(spark, path, "orders")

    assert frame.columns[:6] == SOURCE_COLUMNS["orders"]
    assert any("loyalty_tier" in warning for warning in warnings)
    assert frame.count() == 1


def test_cast_failure_is_quarantined_with_the_column_named(prepare_orders):
    valid, rejected = prepare_orders([order_row(order_ts="not-a-timestamp")])

    assert valid.count() == 0
    assert _reasons(rejected) == ["cast_failure:order_ts"]


def test_missing_required_field_is_quarantined(prepare_orders):
    valid, rejected = prepare_orders([order_row(order_id=""), order_row(order_id="O2")])

    assert valid.count() == 1
    assert _reasons(rejected) == ["missing_required:order_id"]


def test_blank_line_is_reported_as_malformed(spark, prepare_orders):
    rows = [order_row(), {name: "" for name in SOURCE_COLUMNS["orders"]}]
    valid, rejected = prepare_orders(rows)

    assert valid.count() == 1
    assert _reasons(rejected) == ["malformed_row"]


def test_missing_required_field_outranks_cast_failure(prepare_orders):
    """A row with two faults reports one reason, and the priority is a contract."""
    _, rejected = prepare_orders([order_row(customer_id="", order_ts="nope")])

    assert _reasons(rejected) == ["missing_required:customer_id"]


def test_invalid_status_and_currency_are_rejected_separately(prepare_orders):
    valid, rejected = prepare_orders([
        order_row(order_id="O1", status="PENDING"),
        order_row(order_id="O2", currency="XYZ"),
        order_row(order_id="O3", status="delivered", currency="eur"),
    ])

    assert valid.count() == 1
    assert sorted(_reasons(rejected)) == ["invalid_currency", "invalid_status"]


def test_quantity_and_price_rules(prepare_items):
    valid, rejected = prepare_items([
        item_row(line_no="1", quantity="0"),
        item_row(line_no="2", quantity="5000"),
        item_row(line_no="3", unit_price="-1.00"),
        item_row(line_no="4", quantity="3"),
    ])

    assert valid.count() == 1
    assert sorted(_reasons(rejected)) == [
        "invalid_quantity",
        "invalid_unit_price",
        "quantity_above_limit",
    ]


def test_discount_bounds_are_inclusive(prepare_items):
    """0.0 and 1.0 are legal; anything outside the closed interval is not."""
    valid, rejected = prepare_items([
        item_row(line_no="1", discount_pct="0.00"),
        item_row(line_no="2", discount_pct="1.00"),
        item_row(line_no="3", discount_pct="1.50"),
        item_row(line_no="4", discount_pct="-0.01"),
    ])

    assert valid.count() == 2
    assert _reasons(rejected) == ["invalid_discount_pct", "invalid_discount_pct"]


def test_email_and_country_rules(prepare_customers):
    valid, rejected = prepare_customers([
        customer_row(customer_id="C1", email="no-at-sign"),
        customer_row(customer_id="C2", country_code="ZZ"),
        customer_row(customer_id="C3"),
    ])

    assert valid.count() == 1
    assert sorted(_reasons(rejected)) == ["invalid_country_code", "invalid_email"]
