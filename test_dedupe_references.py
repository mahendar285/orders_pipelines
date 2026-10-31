"""De-duplication and referential integrity."""
from __future__ import annotations

from pipeline import validate

from conftest import item_row, order_row, customer_row, product_row


def test_exact_duplicates_collapse_to_one_row(prepare_items):
    valid, _ = prepare_items([
        item_row(line_no="1"),
        item_row(line_no="1"),
        item_row(line_no="2"),
    ])

    kept, losers = validate.deduplicate(valid, "order_items")

    assert kept.count() == 2
    assert [row["reject_reason"] for row in losers.select("reject_reason").collect()] == ["duplicate_exact"]


def test_conflicting_duplicate_keeps_the_newest_version(prepare_items):
    rows = [
        item_row(line_no="1", quantity="2", updated_at="2026-09-05T10:00:00Z"),
        item_row(line_no="1", quantity="9", updated_at="2026-09-09T10:00:00Z"),
    ]
    valid, _ = prepare_items(rows)

    kept, losers = validate.deduplicate(valid, "order_items")

    assert kept.count() == 1
    assert kept.first()["quantity"] == 9
    assert [row["reject_reason"] for row in losers.select("reject_reason").collect()] == [
        "duplicate_key_conflict"
    ]


def test_out_of_order_dimension_version_keeps_the_newest_by_timestamp(prepare_customers):
    rows = [
        customer_row(country_code="DE", updated_at="2026-09-09T00:00:00Z"),
        customer_row(country_code="JP", updated_at="2026-01-01T00:00:00Z"),
    ]
    valid, _ = prepare_customers(rows)

    kept, _ = validate.deduplicate(valid, "customers")

    assert kept.count() == 1
    assert kept.first()["country_code"] == "DE"


def test_orphan_order_is_quarantined_against_the_dimension(spark, prepare_orders, prepare_customers):
    orders, _ = prepare_orders([order_row(order_id="O1", customer_id="C00001")])
    customers, _ = prepare_customers([customer_row(customer_id="C00001")])

    known, orphans = validate.check_reference(orders, customers, "customer_id")

    assert known.count() == 1
    assert [row["reject_reason"] for row in orphans.select("reject_reason").collect()] == [
        "invalid_reference:customer_id"
    ]


def test_item_for_a_rejected_order_becomes_a_reference_reject(spark, prepare_items, prepare_orders):
    """The cascade the README documents: bad order -> its lines are orphans."""
    orders, _ = prepare_orders([order_row(order_id="O1", status="PENDING")])
    items, _ = prepare_items([item_row(order_id="O1", line_no="1")])

    surviving_orders = orders
    _, orphan_items = validate.check_reference(items, surviving_orders, "order_id")

    assert surviving_orders.count() == 0
    assert orphan_items.count() == 1
    assert [row["reject_reason"] for row in orphan_items.select("reject_reason").collect()] == [
        "invalid_reference:order_id"
    ]


def test_unknown_sku_is_quarantined(spark, prepare_items):
    items, _ = prepare_items([
        item_row(line_no="1", sku="SKU0001"),
        item_row(line_no="2", sku="SKU9999"),
    ])
    products = spark.createDataFrame([product_row(sku="SKU0001")])

    known, orphans = validate.check_reference(items, products, "sku")

    assert known.count() == 1
    assert orphans.count() == 1
    assert [row["reject_reason"] for row in orphans.select("reject_reason").collect()] == [
        "invalid_reference:sku"
    ]
