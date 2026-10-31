"""Column contracts.

Source CSVs are read as text and cast explicitly, so this module is the only
place that knows what a column is called, what type it must become, and which
columns are mandatory.
"""
from __future__ import annotations

from typing import Dict, List

from pyspark.sql import types as T

#: Column order per source extract. Order is part of the contract.
SOURCE_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id", "email", "country_code", "segment", "signup_ts", "updated_at"],
    "products": ["sku", "category", "unit_cost", "is_active", "updated_at"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "channel", "currency"],
    "order_items": ["order_id", "line_no", "sku", "quantity", "unit_price", "discount_pct", "updated_at"],
}

#: Nulls in these columns are a reject, not a default.
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id", "email", "country_code", "signup_ts"],
    "products": ["sku", "category", "unit_cost", "is_active"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "currency"],
    "order_items": ["order_id", "line_no", "sku", "quantity", "unit_price", "discount_pct"],
}

#: Natural key of each extract, in the order it appears in a quarantine record_key.
KEY_COLUMNS: Dict[str, List[str]] = {
    "customers": ["customer_id"],
    "products": ["sku"],
    "orders": ["order_id"],
    "order_items": ["order_id", "line_no"],
}

#: The target type of every source column.
CAST_SCHEMAS: Dict[str, T.StructType] = {
    "customers": T.StructType([
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("segment", T.StringType(), True),
        T.StructField("signup_ts", T.TimestampType(), True),
        T.StructField("updated_at", T.TimestampType(), True),
    ]),
    "products": T.StructType([
        T.StructField("sku", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("unit_cost", T.DoubleType(), True),
        T.StructField("is_active", T.BooleanType(), True),
        T.StructField("updated_at", T.TimestampType(), True),
    ]),
    "orders": T.StructType([
        T.StructField("order_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("order_ts", T.TimestampType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
    ]),
    "order_items": T.StructType([
        T.StructField("order_id", T.StringType(), True),
        T.StructField("line_no", T.IntegerType(), True),
        T.StructField("sku", T.StringType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
        T.StructField("discount_pct", T.DoubleType(), True),
        T.StructField("updated_at", T.TimestampType(), True),
    ]),
}

#: Published fact table. Partition column is listed so the write and the checks agree.
FACT_SCHEMA = T.StructType([
    T.StructField("order_date", T.DateType(), False),
    T.StructField("order_id", T.StringType(), False),
    T.StructField("line_no", T.IntegerType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("sku", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("channel", T.StringType(), True),
    T.StructField("currency", T.StringType(), False),
    T.StructField("order_ts", T.TimestampType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("unit_price", T.DoubleType(), False),
    T.StructField("discount_pct", T.DoubleType(), False),
    T.StructField("gross_amount", T.DoubleType(), False),
    T.StructField("discount_amount", T.DoubleType(), False),
    T.StructField("net_amount", T.DoubleType(), False),
    T.StructField("cost_amount", T.DoubleType(), False),
    T.StructField("margin_amount", T.DoubleType(), False),
    T.StructField("margin_pct", T.DoubleType(), True),
    T.StructField("net_revenue_amount", T.DoubleType(), False),
    T.StructField("revenue_band", T.StringType(), True),
    T.StructField("country_code", T.StringType(), True),
    T.StructField("customer_segment", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("product_is_active", T.BooleanType(), True),
    T.StructField("is_late_arriving", T.BooleanType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("source_file", T.StringType(), False),
    T.StructField("ingested_at", T.TimestampType(), False),
])

FACT_COLUMNS: List[str] = [field.name for field in FACT_SCHEMA.fields]

#: One quarantine dataset for all four sources, so one query answers "what did we drop".
QUARANTINE_SCHEMA = T.StructType([
    T.StructField("source_table", T.StringType(), False),
    T.StructField("reject_reason", T.StringType(), False),
    T.StructField("record_key", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("payload", T.StringType(), True),
    T.StructField("raw_row", T.StringType(), True),
    T.StructField("run_id", T.StringType(), False),
])

#: Stable reject vocabulary. Tests pin these strings; do not reword them.
REJECT_REASONS: List[str] = [
    "malformed_row",
    "cast_failure",
    "missing_required",
    "invalid_status",
    "invalid_currency",
    "invalid_quantity",
    "quantity_above_limit",
    "invalid_unit_price",
    "unit_price_above_limit",
    "invalid_discount_pct",
    "invalid_email",
    "invalid_country_code",
    "invalid_unit_cost",
    "future_timestamp",
    "duplicate_exact",
    "duplicate_key_conflict",
    "invalid_reference",
]


def columns_of(schema: T.StructType) -> List[str]:
    """Field names of a StructType, in order."""
    return [field.name for field in schema.fields]
