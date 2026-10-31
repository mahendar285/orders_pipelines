"""The transformation, in a baseline and an optimized form.

Both functions return the same fact schema. tests/test_transform.py asserts row
equality, and benchmark.py asserts a checksum before it reports any timing, so
"optimized" is only allowed to be faster, never different.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType

from . import config as cfg

#: Band boundaries, inclusive on the lower edge. Kept in one place so the UDF
#: version and the native version cannot drift apart.
BAND_SMALL_BELOW = 50.0
BAND_MEDIUM_BELOW = 250.0


@F.udf(StringType())
def revenue_band_udf(net_amount):
    """The baseline's mistake, kept on purpose: per-row Python serialisation."""
    if net_amount is None:
        return None
    if net_amount < BAND_SMALL_BELOW:
        return "small"
    if net_amount < BAND_MEDIUM_BELOW:
        return "medium"
    return "large"


def revenue_band(net_amount):
    """The same mapping as the UDF, as native expressions Catalyst can optimise.

    Null in, null out - the UDF and this expression must agree on every input,
    including the one the pipeline never produces, so the equivalence test is a
    real test and not a tautology.
    """
    return (
        F.when(net_amount.isNull(), F.lit(None).cast(StringType()))
        .when(net_amount < BAND_SMALL_BELOW, F.lit("small"))
        .when(net_amount < BAND_MEDIUM_BELOW, F.lit("medium"))
        .otherwise(F.lit("large"))
    )


def prepare_customer_dim(customers: DataFrame) -> DataFrame:
    """Project the dimension before the join so a broadcast payload stays small."""
    return customers.select("customer_id", "country_code", "segment")


def prepare_product_dim(products: DataFrame) -> DataFrame:
    return products.select("sku", "category", "unit_cost", "is_active")


def _measures(frame: DataFrame, as_of, late_arrival_days: int, use_udf: bool) -> DataFrame:
    gross = F.round(F.col("quantity") * F.col("unit_price"), 2)
    discount = F.round(gross * F.col("discount_pct"), 2)
    net = F.round(gross - discount, 2)
    cost = F.round(F.col("quantity") * F.col("unit_cost"), 2)
    margin = F.round(net - cost, 2)

    revenue = (
        F.when(F.col("status").isin(*cfg.VOID_STATUSES), F.lit(0.0)).otherwise(net)
    )
    margin_pct = (
        F.when(net == 0, F.lit(None).cast("double"))
        .otherwise(F.round(margin / net, 4))
    )

    band = revenue_band_udf(net) if use_udf else revenue_band(net)

    late_threshold = as_of - timedelta(days=late_arrival_days)

    return frame.select(
        F.to_date(F.col("order_ts")).alias("order_date"),
        F.col("order_id"),
        F.col("line_no").cast("int").alias("line_no"),
        F.col("customer_id"),
        F.col("sku"),
        F.upper(F.col("status")).alias("status"),
        F.col("channel"),
        F.upper(F.col("currency")).alias("currency"),
        F.col("order_ts"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.col("discount_pct").cast("double").alias("discount_pct"),
        gross.alias("gross_amount"),
        discount.alias("discount_amount"),
        net.alias("net_amount"),
        cost.alias("cost_amount"),
        margin.alias("margin_amount"),
        margin_pct.alias("margin_pct"),
        revenue.alias("net_revenue_amount"),
        band.alias("revenue_band"),
        F.col("country_code"),
        F.upper(F.col("segment")).alias("customer_segment"),
        F.col("category"),
        F.col("is_active").alias("product_is_active"),
        (F.col("order_ts") < F.lit(late_threshold)).alias("is_late_arriving"),
    )


def fact_baseline(
    orders: DataFrame,
    items: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    as_of,
    run_id: str,
    late_arrival_days: int = 7,
) -> DataFrame:
    """Naive strategy: shuffle joins on wide frames, Python UDF for the band."""
    joined = (
        items.join(orders, "order_id", "inner")
        .join(customers, "customer_id", "inner")
        .join(products, "sku", "inner")
    )
    measured = _measures(joined, as_of, late_arrival_days, use_udf=True)
    return measured.withColumn("run_id", F.lit(run_id)).withColumn("ingested_at", F.lit(as_of))


def fact_optimized(
    orders: DataFrame,
    items: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    as_of,
    run_id: str,
    late_arrival_days: int = 7,
) -> DataFrame:
    """Same rows, less data movement.

    Three changes and nothing else:
      1. the fact-to-dimension joins broadcast the (already narrowed) dimensions;
      2. the revenue band is native SQL instead of a Python UDF;
      3. the fact-to-fact join happens after a projection that drops nothing and
         adds nothing - the point is that the wide dimension attributes are not
         carried through it.
    """
    orders_slim = orders.select(
        "order_id", "customer_id", "order_ts", "status", "channel", "currency"
    )
    items_slim = items.select(
        "order_id", "line_no", "sku", "quantity", "unit_price", "discount_pct"
    )

    joined = (
        items_slim.join(F.broadcast(orders_slim), "order_id", "inner")
        .join(F.broadcast(prepare_customer_dim(customers)), "customer_id", "inner")
        .join(F.broadcast(prepare_product_dim(products)), "sku", "inner")
    )
    measured = _measures(joined, as_of, late_arrival_days, use_udf=False)
    return measured.withColumn("run_id", F.lit(run_id)).withColumn("ingested_at", F.lit(as_of))


STRATEGIES: Dict[str, Any] = {
    "baseline": fact_baseline,
    "optimized": fact_optimized,
}
