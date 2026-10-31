"""Validation and shaping of the four source extracts.

Order is deliberate and documented in docs/architecture.md:

    read as text -> contract check -> cast -> business rules -> de-duplicate -> references

Consequences that the tests pin:

* casting before rules means numeric rules compare numbers, not strings;
* rules before de-duplication means a malformed newest version cannot evict a
  valid older version and then disappear itself;
* de-duplication before references means orphans are reported against the
  authoritative dimension snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql import types as T

from . import config as cfg
from .schemas import CAST_SCHEMAS, KEY_COLUMNS, REQUIRED_COLUMNS, SOURCE_COLUMNS

#: Business columns compared when deciding exact-duplicate vs conflicting-duplicate.
DUPLICATE_ORDER: Dict[str, List[str]] = {
    "customers": ["customer_id"],
    "products": ["sku"],
    "orders": ["order_id"],
    "order_items": ["order_id", "line_no"],
}


class SchemaContractError(Exception):
    """A source extract does not match its documented contract."""


@dataclass
class StageResult:
    """Outcome of validating one extract."""

    table: str
    frame: DataFrame
    rejects: DataFrame
    counts: Dict[str, int] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 1. read
# ---------------------------------------------------------------------------
def read_source(spark, path, table: str) -> Tuple[DataFrame, List[str]]:
    """Read one CSV as text and enforce the column contract by name.

    Reading as text (never inferring types) is what makes every later failure
    explainable: nothing is silently coerced on the way in.
    """
    frame = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "")
        .option("emptyValue", "")
        .csv(str(path))
    )

    expected = SOURCE_COLUMNS[table]
    actual = list(frame.columns)
    missing = [name for name in expected if name not in actual]
    if missing:
        raise SchemaContractError(
            table + ": missing required column(s) " + ", ".join(missing)
            + "; found " + ", ".join(actual)
        )

    warnings: List[str] = []
    unexpected = [name for name in actual if name not in expected]
    if unexpected:
        warnings.append(
            table + ": unexpected column(s) ignored: " + ", ".join(unexpected)
        )

    projected = frame.select(*expected)
    payload = F.to_json(F.struct(*[F.col(name).alias(name) for name in expected]))
    enriched = (
        projected
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_raw_json", payload)
    )
    return enriched, warnings


# ---------------------------------------------------------------------------
# 2. cast
# ---------------------------------------------------------------------------
def _is_blank(column: str):
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def cast_source(frame: DataFrame, table: str) -> DataFrame:
    """Cast every column to its contract type, recording the first failure."""
    schema = CAST_SCHEMAS[table]
    reason = F.lit(None).cast("string")
    all_blank = F.lit(True)
    typed = frame

    for field in schema.fields:
        raw = F.col(field.name)
        blank = _is_blank(field.name)

        if isinstance(field.dataType, T.TimestampType):
            value = F.to_timestamp(raw)
        elif isinstance(field.dataType, T.BooleanType):
            lowered = F.lower(F.trim(raw))
            value = (
                F.when(lowered.isin("true", "1", "yes"), F.lit(True))
                .when(lowered.isin("false", "0", "no"), F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
        else:
            value = raw.cast(field.dataType)

        failed = (~blank) & value.isNull()
        reason = F.when(failed, F.lit("cast_failure:" + field.name)).otherwise(reason)
        all_blank = all_blank & blank
        typed = typed.withColumn(field.name, value)

    return typed.withColumn("_cast_reason", reason).withColumn("_all_blank", all_blank)


# ---------------------------------------------------------------------------
# 3. business rules
# ---------------------------------------------------------------------------
def _rule_conditions(table: str, rules: Dict[str, Any], as_of) -> List[Tuple[str, Any]]:
    """Rule order is priority: the first failing rule is the reported reason."""
    if table == "orders":
        tolerance = timedelta(minutes=int(rules["future_tolerance_minutes"]))
        return [
            ("invalid_status", ~F.upper(F.col("status")).isin(*cfg.VALID_STATUSES)),
            ("invalid_currency", ~F.upper(F.col("currency")).isin(*cfg.VALID_CURRENCIES)),
            ("future_timestamp", F.col("order_ts") > F.lit(as_of + tolerance)),
        ]

    if table == "order_items":
        return [
            ("invalid_quantity", F.col("quantity") <= 0),
            ("quantity_above_limit", F.col("quantity") > int(rules["max_quantity"])),
            ("invalid_unit_price", F.col("unit_price") < 0),
            ("unit_price_above_limit", F.col("unit_price") > float(rules["max_unit_price"])),
            (
                "invalid_discount_pct",
                (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1),
            ),
        ]

    if table == "customers":
        allowed = list(rules["allowed_countries"])
        return [
            ("invalid_email", ~F.col("email").contains("@")),
            ("invalid_country_code", ~F.upper(F.col("country_code")).isin(*allowed)),
        ]

    if table == "products":
        return [("invalid_unit_cost", F.col("unit_cost") < 0)]

    return []


def _missing_reason(table: str):
    reason = F.lit(None).cast("string")
    for column in reversed(REQUIRED_COLUMNS[table]):
        reason = F.when(_is_blank(column), F.lit("missing_required:" + column)).otherwise(reason)
    return reason


def apply_rules(frame: DataFrame, table: str, rules: Dict[str, Any], as_of) -> DataFrame:
    """Attach _reject_reason with the documented priority."""
    rule_reason = F.lit(None).cast("string")
    for code, condition in reversed(_rule_conditions(table, rules, as_of)):
        rule_reason = F.when(condition, F.lit(code)).otherwise(rule_reason)

    return frame.withColumn(
        "_reject_reason",
        F.coalesce(
            F.when(F.col("_all_blank"), F.lit("malformed_row")),
            _missing_reason(table),
            F.col("_cast_reason"),
            rule_reason,
        ),
    )


# ---------------------------------------------------------------------------
# 4. de-duplication
# ---------------------------------------------------------------------------
def _row_hash(columns: List[str]):
    return F.sha2(
        F.concat_ws("|", *[F.coalesce(F.col(name).cast("string"), F.lit("<null>")) for name in columns]),
        256,
    )


def deduplicate(frame: DataFrame, table: str) -> Tuple[DataFrame, DataFrame]:
    """Keep the newest version per natural key; classify the losers.

    duplicate_exact    - byte-identical business payload, safe to drop silently
    duplicate_key_conflict - same key, different payload, newest wins, older is kept
                             in quarantine so the conflict is visible
    """
    keys = DUPLICATE_ORDER[table]
    order_by = [F.col("updated_at").desc_nulls_last()] if "updated_at" in frame.columns else []
    if table == "orders":
        order_by = [F.col("order_ts").desc_nulls_last()]
    order_by = order_by + [F.col("_source_file").desc()]

    window = Window.partitionBy(*keys).orderBy(*order_by)
    business = [name for name in SOURCE_COLUMNS[table]]
    hashed = frame.withColumn("_row_hash", _row_hash(business))

    ranked = (
        hashed.withColumn("_rank", F.row_number().over(window))
        .withColumn("_winner_hash", F.first("_row_hash").over(window))
    )

    kept = ranked.filter(F.col("_rank") == 1).drop("_rank", "_winner_hash", "_row_hash")

    losers = (
        ranked.filter(F.col("_rank") > 1)
        .withColumn(
            "_reject_reason",
            F.when(F.col("_row_hash") == F.col("_winner_hash"), F.lit("duplicate_exact"))
            .otherwise(F.lit("duplicate_key_conflict")),
        )
        .drop("_rank", "_winner_hash")
    )
    return kept, losers


# ---------------------------------------------------------------------------
# 5. referential integrity
# ---------------------------------------------------------------------------
def check_reference(child: DataFrame, parent: DataFrame, key: str) -> Tuple[DataFrame, DataFrame]:
    """Split child rows into (has parent, orphan) without a shuffle on the child."""
    known = parent.select(F.col(key).alias(key)).distinct().withColumn("_known", F.lit(True))
    joined = child.join(F.broadcast(known), key, "left")

    orphans = (
        joined.filter(F.col("_known").isNull())
        .drop("_known")
        .withColumn("_reject_reason", F.lit("invalid_reference:" + key))
    )
    valid = joined.filter(F.col("_known").isNotNull()).drop("_known")
    return valid, orphans


# ---------------------------------------------------------------------------
# 6. quarantine shape
# ---------------------------------------------------------------------------
def _record_key(table: str):
    parts = [F.coalesce(F.col(name).cast("string"), F.lit("<null>")) for name in KEY_COLUMNS[table]]
    return F.concat_ws(":", *parts)


def to_quarantine(frame: DataFrame, table: str, run_id: str) -> DataFrame:
    """Project any reject frame onto the single quarantine contract."""
    return frame.select(
        F.lit(table).alias("source_table"),
        F.col("_reject_reason").alias("reject_reason"),
        _record_key(table).alias("record_key"),
        F.col("_source_file").alias("source_file"),
        F.col("_raw_json").alias("payload"),
        F.lit(None).cast("string").alias("raw_row"),
        F.lit(run_id).alias("run_id"),
    )


# ---------------------------------------------------------------------------
# orchestration of the six steps for one table
# ---------------------------------------------------------------------------
def validate_source(spark, path, table: str, config, run_id: str, as_of) -> StageResult:
    """Run steps 1-4 for one extract and return valid rows plus rejects."""
    raw, warnings = read_source(spark, path, table)
    typed = cast_source(raw, table)
    ruled = apply_rules(typed, table, config.rules(), as_of)

    rejected_mask = F.col("_reject_reason").isNotNull()
    rejects = ruled.filter(rejected_mask)
    candidates = ruled.filter(~rejected_mask)

    kept, duplicates = deduplicate(candidates, table)
    rejects = rejects.drop("_rank", "_winner_hash").unionByName(
        duplicates.drop("_rank", "_winner_hash"), allowMissingColumns=True
    )

    counts = {"raw": raw.count(), "valid": kept.count()}
    return StageResult(table=table, frame=kept, rejects=rejects, counts=counts, warnings=warnings)


def categorise(reason_counts: Dict[str, int]) -> Dict[str, int]:
    """Fold the reject vocabulary into the six buckets the reconciliation identity uses."""
    buckets = {"malformed": 0, "missing": 0, "cast": 0, "rules": 0, "duplicates": 0, "orphans": 0}
    for reason, count in reason_counts.items():
        if reason == "malformed_row":
            buckets["malformed"] += count
        elif reason.startswith("missing_required"):
            buckets["missing"] += count
        elif reason.startswith("cast_failure"):
            buckets["cast"] += count
        elif reason.startswith("duplicate_"):
            buckets["duplicates"] += count
        elif reason.startswith("invalid_reference"):
            buckets["orphans"] += count
        else:
            buckets["rules"] += count
    return buckets


def reason_counts(rejects: DataFrame) -> Dict[str, int]:
    """reject_reason -> count, as a plain dict."""
    rows = rejects.groupBy("reject_reason").count().collect()
    return {row["reject_reason"]: int(row["count"]) for row in rows}
