"""Deterministic synthetic source data with labelled defects.

Two artefacts, deliberately kept apart:

    data/raw/<table>/<table>.csv        the input the pipeline sees
    data/reference/expected_labels.csv  what the generator intended, per injected row

Keeping the labels out of the input is what lets the tests assert that the
pipeline found exactly the defects that were planted, instead of asserting that
it rejected "some" rows.
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from .schemas import SOURCE_COLUMNS

#: Event-time origin of the generated window. Fixed so ids and dates are stable
#: across machines and runs; it is not "today", which keeps the fixture pinned.
BASE_TS = datetime(2026, 9, 1, tzinfo=timezone.utc)
WINDOW_DAYS = 14

COUNTRIES = ["US", "DE", "GB", "FR", "CA", "AU", "JP", "IN", "NL", "ES"]
SEGMENTS = ["RETAIL", "SMB", "ENTERPRISE", ""]
CHANNELS = ["WEB", "APP", "STORE", "PARTNER"]
CURRENCIES = ["USD", "EUR", "GBP"]
CATEGORIES = ["apparel", "electronics", "grocery", "home", "outdoor"]


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _money(value: float) -> str:
    return "{0:.2f}".format(value)


# ---------------------------------------------------------------------------
# rows
# ---------------------------------------------------------------------------
def _customers(count: int, rng: random.Random) -> List[Dict[str, Any]]:
    rows = []
    for index in range(1, count + 1):
        customer_id = "C" + str(index).zfill(5)
        signed_up = BASE_TS - timedelta(days=rng.randint(30, 900))
        rows.append({
            "customer_id": customer_id,
            "email": "user" + customer_id.lower() + "@example.com",
            "country_code": rng.choice(COUNTRIES),
            "segment": rng.choice(SEGMENTS),
            "signup_ts": _iso(signed_up),
            "updated_at": _iso(signed_up),
        })
    return rows


def _products(count: int, rng: random.Random) -> List[Dict[str, Any]]:
    rows = []
    for index in range(1, count + 1):
        sku = "SKU" + str(index).zfill(4)
        rows.append({
            "sku": sku,
            "category": rng.choice(CATEGORIES),
            "unit_cost": _money(rng.uniform(4.0, 400.0)),
            "is_active": rng.choice(["true", "true", "true", "false"]),
            "updated_at": _iso(BASE_TS - timedelta(days=rng.randint(1, 60))),
        })
    return rows


def _orders_and_items(
    order_count: int,
    line_target: int,
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    rng: random.Random,
):
    customer_ids = [row["customer_id"] for row in customers]
    cost = {row["sku"]: float(row["unit_cost"]) for row in products}
    skus = list(cost)

    orders = []
    for index in range(1, order_count + 1):
        placed = BASE_TS + timedelta(minutes=rng.randrange(WINDOW_DAYS * 24 * 60))
        orders.append({
            "order_id": "O" + str(index).zfill(7),
            "customer_id": rng.choice(customer_ids),
            "order_ts": _iso(placed),
            "status": rng.choices(
                ["DELIVERED", "SHIPPED", "PAID", "PLACED"], weights=[5, 3, 1, 1]
            )[0],
            "channel": rng.choice(CHANNELS),
            "currency": rng.choices(CURRENCIES, weights=[6, 2, 2])[0],
        })

    # distribute the line budget across orders so the target grain size is exact
    lines_per_order = [1] * order_count
    for _ in range(max(0, line_target - order_count)):
        lines_per_order[rng.randrange(order_count)] += 1

    items = []
    for order, line_count in zip(orders, lines_per_order):
        for line_no in range(1, line_count + 1):
            sku = rng.choice(skus)
            unit_price = round(cost[sku] * rng.uniform(1.15, 1.9), 2)
            items.append({
                "order_id": order["order_id"],
                "line_no": str(line_no),
                "sku": sku,
                "quantity": str(rng.randint(1, 4)),
                "unit_price": _money(unit_price),
                "discount_pct": "{0:.2f}".format(
                    rng.choices(
                        [0.0, 0.05, 0.1, 0.15, 0.2, 0.5], weights=[12, 3, 3, 2, 2, 1]
                    )[0]
                ),
                "updated_at": order["order_ts"],
            })
    return orders, items


# ---------------------------------------------------------------------------
# defect injection
# ---------------------------------------------------------------------------
def _clear(column: str):
    def mutate(row):
        row[column] = ""
    return mutate


def _set(column: str, value: str):
    def mutate(row):
        row[column] = value
    return mutate


ORDER_DEFECTS = [
    ("missing_required:customer_id", "rejected", _clear("customer_id")),
    ("cast_failure:order_ts", "rejected", _set("order_ts", "not-a-timestamp")),
    ("invalid_status", "rejected", _set("status", "PENDING")),
    ("invalid_currency", "rejected", _set("currency", "XYZ")),
    ("invalid_reference:customer_id", "rejected", _set("customer_id", "C9999999")),
    ("void_status", "kept_with_zero_revenue", _set("status", "CANCELLED")),
]

ITEM_DEFECTS = [
    ("invalid_quantity", "rejected", _set("quantity", "0")),
    ("quantity_above_limit", "rejected", _set("quantity", "5000")),
    ("invalid_unit_price", "rejected", _set("unit_price", "-1.00")),
    ("invalid_discount_pct", "rejected", _set("discount_pct", "1.50")),
    ("missing_required:sku", "rejected", _clear("sku")),
    ("invalid_reference:sku", "rejected", _set("sku", "SKU9999")),
]


def _inject(rows, defects, key_of, table: str, labels: List[Dict[str, str]], budget: int) -> int:
    """Plant one defect per stride so counts are predictable, not random."""
    if not rows:
        return 0
    stride = max(3, len(rows) // max(1, budget))
    applied = 0
    index = 0
    while index < len(rows) and applied < budget:
        label, outcome, mutate = defects[applied % len(defects)]
        row = rows[index]
        labels.append({
            "table": table,
            "record_key": key_of(row),
            "label": label,
            "expected_outcome": outcome,
        })
        mutate(row)
        applied += 1
        index += stride
    return applied


def _looks_valid_item(row) -> bool:
    """True when an item row would survive every rule - used to find a duplicate source."""
    try:
        return (
            bool(row.get("sku"))
            and int(row["quantity"]) > 0
            and float(row["unit_price"]) >= 0
            and 0.0 <= float(row["discount_pct"]) <= 1.0
        )
    except (TypeError, ValueError, KeyError):
        return False


def _first_valid_item(items):
    for row in items:
        if _looks_valid_item(row):
            return row
    return None


def _append_version_noise(orders, items, customers, labels: List[Dict[str, str]]) -> None:
    """Duplicates, conflicting versions and late arrivals - the rerun trap set.

    Runs after defect injection and deliberately sources its duplicates from a row
    that is still valid, so the duplicate cases are not masked by an unrelated rule
    failure.
    """
    source = _first_valid_item(items)

    if source is not None:
        # byte-identical duplicate: must collapse to one fact row
        exact = dict(source)
        items.append(exact)
        labels.append({
            "table": "order_items",
            "record_key": exact["order_id"] + ":" + exact["line_no"],
            "label": "duplicate_exact",
            "expected_outcome": "rejected_duplicate",
        })

        # same key, newer timestamp, different payload: newest wins, older quarantined
        conflict = dict(source)
        conflict["quantity"] = str(int(conflict["quantity"]) + 1)
        conflict["updated_at"] = _iso(BASE_TS + timedelta(days=WINDOW_DAYS + 1))
        items.append(conflict)
        labels.append({
            "table": "order_items",
            "record_key": conflict["order_id"] + ":" + conflict["line_no"],
            "label": "duplicate_key_conflict",
            "expected_outcome": "rejected_duplicate",
        })

    valid_order = next(
        (row for row in orders if row.get("order_id") and row.get("customer_id")),
        None,
    )
    if valid_order is not None:
        # a late arrival: event time well before the as-of date, delivered last
        late_ts = _iso(BASE_TS - timedelta(days=30))
        orders.append({
            "order_id": "O9000001",
            "customer_id": valid_order["customer_id"],
            "order_ts": late_ts,
            "status": "DELIVERED",
            "channel": "WEB",
            "currency": "USD",
        })
        items.append({
            "order_id": "O9000001",
            "line_no": "1",
            "sku": source["sku"] if source else "SKU0001",
            "quantity": "2",
            "unit_price": "19.99",
            "discount_pct": "0.00",
            "updated_at": late_ts,
        })
        labels.append({
            "table": "orders",
            "record_key": "O9000001",
            "label": "late_arrival",
            "expected_outcome": "kept",
        })

    if customers:
        # an older version arriving after a newer one: dedupe must pick by timestamp,
        # not by file position
        newest = customers[0]
        stale = dict(newest)
        stale["country_code"] = "JP"
        try:
            stamp = datetime.strptime(newest["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
            stamp = stamp.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            stamp = BASE_TS
        stale["updated_at"] = _iso(stamp - timedelta(days=10))
        customers.append(stale)
        labels.append({
            "table": "customers",
            "record_key": stale["customer_id"],
            "label": "out_of_order_version",
            "expected_outcome": "rejected_duplicate",
        })


# ---------------------------------------------------------------------------
# io
# ---------------------------------------------------------------------------
def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: ("" if row.get(name) is None else row[name]) for name in columns})


def _write_labels(config, labels: List[Dict[str, str]]) -> None:
    columns = ["table", "record_key", "label", "expected_outcome"]
    _write_csv(Path(config.expected_labels_path), columns, labels)


def _sorted(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda row: row[key])


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------
def generate_dataset(
    config,
    rows: int = 2000,
    seed: int = 42,
    inject_defects: bool = True,
) -> Dict[str, Any]:
    """Write the four extracts plus the label file. Deterministic for a given seed."""
    rng = random.Random(seed)

    line_target = max(3, int(rows))
    order_count = max(1, line_target // 3)
    customer_count = max(20, order_count // 3)
    product_count = max(10, order_count // 15)

    customers = _customers(customer_count, rng)
    products = _products(product_count, rng)
    orders, items = _orders_and_items(order_count, line_target, customers, products, rng)

    labels: List[Dict[str, str]] = []
    if inject_defects:
        # slice first: the appended version-noise rows must stay valid on purpose
        clean_orders = orders[:]
        clean_items = items[:]
        _inject(
            clean_orders,
            ORDER_DEFECTS,
            key_of=lambda row: row["order_id"],
            table="orders",
            labels=labels,
            budget=max(len(ORDER_DEFECTS), order_count // 25),
        )
        _inject(
            clean_items,
            ITEM_DEFECTS,
            key_of=lambda row: row["order_id"] + ":" + row["line_no"],
            table="order_items",
            labels=labels,
            budget=max(len(ITEM_DEFECTS), line_target // 40),
        )
        _append_version_noise(orders, items, customers, labels)
        _write_labels(config, labels)
    else:
        stale_labels = Path(config.expected_labels_path)
        if stale_labels.exists():
            stale_labels.unlink()

    _write_csv(Path(config.raw_source("customers")), SOURCE_COLUMNS["customers"], _sorted(customers, "customer_id"))
    _write_csv(Path(config.raw_source("products")), SOURCE_COLUMNS["products"], _sorted(products, "sku"))
    _write_csv(Path(config.raw_source("orders")), SOURCE_COLUMNS["orders"], _sorted(orders, "order_id"))
    _write_csv(
        Path(config.raw_source("order_items")),
        SOURCE_COLUMNS["order_items"],
        sorted(items, key=lambda row: (row["order_id"], int(row["line_no"] or 0))),
    )

    return {
        "seed": seed,
        "rows_requested": line_target,
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        "order_items": len(items),
        "labels": len(labels),
        "defects_injected": bool(inject_defects),
        "raw_root": str(config.raw_dir),
        "labels_path": str(config.expected_labels_path) if inject_defects else None,
    }
