# Data dictionary

## Conventions

- Types are Spark SQL types. Source CSVs are all read as text and cast explicitly; a cast
  failure is a reject, never a silent null.
- `NOT NULL` below means "required by contract": a null or empty value is quarantined with
  `missing_required:<column>`.
- Timestamps are UTC ISO-8601 `yyyy-MM-dd'T'HH:mm:ss'Z'`.
- `_source_file` and `_corrupt_record` are the only columns the pipeline adds to source
  frames; every other helper column starts with `_` and is dropped before writing.

---

## Source: `customers`

Grain: one row per customer **version**. Natural key `customer_id`. Multiple versions are
allowed; the newest `updated_at` wins and older ones are quarantined as
`duplicate_key_conflict`.

- **customer_id** (string, NOT NULL) — stable business key, `C00001` style.
- **email** (string, NOT NULL) — must contain `@`; otherwise `invalid_email`. Not used as a key.
- **country_code** (string, NOT NULL) — ISO-3166 alpha-2, exactly 2 chars.
- **segment** (string, nullable) — `RETAIL`, `SMB`, `ENTERPRISE`; upper-cased, nulls kept
  (a customer without a segment is valid; the fact exposes `customer_segment` as null).
- **signup_ts** (timestamp, NOT NULL) — contract field; informational in the target.
- **updated_at** (timestamp, nullable) — version ordering only. Never published.

## Source: `products`

Grain: one row per SKU **version**. Natural key `sku`.

- **sku** (string, NOT NULL) — `SKU0001` style.
- **category** (string, NOT NULL) — free text; `UNCATEGORISED` is a valid value, null is not.
- **unit_cost** (double, NOT NULL) — cost of one unit; `>= 0` else `invalid_unit_cost`.
- **is_active** (boolean, NOT NULL) — cast from `true`/`false`/`1`/`0`; null after cast
  means the flag was missing or unparseable and the SKU version is quarantined.
- **updated_at** (timestamp, nullable) — version ordering.

## Source: `orders`

Grain: one row per order. Natural key `order_id`.

- **order_id** (string, NOT NULL) — `O0000001` style.
- **customer_id** (string, NOT NULL) — FK to `customers.customer_id`.
- **order_ts** (timestamp, NOT NULL) — must not be more than 15 minutes in the future
  (`future_tolerance_minutes`), otherwise `future_timestamp`.
- **status** (string, NOT NULL) — one of `PLACED, PAID, SHIPPED, DELIVERED, CANCELLED, REFUNDED`
  (case-insensitive, upper-cased in the target).
- **channel** (string, nullable) — `WEB`, `APP`, `STORE`, `PARTNER`. Nullable on purpose:
  legacy orders predate the field.
- **currency** (string, NOT NULL) — one of `USD, EUR, GBP, CAD, AUD, JPY`.

## Source: `order_items`

Grain: one row per order line. Natural key `(order_id, line_no)` — the grain of the target.

- **order_id** (string, NOT NULL) — FK to `orders.order_id`.
- **line_no** (int, NOT NULL, > 0 implied by uniqueness) — line ordinal within the order.
- **sku** (string, NOT NULL) — FK to `products.sku`.
- **quantity** (int, NOT NULL) — `> 0` else `invalid_quantity`; `> max_quantity (1000)` else
  `quantity_above_limit` (a real data-entry error pattern: someone typed a count into a
  price field).
- **unit_price** (double, NOT NULL) — `>= 0`; `> max_unit_price (100000)` rejected.
- **discount_pct** (double, NOT NULL) — fraction in `[0, 1]`. `1.0` is legal (100% discount,
  net zero) and is the case that makes `margin_pct` null.
- **updated_at** (timestamp, nullable) — for a corrected line re-sent by the source system.

---

## Target: `fact_order_line`

Grain: one row per `(order_id, line_no)`. Unique and NOT NULL on that pair — asserted after
the write. Partitioned by `order_date`.

### Keys and context

- **order_date** (date, NOT NULL) — `to_date(order_ts)`; partition column.
- **order_id** (string, NOT NULL) — FK to `orders`.
- **line_no** (int, NOT NULL).
- **customer_id** (string, NOT NULL) — FK to `customers`.
- **sku** (string, NOT NULL) — FK to `products`.
- **status** (string, NOT NULL) — upper-cased order status.
- **channel** (string, nullable).
- **currency** (string, NOT NULL).
- **order_ts** (timestamp, NOT NULL) — event time; preserved, never truncated in place.

### Measures (all `double`, rounded to 2 dp once each)

- **quantity** (int) — as received.
- **unit_price** (double) — as received.
- **discount_pct** (double) — fraction.
- **gross_amount** = `round(quantity * unit_price, 2)`
- **discount_amount** = `round(gross_amount * discount_pct, 2)`
- **net_amount** = `round(gross_amount - discount_amount, 2)`
- **cost_amount** = `round(quantity * unit_cost, 2)`
- **margin_amount** = `round(net_amount - cost_amount, 2)` — may be negative; that is data, not an error.
- **margin_pct** = `round(margin_amount / net_amount, 4)`, **null when net_amount is 0**.
- **net_revenue_amount** = `net_amount` except `0.0` for `CANCELLED` / `REFUNDED`.
  Use this column for revenue; use `net_amount` for order value.
- **revenue_band** (string) — `small` below 50, `medium` below 250, else `large`, on
  `net_amount`. Boundaries are inclusive on the lower edge (`net_amount = 50` is `medium`).

### Dimension attributes

- **country_code** (string) — from `customers`.
- **customer_segment** (string, nullable) — upper-cased `segment`.
- **category** (string) — from `products`.
- **product_is_active** (boolean) — snapshot value at load time.

### Provenance

- **run_id** (string, NOT NULL) — the run that produced the row.
- **source_file** (string, NOT NULL) — the `order_items` file the line came from.
- **ingested_at** (timestamp, NOT NULL) — run start time, UTC.
- **is_late_arriving** (boolean, NOT NULL) — `order_ts` is more than `late_arrival_days` (7)
  before the run's as-of time. Late rows are **kept**, flagged, and land in the correct
  `order_date` partition — that is the whole point of partitioning by event date.

---

## Quarantine dataset

Written to `data/quarantine/run_id=<id>/`, partitioned inside by `reject_reason`, so a
rerun can purge exactly its own rejects.

- **source_table** (string) — which extract the record came from.
- **reject_reason** (string) — stable snake_case code (list in the README).
- **record_key** (string) — business key of the rejected record, pipe-joined for
  composite keys, so a support question can be answered with one filter.
- **source_file** (string) — file the record came from.
- **payload** (string) — JSON of the original record columns.
- **raw_row** (string, nullable) — the literal CSV line when the row could not be parsed.
- **run_id** (string) — the run that rejected it.

Retention: never deleted by the pipeline. A reject that disappears is a lie; the row is
re-processed on the next run only if the source corrects it, and then the corrected record
is a *new* row in the fact table with the current `run_id`.

## Invariants (asserted after every write)

- `(order_id, line_no)` unique.
- `order_id`, `customer_id`, `sku`, `net_amount`, `order_date` non-null.
- `discount_amount >= 0` and `net_amount <= gross_amount`.
- `status in (CANCELLED, REFUNDED) => net_revenue_amount = 0`.
- `count(fact rows for run) == valid_lines - orphan_lines`.
- Every `order_date` present in the fact table was derived from `order_ts`.
