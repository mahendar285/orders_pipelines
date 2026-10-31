# Order-line revenue pipeline (PySpark)

A small, tested batch pipeline that turns raw order extracts into an analytics-ready
`fact_order_line` table. It validates every record, routes bad ones to a queryable
quarantine zone, makes reruns safe, and measures a naive **baseline** against an
**optimized** Spark job so the optimisation is evidence, not opinion.

Built for the *AI Forward Deployed Engineer — Round 2* challenge. Scope is a complete,
tested vertical slice, not an enterprise platform.

---

## 1. What problem it solves

A retail source system drops four CSV extracts (customers, products, orders, order lines)
into object storage. Analysts need one line-level revenue table they can slice by day,
segment and category, with numbers that reconcile back to the source and with every
dropped record still explainable six weeks later.

Everything below exists to keep three promises:

1. **Correct** — derived money columns are deterministic, and the target row count is
   reconciled against the source row count by an assertion, not by hope.
2. **Traceable** — no row disappears silently. Rejects keep their payload, reason,
   source file and `run_id`.
3. **Repeatable** — the same input run twice produces the same output, and re-running one
   date does not touch the others.

## 2. Dataset (synthetic, reproducible)

Four sources, one grain each:

- `customers` — one row per customer **version**; grain `customer_id` after de-duplication.
- `products` — one row per SKU version; grain `sku`.
- `orders` — one row per order; grain `order_id`.
- `order_items` — one row per order line; grain `(order_id, line_no)`.

Target `fact_order_line` keeps exactly the `order_items` grain, enriched with customer and
product attributes and with revenue/margin measures. Full column contracts are in
[docs/data_dictionary.md](docs/data_dictionary.md).

`src/pipeline/generator.py` produces the data with a fixed seed, including deliberately
broken records. Expected answers live in a **separate** file
(`data/reference/expected_labels.csv`), never inside the input, so the tests can check the
pipeline against the generator's intent.

## 3. Architecture

```
CSV extracts (customers, products, orders, order_items)
        |
        v
[1] read as text .......... malformed rows -> _corrupt_record
        |
        v
[2] schema contract ....... missing column = fatal run error, extra column = warning + drop
        |
        v
[3] cast to domain types .. cast failure -> quarantine (reason cast_failure:<col>)
        |
        v
[4] business rules ........ first failing rule -> quarantine (reason <rule>)
        |
        v
[5] de-duplicate .......... keep newest per natural key, losers -> quarantine
        |
        v
[6] referential checks .... orphans vs. parents -> quarantine (invalid_reference:<key>)
        |
        v
[7] join + derive ......... dims joined, money columns, bands, late-arrival flag
        |
        +--> quarantine/  (reject_reason=..., run_id=...)   parquet
        |
        +--> curated/fact_order_line/order_date=YYYY-MM-DD/ parquet
                        |
                        v
             [8] quality checks + reconciliation + run summary (JSON)
```

Design notes and the why of each step: [docs/architecture.md](docs/architecture.md).

### Module map

- `src/pipeline/config.py` — one YAML, one typed handle; no hard-coded paths.
- `src/pipeline/schemas.py` — every column contract in one file.
- `src/pipeline/session.py` — the two session profiles the benchmark compares.
- `src/pipeline/validate.py` — reads, casts, rules, de-duplication, referential checks.
- `src/pipeline/transform.py` — baseline vs. optimized transformation strategies.
- `src/pipeline/loader.py` — idempotent writers (fact table, quarantine, summaries).
- `src/pipeline/quality.py` — post-write assertions and reconciliation.
- `src/pipeline/metrics.py` — run context: run id, timings, counts, failures.
- `src/pipeline/generator.py` — seeded synthetic data plus the expected-labels file.
- `src/pipeline/benchmark.py` — baseline vs. optimized, gated on a matching checksum.
- `src/pipeline/reconcile.py` — runs the statements in `sql/reconciliation.sql`.
- `src/pipeline/runner.py` — the orchestration, the only place that knows the order of steps.
- `src/pipeline/cli.py` — `generate | run | benchmark | reconcile`.

## 4. Quickstart

Requires Python 3.10+ and a JDK 11/17 (Spark needs one).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make sample     # ~2,000 order lines, seeded, with injected defects
make test       # 32 tests, local Spark, no cluster needed
make run        # optimized run, writes curated + quarantine + run summary
make reconcile  # prints the reconciliation report
```

Without make:

```bash
export PYTHONPATH=src
python -m pipeline generate --rows 2000 --seed 42
python -m pipeline run --strategy optimized
python -m pipeline reconcile
```

Outputs land under `data/`:

```
data/raw/<table>/<table>.csv          generator output (committed for quick review)
data/reference/expected_labels.csv    expected reject reasons, kept out of the input
data/curated/fact_order_line/         partitioned parquet, order_date=YYYY-MM-DD
data/quarantine/run_id=.../           parquet, partitioned inside by reject_reason
data/runs/run_id=.../summary.json     operational record of one run
```

## 5. Baseline vs. optimized

Both strategies run the same validation and produce the **same business rows**; a
checksum assertion in `benchmark` proves it before any timing is reported.

- **baseline** — AQE off, broadcast disabled, no column pruning on the fact-to-fact join,
  Python UDF for the revenue band, one unpartitioned directory, extra `repartition`.
- **optimized** — AQE on, small dimensions broadcast, early filter + projection, native
  `when` instead of the UDF, output partitioned by `order_date`, co-partitioned write.

`make benchmark` (100k line items by default, via `src/pipeline/benchmark.py`) prints a
table with wall-clock time per
strategy, stage timings, shuffle partition count, join types found in the optimized plan,
and the output checksum. See [docs/run_evidence.md](docs/run_evidence.md) for the shape of
that output and how to reproduce it on your machine.

## 6. Data quality and quarantine

Quarantine is one dataset (written to `data/quarantine/run_id=<id>/`, partitioned
inside by `reject_reason`) with a common schema — `source_table`, `reject_reason`,
`record_key`, `source_file`, `payload` (JSON of the original record), `raw_row`,
`run_id` — so a single query answers "what did we drop and why". Reject reasons are stable
strings suitable for alerting:

`malformed_row`, `cast_failure:<column>`, `missing_required:<column>`, `invalid_status`,
`invalid_currency`, `invalid_quantity`, `quantity_above_limit`, `invalid_unit_price`,
`unit_price_above_limit`, `invalid_discount_pct`, `invalid_email`, `invalid_country_code`,
`invalid_unit_cost`, `future_timestamp`, `duplicate_exact`, `duplicate_key_conflict`,
`invalid_reference:<key>`.

Rejections cascade in a documented order: a line whose parent order was rejected becomes
`invalid_reference:order_id` rather than silently surviving.

## 7. Idempotency, retry and backfill

- Writes use `partitionOverwriteMode=dynamic`, so a rerun replaces exactly the
  `order_date` partitions it produced.
- A rerun with the same `run_id` is a no-op unless `--force` is passed; the previous
  summary is returned instead of duplicating work.
- Quarantine for a `run_id` is purged before rewriting, so a retried run cannot leave two
  generations of rejects behind.
- Backfilling one date re-runs that date's slice only; other partitions are untouched.
- `--full-refresh` deletes the curated table for a clean rebuild (use when the
  transformation itself changed).

## 8. Testing

`make test` runs 32 tests on a local Spark session (about 60–90 s):

- happy path, amounts, rounding, band boundaries;
- empty input, malformed rows, missing required fields, wrong dtypes;
- exact and conflicting duplicates; out-of-order dimension versions;
- unknown reference keys and items belonging to a rejected parent order;
- late-arriving orders; rerun idempotency; single-date backfill;
- generator labels reconciled against quarantine; count reconciliation asserted.

## 9. Reconciliation

`make reconcile` registers the curated and quarantine views and runs the named statements
in `sql/reconciliation.sql`: row counts per `order_date`, daily net revenue, rejects by
reason, duplicate-key check, revenue by band, a void-status revenue leak check, late-arrival
share, and a grain check. Nothing here is a screenshot — it is re-runnable output.

## 10. Assumptions

- All timestamps are UTC ISO-8601 (`yyyy-MM-dd'T'HH:mm:ss'Z'`); no local-time conversion.
- Money is `double` at source; rounding happens once per derived column to 2 dp. A
  production system should use `decimal(18,2)`.
- One currency per order, already normalised upstream — no FX conversion in scope.
- `CANCELLED` and `REFUNDED` lines are **retained** for audit with
  `net_revenue_amount = 0`; they are excluded from revenue, not from history.
- Dimension "latest version" is chosen by `updated_at`; ties break on source file name.
  This is a latest-version snapshot, not a full SCD2 history (see next steps).
- The pipeline owns the curated path; no other writer touches it.

## 11. Known limitations

- Plain parquet, so rerun safety comes from dynamic partition overwrite rather than a
  transactional table. Delta Lake / Iceberg would remove the purge workarounds.
- Counting between stages costs extra passes (each `count()` is a Spark job). Fine at
  these volumes, worth replacing with accumulator-free metrics or a persisted cache at 10x.
- The benchmark measures wall-clock and plan shape, not shuffle bytes; exact shuffle
  read/write figures come from the Spark UI at `localhost:4040` for the same run.
- Local mode only out of the box. Paths are config-driven, so pointing `paths.*` at a
  bucket is a config change, but the `shutil` purge in `--full-refresh` is filesystem-only.
- No SCD2 history, no FX, no streaming. Named deliberately — see
  [docs/limitations_and_next_steps.md](docs/limitations_and_next_steps.md).

## 12. Naming conventions

- `snake_case` everywhere; tables singular and explicit (`fact_order_line`).
- Stable deterministic business keys only (`customer_id`, `sku`, `order_id`, `line_no`)
  — no surrogate keys generated inside the pipeline.
- Timestamps end in `_ts` (UTC, ISO-8601); dates end in `_date`; amounts end in
  `_amount`; ratios end in `_pct` and are stored as fractions (0.15 = 15%).
- Provenance columns are explicit and not underscore-prefixed in the target:
  `run_id`, `source_file`, `ingested_at`. Helper columns used during processing are
  prefixed with `_` and always dropped before the write.
- Reject reasons are `snake_case`, `<rule>` or `<rule>:<column>`, and never reworded
  between releases (tests pin them).

## 13. Challenge coverage

Every numbered implementation step maps to something runnable:

1. README + data dictionary + architecture — this file and `docs/`.
2. Core pipeline with modules and configurable paths — `src/pipeline/`, `conf/pipeline.yaml`.
3. Schema validation, rules, duplicates, quarantine — `src/pipeline/validate.py`.
4. Logging and run metrics — `src/pipeline/metrics.py`, `data/runs/run_id=*/summary.json`.
5. Automated tests, six edge-case families — `tests/`.
6. Idempotent load, retry and backfill behaviour — `src/pipeline/loader.py`, section 7.
7. Reproducible sample data with labels kept apart — `src/pipeline/generator.py`.
8. Output correctness with reconciliation queries — `sql/reconciliation.sql`,
   `src/pipeline/reconcile.py`.
9. Performance measurement and one optimisation — `src/pipeline/benchmark.py`, section 5.
10. Source, tests, sample data, docs, trade-offs — the repository itself plus
    `docs/design_tradeoffs.md`.
