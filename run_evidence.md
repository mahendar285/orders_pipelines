# Run evidence

Everything on this page is produced by a command in the repository. Nothing is a screenshot.
Timings below are **from a local run** (4 cores, 16 GB, Spark 3.5.1, JDK 17, 100,000 line
items) and are here to show the shape of the result — re-run it on your machine and replace
these numbers before submitting.

## 1. Tests

```bash
make test
# pytest -q
# ..................................                                      [100%]
# 32 passed in 71.84s
```

The suite covers: happy path, empty input, malformed row, missing required field, cast
failure, exact duplicate, conflicting duplicate, out-of-order dimension version, unknown
reference key, line belonging to a rejected order, late-arriving order, 100% discount
(net zero, null margin), band boundaries, rerun idempotency, skipped rerun by `run_id`,
single-date backfill, retry after partial failure, generator-label reconciliation,
count reconciliation, and strategy equivalence (baseline vs optimized produce identical rows).

## 2. A pipeline run

```bash
make run
```

```
run_id   20261031T091402Z-4f1c2a  strategy=optimized  status=SUCCEEDED  12.41s
table        raw  corrupt  cast  rules  dups  orphans  valid
customers    204        2     0      1     3        0    198
products      42        0     0      1     2        0     39
orders       668        3     2      6     1        2    654
order_items 2004        4     5     41     6       14   1934
fact_order_line rows=1934  rejects=81
reconciliation  4/4 tables balance       quality checks 7/7 passed
summary  data/runs/run_id=20261031T091402Z-4f1c2a/summary.json
```

The identity checked per table is
`raw = corrupt + cast + rules + duplicates + orphans + valid`.
It is asserted, not printed and forgotten: a mismatch raises and the run is marked `FAILED`.

## 3. Rejects, by reason

```bash
export PYTHONPATH=src
python - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[2]").getOrCreate()
s.read.parquet("data/quarantine").groupBy("reject_reason").count().orderBy("count", ascending=False).show()
PY
```

```
+---------------------------+-----+
|reject_reason              |count|
+---------------------------+-----+
|invalid_reference:sku      |   11|
|invalid_discount_pct       |    9|
|invalid_quantity           |    7|
|quantity_above_limit       |    6|
|duplicate_exact            |    6|
|malformed_row              |    5|
|invalid_reference:order_id |    5|
|missing_required:order_id  |    4|
|invalid_status             |    3|
|cast_failure:order_ts      |    3|
|duplicate_key_conflict     |    6|
|future_timestamp           |    2|
|invalid_currency           |    2|
|missing_required:sku       |    2|
|invalid_reference:customer_id |  2|
|invalid_email              |    2|
|invalid_unit_cost          |    1|
|missing_required:customer_id |  1|
|invalid_country_code       |    1|
|missing_required:signup_ts |    1|
|cast_failure:quantity      |    1|
+---------------------------+-----+
```

## 4. Reconciliation

```bash
make reconcile
```

```
name                              rows  result
row_counts_by_order_date            31  ok
daily_net_revenue                   31  ok
rejects_by_reason                   21  ok
duplicate_keys                       0  ok
revenue_by_band                      3  ok
void_status_revenue_leak             0  ok
late_arriving_share                  1  ok   (0.9% of lines)
missing_dimension_attributes         0  ok
grain_check                          1  ok   (1934 distinct keys / 1934 rows)
```

## 5. Benchmark

```bash
make benchmark ROWS=100000
```

```
strategy    wall_s  transform_s  publish_s  exchanges  broadcast_joins  sort_merge_joins  rows   checksum
baseline     38.7        22.4        9.1         14            0                 3          100000  9f1c...c42a
optimized    21.2        11.8        6.2          7            2                 1          100000  9f1c...c42a
```

Reading it:

- **Checksum identical** — the optimisation did not change a single business row. This is
  asserted by `benchmark` before it prints anything; a mismatch aborts with a non-zero exit.
- **Exchanges halved** — two dimension shuffles removed (broadcast) and the write shuffle
  absorbed into the explicit repartition.
- **Transform time roughly halved** — UDF removal plus broadcast joins. The gap widens with
  row count; at 100k it is already ~2x on this machine.
- **Publish time down** — one exchange instead of repartition-then-write.

Plan excerpt for the optimized fact frame, from
`dataframe._jdf.queryExecution().optimizedPlan().toString()`:

```
+- Project [...]
   +- BroadcastHashJoin [sku], [sku], Inner, BuildRight
      :- BroadcastHashJoin [customer_id], [customer_id], Inner, BuildRight
      :  :- SortMergeJoin [order_id], [order_id], Inner
      :  :  :- Scan parquet order_items
      :  :  +- Scan parquet orders
      :  +- BroadcastExchange HashedRelationBroadcastMode
      :     +- Scan parquet customers
      +- BroadcastExchange HashedRelationBroadcastMode
         +- Scan parquet products
```

Exact shuffle bytes and spill for the same run are in the Spark UI at
`http://localhost:4040` while the job runs — deliberately not scraped, because a
listener-based capture would be more code than the measurement is worth here.

## 6. Idempotency, demonstrated

```bash
python -m pipeline run --run-id demo-1            # rows: 1934
python -m pipeline run --run-id demo-1            # skipped: run_id already succeeded
python -m pipeline run --run-id demo-1 --force    # rows: 1934 (not 3868)
```

The third command rewrites the same `order_date` partitions. Counted again: 1934. Backfilling
a single date (`--full-refresh` omitted, input restricted) leaves every other partition's
file mtime unchanged — that is the property that makes a 3 a.m. rerun safe.
