# Architecture

## 1. Shape

A single batch job in the classic **raw -> validated -> curated** layering. There is no
message queue and no orchestrator: one process, one `run_id`, one summary file. That is
deliberate for a 72-hour slice — the interesting engineering is in the contracts and the
failure paths, not in the scheduler.

```
conf/pipeline.yaml
        |
   config.Config  ------> every path, threshold and switch
        |
   runner.run_pipeline
        |
        +-- metrics.RunContext ---- run_id, stage timings, counts, failure reason
        |
        +-- validate  --- raw CSV -> typed, rule-checked, de-duplicated frames
        |                   |
        |                   +--> rejects (unified quarantine schema)
        |
        +-- transform --- fact_order_line (baseline or optimized strategy)
        |
        +-- loader    --- dynamic partition overwrite into curated/, quarantine/
        |
        +-- quality   --- assertions + reconciliation, run fails loudly if violated
```

## 2. Layer contracts

| Layer | Input | Output | May fail the run? |
| --- | --- | --- | --- |
| `config` | YAML | typed handle | yes — missing key is a startup error |
| `validate` | raw CSV | typed frames + rejects | yes — missing required column |
| `transform` | typed frames | target frame | yes — plan error, bad join |
| `loader` | frames | parquet on disk | yes — IO error |
| `quality` | curated table | report | yes — critical check fails |

The rule: **a record never fails the run; a contract does.** Bad rows go to quarantine and
the run continues, because one malformed line in a 100k-row extract must not stop the
business day. A missing required column or a broken invariant stops the run, because
publishing a half-understood table is worse than publishing nothing.

## 3. Why this order of validation

```
read as text -> schema contract -> cast -> business rules -> dedupe -> referential
```

Each step needs the previous one to be true:

- Casting before rules, so `quantity <= 0` compares numbers, not strings (`"0"` would
  sort above `"1000"` lexicographically, which is how type bugs hide).
- Rules before de-duplication, so a malformed newest version does not win over a valid
  older one and then get thrown away, losing the customer's good row too.
- De-duplication before referential checks, so an orphan is reported against the
  authoritative dimension snapshot rather than against a stale duplicate.

This order is asserted indirectly by the tests: they pin the exact reject reason for a
record that violates two rules at once (the first rule in the list wins, and the list order
is a public contract documented in `data_dictionary.md`).

## 4. Reject cascade

Order validation runs before order-line validation. Consequences, all intentional:

- A line whose `order_id` was rejected (bad status, missing customer) is quarantined as
  `invalid_reference:order_id`. It is not silently dropped and not counted as revenue.
- A line whose `sku` is unknown is quarantined as `invalid_reference:sku`.
- Therefore `fact rows == valid lines - lines with an invalid parent`, which is exactly
  the identity the reconciliation check asserts.

## 5. Baseline vs. optimized: where the difference actually lives

Both strategies share validation, dimensions and derived columns. Only data movement differs.

| | baseline | optimized |
| --- | --- | --- |
| AQE | off | on (coalesce + skew join) |
| broadcast threshold | `-1` (disabled) | 10 MB |
| dimension joins | shuffle sort-merge | broadcast hash |
| revenue band | Python UDF | native `when` |
| projection | wide frame carried through joins | pruned before the fact-to-fact join |
| output layout | one flat directory, `repartition(200)` | `order_date` partitions, repartitioned once by the write column |

The UDF is the clearest single win to talk about in review: it serialises every row through
Python, blocks Catalyst from pushing anything through it, and yields exactly the same output
as four `when` branches. It stays in the baseline on purpose — removing it is the measured
optimisation.

Broadcasting is safe here because the dimensions are bounded (products by catalogue size,
customers by customer count) while the fact side grows. The guard is the broadcast
threshold plus the fact that `prepare_*_dim` projects dimensions down to the columns the
fact actually needs before the join, so the broadcast payload stays small.

## 6. Partitioning

`fact_order_line` is partitioned by `order_date` (day). Day is the natural predicate in
every analyst query and in every backfill request. Month would mean fewer files but no
partition pruning for the "yesterday" query that dominates traffic; day gives pruning on
the query that matters and keeps a single backfill cheap to rewrite.

Cost: at 100k rows/day this is one small file per day. Below ~1M rows a day the small-file
overhead is negative value, so the config exposes `write.partition_by` and the pipeline
accepts an empty list (the baseline path exercises exactly that).

The optimized write repartitions by `order_date` before writing, so the data is exchanged
once for the write layout instead of twice (repartition, then write-side shuffle).

## 7. Failure semantics

- **Contract error** (missing column, unreadable input): run marked `FAILED` with the
  exception type and message in `failure_reason`; the summary is written before re-raising
  so the failure is observable after the process exits.
- **Partial failure** (crash mid-run): quarantine for that `run_id` may exist, curated may
  be partially written. Retry with the same `run_id` and `--force`: quarantine for the run
  is purged, then rewritten; the curated partitions touched are overwritten in place.
- **Quality failure** (an invariant broke): run marked `FAILED` with the failing check name.
  The data is already written at that point — that is intentional, since the checks read the
  published table rather than an in-memory frame, so they verify what consumers will see.

## 8. Observability

One JSON document per run, written to `data/runs/run_id=<id>/summary.json`:

- `status` (`RUNNING` / `SUCCEEDED` / `FAILED`), `run_id`, `strategy`;
- `started_at`, `finished_at`, `duration_seconds`, per-stage `timings`;
- input counts per table, reject counts per reason, output row count;
- reconciliation identity per table and the post-write check results;
- `failure_reason` and schema-contract warnings.

Because it is a file per run, a downstream scheduler can glob the folder and alert on
`status == FAILED` or on a reject rate over threshold without reading logs.

## 9. Testing strategy

Tests run against a real local Spark session — no mocks of the engine, because the bugs in
this kind of code live in engine semantics (null propagation, window frames, join
cardinality). Each test writes its own CSVs into `tmp_path` and uses a config pointing at
that directory, so tests are independent and parallel-safe.

The end-to-end test is a **contract between the generator and the pipeline**: the generator
labels what it broke, the pipeline reports why it rejected it, and the test asserts the two
sets match. That is what keeps "we inject defects" from decaying into "we inject defects
nobody checks".
