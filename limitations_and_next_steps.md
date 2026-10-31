# Limitations and next steps

## Limitations

1. **Batch only.** No streaming, no incremental watermark. The unit of work is "a set of
   CSVs and a run", which is what the brief asks for and what most first pipelines look like.
2. **No SCD2.** Dimensions are collapsed to the latest version before the join. Historical
   attribution (segment at order time) is not modelled. Change history survives only in
   quarantine, which is not a dimension.
3. **No transactional table.** Rerun safety is partition-level dynamic overwrite. A rerun
   that produces fewer partitions than a failed attempt needs `--full-refresh`.
4. **`double` money.** Fine for the exercise, wrong for a ledger. `decimal(18,2)` with
   explicit `HALF_UP` rounding is the production answer.
5. **Wall-clock performance evidence.** The benchmark reports elapsed time, stage timings,
   shuffle partition count and the join types in the optimized plan. Shuffle bytes and spill
   are read from the Spark UI for the same run, not captured programmatically.
6. **Extra passes for counts.** Reconciliation counts cost one Spark job per stage.
7. **Local filesystem assumptions.** `--full-refresh` uses `shutil.rmtree`; on object storage
   that path needs the storage SDK instead.
8. **No orchestration, no alerting.** A scheduler (Airflow / Dagster / cron) would run the
   CLI and read `summary.json`. The contract for that already exists; the wiring does not.

## Next steps, in the order I would take them

1. **Delta Lake + `MERGE` on `(order_id, line_no)`.** Turns rerun safety from a documented
   workaround into a format guarantee, gives time travel for free, and removes the quarantine
   purge. Half a day.
2. **SCD2 customer and product dimensions.** `valid_from` / `valid_to`, an as-of join on
   `order_ts`, and a test that a segment change does not rewrite last quarter's revenue.
3. **Incremental ingestion.** Land `(source_file, file_hash)` in an ingestion ledger, skip
   files already processed, and derive the run's date window from the file manifest instead of
   scanning everything. This is the change that makes the job cheap to run hourly.
4. **`decimal` money end to end** plus a property test that `sum(net_amount)` over a date
   equals the independently computed sum from source lines.
5. **Replace stage counts with accumulators** and one `agg()` per stage, then re-run the
   benchmark to show the reconciliation tax dropping.
6. **Data-quality gate as code.** Move the reject-rate thresholds into config and fail the run
   when a reason exceeds budget (e.g. >0.5% `invalid_reference:sku` means the product extract
   lagged — that is a paging alert, not a data problem).
7. **Spark UI capture in CI** so performance regressions (a join flipping from broadcast to
   sort-merge) fail a build rather than a review.
