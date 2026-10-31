# Design and trade-offs

Short version: this is a **vertical slice**. Every decision below was taken to make one
promise provable — reconciliation, traceability, or repeatability — and each one names what
it costs.

## 1. Plain parquet + dynamic partition overwrite instead of Delta

**Chosen:** parquet with `spark.sql.sources.partitionOverwriteMode=dynamic`, plus an explicit
purge of a run's quarantine before rewriting it.

**Why:** no extra dependency, no transaction log to reason about in a 72-hour build, and the
idempotency argument stays visible in the code rather than delegated to a format. Dynamic
overwrite gives exactly the rerun property the brief asks for: rewrite the partitions this
run produced, leave the rest alone.

**Cost, stated honestly:** overwrite is partition-level, not row-level. If a rerun of the
same `run_id` produces *fewer* partitions than the failed attempt, stale files for that run
can survive. `--full-refresh` exists for that case, and the limitation is written down
rather than papered over. Delta `MERGE` on `(order_id, line_no)` removes the whole class of
problem — it is the first next step.

## 2. Partition by `order_date` (day), not by month, not unpartitioned

**Why:** every analyst query filters a date range starting with "yesterday", and every
backfill request is a date. Day-level partitions give pruning where it matters and make a
backfill a small rewrite.

**Cost:** more, smaller files. At the 100k-row sample this is a net loss on raw throughput;
it is the right call from roughly a million rows a day upward. The config exposes
`write.partition_by` (and the baseline path exercises the unpartitioned variant), so the
choice is a parameter, not a hard-coded opinion.

Partitioning by `customer_id` was considered and rejected: high cardinality, and the
dominant predicate is time, not customer.

## 3. Broadcast the dimensions, shuffle the facts

**Why:** the fact side grows with orders while `products` is bounded by the catalogue and
`customers` by the customer base. A broadcast hash join removes a full shuffle of the fact
side per dimension — two shuffles in the baseline become zero.

**Cost:** a broadcast that silently grows (a runaway catalogue pull) breaks the driver. Two
defences: `prepare_product_dim` / `prepare_customer_dim` project dimensions down to the
columns the fact needs before the join, and `spark.sql.autoBroadcastJoinThreshold` is
configured rather than left at a default nobody reads.

The fact-to-fact join (lines to orders) is **not** broadcast: orders scale with lines. That
one stays a sort-merge join, and the benchmark plan output shows exactly that split — which
is the point of printing the plan instead of claiming a win.

## 4. Remove the Python UDF

**Why:** the baseline computes the revenue band with a `udf(...)`, which is the most common
performance mistake in PySpark notebooks: every row is serialised to a Python worker,
Catalyst treats the call as a black box and cannot push predicates or combine projections
through it, and the column type disappears from the plan.

**Cost:** nothing — the four-branch `when` is the same function with the same boundaries, and
the tests assert both strategies produce identical rows. Keeping the UDF in the baseline and
deleting it in the optimized path turns "UDFs are slow" from folklore into a measured delta.

## 5. Quarantine as one dataset with a JSON payload

**Chosen:** a common quarantine schema across all four sources, with the original record as
JSON in `payload`.

**Why:** one query answers "what did we drop and why", across sources, without a union of
four differently-shaped tables. `reject_reason` is a partition column so a dashboard is a
directory listing.

**Cost:** the payload is not directly queryable with column semantics (you need
`from_json` or string matching), and it duplicates bytes. The alternative — four typed
quarantine tables — is friendlier to point queries and much worse for the question that
actually gets asked in an incident: "how many records did we drop and for what reasons".

## 6. Counting between stages (extra passes) for reconciliation

**Why:** the brief asks for source-to-target reconciliation that is *checked*, not asserted
in prose. Counting after every stage makes the identity
`raw = corrupt + cast + rules + duplicates + orphans + valid` testable arithmetic.

**Cost:** each `count()` is a Spark job. At 100k–1M rows this is a couple of seconds; at 10x
it is real money. The mitigation to reach for first is a cached validated frame reused across
counts, then accumulators or a single `agg` per stage. Called out rather than hidden.

## 7. Fail the run on a contract error, not on a bad record

**Why:** a single malformed line must not stop the business day, but publishing a table built
on a source whose schema silently changed is worse than publishing nothing. Missing required
column = exception; unexpected extra column = warning recorded in the run summary and the
column is dropped. That asymmetry is the whole policy.

## 8. Tests against a real local Spark session

**Why:** the failure modes here are engine semantics — null propagation in `when`, window
frame defaults, join cardinality, timestamp parsing. Mocking Spark would test the mocks.

**Cost:** the suite takes ~60–90 s rather than ~2 s, and needs a JVM. Acceptable: correctness
is 30% of the evaluation and CI runners have a JDK.

## 9. Latest-version dimension, not full history

**Chosen:** dimension versions are de-duplicated to the newest `updated_at` before the join,
so a line is attributed to the customer/product attributes as of the *load*, not as of the
order date.

**Why:** a point-in-time join is a much larger design (bitemporal dimensions, effective-date
ranges, a second reconciliation axis). It is the right answer for finance, and the wrong
answer for a 72-hour slice that has to be understood by a reviewer in ten minutes.

**Cost, and the fix:** historical attribution is wrong if a customer changes segment.
Change history is preserved on the source side (versions are quarantined, not overwritten)
and the next step is a proper SCD2 dimension with `valid_from`/`valid_to` and an as-of join.
Named, sized, not hand-waved.
