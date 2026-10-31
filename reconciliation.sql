-- Reconciliation statements.
-- src/pipeline/reconcile.py registers the fact table as the temporary view fact_order_line
-- and the quarantine dataset as quarantine, then executes each statement below
-- and prints the result with its name.
--
-- Run them by hand with:  make reconcile

-- 1. Row counts per partition. Cheap sanity check that the run wrote what it claims.
SELECT order_date, COUNT(*) AS rows_written
FROM fact_order_line
GROUP BY order_date
ORDER BY order_date;

-- 2. Daily revenue. This is the number the business will read, so it is the number
--    the test suite pins for a fixed seed.
SELECT order_date,
       ROUND(SUM(net_revenue_amount), 2) AS net_revenue,
       ROUND(SUM(net_amount), 2)         AS order_value,
       COUNT(DISTINCT order_id)          AS orders
FROM fact_order_line
GROUP BY order_date
ORDER BY order_date;

-- 3. Grain check. Must return exactly one row with duplicate_keys = 0.
SELECT COUNT(*) AS duplicate_keys
FROM (
  SELECT order_id, line_no, COUNT(*) AS n
  FROM fact_order_line
  GROUP BY order_id, line_no
  HAVING COUNT(*) > 1
);

-- 4. Rejects by reason. The incident-answer table: what did we drop, and how much.
SELECT reject_reason, COUNT(*) AS rejects
FROM quarantine
GROUP BY reject_reason
ORDER BY rejects DESC;

-- 5. Revenue leak check. Void statuses must never carry revenue.
SELECT COUNT(*) AS void_status_with_revenue
FROM fact_order_line
WHERE status IN ('CANCELLED', 'REFUNDED')
  AND net_revenue_amount <> 0;

-- 6. Band totals. Sum of the three bands must equal total net amount.
SELECT revenue_band,
       COUNT(*)                      AS lines,
       ROUND(SUM(net_amount), 2)     AS net_amount
FROM fact_order_line
GROUP BY revenue_band
ORDER BY revenue_band;

-- 7. Late arrivals. Share of rows whose event date is older than the run as-of date.
SELECT SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END) AS late_rows,
       COUNT(*)                                          AS total_rows,
       ROUND(100.0 * SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END) / COUNT(*), 2) AS late_pct
FROM fact_order_line;

-- 8. Source-to-target identity, checked from the run summary rather than from SQL,
--    because the stage counts are not stored in the fact table:
--    raw = malformed + cast_failures + rule_rejections + duplicates + orphans + valid
SELECT COUNT(*) AS rows_for_latest_run
FROM fact_order_line;

-- 9. Discount sanity. A discount larger than the gross amount means a rule bug.
SELECT COUNT(*) AS discount_above_gross
FROM fact_order_line
WHERE discount_amount > gross_amount OR net_amount < 0;
