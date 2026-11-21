-- Example analytics on the gold star schema (Databricks SQL; prefix tables with your catalog, e.g. workspace.)

-- 1. Monthly spend by category and customer segment
SELECT year_month, category, segment, txn_count, total_spend, avg_ticket
FROM finance_gold.agg_monthly_category_spend
ORDER BY year_month, total_spend DESC;

-- 2. Top 10 customers by net cash outflow in July 2025
SELECT customer_id, total_debits, total_credits, net_cashflow
FROM finance_gold.agg_customer_monthly_cashflow
WHERE year_month = '2025-07'
ORDER BY net_cashflow ASC
LIMIT 10;

-- 3. Spend attributed to the city a customer lived in AT THE TIME of the transaction (SCD Type 2 at work)
SELECT c.city AS city_at_transaction_time, d.year_month, SUM(f.amount) AS spend
FROM finance_gold.fact_transactions f
JOIN finance_gold.dim_customer c ON f.customer_sk = c.customer_sk
JOIN finance_gold.dim_date d ON f.date_key = d.date_key
WHERE f.txn_type = 'debit' AND f.status = 'posted'
GROUP BY c.city, d.year_month
ORDER BY d.year_month, spend DESC;

-- 4. Reversal rate by channel
SELECT channel, COUNT(*) AS txns, ROUND(AVG(CAST(is_reversed AS INT)) * 100, 2) AS reversal_pct
FROM finance_gold.fact_transactions
GROUP BY channel
ORDER BY reversal_pct DESC;

-- 5. Latest data-quality run
SELECT check_name, table_name, severity, passed, failed_rows, details
FROM finance_ops.dq_results
WHERE run_id = (SELECT max_by(run_id, checked_at) FROM finance_ops.dq_results)
ORDER BY passed, severity;
