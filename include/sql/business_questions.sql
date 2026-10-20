-- Q1a. Daily net revenue by zone (delivered orders, after discount), latest 5 days
-- ETL
SELECT order_date, zone, delivered_orders, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date DESC, zone LIMIT 20;
-- ELT
SELECT order_date, zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 20;

-- Q1b. Daily net revenue by store, latest day
-- ETL
SELECT order_date, store_id, store_name, zone, net_revenue FROM etl_daily_store_kpis WHERE order_date = (SELECT max(order_date) FROM etl_daily_store_kpis) ORDER BY store_id;
-- ELT
SELECT order_date, store_id, store_name, zone, net_revenue FROM mart_daily_store_kpis WHERE order_date = (SELECT max(order_date) FROM mart_daily_store_kpis) ORDER BY store_id;

-- Q2. Revenue and gross margin by category
-- ETL
SELECT category, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC;
-- ELT
SELECT category, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % delivered within 15 minutes
-- ETL
SELECT store_id, round(avg(delivery_minutes), 2) AS avg_delivery_minutes, round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1;
-- ELT
SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1;

-- Q4a. Revenue by payment mode
-- ETL
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;
-- ELT
SELECT payment_mode, delivered_orders, net_revenue FROM mart_payment_mode_revenue ORDER BY 3 DESC;

-- Q4b. Amount that does not reconcile with the payment gateway, by reason
-- ETL
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC;
-- ELT
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC;

-- Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)
-- ETL
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank, f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked, round(sum(f.net_revenue), 2) AS total_spend FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED' GROUP BY f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked ORDER BY 1 LIMIT 10;
-- ELT
SELECT spend_rank, customer_id, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers WHERE spend_rank <= 10 ORDER BY 1;

-- Q6. Rows rejected in the latest run, by file and reason
-- ETL
SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM etl_quarantine WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_pradeep' ORDER BY end_time DESC LIMIT 1)) GROUP BY 1, 2 ORDER BY 1, 3 DESC;
-- ELT
SELECT source_file, failure_reason, rows_rejected FROM mart_rejections WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_pradeep' ORDER BY end_time DESC LIMIT 1) ORDER BY 1, 3 DESC;
