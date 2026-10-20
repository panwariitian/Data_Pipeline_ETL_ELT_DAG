-- Mart: delivered orders only, net revenue after discount, per day and store.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, o.store_id, s.store_name, s.zone,
       count(*) AS delivered_orders,
       round(sum(o.order_amount), 2) AS net_revenue,
       round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       round(avg(o.delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN o.delivery_minutes <= 15 THEN 1 ELSE 0 END), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAIL'
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;
