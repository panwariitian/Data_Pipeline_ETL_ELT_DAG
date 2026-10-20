-- Mart: orders per store per day against capacity_orders_per_day, plus its dq_results row (Warning only).
CREATE OR REPLACE TABLE mart_store_capacity AS
SELECT o.order_date, o.store_id, s.store_name, s.capacity_orders_per_day AS capacity,
       count(*) AS orders,
       count(*) > coalesce(s.capacity_orders_per_day, 0) AS over_capacity
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL'
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'capacity.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'capacity.daily_orders_within_capacity', 'Accuracy', 'Warning',
       count(*), count(*) FILTER (WHERE over_capacity),
       CASE WHEN count(*) FILTER (WHERE over_capacity) = 0 THEN 'PASS' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM mart_store_capacity;
