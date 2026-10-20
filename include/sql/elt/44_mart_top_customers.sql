-- Ranked by delivered spend. Only masked contact fields exist in staging, so nothing raw leaks through here.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT rank() OVER (ORDER BY sum(o.order_amount) DESC) AS spend_rank, o.customer_id, c.loyalty_tier,
       c.email_masked, c.phone_masked, count(*) AS delivered_orders, round(sum(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked ORDER BY 1;
