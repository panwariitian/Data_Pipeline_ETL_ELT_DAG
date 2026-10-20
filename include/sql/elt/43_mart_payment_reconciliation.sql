-- One row per order/payment mismatch, with the reason; plus revenue by payment mode.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH pay AS (
    SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY source_row) AS rn FROM stg_payments) WHERE rn = 1
),
feed AS (SELECT max(payment_ts) AS feed_end FROM pay WHERE payment_ts <= TIMESTAMP '{{now_ist}}'),
clean AS (SELECT * FROM stg_orders WHERE dq_status <> 'FAIL'),
order_side AS (
    SELECT o.order_id, p.payment_id, o.order_status, p.payment_status, o.order_amount, p.amount_paid,
        CASE
          WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL AND o.order_ts > f.feed_end THEN 'DELIVERED_PAYMENT_NOT_YET_RECEIVED'
          WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL THEN 'DELIVERED_NO_PAYMENT'
          WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'FAILED' THEN 'DELIVERED_PAYMENT_FAILED'
          WHEN o.order_status = 'DELIVERED' AND p.payment_status = 'REFUNDED' THEN 'DELIVERED_BUT_REFUNDED'
          WHEN o.order_status = 'DELIVERED' AND abs(p.amount_paid - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
          WHEN o.order_status = 'RETURNED' AND p.payment_id IS NULL THEN 'RETURNED_NO_PAYMENT_RECORD'
          WHEN o.order_status = 'RETURNED' AND p.payment_status = 'SUCCESS' THEN 'RETURNED_NOT_REFUNDED'
          WHEN o.order_status = 'RETURNED' AND abs(p.amount_paid - o.order_amount) > 0.01 THEN 'AMOUNT_MISMATCH'
          WHEN o.order_status = 'CANCELLED' AND p.payment_id IS NOT NULL THEN 'CANCELLED_BUT_PAID'
        END AS mismatch_reason
    FROM clean o LEFT JOIN pay p ON p.order_id = o.order_id CROSS JOIN feed f
),
payment_side AS (
    SELECT p.order_id, p.payment_id, CAST(NULL AS VARCHAR) AS order_status, p.payment_status,
           CAST(NULL AS DOUBLE) AS order_amount, p.amount_paid,
           CASE WHEN p.order_id IN (SELECT order_id FROM stg_orders WHERE order_id IS NOT NULL)
                THEN 'PAYMENT_ORDER_QUARANTINED' ELSE 'PAYMENT_WITHOUT_ORDER' END AS mismatch_reason
    FROM pay p WHERE p.order_id IS NULL OR p.order_id NOT IN (SELECT order_id FROM clean)
),
both_sides AS (
    SELECT * FROM order_side WHERE mismatch_reason IS NOT NULL
    UNION ALL SELECT * FROM payment_side
)
SELECT order_id, payment_id, order_status, payment_status, order_amount, amount_paid, mismatch_reason,
       round(CASE WHEN mismatch_reason IN ('PAYMENT_WITHOUT_ORDER', 'PAYMENT_ORDER_QUARANTINED') THEN amount_paid
                  WHEN mismatch_reason = 'AMOUNT_MISMATCH' THEN abs(order_amount - amount_paid)
                  ELSE order_amount END, 2) AS unreconciled_amount
FROM both_sides;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode, count(*) AS delivered_orders, round(sum(order_amount), 2) AS net_revenue
FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;
