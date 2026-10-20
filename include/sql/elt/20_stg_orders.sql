-- Staging: orders. Classify every raw row, then MERGE the best version of each order into stg_orders.
CREATE OR REPLACE TEMP TABLE b_orders AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY _source_file, order_id ORDER BY _row_num) AS dup_rank
    FROM raw_orders WHERE _run_id = '{{run_id}}'
),
parsed AS (
    SELECT *,
        try_strptime(order_ts, '%Y-%m-%d %H:%M:%S') AS ts_parsed,
        upper(trim(order_status)) AS status_c,
        CASE lower(trim(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
             WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
             WHEN 'wallet' THEN 'Wallet' END AS pay_c,
        try_cast(quantity AS DOUBLE) AS q, try_cast(unit_price AS DOUBLE) AS up,
        try_cast(discount_pct AS DOUBLE) AS dp, try_cast(order_amount AS DOUBLE) AS amt,
        try_cast(delivery_minutes AS DOUBLE) AS dm
    FROM src
),
flags AS (
    SELECT *,
        (order_id IS NULL)                                                     AS f_order_id_not_null,
        (customer_id IS NULL)                                                  AS f_customer_id_not_null,
        (order_ts IS NULL)                                                     AS f_order_ts_not_null,
        NOT coalesce(status_c <> 'DELIVERED' OR dm IS NOT NULL, false)         AS f_delivered_has_delivery_minutes,
        (dup_rank > 1)                                                         AS f_order_id_unique_per_file,
        NOT coalesce(status_c IN ('DELIVERED','CANCELLED','RETURNED'), false)  AS f_order_status_allowed,
        (pay_c IS NULL)                                                        AS f_payment_mode_allowed,
        NOT coalesce(dp BETWEEN 0 AND 100, false)                              AS f_discount_pct_between_0_and_100,
        NOT (order_ts IS NULL OR ts_parsed IS NOT NULL)                        AS f_order_ts_parses,
        NOT coalesce(trim(channel) IN ('App','Web'), false)                    AS w_channel_allowed,
        NOT coalesce(q > 0, false)                                             AS f_quantity_positive,
        NOT coalesce(up > 0, false)                                            AS f_unit_price_positive,
        NOT coalesce(abs(round(q * up * (1 - dp / 100), 2) - amt) <= 0.01, false) AS f_order_amount_matches_formula,
        NOT coalesce((SELECT mrp FROM stg_products m WHERE m.product_id = parsed.product_id AND m.dq_status <> 'FAIL') IS NULL
            OR abs(up / (SELECT mrp FROM stg_products m WHERE m.product_id = parsed.product_id AND m.dq_status <> 'FAIL') - 1) <= 0.2, true)
                                                                               AS w_unit_price_within_20pct_of_mrp,
        NOT coalesce((SELECT mrp FROM stg_products m WHERE m.product_id = parsed.product_id AND m.dq_status <> 'FAIL') IS NULL
            OR (up / (SELECT mrp FROM stg_products m WHERE m.product_id = parsed.product_id AND m.dq_status <> 'FAIL')) BETWEEN 0.2 AND 5, true)
                                                                               AS f_unit_price_within_5x_of_mrp,
        NOT coalesce(delivery_minutes IS NULL OR dm < 120, false)              AS f_delivery_minutes_below_120,
        NOT (customer_id IS NULL OR customer_id IN (SELECT customer_id FROM stg_customers WHERE customer_id IS NOT NULL))
                                                                               AS f_customer_in_master,
        coalesce(customer_id IN (SELECT customer_id FROM stg_customers WHERE customer_id IS NOT NULL), false)
            AND NOT coalesce(customer_id IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL' AND customer_id IS NOT NULL), false)
                                                                               AS f_customer_master_row_valid,
        NOT coalesce(product_id IN (SELECT product_id FROM stg_products WHERE product_id IS NOT NULL), false) AS f_product_in_master,
        coalesce(product_id IN (SELECT product_id FROM stg_products WHERE product_id IS NOT NULL), false)
            AND NOT coalesce(product_id IN (SELECT product_id FROM stg_products WHERE dq_status <> 'FAIL' AND product_id IS NOT NULL), false)
                                                                               AS f_product_master_row_valid,
        NOT coalesce(store_id IN (SELECT store_id FROM stg_stores WHERE store_id IS NOT NULL), false) AS f_store_in_master,
        coalesce(store_id IN (SELECT store_id FROM stg_stores WHERE store_id IS NOT NULL), false)
            AND NOT coalesce(store_id IN (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL' AND store_id IS NOT NULL), false)
                                                                               AS f_store_master_row_valid,
        coalesce(ts_parsed > TIMESTAMP '{{now_ist}}', false)                   AS f_order_ts_not_in_future
    FROM parsed
),
classified AS (
    SELECT *,
        (f_order_id_not_null OR f_customer_id_not_null OR f_order_ts_not_null OR f_delivered_has_delivery_minutes
         OR f_order_id_unique_per_file OR f_order_status_allowed OR f_payment_mode_allowed
         OR f_discount_pct_between_0_and_100 OR f_order_ts_parses OR f_quantity_positive OR f_unit_price_positive
         OR f_order_amount_matches_formula OR f_unit_price_within_5x_of_mrp OR f_delivery_minutes_below_120 OR f_customer_in_master
         OR f_customer_master_row_valid OR f_product_in_master OR f_product_master_row_valid
         OR f_store_in_master OR f_store_master_row_valid OR f_order_ts_not_in_future) AS is_fail
    FROM flags
)
SELECT *,
    array_to_string(list_filter([
        CASE WHEN f_order_id_not_null THEN 'order_id_not_null' END,
        CASE WHEN f_customer_id_not_null THEN 'customer_id_not_null' END,
        CASE WHEN f_order_ts_not_null THEN 'order_ts_not_null' END,
        CASE WHEN f_delivered_has_delivery_minutes THEN 'delivered_has_delivery_minutes' END,
        CASE WHEN f_order_id_unique_per_file THEN 'order_id_unique_per_file' END,
        CASE WHEN f_order_status_allowed THEN 'order_status_allowed' END,
        CASE WHEN f_payment_mode_allowed THEN 'payment_mode_allowed' END,
        CASE WHEN f_discount_pct_between_0_and_100 THEN 'discount_pct_between_0_and_100' END,
        CASE WHEN f_order_ts_parses THEN 'order_ts_parses' END,
        CASE WHEN f_quantity_positive THEN 'quantity_positive' END,
        CASE WHEN f_unit_price_positive THEN 'unit_price_positive' END,
        CASE WHEN f_order_amount_matches_formula THEN 'order_amount_matches_formula' END,
        CASE WHEN f_unit_price_within_5x_of_mrp THEN 'unit_price_within_5x_of_mrp' END,
        CASE WHEN f_delivery_minutes_below_120 THEN 'delivery_minutes_below_120' END,
        CASE WHEN f_customer_in_master THEN 'customer_in_master' END,
        CASE WHEN f_customer_master_row_valid THEN 'customer_master_row_valid' END,
        CASE WHEN f_product_in_master THEN 'product_in_master' END,
        CASE WHEN f_product_master_row_valid THEN 'product_master_row_valid' END,
        CASE WHEN f_store_in_master THEN 'store_in_master' END,
        CASE WHEN f_store_master_row_valid THEN 'store_master_row_valid' END,
        CASE WHEN f_order_ts_not_in_future THEN 'order_ts_not_in_future' END], x -> x IS NOT NULL), '; ') AS reason
FROM classified;

-- one row per order_id: a passing version beats a failing one, then the newest file wins
CREATE OR REPLACE TEMP TABLE b_orders_best AS
SELECT order_id, ts_parsed AS order_ts, CAST(ts_parsed AS DATE) AS order_date, trim(customer_id) AS customer_id,
       trim(store_id) AS store_id, trim(product_id) AS product_id, q AS quantity, up AS unit_price,
       dp AS discount_pct, amt AS order_amount, pay_c AS payment_mode, status_c AS order_status,
       trim(channel) AS channel, dm AS delivery_minutes,
       CASE WHEN is_fail THEN 'FAIL'
            WHEN w_channel_allowed OR w_unit_price_within_20pct_of_mrp THEN 'WARN' ELSE 'PASS' END AS dq_status,
       reason AS failure_reason, _source_file AS source_file, _source_seq AS source_seq,
       _row_num AS source_row, _run_id AS loaded_run_id, _loaded_at AS loaded_at
FROM (
    SELECT *, row_number() OVER (PARTITION BY order_id
              ORDER BY (NOT is_fail) DESC, _source_seq DESC, dup_rank ASC, _row_num DESC) AS win_rank
    FROM b_orders WHERE order_id IS NOT NULL
) WHERE win_rank = 1;

MERGE INTO stg_orders AS t
USING b_orders_best AS s ON (t.order_id = s.order_id)
WHEN MATCHED AND ((s.dq_status <> 'FAIL')::INTEGER, s.source_seq) >= ((t.dq_status <> 'FAIL')::INTEGER, t.source_seq)
THEN UPDATE SET order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id,
    store_id = s.store_id, product_id = s.product_id, quantity = s.quantity, unit_price = s.unit_price,
    discount_pct = s.discount_pct, order_amount = s.order_amount, payment_mode = s.payment_mode,
    order_status = s.order_status, channel = s.channel, delivery_minutes = s.delivery_minutes,
    dq_status = s.dq_status, failure_reason = s.failure_reason, source_file = s.source_file,
    source_seq = s.source_seq, source_row = s.source_row, loaded_run_id = s.loaded_run_id, loaded_at = s.loaded_at
WHEN NOT MATCHED THEN INSERT (order_id, order_ts, order_date, customer_id, store_id, product_id, quantity,
    unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status,
    failure_reason, source_file, source_seq, source_row, loaded_run_id, loaded_at)
VALUES (s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id, s.quantity,
    s.unit_price, s.discount_pct, s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes,
    s.dq_status, s.failure_reason, s.source_file, s.source_seq, s.source_row, s.loaded_run_id, s.loaded_at);

DELETE FROM elt_rejected_rows WHERE run_id = '{{run_id}}' AND source_table = 'orders';
INSERT INTO elt_rejected_rows
SELECT '{{run_id}}', 'orders', order_id, _source_file, _row_num, reason, TIMESTAMP '{{now_ist}}'
FROM b_orders WHERE is_fail;

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'orders.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'orders.' || check_name, dimension, severity, n, failed,
       CASE WHEN failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM (
    SELECT 'order_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, count(*) AS n, count(*) FILTER (WHERE f_order_id_not_null) AS failed FROM b_orders UNION ALL
    SELECT 'customer_id_not_null', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE f_customer_id_not_null) FROM b_orders UNION ALL
    SELECT 'order_ts_not_null', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE f_order_ts_not_null) FROM b_orders UNION ALL
    SELECT 'delivered_has_delivery_minutes', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE f_delivered_has_delivery_minutes) FROM b_orders UNION ALL
    SELECT 'order_id_unique_per_file', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE f_order_id_unique_per_file) FROM b_orders UNION ALL
    SELECT 'order_status_allowed', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_order_status_allowed) FROM b_orders UNION ALL
    SELECT 'payment_mode_allowed', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_payment_mode_allowed) FROM b_orders UNION ALL
    SELECT 'discount_pct_between_0_and_100', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_discount_pct_between_0_and_100) FROM b_orders UNION ALL
    SELECT 'order_ts_parses', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_order_ts_parses) FROM b_orders UNION ALL
    SELECT 'channel_allowed', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_channel_allowed) FROM b_orders UNION ALL
    SELECT 'quantity_positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_quantity_positive) FROM b_orders UNION ALL
    SELECT 'unit_price_positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_unit_price_positive) FROM b_orders UNION ALL
    SELECT 'order_amount_matches_formula', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_order_amount_matches_formula) FROM b_orders UNION ALL
    SELECT 'unit_price_within_20pct_of_mrp', 'Accuracy', 'Warning', count(*), count(*) FILTER (WHERE w_unit_price_within_20pct_of_mrp) FROM b_orders UNION ALL
    SELECT 'unit_price_within_5x_of_mrp', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_unit_price_within_5x_of_mrp) FROM b_orders UNION ALL
    SELECT 'delivery_minutes_below_120', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_delivery_minutes_below_120) FROM b_orders UNION ALL
    SELECT 'customer_in_master', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_customer_in_master) FROM b_orders UNION ALL
    SELECT 'customer_master_row_valid', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_customer_master_row_valid) FROM b_orders UNION ALL
    SELECT 'product_in_master', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_product_in_master) FROM b_orders UNION ALL
    SELECT 'product_master_row_valid', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_product_master_row_valid) FROM b_orders UNION ALL
    SELECT 'store_in_master', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_store_in_master) FROM b_orders UNION ALL
    SELECT 'store_master_row_valid', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_store_master_row_valid) FROM b_orders UNION ALL
    SELECT 'order_ts_not_in_future', 'Timeliness', 'Critical', count(*), count(*) FILTER (WHERE f_order_ts_not_in_future) FROM b_orders UNION ALL
    SELECT 'latest_order_not_older_than_1_day', 'Timeliness', 'Warning', 1,
           CASE WHEN coalesce(max(ts_parsed) FILTER (WHERE ts_parsed <= TIMESTAMP '{{now_ist}}'), TIMESTAMP '1970-01-01') < TIMESTAMP '{{now_ist}}' - INTERVAL 1 DAY THEN 1 ELSE 0 END FROM b_orders UNION ALL
    SELECT 'volume_not_down_more_than_90pct', 'Completeness', 'Warning', 1,
           CASE WHEN prev IS NOT NULL AND cnt < 0.1 * prev THEN 1 ELSE 0 END
    FROM (SELECT count(*) AS cnt FROM b_orders) c,
         (SELECT median(rows_checked) AS prev FROM (
              SELECT rows_checked FROM dq_results
              WHERE dag_id = '{{dag_id}}' AND check_name = 'orders.order_id_not_null' AND run_id <> '{{run_id}}'
              ORDER BY checked_at DESC LIMIT 7)) p
);
