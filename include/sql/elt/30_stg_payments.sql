-- Staging: payments. Needs stg_orders so the order reference and SUCCESS amounts can be checked.
CREATE OR REPLACE TEMP TABLE b_payments AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) AS dup_rank
    FROM raw_payments WHERE _run_id = '{{run_id}}'
),
parsed AS (
    SELECT p.*, try_strptime(p.payment_ts, '%Y-%m-%d %H:%M:%S') AS ts_parsed,
        CASE lower(trim(p.payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
             WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
             WHEN 'wallet' THEN 'Wallet' END AS pay_c,
        upper(trim(p.payment_status)) AS status_c,
        try_cast(p.amount_paid AS DOUBLE) AS paid,
        o.order_id AS o_id, o.order_amount AS o_amount
    FROM src p LEFT JOIN (SELECT DISTINCT order_id, order_amount FROM stg_orders) o ON o.order_id = p.order_id
),
flags AS (
    SELECT *,
        (payment_id IS NULL)                                                        AS f_payment_id_not_null,
        (order_id IS NULL)                                                          AS f_payment_order_id_not_null,
        NOT coalesce(pay_c = 'Cash on Delivery' OR gateway_ref IS NOT NULL, false)  AS w_gateway_ref_present_for_online,
        (dup_rank > 1)                                                              AS f_payment_id_unique,
        (pay_c IS NULL)                                                             AS f_payment_mode_allowed,
        NOT coalesce(status_c IN ('SUCCESS','REFUNDED','FAILED'), false)            AS f_payment_status_allowed,
        (ts_parsed IS NULL)                                                         AS f_payment_ts_parses,
        NOT coalesce(paid > 0, false)                                               AS f_amount_paid_positive,
        (o_id IS NULL)                                                              AS f_payment_order_exists,
        NOT coalesce(status_c <> 'SUCCESS' OR o_amount IS NULL OR abs(paid - o_amount) <= 0.01, false)
                                                                                    AS f_success_amount_equals_order_amount,
        coalesce(ts_parsed > TIMESTAMP '{{now_ist}}', false)                        AS f_payment_ts_not_in_future
    FROM parsed
)
SELECT *,
    (f_payment_id_not_null OR f_payment_order_id_not_null OR f_payment_id_unique OR f_payment_mode_allowed
     OR f_payment_status_allowed OR f_payment_ts_parses OR f_amount_paid_positive OR f_payment_order_exists
     OR f_success_amount_equals_order_amount OR f_payment_ts_not_in_future) AS is_fail,
    array_to_string(list_filter([
        CASE WHEN f_payment_id_not_null THEN 'payment_id_not_null' END,
        CASE WHEN f_payment_order_id_not_null THEN 'payment_order_id_not_null' END,
        CASE WHEN f_payment_id_unique THEN 'payment_id_unique' END,
        CASE WHEN f_payment_mode_allowed THEN 'payment_mode_allowed' END,
        CASE WHEN f_payment_status_allowed THEN 'payment_status_allowed' END,
        CASE WHEN f_payment_ts_parses THEN 'payment_ts_parses' END,
        CASE WHEN f_amount_paid_positive THEN 'amount_paid_positive' END,
        CASE WHEN f_payment_order_exists THEN 'payment_order_exists' END,
        CASE WHEN f_success_amount_equals_order_amount THEN 'success_amount_equals_order_amount' END,
        CASE WHEN f_payment_ts_not_in_future THEN 'payment_ts_not_in_future' END], x -> x IS NOT NULL), '; ') AS reason
FROM flags;

CREATE OR REPLACE TABLE stg_payments AS
SELECT payment_id, order_id, ts_parsed AS payment_ts, pay_c AS payment_mode, paid AS amount_paid,
       status_c AS payment_status, gateway_ref,
       CASE WHEN is_fail THEN 'FAIL' WHEN w_gateway_ref_present_for_online THEN 'WARN' ELSE 'PASS' END AS dq_status,
       reason AS failure_reason, _source_file AS source_file, _row_num AS source_row,
       _run_id AS loaded_run_id, _loaded_at AS loaded_at
FROM b_payments;

DELETE FROM elt_rejected_rows WHERE run_id = '{{run_id}}' AND source_table = 'payments';
INSERT INTO elt_rejected_rows
SELECT '{{run_id}}', 'payments', payment_id, source_file, source_row, failure_reason, TIMESTAMP '{{now_ist}}'
FROM stg_payments WHERE dq_status = 'FAIL';

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'payments.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'payments.' || check_name, dimension, severity, n, failed,
       CASE WHEN failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM (
    SELECT 'payment_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, count(*) AS n, count(*) FILTER (WHERE f_payment_id_not_null) AS failed FROM b_payments UNION ALL
    SELECT 'payment_order_id_not_null', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE f_payment_order_id_not_null) FROM b_payments UNION ALL
    SELECT 'gateway_ref_present_for_online', 'Completeness', 'Warning', count(*), count(*) FILTER (WHERE w_gateway_ref_present_for_online) FROM b_payments UNION ALL
    SELECT 'payment_id_unique', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE f_payment_id_unique) FROM b_payments UNION ALL
    SELECT 'payment_mode_allowed', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_payment_mode_allowed) FROM b_payments UNION ALL
    SELECT 'payment_status_allowed', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_payment_status_allowed) FROM b_payments UNION ALL
    SELECT 'payment_ts_parses', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_payment_ts_parses) FROM b_payments UNION ALL
    SELECT 'amount_paid_positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_amount_paid_positive) FROM b_payments UNION ALL
    SELECT 'payment_order_exists', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_payment_order_exists) FROM b_payments UNION ALL
    SELECT 'success_amount_equals_order_amount', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_success_amount_equals_order_amount) FROM b_payments UNION ALL
    SELECT 'payment_ts_not_in_future', 'Timeliness', 'Critical', count(*), count(*) FILTER (WHERE f_payment_ts_not_in_future) FROM b_payments
);
