-- Staging: customers. Types, standardise, mask PII, apply quality rules. Rebuilt from this run's raw rows.
CREATE OR REPLACE TEMP TABLE b_customers AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) AS dup_rank
    FROM raw_customers WHERE _run_id = '{{run_id}}'
),
flags AS (
    SELECT *,
        try_strptime(signup_date, '%Y-%m-%d') AS signup_ts,
        regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g') AS phone_digits,
        (customer_id IS NULL)                                                             AS f_customer_id_not_null,
        (city IS NULL)                                                                    AS w_city_not_null,
        (dup_rank > 1)                                                                    AS f_customer_id_unique,
        ((upper(substr(trim(loyalty_tier), 1, 1)) || lower(substr(trim(loyalty_tier), 2))) NOT IN ('Bronze','Silver','Gold','Platinum')
            OR loyalty_tier IS NULL)                                                      AS f_loyalty_tier_valid,
        (try_strptime(signup_date, '%Y-%m-%d') IS NULL)                                   AS f_signup_date_parses,
        NOT coalesce(regexp_matches(email, '^[^@\s]+@[^@\s]+\.[^@\s]+$'), false)          AS w_email_format,
        (length(regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g')) <> 10)            AS w_phone_is_10_digits,
        NOT coalesce(regexp_full_match(pincode, '[0-9]{6}'), false)                       AS w_pincode_is_6_digits,
        NOT coalesce(is_active IN ('Y','N'), false)                                       AS w_is_active_allowed,
        (loyalty_tier IS NULL OR loyalty_tier <> (upper(substr(trim(loyalty_tier), 1, 1)) || lower(substr(trim(loyalty_tier), 2))))             AS w_loyalty_tier_casing,
        coalesce(try_strptime(signup_date, '%Y-%m-%d') > TIMESTAMP '{{now_ist}}', false)  AS f_signup_date_not_in_future
    FROM src
)
SELECT * FROM flags;

CREATE OR REPLACE TABLE stg_customers AS
SELECT
    trim(customer_id)                                                            AS customer_id,
    CASE WHEN position('@' IN email) = 0 THEN '***'
         ELSE substr(split_part(email, '@', 1), 1, 2) || '***@' || split_part(email, '@', 2) END AS email_masked,
    CASE WHEN length(phone_digits) <> 10 THEN '**********'
         ELSE substr(phone_digits, 1, 2) || '******' || substr(phone_digits, 9, 2) END           AS phone_masked,
    coalesce(trim(city), 'Unknown')                                              AS city,
    trim(area)                                                                   AS area,
    CASE WHEN NOT w_pincode_is_6_digits THEN pincode END                         AS pincode,
    CAST(signup_ts AS DATE)                                                      AS signup_date,
    (upper(substr(trim(loyalty_tier), 1, 1)) || lower(substr(trim(loyalty_tier), 2)))                                                  AS loyalty_tier,
    is_active = 'Y'                                                              AS is_active,
    CASE WHEN f_customer_id_not_null OR f_customer_id_unique OR f_loyalty_tier_valid
              OR f_signup_date_parses OR f_signup_date_not_in_future THEN 'FAIL'
         WHEN w_city_not_null OR w_email_format OR w_phone_is_10_digits OR w_pincode_is_6_digits
              OR w_is_active_allowed OR w_loyalty_tier_casing THEN 'WARN' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN f_customer_id_not_null THEN 'customer_id_not_null' END,
        CASE WHEN f_customer_id_unique THEN 'customer_id_unique' END,
        CASE WHEN f_loyalty_tier_valid THEN 'loyalty_tier_valid' END,
        CASE WHEN f_signup_date_parses THEN 'signup_date_parses' END,
        CASE WHEN f_signup_date_not_in_future THEN 'signup_date_not_in_future' END], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file AS source_file, _row_num AS source_row, _run_id AS loaded_run_id, _loaded_at AS loaded_at
FROM b_customers;

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'customers.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'customers.' || check_name, dimension, severity, n, failed,
       CASE WHEN failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM (
    SELECT 'customer_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, count(*) AS n, count(*) FILTER (WHERE f_customer_id_not_null) AS failed FROM b_customers UNION ALL
    SELECT 'city_not_null', 'Completeness', 'Warning', count(*), count(*) FILTER (WHERE w_city_not_null) FROM b_customers UNION ALL
    SELECT 'customer_id_unique', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE f_customer_id_unique) FROM b_customers UNION ALL
    SELECT 'loyalty_tier_valid', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_loyalty_tier_valid) FROM b_customers UNION ALL
    SELECT 'signup_date_parses', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_signup_date_parses) FROM b_customers UNION ALL
    SELECT 'email_format', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_email_format) FROM b_customers UNION ALL
    SELECT 'phone_is_10_digits', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_phone_is_10_digits) FROM b_customers UNION ALL
    SELECT 'pincode_is_6_digits', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_pincode_is_6_digits) FROM b_customers UNION ALL
    SELECT 'is_active_allowed', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_is_active_allowed) FROM b_customers UNION ALL
    SELECT 'loyalty_tier_casing', 'Consistency', 'Warning', count(*), count(*) FILTER (WHERE w_loyalty_tier_casing) FROM b_customers UNION ALL
    SELECT 'signup_date_not_in_future', 'Timeliness', 'Critical', count(*), count(*) FILTER (WHERE f_signup_date_not_in_future) FROM b_customers
);

DELETE FROM elt_rejected_rows WHERE run_id = '{{run_id}}' AND source_table = 'customers';
INSERT INTO elt_rejected_rows
SELECT '{{run_id}}', 'customers', customer_id, source_file, source_row, failure_reason, TIMESTAMP '{{now_ist}}'
FROM stg_customers WHERE dq_status = 'FAIL';
