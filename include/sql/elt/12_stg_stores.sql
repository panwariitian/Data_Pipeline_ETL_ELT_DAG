-- Staging: stores.
CREATE OR REPLACE TEMP TABLE b_stores AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _row_num) AS dup_rank
    FROM raw_stores WHERE _run_id = '{{run_id}}'
)
SELECT *,
    (store_id IS NULL)                                                       AS f_store_id_not_null,
    (zone IS NULL)                                                           AS w_zone_not_null,
    (dup_rank > 1)                                                           AS f_store_id_unique,
    NOT coalesce(zone IS NULL OR zone IN ('North','South','East','West'), false) AS f_zone_allowed,
    (try_strptime(open_date, '%Y-%m-%d') IS NULL)                            AS f_open_date_parses,
    NOT coalesce(try_cast(capacity_orders_per_day AS DOUBLE) > 0, false)     AS w_capacity_positive,
    coalesce(try_strptime(open_date, '%Y-%m-%d') > TIMESTAMP '{{now_ist}}', false) AS f_open_date_not_in_future
FROM src;

CREATE OR REPLACE TABLE stg_stores AS
SELECT trim(store_id) AS store_id, trim(store_name) AS store_name, coalesce(trim(zone), 'Unknown') AS zone,
    trim(area) AS area, pincode, CAST(try_strptime(open_date, '%Y-%m-%d') AS DATE) AS open_date, manager_name,
    try_cast(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN f_store_id_not_null OR f_store_id_unique OR f_zone_allowed OR f_open_date_parses
              OR f_open_date_not_in_future THEN 'FAIL'
         WHEN w_zone_not_null OR w_capacity_positive THEN 'WARN' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN f_store_id_not_null THEN 'store_id_not_null' END,
        CASE WHEN f_store_id_unique THEN 'store_id_unique' END,
        CASE WHEN f_zone_allowed THEN 'zone_allowed' END,
        CASE WHEN f_open_date_parses THEN 'open_date_parses' END,
        CASE WHEN f_open_date_not_in_future THEN 'open_date_not_in_future' END], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file AS source_file, _row_num AS source_row, _run_id AS loaded_run_id, _loaded_at AS loaded_at
FROM b_stores;

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'stores.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'stores.' || check_name, dimension, severity, n, failed,
       CASE WHEN failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM (
    SELECT 'store_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, count(*) AS n, count(*) FILTER (WHERE f_store_id_not_null) AS failed FROM b_stores UNION ALL
    SELECT 'zone_not_null', 'Completeness', 'Warning', count(*), count(*) FILTER (WHERE w_zone_not_null) FROM b_stores UNION ALL
    SELECT 'store_id_unique', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE f_store_id_unique) FROM b_stores UNION ALL
    SELECT 'zone_allowed', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_zone_allowed) FROM b_stores UNION ALL
    SELECT 'open_date_parses', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_open_date_parses) FROM b_stores UNION ALL
    SELECT 'capacity_positive', 'Accuracy', 'Warning', count(*), count(*) FILTER (WHERE w_capacity_positive) FROM b_stores UNION ALL
    SELECT 'open_date_not_in_future', 'Timeliness', 'Critical', count(*), count(*) FILTER (WHERE f_open_date_not_in_future) FROM b_stores
);

DELETE FROM elt_rejected_rows WHERE run_id = '{{run_id}}' AND source_table = 'stores';
INSERT INTO elt_rejected_rows
SELECT '{{run_id}}', 'stores', store_id, source_file, source_row, failure_reason, TIMESTAMP '{{now_ist}}'
FROM stg_stores WHERE dq_status = 'FAIL';
