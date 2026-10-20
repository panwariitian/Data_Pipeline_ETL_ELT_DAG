-- Staging: products.
CREATE OR REPLACE TEMP TABLE b_products AS
WITH src AS (
    SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY _row_num) AS dup_rank
    FROM raw_products WHERE _run_id = '{{run_id}}'
)
SELECT *,
    (product_id IS NULL)                                                    AS f_product_id_not_null,
    (category IS NULL)                                                      AS f_category_not_null,
    (dup_rank > 1)                                                          AS f_product_id_unique,
    (try_strptime(launch_date, '%Y-%m-%d') IS NULL)                         AS f_launch_date_parses,
    NOT coalesce(is_perishable IN ('True','False'), false)                  AS w_is_perishable_allowed,
    NOT coalesce(unit IN ('pack','unit','kg'), false)                       AS w_unit_allowed,
    NOT coalesce(try_cast(mrp AS DOUBLE) > 0, false)                        AS f_mrp_positive,
    NOT coalesce(try_cast(cost_price AS DOUBLE) > 0, false)                 AS f_cost_price_positive,
    NOT coalesce(try_cast(cost_price AS DOUBLE) <= try_cast(mrp AS DOUBLE), false) AS f_cost_price_not_above_mrp,
    coalesce(try_strptime(launch_date, '%Y-%m-%d') > TIMESTAMP '{{now_ist}}', false) AS f_launch_date_not_in_future
FROM src;

CREATE OR REPLACE TABLE stg_products AS
SELECT trim(product_id) AS product_id, trim(product_name) AS product_name, trim(category) AS category,
    trim(sub_category) AS sub_category, trim(brand) AS brand, try_cast(mrp AS DOUBLE) AS mrp,
    try_cast(cost_price AS DOUBLE) AS cost_price, unit, is_perishable = 'True' AS is_perishable,
    CAST(try_strptime(launch_date, '%Y-%m-%d') AS DATE) AS launch_date,
    CASE WHEN f_product_id_not_null OR f_category_not_null OR f_product_id_unique OR f_launch_date_parses
              OR f_mrp_positive OR f_cost_price_positive OR f_cost_price_not_above_mrp
              OR f_launch_date_not_in_future THEN 'FAIL'
         WHEN w_is_perishable_allowed OR w_unit_allowed THEN 'WARN' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN f_product_id_not_null THEN 'product_id_not_null' END,
        CASE WHEN f_category_not_null THEN 'category_not_null' END,
        CASE WHEN f_product_id_unique THEN 'product_id_unique' END,
        CASE WHEN f_launch_date_parses THEN 'launch_date_parses' END,
        CASE WHEN f_mrp_positive THEN 'mrp_positive' END,
        CASE WHEN f_cost_price_positive THEN 'cost_price_positive' END,
        CASE WHEN f_cost_price_not_above_mrp THEN 'cost_price_not_above_mrp' END,
        CASE WHEN f_launch_date_not_in_future THEN 'launch_date_not_in_future' END], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file AS source_file, _row_num AS source_row, _run_id AS loaded_run_id, _loaded_at AS loaded_at
FROM b_products;

DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}' AND check_name LIKE 'products.%';
INSERT INTO dq_results
SELECT '{{run_id}}', '{{dag_id}}', 'products.' || check_name, dimension, severity, n, failed,
       CASE WHEN failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END, TIMESTAMP '{{now_ist}}'
FROM (
    SELECT 'product_id_not_null' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity, count(*) AS n, count(*) FILTER (WHERE f_product_id_not_null) AS failed FROM b_products UNION ALL
    SELECT 'category_not_null', 'Completeness', 'Critical', count(*), count(*) FILTER (WHERE f_category_not_null) FROM b_products UNION ALL
    SELECT 'product_id_unique', 'Uniqueness', 'Critical', count(*), count(*) FILTER (WHERE f_product_id_unique) FROM b_products UNION ALL
    SELECT 'launch_date_parses', 'Validity', 'Critical', count(*), count(*) FILTER (WHERE f_launch_date_parses) FROM b_products UNION ALL
    SELECT 'is_perishable_allowed', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_is_perishable_allowed) FROM b_products UNION ALL
    SELECT 'unit_allowed', 'Validity', 'Warning', count(*), count(*) FILTER (WHERE w_unit_allowed) FROM b_products UNION ALL
    SELECT 'mrp_positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_mrp_positive) FROM b_products UNION ALL
    SELECT 'cost_price_positive', 'Accuracy', 'Critical', count(*), count(*) FILTER (WHERE f_cost_price_positive) FROM b_products UNION ALL
    SELECT 'cost_price_not_above_mrp', 'Consistency', 'Critical', count(*), count(*) FILTER (WHERE f_cost_price_not_above_mrp) FROM b_products UNION ALL
    SELECT 'launch_date_not_in_future', 'Timeliness', 'Critical', count(*), count(*) FILTER (WHERE f_launch_date_not_in_future) FROM b_products
);

DELETE FROM elt_rejected_rows WHERE run_id = '{{run_id}}' AND source_table = 'products';
INSERT INTO elt_rejected_rows
SELECT '{{run_id}}', 'products', product_id, source_file, source_row, failure_reason, TIMESTAMP '{{now_ist}}'
FROM stg_products WHERE dq_status = 'FAIL';
