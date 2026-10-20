-- Shared audit tables and the persistent staging table for orders (MERGE target).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR);
CREATE TABLE IF NOT EXISTS elt_rejected_rows (
    run_id VARCHAR, source_table VARCHAR, row_key VARCHAR, source_file VARCHAR, source_row BIGINT,
    failure_reason VARCHAR, rejected_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR, store_id VARCHAR,
    product_id VARCHAR, quantity DOUBLE, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
    dq_status VARCHAR, failure_reason VARCHAR, source_file VARCHAR, source_seq INTEGER, source_row BIGINT,
    loaded_run_id VARCHAR, loaded_at TIMESTAMP);
