## Experiment 7 - Volume anomaly (only 50 orders)

### nammamart_etl_pradeep  run=exp7_etl_052011  state=success  wall=11s  (orders.csv cut to 50 rows)
```
extract_orders | success | 1 | 12:20:13.388 | 1.1
extract_payments | success | 1 | 12:20:13.389 | 1.0
extract_products | success | 1 | 12:20:13.420 | 1.0
extract_stores | success | 1 | 12:20:13.426 | 1.0
extract_customers | success | 1 | 12:20:13.431 | 1.0
validate_customers | success | 1 | 12:20:14.948 | 0.6
validate_stores | success | 1 | 12:20:14.957 | 0.6
validate_products | success | 1 | 12:20:14.986 | 0.6
validate_orders | success | 1 | 12:20:16.052 | 0.4
validate_payments | success | 1 | 12:20:17.156 | 0.6
transform | success | 1 | 12:20:17.161 | 0.5
load_warehouse | success | 1 | 12:20:18.237 | 0.5
reconcile_payments | success | 1 | 12:20:19.054 | 0.4
report | success | 1 | 12:20:20.168 | 0.4
run_audit | success | 1 | 12:20:21.279 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp7_etl_052011 SUCCESS            3010         2932                78
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical           210            1   FAIL
           orders.customer_master_row_valid Critical           210            1   FAIL
   orders.latest_order_not_older_than_1_day  Warning             1            1   WARN
                orders.payment_mode_allowed Critical           210            1   FAIL
            orders.product_master_row_valid Critical           210           14   FAIL
     orders.volume_not_down_more_than_90pct  Warning             1            1   WARN
              payments.amount_paid_positive Critical          2228           10   FAIL
                 payments.payment_id_unique Critical          2228            4   FAIL
              payments.payment_mode_allowed Critical          2228            2   FAIL
              payments.payment_order_exists Critical          2228            8   FAIL
          payments.payment_ts_not_in_future Critical          2228            7   FAIL
payments.success_amount_equals_order_amount Critical          2228           14   FAIL
                 products.category_not_null Critical           152            3   FAIL
          products.cost_price_not_above_mrp Critical           152            5   FAIL
                      products.mrp_positive Critical           152            1   FAIL
                 products.product_id_unique Critical           152            2   FAIL
                   stores.capacity_positive  Warning            15            1   WARN
                       stores.zone_not_null  Warning            15            1   WARN
```

### nammamart_elt_pradeep  run=exp7_elt_052024  state=success  wall=26s  (orders.csv cut to 50 rows)
```
extract_orders | success | 1 | 12:20:26.574 | 0.6
extract_stores | success | 1 | 12:20:26.604 | 0.5
extract_payments | success | 1 | 12:20:26.633 | 0.5
extract_products | success | 1 | 12:20:26.633 | 0.4
extract_customers | success | 1 | 12:20:26.677 | 0.4
load_raw | success | 1 | 12:20:27.698 | 0.6
load_raw_increment | success | 1 | 12:20:28.775 | 0.4
stg_customers | success | 1 | 12:20:30.187 | 0.2
stg_products | success | 1 | 12:20:31.306 | 0.2
stg_stores | success | 1 | 12:20:32.444 | 0.2
stg_orders_merge | success | 1 | 12:20:33.566 | 0.3
stg_payments | success | 1 | 12:20:34.952 | 0.2
mart_daily_store_kpis | success | 1 | 12:20:35.725 | 2.4
mart_category_margin | success | 1 | 12:20:35.730 | 0.4
mart_payment_reconciliation | success | 1 | 12:20:35.773 | 8.5
mart_top_customers | success | 1 | 12:20:35.782 | 4.5
mart_rejections | success | 1 | 12:20:35.784 | 10.4
mart_delivery_sla | success | 1 | 12:20:35.799 | 6.4
report | success | 1 | 12:20:47.177 | 0.4
run_audit | success | 1 | 12:20:48.290 | 0.3
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp7_elt_052024 SUCCESS            3010         2932                78
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical           210            1   FAIL
           orders.customer_master_row_valid Critical           210            1   FAIL
   orders.latest_order_not_older_than_1_day  Warning             1            1   WARN
                orders.payment_mode_allowed Critical           210            1   FAIL
            orders.product_master_row_valid Critical           210           14   FAIL
     orders.volume_not_down_more_than_90pct  Warning             1            1   WARN
              payments.amount_paid_positive Critical          2228           10   FAIL
                 payments.payment_id_unique Critical          2228            4   FAIL
              payments.payment_mode_allowed Critical          2228            2   FAIL
              payments.payment_order_exists Critical          2228            8   FAIL
          payments.payment_ts_not_in_future Critical          2228            7   FAIL
payments.success_amount_equals_order_amount Critical          2228           14   FAIL
                 products.category_not_null Critical           152            3   FAIL
          products.cost_price_not_above_mrp Critical           152            5   FAIL
                      products.mrp_positive Critical           152            1   FAIL
                 products.product_id_unique Critical           152            2   FAIL
                   stores.capacity_positive  Warning            15            1   WARN
                       stores.zone_not_null  Warning            15            1   WARN
```
