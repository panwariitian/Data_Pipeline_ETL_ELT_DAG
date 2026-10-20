## Experiment 2 - Re-run (idempotency)
before:
```
etl_fact  raw_orders  stg_orders  etl_revenue  elt_revenue
     2177        4824        2400   1059881.95   1059881.95
```

### nammamart_etl_pradeep  run=exp2_etl_045349  state=success  wall=11s  (same inputs, second run)
```
extract_payments | success | 1 | 11:53:51.152 | 1.0
extract_orders | success | 1 | 11:53:51.250 | 0.9
extract_stores | success | 1 | 11:53:51.265 | 1.0
extract_customers | success | 1 | 11:53:51.275 | 0.8
extract_products | success | 1 | 11:53:51.290 | 0.9
validate_payments | success | 1 | 11:53:52.803 | 0.5
validate_customers | success | 1 | 11:53:52.805 | 0.5
validate_stores | success | 1 | 11:53:52.807 | 0.5
validate_products | success | 1 | 11:53:52.809 | 0.5
validate_orders | success | 1 | 11:53:53.717 | 0.4
transform | success | 1 | 11:53:54.816 | 0.3
load_warehouse | success | 1 | 11:53:55.907 | 0.4
reconcile_payments | success | 1 | 11:53:57.014 | 0.5
report | success | 1 | 11:53:58.100 | 0.3
run_audit | success | 1 | 11:53:59.237 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp2_etl_045349 SUCCESS            5212         4916               296
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2412           18   FAIL
           orders.customer_master_row_valid Critical          2412           18   FAIL
      orders.delivered_has_delivery_minutes Critical          2412            8   FAIL
          orders.delivery_minutes_below_120 Critical          2412            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2412            3   FAIL
        orders.order_amount_matches_formula Critical          2412           18   FAIL
            orders.order_id_unique_per_file Critical          2412           12   FAIL
                orders.order_status_allowed Critical          2412            3   FAIL
              orders.order_ts_not_in_future Critical          2412            7   FAIL
                   orders.order_ts_not_null Critical          2412            1   FAIL
                     orders.order_ts_parses Critical          2412            5   FAIL
                orders.payment_mode_allowed Critical          2412            2   FAIL
                   orders.product_in_master Critical          2412            9   FAIL
            orders.product_master_row_valid Critical          2412          126   FAIL
                   orders.quantity_positive Critical          2412           10   FAIL
                     orders.store_in_master Critical          2412            5   FAIL
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

### nammamart_elt_pradeep  run=exp2_elt_045403  state=success  wall=21s  (same inputs, second run)
```
extract_orders | success | 1 | 11:54:05.957 | 0.4
extract_payments | success | 1 | 11:54:05.958 | 0.4
extract_customers | success | 1 | 11:54:05.971 | 0.3
extract_stores | success | 1 | 11:54:05.980 | 0.4
extract_products | success | 1 | 11:54:05.993 | 0.3
load_raw | success | 1 | 11:54:06.605 | 0.6
load_raw_increment | success | 1 | 11:54:07.710 | 0.2
stg_customers | success | 1 | 11:54:08.798 | 0.2
stg_products | success | 1 | 11:54:09.917 | 0.2
stg_stores | success | 1 | 11:54:11.020 | 0.2
stg_orders_merge | success | 1 | 11:54:11.851 | 0.2
stg_payments | success | 1 | 11:54:12.983 | 0.2
mart_daily_store_kpis | success | 1 | 11:54:14.117 | 4.4
mart_delivery_sla | success | 1 | 11:54:14.130 | 0.3
mart_payment_reconciliation | success | 1 | 11:54:14.131 | 0.3
mart_top_customers | success | 1 | 11:54:14.135 | 8.5
mart_category_margin | success | 1 | 11:54:14.147 | 6.4
mart_rejections | success | 1 | 11:54:14.177 | 2.3
report | success | 1 | 11:54:23.010 | 0.4
run_audit | success | 1 | 11:54:23.886 | 0.3
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp2_elt_045403 SUCCESS            5212         4916               296
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2412           18   FAIL
           orders.customer_master_row_valid Critical          2412           18   FAIL
      orders.delivered_has_delivery_minutes Critical          2412            8   FAIL
          orders.delivery_minutes_below_120 Critical          2412            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2412            3   FAIL
        orders.order_amount_matches_formula Critical          2412           18   FAIL
            orders.order_id_unique_per_file Critical          2412           12   FAIL
                orders.order_status_allowed Critical          2412            3   FAIL
              orders.order_ts_not_in_future Critical          2412            7   FAIL
                   orders.order_ts_not_null Critical          2412            1   FAIL
                     orders.order_ts_parses Critical          2412            5   FAIL
                orders.payment_mode_allowed Critical          2412            2   FAIL
                   orders.product_in_master Critical          2412            9   FAIL
            orders.product_master_row_valid Critical          2412          126   FAIL
                   orders.quantity_positive Critical          2412           10   FAIL
                     orders.store_in_master Critical          2412            5   FAIL
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
after:
```
etl_fact  raw_orders  stg_orders  etl_revenue  elt_revenue
     2177        7236        2400   1059881.95   1059881.95
```
