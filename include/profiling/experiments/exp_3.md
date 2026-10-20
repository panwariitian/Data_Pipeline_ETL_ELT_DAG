## Experiment 3 - Incremental load
before:
```
etl_fact  raw_orders  stg_orders  etl_revenue  elt_revenue
     2177        7236        2400   1059881.95   1059881.95
```

### nammamart_etl_pradeep  run=exp3_etl_045427  state=success  wall=10s  (increment file added)
```
extract_payments | success | 1 | 11:54:30.193 | 1.0
extract_customers | success | 1 | 11:54:30.217 | 0.9
extract_products | success | 1 | 11:54:30.219 | 1.0
extract_orders | success | 1 | 11:54:30.236 | 0.9
extract_stores | success | 1 | 11:54:30.252 | 1.0
validate_payments | success | 1 | 11:54:31.293 | 0.5
validate_products | success | 1 | 11:54:31.294 | 0.4
validate_customers | success | 1 | 11:54:31.297 | 0.5
validate_stores | success | 1 | 11:54:31.317 | 0.4
validate_orders | success | 1 | 11:54:31.956 | 0.4
transform | success | 1 | 11:54:33.065 | 0.4
load_warehouse | success | 1 | 11:54:34.459 | 0.4
reconcile_payments | success | 1 | 11:54:35.581 | 0.4
report | success | 1 | 11:54:36.662 | 0.3
run_audit | success | 1 | 11:54:37.763 | 0.3
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp3_etl_045427 SUCCESS            5372         5061               311
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2572           18   FAIL
           orders.customer_master_row_valid Critical          2572           19   FAIL
      orders.delivered_has_delivery_minutes Critical          2572            8   FAIL
          orders.delivery_minutes_below_120 Critical          2572            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2572            3   FAIL
        orders.order_amount_matches_formula Critical          2572           18   FAIL
            orders.order_id_unique_per_file Critical          2572           12   FAIL
                orders.order_status_allowed Critical          2572            3   FAIL
              orders.order_ts_not_in_future Critical          2572            7   FAIL
                   orders.order_ts_not_null Critical          2572            1   FAIL
                     orders.order_ts_parses Critical          2572            5   FAIL
                orders.payment_mode_allowed Critical          2572            2   FAIL
                   orders.product_in_master Critical          2572            9   FAIL
            orders.product_master_row_valid Critical          2572          140   FAIL
                   orders.quantity_positive Critical          2572           10   FAIL
                     orders.store_in_master Critical          2572            5   FAIL
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

### nammamart_elt_pradeep  run=exp3_elt_045441  state=success  wall=21s  (increment file added)
```
extract_orders | success | 1 | 11:54:43.102 | 0.6
extract_customers | success | 1 | 11:54:43.106 | 0.5
extract_payments | success | 1 | 11:54:43.112 | 0.6
extract_stores | success | 1 | 11:54:43.119 | 0.5
extract_products | success | 1 | 11:54:43.144 | 0.5
load_raw | success | 1 | 11:54:44.153 | 0.7
load_raw_increment | success | 1 | 11:54:45.334 | 0.4
stg_customers | success | 1 | 11:54:46.448 | 0.3
stg_products | success | 1 | 11:54:47.559 | 0.2
stg_stores | success | 1 | 11:54:48.658 | 0.2
stg_orders_merge | success | 1 | 11:54:49.763 | 0.2
stg_payments | success | 1 | 11:54:50.862 | 0.2
mart_delivery_sla | success | 1 | 11:54:51.959 | 2.3
mart_payment_reconciliation | success | 1 | 11:54:51.969 | 0.3
mart_daily_store_kpis | success | 1 | 11:54:51.970 | 2.3
mart_category_margin | success | 1 | 11:54:51.972 | 6.4
mart_top_customers | success | 1 | 11:54:51.986 | 4.3
mart_rejections | success | 1 | 11:54:52.000 | 0.3
report | success | 1 | 11:54:59.246 | 0.5
run_audit | success | 1 | 11:55:00.389 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp3_elt_045441 SUCCESS            5372         5061               311
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2572           18   FAIL
           orders.customer_master_row_valid Critical          2572           19   FAIL
      orders.delivered_has_delivery_minutes Critical          2572            8   FAIL
          orders.delivery_minutes_below_120 Critical          2572            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2572            3   FAIL
        orders.order_amount_matches_formula Critical          2572           18   FAIL
            orders.order_id_unique_per_file Critical          2572           12   FAIL
                orders.order_status_allowed Critical          2572            3   FAIL
              orders.order_ts_not_in_future Critical          2572            7   FAIL
                   orders.order_ts_not_null Critical          2572            1   FAIL
                     orders.order_ts_parses Critical          2572            5   FAIL
                orders.payment_mode_allowed Critical          2572            2   FAIL
                   orders.product_in_master Critical          2572            9   FAIL
            orders.product_master_row_valid Critical          2572          140   FAIL
                   orders.quantity_positive Critical          2572           10   FAIL
                     orders.store_in_master Critical          2572            5   FAIL
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
etl_fact  stg_orders  etl_from_increment  stg_from_increment  etl_returned_corrections  stg_returned_corrections  etl_dups  stg_dups
     2313        2550                 145                 160                         9                        10         0         0
```
