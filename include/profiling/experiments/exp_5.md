## Experiment 5 - New bad data (5 rows appended to orders.csv)

### nammamart_etl_pradeep  run=exp5_etl_050730  state=success  wall=11s  (5 bad rows appended)
```
extract_customers | success | 1 | 12:07:32.066 | 1.2
extract_payments | success | 1 | 12:07:32.069 | 1.2
extract_orders | success | 1 | 12:07:32.081 | 1.3
extract_stores | success | 1 | 12:07:32.081 | 1.3
extract_products | success | 1 | 12:07:32.109 | 1.3
validate_customers | success | 1 | 12:07:33.452 | 0.9
validate_stores | success | 1 | 12:07:33.460 | 0.9
validate_payments | success | 1 | 12:07:33.536 | 1.1
validate_products | success | 1 | 12:07:33.580 | 0.7
validate_orders | success | 1 | 12:07:34.740 | 0.5
transform | success | 1 | 12:07:35.760 | 0.4
load_warehouse | success | 1 | 12:07:36.920 | 0.5
reconcile_payments | success | 1 | 12:07:37.925 | 0.6
report | success | 1 | 12:07:39.050 | 0.4
run_audit | success | 1 | 12:07:40.177 | 0.5
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp5_etl_050730 SUCCESS            5377         5061               316
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2577           19   FAIL
           orders.customer_master_row_valid Critical          2577           19   FAIL
      orders.delivered_has_delivery_minutes Critical          2577            8   FAIL
          orders.delivery_minutes_below_120 Critical          2577            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2577            3   FAIL
        orders.order_amount_matches_formula Critical          2577           18   FAIL
            orders.order_id_unique_per_file Critical          2577           12   FAIL
                orders.order_status_allowed Critical          2577            4   FAIL
              orders.order_ts_not_in_future Critical          2577            8   FAIL
                   orders.order_ts_not_null Critical          2577            1   FAIL
                     orders.order_ts_parses Critical          2577            5   FAIL
                orders.payment_mode_allowed Critical          2577            2   FAIL
                   orders.product_in_master Critical          2577            9   FAIL
            orders.product_master_row_valid Critical          2577          140   FAIL
                   orders.quantity_positive Critical          2577           11   FAIL
                     orders.store_in_master Critical          2577            6   FAIL
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

### nammamart_elt_pradeep  run=exp5_elt_050744  state=success  wall=21s  (5 bad rows appended)
```
extract_payments | success | 1 | 12:07:46.914 | 0.9
extract_orders | success | 1 | 12:07:46.916 | 1.0
extract_customers | success | 1 | 12:07:46.944 | 0.9
extract_products | success | 1 | 12:07:46.944 | 0.9
extract_stores | success | 1 | 12:07:46.970 | 0.9
load_raw | success | 1 | 12:07:48.002 | 0.9
load_raw_increment | success | 1 | 12:07:49.111 | 0.4
stg_customers | success | 1 | 12:07:50.220 | 0.2
stg_products | success | 1 | 12:07:51.326 | 0.2
stg_stores | success | 1 | 12:07:52.444 | 0.2
stg_orders_merge | success | 1 | 12:07:53.574 | 0.3
stg_payments | success | 1 | 12:07:54.679 | 0.2
mart_payment_reconciliation | success | 1 | 12:07:55.785 | 4.5
mart_top_customers | success | 1 | 12:07:55.794 | 0.4
mart_delivery_sla | success | 1 | 12:07:55.799 | 0.4
mart_rejections | success | 1 | 12:07:55.815 | 6.5
mart_daily_store_kpis | success | 1 | 12:07:55.824 | 2.4
mart_category_margin | success | 1 | 12:07:55.873 | 8.5
report | success | 1 | 12:08:05.440 | 0.6
run_audit | success | 1 | 12:08:06.961 | 0.7
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp5_elt_050744 SUCCESS            5377         5061               316
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
                orders.customer_id_not_null Critical          2577           19   FAIL
           orders.customer_master_row_valid Critical          2577           19   FAIL
      orders.delivered_has_delivery_minutes Critical          2577            8   FAIL
          orders.delivery_minutes_below_120 Critical          2577            6   FAIL
      orders.discount_pct_between_0_and_100 Critical          2577            3   FAIL
        orders.order_amount_matches_formula Critical          2577           18   FAIL
            orders.order_id_unique_per_file Critical          2577           12   FAIL
                orders.order_status_allowed Critical          2577            4   FAIL
              orders.order_ts_not_in_future Critical          2577            8   FAIL
                   orders.order_ts_not_null Critical          2577            1   FAIL
                     orders.order_ts_parses Critical          2577            5   FAIL
                orders.payment_mode_allowed Critical          2577            2   FAIL
                   orders.product_in_master Critical          2577            9   FAIL
            orders.product_master_row_valid Critical          2577          140   FAIL
                   orders.quantity_positive Critical          2577           11   FAIL
                     orders.store_in_master Critical          2577            6   FAIL
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
etl_quarantine:
```
row_key         failure_reason
ORD900001   customer_id_not_null
ORD900002      quantity_positive
ORD900003        store_in_master
ORD900004   order_status_allowed
ORD900005 order_ts_not_in_future
```
elt_rejected_rows:
```
row_key         failure_reason
ORD900001   customer_id_not_null
ORD900002      quantity_positive
ORD900003        store_in_master
ORD900004   order_status_allowed
ORD900005 order_ts_not_in_future
```
