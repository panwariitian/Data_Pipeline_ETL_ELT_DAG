## Experiment 10 - Recovery (clear failed task after fixing the cause)

### nammamart_etl_pradeep run=exp4_etl_050458: states before clear
```
extract_stores | success | 1 | 12:05:01.726 | 1.2
extract_payments | success | 1 | 12:05:01.740 | 1.3
extract_orders | success | 1 | 12:05:01.792 | 1.2
extract_customers | success | 1 | 12:05:01.807 | 1.1
validate_stores | success | 1 | 12:05:03.995 | 0.5
validate_payments | success | 1 | 12:05:03.997 | 0.6
validate_customers | success | 1 | 12:05:04.000 | 0.5
extract_products | failed | 3 | 12:06:04.876 | 0.4
validate_orders | upstream_failed | 0 | 12:06:06.305 | 0.0
validate_products | upstream_failed | 0 | 12:06:06.308 | 0.0
transform | upstream_failed | 0 | 12:06:07.369 | 0.0
load_warehouse | upstream_failed | 0 | 12:06:07.711 | 0.0
reconcile_payments | upstream_failed | 0 | 12:06:08.770 | 0.0
report | upstream_failed | 0 | 12:06:09.821 | 0.0
run_audit | failed | 1 | 12:06:10.930 | 0.4
```
after clear: run state=success wall=0s
```
extract_stores | success | 1 | 12:05:01.726 | 1.2
extract_payments | success | 1 | 12:05:01.740 | 1.3
extract_orders | success | 1 | 12:05:01.792 | 1.2
extract_customers | success | 1 | 12:05:01.807 | 1.1
validate_stores | success | 1 | 12:05:03.995 | 0.5
validate_payments | success | 1 | 12:05:03.997 | 0.6
validate_customers | success | 1 | 12:05:04.000 | 0.5
extract_products | success | 4 | 12:11:58.645 | 0.5
validate_products | success | 1 | 12:11:59.769 | 0.4
validate_orders | success | 1 | 12:12:00.850 | 0.4
transform | success | 1 | 12:12:01.950 | 0.4
load_warehouse | success | 1 | 12:12:03.052 | 0.4
reconcile_payments | success | 1 | 12:12:04.178 | 0.5
report | success | 1 | 12:12:05.259 | 0.3
run_audit | success | 2 | 12:12:06.367 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp4_etl_050458 SUCCESS            5372         5061               311
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

### nammamart_elt_pradeep run=exp4_elt_050614: states before clear
```
extract_customers | success | 1 | 12:06:16.378 | 0.6
extract_orders | success | 1 | 12:06:16.441 | 0.6
extract_stores | success | 1 | 12:06:16.449 | 0.6
extract_payments | success | 1 | 12:06:16.475 | 0.5
extract_products | failed | 3 | 12:07:18.829 | 0.3
load_raw | upstream_failed | 0 | 12:07:19.872 | 0.0
load_raw_increment | upstream_failed | 0 | 12:07:19.876 | 0.0
stg_customers | upstream_failed | 0 | 12:07:20.938 | 0.0
stg_products | upstream_failed | 0 | 12:07:21.988 | 0.0
stg_stores | upstream_failed | 0 | 12:07:23.032 | 0.0
stg_orders_merge | upstream_failed | 0 | 12:07:24.070 | 0.0
stg_payments | upstream_failed | 0 | 12:07:25.117 | 0.0
mart_category_margin | upstream_failed | 0 | 12:07:25.645 | 0.0
mart_daily_store_kpis | upstream_failed | 0 | 12:07:25.646 | 0.0
mart_delivery_sla | upstream_failed | 0 | 12:07:25.647 | 0.0
mart_payment_reconciliation | upstream_failed | 0 | 12:07:25.648 | 0.0
mart_rejections | upstream_failed | 0 | 12:07:25.649 | 0.0
mart_top_customers | upstream_failed | 0 | 12:07:25.649 | 0.0
report | upstream_failed | 0 | 12:07:26.699 | 0.0
run_audit | failed | 1 | 12:07:26.832 | 0.4
```
after clear: run state=success wall=11s
```
extract_customers | success | 1 | 12:06:16.378 | 0.6
extract_orders | success | 1 | 12:06:16.441 | 0.6
extract_stores | success | 1 | 12:06:16.449 | 0.6
extract_payments | success | 1 | 12:06:16.475 | 0.5
extract_products | success | 4 | 12:12:09.460 | 0.2
load_raw | success | 1 | 12:12:10.570 | 0.8
load_raw_increment | success | 1 | 12:12:11.693 | 0.4
stg_customers | success | 1 | 12:12:12.593 | 0.2
stg_products | success | 1 | 12:12:13.749 | 0.2
stg_stores | success | 1 | 12:12:14.821 | 0.2
stg_orders_merge | success | 1 | 12:12:15.950 | 0.2
stg_payments | success | 1 | 12:12:17.059 | 0.2
mart_category_margin | success | 1 | 12:12:18.171 | 0.5
mart_delivery_sla | success | 1 | 12:12:18.182 | 0.4
mart_payment_reconciliation | success | 1 | 12:12:18.217 | 2.5
mart_rejections | success | 1 | 12:12:18.223 | 6.5
mart_daily_store_kpis | success | 1 | 12:12:18.226 | 0.5
mart_top_customers | success | 1 | 12:12:18.241 | 4.4
report | success | 1 | 12:12:25.803 | 0.5
run_audit | success | 2 | 12:12:26.880 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp4_elt_050614 SUCCESS            5372         5061               311
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
