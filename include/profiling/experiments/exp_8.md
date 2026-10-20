## Experiment 8 - Distribution drift (unit_price x100 for store S05)
rows changed: 144
S05 revenue before:
```
store_id  net_revenue  orders
     S05     82229.75   134.0
```

### nammamart_etl_pradeep  run=exp8_etl_052055  state=success  wall=11s  (S05 unit_price x100)
```
extract_payments | success | 1 | 12:20:57.846 | 1.3
extract_customers | success | 1 | 12:20:57.884 | 1.1
extract_stores | success | 1 | 12:20:57.897 | 1.1
extract_orders | success | 1 | 12:20:57.908 | 1.1
extract_products | success | 1 | 12:20:57.963 | 1.0
validate_products | success | 1 | 12:20:59.069 | 0.5
validate_customers | success | 1 | 12:20:59.081 | 0.5
validate_stores | success | 1 | 12:20:59.083 | 0.5
validate_orders | success | 1 | 12:21:00.610 | 0.9
transform | success | 1 | 12:21:01.762 | 0.7
validate_payments | success | 1 | 12:21:01.766 | 0.8
load_warehouse | success | 1 | 12:21:02.918 | 0.4
reconcile_payments | success | 1 | 12:21:04.001 | 0.7
report | success | 1 | 12:21:05.147 | 0.4
run_audit | success | 1 | 12:21:06.249 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp8_etl_052055 SUCCESS            5372         4928               444
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
        orders.order_amount_matches_formula Critical          2572          162   FAIL
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
      orders.unit_price_within_20pct_of_mrp  Warning          2572          138   WARN
         orders.unit_price_within_5x_of_mrp Critical          2572          138   FAIL
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

### nammamart_elt_pradeep  run=exp8_elt_052110  state=success  wall=21s  (S05 unit_price x100)
```
extract_products | success | 1 | 12:21:12.538 | 0.5
extract_customers | success | 1 | 12:21:12.557 | 0.5
extract_orders | success | 1 | 12:21:12.560 | 0.5
extract_payments | success | 1 | 12:21:12.600 | 0.4
extract_stores | success | 1 | 12:21:12.601 | 0.4
load_raw | success | 1 | 12:21:13.684 | 0.7
load_raw_increment | success | 1 | 12:21:15.179 | 0.5
stg_customers | success | 1 | 12:21:16.279 | 0.3
stg_products | success | 1 | 12:21:17.399 | 0.3
stg_stores | success | 1 | 12:21:18.522 | 0.2
stg_orders_merge | success | 1 | 12:21:19.457 | 0.3
stg_payments | success | 1 | 12:21:20.570 | 0.2
mart_daily_store_kpis | success | 1 | 12:21:21.711 | 2.5
mart_category_margin | success | 1 | 12:21:21.718 | 4.5
mart_top_customers | success | 1 | 12:21:21.777 | 0.4
mart_rejections | success | 1 | 12:21:21.781 | 0.4
mart_delivery_sla | success | 1 | 12:21:21.790 | 2.4
mart_payment_reconciliation | success | 1 | 12:21:21.797 | 6.4
report | success | 1 | 12:21:29.170 | 0.4
run_audit | success | 1 | 12:21:30.554 | 0.3
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp8_elt_052110 SUCCESS            5372         4928               444
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
        orders.order_amount_matches_formula Critical          2572          162   FAIL
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
      orders.unit_price_within_20pct_of_mrp  Warning          2572          138   WARN
         orders.unit_price_within_5x_of_mrp Critical          2572          138   FAIL
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
S05 revenue after (mart):
```
store_id  net_revenue  orders
     S05     82229.75   134.0
```
S05 revenue after (etl fact):
```
store_id  net_revenue  orders
     S05     82229.75     134
```
