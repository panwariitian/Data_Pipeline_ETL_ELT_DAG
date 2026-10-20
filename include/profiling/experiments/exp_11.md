## Experiment 8b - Consistent drift (unit_price AND order_amount x100 for store S05)
rows changed: 144
S05 revenue before (mart):
```
store_id  net_revenue  orders
     S05     82229.75   134.0
```

### nammamart_etl_pradeep  run=exp11_etl_052528  state=success  wall=11s  (S05 price and amount x100)
```
extract_customers | success | 1 | 12:25:30.615 | 1.1
extract_payments | success | 1 | 12:25:30.616 | 1.1
extract_orders | success | 1 | 12:25:30.620 | 1.1
extract_stores | success | 1 | 12:25:30.637 | 1.0
extract_products | success | 1 | 12:25:30.656 | 1.0
validate_customers | success | 1 | 12:25:32.798 | 0.5
validate_stores | success | 1 | 12:25:32.806 | 0.5
validate_products | success | 1 | 12:25:32.817 | 0.4
validate_orders | success | 1 | 12:25:33.941 | 0.4
transform | success | 1 | 12:25:35.026 | 0.5
validate_payments | success | 1 | 12:25:35.026 | 0.5
load_warehouse | success | 1 | 12:25:35.972 | 0.4
reconcile_payments | success | 1 | 12:25:37.081 | 0.4
report | success | 1 | 12:25:38.182 | 0.4
run_audit | success | 1 | 12:25:39.265 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp11_etl_052528 SUCCESS            5372         4920               452
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
      orders.unit_price_within_20pct_of_mrp  Warning          2572          138   WARN
         orders.unit_price_within_5x_of_mrp Critical          2572          138   FAIL
              payments.amount_paid_positive Critical          2228           10   FAIL
                 payments.payment_id_unique Critical          2228            4   FAIL
              payments.payment_mode_allowed Critical          2228            2   FAIL
              payments.payment_order_exists Critical          2228            8   FAIL
          payments.payment_ts_not_in_future Critical          2228            7   FAIL
payments.success_amount_equals_order_amount Critical          2228           23   FAIL
                 products.category_not_null Critical           152            3   FAIL
          products.cost_price_not_above_mrp Critical           152            5   FAIL
                      products.mrp_positive Critical           152            1   FAIL
                 products.product_id_unique Critical           152            2   FAIL
                   stores.capacity_positive  Warning            15            1   WARN
                       stores.zone_not_null  Warning            15            1   WARN
```

### nammamart_elt_pradeep  run=exp11_elt_052541  state=success  wall=21s  (S05 price and amount x100)
```
extract_payments | success | 1 | 12:25:43.785 | 0.5
extract_orders | success | 1 | 12:25:43.800 | 0.5
extract_stores | success | 1 | 12:25:43.802 | 0.5
extract_products | success | 1 | 12:25:43.813 | 0.5
extract_customers | success | 1 | 12:25:43.821 | 0.5
load_raw | success | 1 | 12:25:44.801 | 0.9
load_raw_increment | success | 1 | 12:25:45.935 | 0.4
stg_customers | success | 1 | 12:25:47.023 | 0.2
stg_products | success | 1 | 12:25:48.150 | 0.3
stg_stores | success | 1 | 12:25:49.306 | 0.2
stg_orders_merge | success | 1 | 12:25:50.432 | 0.3
stg_payments | success | 1 | 12:25:51.713 | 0.2
mart_delivery_sla | success | 1 | 12:25:52.872 | 2.7
mart_payment_reconciliation | success | 1 | 12:25:52.878 | 0.7
mart_daily_store_kpis | success | 1 | 12:25:52.894 | 6.8
mart_category_margin | success | 1 | 12:25:52.921 | 2.8
mart_top_customers | success | 1 | 12:25:52.943 | 0.6
mart_rejections | success | 1 | 12:25:52.984 | 4.7
report | success | 1 | 12:25:59.827 | 0.7
run_audit | success | 1 | 12:26:00.702 | 0.4
```
```
pipeline_runs:
run_id  status  rows_extracted  rows_loaded  rows_quarantined
exp11_elt_052541 SUCCESS            5372         4920               452
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
      orders.unit_price_within_20pct_of_mrp  Warning          2572          138   WARN
         orders.unit_price_within_5x_of_mrp Critical          2572          138   FAIL
              payments.amount_paid_positive Critical          2228           10   FAIL
                 payments.payment_id_unique Critical          2228            4   FAIL
              payments.payment_mode_allowed Critical          2228            2   FAIL
              payments.payment_order_exists Critical          2228            8   FAIL
          payments.payment_ts_not_in_future Critical          2228            7   FAIL
payments.success_amount_equals_order_amount Critical          2228           23   FAIL
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
