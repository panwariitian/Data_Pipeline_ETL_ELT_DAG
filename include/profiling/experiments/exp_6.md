## Experiment 6 - Schema change (order_amount renamed to total)

### nammamart_etl_pradeep  run=exp6_etl_050810  state=failed  wall=68s  (order_amount renamed to total)
```
extract_payments | success | 1 | 12:08:13.071 | 1.6
extract_customers | success | 1 | 12:08:13.097 | 1.6
extract_products | success | 1 | 12:08:13.156 | 1.5
extract_stores | success | 1 | 12:08:13.158 | 1.6
validate_customers | success | 1 | 12:08:15.608 | 0.7
validate_products | success | 1 | 12:08:15.609 | 0.7
validate_stores | success | 1 | 12:08:15.614 | 0.7
extract_orders | failed | 3 | 12:09:16.276 | 0.5
reconcile_payments | upstream_failed | 0 | 12:09:17.333 | 0.0
validate_orders | upstream_failed | 0 | 12:09:17.337 | 0.0
validate_payments | upstream_failed | 0 | 12:09:17.338 | 0.0
report | upstream_failed | 0 | 12:09:18.400 | 0.0
transform | upstream_failed | 0 | 12:09:18.403 | 0.0
load_warehouse | upstream_failed | 0 | 12:09:19.449 | 0.0
run_audit | failed | 1 | 12:09:19.486 | 0.5
```
```
pipeline_runs:
run_id status  rows_extracted  rows_loaded  rows_quarantined
exp6_etl_050810 FAILED            2800          554                18
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
            customers.city_not_null  Warning           405            6   WARN
       customers.customer_id_unique Critical           405            5   FAIL
             customers.email_format  Warning           405           12   WARN
      customers.loyalty_tier_casing  Warning           405            8   WARN
       customers.phone_is_10_digits  Warning           405            7   WARN
      customers.pincode_is_6_digits  Warning           405            3   WARN
customers.signup_date_not_in_future Critical           405            3   FAIL
         products.category_not_null Critical           152            3   FAIL
  products.cost_price_not_above_mrp Critical           152            5   FAIL
              products.mrp_positive Critical           152            1   FAIL
         products.product_id_unique Critical           152            2   FAIL
           stores.capacity_positive  Warning            15            1   WARN
               stores.zone_not_null  Warning            15            1   WARN
```

### nammamart_elt_pradeep  run=exp6_elt_050922  state=failed  wall=72s  (order_amount renamed to total)
```
extract_payments | success | 1 | 12:09:24.777 | 0.7
extract_customers | success | 1 | 12:09:24.795 | 0.7
extract_stores | success | 1 | 12:09:24.804 | 0.6
extract_products | success | 1 | 12:09:24.817 | 0.6
extract_orders | failed | 3 | 12:10:26.620 | 0.3
load_raw | upstream_failed | 0 | 12:10:27.301 | 0.0
load_raw_increment | upstream_failed | 0 | 12:10:27.303 | 0.0
stg_customers | upstream_failed | 0 | 12:10:28.356 | 0.0
stg_products | upstream_failed | 0 | 12:10:29.418 | 0.0
stg_stores | upstream_failed | 0 | 12:10:30.485 | 0.0
stg_orders_merge | upstream_failed | 0 | 12:10:31.803 | 0.0
stg_payments | upstream_failed | 0 | 12:10:32.845 | 0.0
mart_category_margin | upstream_failed | 0 | 12:10:33.709 | 0.0
mart_daily_store_kpis | upstream_failed | 0 | 12:10:33.710 | 0.0
mart_delivery_sla | upstream_failed | 0 | 12:10:33.711 | 0.0
mart_payment_reconciliation | upstream_failed | 0 | 12:10:33.711 | 0.0
mart_rejections | upstream_failed | 0 | 12:10:33.712 | 0.0
mart_top_customers | upstream_failed | 0 | 12:10:33.712 | 0.0
report | upstream_failed | 0 | 12:10:34.754 | 0.0
run_audit | failed | 1 | 12:10:35.217 | 0.4
```
```
pipeline_runs:
run_id status  rows_extracted  rows_loaded  rows_quarantined
exp6_elt_050922 FAILED               0            0                 0
dq_results not PASS:
Empty DataFrame
Columns: [check_name, severity, rows_checked, rows_failed, status]
Index: []
```
