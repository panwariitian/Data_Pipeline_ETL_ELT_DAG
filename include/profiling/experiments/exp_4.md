## Experiment 4 - Missing source file (products.csv renamed)

### nammamart_etl_pradeep  run=exp4_etl_050458  state=failed  wall=73s  (products.csv missing)
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
```
pipeline_runs:
run_id status  rows_extracted  rows_loaded  rows_quarantined
exp4_etl_050458 FAILED            5220         2597                51
dq_results not PASS:
check_name severity  rows_checked  rows_failed status
                    customers.city_not_null  Warning           405            6   WARN
               customers.customer_id_unique Critical           405            5   FAIL
                     customers.email_format  Warning           405           12   WARN
              customers.loyalty_tier_casing  Warning           405            8   WARN
               customers.phone_is_10_digits  Warning           405            7   WARN
              customers.pincode_is_6_digits  Warning           405            3   WARN
        customers.signup_date_not_in_future Critical           405            3   FAIL
              payments.amount_paid_positive Critical          2228           10   FAIL
                 payments.payment_id_unique Critical          2228            4   FAIL
              payments.payment_mode_allowed Critical          2228            2   FAIL
              payments.payment_order_exists Critical          2228            8   FAIL
          payments.payment_ts_not_in_future Critical          2228            7   FAIL
payments.success_amount_equals_order_amount Critical          2228           14   FAIL
                   stores.capacity_positive  Warning            15            1   WARN
                       stores.zone_not_null  Warning            15            1   WARN
```

### nammamart_elt_pradeep  run=exp4_elt_050614  state=failed  wall=72s  (products.csv missing)
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
```
pipeline_runs:
run_id status  rows_extracted  rows_loaded  rows_quarantined
exp4_elt_050614 FAILED               0            0                 0
dq_results not PASS:
Empty DataFrame
Columns: [check_name, severity, rows_checked, rows_failed, status]
Index: []
```
file restored.
