# Business questions: results from both pipelines

## Q1a. Daily net revenue by zone (delivered orders, after discount), latest 5 days

**ETL**
```sql
SELECT order_date, zone, delivered_orders, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date DESC, zone LIMIT 20
```
```
order_date    zone  delivered_orders  net_revenue
2026-10-01    East                50     21572.45
2026-10-01   North                26     14546.80
2026-10-01   South                22     14552.75
2026-10-01 Unknown                 5      2656.40
2026-10-01    West                33     18862.80
2026-09-30    East                17      8542.55
2026-09-30   North                 8      4396.40
2026-09-30   South                16      7989.95
2026-09-30 Unknown                 6      1702.10
2026-09-30    West                13      4438.95
2026-09-29    East                17      9185.15
2026-09-29   North                23     12641.35
2026-09-29   South                16     11095.55
2026-09-29 Unknown                 1       205.70
2026-09-29    West                17      8630.80
2026-09-28    East                14      6613.50
2026-09-28   North                11      7025.75
2026-09-28   South                11      3377.10
2026-09-28 Unknown                 4      4376.00
2026-09-28    West                16      7833.05
```

**ELT**
```sql
SELECT order_date, zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM mart_daily_store_kpis GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 20
```
```
order_date    zone  delivered_orders  net_revenue
2026-10-01    East              50.0     21572.45
2026-10-01   North              26.0     14546.80
2026-10-01   South              22.0     14552.75
2026-10-01 Unknown               5.0      2656.40
2026-10-01    West              33.0     18862.80
2026-09-30    East              17.0      8542.55
2026-09-30   North               8.0      4396.40
2026-09-30   South              16.0      7989.95
2026-09-30 Unknown               6.0      1702.10
2026-09-30    West              13.0      4438.95
2026-09-29    East              17.0      9185.15
2026-09-29   North              23.0     12641.35
2026-09-29   South              16.0     11095.55
2026-09-29 Unknown               1.0       205.70
2026-09-29    West              17.0      8630.80
2026-09-28    East              14.0      6613.50
2026-09-28   North              11.0      7025.75
2026-09-28   South              11.0      3377.10
2026-09-28 Unknown               4.0      4376.00
2026-09-28    West              16.0      7833.05
```

## Q1b. Daily net revenue by store, latest day

**ETL**
```sql
SELECT order_date, store_id, store_name, zone, net_revenue FROM etl_daily_store_kpis WHERE order_date = (SELECT max(order_date) FROM etl_daily_store_kpis) ORDER BY store_id
```
```
order_date store_id                  store_name    zone  net_revenue
2026-10-01      S01            NammaMart Hebbal   North      6780.25
2026-10-01      S02         NammaMart Yelahanka   North      4070.80
2026-10-01      S03          NammaMart RT Nagar   North      3695.75
2026-10-01      S04         NammaMart Jayanagar   South      2168.00
2026-10-01      S05          NammaMart JP Nagar   South      7491.20
2026-10-01      S06      NammaMart Banashankari   South      4893.55
2026-10-01      S07        NammaMart BTM Layout Unknown      2656.40
2026-10-01      S08        NammaMart Whitefield    East      6200.90
2026-10-01      S09       NammaMart Indiranagar    East      2610.05
2026-10-01      S10      NammaMart Marathahalli    East      4522.90
2026-10-01      S11        NammaMart HSR Layout    East      8238.60
2026-10-01      S12       NammaMart Rajajinagar    West      5554.25
2026-10-01      S13      NammaMart Malleshwaram    West      4498.65
2026-10-01      S14       NammaMart Vijayanagar    West      6145.50
2026-10-01      S15 NammaMart Basaveshwaranagar    West      2664.40
```

**ELT**
```sql
SELECT order_date, store_id, store_name, zone, net_revenue FROM mart_daily_store_kpis WHERE order_date = (SELECT max(order_date) FROM mart_daily_store_kpis) ORDER BY store_id
```
```
order_date store_id                  store_name    zone  net_revenue
2026-10-01      S01            NammaMart Hebbal   North      6780.25
2026-10-01      S02         NammaMart Yelahanka   North      4070.80
2026-10-01      S03          NammaMart RT Nagar   North      3695.75
2026-10-01      S04         NammaMart Jayanagar   South      2168.00
2026-10-01      S05          NammaMart JP Nagar   South      7491.20
2026-10-01      S06      NammaMart Banashankari   South      4893.55
2026-10-01      S07        NammaMart BTM Layout Unknown      2656.40
2026-10-01      S08        NammaMart Whitefield    East      6200.90
2026-10-01      S09       NammaMart Indiranagar    East      2610.05
2026-10-01      S10      NammaMart Marathahalli    East      4522.90
2026-10-01      S11        NammaMart HSR Layout    East      8238.60
2026-10-01      S12       NammaMart Rajajinagar    West      5554.25
2026-10-01      S13      NammaMart Malleshwaram    West      4498.65
2026-10-01      S14       NammaMart Vijayanagar    West      6145.50
2026-10-01      S15 NammaMart Basaveshwaranagar    West      2664.40
```

## Q2. Revenue and gross margin by category

**ETL**
```sql
SELECT category, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC
```
```
           category  net_revenue  gross_margin  margin_pct
            Staples    264456.90      66435.28       25.12
          Household    210229.95      54055.27       25.71
      Personal Care    208774.70      46709.57       22.37
     Dairy & Bakery    177404.85      47200.82       26.61
 Snacks & Beverages    174575.60      41339.24       23.68
Fruits & Vegetables     92968.45      22018.22       23.68
```

**ELT**
```sql
SELECT category, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC
```
```
           category  net_revenue  gross_margin  margin_pct
            Staples    264456.90      66435.28       25.12
          Household    210229.95      54055.27       25.71
      Personal Care    208774.70      46709.57       22.37
     Dairy & Bakery    177404.85      47200.82       26.61
 Snacks & Beverages    174575.60      41339.24       23.68
Fruits & Vegetables     92968.45      22018.22       23.68
```

## Q3. Average delivery time per store and % delivered within 15 minutes

**ETL**
```sql
SELECT store_id, round(avg(delivery_minutes), 2) AS avg_delivery_minutes, round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1
```
```
store_id  avg_delivery_minutes  pct_within_15_min
     S01                 20.62              31.21
     S02                 20.99              30.94
     S03                 20.71              29.20
     S04                 21.59              26.92
     S05                 21.24              29.10
     S06                 22.33              22.07
     S07                 21.10              33.57
     S08                 20.60              35.48
     S09                 21.14              27.56
     S10                 21.90              26.95
     S11                 20.97              27.78
     S12                 21.99              23.89
     S13                 21.80              27.01
     S14                 21.50              31.48
     S15                 22.33              20.98
```

**ELT**
```sql
SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1
```
```
store_id  avg_delivery_minutes  pct_within_15_min
     S01                 20.62              31.21
     S02                 20.99              30.94
     S03                 20.71              29.20
     S04                 21.59              26.92
     S05                 21.24              29.10
     S06                 22.33              22.07
     S07                 21.10              33.57
     S08                 20.60              35.48
     S09                 21.14              27.56
     S10                 21.90              26.95
     S11                 20.97              27.78
     S12                 21.99              23.89
     S13                 21.80              27.01
     S14                 21.50              31.48
     S15                 22.33              20.98
```

## Q4a. Revenue by payment mode

**ETL**
```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC
```
```
    payment_mode  delivered_orders  net_revenue
             UPI              1131    598618.20
            Card               428    231787.85
Cash on Delivery               298    169026.60
          Wallet               234    128977.80
```

**ELT**
```sql
SELECT payment_mode, delivered_orders, net_revenue FROM mart_payment_mode_revenue ORDER BY 3 DESC
```
```
    payment_mode  delivered_orders  net_revenue
             UPI              1131    598618.20
            Card               428    231787.85
Cash on Delivery               298    169026.60
          Wallet               234    128977.80
```

## Q4b. Amount that does not reconcile with the payment gateway, by reason

**ETL**
```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC
```
```
                   mismatch_reason  rows    amount
         PAYMENT_ORDER_QUARANTINED   213 152863.63
DELIVERED_PAYMENT_NOT_YET_RECEIVED   136  72191.20
          DELIVERED_PAYMENT_FAILED    42  25722.25
              DELIVERED_NO_PAYMENT    26  16381.35
             PAYMENT_WITHOUT_ORDER     8   4580.57
             RETURNED_NOT_REFUNDED     9   3662.70
        RETURNED_NO_PAYMENT_RECORD     1    812.00
                   AMOUNT_MISMATCH    12    660.00
```

**ELT**
```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC
```
```
                   mismatch_reason  rows    amount
         PAYMENT_ORDER_QUARANTINED   213 152863.63
DELIVERED_PAYMENT_NOT_YET_RECEIVED   136  72191.20
          DELIVERED_PAYMENT_FAILED    42  25722.25
              DELIVERED_NO_PAYMENT    26  16381.35
             PAYMENT_WITHOUT_ORDER     8   4580.57
             RETURNED_NOT_REFUNDED     9   3662.70
        RETURNED_NO_PAYMENT_RECORD     1    812.00
                   AMOUNT_MISMATCH    12    660.00
```

## Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)

**ETL**
```sql
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank, f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked, round(sum(f.net_revenue), 2) AS total_spend FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED' GROUP BY f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked ORDER BY 1 LIMIT 10
```
```
 spend_rank customer_id loyalty_tier      email_masked phone_masked  total_spend
          1      C00136       Bronze sa***@example.com   96******88      8738.10
          2      C00236     Platinum sn***@example.com   90******01      7831.80
          3      C00115       Bronze sa***@example.com   96******85      7337.70
          4      C00310       Bronze     ki***@example   91******35      7258.95
          5      C00265         Gold vi***@example.com   91******69      7151.30
          6      C00338       Silver bh***@example.com   94******05      6848.55
          7      C00283       Bronze ni***@example.com   91******43      6766.40
          8      C00357       Bronze ar***@example.com   **********      6637.90
          9      C00209     Platinum ka***@example.com   98******97      6519.40
         10      C00072       Silver na***@example.com   98******05      6433.95
```

**ELT**
```sql
SELECT spend_rank, customer_id, loyalty_tier, email_masked, phone_masked, total_spend FROM mart_top_customers WHERE spend_rank <= 10 ORDER BY 1
```
```
 spend_rank customer_id loyalty_tier      email_masked phone_masked  total_spend
          1      C00136       Bronze sa***@example.com   96******88      8738.10
          2      C00236     Platinum sn***@example.com   90******01      7831.80
          3      C00115       Bronze sa***@example.com   96******85      7337.70
          4      C00310       Bronze     ki***@example   91******35      7258.95
          5      C00265         Gold vi***@example.com   91******69      7151.30
          6      C00338       Silver bh***@example.com   94******05      6848.55
          7      C00283       Bronze ni***@example.com   91******43      6766.40
          8      C00357       Bronze ar***@example.com   **********      6637.90
          9      C00209     Platinum ka***@example.com   98******97      6519.40
         10      C00072       Silver na***@example.com   98******05      6433.95
```

## Q6. Rows rejected in the latest run, by file and reason

**ETL**
```sql
SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM etl_quarantine WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_pradeep' ORDER BY end_time DESC LIMIT 1)) GROUP BY 1, 2 ORDER BY 1, 3 DESC
```
```
source_file                     failure_reason  rows_rejected
  customers                 customer_id_unique              5
  customers          signup_date_not_in_future              3
     orders           product_master_row_valid            140
     orders          customer_master_row_valid             19
     orders       order_amount_matches_formula             18
     orders               customer_id_not_null             18
     orders           order_id_unique_per_file             12
     orders                  quantity_positive             10
     orders                  product_in_master              9
     orders     delivered_has_delivery_minutes              8
     orders             order_ts_not_in_future              7
     orders         delivery_minutes_below_120              6
     orders                    order_ts_parses              5
     orders                    store_in_master              5
     orders     discount_pct_between_0_and_100              3
     orders               order_status_allowed              3
     orders               payment_mode_allowed              2
     orders                  order_ts_not_null              1
   payments success_amount_equals_order_amount             14
   payments               amount_paid_positive             10
   payments               payment_order_exists              8
   payments           payment_ts_not_in_future              7
   payments                  payment_id_unique              4
   payments               payment_mode_allowed              2
   products           cost_price_not_above_mrp              5
   products                  category_not_null              3
   products                  product_id_unique              2
   products                       mrp_positive              1
```

**ELT**
```sql
SELECT source_file, failure_reason, rows_rejected FROM mart_rejections WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_pradeep' ORDER BY end_time DESC LIMIT 1) ORDER BY 1, 3 DESC
```
```
source_file                     failure_reason  rows_rejected
  customers                 customer_id_unique              5
  customers          signup_date_not_in_future              3
     orders           product_master_row_valid            140
     orders          customer_master_row_valid             19
     orders               customer_id_not_null             18
     orders       order_amount_matches_formula             18
     orders           order_id_unique_per_file             12
     orders                  quantity_positive             10
     orders                  product_in_master              9
     orders     delivered_has_delivery_minutes              8
     orders             order_ts_not_in_future              7
     orders         delivery_minutes_below_120              6
     orders                    order_ts_parses              5
     orders                    store_in_master              5
     orders     discount_pct_between_0_and_100              3
     orders               order_status_allowed              3
     orders               payment_mode_allowed              2
     orders                  order_ts_not_null              1
   payments success_amount_equals_order_amount             14
   payments               amount_paid_positive             10
   payments               payment_order_exists              8
   payments           payment_ts_not_in_future              7
   payments                  payment_id_unique              4
   payments               payment_mode_allowed              2
   products           cost_price_not_above_mrp              5
   products                  category_not_null              3
   products                  product_id_unique              2
   products                       mrp_positive              1
```
