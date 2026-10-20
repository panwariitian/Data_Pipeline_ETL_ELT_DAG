# NammaMart quick-commerce pipelines: ETL and ELT in Airflow

Pradeep Singh Panwar, FDAIE Module 2

Everything below comes from runs on my machine (Astro runtime 3.3 on Colima/Docker). Where a number appears, the query or log that produced it is in `include/profiling/` or `docs/screenshots/`.

## 0. What is in the repo

| Path | What it is |
|---|---|
| `dags/nammamart_etl_pradeep.py`, `dags/nammamart_elt_pradeep.py` | The two DAGs. They only wire tasks together. |
| `include/nammamart/` | `config.py`, `quality.py` (ETL rules), `masking.py`, `etl_core.py`, `elt_core.py`, `alerts.py` (Slack failure alert), `dq_trend.py` (quality chart) |
| `include/sql/elt/*.sql` | Every ELT transformation: init, five staging files, mart files (including `46_mart_store_capacity.sql`) |
| `include/sql/business_questions.sql` | The six business questions, once for ETL tables and once for ELT marts |
| `include/profiling/` | Day-one profiling scripts, `data_issues_log.csv`, `business_questions_results.md`, `experiments/exp_*.md` |
| `tests/` | `run_experiments.py` (Part F), `business_questions.py`, `take_screenshots.py` |
| `docs/screenshots/` | Evidence (index in section 8) |

Warehouse: `include/warehouse_pradeep.duckdb` (git-ignored). Quarantine: `include/quarantine/pradeep/`.

To run: `astro dev start`, unpause both DAGs, trigger them. Both finish in 10-30 seconds.

**Both pipelines give the same answers.** After the final run:

| Check | ETL | ELT |
|---|---|---|
| Total net revenue (delivered, after discount) | 1,128,410.45 | 1,128,410.45 |
| Rows rejected | 311 | 311 |
| Store/day KPI rows that differ between the two | 0 | 0 |
| Payment reconciliation mismatches | 447 | 447 (0 rows differ) |
| `dq_results` rows where `rows_failed` differs | 0 of 65 checks | 0 of 65 checks |

---

## 1. Part 0: Discovery and data profiling

I profiled all six files before writing any pipeline code (`include/profiling/profile_data.py`, read-only). Row counts match the brief: orders 2,412, payments 2,228, customers 405, products 152, stores 15, increment 160 (5,372 in total, plus the increment).

**What I checked:** null counts per column, duplicate keys, min/max of numeric and date columns, distinct values of every category column, the four foreign-key relationships, and every business rule from section 3 of the brief (amount formula, allowed statuses and payment modes, delivery range, SUCCESS payment equals order amount).

**Headline findings**

- Orders: 12 exact duplicate order_ids; 18 null customer_ids; 10 quantities of zero or below; discount_pct up to 250; 18 amounts that do not match quantity x price x (1 - discount); timestamps such as `N/A`, `2026-13-05 11:00:00` and `31/09/2026 10:00` (5 unparseable); 7 timestamps in the future (late Oct 2026 to Jan 2027); 8 DELIVERED orders with no delivery time; 6 delivery times over 120 minutes (max 1,440); statuses `delivered` (2, fixable) and `Pending??` (3, not fixable); payment modes `upi`, `UPI `, `CARD`, `cod` (fixable) and `Cashh` (not fixable).
- Referential integrity: 9 orders point at products that do not exist and 5 at stores that do not exist; every customer_id that is present exists. 8 payments point at orders that do not exist. No cancelled order has a payment. 27 DELIVERED orders have no payment in the file.
- Payments: 4 duplicate payment_ids; 10 non-positive amounts; 14 SUCCESS payments whose amount differs from the order amount.
- Customers: 5 customer_ids appear twice with different phone/area/pincode; 12 bad emails; 7 phones that are not 10 digits; 3 bad pincodes; tiers in `BRONZE`/`bronze`; 3 signup dates in the future; 6 missing cities.
- Products: 2 duplicate product_ids (one copy is named "... (old)" with a different mrp); 3 missing categories; 1 mrp below zero; 5 products where cost_price is above mrp.
- Stores: one missing zone (S07), one store with capacity 0 (S12).
- Increment file: 160 rows, 10 corrections to existing orders (all `RETURNED`) and 150 new orders. All new orders are dated 1 Oct, so none of them touches September revenue.

### Data Issues Log

246 individual issues across all five data files (orders 143, payments 46, customers 44, products 11, stores 2). The full row-level log, one line per issue, is `include/profiling/data_issues_log.csv`. This table groups it by file and column. Severity follows what the pipelines do: Critical rows are quarantined, Warning rows load and are counted in `dq_results`.

| File | Column | Problem (example) | Rows | Example keys | Dimension | Severity | Proposed rule |
|---|---|---|---|---|---|---|---|
| orders | customer_id | null customer_id | 18 | ORD002255, ORD000701 | Completeness | Critical | customer_id not null |
| orders | delivery_minutes | DELIVERED without delivery_minutes | 8 | ORD001554, ORD001800 | Completeness | Critical | required for DELIVERED |
| orders | delivery_minutes | 480 minutes is implausible | 6 | ORD001037, ORD002101 | Accuracy | Critical | delivery_minutes < 120 |
| orders | discount_pct | discount 250% outside 0-100 | 3 | ORD002103, ORD001468 | Validity | Critical | 0 <= discount_pct <= 100 |
| orders | order_amount | amount 492.0 does not match formula | 18 | ORD002103, ORD002305 | Accuracy | Critical | recompute amount, compare |
| orders | order_id | exact duplicate row | 12 | ORD001462, ORD001441 | Uniqueness | Critical | keep first row per order_id |
| orders | order_status | 'delivered' differs only in casing/whitespace | 2 | ORD001066, ORD000153 | Consistency | Warning | standardise with trim/upper |
| orders | order_status | 'Pending??' is not an allowed status | 3 | ORD000500, ORD001970 | Validity | Critical | status must be DELIVERED/CANCELLED/RETURNED |
| orders | order_status | DELIVERED but no payment record in payments.csv | 27 | ORD001113, ORD000737 | Consistency | Warning | delivered orders reconcile to a payment |
| orders | order_ts | unparseable timestamp 'N/A' | 5 | ORD000403, ORD001610 | Validity | Critical | order_ts must parse |
| orders | order_ts | future-dated 2027-01-01 09:00:00 | 7 | ORD002119, ORD001693 | Timeliness | Critical | order_ts <= run time |
| orders | payment_mode | 'Cashh' is not a recognisable payment mode | 2 | ORD001812, ORD001414 | Validity | Critical | payment_mode in allowed list |
| orders | payment_mode | 'upi' is a known variant | 8 | ORD000159, ORD001364 | Consistency | Warning | map known variants to one spelling |
| orders | product_id | P9455 not in products | 9 | ORD000986, ORD001058 | Consistency | Critical | product_id must exist in products |
| orders | quantity | quantity 0 is not positive | 10 | ORD000469, ORD001296 | Accuracy | Critical | quantity > 0 |
| orders | store_id | S99 not in stores | 5 | ORD001799, ORD001046 | Consistency | Critical | store_id must exist in stores |
| payments | amount_paid | paid -43.05 vs order 56.95 | 14 | PAY0001568, PAY0000354 | Consistency | Critical | SUCCESS amount = order_amount |
| payments | amount_paid | amount -43.05 is not positive | 10 | PAY0001568, PAY0002175 | Accuracy | Critical | amount_paid > 0 |
| payments | order_id | ORD970544 not in orders | 8 | PAY0002219, PAY0002220 | Consistency | Critical | payment must match an order |
| payments | payment_id | exact duplicate row | 4 | PAY0000509, PAY0001651 | Uniqueness | Critical | keep first per payment_id |
| payments | payment_mode | 'upi' is a known variant | 8 | PAY0002188, PAY0001850 | Consistency | Warning | map known variants to one spelling |
| payments | payment_mode | 'Cashh' is not a recognisable payment mode | 2 | PAY0000522, PAY0000044 | Validity | Critical | payment_mode in allowed list |
| customers | city | null city | 6 | C00355, C00394 | Completeness | Warning | city not null |
| customers | customer_id | duplicate id with differing attributes | 5 | C00103, C00252 | Uniqueness | Critical | one row per customer_id |
| customers | email | invalid email format | 12 | C00337, C00307 | Validity | Warning | email regex |
| customers | loyalty_tier | 'SILVER' inconsistent casing | 8 | C00199, C00171 | Consistency | Warning | title-case tier |
| customers | phone | phone is not 10 digits | 7 | C00151, C00240 | Validity | Warning | 10 digits |
| customers | pincode | '5600XX' not 6 digits | 3 | C00132, C00083 | Validity | Warning | 6 digits |
| customers | signup_date | future-dated 2027-01-05 | 3 | C00400, C00398 | Timeliness | Critical | signup_date <= run time |
| products | category | null category | 3 | P0051, P0060 | Completeness | Critical | category not null |
| products | cost_price | cost 264.0 > mrp 220.0 | 5 | P0019, P0073 | Accuracy | Critical | cost_price <= mrp |
| products | mrp | mrp -203.0 not positive | 1 | P0109 | Accuracy | Critical | mrp > 0 |
| products | product_id | duplicate id | 2 | P0030, P0140 | Uniqueness | Critical | one row per product_id |
| stores | capacity_orders_per_day | capacity 0 | 1 | S12 | Accuracy | Warning | capacity > 0 |
| stores | zone | null zone | 1 | S07 | Completeness | Warning | zone not null (load as Unknown, flag) |

### Three questions I would ask the client before building

1. **A product record that fails validation (no category, cost above mrp) currently causes every order for that product to be rejected: 140 orders, about 6% of the file.** Should the order still count for revenue with an "unknown category" label, or is losing it the right call until the catalogue is fixed? Finance and ops will probably answer differently.
2. **Which copy of a duplicated customer is the right one?** C00036 appears twice with different phone and area. I keep the first row and quarantine the second. Does CRM have a "last updated" field, or a rule such as "latest wins"?
3. **Is the payment feed meant to lag the order feed?** The 136 delivered orders from the 1 Oct increment have no payment rows, while the payments file stops at 30 Sep. I label those `DELIVERED_PAYMENT_NOT_YET_RECEIVED` and keep them apart from genuinely missing payments. If the gateway is supposed to be same-day, they are real problems.

---

## 2. Part C: Scheduling (answers)

Both DAGs use `schedule=CronTriggerTimetable("0 9 * * 1-6", timezone="Asia/Kolkata")` and `start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`, `catchup=False`, `max_active_runs=1`. `default_args`: `retries=2`, `retry_delay=30s`, `execution_timeout=15 min`.

**What is your cron expression?** `0 9 * * 1-6`: 09:00 every Monday to Saturday, interpreted in Asia/Kolkata. Sunday is skipped. Airflow's own "next runs" list confirms it: `2026-10-12T03:30Z, 10-13, 10-14, 10-15`, a Monday to Thursday run, and 11 Oct (a Sunday) was skipped.

**What time does the run show in UTC in the Airflow UI?** 03:30 UTC (IST is UTC+5:30). The scheduled run that Airflow created is `scheduled__2026-10-10T03:30:00+00:00`. The UI shows times in the browser's timezone: `docs/screenshots/02_etl_schedule_ist.png` shows next run 2026-10-12 09:00:00, and `04_etl_schedule_utc.png` shows the same run at 03:30 when the UI is switched to UTC.

**What would happen at 9 AM if the start_date were in UTC?** The schedule would be read as 09:00 UTC, which is 14:30 IST. The numbers would land five hours after the 9:30 AM review. I did not run that variant; this is from how the timetable is defined.

**What happens if my laptop or Docker is off at 9 AM?** Nothing runs, because the scheduler is the thing that is off. I saw what happens next: I unpaused the DAG on Sunday 11 Oct at about 17:17 IST, and Airflow immediately created and ran `scheduled__2026-10-10T03:30:00+00:00`, the most recent missed slot (Saturday 9 AM), and only that one. That is what `catchup=False` does.

**Why `catchup=False`?** Each run reads the full current files, not a date partition, and the loads are idempotent. Replaying thirty missed mornings would redo the same work thirty times and put thirty reports in the logs for no benefit. One late run is enough. If the laptop is off for a week, finance gets one run on return, not seven.

**Retries and timeout.** Two retries, 30 seconds apart, covers transient problems (a file still being copied, another process holding the DuckDB lock). A full run takes 10-30 seconds, so 15 minutes is a generous ceiling that still stops a hung task. Experiment 6 showed the cost: a schema error is not transient, and three attempts delayed the failure by about a minute.

---

## 3. Part A and B summary: how the pipelines are built

**ETL** (`dags/nammamart_etl_pradeep.py`): five parallel extract tasks, then validate the three masters, then orders, then payments, then transform, load, reconcile, report, and a final audit task. Failed rows are written to `include/quarantine/pradeep/<file>_failed_<run_id>.csv` with a `failure_reason` column and also to `etl_quarantine`. The star schema is `etl_fact_orders` plus `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. Load is an upsert by key inside a transaction, with a guard so an older file cannot overwrite a row a newer file already corrected. Reconciliation is `etl_payment_reconciliation`; reports are `etl_daily_zone_revenue`, `etl_daily_store_kpis`, `etl_category_margin`.

**ELT** (`dags/nammamart_elt_pradeep.py`): five parallel extract checks, `load_raw` copies each CSV as text into `raw_*` tables with `_loaded_at`, `_source_file`, `_run_id` (and `_row_num`, `_source_seq`), `load_raw_increment` appends the next-day file, then staging SQL (`stg_customers`, `stg_products`, `stg_stores`, `stg_orders` via `MERGE`, `stg_payments`), then six mart tasks that run in parallel. Nothing is cleaned in Python; every rule is in `include/sql/elt/*.sql`.

**Decisions I made that you should know about**

- `stg_orders` keeps one row per order_id: a passing version beats a failing one, then the newest file wins. A failing correction therefore never overwrites a good row. The ETL upsert follows the same rule so the two stay in step.
- Unrecognisable values (`Pending??`, `Cashh`) are rejected. Known variants (`upi`, `UPI `, `delivered`, `cod`) are standardised and counted as warnings.
- A missing store zone is a Warning (the store loads with zone `Unknown`). Making it Critical would reject every order from that store.
- `full_name` is not carried into any reporting table (data minimisation).
- Payments are judged against the best known version of each order (loaded fact, then today's file), not only against today's file. Experiment 7 showed why.

---

## 4. Part D: Data quality dimensions

All six dimensions are covered in both pipelines. Python in ETL (`include/nammamart/quality.py`), SQL in ELT (`include/sql/elt/`). Each run writes every check to `dq_results` (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at). Each pipeline has **64 checks: 49 Critical, 15 Warning.**

| Dimension | Critical | Warning | Examples |
|---|---|---|---|
| Completeness | 10 | 4 | `orders.customer_id_not_null`, `delivered_has_delivery_minutes`, `products.category_not_null`, `payments.gateway_ref_present_for_online` (warning) |
| Uniqueness | 5 | 0 | `order_id_unique_per_file`, `payment_id_unique`, `customer_id_unique`, `product_id_unique`, `store_id_unique` |
| Validity | 12 | 7 | status and payment mode in allowed list, discount 0-100, timestamp parses; email, phone, pincode, channel as warnings |
| Accuracy | 8 | 2 | `quantity_positive`, `order_amount_matches_formula`, `delivery_minutes_below_120`, `cost_price_not_above_mrp`, `unit_price_within_5x_of_mrp` (Critical) and `unit_price_within_20pct_of_mrp` (Warning) |
| Consistency | 9 | 1 | `customer/product/store_in_master`, `*_master_row_valid`, `success_amount_equals_order_amount`, `loyalty_tier_casing` (warning) |
| Timeliness | 5 | 1 | `order_ts_not_in_future`, `signup_date_not_in_future`; `latest_order_not_older_than_1_day` (warning) |

Two checks apply to the whole file and not row by row: the freshness check and `volume_not_down_more_than_90pct` (median of the last seven runs, from `dq_results`). Both are warnings. Query results: `docs/screenshots/10_dq_results.png`.

---

## 5. Part E: Data governance

**Ownership.** Orders: Operations (COO's team, with the order-management system owner). Payments: Finance (CFO's team). Customers: CRM / marketing, with the person responsible for data protection accountable for the personal data. Products: catalogue manager. Stores: store operations. Warehouse tables and the pipelines: the data owner is the CTO, Sameer, and one named engineer after my handover. A quality-rule change is proposed by the dataset owner and approved by whoever owns the business meaning: the 120-minute delivery limit defines the "10-minute promise" KPI, so Kavya (COO) approves it, and the change goes in as a reviewed pull request.

**Data dictionary** (fact and dimension tables, as built):

| Table.column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| etl_fact_orders.order_id | VARCHAR | Order key | orders.csv | unique | no |
| .order_ts / .order_date | TIMESTAMP / DATE | When the order was placed (IST wall clock) | orders.csv | not in the future | no |
| .customer_id, .product_id, .store_id | VARCHAR | Keys to the dimensions | orders.csv | must exist in a valid master row | indirect (customer_id) |
| .store_zone, .category | VARCHAR | Copied from store/product at load for simple queries | stores / products | zone: North, South, East, West, Unknown | no |
| .quantity, .unit_price, .discount_pct | INT / DOUBLE | As ordered | orders.csv | quantity > 0; discount 0-100 | no |
| .order_amount | DOUBLE | quantity x price x (1 - discount/100), after discount | orders.csv | equals the formula | no |
| .cost_amount | DOUBLE | quantity x cost_price | derived | | no |
| .payment_mode | VARCHAR | Standardised | orders.csv | UPI, Card, Cash on Delivery, Wallet | no |
| .order_status | VARCHAR | Standardised | orders.csv | DELIVERED, CANCELLED, RETURNED | no |
| .delivery_minutes | INT | Minutes to deliver | orders.csv | required if DELIVERED; < 120 | no |
| .net_revenue | DOUBLE | order_amount if DELIVERED, else 0 | derived | | no |
| .gross_margin | DOUBLE | net_revenue - cost_amount (delivered only) | derived | | no |
| .within_15_min | BOOLEAN | Delivered and delivery_minutes <= 15 | derived | | no |
| .source_file, .source_seq, .source_row | text / int | Where the row came from (lineage) | pipeline | | no |
| etl_dim_customer.customer_id | VARCHAR | Customer key | customers.csv | unique | indirect |
| .email_masked | VARCHAR | e.g. `pr***@example.com` | customers.email | | derived from PII |
| .phone_masked | VARCHAR | e.g. `98******21` | customers.phone | | derived from PII |
| .city, .area, .pincode | VARCHAR | Location (pincode nulled if not 6 digits) | customers.csv | | quasi-identifier |
| .signup_date, .loyalty_tier, .is_active | DATE / VARCHAR / BOOL | | customers.csv | tier: Bronze, Silver, Gold, Platinum | no |
| etl_dim_product.* | | product_id, name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date | products.csv | mrp > 0; cost_price <= mrp | no |
| etl_dim_store.* | | store_id, name, zone, area, pincode, open_date, manager_name, capacity | stores.csv | zone as above | manager_name is a person's name |

**Lineage.** Take `mart_daily_store_kpis`, store S05 on 2026-09-10: net revenue 1,015.80 from 4 delivered orders. In ELT one query reaches the source: `stg_orders` gives ORD000688 (113.05), ORD000689 (24.00), ORD000698 (684.00) and ORD000709 (194.75) with `source_file = orders.csv` and source rows 624, 1529, 1329, 639, and `raw_orders` holds the same rows as text with `_row_num`, `_run_id` and `_loaded_at`. Their sum is 1,015.80. In ETL the same trace works through `etl_fact_orders.source_file` and `source_row` back to the CSV line, but the as-received row is not in the warehouse, so I have to open the file, and if the file has since been replaced I cannot. **ELT is easier**, because the raw copy sits next to the result and keeps every load.

**Personal data.** In this dataset: `customers.full_name`, `email`, `phone`; `area` and `pincode` together with `signup_date` can identify a person, and `manager_name` in stores is an individual's name. `customer_id` links all of it. I masked email (first two characters + domain) and phone (first two and last two digits), dropped `full_name` from the reporting tables, and null out invalid pincodes. I mask instead of deleting because ops and finance still need to count, group and join per customer, and support can still match a masked value to a record in the CRM. The law that applies is India's Digital Personal Data Protection Act, 2023: NammaMart is the data fiduciary and the pipeline is a processor, so it expects a stated purpose, only the data needed for it, reasonable security safeguards, breach notification, deletion when the purpose ends, and honouring access/erasure requests.

**Access control.** `raw_*`: the pipeline's service account and one or two data engineers, nobody else. `raw_customers` has unmasked phones: only the data protection owner, on request, logged. Staging: data engineers and analysts (staging holds masked contacts only). Marts: finance, ops, dashboards. Quarantine: data engineers and the dataset owners; the customers quarantine file masks email, phone and name. DuckDB has no roles, so in production this needs a real warehouse with grants.

**Retention (my proposal, to confirm with NammaMart's legal).** Raw: 24 months rolling, then deleted or aggregated, except order and payment records which are financial records and are kept for the statutory period. Quarantine files: 90 days, after which the `etl_quarantine`/`elt_rejected_rows` log (no personal data) is the record. Masked customer data: while the account is active plus two years, then anonymised (customer_id replaced by an irreversible hash).

**Secrets.** There are no secrets in my DAGs. If I add the Slack alert, the webhook URL belongs in an Airflow Variable or Connection (`Variable.get("SLACK_WEBHOOK_URL")`), stored in Airflow's metadata database or the platform's secret store. It must never be in the DAG file because the DAG file is committed: anyone with repo access, or the public if it ever goes public, could post to that Slack channel, and a leaked key cannot be un-leaked, only rotated.

**Audit: "pick a rejected order, show it as received, the rule it broke, and prove it is not in the CFO's total".** ORD002103 (S03, 27 Sep, discount_pct 250). *ETL:* `etl_quarantine` holds it with `raw_row = {"order_id": "ORD002103", ..., "discount_pct": "250", "order_amount": "492.0", ...}` (source row 66) and `failure_reason = discount_pct_between_0_and_100; order_amount_matches_formula`; `SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD002103'` returns 0. *ELT:* `raw_orders` has it untouched (row 66, discount "250"); `stg_orders` has it with `dq_status = FAIL` and the same reasons; and for S03 on 27 Sep the mart shows 2,189.20, which equals the sum of the passing delivered rows, while including failed rows would give 3,524.80.

**Change history: DELIVERED on 3 September, RETURNED on 1 October.** After the increment, both pipelines show the order as RETURNED with net revenue 0, so September revenue falls. I measured it: September was 1,059,881.95 before the increment and 1,056,219.25 after, down 3,662.70, which is exactly the nine September orders that came back as RETURNED. Finance probably wants both numbers: the September figure as reported at month end, and the corrected one. To keep both, store status history (a dated row per status change) or snapshot the daily revenue table each run. ELT can already rebuild "as of 30 Sep" from raw, because raw keeps the DELIVERED row from the earlier loads next to the RETURNED row from the increment. ETL cannot, because it overwrites the fact row. I did not build the history table.

---

## 6. Part F: Observability tests

First, the run audit: the last task of each DAG (`run_audit`, `trigger_rule="all_done"`) writes one row per run to `pipeline_runs` (run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status). It also writes that run's checks to `dq_results` and, for ETL, the rejected rows to `etl_quarantine`. Across all my runs `pipeline_runs` holds 50+ runs, with the failed ones marked FAILED (`docs/screenshots/11_pipeline_runs.png`).

Method: `tests/run_experiments.py` edits the source file, triggers the DAG through the scheduler, collects task states from Airflow's metadata database and results from the warehouse, then puts the original file back. Raw output for every run is in `include/profiling/experiments/exp_<n>.md`. I ran each experiment on both DAGs. Experiments 4-6 and 8-9 exposed real bugs in my first version; those are marked **Found and fixed**.

| # | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|
| 1. Happy path | Green run; five extracts in parallel; rejected rows counted per file | **ETL:** 11 s. All five extracts started within 30 ms of each other; the three master validations ran together. Rows in / passed / quarantined: customers 405/397/8, products 152/142/10, stores 15/15/0, orders 2,572/2,322/250 (2,412 plus the 160-row increment already in the folder), payments 2,228/2,185/43; total 5,372 / 5,061 / 311. **ELT:** 21 s. Five extracts parallel, raw load, raw increment, staging in sequence (DuckDB takes one writer at a time), then all six marts started within 40 ms. Same 5,372 / 5,061 / 311 | Grid and Gantt views (`05`, `06` screenshots), task logs, `pipeline_runs` | Staging is serial because of the single writer; a real warehouse would let it run in parallel |
| 2. Re-run | Fact counts unchanged | **ETL:** `etl_fact_orders` 2,177 before and after; revenue 1,059,881.95 both times. **ELT:** `raw_orders` grew 4,824 to 7,236 (+2,412, one more copy of the file); `stg_orders` stayed 2,400; mart revenue unchanged. Raw growing is correct: raw is the record of every load; staging and marts are rebuilt or merged per run | Count queries before/after in `exp_2.md` | Skip loading a file whose checksum was already loaded, so identical copies do not pile up in raw |
| 3. Incremental load | New orders added, 10 corrections applied, no duplicates | **ETL:** fact 2,177 to 2,313: 136 inserted, 9 updated to RETURNED, 15 of the 160 rows quarantined, 0 duplicate order_ids. **ELT:** `stg_orders` 2,400 to 2,550: 150 new, 10 corrections merged, 0 duplicates; `raw_orders` +160. The same 2,313 non-failing rows as ETL (ELT keeps the failed ones in staging too). Correction status after: all RETURNED | Count queries in `exp_3.md` | Store inserted/updated counts in `pipeline_runs`; I had to derive them with SQL |
| 4. Missing file | Retries, then a clear failure; nothing half-loaded | `extract_products` was tried 3 times (initial + 2 retries) and failed about 62 s after the others finished; every downstream task showed `upstream_failed`; `run_audit` still ran and wrote a FAILED row. **ETL** had already extracted four files (5,220 rows) before failing; **ELT** loaded nothing (0 rows), because the raw load waits for all five extracts. **Found and fixed:** the first version showed the run as green because the audit task, being the only final task, succeeded. It now fails the run when the pipeline did not complete | Grid view (red extract_products, orange upstream_failed), `pipeline_runs`, task log | Nothing pushed me an alert; I only saw it by looking. I would add a Slack/e-mail failure callback (bonus; not built) |
| 5. New bad data | Five rows quarantined with the right reasons; dq_results updated | All five landed in quarantine in both pipelines with exactly the expected reason: ORD900001 `customer_id_not_null`, ORD900002 `quantity_positive`, ORD900003 `store_in_master`, ORD900004 `order_status_allowed`, ORD900005 `order_ts_not_in_future`. Counts moved by 1 each (e.g. `customer_id_not_null` 18 to 19) and rows quarantined went 311 to 316. **Found and fixed:** the first ELT run failed because my edit left mixed line endings in a CRLF file and DuckDB's CSV sniffer rejected the whole file. ETL (pandas) read it fine. ELT's raw load now uses `strict_mode=false` | `etl_quarantine`, `elt_rejected_rows`, `dq_results`, task log | ELT's raw layer is only as robust as its CSV reader; add a header/line-ending check at the extract step |
| 6. Schema change | Fail loudly at the first task | Both pipelines: `extract_orders` failed with `ValueError: orders.csv: schema changed. missing=['order_amount'] unexpected=['total']`. Nothing was loaded; the run was marked failed. I built the header check into extract, and I did not test what pandas would do without it | Task log (`09_failed_task_log.png`), failed graph (`07`, `08`) | A schema error is not transient; it took about 68-72 s because of three tries. Raise `AirflowFailException` so it fails at once |
| 7. Volume drop | A warning that volume fell by more than 90% | Both: `orders.volume_not_down_more_than_90pct` = WARN (this run read 210 order rows, 50 plus the 160-row increment, against a median of about 2,500). The run still succeeded. **Found and fixed:** the first ETL version also quarantined 2,130 valid payments, because it judged payments against only that day's orders file; ELT, which compares against the staged orders, did not. ETL now uses the same view of orders as ELT, and both quarantine 78 rows | `dq_results`, `pipeline_runs` (rows_extracted 3,010 vs 5,372) | A warning does not stop the load. I would make a >90% drop block the reporting load until someone confirms |
| 8. Drift (unit_price x100, store S05) | Something notices | 144 S05 rows changed. The amount-formula check caught all of them (18 to 162 failures), so S05's revenue stayed at 82,229.75 in both marts. That check only worked because order_amount was left unchanged. A drift that changes price and amount together (8b) is invisible to the original 62 checks; I did not run it before adding a check. I added `unit_price_within_5x_of_mrp` (Critical) and `unit_price_within_20pct_of_mrp` (Warning). Rerun of both variants: 138 rows flagged and quarantined, S05 revenue unchanged | `dq_results`, store KPI mart | These checks look at one row against the catalogue. Nothing yet compares a store's daily revenue with its own history; that is the monitoring I would add next |
| 9. Freshness (dates 30 days old) | A timeliness warning | **Found and fixed:** the first version did not warn at all, because the 2027-dated rows counted as "the latest order". It now ignores future timestamps, and warns in both pipelines. It also warns on a normal run now, since the data is static (latest valid order is 1 Oct) and the test runs on 11 Oct, so it cannot tell a real staleness from the sample data. Side effect: the shifted dates made 3 previously future-dated rows valid, they loaded, and nothing removes them later; I deleted them by hand and reran | `dq_results`; the stray rows in `etl_fact_orders` | Measure freshness against when a file was expected, and trend it. Pipelines that only upsert never retract a row that later turns invalid |
| 10. Recovery | After the fix, only the failed task and its downstream re-run | I restored `products.csv` and used Clear (with downstream) on `extract_products` of the failed experiment-4 run. **ETL:** `extract_products` ran again (try 4), then `validate_products`, `validate_orders`, `transform`, `load_warehouse`, `reconcile_payments`, `report`, `run_audit` (try 2). The tasks that had already succeeded kept try 1 and their original timestamps. The run went failed to success and its `pipeline_runs` row went FAILED to SUCCESS (5,372 / 5,061 / 311). **ELT:** the same pattern, from `extract_products` through `run_audit` | Task states before/after in `exp_10.md` | The audit row is replaced, not appended, so the earlier failure is no longer visible in `pipeline_runs`; keep the history in the Airflow log |

**Five pillars of data observability**

| Pillar | What I have | Strength |
|---|---|---|
| Schema | Header check at extract against the agreed columns; fails before any write (exp. 6) | **Best covered** |
| Volume | `rows_extracted/loaded/quarantined` per run; 90% drop warning on orders only | Medium: orders only, and warning only |
| Freshness | `latest_order_not_older_than_1_day` | Weak: static sample data makes it warn every run, no trend |
| Distribution | Price within 20%/5x of mrp; amount formula; delivery under 120 min | **Weakest:** row-level rules only, no comparison of today's store or category totals with their own history |
| Lineage | ELT: `source_file`, `source_row`, `_run_id` on every layer. ETL: `source_file`, `source_row` on the fact table | Good in ELT, partial in ETL |

---

## 7. Business questions

Each is one query on the final tables. SQL for both pipelines is in `include/sql/business_questions.sql`; results are in `include/profiling/business_questions_results.md`. Key numbers (identical from ETL and ELT):

| Question | Answer |
|---|---|
| 1. Daily net revenue by zone and store | Total 1,128,410.45 over 1-30 Sep plus 1 Oct. Latest day (1 Oct): East 21,572.45, West 18,862.80, North 14,546.80, South 14,552.75, Unknown (store S07, zone missing) 2,656.40 |
| 2. Revenue and margin by category | Staples 264,456.90 revenue / 66,435.28 margin (25.12%); Household 210,229.95 / 54,055.27 (25.71%); Personal Care 208,774.70 / 46,709.57 (22.37%); Dairy & Bakery 177,404.85 / 47,200.82 (26.61%); Snacks & Beverages 174,575.60 / 41,339.24 (23.68%); Fruits & Vegetables 92,968.45 / 22,018.22 (23.68%) |
| 3. Delivery time per store and % within 15 min | Per store in the results file; averages run from about 20 to 22 minutes and 21% to 35% of deliveries finish within 15 minutes, so the 10-minute promise is not met at any store |
| 4. Revenue by payment mode and unreconciled amount | UPI 598,618.20 (1,131 orders), Card 231,787.85 (428), Cash on Delivery 169,026.60 (298), Wallet 128,977.80 (234). Mismatches: 447 rows, 276,873.70 total; of that 72,191.20 is the 136 increment orders whose payments have not arrived yet and 152,863.63 is payments for orders that were themselves rejected |
| 5. Top 10 customers | Ranked by delivered spend with tier and masked contact only; #1 C00136 (Bronze) 8,738.10 |
| 6. Rows rejected in the latest run | 311: orders 250, payments 43, products 10, customers 8, by reason in the results file |

---

## 8. Screenshots (`docs/screenshots/`)

Browser screenshots are real captures of the Airflow UI. The query result images and the quarantine file image are rendered from live query output into a plain grid and then captured; they show the actual rows but are not screenshots of VS Code.

| File | Shows |
|---|---|
| `01_dag_list.png` | Both DAGs |
| `02_etl_schedule_ist.png`, `03_elt_schedule_ist.png` | Schedule `0 9 * * 1-6` and next run 2026-10-12 09:00 IST |
| `04_etl_schedule_utc.png` | The same next run in UTC (03:30) |
| `05_etl_graph_green.png`, `06_elt_graph_green.png` | Graph view, all tasks green |
| `07_*`, `08_*`, `09_failed_task_log.png` | A failed run (schema change) and its task log |
| `10_dq_results.png`, `11_pipeline_runs.png` | `dq_results` and `pipeline_runs` |
| `12_*`, `13_*` | `etl_payment_reconciliation`: summary and rows |
| `14_quarantine_file.png` | A quarantine file with `failure_reason` |
| `15_dq_trend.png` | Bonus: data quality per run for both DAGs, built from `dq_results` |
| `16_scd2_customer_history.png` | Bonus: one customer's three SCD Type 2 versions (Bronze, Gold, Bronze) |
| `17_store_capacity_by_store.png` | Bonus: store-days over capacity, by store |
| `18_dq_results_capacity_rows.png` | Bonus: the capacity check rows in `dq_results`, both pipelines |
| `19_slack_failure_alert.png` | Bonus: the failure alert text received by a local test webhook |

---

## 9. Part G: Recommendation to Sameer (CTO)

**Sameer, I recommend ELT, with the finance guarantee kept as a hard gate between staging and the marts.**

I built both on the same five files and ran them side by side. They give the same answer: net revenue 1,128,410.45, 311 rows rejected, 447 reconciliation mismatches, and the same result for all 65 quality checks in the latest run (37 checks flag a failure in both). So the question is not which one is more correct. It is which one is easier to trust, audit and change.

**Auditability.** ELT keeps every row exactly as received (`raw_*`), with the file, row and run it came from. When finance asked me to trace ORD002103, ELT showed the raw row, the rule it broke and the proof it was not in the total from one database. ETL could show the rejected row and the rule but not the original file in the warehouse. **Quality guarantees.** ETL gives the stronger promise: a bad row never enters a reporting table. ELT matches it only by discipline, because bad rows live in staging and every mart must filter them. I built that filter and tested it, but it is a rule, not a wall. So the marts are the only thing finance gets access to. **Ease of change.** When experiments exposed gaps I added two price checks and fixed a freshness check. In ELT a rule is a SQL edit, and the raw history it would need is already in the warehouse, so a changed rule could be replayed over past loads without re-reading the source files (I have not built that replay; staging currently processes the current run's rows). In ETL a changed rule only applies to files that are extracted again. **Failures.** ETL also behaved worse in one case I did not expect: validating payments against only the day's orders file wrongly rejected about 2,100 good payments when the orders file was short. ELT did not, because its staging table remembers. I fixed ETL, but it shows validation tied to a single run's context is fragile. ELT's weakness is the opposite one: the raw load was the first thing to break on a badly formatted file. **Growth to 1000x.** Pandas holds the whole file in memory; SQL in the warehouse does not. At around 2.4 million orders a month both would still run on DuckDB, but ELT moves to a bigger warehouse by changing the engine, not the logic. **Cost.** Both are open source. ELT stores raw copies, so storage grows with every load (raw orders grew by 2,412 rows per run in my tests); a checksum skip and a retention policy keep that small.

**Top three risks of ELT**

1. **Raw personal data.** `raw_customers` holds unmasked names, emails and phones, which is exactly what the DPDP Act wants minimised. Mask personal fields as the file is loaded, restrict raw to the service account, and set retention before go-live.
2. **One machine, one writer.** DuckDB allows one writer and the schedule depends on my laptop being on at 09:00. If the host is off, nothing runs and nobody is told. Move to a server and a multi-user database, and add a failure alert.
3. **Rules that nobody owns.** Rules in SQL silently drop revenue. A product with a bad catalogue record caused 140 orders (about 6%) to be rejected, and a warning on a volume drop did not stop anything. Each rule needs an owner and an approval path, and the volume check should block the reporting load.

---

## 10. What is not done

- No order-status history table: the 10 corrected orders overwrite their old rows in both pipelines (see the change-history answer in Part E).
- The pipelines only upsert: a row that later turns invalid is not removed from the warehouse (experiment 9).
- SCD Type 2 is built for the ETL pipeline only. The ELT raw layer already keeps every version of every file, so it can rebuild history, but no SQL SCD table exists there.
- The Slack alert is the only alert channel. Email is not wired up.
- The screenshots in 01 to 14 were taken before the bonus work. The graph views show the tasks as they were then, without `dq_trend_chart` or the new capacity mart.

---

## 11. Bonus items

All four are built, run on the same warehouse, and their evidence is in section 8 (images 15 to 19).

**1. Failure alert (Slack, through an Airflow Variable).** `include/nammamart/alerts.py` is set as `on_failure_callback` in `default_args` of both DAGs. It reads `SLACK_WEBHOOK_URL` with `Variable.get`, so the URL is never in code or git. With no variable set, the alert is logged and skipped, so a missing secret cannot fail a run.
- Test: I set the variable to a local receiver and moved `products.csv` aside for one ETL run. The alert arrived with the DAG, task, try number, run ID and error (image 19). I then removed the variable and the local receiver, so no webhook address is left anywhere. Set your own value in Admin > Variables to receive alerts.
- The same callback also fired for `dq_trend_chart` when it had a bug, so it covers every task, not only the extracts.

**2. SCD Type 2 customer dimension (ETL only).** Table `etl_dim_customer_history` has one row per customer version: `customer_key`, `customer_id`, `loyalty_tier`, `valid_from`, `valid_to`, `is_current`. Each load compares the incoming tier with the open version. A change closes the old version and opens a new one at the run time. A re-run with no change adds nothing.
- Test (image 16): a baseline run, then C00159's tier changed from Bronze to Gold for one run, then the file restored. C00159 has three versions. Its history is Bronze, then Gold, then Bronze, with a valid range for each. Across all 397 customers there is exactly one current row each.
- Design choice: I first used dates, and that lost a version when two changes happened on the same day. Timestamps fixed it. That also means the history starts at the first load after this change. The table was rebuilt for that reason.

**3. Store capacity check (both pipelines, Warning).** `capacity.daily_orders_within_capacity` counts orders per store per day against `capacity_orders_per_day`. It runs in the ETL audit and in `46_mart_store_capacity.sql` for ELT. Warning only, because the orders still load.
- Result (images 17 and 18): every store is within capacity except **S12, whose capacity in `stores.csv` is 0**. Every day with orders is therefore over capacity (31 of 31 days, in both pipelines). That is a data issue to raise with NammaMart, not a pipeline bug. A capacity of 0 should be a Critical catalogue error.

**4. Data quality trend chart.** `include/nammamart/dq_trend.py` draws one bar per run from `dq_results`, for both DAGs, coloured by PASS, WARN and FAIL. The last task of the ETL DAG (`dq_trend_chart`, runs on `all_done`) rebuilds `include/reports/nammamart_dq_trend_pradeep.png` after every run. The chart is in image 15. The red FAIL segments are real: the sample data quarantines 311 rows in every run, so the chart shows a steady number of failing checks, and the early red spikes are the failure experiments.
- Added `matplotlib` to `requirements.txt`, which needed an Airflow restart.
- `include/reports/` is git-ignored because the chart is rebuilt on every run. The copy in `docs/screenshots/` is the one submitted.

