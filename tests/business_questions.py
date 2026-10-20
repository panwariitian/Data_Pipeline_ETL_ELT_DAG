"""Answers the six business questions from the ETL tables and from the ELT marts.

Writes the SQL to include/sql/business_questions.sql and the results to
include/profiling/business_questions_results.md. Runs queries inside the scheduler container,
so the Astro stack must be up.
"""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

LAST_ETL_RUN = "(SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_pradeep' ORDER BY end_time DESC LIMIT 1)"
LAST_ELT_RUN = "(SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_pradeep' ORDER BY end_time DESC LIMIT 1)"

QUESTIONS = [
    ("Q1a. Daily net revenue by zone (delivered orders, after discount), latest 5 days",
     "SELECT order_date, zone, delivered_orders, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date DESC, zone LIMIT 20",
     "SELECT order_date, zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue "
     "FROM mart_daily_store_kpis GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 20"),
    ("Q1b. Daily net revenue by store, latest day",
     "SELECT order_date, store_id, store_name, zone, net_revenue FROM etl_daily_store_kpis "
     "WHERE order_date = (SELECT max(order_date) FROM etl_daily_store_kpis) ORDER BY store_id",
     "SELECT order_date, store_id, store_name, zone, net_revenue FROM mart_daily_store_kpis "
     "WHERE order_date = (SELECT max(order_date) FROM mart_daily_store_kpis) ORDER BY store_id"),
    ("Q2. Revenue and gross margin by category",
     "SELECT category, net_revenue, gross_margin, margin_pct FROM etl_category_margin ORDER BY net_revenue DESC",
     "SELECT category, net_revenue, gross_margin, margin_pct FROM mart_category_margin ORDER BY net_revenue DESC"),
    ("Q3. Average delivery time per store and % delivered within 15 minutes",
     "SELECT store_id, round(avg(delivery_minutes), 2) AS avg_delivery_minutes, "
     "round(100.0 * avg(CASE WHEN within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min "
     "FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 1",
     "SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY 1"),
    ("Q4a. Revenue by payment mode",
     "SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue "
     "FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC",
     "SELECT payment_mode, delivered_orders, net_revenue FROM mart_payment_mode_revenue ORDER BY 3 DESC"),
    ("Q4b. Amount that does not reconcile with the payment gateway, by reason",
     "SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount "
     "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC",
     "SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount "
     "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC"),
    ("Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)",
     "SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank, f.customer_id, c.loyalty_tier, "
     "c.email_masked, c.phone_masked, round(sum(f.net_revenue), 2) AS total_spend "
     "FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id) WHERE f.order_status = 'DELIVERED' "
     "GROUP BY f.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked ORDER BY 1 LIMIT 10",
     "SELECT spend_rank, customer_id, loyalty_tier, email_masked, phone_masked, total_spend "
     "FROM mart_top_customers WHERE spend_rank <= 10 ORDER BY 1"),
    ("Q6. Rows rejected in the latest run, by file and reason",
     "SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected FROM ("
     "SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM etl_quarantine "
     f"WHERE run_id = {LAST_ETL_RUN}) GROUP BY 1, 2 ORDER BY 1, 3 DESC",
     "SELECT source_file, failure_reason, rows_rejected FROM mart_rejections "
     f"WHERE run_id = {LAST_ELT_RUN} ORDER BY 1, 3 DESC"),
]


def duck(sql):
    code = ("import duckdb,sys,pandas as pd;pd.set_option('display.width',250);pd.set_option('display.max_columns',30);"
            "c=duckdb.connect('/usr/local/airflow/include/warehouse_pradeep.duckdb',read_only=True);"
            "print(c.execute(sys.argv[1]).df().to_string(index=False))")
    cid = subprocess.run("docker ps -qf name=scheduler", shell=True, capture_output=True, text=True).stdout.split()[0]
    r = subprocess.run(["docker", "exec", cid, "python", "-c", code, sql], capture_output=True, text=True)
    return (r.stdout or r.stderr).rstrip()


if __name__ == "__main__":
    sql_out, md = [], ["# Business questions: results from both pipelines\n"]
    for title, etl_sql, elt_sql in QUESTIONS:
        sql_out += [f"-- {title}", f"-- ETL\n{etl_sql};", f"-- ELT\n{elt_sql};\n"]
        md += [f"## {title}\n", "**ETL**\n```sql\n" + etl_sql + "\n```\n```\n" + duck(etl_sql) + "\n```\n",
               "**ELT**\n```sql\n" + elt_sql + "\n```\n```\n" + duck(elt_sql) + "\n```\n"]
    (ROOT / "include" / "sql" / "business_questions.sql").write_text("\n".join(sql_out))
    (ROOT / "include" / "profiling" / "business_questions_results.md").write_text("\n".join(md))
    print("written")
