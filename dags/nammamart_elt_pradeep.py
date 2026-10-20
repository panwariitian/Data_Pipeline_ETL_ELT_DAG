"""
NammaMart ELT pipeline: keep every raw row, clean and decide in the warehouse with SQL.

  extract x5 (parallel) -> load raw -> load raw increment (if a next-day file exists)
  -> staging: customers, products, stores -> orders (MERGE) -> payments
  -> marts (SQL) -> report ; every task -> audit

Runs at 09:00 IST, Monday to Saturday. All transformations are .sql files in include/sql/elt/.
"""
import sys

sys.path.insert(0, "/usr/local/airflow/include")

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable

from nammamart import alerts
from nammamart import elt_core as elt

IST = "Asia/Kolkata"

default_args = {
    "owner": "pradeep",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": alerts.notify_failure,
}

MARTS = [
    ("mart_daily_store_kpis", "40_mart_daily_store_kpis.sql"),
    ("mart_category_margin", "41_mart_category_margin.sql"),
    ("mart_delivery_sla", "42_mart_delivery_sla.sql"),
    ("mart_payment_reconciliation", "43_mart_payment_reconciliation.sql"),
    ("mart_top_customers", "44_mart_top_customers.sql"),
    ("mart_store_capacity", "46_mart_store_capacity.sql"),
]


@dag(
    dag_id="nammamart_elt_pradeep",
    description="NammaMart ELT: raw -> staging -> marts, all in SQL",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone=IST),
    start_date=pendulum.datetime(2026, 10, 1, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", "pradeep"],
)
def nammamart_elt_pradeep():
    @task
    def extract_orders(run_id=None):
        return elt.extract("orders", run_id)

    @task
    def extract_payments(run_id=None):
        return elt.extract("payments", run_id)

    @task
    def extract_customers(run_id=None):
        return elt.extract("customers", run_id)

    @task
    def extract_products(run_id=None):
        return elt.extract("products", run_id)

    @task
    def extract_stores(run_id=None):
        return elt.extract("stores", run_id)

    @task
    def load_raw(infos, run_id=None):
        elt.init_warehouse(run_id)
        return elt.load_raw(run_id, infos)

    @task
    def load_raw_increment(infos, base_done, run_id=None):
        return elt.load_raw(run_id, infos, increments_only=True)

    def sql_task(task_id, filename):
        @task(task_id=task_id)
        def _run(upstream, run_id=None):
            elt.run_sql_file(filename, run_id)
            return filename
        return _run

    @task
    def report(upstream, run_id=None):
        return elt.report(run_id)

    @task(trigger_rule="all_done", retries=0)
    def run_audit(run_id=None):
        ctx = get_current_context()
        start = ctx["dag_run"].start_date or pendulum.now("UTC")
        result = elt.audit(run_id, start, pendulum.now("UTC"))
        if result["status"] != "SUCCESS":
            raise AirflowFailException(f"run {run_id} did not complete: see pipeline_runs and the failed tasks")
        return result

    infos = [extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores()]
    raw = load_raw(infos)
    raw_inc = load_raw_increment(infos, raw)

    stg_customers = sql_task("stg_customers", "10_stg_customers.sql")(raw_inc)
    stg_products = sql_task("stg_products", "11_stg_products.sql")(stg_customers)
    stg_stores = sql_task("stg_stores", "12_stg_stores.sql")(stg_products)
    stg_orders = sql_task("stg_orders_merge", "20_stg_orders.sql")(stg_stores)
    stg_payments = sql_task("stg_payments", "30_stg_payments.sql")(stg_orders)

    marts = [sql_task(name, sql)(stg_payments) for name, sql in MARTS]
    marts.append(sql_task("mart_rejections", "45_mart_rejections.sql")(stg_payments))
    reported = report(marts)
    reported >> run_audit()


nammamart_elt_pradeep()
