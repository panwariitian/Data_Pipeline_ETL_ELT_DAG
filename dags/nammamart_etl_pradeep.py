"""
NammaMart ETL pipeline: bad rows are stopped before the warehouse.

  extract x5 (parallel) -> validate masters -> validate orders / payments -> transform
  -> load star schema -> reconcile with payments -> report
  every task -> audit (runs even when something failed)

Runs at 09:00 IST, Monday to Saturday. Business logic lives in include/nammamart/.
"""
import sys

sys.path.insert(0, "/usr/local/airflow/include")

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable

from nammamart import alerts
from nammamart import dq_trend
from nammamart import etl_core as etl

IST = "Asia/Kolkata"

default_args = {
    "owner": "pradeep",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": alerts.notify_failure,
}


def _slim(summary: dict) -> dict:
    """dq rows stay on disk; XCom only carries what downstream tasks need."""
    return {k: v for k, v in summary.items() if k != "dq"}


@dag(
    dag_id="nammamart_etl_pradeep",
    description="NammaMart ETL: validate and quarantine before load",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone=IST),
    start_date=pendulum.datetime(2026, 10, 1, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", "pradeep"],
)
def nammamart_etl_pradeep():
    # ---- extract (parallel) -------------------------------------------------
    @task
    def extract_orders(run_id=None):
        return etl.extract("orders", run_id)

    @task
    def extract_payments(run_id=None):
        return etl.extract("payments", run_id)

    @task
    def extract_customers(run_id=None):
        return etl.extract("customers", run_id)

    @task
    def extract_products(run_id=None):
        return etl.extract("products", run_id)

    @task
    def extract_stores(run_id=None):
        return etl.extract("stores", run_id)

    # ---- validate -----------------------------------------------------------
    @task
    def validate_customers(info, run_id=None):
        return _slim(etl.validate("customers", run_id, info))

    @task
    def validate_products(info, run_id=None):
        return _slim(etl.validate("products", run_id, info))

    @task
    def validate_stores(info, run_id=None):
        return _slim(etl.validate("stores", run_id, info))

    @task
    def validate_orders(info, v_customers, v_products, v_stores, x_customers, x_products, x_stores, run_id=None):
        deps = {"customers": v_customers, "products": v_products, "stores": v_stores,
                "customers_extract": x_customers, "products_extract": x_products, "stores_extract": x_stores}
        return _slim(etl.validate("orders", run_id, info, deps))

    @task
    def validate_payments(info, orders_info, orders_validated, run_id=None):
        return _slim(etl.validate("payments", run_id, info,
                                  {"orders_extract": orders_info, "orders_validated": orders_validated}))

    # ---- transform, load, reconcile, report --------------------------------
    @task
    def transform(v_orders, v_customers, v_products, v_stores, run_id=None):
        return etl.transform(run_id, {"orders": v_orders, "customers": v_customers,
                                      "products": v_products, "stores": v_stores})

    @task
    def load_warehouse(transformed, run_id=None):
        return etl.load(run_id, transformed)

    @task
    def reconcile_payments(loaded, x_orders, x_payments, run_id=None):
        return etl.reconcile(run_id, {"orders": x_orders, "payments": x_payments})

    @task
    def report(reconciled, run_id=None):
        return etl.report(run_id)

    # ---- audit: always runs -------------------------------------------------
    @task(trigger_rule="all_done", retries=0)
    def run_audit(run_id=None):
        ctx = get_current_context()
        start = ctx["dag_run"].start_date or pendulum.now("UTC")
        result = etl.audit(run_id, start, pendulum.now("UTC"), expected_ok=True)
        if result["status"] != "SUCCESS":
            # the audit row is already written; failing here keeps a broken run from showing green in the UI
            raise AirflowFailException(f"run {run_id} did not complete: see pipeline_runs and the failed tasks")
        return result

    x_orders, x_payments = extract_orders(), extract_payments()
    x_customers, x_products, x_stores = extract_customers(), extract_products(), extract_stores()

    v_customers = validate_customers(x_customers)
    v_products = validate_products(x_products)
    v_stores = validate_stores(x_stores)
    v_orders = validate_orders(x_orders, v_customers, v_products, v_stores, x_customers, x_products, x_stores)
    v_payments = validate_payments(x_payments, x_orders, v_orders)

    t = transform(v_orders, v_customers, v_products, v_stores)
    loaded = load_warehouse(t)
    reconciled = reconcile_payments(loaded, x_orders, x_payments)
    reported = report(reconciled)

    @task(trigger_rule="all_done", retries=0)
    def dq_trend_chart(run_id=None):
        return dq_trend.build()

    audit = run_audit()
    [reported, v_payments] >> audit >> dq_trend_chart()


nammamart_etl_pradeep()
