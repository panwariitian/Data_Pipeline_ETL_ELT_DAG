"""Captures the evidence screenshots for SUBMISSION.md from the local Airflow UI and the warehouse.

UI pages are real browser screenshots (headless Chrome). Query results and the quarantine file are
rendered from live query output into a simple HTML grid and then screenshotted.
"""
import os
import subprocess
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
BASE = os.environ.get("AIRFLOW_UI", "http://localhost:8080")
ETL, ELT = "nammamart_etl_pradeep", "nammamart_elt_pradeep"


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def psql(sql):
    return sh(f"docker exec {sh('docker ps -qf name=postgres')} psql -U postgres -t -A -c \"{sql}\"")


def run_id(dag, state, like=""):
    return psql(f"select run_id from dag_run where dag_id='{dag}' and state='{state}' {like} order by end_date desc limit 1")


def table_html(sql, title):
    code = ("import duckdb,sys;c=duckdb.connect('/usr/local/airflow/include/warehouse_pradeep.duckdb',read_only=True);"
            "print(c.execute(sys.argv[1]).df().to_html(index=False,border=0,na_rep='NULL'))")
    html = subprocess.run(["docker", "exec", sh("docker ps -qf name=scheduler"), "python", "-c", code, sql],
                          capture_output=True, text=True).stdout
    return page(title, sql, html)


def page(title, sql, table):
    return f"""<html><body style="font-family:-apple-system,Helvetica,Arial;margin:24px;color:#222">
<div style="font-size:15px;font-weight:600;margin-bottom:6px">{title}</div>
<pre style="background:#f4f4f6;padding:10px;border-radius:6px;font-size:12px;white-space:pre-wrap">{sql}</pre>
<style>table{{border-collapse:collapse;font-size:12px}}th{{background:#2b3a55;color:#fff;text-align:left;padding:5px 10px}}
td{{padding:4px 10px;border-bottom:1px solid #ddd}}tr:nth-child(even){{background:#f7f7fa}}</style>{table}</body></html>"""


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    ok_etl, ok_elt = run_id(ETL, "success", "and run_id like 'exp1_%'"), run_id(ELT, "success", "and run_id like 'exp1_%'")
    bad_etl, bad_elt = run_id(ETL, "failed", "and run_id like 'exp6_%'"), run_id(ELT, "failed", "and run_id like 'exp6_%'")
    with sync_playwright() as p:
        b = p.chromium.launch(channel="chrome", headless=True)

        def ui(name, path, tz="Asia/Kolkata", graph=False, height=1000, wait=3000):
            ctx = b.new_context(viewport={"width": 1600, "height": height}, timezone_id=tz)
            pg = ctx.new_page()
            pg.goto(BASE + path, wait_until="networkidle")
            pg.wait_for_timeout(wait)
            if graph:
                pg.mouse.click(163, 79)
                pg.wait_for_timeout(3500)
            pg.screenshot(path=str(OUT / name))
            ctx.close()
            print("saved", name)

        ui("01_dag_list.png", "/dags")
        ui("02_etl_schedule_ist.png", f"/dags/{ETL}")
        ui("03_elt_schedule_ist.png", f"/dags/{ELT}")
        ui("04_etl_schedule_utc.png", f"/dags/{ETL}", tz="UTC")
        ui("05_etl_graph_green.png", f"/dags/{ETL}/runs/{ok_etl}", graph=True)
        ui("06_elt_graph_green.png", f"/dags/{ELT}/runs/{ok_elt}", graph=True)
        ui("07_etl_failed_run_missing_schema.png", f"/dags/{ETL}/runs/{bad_etl}", graph=True)
        ui("08_elt_failed_run_missing_schema.png", f"/dags/{ELT}/runs/{bad_elt}", graph=True)
        ui("09_failed_task_log.png", f"/dags/{ETL}/runs/{bad_etl}/tasks/extract_orders?try_number=3", wait=4000)

        def grid(name, sql, title, width=1500, height=900):
            ctx = b.new_context(viewport={"width": width, "height": height})
            pg = ctx.new_page()
            pg.set_content(table_html(sql, title))
            pg.screenshot(path=str(OUT / name), full_page=True)
            ctx.close()
            print("saved", name)

        grid("10_dq_results.png",
             f"SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results "
             f"WHERE run_id IN ('{ok_etl}', '{ok_elt}') AND status <> 'PASS' ORDER BY dag_id, dimension, check_name",
             "dq_results: checks that did not pass in the latest healthy run of each pipeline", height=1200)
        grid("11_pipeline_runs.png",
             "SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status "
             "FROM pipeline_runs ORDER BY end_time DESC LIMIT 14", "pipeline_runs: the run audit table")
        grid("12_payment_reconciliation.png",
             "SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS amount "
             "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC",
             "etl_payment_reconciliation: every order/payment mismatch, summarised by reason", height=500)
        grid("13_payment_reconciliation_rows.png",
             "SELECT order_id, payment_id, order_status, payment_status, order_amount, amount_paid, mismatch_reason, "
             "unreconciled_amount FROM etl_payment_reconciliation WHERE mismatch_reason NOT LIKE 'PAYMENT_ORDER%' "
             "ORDER BY mismatch_reason, order_id LIMIT 25", "etl_payment_reconciliation: first 25 mismatch rows", height=900)
        qfile = sorted((ROOT / "include" / "quarantine" / "pradeep").glob(f"orders_failed_{ok_etl}*.csv"))
        if qfile:
            import csv
            rows = list(csv.reader(open(qfile[0])))[:16]
            head = "".join(f"<th>{h}</th>" for h in rows[0])
            body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows[1:])
            ctx = b.new_context(viewport={"width": 1700, "height": 700})
            pg = ctx.new_page()
            pg.set_content(page(f"Quarantine file: include/quarantine/pradeep/{qfile[0].name} (first 15 rows)", "", f"<table><tr>{head}</tr>{body}</table>"))
            pg.screenshot(path=str(OUT / "14_quarantine_file.png"), full_page=True)
            ctx.close()
            print("saved 14_quarantine_file.png")
        b.close()


if __name__ == "__main__":
    sys.exit(main())
