"""Runs the Part F observability experiments against the local Astro stack and logs what happened.

Usage: python tests/run_experiments.py <experiment number|all> [--dags etl,elt]
It edits files in include/data/nammamart, triggers both DAGs through the scheduler, collects task states
from the Airflow metadata DB and results from the warehouse, then restores the original files.
Output goes to include/profiling/experiments/exp_<n>.md
"""
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "include" / "data" / "nammamart"
SRC = ROOT / "assignment" / "nammamart_dataset"
OUT = ROOT / "include" / "profiling" / "experiments"
DAGS = {"etl": "nammamart_etl_pradeep", "elt": "nammamart_elt_pradeep"}


def sh(cmd, check=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(f"{cmd}\n{r.stderr}")
    return r.stdout.strip()


def cid(name):
    return sh(f"docker ps -qf name={name}").splitlines()[0]


def psql(sql):
    out = sh(f"docker exec {cid('postgres')} psql -U postgres -t -A -F ' | ' -c \"{sql}\"")
    return out


def duck(sql, fmt="df"):
    code = ("import duckdb,sys;c=duckdb.connect('/usr/local/airflow/include/warehouse_pradeep.duckdb',read_only=True);"
            "import pandas as pd;pd.set_option('display.width',250);pd.set_option('display.max_columns',30);"
            "pd.set_option('display.max_colwidth',90);print(c.execute(sys.argv[1]).df().to_string(index=False))")
    return sh(f"docker exec {cid('scheduler')} python -c \"{code}\" \"{sql}\"")


def trigger(key, tag):
    run_id = f"{tag}_{key}_{datetime.now().strftime('%H%M%S')}"
    sh(f"docker exec {cid('scheduler')} airflow dags trigger {DAGS[key]} --run-id {run_id}")
    return run_id


def wait(key, run_id, timeout=900):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = psql(f"select state from dag_run where dag_id='{DAGS[key]}' and run_id='{run_id}'")
        if st in ("success", "failed"):
            return st, round(time.time() - t0)
        time.sleep(5)
    return "timeout", timeout


def clear_task(key, run_id, task_id):
    """Same as pressing Clear (with downstream) on a task in the UI, through the REST API."""
    body = json.dumps({"dry_run": False, "dag_run_id": run_id, "task_ids": [task_id],
                       "include_downstream": True, "only_failed": False, "reset_dag_runs": True})
    code = ("import urllib.request,json,sys;"
            "t=json.load(urllib.request.urlopen('http://localhost:8080/auth/token'))['access_token'];"
            "r=urllib.request.Request('http://localhost:8080/api/v2/dags/'+sys.argv[1]+'/clearTaskInstances',method='POST',data=sys.argv[2].encode(),headers={'Authorization':'Bearer '+t,'Content-Type':'application/json'});"
            "print(urllib.request.urlopen(r).status)")
    return sh(f"docker exec {cid('api-server')} python -c \"{code}\" {DAGS[key]} '{body}'", check=False)


def task_table(key, run_id):
    return psql(f"select task_id, state, try_number, to_char(start_date,'HH24:MI:SS.MS') started, "
                f"round(extract(epoch from (end_date-start_date))::numeric,1) secs from task_instance "
                f"where dag_id='{DAGS[key]}' and run_id='{run_id}' order by start_date")


def run_summary(key, run_id):
    d = DAGS[key]
    parts = ["pipeline_runs:", duck(f"select run_id, status, rows_extracted, rows_loaded, rows_quarantined "
                                    f"from pipeline_runs where dag_id='{d}' and run_id='{run_id}'"),
             "dq_results not PASS:", duck(f"select check_name, severity, rows_checked, rows_failed, status from dq_results "
                                          f"where dag_id='{d}' and run_id='{run_id}' and status<>'PASS' order by check_name")]
    return "\n".join(parts)


class Edit:
    """Backs up a source file, lets the experiment change it, restores it afterwards."""

    def __init__(self, name):
        self.path = DATA / name
        self.backup = DATA / (name + ".orig")

    def __enter__(self):
        if self.path.exists():
            shutil.copy(self.path, self.backup)
        return self.path

    def __exit__(self, *exc):
        if self.backup.exists():
            shutil.move(self.backup, self.path)


def log(n, text):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"exp_{n}.md", "a") as fh:
        fh.write(text + "\n")
    print(text)


def run_both(n, label, keys):
    """Trigger, wait, and log each DAG one after the other (DuckDB allows one writer)."""
    results = {}
    for key in keys:
        rid = trigger(key, f"exp{n}")
        state, secs = wait(key, rid)
        log(n, f"\n### {DAGS[key]}  run={rid}  state={state}  wall={secs}s  ({label})")
        log(n, "```\n" + task_table(key, rid) + "\n```")
        log(n, "```\n" + run_summary(key, rid) + "\n```")
        results[key] = rid
    return results


# ------------------------------------------------------------------ experiments
def counts():
    return duck("select (select count(*) from etl_fact_orders) etl_fact, (select count(*) from raw_orders) raw_orders, "
                "(select count(*) from stg_orders) stg_orders, "
                "(select round(sum(net_revenue),2) from etl_fact_orders) etl_revenue, "
                "(select round(sum(net_revenue),2) from mart_daily_store_kpis) elt_revenue")


def e1(keys):
    log(1, "## Experiment 1 - Happy path")
    run_both(1, "no change", keys)


def e2(keys):
    log(2, "## Experiment 2 - Re-run (idempotency)")
    log(2, "before:\n```\n" + counts() + "\n```")
    run_both(2, "same inputs, second run", keys)
    log(2, "after:\n```\n" + counts() + "\n```")


def e3(keys):
    log(3, "## Experiment 3 - Incremental load")
    log(3, "before:\n```\n" + counts() + "\n```")
    inc = "orders_2026-10-01_increment.csv"
    shutil.copy(SRC / inc, DATA / inc)
    try:
        run_both(3, "increment file added", keys)
        q = ("select (select count(*) from etl_fact_orders) etl_fact, (select count(*) from stg_orders) stg_orders, "
             "(select count(*) from etl_fact_orders where source_file like '%increment%') etl_from_increment, "
             "(select count(*) from stg_orders where source_file like '%increment%') stg_from_increment, "
             "(select count(*) from etl_fact_orders where order_status='RETURNED' and source_file like '%increment%') etl_returned_corrections, "
             "(select count(*) from stg_orders where order_status='RETURNED' and source_file like '%increment%') stg_returned_corrections, "
             "(select count(*)-count(distinct order_id) from etl_fact_orders) etl_dups, "
             "(select count(*)-count(distinct order_id) from stg_orders) stg_dups")
        log(3, "after:\n```\n" + duck(q) + "\n```")
    finally:
        pass


def e4(keys):
    log(4, "## Experiment 4 - Missing source file (products.csv renamed)")
    with Edit("products.csv") as p:
        p.rename(str(p) + ".missing")
        try:
            res = run_both(4, "products.csv missing", keys)
        finally:
            Path(str(p) + ".missing").rename(p)
    (ROOT / "include" / "profiling" / "experiments" / "e4_runs.json").write_text(json.dumps(res))
    log(4, "file restored.")


def e5(keys):
    log(5, "## Experiment 5 - New bad data (5 rows appended to orders.csv)")
    rows = [
        "ORD900001,2026-09-15 10:00:00,,S01,P0001,1,100.0,0,100.0,UPI,DELIVERED,App,15",
        "ORD900002,2026-09-15 10:05:00,C00001,S01,P0001,-2,100.0,0,-200.0,UPI,DELIVERED,App,15",
        "ORD900003,2026-09-15 10:10:00,C00001,S99,P0001,1,100.0,0,100.0,UPI,DELIVERED,App,15",
        "ORD900004,2026-09-15 10:15:00,C00001,S01,P0001,1,100.0,0,100.0,UPI,SHIPPED,App,15",
        "ORD900005,2027-03-01 10:20:00,C00001,S01,P0001,1,100.0,0,100.0,UPI,DELIVERED,App,15",
    ]
    with Edit("orders.csv") as p:
        with open(p, "a") as fh:
            fh.write("\n".join(rows) + "\n")
        res = run_both(5, "5 bad rows appended", keys)
        ids = "'ORD900001','ORD900002','ORD900003','ORD900004','ORD900005'"
        log(5, "etl_quarantine:\n```\n" + duck(f"select row_key, failure_reason from etl_quarantine where source_file='orders' and row_key in ({ids}) and run_id='{res.get('etl','')}' order by 1") + "\n```")
        log(5, "elt_rejected_rows:\n```\n" + duck(f"select row_key, failure_reason from elt_rejected_rows where source_table='orders' and row_key in ({ids}) and run_id='{res.get('elt','')}' order by 1") + "\n```")


def e6(keys):
    log(6, "## Experiment 6 - Schema change (order_amount renamed to total)")
    with Edit("orders.csv") as p:
        lines = p.read_text().splitlines()
        lines[0] = lines[0].replace("order_amount", "total")
        p.write_text("\n".join(lines) + "\n")
        run_both(6, "order_amount renamed to total", keys)


def e7(keys):
    log(7, "## Experiment 7 - Volume anomaly (only 50 orders)")
    with Edit("orders.csv") as p:
        lines = p.read_text().splitlines()
        p.write_text("\n".join(lines[:51]) + "\n")
        run_both(7, "orders.csv cut to 50 rows", keys)


def e8(keys):
    import pandas as pd
    log(8, "## Experiment 8 - Distribution drift (unit_price x100 for store S05)")
    with Edit("orders.csv") as p:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        m = df.store_id == "S05"
        df.loc[m, "unit_price"] = (pd.to_numeric(df.loc[m, "unit_price"]) * 100).astype(str)
        df.to_csv(p, index=False)
        log(8, f"rows changed: {int(m.sum())}")
        log(8, "S05 revenue before:\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, sum(delivered_orders) orders from mart_daily_store_kpis where store_id='S05' group by 1") + "\n```")
        run_both(8, "S05 unit_price x100", keys)
        log(8, "S05 revenue after (mart):\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, sum(delivered_orders) orders from mart_daily_store_kpis where store_id='S05' group by 1") + "\n```")
        log(8, "S05 revenue after (etl fact):\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, count(*) orders from etl_fact_orders where store_id='S05' and order_status='DELIVERED' group by 1") + "\n```")


def e9(keys):
    import pandas as pd
    log(9, "## Experiment 9 - Freshness (order_ts moved 30 days back)")
    with Edit("orders.csv") as p:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        t = pd.to_datetime(df.order_ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        move = t.notna() & (t <= pd.Timestamp.now())  # leave the future-dated rows alone
        df.loc[move, "order_ts"] = (t[move] - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
        df.to_csv(p, index=False)
        run_both(9, "order_ts shifted -30 days", keys)


def e10(keys):
    log(10, "## Experiment 10 - Recovery (clear failed task after fixing the cause)")
    runs = json.loads((OUT / "e4_runs.json").read_text())
    for key in keys:
        rid = runs[key]
        log(10, f"\n### {DAGS[key]} run={rid}: states before clear\n```\n{task_table(key, rid)}\n```")
        clear_task(key, rid, "extract_products")
        time.sleep(10)
        state, secs = wait(key, rid)
        log(10, f"after clear: run state={state} wall={secs}s\n```\n{task_table(key, rid)}\n```")
        log(10, "```\n" + run_summary(key, rid) + "\n```")


def e11(keys):
    import pandas as pd
    log(11, "## Experiment 8b - Consistent drift (unit_price AND order_amount x100 for store S05)")
    with Edit("orders.csv") as p:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        m = df.store_id == "S05"
        for col in ("unit_price", "order_amount"):
            df.loc[m, col] = (pd.to_numeric(df.loc[m, col]) * 100).astype(str)
        df.to_csv(p, index=False)
        log(11, f"rows changed: {int(m.sum())}")
        log(11, "S05 revenue before (mart):\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, sum(delivered_orders) orders from mart_daily_store_kpis where store_id='S05' group by 1") + "\n```")
        run_both(11, "S05 price and amount x100", keys)
        log(11, "S05 revenue after (mart):\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, sum(delivered_orders) orders from mart_daily_store_kpis where store_id='S05' group by 1") + "\n```")
        log(11, "S05 revenue after (etl fact):\n```\n" + duck("select store_id, round(sum(net_revenue),2) net_revenue, count(*) orders from etl_fact_orders where store_id='S05' and order_status='DELIVERED' group by 1") + "\n```")


EXPERIMENTS = {11: e11, 1: e1, 2: e2, 3: e3, 4: e4, 5: e5, 6: e6, 7: e7, 8: e8, 9: e9, 10: e10}

if __name__ == "__main__":
    which = sys.argv[1]
    keys = [k for k in (sys.argv[2].split(",") if len(sys.argv) > 2 else ["etl", "elt"])]
    for n in (EXPERIMENTS if which == "all" else [int(which)]):
        (OUT / f"exp_{n}.md").unlink(missing_ok=True) if OUT.exists() else None
        EXPERIMENTS[n](keys)
