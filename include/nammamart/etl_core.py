"""ETL pipeline logic: extract -> validate -> transform -> load -> reconcile -> report -> audit.

The Airflow DAG (dags/nammamart_etl_pradeep.py) only wires these functions into tasks.
Tasks hand files to each other through include/work/pradeep/<run_id>/ so that a failed
run can be inspected and a single task can be cleared and re-run.
"""
import json
from pathlib import Path

import pandas as pd

from nammamart import config as cfg
from nammamart.masking import mask_email, mask_name, mask_phone
from nammamart.quality import (canon_payment_mode, canon_tier, dataset_check, ts, upper, run_checks)

DAG_ID = "nammamart_etl_pradeep"


# ------------------------------------------------------------------ helpers
def run_dir(run_id: str) -> Path:
    d = cfg.WORK_DIR / cfg.safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def read_csv(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def write_json(run_id: str, name: str, payload: dict) -> None:
    (run_dir(run_id) / f"{name}.json").write_text(json.dumps(payload, default=str, indent=2))


# ------------------------------------------------------------------ extract
def extract(file_key: str, run_id: str) -> dict:
    out_dir = run_dir(run_id)
    if file_key == "orders":
        frames = []
        for path in cfg.order_files():
            if not path.exists():
                raise FileNotFoundError(f"missing source file: {path}")
            cfg.check_header("orders", path)
            part = read_csv(path)
            part["_source_file"] = path.name
            part["_source_seq"] = cfg.file_seq(path.name)
            part["_source_row"] = range(2, len(part) + 2)
            frames.append(part)
        df = pd.concat(frames, ignore_index=True)
    else:
        path = cfg.DATA_DIR / f"{file_key}.csv"
        if not path.exists():
            raise FileNotFoundError(f"missing source file: {path}")
        cfg.check_header(file_key, path)
        df = read_csv(path)
        df["_source_file"] = path.name
        df["_source_seq"] = 0
        df["_source_row"] = range(2, len(df) + 2)
    target = out_dir / f"extract_{file_key}.csv"
    df.to_csv(target, index=False)
    info = {"file_key": file_key, "path": str(target), "rows": len(df)}
    write_json(run_id, f"extract_{file_key}", info)
    print(f"[extract] {file_key}: {len(df)} rows")
    return info


# ----------------------------------------------------------------- validate
def _write_validation(file_key, run_id, passed, failed, dq_rows, rows_in, mask_pii=False):
    out_dir = run_dir(run_id)
    passed_path = out_dir / f"passed_{file_key}.csv"
    passed.to_csv(passed_path, index=False)
    qdir = cfg.QUARANTINE_DIR
    qdir.mkdir(parents=True, exist_ok=True)
    qfile = qdir / f"{file_key}_failed_{cfg.safe_run_id(run_id)}.csv"
    out = failed.copy()
    if mask_pii and len(out):
        out["email"] = out["email"].map(mask_email)
        out["phone"] = out["phone"].map(mask_phone)
        out["full_name"] = out["full_name"].map(mask_name)
    out.to_csv(qfile, index=False)
    summary = {"file_key": file_key, "rows_in": rows_in, "rows_passed": len(passed), "rows_failed": len(failed),
               "passed_path": str(passed_path), "quarantine_path": str(qfile), "dq": dq_rows}
    write_json(run_id, f"validate_{file_key}", summary)
    print(f"[validate] {file_key}: in={rows_in} passed={len(passed)} quarantined={len(failed)}")
    for r in dq_rows:
        if r["rows_failed"]:
            print(f"   {r['status']:4} {r['check_name']:55} {r['rows_failed']} rows ({r['severity']})")
    return summary


def validate(file_key: str, run_id: str, extract_info: dict, deps: dict | None = None) -> dict:
    now = cfg.now_ist_naive()
    df = read_csv(extract_info["path"])
    ctx = {"now": now}
    if file_key == "orders":
        ctx["customers"] = set(read_csv(deps["customers"]["passed_path"]).customer_id)
        clean_products = read_csv(deps["products"]["passed_path"])
        ctx["products"] = set(clean_products.product_id)
        ctx["mrp"] = dict(zip(clean_products.product_id, pd.to_numeric(clean_products.mrp, errors="coerce")))
        ctx["stores"] = set(read_csv(deps["stores"]["passed_path"]).store_id)
        for key, col in (("customers", "customer_id"), ("products", "product_id"), ("stores", "store_id")):
            ctx[f"{key}_all"] = set(read_csv(deps[f"{key}_extract"]["path"])[col])
    if file_key == "payments":
        ctx["order_amounts"] = _known_order_amounts(deps["orders_extract"]["path"], deps["orders_validated"]["passed_path"])
        ctx["order_ids"] = set(ctx["order_amounts"])
    passed, failed, dq = run_checks(df, file_key, ctx, run_id, DAG_ID)
    if file_key == "orders":
        dq += _orders_dataset_checks(df, run_id, now)
    return _write_validation(file_key, run_id, passed, failed, dq, len(df), mask_pii=(file_key == "customers"))


def _known_order_amounts(extract_path: str, passed_path: str) -> dict:
    """Best known amount for every order we have ever seen, so payments are judged against the same
    order view the ELT staging table holds: a passing version beats a failing one, newest file wins.
    Precedence, lowest to highest: quarantined history, today's rows as received, loaded fact, today's passing rows."""
    history, fact = {}, {}
    try:
        con = cfg.connect(read_only=True, wait_seconds=60)
        try:
            for key, raw in con.execute("SELECT row_key, raw_row FROM etl_quarantine WHERE source_file = 'orders' "
                                        "ORDER BY quarantined_at").fetchall():
                history[key] = json.loads(raw).get("order_amount")
            for key, amt in con.execute("SELECT order_id, order_amount FROM etl_fact_orders").fetchall():
                fact[key] = str(amt)
        finally:
            con.close()
    except Exception:
        pass  # first ever run: no warehouse yet
    latest = lambda df: df.sort_values(["_source_seq", "_source_row"]).drop_duplicates("order_id", keep="last")
    this_run = latest(read_csv(extract_path))
    passed = latest(read_csv(passed_path))
    amounts = dict(history)
    amounts.update(zip(this_run.order_id, this_run.order_amount))
    amounts.update(fact)
    amounts.update(zip(passed.order_id, passed.order_amount))
    return amounts


def _orders_dataset_checks(df, run_id, now) -> list[dict]:
    parsed = ts(df.order_ts)
    newest = parsed[parsed <= now].max()  # a future-dated row must not make the feed look fresh
    stale = pd.isna(newest) or newest < now - pd.Timedelta(days=1)
    rows = [dataset_check("orders.latest_order_not_older_than_1_day", "Timeliness", "Warning", bool(stale),
                          run_id, DAG_ID, now)]
    previous = _median_previous_volume()
    dropped = previous is not None and len(df) < 0.1 * previous
    rows.append(dataset_check("orders.volume_not_down_more_than_90pct", "Completeness", "Warning", dropped,
                              run_id, DAG_ID, now))
    return rows


def _median_previous_volume():
    try:
        con = cfg.connect(read_only=True, wait_seconds=60)
    except Exception:
        return None
    try:
        row = con.execute(
            """SELECT median(rows_checked) FROM (
                 SELECT rows_checked FROM dq_results
                 WHERE dag_id = ? AND check_name = 'orders.order_id_not_null'
                 ORDER BY checked_at DESC LIMIT 7)""", [DAG_ID]).fetchone()
        return row[0] if row and row[0] is not None else None
    except Exception:
        return None
    finally:
        con.close()


# ---------------------------------------------------------------- transform
def transform(run_id: str, v: dict) -> dict:
    out_dir = run_dir(run_id)
    now_stamp = cfg.now_ist_naive()
    cust = read_csv(v["customers"]["passed_path"]).copy()
    prod = read_csv(v["products"]["passed_path"]).copy()
    store = read_csv(v["stores"]["passed_path"]).copy()
    orders = read_csv(v["orders"]["passed_path"]).copy()

    dim_customer = pd.DataFrame({
        "customer_id": cust.customer_id.str.strip(),
        "email_masked": cust.email.map(mask_email),
        "phone_masked": cust.phone.map(mask_phone),
        "city": cust.city.fillna("Unknown").str.strip(),
        "area": cust.area.str.strip(),
        "pincode": cust.pincode.where(cust.pincode.astype("string").str.fullmatch(r"\d{6}"), None),
        "signup_date": pd.to_datetime(cust.signup_date, format="%Y-%m-%d").dt.date,
        "loyalty_tier": canon_tier(cust.loyalty_tier),
        "is_active": cust.is_active.map({"Y": True, "N": False}),
    })
    dim_product = pd.DataFrame({
        "product_id": prod.product_id.str.strip(),
        "product_name": prod.product_name.str.strip(),
        "category": prod.category.str.strip(),
        "sub_category": prod.sub_category.str.strip(),
        "brand": prod.brand.str.strip(),
        "mrp": pd.to_numeric(prod.mrp),
        "cost_price": pd.to_numeric(prod.cost_price),
        "unit": prod.unit,
        "is_perishable": prod.is_perishable.map({"True": True, "False": False}),
        "launch_date": pd.to_datetime(prod.launch_date, format="%Y-%m-%d").dt.date,
    })
    dim_store = pd.DataFrame({
        "store_id": store.store_id.str.strip(),
        "store_name": store.store_name.str.strip(),
        "zone": store.zone.fillna("Unknown").str.strip(),
        "area": store.area.str.strip(),
        "pincode": store.pincode,
        "open_date": pd.to_datetime(store.open_date, format="%Y-%m-%d").dt.date,
        "manager_name": store.manager_name,
        "capacity_orders_per_day": pd.to_numeric(store.capacity_orders_per_day).astype("Int64"),
    })

    # a corrected order in a later file replaces the earlier version
    orders = orders.sort_values(["_source_seq", "_source_row"]).drop_duplicates("order_id", keep="last")
    f = orders.merge(dim_product[["product_id", "category", "cost_price"]], on="product_id", how="left")
    f = f.merge(dim_store[["store_id", "zone"]].rename(columns={"zone": "store_zone"}), on="store_id", how="left")
    f["order_ts"] = ts(f.order_ts)
    f["order_status"] = upper(f.order_status)
    f["payment_mode"] = canon_payment_mode(f.payment_mode)
    for col in ("quantity", "unit_price", "discount_pct", "order_amount"):
        f[col] = pd.to_numeric(f[col])
    f["delivery_minutes"] = pd.to_numeric(f.delivery_minutes).astype("Int64")
    delivered = f.order_status == "DELIVERED"
    f["cost_amount"] = (f.quantity * f.cost_price).round(2)
    f["net_revenue"] = f.order_amount.where(delivered, 0.0)
    f["gross_margin"] = (f.net_revenue - f.cost_amount).where(delivered, 0.0).round(2)
    f["within_15_min"] = delivered & (f.delivery_minutes <= 15).fillna(False)
    fact = pd.DataFrame({
        "order_id": f.order_id, "order_ts": f.order_ts, "order_date": f.order_ts.dt.date,
        "customer_id": f.customer_id, "product_id": f.product_id, "store_id": f.store_id,
        "store_zone": f.store_zone, "category": f.category, "quantity": f.quantity.astype(int),
        "unit_price": f.unit_price, "discount_pct": f.discount_pct, "order_amount": f.order_amount,
        "cost_amount": f.cost_amount, "payment_mode": f.payment_mode, "order_status": f.order_status,
        "channel": f.channel.str.strip(), "delivery_minutes": f.delivery_minutes,
        "net_revenue": f.net_revenue, "gross_margin": f.gross_margin, "within_15_min": f.within_15_min,
        "source_file": f._source_file, "source_seq": f._source_seq.astype(int),
        "source_row": f._source_row.astype(int),
    })
    paths = {}
    for name, frame in {"fact_orders": fact, "dim_customer": dim_customer, "dim_product": dim_product,
                        "dim_store": dim_store}.items():
        p = out_dir / f"transformed_{name}.pkl"
        frame.to_pickle(p)
        paths[name] = str(p)
    info = {"paths": paths, "fact_rows": len(fact), "transformed_at": str(now_stamp)}
    write_json(run_id, "transform", info)
    print(f"[transform] fact={len(fact)} customers={len(dim_customer)} products={len(dim_product)} stores={len(dim_store)}")
    return info


# --------------------------------------------------------------------- load
DDL = {
    "etl_dim_customer": """(customer_id VARCHAR, email_masked VARCHAR, phone_masked VARCHAR, city VARCHAR,
        area VARCHAR, pincode VARCHAR, signup_date DATE, loyalty_tier VARCHAR, is_active BOOLEAN,
        _run_id VARCHAR, _loaded_at TIMESTAMP)""",
    "etl_dim_product": """(product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR,
        brand VARCHAR, mrp DOUBLE, cost_price DOUBLE, unit VARCHAR, is_perishable BOOLEAN, launch_date DATE,
        _run_id VARCHAR, _loaded_at TIMESTAMP)""",
    "etl_dim_store": """(store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR,
        open_date DATE, manager_name VARCHAR, capacity_orders_per_day INTEGER,
        _run_id VARCHAR, _loaded_at TIMESTAMP)""",
    "etl_fact_orders": """(order_id VARCHAR, order_ts TIMESTAMP, order_date DATE, customer_id VARCHAR,
        product_id VARCHAR, store_id VARCHAR, store_zone VARCHAR, category VARCHAR, quantity INTEGER,
        unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE, cost_amount DOUBLE, payment_mode VARCHAR,
        order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER, net_revenue DOUBLE,
        gross_margin DOUBLE, within_15_min BOOLEAN, source_file VARCHAR, source_seq INTEGER,
        source_row INTEGER, _run_id VARCHAR, _loaded_at TIMESTAMP)""",
}
HISTORY_DDL = {
    "etl_dim_customer_history": """(customer_key VARCHAR, customer_id VARCHAR, loyalty_tier VARCHAR,
        valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _run_id VARCHAR, _loaded_at TIMESTAMP)""",
}
AUDIT_DDL = {
    "dq_results": """(run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
        rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP)""",
    "pipeline_runs": """(run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
        rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR)""",
    "etl_quarantine": """(run_id VARCHAR, source_file VARCHAR, row_key VARCHAR, failure_reason VARCHAR,
        raw_row VARCHAR, quarantined_at TIMESTAMP)""",
}


def ensure_tables(con) -> None:
    for name, ddl in {**DDL, **HISTORY_DDL, **AUDIT_DDL}.items():
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} {ddl}")
    con.execute("ALTER TABLE etl_fact_orders ADD COLUMN IF NOT EXISTS source_row INTEGER")


def _upsert(con, table: str, key: str, df: pd.DataFrame, run_id: str, guard_seq: bool = False) -> None:
    df = df.copy()
    df["_run_id"] = run_id
    df["_loaded_at"] = cfg.now_ist_naive()
    con.register("incoming", df)
    try:
        con.execute("BEGIN")
        if guard_seq:
            # never let an older file overwrite a row that a newer file already corrected
            con.execute(f"""CREATE OR REPLACE TEMP TABLE keep_rows AS
                SELECT i.* FROM incoming i
                LEFT JOIN {table} t ON t.{key} = i.{key}
                WHERE t.{key} IS NULL OR i.source_seq >= t.source_seq""")
            con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM keep_rows)")
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM keep_rows")
        else:
            con.execute(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM incoming)")
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM incoming")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("incoming")


def _scd2_customers(con, dim_customer: pd.DataFrame, run_id: str, now) -> dict:
    """Slowly changing dimension, type 2: a customer gets a new version when its loyalty tier changes.
    The old version ends at the run time (valid_to, is_current = FALSE) and the new one starts there.
    A re-run with no tier change adds nothing. History starts at the first load: earlier changes are unknown."""
    open_tier = dict(con.execute("SELECT customer_id, loyalty_tier FROM etl_dim_customer_history "
                                 "WHERE is_current").fetchall())
    opened = closed = 0
    for cid, tier in zip(dim_customer.customer_id, dim_customer.loyalty_tier):
        tier = "Unknown" if pd.isna(tier) else tier
        if open_tier.get(cid) == tier:
            continue
        if cid in open_tier:
            con.execute("UPDATE etl_dim_customer_history SET valid_to = ?, is_current = FALSE "
                        "WHERE customer_id = ? AND is_current", [now, cid])
            closed += 1
        version = con.execute("SELECT count(*) + 1 FROM etl_dim_customer_history WHERE customer_id = ?",
                              [cid]).fetchone()[0]
        con.execute("INSERT INTO etl_dim_customer_history VALUES (?, ?, ?, ?, NULL, TRUE, ?, ?)",
                    [f"{cid}-{version}", cid, tier, now, run_id, now])
        opened += 1
    print(f"[scd2] customers: {opened} versions opened, {closed} closed (tier changes)")
    return {"opened": opened, "closed": closed}


def load(run_id: str, t: dict) -> dict:
    con = cfg.connect()
    try:
        ensure_tables(con)
        p = t["paths"]
        _scd2_customers(con, pd.read_pickle(p["dim_customer"]), run_id, cfg.now_ist_naive())
        _upsert(con, "etl_dim_customer", "customer_id", pd.read_pickle(p["dim_customer"]), run_id)
        _upsert(con, "etl_dim_product", "product_id", pd.read_pickle(p["dim_product"]), run_id)
        _upsert(con, "etl_dim_store", "store_id", pd.read_pickle(p["dim_store"]), run_id)
        _upsert(con, "etl_fact_orders", "order_id", pd.read_pickle(p["fact_orders"]), run_id, guard_seq=True)
        n, distinct = con.execute("SELECT count(*), count(DISTINCT order_id) FROM etl_fact_orders").fetchone()
        if n != distinct:
            raise RuntimeError(f"etl_fact_orders has duplicate order_ids ({n} rows, {distinct} distinct)")
        print(f"[load] etl_fact_orders now holds {n} rows (unique order_id)")
        return {"fact_rows": n}
    finally:
        con.close()


# ---------------------------------------------------------------- reconcile
def reconcile(run_id: str, extract_infos: dict) -> dict:
    """One row per order/payment mismatch, with the reason."""
    pay = read_csv(extract_infos["payments"]["path"]).drop_duplicates("payment_id", keep="first")
    pay["amount_paid"] = pd.to_numeric(pay.amount_paid, errors="coerce")
    pay["payment_status"] = upper(pay.payment_status)
    pay_ts = ts(pay.payment_ts)
    feed_end = pay_ts[pay_ts <= cfg.now_ist_naive()].max()
    src_orders = read_csv(extract_infos["orders"]["path"]).sort_values(["_source_seq", "_source_row"])
    source_ids = set(src_orders.drop_duplicates("order_id", keep="last").order_id)

    con = cfg.connect()
    try:
        fact = con.execute(
            "SELECT order_id, order_ts, order_status, order_amount, payment_mode, store_id FROM etl_fact_orders").df()
        pay_by_order = pay.set_index("order_id")
        rows = []
        for o in fact.itertuples():
            p = pay_by_order.loc[o.order_id] if o.order_id in pay_by_order.index else None
            reason = None
            if o.order_status == "DELIVERED":
                if p is None:
                    reason = ("DELIVERED_PAYMENT_NOT_YET_RECEIVED" if o.order_ts > feed_end
                              else "DELIVERED_NO_PAYMENT")
                elif p.payment_status == "FAILED":
                    reason = "DELIVERED_PAYMENT_FAILED"
                elif p.payment_status == "REFUNDED":
                    reason = "DELIVERED_BUT_REFUNDED"
                elif abs(p.amount_paid - o.order_amount) > 0.01:
                    reason = "AMOUNT_MISMATCH"
            elif o.order_status == "RETURNED":
                if p is None:
                    reason = "RETURNED_NO_PAYMENT_RECORD"
                elif p.payment_status == "SUCCESS":
                    reason = "RETURNED_NOT_REFUNDED"
                elif abs(p.amount_paid - o.order_amount) > 0.01:
                    reason = "AMOUNT_MISMATCH"
            elif o.order_status == "CANCELLED" and p is not None:
                reason = "CANCELLED_BUT_PAID"
            if reason:
                rows.append(dict(order_id=o.order_id, payment_id=None if p is None else p.payment_id,
                                 order_status=o.order_status,
                                 payment_status=None if p is None else p.payment_status,
                                 order_amount=o.order_amount,
                                 amount_paid=None if p is None else p.amount_paid, mismatch_reason=reason))
        clean_ids = set(fact.order_id)
        for p in pay.itertuples():
            if p.order_id in clean_ids:
                continue
            reason = "PAYMENT_ORDER_QUARANTINED" if p.order_id in source_ids else "PAYMENT_WITHOUT_ORDER"
            rows.append(dict(order_id=p.order_id, payment_id=p.payment_id, order_status=None,
                             payment_status=p.payment_status, order_amount=None, amount_paid=p.amount_paid,
                             mismatch_reason=reason))
        rec = pd.DataFrame(rows, columns=["order_id", "payment_id", "order_status", "payment_status",
                                          "order_amount", "amount_paid", "mismatch_reason"])
        payment_side = rec.mismatch_reason.isin(["PAYMENT_WITHOUT_ORDER", "PAYMENT_ORDER_QUARANTINED"])
        rec["unreconciled_amount"] = rec.order_amount.where(~payment_side, rec.amount_paid)
        is_amount = rec.mismatch_reason == "AMOUNT_MISMATCH"
        rec.loc[is_amount, "unreconciled_amount"] = (rec.order_amount - rec.amount_paid).abs()[is_amount]
        rec["unreconciled_amount"] = rec.unreconciled_amount.astype(float).round(2)
        rec["_run_id"] = run_id
        con.register("rec", rec)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM rec")
        con.unregister("rec")
        summary = rec.groupby("mismatch_reason").agg(rows=("order_id", "size"), amount=("unreconciled_amount", "sum"))
        print("[reconcile]\n" + summary.to_string())
        return {"mismatches": len(rec), "unreconciled_amount": float(rec.unreconciled_amount.sum())}
    finally:
        con.close()


# ------------------------------------------------------------------- report
def report(run_id: str) -> dict:
    con = cfg.connect()
    try:
        con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, store_zone AS zone, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
            FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1, 2 ORDER BY 1, 2""")
        con.execute("""CREATE OR REPLACE TABLE etl_daily_store_kpis AS
            SELECT f.order_date, f.store_id, s.store_name, f.store_zone AS zone,
                   count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_revenue,
                   round(sum(f.gross_margin), 2) AS gross_margin,
                   round(avg(f.delivery_minutes), 2) AS avg_delivery_minutes,
                   round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED' GROUP BY 1, 2, 3, 4 ORDER BY 1, 2""")
        con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category, round(sum(net_revenue), 2) AS net_revenue, round(sum(gross_margin), 2) AS gross_margin,
                   round(100.0 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
            FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 2 DESC""")
        print("[report] etl_daily_zone_revenue (last 7 days)")
        print(con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY order_date DESC, zone LIMIT 28").df().to_string(index=False))
        print("[report] etl_category_margin")
        print(con.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
        (run_dir(run_id) / "report_done").write_text("ok")
        return {"total_net_revenue": total}
    finally:
        con.close()


# -------------------------------------------------------------------- audit
def capacity_check(con, run_id: str, now) -> dict:
    """Orders per store per day against capacity_orders_per_day. Warning only: the orders still load."""
    daily = con.execute("""SELECT f.store_id, f.order_date, s.capacity_orders_per_day AS capacity, count(*) AS orders
                           FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
                           GROUP BY 1, 2, 3 ORDER BY 1, 2""").df()
    over = daily.orders > daily.capacity.fillna(0)
    daily["over_capacity"] = over
    daily["_run_id"] = run_id
    con.register("cap", daily)
    con.execute("CREATE OR REPLACE TABLE etl_store_capacity AS SELECT * FROM cap")
    con.unregister("cap")
    failed = int(over.sum())
    return dict(run_id=run_id, dag_id=DAG_ID, check_name="capacity.daily_orders_within_capacity",
                dimension="Accuracy", severity="Warning", rows_checked=len(daily), rows_failed=failed,
                status="PASS" if failed == 0 else "WARN", checked_at=now)


def _naive_ist(t):
    return t.astimezone(cfg.IST).replace(tzinfo=None) if getattr(t, "tzinfo", None) else t


def audit(run_id: str, start_time, end_time, expected_ok: bool) -> dict:
    """Runs with trigger_rule=all_done: writes dq_results, quarantine rows and one pipeline_runs row."""
    start_time, end_time = _naive_ist(start_time), _naive_ist(end_time)
    d = run_dir(run_id)
    extracted = loaded = quarantined = 0
    dq_rows, q_rows = [], []
    now = cfg.now_ist_naive()
    for f in cfg.EXPECTED_COLUMNS:
        ex = d / f"extract_{f}.json"
        if ex.exists():
            extracted += json.loads(ex.read_text())["rows"]
        va = d / f"validate_{f}.json"
        if va.exists():
            s = json.loads(va.read_text())
            loaded += s["rows_passed"]
            quarantined += s["rows_failed"]
            dq_rows += s["dq"]
            q = Path(s["quarantine_path"])
            if q.exists() and s["rows_failed"]:
                qdf = read_csv(q)
                key = cfg.KEY_COLUMN[f]
                for rec in qdf.to_dict("records"):
                    q_rows.append(dict(run_id=run_id, source_file=f, row_key=rec.get(key),
                                       failure_reason=rec.get("failure_reason"),
                                       raw_row=json.dumps({k: v for k, v in rec.items() if k != "failure_reason"},
                                                          default=str), quarantined_at=now))
    status = "SUCCESS" if (d / "report_done").exists() and expected_ok else "FAILED"
    con = cfg.connect()
    try:
        ensure_tables(con)
        if status == "SUCCESS":
            dq_rows.append(capacity_check(con, run_id, now))
        for table, rows in (("dq_results", dq_rows), ("etl_quarantine", q_rows)):
            con.execute(f"DELETE FROM {table} WHERE run_id = ?" + (" AND dag_id = ?" if table == "dq_results" else ""),
                        [run_id, DAG_ID] if table == "dq_results" else [run_id])
            if rows:
                df = pd.DataFrame(rows)
                con.register("batch", df)
                con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM batch")
                con.unregister("batch")
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, DAG_ID, start_time, end_time, extracted, loaded, quarantined, status])
        print(f"[audit] run={run_id} status={status} extracted={extracted} loaded={loaded} quarantined={quarantined}")
    finally:
        con.close()
    return {"status": status, "rows_extracted": extracted, "rows_loaded": loaded, "rows_quarantined": quarantined}
