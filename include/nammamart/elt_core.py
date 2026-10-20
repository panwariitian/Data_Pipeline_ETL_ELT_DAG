"""ELT pipeline logic: extract (check) -> load raw as received -> transform inside DuckDB with SQL files.

Raw tables are append-only per run and are never cleaned. Quality rules, typing, masking and
business tables all live in include/sql/elt/*.sql.
"""
import json
from pathlib import Path

from nammamart import config as cfg

DAG_ID = "nammamart_elt_pradeep"
SQL_ELT = cfg.SQL_DIR / "elt"
RAW_TABLE = {k: f"raw_{k}" for k in cfg.EXPECTED_COLUMNS}


def run_dir(run_id: str) -> Path:
    d = cfg.WORK_DIR / ("elt_" + cfg.safe_run_id(run_id))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _count_rows(path: Path) -> int:
    with open(path, encoding="utf-8") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def extract(file_key: str, run_id: str) -> dict:
    """Confirms the file(s) exist and still match the agreed columns, and counts rows."""
    paths = cfg.order_files() if file_key == "orders" else [cfg.DATA_DIR / f"{file_key}.csv"]
    files = []
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(f"missing source file: {p}")
        cfg.check_header(file_key, p)
        files.append({"path": str(p), "name": p.name, "rows": _count_rows(p), "seq": cfg.file_seq(p.name)})
    info = {"file_key": file_key, "files": files, "rows": sum(f["rows"] for f in files)}
    (run_dir(run_id) / f"extract_{file_key}.json").write_text(json.dumps(info, indent=2))
    print(f"[extract] {file_key}: " + ", ".join(f"{f['name']}={f['rows']}" for f in files))
    return info


def _render(sql: str, run_id: str) -> str:
    return (sql.replace("{{run_id}}", run_id.replace("'", "''"))
               .replace("{{dag_id}}", DAG_ID)
               .replace("{{now_ist}}", cfg.now_ist_naive().strftime("%Y-%m-%d %H:%M:%S")))


def run_sql_file(name: str, run_id: str) -> None:
    sql = _render((SQL_ELT / name).read_text(), run_id)
    con = cfg.connect()
    try:
        con.execute(sql)
        print(f"[sql] {name} done")
    finally:
        con.close()


def init_warehouse(run_id: str) -> None:
    run_sql_file("00_init.sql", run_id)


def load_raw(run_id: str, infos: list[dict], increments_only: bool = False) -> dict:
    """Copies each CSV into raw_<table> exactly as text, adding lineage columns. Re-loading the same
    run replaces only that run's rows for that file, so a retry never duplicates."""
    con = cfg.connect()
    loaded = {}
    try:
        for info in infos:
            key = info["file_key"]
            for f in info["files"]:
                is_increment = f["seq"] != 0
                if increments_only != is_increment:
                    continue
                table = RAW_TABLE[key]
                src = f["path"].replace("'", "''")
                select = (f"SELECT *, now()::TIMESTAMP AS _loaded_at, '{f['name']}' AS _source_file, "
                          f"'{run_id.replace(chr(39), chr(39) * 2)}' AS _run_id, {f['seq']}::INTEGER AS _source_seq, "
                          f"(row_number() OVER ()) + 1 AS _row_num "
                          f"FROM read_csv('{src}', all_varchar = true, header = true, strict_mode = false)")
                con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS {select} LIMIT 0")
                con.execute(f"DELETE FROM {table} WHERE _run_id = ? AND _source_file = ?", [run_id, f["name"]])
                con.execute(f"INSERT INTO {table} {select}")
                loaded[f["name"]] = f["rows"]
                print(f"[load_raw] {f['name']} -> {table}: {f['rows']} rows")
        return loaded
    finally:
        con.close()


def report(run_id: str) -> dict:
    con = cfg.connect()
    try:
        for t in ("mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla"):
            print(f"[report] {t}")
            print(con.execute(f"SELECT * FROM {t} ORDER BY 1 DESC LIMIT 10").df().to_string(index=False))
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis").fetchone()[0]
        (run_dir(run_id) / "mart_done").write_text("ok")
        return {"total_net_revenue": total}
    finally:
        con.close()


def audit(run_id: str, start_time, end_time) -> dict:
    """Runs with trigger_rule=all_done so a failed run still leaves a pipeline_runs row."""
    from nammamart.etl_core import _naive_ist

    d = run_dir(run_id)
    start_time, end_time = _naive_ist(start_time), _naive_ist(end_time)
    con = cfg.connect()
    try:
        con.execute((SQL_ELT / "00_init.sql").read_text())
        extracted = 0
        for t in RAW_TABLE.values():
            try:
                extracted += con.execute(f"SELECT count(*) FROM {t} WHERE _run_id = ?", [run_id]).fetchone()[0]
            except Exception:
                pass
        quarantined = con.execute("SELECT count(*) FROM elt_rejected_rows WHERE run_id = ?", [run_id]).fetchone()[0]
        status = "SUCCESS" if (d / "mart_done").exists() else "FAILED"
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, DAG_ID, start_time, end_time, extracted, extracted - quarantined, quarantined, status])
        print(f"[audit] run={run_id} status={status} extracted={extracted} quarantined={quarantined}")
        return {"status": status, "rows_extracted": extracted, "rows_quarantined": quarantined}
    finally:
        con.close()
