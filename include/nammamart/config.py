"""Paths and constants shared by the NammaMart ETL and ELT pipelines."""
import os
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

BASE = Path(os.environ.get("NM_INCLUDE_DIR", Path(__file__).resolve().parents[1]))
DATA_DIR = BASE / "data" / "nammamart"
WAREHOUSE = BASE / "warehouse_pradeep.duckdb"
QUARANTINE_DIR = BASE / "quarantine" / "pradeep"
WORK_DIR = BASE / "work" / "pradeep"
SQL_DIR = BASE / "sql"

# one entry per source file: (file name, key column, expected header)
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}
KEY_COLUMN = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
              "products": "product_id", "stores": "store_id"}

ORDER_STATUSES = ["DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_MODES = {"upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
                 "cod": "Cash on Delivery", "wallet": "Wallet"}
PAYMENT_STATUSES = ["SUCCESS", "REFUNDED", "FAILED"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
ZONES = ["North", "South", "East", "West"]

IST = ZoneInfo("Asia/Kolkata")


def now_ist_naive() -> datetime:
    """Source timestamps carry no timezone; treat them as IST wall-clock."""
    return datetime.now(IST).replace(tzinfo=None)


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:+-]", "_", str(run_id))


def order_files() -> list[Path]:
    """Base orders file first, then increments in date order."""
    base = DATA_DIR / "orders.csv"
    incs = sorted(DATA_DIR.glob("orders_*_increment.csv"))
    return [base, *incs]


def file_seq(name: str) -> int:
    """orders.csv is 0; an increment file gets its date (e.g. 20261001) so later files win."""
    m = re.search(r"orders_(\d{4})-(\d{2})-(\d{2})_increment", name)
    return int("".join(m.groups())) if m else 0


def connect(read_only: bool = False, wait_seconds: int = 300):
    """DuckDB allows one writer process, so wait for the file lock instead of failing the task."""
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + wait_seconds
    while True:
        try:
            return duckdb.connect(str(WAREHOUSE), read_only=read_only)
        except duckdb.IOException:
            if time.time() > deadline:
                raise
            time.sleep(2)


def check_header(file_key: str, path: Path) -> None:
    """Fail loudly when a source file's columns are not the agreed contract."""
    with open(path, encoding="utf-8") as fh:
        header = [h.strip() for h in fh.readline().strip().split(",")]
    expected = EXPECTED_COLUMNS[file_key]
    missing = [c for c in expected if c not in header]
    extra = [c for c in header if c not in expected]
    if missing or extra:
        raise ValueError(f"{path.name}: schema changed. missing={missing} unexpected={extra}")
