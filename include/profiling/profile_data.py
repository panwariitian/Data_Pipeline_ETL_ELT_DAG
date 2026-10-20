"""Day-one profiling of the NammaMart exports (Part 0). Read-only: never edits the CSVs."""
import sys
from pathlib import Path

import pandas as pd

DATA = Path(__file__).resolve().parents[2] / "assignment" / "nammamart_dataset"
if not DATA.exists():
    DATA = Path(__file__).resolve().parents[2] / "assignment" / "nammamart_dataset"

FILES = ["orders", "payments", "customers", "products", "stores", "orders_2026-10-01_increment"]
KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id", "orders_2026-10-01_increment": "order_id"}
CATS = {"orders": ["order_status", "payment_mode", "channel"], "payments": ["payment_mode", "payment_status"],
        "customers": ["loyalty_tier", "city", "is_active"], "products": ["category", "unit", "is_perishable"],
        "stores": ["zone"], "orders_2026-10-01_increment": ["order_status", "payment_mode", "channel"]}


def load(name):
    return pd.read_csv(DATA / f"{name}.csv", dtype=str, keep_default_na=False, na_values=[""])


def profile(name):
    df = load(name)
    print(f"\n===== {name}: {len(df)} rows x {len(df.columns)} cols =====")
    print("nulls:", {c: int(n) for c, n in df.isna().sum().items() if n})
    key = KEYS[name]
    dup = df[df.duplicated(key, keep=False) & df[key].notna()]
    print(f"duplicate {key}: {dup[key].nunique()} keys / {len(dup)} rows; fully identical rows: {int(df.duplicated().sum())}")
    for c in df.columns:
        if c in CATS[name]:
            print(f"distinct {c}:", df[c].value_counts(dropna=False).to_dict())
            continue
        num = pd.to_numeric(df[c], errors="coerce")
        if num.notna().sum() > 0.8 * df[c].notna().sum() and df[c].notna().any():
            print(f"{c}: min={num.min()} max={num.max()} non-numeric={int((num.isna() & df[c].notna()).sum())}")
        elif c.endswith(("_ts", "_date")):
            ts = pd.to_datetime(df[c], errors="coerce", format="mixed")
            print(f"{c}: min={ts.min()} max={ts.max()} unparseable={int((ts.isna() & df[c].notna()).sum())}")
    return df


def main():
    d = {n: profile(n) for n in FILES}
    o, p, c, pr, s = d["orders"], d["payments"], d["customers"], d["products"], d["stores"]

    print("\n===== referential integrity =====")
    for col, ref, refcol in [("customer_id", c, "customer_id"), ("product_id", pr, "product_id"), ("store_id", s, "store_id")]:
        bad = o[o[col].notna() & ~o[col].isin(ref[refcol])]
        print(f"orders.{col} not in master: {len(bad)}")
    orphan = p[~p["order_id"].isin(o["order_id"])]
    print(f"payments.order_id not in orders: {len(orphan)}")
    cancelled_paid = o[o.order_status.str.upper().eq("CANCELLED") & o.order_id.isin(p.order_id)]
    print(f"cancelled orders that have a payment: {len(cancelled_paid)}")
    delivered_unpaid = o[o.order_status.str.upper().eq("DELIVERED") & ~o.order_id.isin(p.order_id)]
    print(f"delivered orders with no payment: {len(delivered_unpaid)}")

    print("\n===== business rules (orders) =====")
    q = pd.to_numeric(o.quantity, errors="coerce")
    up = pd.to_numeric(o.unit_price, errors="coerce")
    dp = pd.to_numeric(o.discount_pct, errors="coerce")
    amt = pd.to_numeric(o.order_amount, errors="coerce")
    calc = (q * up * (1 - dp / 100)).round(2)
    print("order_amount != formula:", int(((calc - amt).abs() > 0.01).sum()))
    print("quantity <= 0:", int((q <= 0).sum()), "| discount outside 0-100:", int(((dp < 0) | (dp > 100)).sum()))
    dm = pd.to_numeric(o.delivery_minutes, errors="coerce")
    deliv = o.order_status.str.upper().eq("DELIVERED")
    print("DELIVERED without delivery_minutes:", int((deliv & dm.isna()).sum()),
          "| delivery_minutes >120:", int((dm > 120).sum()), "| <8:", int((dm < 8).sum()))
    ots = pd.to_datetime(o.order_ts, errors="coerce", format="mixed")
    print("order_ts in future (after 2026-10-01):", int((ots > "2026-10-01").sum()))

    print("\n===== business rules (payments) =====")
    m = p.merge(o[["order_id", "order_amount"]].drop_duplicates("order_id"), on="order_id", how="inner")
    ok = m.payment_status.str.upper().eq("SUCCESS")
    diff = (pd.to_numeric(m.amount_paid, errors="coerce") - pd.to_numeric(m.order_amount, errors="coerce")).abs() > 0.01
    print("SUCCESS payments where amount != order_amount:", int((ok & diff).sum()))

    print("\n===== business rules (master data) =====")
    em = c.email.fillna("")
    print("bad email format:", int((~em.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") & c.email.notna()).sum()))
    ph = c.phone.fillna("").str.replace(r"\D", "", regex=True)
    print("phone not 10 digits:", int((ph.str.len() != 10).sum()), "| pincode not 6 digits:", int((~c.pincode.fillna("").str.fullmatch(r"\d{6}")).sum()))
    mrp = pd.to_numeric(pr.mrp, errors="coerce")
    cost = pd.to_numeric(pr.cost_price, errors="coerce")
    print("mrp<=0:", int((mrp <= 0).sum()), "| cost_price > mrp:", int((cost > mrp).sum()))
    print("signup_date in future:", int((pd.to_datetime(c.signup_date, errors="coerce", format="mixed") > "2026-10-01").sum()))

    print("\n===== increment file vs orders =====")
    inc = d["orders_2026-10-01_increment"]
    print("ids already in orders (corrections):", int(inc.order_id.isin(o.order_id).sum()), "| new ids:", int((~inc.order_id.isin(o.order_id)).sum()))
    print(inc[inc.order_id.isin(o.order_id)].order_status.value_counts().to_dict())


if __name__ == "__main__":
    sys.exit(main())
