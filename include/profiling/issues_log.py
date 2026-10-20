"""Builds the row-level Data Issues Log (Part 0) as a CSV and a markdown table."""
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
DATA = BASE.parent / "assignment" / "nammamart_dataset"  # pristine copy; include/data is edited by the experiments
OUT = BASE / "profiling"
rows = []


def log(file, key, col, problem, dim, rule, sev):
    rows.append(dict(file=file, row_key=key, column=col, problem=problem, quality_dimension=dim,
                     proposed_rule=rule, severity=sev))


def load(n):
    return pd.read_csv(DATA / f"{n}.csv", dtype=str, keep_default_na=False, na_values=[""])


o, p, c, pr, s = (load(n) for n in ["orders", "payments", "customers", "products", "stores"])
num = lambda df, col: pd.to_numeric(df[col], errors="coerce")

# ---- orders
for _, r in o[o.duplicated("order_id", keep="first")].iterrows():
    log("orders", r.order_id, "order_id", "exact duplicate row", "Uniqueness", "keep first row per order_id", "Critical")
for _, r in o[o.customer_id.isna()].iterrows():
    log("orders", r.order_id, "customer_id", "null customer_id", "Completeness", "customer_id not null", "Critical")
for _, r in o[num(o, "quantity") <= 0].iterrows():
    log("orders", r.order_id, "quantity", f"quantity {r.quantity} is not positive", "Accuracy", "quantity > 0", "Critical")
for _, r in o[(num(o, "discount_pct") < 0) | (num(o, "discount_pct") > 100)].iterrows():
    log("orders", r.order_id, "discount_pct", f"discount {r.discount_pct}% outside 0-100", "Validity", "0 <= discount_pct <= 100", "Critical")
calc = (num(o, "quantity") * num(o, "unit_price") * (1 - num(o, "discount_pct") / 100)).round(2)
for _, r in o[(calc - num(o, "order_amount")).abs() > 0.01].iterrows():
    log("orders", r.order_id, "order_amount", f"amount {r.order_amount} does not match formula", "Accuracy", "recompute amount, compare", "Critical")
ts = pd.to_datetime(o.order_ts, errors="coerce", format="mixed")
for _, r in o[ts.isna() & o.order_ts.notna()].iterrows():
    log("orders", r.order_id, "order_ts", f"unparseable timestamp '{r.order_ts}'", "Validity", "order_ts must parse", "Critical")
for _, r in o[ts > "2026-10-01"].iterrows():
    log("orders", r.order_id, "order_ts", f"future-dated {r.order_ts}", "Timeliness", "order_ts <= run time", "Critical")
for _, r in o[~o.order_status.isin(["DELIVERED", "CANCELLED", "RETURNED"])].iterrows():
    fixable = r.order_status.strip().upper() in ("DELIVERED", "CANCELLED", "RETURNED")
    log("orders", r.order_id, "order_status", f"'{r.order_status}' " + ("differs only in casing/whitespace" if fixable else "is not an allowed status"),
        "Validity" if not fixable else "Consistency", "standardise with trim/upper" if fixable else "status must be DELIVERED/CANCELLED/RETURNED",
        "Warning" if fixable else "Critical")
KNOWN = {"upi", "card", "cash on delivery", "cod", "wallet"}
for _, r in o[~o.payment_mode.isin(["UPI", "Card", "Cash on Delivery", "Wallet"])].iterrows():
    fixable = r.payment_mode.strip().lower() in KNOWN
    log("orders", r.order_id, "payment_mode", f"'{r.payment_mode}' " + ("is a known variant" if fixable else "is not a recognisable payment mode"),
        "Consistency" if fixable else "Validity", "map known variants to one spelling" if fixable else "payment_mode in allowed list",
        "Warning" if fixable else "Critical")
dm = num(o, "delivery_minutes")
for _, r in o[o.order_status.str.upper().eq("DELIVERED") & dm.isna()].iterrows():
    log("orders", r.order_id, "delivery_minutes", "DELIVERED without delivery_minutes", "Completeness", "required for DELIVERED", "Critical")
for _, r in o[dm > 120].iterrows():
    log("orders", r.order_id, "delivery_minutes", f"{r.delivery_minutes} minutes is implausible", "Accuracy", "delivery_minutes < 120", "Critical")
for col, ref, name in [("product_id", pr, "products"), ("store_id", s, "stores")]:
    for _, r in o[o[col].notna() & ~o[col].isin(ref[col])].iterrows():
        log("orders", r.order_id, col, f"{r[col]} not in {name}", "Consistency", f"{col} must exist in {name}", "Critical")

# ---- payments
for _, r in p[p.duplicated("payment_id", keep="first")].iterrows():
    log("payments", r.payment_id, "payment_id", "exact duplicate row", "Uniqueness", "keep first per payment_id", "Critical")
for _, r in p[~p.order_id.isin(o.order_id)].iterrows():
    log("payments", r.payment_id, "order_id", f"{r.order_id} not in orders", "Consistency", "payment must match an order", "Critical")
m = p.merge(o.drop_duplicates("order_id")[["order_id", "order_amount"]], on="order_id")
bad = m[m.payment_status.eq("SUCCESS") & ((num(m, "amount_paid") - num(m, "order_amount")).abs() > 0.01)]
for _, r in bad.iterrows():
    log("payments", r.payment_id, "amount_paid", f"paid {r.amount_paid} vs order {r.order_amount}", "Consistency", "SUCCESS amount = order_amount", "Critical")
for _, r in p[~p.payment_mode.isin(["UPI", "Card", "Cash on Delivery", "Wallet"])].iterrows():
    fixable = r.payment_mode.strip().lower() in KNOWN
    log("payments", r.payment_id, "payment_mode", f"'{r.payment_mode}' " + ("is a known variant" if fixable else "is not a recognisable payment mode"),
        "Consistency" if fixable else "Validity", "map known variants to one spelling" if fixable else "payment_mode in allowed list",
        "Warning" if fixable else "Critical")
for _, r in p[pd.to_numeric(p.amount_paid, errors="coerce") <= 0].iterrows():
    log("payments", r.payment_id, "amount_paid", f"amount {r.amount_paid} is not positive", "Accuracy", "amount_paid > 0", "Critical")
pay_ids = set(p.order_id)
delivered = o[o.order_status.str.strip().str.upper().eq("DELIVERED")].drop_duplicates("order_id")
for _, r in delivered[~delivered.order_id.isin(pay_ids)].iterrows():
    log("orders", r.order_id, "order_status", "DELIVERED but no payment record in payments.csv", "Consistency", "delivered orders reconcile to a payment", "Warning")

# ---- customers
for _, r in c[c.duplicated("customer_id", keep="first")].iterrows():
    log("customers", r.customer_id, "customer_id", "duplicate id with differing attributes", "Uniqueness", "one row per customer_id", "Critical")
for _, r in c[~c.email.fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")].iterrows():
    log("customers", r.customer_id, "email", "invalid email format", "Validity", "email regex", "Warning")
for _, r in c[c.phone.fillna("").str.replace(r"\D", "", regex=True).str.len() != 10].iterrows():
    log("customers", r.customer_id, "phone", "phone is not 10 digits", "Validity", "10 digits", "Warning")
for _, r in c[~c.pincode.fillna("").str.fullmatch(r"\d{6}")].iterrows():
    log("customers", r.customer_id, "pincode", f"'{r.pincode}' not 6 digits", "Validity", "6 digits", "Warning")
for _, r in c[~c.loyalty_tier.isin(["Bronze", "Silver", "Gold", "Platinum"])].iterrows():
    log("customers", r.customer_id, "loyalty_tier", f"'{r.loyalty_tier}' inconsistent casing", "Consistency", "title-case tier", "Warning")
for _, r in c[pd.to_datetime(c.signup_date, errors="coerce") > "2026-10-01"].iterrows():
    log("customers", r.customer_id, "signup_date", f"future-dated {r.signup_date}", "Timeliness", "signup_date <= run time", "Critical")
for _, r in c[c.city.isna()].iterrows():
    log("customers", r.customer_id, "city", "null city", "Completeness", "city not null", "Warning")

# ---- products
for _, r in pr[pr.duplicated("product_id", keep="first")].iterrows():
    log("products", r.product_id, "product_id", "duplicate id", "Uniqueness", "one row per product_id", "Critical")
for _, r in pr[pr.category.isna()].iterrows():
    log("products", r.product_id, "category", "null category", "Completeness", "category not null", "Critical")
for _, r in pr[num(pr, "mrp") <= 0].iterrows():
    log("products", r.product_id, "mrp", f"mrp {r.mrp} not positive", "Accuracy", "mrp > 0", "Critical")
for _, r in pr[num(pr, "cost_price") > num(pr, "mrp")].iterrows():
    log("products", r.product_id, "cost_price", f"cost {r.cost_price} > mrp {r.mrp}", "Accuracy", "cost_price <= mrp", "Critical")

# ---- stores
for _, r in s[s.zone.isna()].iterrows():
    log("stores", r.store_id, "zone", "null zone", "Completeness", "zone not null (load as Unknown, flag)", "Warning")
for _, r in s[num(s, "capacity_orders_per_day") <= 0].iterrows():
    log("stores", r.store_id, "capacity_orders_per_day", f"capacity {r.capacity_orders_per_day}", "Accuracy", "capacity > 0", "Warning")

log_df = pd.DataFrame(rows)
log_df.to_csv(OUT / "data_issues_log.csv", index=False)
print(log_df.groupby(["file", "quality_dimension"]).size().unstack(fill_value=0))
print("total issues:", len(log_df))
