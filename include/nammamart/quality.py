"""Data quality rules for the ETL pipeline (pandas).

Every check is written as a condition that must hold. A row fails a check when the
condition is false or cannot be evaluated (null). Critical failures quarantine the row;
Warning failures are logged in dq_results and the row still loads.
The ELT pipeline implements the same rules in include/sql/elt/.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import pandas as pd

from nammamart.config import (LOYALTY_TIERS, ORDER_STATUSES, PAYMENT_MODES, PAYMENT_STATUSES, ZONES)

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"


@dataclass
class Check:
    name: str
    dimension: str
    severity: str  # "Critical" or "Warning"
    holds: Callable[[pd.DataFrame, dict], pd.Series]


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=TS_FORMAT, errors="coerce")


def dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=DATE_FORMAT, errors="coerce")


def upper(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.upper()


def canon_payment_mode(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.lower().map(PAYMENT_MODES)


def canon_tier(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.title()


def _ok(series: pd.Series) -> pd.Series:
    """Nulls count as 'condition not met'."""
    return series.fillna(False).astype(bool)


def _dup_in_file(df: pd.DataFrame, key: str) -> pd.Series:
    cols = [c for c in ("_source_file", key) if c in df.columns]
    return df.duplicated(cols, keep="first")


# ---------------------------------------------------------------- orders
def order_checks() -> list[Check]:
    amount = lambda d: (num(d.quantity) * num(d.unit_price) * (1 - num(d.discount_pct) / 100)).round(2)
    return [
        Check("order_id_not_null", "Completeness", "Critical", lambda d, c: d.order_id.notna()),
        Check("customer_id_not_null", "Completeness", "Critical", lambda d, c: d.customer_id.notna()),
        Check("order_ts_not_null", "Completeness", "Critical", lambda d, c: d.order_ts.notna()),
        Check("delivered_has_delivery_minutes", "Completeness", "Critical",
              lambda d, c: _ok((upper(d.order_status) != "DELIVERED") | num(d.delivery_minutes).notna())),
        Check("order_id_unique_per_file", "Uniqueness", "Critical", lambda d, c: ~_dup_in_file(d, "order_id")),
        Check("order_status_allowed", "Validity", "Critical", lambda d, c: _ok(upper(d.order_status).isin(ORDER_STATUSES))),
        Check("payment_mode_allowed", "Validity", "Critical", lambda d, c: canon_payment_mode(d.payment_mode).notna()),
        Check("discount_pct_between_0_and_100", "Validity", "Critical",
              lambda d, c: _ok(num(d.discount_pct).between(0, 100))),
        Check("order_ts_parses", "Validity", "Critical", lambda d, c: _ok(d.order_ts.isna() | ts(d.order_ts).notna())),
        Check("channel_allowed", "Validity", "Warning",
              lambda d, c: _ok(d.channel.astype("string").str.strip().isin(["App", "Web"]))),
        Check("quantity_positive", "Accuracy", "Critical", lambda d, c: _ok(num(d.quantity) > 0)),
        Check("unit_price_positive", "Accuracy", "Critical", lambda d, c: _ok(num(d.unit_price) > 0)),
        Check("order_amount_matches_formula", "Accuracy", "Critical",
              lambda d, c: _ok((amount(d) - num(d.order_amount)).abs() <= 0.01)),
        Check("unit_price_within_20pct_of_mrp", "Accuracy", "Warning",
              lambda d, c: _ok(d.product_id.map(c["mrp"]).isna() | ((num(d.unit_price) / d.product_id.map(c["mrp"]) - 1).abs() <= 0.2))),
        Check("unit_price_within_5x_of_mrp", "Accuracy", "Critical",
              lambda d, c: _ok(d.product_id.map(c["mrp"]).isna() | (num(d.unit_price) / d.product_id.map(c["mrp"])).between(0.2, 5))),
        Check("delivery_minutes_below_120", "Accuracy", "Critical",
              lambda d, c: _ok(d.delivery_minutes.isna() | (num(d.delivery_minutes) < 120))),
        Check("customer_in_master", "Consistency", "Critical",
              lambda d, c: _ok(d.customer_id.isna() | d.customer_id.isin(c["customers_all"]))),
        Check("customer_master_row_valid", "Consistency", "Critical",
              lambda d, c: _ok(~d.customer_id.isin(c["customers_all"]) | d.customer_id.isin(c["customers"]))),
        Check("product_in_master", "Consistency", "Critical", lambda d, c: _ok(d.product_id.isin(c["products_all"]))),
        Check("product_master_row_valid", "Consistency", "Critical",
              lambda d, c: _ok(~d.product_id.isin(c["products_all"]) | d.product_id.isin(c["products"]))),
        Check("store_in_master", "Consistency", "Critical", lambda d, c: _ok(d.store_id.isin(c["stores_all"]))),
        Check("store_master_row_valid", "Consistency", "Critical",
              lambda d, c: _ok(~d.store_id.isin(c["stores_all"]) | d.store_id.isin(c["stores"]))),
        Check("order_ts_not_in_future", "Timeliness", "Critical",
              lambda d, c: _ok(ts(d.order_ts).isna() | (ts(d.order_ts) <= c["now"]))),
    ]


# -------------------------------------------------------------- payments
def payment_checks() -> list[Check]:
    def success_matches(d, c):
        order_amt = d.order_id.map(c["order_amounts"])
        diff = (num(d.amount_paid) - num(order_amt)).abs()
        return _ok((upper(d.payment_status) != "SUCCESS") | order_amt.isna() | (diff <= 0.01))

    return [
        Check("payment_id_not_null", "Completeness", "Critical", lambda d, c: d.payment_id.notna()),
        Check("payment_order_id_not_null", "Completeness", "Critical", lambda d, c: d.order_id.notna()),
        Check("gateway_ref_present_for_online", "Completeness", "Warning",
              lambda d, c: _ok((canon_payment_mode(d.payment_mode) == "Cash on Delivery") | d.gateway_ref.notna())),
        Check("payment_id_unique", "Uniqueness", "Critical", lambda d, c: ~_dup_in_file(d, "payment_id")),
        Check("payment_mode_allowed", "Validity", "Critical", lambda d, c: canon_payment_mode(d.payment_mode).notna()),
        Check("payment_status_allowed", "Validity", "Critical",
              lambda d, c: _ok(upper(d.payment_status).isin(PAYMENT_STATUSES))),
        Check("payment_ts_parses", "Validity", "Critical", lambda d, c: _ok(ts(d.payment_ts).notna())),
        Check("amount_paid_positive", "Accuracy", "Critical", lambda d, c: _ok(num(d.amount_paid) > 0)),
        Check("payment_order_exists", "Consistency", "Critical", lambda d, c: _ok(d.order_id.isin(c["order_ids"]))),
        Check("success_amount_equals_order_amount", "Consistency", "Critical", success_matches),
        Check("payment_ts_not_in_future", "Timeliness", "Critical",
              lambda d, c: _ok(ts(d.payment_ts).isna() | (ts(d.payment_ts) <= c["now"]))),
    ]


# ------------------------------------------------------------- customers
def customer_checks() -> list[Check]:
    digits = lambda s: s.astype("string").str.replace(r"\D", "", regex=True)
    return [
        Check("customer_id_not_null", "Completeness", "Critical", lambda d, c: d.customer_id.notna()),
        Check("city_not_null", "Completeness", "Warning", lambda d, c: d.city.notna()),
        Check("customer_id_unique", "Uniqueness", "Critical", lambda d, c: ~_dup_in_file(d, "customer_id")),
        Check("loyalty_tier_valid", "Validity", "Critical", lambda d, c: _ok(canon_tier(d.loyalty_tier).isin(LOYALTY_TIERS))),
        Check("signup_date_parses", "Validity", "Critical", lambda d, c: _ok(dt(d.signup_date).notna())),
        Check("email_format", "Validity", "Warning",
              lambda d, c: _ok(d.email.astype("string").str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))),
        Check("phone_is_10_digits", "Validity", "Warning", lambda d, c: _ok(digits(d.phone).str.len() == 10)),
        Check("pincode_is_6_digits", "Validity", "Warning",
              lambda d, c: _ok(d.pincode.astype("string").str.fullmatch(r"\d{6}"))),
        Check("is_active_allowed", "Validity", "Warning", lambda d, c: _ok(d.is_active.isin(["Y", "N"]))),
        Check("loyalty_tier_casing", "Consistency", "Warning",
              lambda d, c: _ok(d.loyalty_tier.astype("string") == canon_tier(d.loyalty_tier))),
        Check("signup_date_not_in_future", "Timeliness", "Critical",
              lambda d, c: _ok(dt(d.signup_date).isna() | (dt(d.signup_date) <= c["now"]))),
    ]


# -------------------------------------------------------------- products
def product_checks() -> list[Check]:
    return [
        Check("product_id_not_null", "Completeness", "Critical", lambda d, c: d.product_id.notna()),
        Check("category_not_null", "Completeness", "Critical", lambda d, c: d.category.notna()),
        Check("product_id_unique", "Uniqueness", "Critical", lambda d, c: ~_dup_in_file(d, "product_id")),
        Check("launch_date_parses", "Validity", "Critical", lambda d, c: _ok(dt(d.launch_date).notna())),
        Check("is_perishable_allowed", "Validity", "Warning",
              lambda d, c: _ok(d.is_perishable.isin(["True", "False"]))),
        Check("unit_allowed", "Validity", "Warning", lambda d, c: _ok(d.unit.isin(["pack", "unit", "kg"]))),
        Check("mrp_positive", "Accuracy", "Critical", lambda d, c: _ok(num(d.mrp) > 0)),
        Check("cost_price_positive", "Accuracy", "Critical", lambda d, c: _ok(num(d.cost_price) > 0)),
        Check("cost_price_not_above_mrp", "Consistency", "Critical",
              lambda d, c: _ok(num(d.cost_price) <= num(d.mrp))),
        Check("launch_date_not_in_future", "Timeliness", "Critical",
              lambda d, c: _ok(dt(d.launch_date).isna() | (dt(d.launch_date) <= c["now"]))),
    ]


# ---------------------------------------------------------------- stores
def store_checks() -> list[Check]:
    return [
        Check("store_id_not_null", "Completeness", "Critical", lambda d, c: d.store_id.notna()),
        Check("zone_not_null", "Completeness", "Warning", lambda d, c: d.zone.notna()),
        Check("store_id_unique", "Uniqueness", "Critical", lambda d, c: ~_dup_in_file(d, "store_id")),
        Check("zone_allowed", "Validity", "Critical", lambda d, c: _ok(d.zone.isna() | d.zone.isin(ZONES))),
        Check("open_date_parses", "Validity", "Critical", lambda d, c: _ok(dt(d.open_date).notna())),
        Check("capacity_positive", "Accuracy", "Warning", lambda d, c: _ok(num(d.capacity_orders_per_day) > 0)),
        Check("open_date_not_in_future", "Timeliness", "Critical",
              lambda d, c: _ok(dt(d.open_date).isna() | (dt(d.open_date) <= c["now"]))),
    ]


CHECKS = {"orders": order_checks, "payments": payment_checks, "customers": customer_checks,
          "products": product_checks, "stores": store_checks}


def run_checks(df: pd.DataFrame, file_key: str, ctx: dict, run_id: str, dag_id: str):
    """Returns (passed_df, failed_df, dq_rows). failed_df carries a failure_reason column."""
    now = ctx["now"]
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    results = []
    for chk in CHECKS[file_key]():
        holds = chk.holds(df, ctx).reindex(df.index).fillna(False).astype(bool)
        failed = ~holds
        n_failed = int(failed.sum())
        if chk.severity == "Critical" and n_failed:
            for i in df.index[failed]:
                reasons.at[i] = reasons.at[i] + [f"{chk.name}"]
        results.append(dict(run_id=run_id, dag_id=dag_id, check_name=f"{file_key}.{chk.name}",
                            dimension=chk.dimension, severity=chk.severity, rows_checked=len(df),
                            rows_failed=n_failed,
                            status="PASS" if n_failed == 0 else ("FAIL" if chk.severity == "Critical" else "WARN"),
                            checked_at=now))
    has_reason = reasons.map(len) > 0
    failed_df = df[has_reason].copy()
    failed_df["failure_reason"] = reasons[has_reason].map("; ".join)
    return df[~has_reason].copy(), failed_df, results


def dataset_check(name: str, dimension: str, severity: str, failed: bool, run_id: str, dag_id: str,
                  now: datetime) -> dict:
    return dict(run_id=run_id, dag_id=dag_id, check_name=name, dimension=dimension, severity=severity,
                rows_checked=1, rows_failed=int(failed),
                status="PASS" if not failed else ("FAIL" if severity == "Critical" else "WARN"), checked_at=now)
