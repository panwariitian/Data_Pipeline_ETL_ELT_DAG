"""PII masking used by the ETL pipeline (the ELT pipeline does the same in SQL)."""
import re

import pandas as pd


def mask_email(value) -> str | None:
    if pd.isna(value):
        return None
    value = str(value).strip()
    if "@" not in value:
        return "***"
    local, domain = value.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str | None:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 10:
        return "*" * 10
    return f"{digits[:2]}******{digits[-2:]}"


def mask_name(value) -> str | None:
    if pd.isna(value):
        return None
    value = str(value).strip()
    return f"{value[:1]}***" if value else None
