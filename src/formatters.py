from __future__ import annotations

import math

import pandas as pd

from config import DATE_DISPLAY_FORMAT, PERCENT_DECIMALS


def is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or pd.isna(value)


def format_percent(value: float | None) -> str:
    if is_missing(value):
        return "-"
    return f"{float(value):+.{PERCENT_DECIMALS}%}"


def format_bps(value: float | None) -> str:
    if is_missing(value):
        return "-"
    return f"{float(value):+.0f} bps"


def format_level(value: float | None) -> str:
    if is_missing(value):
        return "-"

    number = float(value)
    absolute = abs(number)
    if absolute >= 1000:
        return f"{number:,.0f}"
    if absolute >= 100:
        return f"{number:,.1f}"
    return f"{number:,.2f}"


def format_date(value: object) -> str:
    if is_missing(value):
        return "-"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return "-"
    return parsed.strftime(DATE_DISPLAY_FORMAT)


def format_freshness(value: object) -> str:
    if is_missing(value):
        return "-"
    return str(value)
