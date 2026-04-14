from __future__ import annotations

import pandas as pd

from config import AGED_MAX_LAG_DAYS
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS


def validate_asset_master(asset_master: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    missing_columns = [column for column in ASSET_MASTER_COLUMNS if column not in asset_master.columns]
    if missing_columns:
        issues.append(f"Missing asset_master columns: {', '.join(missing_columns)}")

    if "asset_id" in asset_master.columns and asset_master["asset_id"].duplicated().any():
        issues.append("asset_master contains duplicate asset_id values.")

    return issues


def validate_prices(prices: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    missing_columns = [column for column in PRICES_COLUMNS if column not in prices.columns]
    if missing_columns:
        issues.append(f"Missing prices columns: {', '.join(missing_columns)}")
        return issues

    if prices.empty:
        return issues

    duplicated = prices.duplicated(subset=["asset_id", "date"], keep=False)
    if duplicated.any():
        issues.append("prices contains duplicate asset_id/date observations.")

    return issues


def collect_validation_issues(asset_master: pd.DataFrame, prices: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    issues.extend(validate_asset_master(asset_master))
    issues.extend(validate_prices(prices))

    if asset_master.empty:
        issues.append("asset_master is empty.")
    if prices.empty:
        issues.append("prices.parquet is empty or missing.")

    return issues


def validate_snapshot_freshness(snapshot: pd.DataFrame, max_staleness_days: int = AGED_MAX_LAG_DAYS) -> list[str]:
    issues: list[str] = []
    if snapshot.empty:
        return issues

    if "lag_days" in snapshot.columns and "data_as_of" in snapshot.columns:
        stale = snapshot.loc[pd.to_numeric(snapshot["lag_days"], errors="coerce") > max_staleness_days, ["asset_id", "data_as_of"]].copy()
        if stale.empty:
            return issues

        stale["data_as_of"] = pd.to_datetime(stale["data_as_of"], errors="coerce").dt.strftime("%Y-%m-%d")
        sample = ", ".join(
            f"{row.asset_id} ({row.data_as_of})"
            for row in stale.head(5).itertuples(index=False)
        )
        issues.append(
            f"{len(stale)} series are stale with lag greater than {max_staleness_days} days. Example: {sample}"
        )
        return issues

    return issues
