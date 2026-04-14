from __future__ import annotations

import argparse
from dataclasses import dataclass

import pandas as pd

from config import (
    AGED_MAX_LAG_DAYS,
    COMPARISON_INCLUDED_FRESHNESS,
    DEFAULT_EVENT_DATE,
    DEFAULT_EVENT_ID,
    FRESH_MAX_LAG_DAYS,
    SNAPSHOT_PATH,
)
from src.constants import SERIES_TYPE_BPS, SERIES_TYPE_PERCENT, SNAPSHOT_COLUMNS


@dataclass(slots=True)
class BasePoints:
    ytd: float | None
    event: float | None
    yoy: float | None


def compute_percent_change(latest_value: float | None, base_value: float | None) -> float | None:
    if latest_value in (None, 0) and latest_value != 0:
        return None
    if base_value in (None, 0):
        return None
    return round((float(latest_value) / float(base_value)) - 1.0, 10)


def compute_bps_change(latest_value: float | None, base_value: float | None) -> float | None:
    if latest_value is None or base_value is None:
        return None
    return round((float(latest_value) - float(base_value)) * 100.0, 10)


def _resolve_event_date(events: pd.DataFrame) -> pd.Timestamp:
    if events.empty:
        return pd.Timestamp(DEFAULT_EVENT_DATE)

    active_events = events.copy()
    if "is_active" in active_events.columns:
        active_events = active_events[active_events["is_active"].astype(str).str.lower().isin({"true", "1", "yes"})]

    match = active_events.loc[active_events["event_id"] == DEFAULT_EVENT_ID, "event_date"]
    if not match.empty and pd.notna(match.iloc[0]):
        return pd.Timestamp(match.iloc[0])
    return pd.Timestamp(DEFAULT_EVENT_DATE)


def _select_value_on_or_before(series_frame: pd.DataFrame, target_date: pd.Timestamp) -> float | None:
    eligible = series_frame.loc[series_frame["date"] <= target_date].sort_values("date")
    if eligible.empty:
        return None
    return float(eligible.iloc[-1]["value"])


def _latest_value(series_frame: pd.DataFrame) -> tuple[pd.Timestamp | None, float | None]:
    ordered = series_frame.sort_values(["date", "source_timestamp"], na_position="last")
    if ordered.empty:
        return None, None
    row = ordered.iloc[-1]
    return pd.Timestamp(row["date"]), float(row["value"])


def _base_points(series_frame: pd.DataFrame, latest_date: pd.Timestamp, event_date: pd.Timestamp) -> BasePoints:
    prev_year_end = pd.Timestamp(year=latest_date.year - 1, month=12, day=31)
    yoy_target = latest_date - pd.DateOffset(years=1)
    return BasePoints(
        ytd=_select_value_on_or_before(series_frame, prev_year_end),
        event=_select_value_on_or_before(series_frame, event_date),
        yoy=_select_value_on_or_before(series_frame, yoy_target),
    )


def classify_freshness(lag_days: int | None) -> str | None:
    if lag_days is None:
        return None
    if lag_days <= FRESH_MAX_LAG_DAYS:
        return "Fresh"
    if lag_days <= AGED_MAX_LAG_DAYS:
        return "Aged"
    return "Stale"


def build_snapshot(
    asset_master: pd.DataFrame,
    prices: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    if asset_master.empty or prices.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    event_date = _resolve_event_date(events)
    series_types = asset_master[["asset_id", "series_type"]].drop_duplicates()
    merged = prices.merge(series_types, on="asset_id", how="inner")
    snapshot_date = pd.to_datetime(merged["date"], errors="coerce").max()
    if pd.isna(snapshot_date):
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    rows: list[dict[str, object]] = []
    for asset_id, asset_prices in merged.groupby("asset_id", dropna=False):
        clean_prices = asset_prices.dropna(subset=["date", "value"]).copy()
        if clean_prices.empty:
            continue

        clean_prices = clean_prices.sort_values(["date", "source_timestamp"], na_position="last")
        latest_date, latest_value = _latest_value(clean_prices)
        if latest_date is None or latest_value is None:
            continue

        base_points = _base_points(clean_prices, latest_date=latest_date, event_date=event_date)
        series_type = clean_prices["series_type"].dropna().iloc[0] if clean_prices["series_type"].notna().any() else None
        lag_days = int((snapshot_date.normalize() - latest_date.normalize()).days)
        freshness_status = classify_freshness(lag_days)

        row = {
            "snapshot_date": snapshot_date,
            "data_as_of": latest_date,
            "lag_days": lag_days,
            "freshness_status": freshness_status,
            "comparison_eligible": freshness_status in COMPARISON_INCLUDED_FRESHNESS,
            "asset_id": asset_id,
            "latest_value": latest_value,
            "ytd": None,
            "since_event": None,
            "yoy": None,
            "ytd_bps": None,
            "since_event_bps": None,
            "yoy_bps": None,
            "base_ytd": base_points.ytd,
            "base_event": base_points.event,
            "base_yoy": base_points.yoy,
        }

        if series_type in SERIES_TYPE_PERCENT:
            row["ytd"] = compute_percent_change(latest_value, base_points.ytd)
            row["since_event"] = compute_percent_change(latest_value, base_points.event)
            row["yoy"] = compute_percent_change(latest_value, base_points.yoy)
        elif series_type in SERIES_TYPE_BPS:
            row["ytd_bps"] = compute_bps_change(latest_value, base_points.ytd)
            row["since_event_bps"] = compute_bps_change(latest_value, base_points.event)
            row["yoy_bps"] = compute_bps_change(latest_value, base_points.yoy)

        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    return pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)


def write_snapshot(
    asset_master: pd.DataFrame,
    prices: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    snapshot = build_snapshot(asset_master=asset_master, prices=prices, events=events)
    snapshot.to_parquet(SNAPSHOT_PATH, index=False)
    return snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute snapshot.parquet from project metadata and prices.")
    parser.add_argument(
        "--write-empty",
        action="store_true",
        help="Write an empty snapshot file even when there is no asset metadata or price history.",
    )
    return parser


def main() -> None:
    from src.load_data import load_asset_master, load_events, load_prices

    parser = build_parser()
    args = parser.parse_args()

    asset_master = load_asset_master()
    prices = load_prices()
    events = load_events()
    snapshot = build_snapshot(asset_master=asset_master, prices=prices, events=events)

    if snapshot.empty and not args.write_empty:
        print("Snapshot is empty. Nothing written. Use --write-empty to persist an empty snapshot file.")
        return

    snapshot.to_parquet(SNAPSHOT_PATH, index=False)
    print(f"Wrote {len(snapshot)} rows to {SNAPSHOT_PATH}")


if __name__ == "__main__":
    main()
