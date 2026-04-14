from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config import (
    ASSET_MASTER_PATH,
    COMMODITY_CATEGORY_DISPLAY_LABELS,
    EVENTS_PATH,
    PRICES_PATH,
    SERIES_DISPLAY_ALIASES,
    SNAPSHOT_PATH,
)
from src.compute_snapshot import build_snapshot
from src.constants import (
    ASSET_MASTER_COLUMNS,
    EVENTS_COLUMNS,
    PRICES_COLUMNS,
    SNAPSHOT_COLUMNS,
)


@dataclass(slots=True)
class DashboardBundle:
    asset_master: pd.DataFrame
    events: pd.DataFrame
    prices: pd.DataFrame
    snapshot: pd.DataFrame


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return _empty_frame(columns)

    frame = pd.read_csv(path)
    missing_columns = [column for column in columns if column not in frame.columns]
    for column in missing_columns:
        frame[column] = pd.NA
    return frame[columns]


def _load_parquet(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return _empty_frame(columns)

    frame = pd.read_parquet(path)
    missing_columns = [column for column in columns if column not in frame.columns]
    for column in missing_columns:
        frame[column] = pd.NA
    return frame[columns]


def load_asset_master() -> pd.DataFrame:
    asset_master = _load_csv(ASSET_MASTER_PATH, ASSET_MASTER_COLUMNS)
    return _apply_display_labels(asset_master)


def load_events() -> pd.DataFrame:
    events = _load_csv(EVENTS_PATH, EVENTS_COLUMNS)
    if not events.empty and "event_date" in events.columns:
        events["event_date"] = pd.to_datetime(events["event_date"], errors="coerce")
    return events


def load_prices() -> pd.DataFrame:
    prices = _load_parquet(PRICES_PATH, PRICES_COLUMNS)
    if prices.empty:
        return prices

    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["source_timestamp"] = pd.to_datetime(prices["source_timestamp"], errors="coerce")
    prices["value"] = pd.to_numeric(prices["value"], errors="coerce")
    return prices


def _apply_display_labels(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    styled = frame.copy()
    if "display_name" in styled.columns:
        styled["display_name"] = styled["display_name"].replace(SERIES_DISPLAY_ALIASES)
    if "commodity_category" in styled.columns:
        styled["commodity_category"] = styled["commodity_category"].replace(COMMODITY_CATEGORY_DISPLAY_LABELS)
    return styled


def _attach_asset_metadata(snapshot: pd.DataFrame, asset_master: pd.DataFrame) -> pd.DataFrame:
    if snapshot.empty or asset_master.empty or "asset_id" not in snapshot.columns or "asset_id" not in asset_master.columns:
        return snapshot

    metadata = asset_master.drop_duplicates(subset=["asset_id"]).copy()
    extra_columns = [column for column in metadata.columns if column not in snapshot.columns or column == "asset_id"]
    return snapshot.merge(metadata[extra_columns], on="asset_id", how="left")


def load_snapshot(asset_master: pd.DataFrame, prices: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    persisted = _load_parquet(SNAPSHOT_PATH, SNAPSHOT_COLUMNS)
    if not persisted.empty:
        persisted = _attach_asset_metadata(persisted, asset_master)
    else:
        computed = build_snapshot(asset_master=asset_master, prices=prices, events=events)
        persisted = _attach_asset_metadata(computed, asset_master)

    for column in ["snapshot_date", "data_as_of"]:
        if column in persisted.columns:
            persisted[column] = pd.to_datetime(persisted[column], errors="coerce")
    if "lag_days" in persisted.columns:
        persisted["lag_days"] = pd.to_numeric(persisted["lag_days"], errors="coerce")
    if "comparison_eligible" in persisted.columns:
        persisted["comparison_eligible"] = persisted["comparison_eligible"].fillna(False).astype(bool)

    return persisted


def load_dashboard_bundle() -> DashboardBundle:
    asset_master = load_asset_master()
    events = load_events()
    prices = load_prices()
    snapshot = load_snapshot(asset_master=asset_master, prices=prices, events=events)
    return DashboardBundle(
        asset_master=asset_master,
        events=events,
        prices=prices,
        snapshot=snapshot,
    )
