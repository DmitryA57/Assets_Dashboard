from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from config import (
    RUSSIA_DAILY_LAST_PRICE_LONG_PATH,
    RUSSIA_SUMMARY_BONDS_PATH,
    RUSSIA_SUMMARY_EQUITIES_PATH,
)
from src.analytics import apply_reference_window
from src.compute_snapshot import build_snapshot
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS
from src.services.russia_common import RussiaMarketState, universe_for_market


RUSSIA_EXPORT_SOURCE = "T-Bank export"


SUMMARY_PATHS = {
    "equities": RUSSIA_SUMMARY_EQUITIES_PATH,
    "bonds": RUSSIA_SUMMARY_BONDS_PATH,
}


def _empty_prices() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICES_COLUMNS)


def _empty_asset_master() -> pd.DataFrame:
    return pd.DataFrame(columns=ASSET_MASTER_COLUMNS)


def _summary_merge_columns(summary: pd.DataFrame) -> list[str]:
    preferred = [
        "requested_ticker",
        "name",
        "uid",
        "figi",
        "class_code",
        "exchange",
        "last_price",
        "close_price",
        "trading_status",
        "last_history_dt",
        "history_rows",
        "stale_flag",
    ]
    return [column for column in preferred if column in summary.columns]


def _load_summary_frame(market_key: str) -> pd.DataFrame:
    path = SUMMARY_PATHS[market_key]
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    if "last_history_dt" in frame.columns:
        frame["last_history_dt"] = pd.to_datetime(frame["last_history_dt"], errors="coerce")
    return frame


def _load_daily_history_frame(market_key: str) -> pd.DataFrame:
    if not RUSSIA_DAILY_LAST_PRICE_LONG_PATH.exists():
        return pd.DataFrame()
    frame = pd.read_csv(RUSSIA_DAILY_LAST_PRICE_LONG_PATH)
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["daily_last_price"] = pd.to_numeric(frame["daily_last_price"], errors="coerce")
    return frame[frame["group"] == market_key].copy()


def _build_asset_master(market_key: str, summary: pd.DataFrame) -> pd.DataFrame:
    universe = universe_for_market(market_key)
    summary_by_ticker = {}
    if not summary.empty and "requested_ticker" in summary.columns:
        summary_by_ticker = summary.set_index("requested_ticker").to_dict(orient="index")

    rows: list[dict[str, object]] = []
    for member in universe:
        summary_row = summary_by_ticker.get(member.ticker, {})
        rows.append(
            {
                "asset_id": member.asset_id,
                "asset_name": summary_row.get("name") or member.display_name,
                "display_name": member.display_name,
                "bbg_ticker": member.ticker,
                "source_field": "daily_last_price",
                "source": RUSSIA_EXPORT_SOURCE,
                "asset_class": member.asset_class,
                "sub_asset_class": member.sub_asset_class,
                "country": "Russia",
                "region": "Russia",
                "dm_em_flag": "EM",
                "commodity_category": "",
                "sector_name": member.sector_name,
                "series_type": member.series_type,
                "return_variant": member.return_variant,
                "currency": summary_row.get("currency") or member.currency or "RUB",
                "unit": member.unit,
                "is_active": True,
                "notes": "Loaded from exported T-Bank MOEX daily CSV files.",
            }
        )
    return pd.DataFrame(rows, columns=ASSET_MASTER_COLUMNS)


def _build_prices(market_key: str, history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return _empty_prices()
    universe = universe_for_market(market_key)
    asset_id_by_ticker = {member.ticker: member.asset_id for member in universe}

    prices = history.copy()
    prices["asset_id"] = prices["requested_ticker"].map(asset_id_by_ticker)
    prices["value"] = prices["daily_last_price"]
    prices["source_timestamp"] = prices["date"]
    prices = prices.dropna(subset=["asset_id", "date", "value"])
    prices = prices[["date", "asset_id", "value", "source_timestamp"]].copy()
    return prices.sort_values(["asset_id", "date", "source_timestamp"]).reset_index(drop=True)


def _enrich_snapshot(
    market_key: str,
    asset_master: pd.DataFrame,
    snapshot: pd.DataFrame,
    summary: pd.DataFrame,
    prices: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], str]:
    universe = universe_for_market(market_key)
    base = asset_master.copy()
    summary = summary.copy()
    if summary.empty:
        summary = pd.DataFrame(columns=["requested_ticker"])

    base["ticker"] = base["bbg_ticker"]
    base["market_price"] = pd.NA
    base["trading_status"] = ""
    base["last_update_date"] = pd.NaT
    base["maturity_date"] = pd.NaT
    base["uid"] = pd.NA
    base["figi"] = pd.NA
    base["class_code"] = pd.NA
    base["exchange"] = pd.NA
    base["history_rows_export"] = pd.NA
    base["stale_flag_export"] = pd.NA

    if not summary.empty and "requested_ticker" in summary.columns:
        merge_columns = _summary_merge_columns(summary)
        summary_subset = summary[merge_columns].copy()
        summary_subset = summary_subset.rename(
            columns={
                "requested_ticker": "ticker",
                "name": "summary_name",
                "uid": "summary_uid",
                "figi": "summary_figi",
                "class_code": "summary_class_code",
                "exchange": "summary_exchange",
                "last_price": "summary_last_price",
                "close_price": "summary_close_price",
                "trading_status": "summary_trading_status",
                "last_history_dt": "summary_last_history_dt",
                "history_rows": "summary_history_rows",
                "stale_flag": "summary_stale_flag",
            }
        )
        base = base.merge(summary_subset, on="ticker", how="left")

        if "summary_name" in base.columns:
            base["asset_name"] = base["summary_name"].fillna(base["asset_name"])
        if "summary_uid" in base.columns:
            base["uid"] = base["summary_uid"]
        if "summary_figi" in base.columns:
            base["figi"] = base["summary_figi"]
        if "summary_class_code" in base.columns:
            base["class_code"] = base["summary_class_code"]
        if "summary_exchange" in base.columns:
            base["exchange"] = base["summary_exchange"]
        if "summary_last_price" in base.columns or "summary_close_price" in base.columns:
            last_price = base["summary_last_price"] if "summary_last_price" in base.columns else pd.Series(pd.NA, index=base.index)
            close_price = base["summary_close_price"] if "summary_close_price" in base.columns else pd.Series(pd.NA, index=base.index)
            base["market_price"] = last_price.fillna(close_price)
        if "summary_trading_status" in base.columns:
            base["trading_status"] = base["summary_trading_status"].fillna("")
        if "summary_last_history_dt" in base.columns:
            base["last_update_date"] = pd.to_datetime(base["summary_last_history_dt"], errors="coerce")
        if "summary_history_rows" in base.columns:
            base["history_rows_export"] = pd.to_numeric(base["summary_history_rows"], errors="coerce")
        if "summary_stale_flag" in base.columns:
            base["stale_flag_export"] = base["summary_stale_flag"]

        redundant_columns = [
            "summary_name",
            "summary_uid",
            "summary_figi",
            "summary_class_code",
            "summary_exchange",
            "summary_last_price",
            "summary_close_price",
            "summary_trading_status",
            "summary_last_history_dt",
            "summary_history_rows",
            "summary_stale_flag",
        ]
        base = base.drop(columns=[column for column in redundant_columns if column in base.columns])

    snapshot_date = prices["date"].max() if not prices.empty else pd.NaT
    full = base.merge(
        snapshot[
            [
                "snapshot_date",
                "data_as_of",
                "lag_days",
                "freshness_status",
                "comparison_eligible",
                "asset_id",
                "latest_value",
                "ytd",
                "since_event",
                "yoy",
                "ytd_bps",
                "since_event_bps",
                "yoy_bps",
                "base_ytd",
                "base_event",
                "base_yoy",
            ]
        ] if not snapshot.empty else pd.DataFrame(columns=[
            "snapshot_date",
            "data_as_of",
            "lag_days",
            "freshness_status",
            "comparison_eligible",
            "asset_id",
            "latest_value",
            "ytd",
            "since_event",
            "yoy",
            "ytd_bps",
            "since_event_bps",
            "yoy_bps",
            "base_ytd",
            "base_event",
            "base_yoy",
        ]),
        on="asset_id",
        how="left",
    )

    missing_mask = full["data_as_of"].isna()
    if pd.notna(snapshot_date):
        full.loc[missing_mask, "snapshot_date"] = snapshot_date
    full.loc[missing_mask, "freshness_status"] = "Missing"
    full.loc[missing_mask, "comparison_eligible"] = False
    full["market_price"] = full["market_price"].fillna(full["latest_value"])
    full["trading_status"] = full["trading_status"].fillna("")

    warnings: list[str] = []
    available_tickers = set(summary["requested_ticker"].dropna().astype(str)) if "requested_ticker" in summary.columns else set()
    missing_summary = [member.ticker for member in universe if member.ticker not in available_tickers]
    missing_history = []
    if "history_rows_export" in full.columns:
        missing_history = full.loc[pd.to_numeric(full["history_rows_export"], errors="coerce").fillna(0).eq(0), "ticker"].dropna().astype(str).tolist()

    if missing_summary:
        warnings.append("No summary export row for: " + ", ".join(sorted(missing_summary)))
    if missing_history:
        warnings.append("No daily history in export for: " + ", ".join(sorted(set(missing_history))))

    schedule_status = ""
    if pd.notna(snapshot_date):
        schedule_status = f"Export snapshot date: {pd.Timestamp(snapshot_date).date()}"

    return full, warnings, schedule_status


def load_russia_export_market_state(market_key: str, reference_date: pd.Timestamp) -> RussiaMarketState:
    summary = _load_summary_frame(market_key)
    history = _load_daily_history_frame(market_key)
    asset_master = _build_asset_master(market_key, summary)
    prices = _build_prices(market_key, history)

    snapshot = build_snapshot(
        asset_master=asset_master,
        prices=prices,
        events=pd.DataFrame(columns=["event_id", "event_name", "event_date", "description", "is_active"]),
    )
    if not snapshot.empty:
        snapshot = snapshot.merge(
            asset_master[["asset_id", "series_type"]].drop_duplicates(subset=["asset_id"]),
            on="asset_id",
            how="left",
        )
        snapshot = apply_reference_window(snapshot, prices, reference_date)

    enriched_snapshot, warnings, schedule_status = _enrich_snapshot(
        market_key=market_key,
        asset_master=asset_master,
        snapshot=snapshot,
        summary=summary,
        prices=prices,
    )

    comparison_snapshot = enriched_snapshot.loc[enriched_snapshot["comparison_eligible"].eq(True)].copy()
    return RussiaMarketState(
        snapshot=enriched_snapshot,
        comparison_snapshot=comparison_snapshot,
        prices=prices,
        asset_master=asset_master,
        warnings=warnings,
        schedule_status=schedule_status,
        data_source=RUSSIA_EXPORT_SOURCE,
    )
