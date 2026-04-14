from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from config import DEFAULT_EVENT_DATE, DEFAULT_EVENT_ID, DEFAULT_EVENT_LABEL
from src.constants import SERIES_TYPE_BPS, SERIES_TYPE_PERCENT


DEFAULT_REFERENCE_DATE = pd.Timestamp(DEFAULT_EVENT_DATE)
DEFAULT_REFERENCE_WINDOW_LABEL = f"{DEFAULT_EVENT_LABEL} ({DEFAULT_REFERENCE_DATE.strftime('%d.%m.%Y')})"


WINDOW_LABELS = {
    "ytd": "YTD",
    "since_event": DEFAULT_REFERENCE_WINDOW_LABEL,
    "yoy": "YoY",
    "ytd_bps": "YTD",
    "since_event_bps": DEFAULT_REFERENCE_WINDOW_LABEL,
    "yoy_bps": "YoY",
}

WINDOW_COLUMNS = ["ytd", "since_event", "yoy"]
BPS_COLUMNS = ["ytd_bps", "since_event_bps", "yoy_bps"]

USER_TABLE_COLUMNS = [
    "display_name",
    "country",
    "region",
    "commodity_category",
    "return_variant",
    "latest_value",
    "ytd",
    "since_event",
    "yoy",
    "data_as_of",
    "freshness_status",
    "lag_days",
]

COMMODITY_CATEGORY_ORDER = [
    "Energy - crude oil & refined products",
    "Energy - gas, LNG, coal",
    "Precious metals",
    "Base metals",
    "Ferrous / Steel chain",
    "Fertilizers",
    "Agriculture",
    "Energy materials",
]


@dataclass(slots=True)
class KpiCard:
    title: str
    value: str
    detail: str


def subset_display_columns(frame: pd.DataFrame, preferred_columns: list[str] | None = None) -> pd.DataFrame:
    columns = preferred_columns or USER_TABLE_COLUMNS
    available = [column for column in columns if column in frame.columns]
    return frame[available].copy() if available else frame.copy()


def filter_asset_class(frame: pd.DataFrame, asset_class: str) -> pd.DataFrame:
    if frame.empty or "asset_class" not in frame.columns:
        return frame.copy()
    return frame[frame["asset_class"] == asset_class].copy()


def filter_series_types(frame: pd.DataFrame, series_types: set[str]) -> pd.DataFrame:
    if frame.empty or "series_type" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame[frame["series_type"].isin(series_types)].copy()


def comparison_universe(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "comparison_eligible" not in frame.columns:
        return frame.copy()
    return frame[frame["comparison_eligible"].fillna(False)].copy()


def choose_primary_metric(frame: pd.DataFrame, preferred: list[str] | None = None) -> str | None:
    candidates = preferred or WINDOW_COLUMNS + BPS_COLUMNS
    for column in candidates:
        if column in frame.columns and frame[column].notna().any():
            return column
    return None


def resolve_reference_date(events: pd.DataFrame) -> pd.Timestamp:
    if events.empty:
        return DEFAULT_REFERENCE_DATE

    active_events = events.copy()
    if "is_active" in active_events.columns:
        active_events = active_events[active_events["is_active"].astype(str).str.lower().isin({"true", "1", "yes"})]

    match = active_events.loc[active_events["event_id"] == DEFAULT_EVENT_ID, "event_date"]
    if not match.empty and pd.notna(match.iloc[0]):
        return pd.Timestamp(match.iloc[0]).normalize()
    return DEFAULT_REFERENCE_DATE


def format_reference_window_label(
    reference_date: pd.Timestamp | None,
    *,
    default_reference_date: pd.Timestamp | None = None,
) -> str:
    normalized_reference_date = pd.Timestamp(DEFAULT_REFERENCE_DATE if reference_date is None or pd.isna(reference_date) else reference_date).normalize()
    normalized_default_date = pd.Timestamp(
        DEFAULT_REFERENCE_DATE if default_reference_date is None or pd.isna(default_reference_date) else default_reference_date
    ).normalize()
    date_label = normalized_reference_date.strftime("%d.%m.%Y")
    if normalized_reference_date == normalized_default_date:
        return f"{DEFAULT_EVENT_LABEL} ({date_label})"
    return f"Since {date_label}"


def metric_label(metric: str, reference_window_label: str | None = None) -> str:
    if metric in {"since_event", "since_event_bps"} and reference_window_label:
        return reference_window_label
    return WINDOW_LABELS.get(metric, metric)


def apply_reference_window(frame: pd.DataFrame, prices: pd.DataFrame, reference_date: pd.Timestamp | None) -> pd.DataFrame:
    updated = frame.copy()
    if updated.empty:
        return updated

    if "since_event" in updated.columns:
        updated["since_event"] = pd.NA
    if "since_event_bps" in updated.columns:
        updated["since_event_bps"] = pd.NA
    if "base_event" in updated.columns:
        updated["base_event"] = pd.NA

    required_columns = {"asset_id", "latest_value", "series_type"}
    if not required_columns.issubset(updated.columns) or prices.empty or reference_date is None:
        return updated

    normalized_reference_date = pd.Timestamp(reference_date).normalize()
    price_columns = [column for column in ["asset_id", "date", "value", "source_timestamp"] if column in prices.columns]
    price_frame = prices[price_columns].copy()
    price_frame["date"] = pd.to_datetime(price_frame["date"], errors="coerce")
    if "source_timestamp" in price_frame.columns:
        price_frame["source_timestamp"] = pd.to_datetime(price_frame["source_timestamp"], errors="coerce")

    relevant_asset_ids = updated["asset_id"].dropna().unique().tolist()
    eligible = price_frame[
        price_frame["asset_id"].isin(relevant_asset_ids)
        & price_frame["date"].notna()
        & (price_frame["date"] <= normalized_reference_date)
    ]
    if eligible.empty:
        return updated

    sort_columns = [column for column in ["asset_id", "date", "source_timestamp"] if column in eligible.columns]
    base_frame = (
        eligible.sort_values(sort_columns, na_position="last")
        .groupby("asset_id", dropna=False)
        .tail(1)[["asset_id", "value"]]
        .rename(columns={"value": "reference_base"})
    )
    updated = updated.merge(base_frame, on="asset_id", how="left")

    latest_values = pd.to_numeric(updated["latest_value"], errors="coerce")
    reference_bases = pd.to_numeric(updated["reference_base"], errors="coerce")
    series_type = updated["series_type"].fillna("")

    percent_mask = series_type.isin(SERIES_TYPE_PERCENT) & latest_values.notna() & reference_bases.notna() & (reference_bases != 0)
    bps_mask = series_type.isin(SERIES_TYPE_BPS) & latest_values.notna() & reference_bases.notna()

    if "since_event" in updated.columns:
        updated.loc[percent_mask, "since_event"] = ((latest_values[percent_mask] / reference_bases[percent_mask]) - 1.0).round(10)
    if "since_event_bps" in updated.columns:
        updated.loc[bps_mask, "since_event_bps"] = ((latest_values[bps_mask] - reference_bases[bps_mask]) * 100.0).round(10)
    if "base_event" in updated.columns:
        updated["base_event"] = reference_bases

    return updated.drop(columns=["reference_base"])


def top_bottom(frame: pd.DataFrame, metric: str, n: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty or metric not in frame.columns:
        empty = frame.iloc[0:0].copy()
        return empty, empty

    ranked = frame.dropna(subset=[metric]).sort_values(metric, ascending=False)
    top = ranked.head(n)
    bottom = ranked.tail(n).sort_values(metric, ascending=True)
    return top, bottom


def aggregate_metric(frame: pd.DataFrame, group_by: str, metric: str) -> pd.DataFrame:
    if frame.empty or metric not in frame.columns or group_by not in frame.columns:
        return pd.DataFrame(columns=[group_by, metric])

    aggregated = (
        frame.dropna(subset=[metric, group_by])
        .groupby(group_by, dropna=True)[metric]
        .mean()
        .reset_index()
        .sort_values(metric, ascending=False)
    )
    return aggregated


def build_heatmap_matrix(frame: pd.DataFrame, metric: str, group_by: str = "asset_class") -> pd.DataFrame:
    if frame.empty or metric not in frame.columns or group_by not in frame.columns:
        return pd.DataFrame()

    return (
        frame.dropna(subset=[metric, group_by])
        .groupby(group_by, dropna=True)[metric]
        .mean()
        .to_frame()
        .T
    )


def build_multi_metric_heatmap(frame: pd.DataFrame, group_by: str, metrics: list[str]) -> pd.DataFrame:
    usable_metrics = [metric for metric in metrics if metric in frame.columns]
    if frame.empty or group_by not in frame.columns or not usable_metrics:
        return pd.DataFrame()

    heatmap = (
        frame.dropna(subset=[group_by])
        .groupby(group_by, dropna=True)[usable_metrics]
        .mean()
        .reindex(columns=usable_metrics)
    )
    return heatmap


def attach_price_metadata(prices: pd.DataFrame, asset_master: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices.copy()

    metadata_columns = [
        column
        for column in [
            "asset_id",
            "display_name",
            "asset_class",
            "return_variant",
            "country",
            "region",
            "commodity_category",
            "series_type",
        ]
        if column in asset_master.columns
    ]
    if "asset_id" not in metadata_columns:
        return prices.copy()

    metadata = asset_master[metadata_columns].drop_duplicates(subset=["asset_id"])
    return prices.merge(metadata, on="asset_id", how="left")


def resolve_start_reference(prices: pd.DataFrame, mode: str, latest_date: pd.Timestamp, event_date: pd.Timestamp | None) -> pd.Timestamp:
    if mode == "Year Start":
        return pd.Timestamp(year=latest_date.year, month=1, day=1)
    if mode in {"Event Date", "Reference Date"} and event_date is not None:
        return event_date.normalize()
    if mode == "1Y Ago":
        return (latest_date - pd.DateOffset(years=1)).normalize()
    return pd.to_datetime(prices["date"], errors="coerce").min()


def normalized_history(
    prices: pd.DataFrame,
    asset_master: pd.DataFrame,
    asset_ids: list[str],
    *,
    start_mode: str,
    event_date: pd.Timestamp | None,
) -> pd.DataFrame:
    if prices.empty or not asset_ids:
        return pd.DataFrame(columns=["date", "display_name", "normalized"])

    enriched = attach_price_metadata(prices, asset_master)
    frame = enriched[enriched["asset_id"].isin(asset_ids)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["date", "display_name", "normalized"])

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    latest_date = frame["date"].max()
    if pd.isna(latest_date):
        return pd.DataFrame(columns=["date", "display_name", "normalized"])

    start_reference = resolve_start_reference(frame, start_mode, latest_date, event_date)
    rows: list[pd.DataFrame] = []
    for asset_id, series in frame.groupby("asset_id", dropna=False):
        series = series.sort_values("date")
        eligible = series[series["date"] >= start_reference]
        if eligible.empty:
            eligible = series.copy()
        base_value = eligible["value"].iloc[0]
        if not base_value:
            continue
        eligible = eligible.copy()
        eligible["normalized"] = (eligible["value"] / base_value) * 100.0
        rows.append(eligible[["date", "display_name", "normalized"]])

    if not rows:
        return pd.DataFrame(columns=["date", "display_name", "normalized"])
    return pd.concat(rows, ignore_index=True)


def performance_breadth(frame: pd.DataFrame, metric: str) -> float | None:
    if frame.empty or metric not in frame.columns:
        return None
    valid = frame[metric].dropna()
    if valid.empty:
        return None
    return float((valid > 0).mean())


def compute_overview_kpis(frame: pd.DataFrame, metric: str, metric_title: str | None = None) -> list[KpiCard]:
    cards: list[KpiCard] = []
    if frame.empty or metric not in frame.columns:
        return cards

    asset_class = aggregate_metric(frame, "asset_class", metric)
    if not asset_class.empty:
        best = asset_class.iloc[0]
        worst = asset_class.iloc[-1]
        cards.append(KpiCard("Best Asset Class", str(best["asset_class"]), f"{best[metric]:+.1%}"))
        cards.append(KpiCard("Worst Asset Class", str(worst["asset_class"]), f"{worst[metric]:+.1%}"))

    dm_em = aggregate_metric(frame, "dm_em_flag", metric)
    if not dm_em.empty and {"DM", "EM"} & set(dm_em["dm_em_flag"]):
        dm_value = dm_em.loc[dm_em["dm_em_flag"] == "DM", metric]
        em_value = dm_em.loc[dm_em["dm_em_flag"] == "EM", metric]
        if not dm_value.empty and not em_value.empty:
            cards.append(KpiCard("DM vs EM", f"{dm_value.iloc[0]:+.1%} vs {em_value.iloc[0]:+.1%}", metric_title or metric_label(metric)))

    regions = aggregate_metric(frame, "region", metric)
    if not regions.empty:
        best_region = regions.iloc[0]
        cards.append(KpiCard("Best Region", str(best_region["region"]), f"{best_region[metric]:+.1%}"))

    commodities = aggregate_metric(filter_asset_class(frame, "Commodities"), "commodity_category", metric)
    if not commodities.empty:
        bucket = commodities.iloc[0]
        cards.append(KpiCard("Best Commodity Bucket", str(bucket["commodity_category"]), f"{bucket[metric]:+.1%}"))

    breadth = performance_breadth(frame, metric)
    if breadth is not None:
        cards.append(KpiCard("Market Breadth", f"{breadth:.0%}", "Share of series with positive returns"))

    return cards[:6]


def comparison_exclusion_summary(frame: pd.DataFrame) -> tuple[int, int]:
    if frame.empty or "comparison_eligible" not in frame.columns:
        return 0, len(frame)
    eligible = int(frame["comparison_eligible"].fillna(False).sum())
    excluded = int((~frame["comparison_eligible"].fillna(False)).sum())
    return eligible, excluded


def default_series_selection(frame: pd.DataFrame, metric: str, n: int = 4) -> list[str]:
    if frame.empty:
        return []
    label_column = "display_name" if "display_name" in frame.columns else "asset_id"
    ranked = frame.dropna(subset=[metric]).sort_values(metric, ascending=False)
    return ranked[label_column].dropna().head(n).tolist()


def category_sort_key(value: str) -> tuple[int, str]:
    try:
        return COMMODITY_CATEGORY_ORDER.index(value), value
    except ValueError:
        return len(COMMODITY_CATEGORY_ORDER), value


def ordered_categories(values: list[str]) -> list[str]:
    return sorted(values, key=category_sort_key)
