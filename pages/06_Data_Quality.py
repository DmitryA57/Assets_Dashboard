from __future__ import annotations

import pandas as pd
import streamlit as st

from config import AGED_MAX_LAG_DAYS, DEFAULT_EVENT_DATE, DEFAULT_EVENT_LABEL, FRESH_MAX_LAG_DAYS
from src.load_data import load_dashboard_bundle
from src.ui import render_page_header, render_summary_table
from src.validators import collect_validation_issues, validate_snapshot_freshness


bundle = load_dashboard_bundle()
render_page_header(
    "Data Quality",
    "Internal monitoring for freshness, coverage, taxonomy checks, and snapshot health.",
    bundle.snapshot,
)

issues = collect_validation_issues(bundle.asset_master, bundle.prices)
issues.extend(validate_snapshot_freshness(bundle.snapshot))

coverage_ratio = 0.0
if len(bundle.asset_master) > 0:
    coverage_ratio = len(bundle.snapshot) / len(bundle.asset_master)

latest_snapshot_date = "-"
if not bundle.snapshot.empty and "snapshot_date" in bundle.snapshot.columns:
    latest_snapshot_date = pd.to_datetime(bundle.snapshot["snapshot_date"], errors="coerce").max().strftime("%d %b %Y")

comparison_eligible = int(bundle.snapshot["comparison_eligible"].fillna(False).sum()) if "comparison_eligible" in bundle.snapshot.columns else 0
stale_count = int((bundle.snapshot["freshness_status"] == "Stale").sum()) if "freshness_status" in bundle.snapshot.columns else 0
aged_count = int((bundle.snapshot["freshness_status"] == "Aged").sum()) if "freshness_status" in bundle.snapshot.columns else 0
fresh_count = int((bundle.snapshot["freshness_status"] == "Fresh").sum()) if "freshness_status" in bundle.snapshot.columns else 0

cards = st.columns(4)
cards[0].metric("Assets Configured", f"{len(bundle.asset_master):,}")
cards[1].metric("Snapshot Coverage", f"{coverage_ratio:.0%}")
cards[2].metric("Eligible for Comparison", f"{comparison_eligible:,}")
cards[3].metric("Latest Snapshot", latest_snapshot_date)

st.subheader("Quality Checks")
if not issues:
    st.success("No structural data-quality issues detected.")
else:
    for issue in issues:
        st.write(f"- {issue}")

freshness_cards = st.columns(4)
freshness_cards[0].metric("Fresh Series", f"{fresh_count:,}")
freshness_cards[1].metric("Aged Series", f"{aged_count:,}")
freshness_cards[2].metric("Stale Series", f"{stale_count:,}")
freshness_cards[3].metric("Excluded from Comparison", f"{stale_count:,}")

comparison_summary = pd.DataFrame(
    [
        {"Metric": "Configured assets", "Value": len(bundle.asset_master)},
        {"Metric": "Assets with snapshot row", "Value": len(bundle.snapshot)},
        {"Metric": "Assets eligible for comparison", "Value": comparison_eligible},
        {"Metric": "Excluded stale assets", "Value": stale_count},
    ]
)
render_summary_table("Comparison Universe Summary", comparison_summary, ["Metric", "Value"])

coverage = bundle.asset_master.groupby("asset_class", dropna=False).size().reset_index(name="Configured Assets")
snapshot_coverage = bundle.snapshot.groupby("asset_class", dropna=False).size().reset_index(name="Snapshot Rows")
coverage = coverage.merge(snapshot_coverage, on="asset_class", how="left").fillna(0)
render_summary_table("Coverage by Asset Class", coverage, ["asset_class", "Configured Assets", "Snapshot Rows"])

if not bundle.snapshot.empty:
    freshness = bundle.snapshot[["display_name", "asset_class", "commodity_category", "data_as_of", "lag_days", "freshness_status"]].copy()
    freshness = freshness.sort_values(["lag_days", "display_name"], ascending=[False, True])
    render_summary_table("Freshness Detail", freshness, ["display_name", "asset_class", "commodity_category", "data_as_of", "lag_days", "freshness_status"])

    excluded = bundle.snapshot.loc[
        ~bundle.snapshot["comparison_eligible"].fillna(False),
        ["display_name", "asset_class", "commodity_category", "data_as_of", "lag_days", "freshness_status"],
    ].copy()
    if not excluded.empty:
        excluded["excluded_reason"] = "Stale relative to snapshot date"
        render_summary_table(
            "Excluded Series",
            excluded.sort_values(["lag_days", "display_name"], ascending=[False, True]),
            ["display_name", "asset_class", "commodity_category", "data_as_of", "lag_days", "freshness_status", "excluded_reason"],
        )

null_counts = (
    bundle.asset_master.isna()
    .sum()
    .reset_index()
    .rename(columns={"index": "Field", 0: "Null Count"})
    .sort_values("Null Count", ascending=False)
)
render_summary_table("Asset Master Null Checks", null_counts, ["Field", "Null Count"])

with st.expander("Return Definitions & Data Rules", expanded=False):
    st.markdown(
        f"""
        **Definitions**

        - `Snapshot Date`: dashboard-wide comparison anchor date.
        - `Data as of`: latest available observation date for a specific series.
        - `Freshness`: lag-based status derived from `Snapshot Date - Data as of`.
        - `Last`: latest available level for the series.
        - `YTD`, `{DEFAULT_EVENT_LABEL} ({pd.Timestamp(DEFAULT_EVENT_DATE).strftime("%d.%m.%Y")})`, `YoY`: default return windows calculated from each series' latest eligible observation.
        - `Reference date`: user-selected override for the middle return window across the dashboard.

        **Return formulas**

        - `YTD = latest eligible value / value at last valid close of prior year - 1`
        - `Reference window = latest eligible value / value at selected reference date - 1`
        - `YoY = latest eligible value / value approximately one year earlier - 1`

        **Freshness rules**

        - `Fresh`: lag <= {FRESH_MAX_LAG_DAYS} days
        - `Aged`: lag between {FRESH_MAX_LAG_DAYS + 1} and {AGED_MAX_LAG_DAYS} days
        - `Stale`: lag > {AGED_MAX_LAG_DAYS} days
        - Stale series are excluded from rankings, heatmaps, and cross-asset summary comparisons.

        **Missing data handling**

        - Missing observations are not forward-filled for return calculations.
        - Reference-date lookup uses the latest valid observation on or before the selected date.
        - Series can remain visible in detail tables even when excluded from summary logic.

        **Comparison universe rule**

        - Rankings, heatmaps, and cross-sectional comparisons use only series that pass freshness checks relative to the current snapshot date.

        **Commodity taxonomy**

        - Battery Materials
        - Energy Materials
        - Base Metals
        - Precious Metals
        - Agriculture
        - Fertilizers
        - Steel Chain
        - Energy buckets
        """
    )
