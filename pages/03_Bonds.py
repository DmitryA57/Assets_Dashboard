from __future__ import annotations

import pandas as pd
import streamlit as st

from src.analytics import (
    KpiCard,
    WINDOW_COLUMNS,
    aggregate_metric,
    apply_reference_window,
    build_multi_metric_heatmap,
    comparison_exclusion_summary,
    comparison_universe,
    default_series_selection,
    filter_asset_class,
    normalized_history,
    top_bottom,
)
from src.filters import apply_dashboard_filters
from src.load_data import load_dashboard_bundle
from src.ui import (
    build_page_filters,
    build_reference_date_control,
    format_metric_option,
    render_empty_state,
    render_filter_chips,
    render_heatmap,
    render_kpi_cards,
    render_normalized_line_chart,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


BOND_CATEGORY_ORDER = ["Global", "U.S. fixed income", "Regional", "Other"]
BOND_DETAIL_COLUMNS = [
    "display_name",
    "sub_asset_class",
    "sector_name",
    "country",
    "latest_value",
    "ytd",
    "since_event",
    "yoy",
    "data_as_of",
    "freshness_status",
    "lag_days",
]


def ordered_bond_categories(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "sub_asset_class" not in frame.columns:
        return []
    available = frame["sub_asset_class"].dropna().astype(str).unique().tolist()
    return [category for category in BOND_CATEGORY_ORDER if category in available]


def bond_kpi_cards(frame: pd.DataFrame, metric: str, metric_title: str) -> list[KpiCard]:
    cards: list[KpiCard] = []
    if frame.empty or metric not in frame.columns:
        return cards

    category_avg = aggregate_metric(frame, "sub_asset_class", metric)
    if not category_avg.empty:
        cards.append(KpiCard("Best Bond Bucket", str(category_avg.iloc[0]["sub_asset_class"]), f"{category_avg.iloc[0][metric]:+.1%}"))
        cards.append(KpiCard("Worst Bond Bucket", str(category_avg.iloc[-1]["sub_asset_class"]), f"{category_avg.iloc[-1][metric]:+.1%}"))

    top, bottom = top_bottom(frame, metric, n=1)
    if not top.empty:
        cards.append(KpiCard("Best Index", str(top.iloc[0]["display_name"]), f"{top.iloc[0][metric]:+.1%}"))
    if not bottom.empty:
        cards.append(KpiCard("Worst Index", str(bottom.iloc[0]["display_name"]), f"{bottom.iloc[0][metric]:+.1%}"))

    high_yield = frame[frame["sector_name"].fillna("") == "High Yield"]
    if not high_yield.empty:
        cards.append(KpiCard("High Yield Avg", f"{high_yield[metric].mean():+.1%}", metric_title))
    return cards[:5]


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="bonds", key_prefix="bonds")
snapshot = filter_asset_class(apply_dashboard_filters(bundle.snapshot, filters), "Bonds")
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="bonds")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Bonds",
    "Global, U.S., regional, inflation-linked, municipal, and high-yield bond index monitors.",
    snapshot,
)
render_filter_chips(filters)

metric = st.radio(
    "Return window",
    options=WINDOW_COLUMNS,
    format_func=lambda value: format_metric_option(value, reference_label),
    horizontal=True,
)
metric_title = format_metric_option(metric, reference_label)

if snapshot.empty:
    render_empty_state(
        "Bond data is not loaded yet.",
        "Import the approved bond index universe to populate this section.",
    )
else:
    eligible_count, excluded_count = comparison_exclusion_summary(snapshot)
    if excluded_count:
        st.caption(f"{eligible_count} bond indices are included in comparison views. {excluded_count} stale rows remain visible only in detail tables.")

    render_kpi_cards(bond_kpi_cards(comparison_snapshot, metric, metric_title))

    overview_tab, segments_tab, high_yield_tab, detail_tab = st.tabs(["Overview", "Segments", "High Yield", "Detail"])

    with overview_tab:
        if comparison_snapshot.empty:
            render_empty_state("Not enough fresh bond indices for rankings.", "Detail tables still show all available rows.")
        else:
            leader_col, heatmap_col = st.columns([3, 2])
            with leader_col:
                render_ranked_bars("Bond Index Leadership", comparison_snapshot, metric, limit=12, metric_title=metric_title)
            with heatmap_col:
                render_heatmap(
                    "Bond Bucket Heatmap",
                    build_multi_metric_heatmap(comparison_snapshot, "sub_asset_class", WINDOW_COLUMNS),
                    percent=True,
                    column_labels=window_column_labels,
                )

            history_col, options_col = st.columns([3, 1])
            label_map = snapshot[["asset_id", "display_name"]].drop_duplicates().dropna()
            default_labels = default_series_selection(comparison_snapshot if not comparison_snapshot.empty else snapshot, metric, n=min(5, len(label_map)))
            with options_col:
                selected_labels = st.multiselect(
                    "Compare bond indices",
                    options=label_map["display_name"].tolist(),
                    default=default_labels,
                    help="Select up to five bond indices for normalized comparison.",
                )
                start_mode = st.selectbox("Normalize from", ["Year Start", "Reference Date", "1Y Ago", "First Available"])

            selected_assets = label_map.loc[label_map["display_name"].isin(selected_labels), "asset_id"].tolist()
            history = normalized_history(
                bundle.prices,
                bundle.asset_master,
                selected_assets[:5],
                start_mode=start_mode,
                event_date=reference_date,
            )
            with history_col:
                render_normalized_line_chart("Normalized Bond Index History", history, event_date=reference_date)

    with segments_tab:
        for category in ordered_bond_categories(snapshot):
            category_frame = snapshot[snapshot["sub_asset_class"] == category].sort_values(metric, ascending=False)
            comparison_category = comparison_snapshot[comparison_snapshot["sub_asset_class"] == category].sort_values(metric, ascending=False)
            st.markdown(f"## {category}")

            bars_col, table_col = st.columns([2, 3])
            with bars_col:
                render_ranked_bars(
                    f"{category} Leadership",
                    comparison_category if not comparison_category.empty else category_frame,
                    metric,
                    limit=min(8, len(category_frame)),
                    metric_title=metric_title,
                )
            with table_col:
                render_summary_table(
                    "Detail",
                    category_frame,
                    BOND_DETAIL_COLUMNS,
                    column_labels=window_column_labels,
                )

    with high_yield_tab:
        high_yield = snapshot[snapshot["sector_name"].fillna("") == "High Yield"].copy()
        comparison_high_yield = comparison_snapshot[comparison_snapshot["sector_name"].fillna("") == "High Yield"].copy()
        if high_yield.empty:
            render_empty_state("No high-yield bond indices available.", "The approved high-yield universe is Global HY, U.S. HY, and Pan-European HY.")
        else:
            render_ranked_bars(
                "High Yield Leadership",
                comparison_high_yield if not comparison_high_yield.empty else high_yield,
                metric,
                limit=6,
                metric_title=metric_title,
            )
            render_summary_table(
                "High Yield Detail",
                high_yield.sort_values(metric, ascending=False),
                BOND_DETAIL_COLUMNS,
                column_labels=window_column_labels,
            )

    with detail_tab:
        render_summary_table(
            "Full Bond Index Table",
            snapshot.sort_values(["sub_asset_class", metric], ascending=[True, False]),
            BOND_DETAIL_COLUMNS + ["asset_name", "bbg_ticker"],
            column_labels=window_column_labels,
        )
