from __future__ import annotations

import pandas as pd
import streamlit as st

from src.analytics import (
    WINDOW_COLUMNS,
    apply_reference_window,
    aggregate_metric,
    build_multi_metric_heatmap,
    comparison_exclusion_summary,
    comparison_universe,
    compute_overview_kpis,
    top_bottom,
)
from src.filters import apply_dashboard_filters
from src.load_data import DashboardBundle, load_dashboard_bundle
from src.services.overview_service import exclude_russia_rows, load_russia_overview_bundle
from src.ui import (
    build_page_filters,
    build_reference_date_control,
    format_metric_option,
    render_empty_state,
    render_filter_chips,
    render_heatmap,
    render_kpi_cards,
    render_page_header,
    render_page_links,
    render_ranked_bars,
)


bundle = load_dashboard_bundle()
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="overview_rus_world")

world_snapshot = exclude_russia_rows(bundle.snapshot)
world_prices = bundle.prices.copy()
russia_bundle = load_russia_overview_bundle(reference_date)
russia_snapshot = russia_bundle.snapshot
warnings = russia_bundle.warnings

world_snapshot = apply_reference_window(world_snapshot, world_prices, reference_date)
overview_snapshot = pd.concat([world_snapshot, russia_snapshot], ignore_index=True, sort=False)
overview_bundle = DashboardBundle(
    asset_master=bundle.asset_master,
    events=bundle.events,
    prices=bundle.prices,
    snapshot=overview_snapshot,
)
filters = build_page_filters(overview_bundle, page_name="overview", key_prefix="overview_rus_world")
snapshot = apply_dashboard_filters(overview_snapshot, filters)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Overview Rus & World",
    "Cross-asset overview that combines the global dashboard universe with RTSI as the single Russian cross-market benchmark.",
    snapshot,
)
st.caption("Methodology: if the selected reference date is a non-trading day for a series, the calculation uses the first available observation after that date.")
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
        "No market view available.",
        "Try broadening the filters or verify the exported Russia data files.",
    )
elif comparison_snapshot.empty:
    render_empty_state(
        "Not enough fresh series for market comparison.",
        "All currently visible rows are stale relative to the snapshot date. Check Data Quality for exclusions.",
    )
else:
    eligible_count, excluded_count = comparison_exclusion_summary(snapshot)
    if excluded_count:
        st.caption(f"{eligible_count} series are used in summary logic. {excluded_count} stale series are excluded from rankings and heatmaps.")

    render_kpi_cards(compute_overview_kpis(comparison_snapshot, metric, metric_title=metric_title))

    leadership_left, leadership_right = st.columns(2)
    with leadership_left:
        asset_class_avg = aggregate_metric(comparison_snapshot, "asset_class", metric)
        render_ranked_bars("Cross-Asset Leadership", asset_class_avg, metric, label_column="asset_class", limit=6, metric_title=metric_title)
    with leadership_right:
        dm_em_avg = aggregate_metric(comparison_snapshot, "dm_em_flag", metric)
        render_ranked_bars("DM vs EM", dm_em_avg, metric, label_column="dm_em_flag", limit=4, metric_title=metric_title)

    top, bottom = top_bottom(comparison_snapshot, metric=metric, n=8)
    top_col, bottom_col = st.columns(2)
    with top_col:
        render_ranked_bars("Top Performers", top, metric, limit=8, metric_title=metric_title)
    with bottom_col:
        render_ranked_bars("Bottom Performers", bottom, metric, limit=8, metric_title=metric_title)

    heatmap = build_multi_metric_heatmap(comparison_snapshot, group_by="asset_class", metrics=WINDOW_COLUMNS)
    render_heatmap("Cross-Asset Heatmap", heatmap, percent=True, column_labels=window_column_labels)

    st.subheader("Quick Drill-Down")
    st.caption("Move from the market summary into the asset class pages.")
    render_page_links()

for warning in warnings:
    st.warning(warning)
