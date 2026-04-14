from __future__ import annotations

import pandas as pd
import streamlit as st

from src.analytics import (
    WINDOW_COLUMNS,
    apply_reference_window,
    aggregate_metric,
    build_multi_metric_heatmap,
    comparison_universe,
    default_series_selection,
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
    render_normalized_line_chart,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="compare", key_prefix="compare")
snapshot = apply_dashboard_filters(bundle.snapshot, filters)
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="compare")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Compare",
    "Cross-asset and cross-region comparison tools for market leadership, normalized performance, and relative heatmaps.",
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
    render_empty_state("No comparison results available.", "Adjust the filters or import more series to compare.")
else:
    selector_col, options_col = st.columns([3, 1])
    label_map = snapshot[["asset_id", "display_name"]].drop_duplicates().dropna()
    default_labels = default_series_selection(comparison_snapshot if not comparison_snapshot.empty else snapshot, metric, n=min(4, len(label_map)))

    with options_col:
        selected_labels = st.multiselect(
            "Selected series",
            options=label_map["display_name"].tolist(),
            default=default_labels,
            help="Choose between two and eight series for normalized comparison.",
        )
        start_mode = st.selectbox("Normalize from", ["Year Start", "Reference Date", "1Y Ago", "First Available"])
        group_by = st.selectbox(
            "Heatmap grouping",
            options=[column for column in ["asset_class", "region", "commodity_category", "country"] if column in snapshot.columns],
        )

    selected_assets = label_map.loc[label_map["display_name"].isin(selected_labels), "asset_id"].tolist()[:8]
    history = normalized_history(
        bundle.prices,
        bundle.asset_master,
        selected_assets,
        start_mode=start_mode,
        event_date=reference_date,
    )
    with selector_col:
        render_normalized_line_chart("Normalized Performance", history, event_date=reference_date)

    leadership_left, leadership_right = st.columns(2)
    if comparison_snapshot.empty:
        render_empty_state(
            "Not enough fresh series to render comparison heatmap.",
            "Current filters leave only stale rows in the comparison universe. The normalized chart still supports direct series inspection.",
        )
    else:
        top, bottom = top_bottom(comparison_snapshot, metric, n=10)
        with leadership_left:
            render_ranked_bars("Cross-Asset Leaders", top, metric, limit=10, metric_title=metric_title)
        with leadership_right:
            render_ranked_bars("Cross-Asset Laggards", bottom, metric, limit=10, metric_title=metric_title)

        render_heatmap(
            "Compare Heatmap",
            build_multi_metric_heatmap(comparison_snapshot, group_by, WINDOW_COLUMNS),
            percent=True,
            column_labels=window_column_labels,
        )

        dm_em = aggregate_metric(comparison_snapshot, "dm_em_flag", metric)
        if not dm_em.empty:
            render_ranked_bars("DM vs EM Comparison", dm_em, metric, label_column="dm_em_flag", limit=4, metric_title=metric_title)

    render_summary_table(
        "Comparison Detail",
        snapshot.sort_values(metric, ascending=False),
        ["display_name", "asset_class", "country", "commodity_category", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days"],
        column_labels=window_column_labels,
    )
