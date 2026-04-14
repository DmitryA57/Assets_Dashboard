from __future__ import annotations

import streamlit as st

from src.analytics import WINDOW_COLUMNS, apply_reference_window, build_multi_metric_heatmap, comparison_universe, filter_asset_class, filter_series_types
from src.filters import apply_dashboard_filters
from src.load_data import load_dashboard_bundle
from src.ui import (
    build_page_filters,
    build_reference_date_control,
    format_metric_option,
    render_empty_state,
    render_filter_chips,
    render_heatmap,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="bonds", key_prefix="bonds")
snapshot = filter_asset_class(apply_dashboard_filters(bundle.snapshot, filters), "Bonds")
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="bonds")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Bonds",
    "Bond total return indices and yield monitors are separated so rates and returns are not mixed in the same view.",
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
        "The page structure is ready for total return indices, yields, and later spread views.",
    )
else:
    total_return_tab, yields_tab = st.tabs(["Total Return Indices", "Yields"])

    with total_return_tab:
        total_return = filter_series_types(comparison_snapshot, {"bond_total_return_index", "credit_total_return_index"})
        if total_return.empty:
            render_empty_state("No bond total return rows available.", "Import sovereign or credit benchmarks to populate this section.")
        else:
            render_ranked_bars("Bond Return Leadership", total_return, metric, limit=10, metric_title=metric_title)
            render_heatmap(
                "Bond Return Heatmap",
                build_multi_metric_heatmap(total_return, "country", WINDOW_COLUMNS),
                percent=True,
                column_labels=window_column_labels,
            )
            render_summary_table(
                "Bond Detail",
                snapshot.sort_values(metric, ascending=False),
                ["display_name", "country", "sub_asset_class", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days"],
                column_labels=window_column_labels,
            )

    with yields_tab:
        yields = filter_series_types(comparison_snapshot, {"government_yield"})
        if yields.empty:
            render_empty_state("No yield monitor rows available.", "Government yield views will appear here once rates data is loaded.")
        else:
            render_ranked_bars("Yield Leaders", yields, "ytd_bps", limit=10)
            render_heatmap(
                "Yield Change Heatmap",
                build_multi_metric_heatmap(yields, "country", ["ytd_bps", "since_event_bps", "yoy_bps"]),
                percent=False,
                column_labels=window_column_labels,
            )
            render_summary_table(
                "Yield Detail",
                snapshot.sort_values("ytd_bps", ascending=False),
                ["display_name", "country", "latest_value", "ytd_bps", "since_event_bps", "yoy_bps", "data_as_of", "freshness_status", "lag_days"],
                column_labels=window_column_labels,
            )
