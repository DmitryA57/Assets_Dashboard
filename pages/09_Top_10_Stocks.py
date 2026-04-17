from __future__ import annotations

import streamlit as st

from src.analytics import (
    KpiCard,
    WINDOW_COLUMNS,
    apply_reference_window,
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
    render_kpi_cards,
    render_normalized_line_chart,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


TOP_STOCK_DETAIL_COLUMNS = [
    "display_name",
    "country",
    "sector_name",
    "latest_value",
    "ytd",
    "since_event",
    "yoy",
    "data_as_of",
    "freshness_status",
    "lag_days",
]


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="top_stocks", key_prefix="top_stocks")
snapshot = filter_asset_class(apply_dashboard_filters(bundle.snapshot, filters), "Top-10 Stocks")
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="top_stocks")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Top-10 Stocks",
    "A focused watchlist for the individual stocks you want to monitor separately from the country-index dashboard.",
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
    render_empty_state("Top-stock data is not loaded yet.", "Import Assets_data.xlsx to populate the focused stock watchlist.")
else:
    if len(snapshot) != 10:
        st.caption(f"Current source file contains {len(snapshot)} tracked stock rows.")

    eligible_count, excluded_count = comparison_exclusion_summary(snapshot)
    if excluded_count:
        st.caption(f"{eligible_count} stock rows are included in comparison logic. {excluded_count} stale rows remain visible in the detail table.")

    cards: list[KpiCard] = []
    top, bottom = top_bottom(comparison_snapshot if not comparison_snapshot.empty else snapshot, metric, n=1)
    if not top.empty:
        cards.append(KpiCard("Best Stock", str(top.iloc[0]["display_name"]), f"{top.iloc[0][metric]:+.1%}"))
    if not bottom.empty:
        cards.append(KpiCard("Worst Stock", str(bottom.iloc[0]["display_name"]), f"{bottom.iloc[0][metric]:+.1%}"))
    metric_frame = comparison_snapshot if not comparison_snapshot.empty else snapshot
    metric_values = metric_frame[metric].dropna() if metric in metric_frame.columns else metric_frame.iloc[0:0]
    if not metric_values.empty:
        cards.append(KpiCard("Average Return", f"{metric_values.mean():+.1%}", metric_title))
    render_kpi_cards(cards[:3])

    overview_tab, detail_tab = st.tabs(["Overview", "Detail"])

    with overview_tab:
        chart_col, history_col = st.columns([2, 3])
        with chart_col:
            render_ranked_bars(
                "Stock Leadership",
                comparison_snapshot if not comparison_snapshot.empty else snapshot,
                metric,
                limit=min(10, len(snapshot)),
                metric_title=metric_title,
            )

        label_map = snapshot[["asset_id", "display_name"]].drop_duplicates().dropna()
        default_labels = default_series_selection(comparison_snapshot if not comparison_snapshot.empty else snapshot, metric, n=min(5, len(label_map)))
        selected_labels = history_col.multiselect(
            "Compare stocks",
            options=label_map["display_name"].tolist(),
            default=default_labels,
            help="Select up to six names for normalized comparison.",
        )
        start_mode = history_col.selectbox("Normalize from", ["Year Start", "Reference Date", "1Y Ago", "First Available"])
        selected_assets = label_map.loc[label_map["display_name"].isin(selected_labels), "asset_id"].tolist()[:6]
        history = normalized_history(
            bundle.prices,
            bundle.asset_master,
            selected_assets,
            start_mode=start_mode,
            event_date=reference_date,
        )
        with history_col:
            render_normalized_line_chart("Normalized Stock History", history, event_date=reference_date)

    with detail_tab:
        render_summary_table(
            "Top-10 Stock Detail",
            snapshot.sort_values(metric, ascending=False),
            TOP_STOCK_DETAIL_COLUMNS + ["bbg_ticker"],
            column_labels=window_column_labels,
        )
