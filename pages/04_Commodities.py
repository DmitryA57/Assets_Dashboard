from __future__ import annotations

import streamlit as st

from src.analytics import (
    COMMODITY_CATEGORY_ORDER,
    KpiCard,
    WINDOW_COLUMNS,
    apply_reference_window,
    aggregate_metric,
    category_sort_key,
    comparison_exclusion_summary,
    comparison_universe,
    filter_asset_class,
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
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="commodities", key_prefix="commodities")
snapshot = filter_asset_class(apply_dashboard_filters(bundle.snapshot, filters), "Commodities")
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="commodities")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Commodities",
    "Commodities are grouped by market bucket so the page reads like a structured commodity map, not a flat list of tickers.",
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
        "No commodity results for this selection.",
        "Import benchmark commodity series to populate the commodity buckets.",
    )
else:
    eligible_count, excluded_count = comparison_exclusion_summary(snapshot)
    if excluded_count:
        st.caption(f"{eligible_count} commodity series are used in bucket comparisons. {excluded_count} stale series remain visible in detail tables only.")

    cards = []
    bucket_avg = aggregate_metric(comparison_snapshot, "commodity_category", metric)
    if not bucket_avg.empty:
        cards.append(KpiCard("Best Bucket", str(bucket_avg.iloc[0]["commodity_category"]), f"{bucket_avg.iloc[0][metric]:+.1%}"))
        cards.append(KpiCard("Worst Bucket", str(bucket_avg.iloc[-1]["commodity_category"]), f"{bucket_avg.iloc[-1][metric]:+.1%}"))
    best, worst = top_bottom(comparison_snapshot, metric, n=1)
    if not best.empty:
        cards.append(KpiCard("Best Series", str(best.iloc[0]["display_name"]), f"{best.iloc[0][metric]:+.1%}"))
    if not worst.empty:
        cards.append(KpiCard("Worst Series", str(worst.iloc[0]["display_name"]), f"{worst.iloc[0][metric]:+.1%}"))
    render_kpi_cards(cards[:4])

    available_categories = snapshot["commodity_category"].dropna().astype(str).unique().tolist()
    ordered = sorted(available_categories, key=category_sort_key)
    for category in ordered:
        category_frame = snapshot[snapshot["commodity_category"] == category].sort_values(metric, ascending=False)
        comparison_category = comparison_snapshot[comparison_snapshot["commodity_category"] == category].sort_values(metric, ascending=False)
        if category_frame.empty:
            continue

        st.markdown(f"## {category}")
        if comparison_category.empty:
            st.caption("No fresh series in this category for comparison logic. Detail table below still shows all rows.")
            best_row = None
            worst_row = None
            avg_value = None
        else:
            best_row = comparison_category.iloc[0]
            worst_row = comparison_category.iloc[-1]
            avg_value = comparison_category[metric].dropna().mean()
        left, mid, right = st.columns(3)
        left.metric("Average Return", "-" if avg_value is None else f"{avg_value:+.1%}")
        mid.metric("Best Series", "-" if best_row is None else str(best_row["display_name"]), "-" if best_row is None else f"{best_row[metric]:+.1%}")
        right.metric("Worst Series", "-" if worst_row is None else str(worst_row["display_name"]), "-" if worst_row is None else f"{worst_row[metric]:+.1%}")

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
                ["display_name", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days"],
                column_labels=window_column_labels,
            )

    st.markdown("## All Commodities")
    render_summary_table(
        "Full Commodity Table",
        snapshot.sort_values(metric, ascending=False),
        ["display_name", "commodity_category", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days"],
        column_labels=window_column_labels,
    )
