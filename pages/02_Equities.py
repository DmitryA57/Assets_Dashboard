from __future__ import annotations

import pandas as pd
import streamlit as st

from src.analytics import (
    KpiCard,
    WINDOW_COLUMNS,
    apply_reference_window,
    aggregate_metric,
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
    prepare_user_table,
    render_empty_state,
    render_filter_chips,
    render_heatmap,
    render_kpi_cards,
    render_normalized_line_chart,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


bundle = load_dashboard_bundle()
filters = build_page_filters(bundle, page_name="equities", key_prefix="equities")
snapshot = filter_asset_class(apply_dashboard_filters(bundle.snapshot, filters), "Equities")
reference_date, reference_label = build_reference_date_control(bundle, key_prefix="equities")
snapshot = apply_reference_window(snapshot, bundle.prices, reference_date)
comparison_snapshot = comparison_universe(snapshot)
headline_snapshot = snapshot[snapshot["sub_asset_class"].fillna("Headline Index") == "Headline Index"].copy()
headline_comparison_snapshot = comparison_universe(headline_snapshot)
sector_snapshot = snapshot[snapshot["sub_asset_class"].fillna("") == "S&P 500 Sector"].copy()
sector_comparison_snapshot = comparison_universe(sector_snapshot)
window_column_labels = {"since_event": reference_label, "since_event_bps": reference_label}

render_page_header(
    "Equities",
    "Country indices first, with return windows designed for fast relative-performance analysis.",
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
        "No equity results for this selection.",
        "Broaden the filters or import additional equity series.",
    )
else:
    eligible_count, excluded_count = comparison_exclusion_summary(snapshot)
    if excluded_count:
        st.caption(f"{eligible_count} equity series are included in comparisons. {excluded_count} stale series remain visible only in detail views.")

    cards = []
    country_avg = aggregate_metric(headline_comparison_snapshot, "country", metric)
    if not country_avg.empty:
        cards.append(KpiCard("Best Country", str(country_avg.iloc[0]["country"]), f"{country_avg.iloc[0][metric]:+.1%}"))
        cards.append(KpiCard("Worst Country", str(country_avg.iloc[-1]["country"]), f"{country_avg.iloc[-1][metric]:+.1%}"))
    dm_em = aggregate_metric(headline_comparison_snapshot, "dm_em_flag", metric)
    if not dm_em.empty:
        for _, row in dm_em.iterrows():
            cards.append(KpiCard(f"{row['dm_em_flag']} Average", f"{row[metric]:+.1%}", "Headline equity indices"))
    render_kpi_cards(cards[:5])

    headline_tab, sectors_tab = st.tabs(["Headline Indices", "Sectors"])

    with headline_tab:
        if headline_comparison_snapshot.empty:
            render_empty_state("Not enough fresh series for rankings.", "Only sufficiently fresh equity series are used for comparisons and heatmaps.")
        else:
            render_ranked_bars("Country Index Performance", headline_comparison_snapshot, metric, limit=12, metric_title=metric_title)
            render_heatmap(
                "Equity Heatmap",
                build_multi_metric_heatmap(headline_comparison_snapshot, "country", WINDOW_COLUMNS),
                percent=True,
                column_labels=window_column_labels,
            )

        history_col, options_col = st.columns([3, 1])
        label_map = headline_snapshot[["asset_id", "display_name"]].drop_duplicates().dropna()
        default_labels = default_series_selection(
            headline_comparison_snapshot if not headline_comparison_snapshot.empty else headline_snapshot,
            metric,
            n=min(5, len(label_map)),
        )
        with options_col:
            selected_labels = st.multiselect(
                "Compare series",
                options=label_map["display_name"].tolist(),
                default=default_labels,
                help="Select up to five series for normalized comparison.",
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
            render_normalized_line_chart("Normalized Price History", history, event_date=reference_date)

        render_summary_table(
            "Detail Table",
            headline_snapshot.sort_values(metric, ascending=False),
            ["display_name", "country", "region", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days", "return_variant"],
            column_labels=window_column_labels,
        )

    with sectors_tab:
        if sector_snapshot.empty:
            render_empty_state(
                "No sector series available yet.",
                "Load the S&P500 sector sheet into Assets_data.xlsx to populate this block.",
            )
        else:
            top, bottom = top_bottom(sector_comparison_snapshot if not sector_comparison_snapshot.empty else sector_snapshot, metric, n=6)
            left, right = st.columns(2)
            with left:
                render_ranked_bars("Top Sectors", top, metric, limit=6, metric_title=metric_title)
            with right:
                render_ranked_bars("Bottom Sectors", bottom, metric, limit=6, metric_title=metric_title)
            render_summary_table(
                "Sector Detail",
                sector_snapshot.sort_values(metric, ascending=False),
                ["display_name", "sector_name", "latest_value", "ytd", "since_event", "yoy", "data_as_of", "freshness_status", "lag_days"],
                column_labels=window_column_labels,
            )
