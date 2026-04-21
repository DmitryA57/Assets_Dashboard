from __future__ import annotations

import pandas as pd
import streamlit as st

from src.analytics import WINDOW_COLUMNS, format_reference_window_label, resolve_reference_date, top_bottom
from src.load_data import load_dashboard_bundle
from src.services.russia_bonds import load_russia_bonds_state
from src.services.russia_common import build_russia_kpi_cards, build_status_lines
from src.services.russia_equities import load_russia_equities_state
from src.ui import (
    format_metric_option,
    render_empty_state,
    render_kpi_cards,
    render_page_header,
    render_ranked_bars,
    render_summary_table,
)


bundle = load_dashboard_bundle()
default_reference_date = resolve_reference_date(bundle.events)

selected_reference_date = st.sidebar.date_input(
    "Reference date",
    value=default_reference_date.date(),
    key="russia_reference_date",
    help="Used for the middle performance window. If the selected date is not a MOEX trading day, the page uses the first available observation after that date.",
)
reference_date = pd.Timestamp(selected_reference_date)
reference_label = format_reference_window_label(reference_date, default_reference_date=default_reference_date)
st.sidebar.caption(f"Middle window: {reference_label}")

market_mode = st.radio(
    "Market",
    options=["Equities", "Bonds"],
    horizontal=True,
)

metric = st.radio(
    "Return window",
    options=WINDOW_COLUMNS,
    format_func=lambda value: format_metric_option(value, reference_label),
    horizontal=True,
)
metric_title = format_metric_option(metric, reference_label)

empty_snapshot = pd.DataFrame(columns=["snapshot_date"])

load_state = load_russia_equities_state if market_mode == "Equities" else load_russia_bonds_state
try:
    state = load_state("", reference_date)
except Exception as error:
    render_page_header(
        "Russia",
        "Russian market monitor built from exported T-Bank data.",
        empty_snapshot,
    )
    st.caption("Source: T-Bank export")
    st.error(f"Russia data could not be loaded: {error}")
    st.stop()

subtitle = (
    "Russian equity indices with headline and sector coverage from exported T-Bank history."
    if market_mode == "Equities"
    else "Russian bond index monitor for government and corporate benchmarks from exported T-Bank history."
)
render_page_header("Russia", subtitle, state.snapshot)
st.caption(f"Source: {state.data_source}")
st.caption("Methodology: if the selected reference date is a non-trading day for a series, the calculation uses the first available observation after that date.")

if state.snapshot.empty:
    render_empty_state(
        "Russia data is unavailable.",
        "Check the exported Russia CSV files in data/russia.",
    )
else:
    ranking_frame = state.comparison_snapshot if not state.comparison_snapshot.empty else state.snapshot
    render_kpi_cards(build_russia_kpi_cards(state.snapshot, metric, metric_title))

    top, bottom = top_bottom(ranking_frame, metric=metric, n=min(6, len(ranking_frame)))
    top_col, bottom_col = st.columns(2)
    with top_col:
        render_ranked_bars("Top Performers", top if not top.empty else ranking_frame, metric, limit=6, metric_title=metric_title)
    with bottom_col:
        render_ranked_bars("Bottom Performers", bottom if not bottom.empty else ranking_frame, metric, limit=6, metric_title=metric_title)

    table_columns = (
        ["display_name", "ticker", "market_price", "ytd", "since_event", "yoy", "trading_status", "last_update_date", "freshness_status", "lag_days"]
        if market_mode == "Equities"
        else ["display_name", "ticker", "market_price", "ytd", "since_event", "yoy", "trading_status", "last_update_date", "maturity_date", "freshness_status", "lag_days"]
    )
    render_summary_table(
        f"{market_mode} Detail",
        state.snapshot.sort_values(metric, ascending=False),
        table_columns,
        column_labels={"since_event": reference_label},
    )

st.subheader("Data Quality")
for line in build_status_lines(state):
    st.caption(line)

for warning in state.warnings:
    st.warning(warning)
