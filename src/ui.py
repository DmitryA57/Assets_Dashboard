from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from src.analytics import WINDOW_LABELS, format_reference_window_label, metric_label, resolve_reference_date
from src.filters import DashboardFilters
from src.formatters import format_bps, format_date, format_freshness, format_level, format_percent
from src.load_data import DashboardBundle


FILTER_CONFIG = {
    "asset_class": {"label": "Asset class", "field": "asset_class"},
    "dm_em_flag": {"label": "DM / EM", "field": "dm_em_flag"},
    "country": {"label": "Country", "field": "country"},
    "region": {"label": "Region", "field": "region"},
    "return_variant": {"label": "Return type", "field": "return_variant"},
    "sector_name": {"label": "Sector", "field": "sector_name"},
    "commodity_category": {"label": "Commodity category", "field": "commodity_category"},
    "sub_asset_class": {"label": "Bond type", "field": "sub_asset_class"},
}

PAGE_FILTERS = {
    "overview": ["asset_class", "dm_em_flag", "region"],
    "equities": ["dm_em_flag", "country", "region", "sector_name", "return_variant"],
    "bonds": ["dm_em_flag", "country", "region", "sub_asset_class", "sector_name"],
    "commodities": ["commodity_category"],
    "compare": ["asset_class", "dm_em_flag", "region", "commodity_category"],
}

TABLE_LABELS = {
    "display_name": "Name",
    "country": "Country",
    "region": "Region",
    "commodity_category": "Category",
    "return_variant": "Return Type",
    "latest_value": "Last",
    "ytd": "YTD",
    "since_event": WINDOW_LABELS["since_event"],
    "yoy": "YoY",
    "ytd_bps": "YTD",
    "since_event_bps": WINDOW_LABELS["since_event_bps"],
    "yoy_bps": "YoY",
    "data_as_of": "Data as of",
    "freshness_status": "Freshness",
    "lag_days": "Lag (days)",
    "snapshot_date": "Snapshot Date",
}


def _options(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    values = frame[column].dropna().astype(str).str.strip()
    values = values[values != ""]
    return sorted(values.unique().tolist())


def _coalesced_options(bundle: DashboardBundle, field: str) -> list[str]:
    snapshot_values = _options(bundle.snapshot, field)
    if snapshot_values:
        return snapshot_values
    return _options(bundle.asset_master, field)


def build_page_filters(bundle: DashboardBundle, page_name: str, key_prefix: str) -> DashboardFilters:
    fields = PAGE_FILTERS.get(page_name, [])
    st.sidebar.header("Filters")
    selections: dict[str, list[str]] = {key: [] for key in FILTER_CONFIG}

    for field in fields:
        config = FILTER_CONFIG[field]
        options = _coalesced_options(bundle, config["field"])
        if not options:
            continue
        selections[field] = st.sidebar.multiselect(
            config["label"],
            options=options,
            default=[],
            placeholder="All",
            key=f"{key_prefix}_{field}",
        )

    return DashboardFilters(
        page_fields=fields,
        asset_classes=selections["asset_class"],
        dm_em_flags=selections["dm_em_flag"],
        countries=selections["country"],
        regions=selections["region"],
        return_variants=selections["return_variant"],
        sectors=selections["sector_name"],
        commodity_categories=selections["commodity_category"],
        sub_asset_classes=selections["sub_asset_class"],
    )


def build_reference_date_control(bundle: DashboardBundle, key_prefix: str) -> tuple[pd.Timestamp, str]:
    default_reference_date = resolve_reference_date(bundle.events)
    available_dates = pd.Series(dtype="datetime64[ns]")
    if not bundle.prices.empty and "date" in bundle.prices.columns:
        available_dates = pd.to_datetime(bundle.prices["date"], errors="coerce").dropna()

    min_date = available_dates.min() if not available_dates.empty else default_reference_date
    max_date = available_dates.max() if not available_dates.empty else default_reference_date
    selected_default = default_reference_date
    if pd.notna(min_date) and selected_default < min_date:
        selected_default = min_date
    if pd.notna(max_date) and selected_default > max_date:
        selected_default = max_date

    date_input_kwargs = {
        "label": "Reference date",
        "value": selected_default.date(),
        "key": "reference_date",
        "help": "Used for the middle performance window and reference-date normalization.",
    }
    if pd.notna(min_date):
        date_input_kwargs["min_value"] = min_date.date()
    if pd.notna(max_date):
        date_input_kwargs["max_value"] = max_date.date()

    selected_value = st.sidebar.date_input(**date_input_kwargs)
    reference_date = pd.Timestamp(selected_value)
    reference_label = format_reference_window_label(reference_date, default_reference_date=default_reference_date)
    st.sidebar.caption(f"Middle window: {reference_label}")
    return reference_date, reference_label


def format_metric_option(metric: str, reference_window_label: str | None = None) -> str:
    return metric_label(metric, reference_window_label)


def render_empty_state(title: str, body: str) -> None:
    st.info(f"{title} {body}")


def render_page_header(title: str, subtitle: str, snapshot: pd.DataFrame) -> None:
    st.title(title)
    latest = "-"
    if not snapshot.empty and "snapshot_date" in snapshot.columns:
        latest = format_date(pd.to_datetime(snapshot["snapshot_date"], errors="coerce").max())
    left, right = st.columns([4, 1])
    with left:
        st.caption(subtitle)
    with right:
        st.caption(f"Snapshot date {latest}")


def render_filter_chips(filters: DashboardFilters) -> None:
    selected: list[str] = []
    if filters.asset_classes:
        selected.append("Asset class: " + ", ".join(filters.asset_classes))
    if filters.dm_em_flags:
        selected.append("DM / EM: " + ", ".join(filters.dm_em_flags))
    if filters.countries:
        selected.append("Country: " + ", ".join(filters.countries))
    if filters.regions:
        selected.append("Region: " + ", ".join(filters.regions))
    if filters.return_variants:
        selected.append("Return type: " + ", ".join(filters.return_variants))
    if filters.sectors:
        selected.append("Sector: " + ", ".join(filters.sectors))
    if filters.commodity_categories:
        selected.append("Commodity category: " + ", ".join(filters.commodity_categories))
    if filters.sub_asset_classes:
        selected.append("Bond type: " + ", ".join(filters.sub_asset_classes))

    if selected:
        st.caption(" | ".join(selected))


def render_kpi_cards(cards: list[object]) -> None:
    if not cards:
        return
    columns = st.columns(len(cards))
    for column, card in zip(columns, cards):
        with column:
            st.markdown(f"**{card.title}**")
            st.markdown(f"<div style='font-size:1.55rem;font-weight:700'>{card.value}</div>", unsafe_allow_html=True)
            st.caption(card.detail)


def _format_metric_series(series: pd.Series, column: str) -> pd.Series:
    if column.endswith("_bps"):
        return series.map(format_bps)
    if column in {"ytd", "since_event", "yoy"}:
        return series.map(format_percent)
    if column == "latest_value":
        return series.map(format_level)
    if column in {"snapshot_date", "data_as_of"}:
        return series.map(format_date)
    if column == "freshness_status":
        return series.map(format_freshness)
    if column == "lag_days":
        numeric = pd.to_numeric(series, errors="coerce")
        return numeric.map(lambda value: "-" if pd.isna(value) else f"{int(value)}")
    return series.fillna("")


def prepare_user_table(frame: pd.DataFrame, columns: list[str], column_labels: dict[str, str] | None = None) -> pd.DataFrame:
    present = [column for column in columns if column in frame.columns]
    table = frame[present].copy()
    for column in present:
        if column in {"latest_value", "ytd", "since_event", "yoy", "ytd_bps", "since_event_bps", "yoy_bps", "snapshot_date", "data_as_of", "freshness_status", "lag_days"}:
            table[column] = _format_metric_series(table[column], column)
        else:
            table[column] = table[column].fillna("")
    labels = {column: TABLE_LABELS.get(column, column) for column in present}
    if column_labels:
        labels.update({column: label for column, label in column_labels.items() if column in present})
    return table.rename(columns=labels)


def render_summary_table(title: str, frame: pd.DataFrame, columns: list[str], column_labels: dict[str, str] | None = None) -> None:
    st.subheader(title)
    if frame.empty:
        st.caption("No rows to display.")
        return
    st.dataframe(prepare_user_table(frame, columns, column_labels=column_labels), use_container_width=True, hide_index=True)


def _prepare_ranked_bar_chart(frame: pd.DataFrame, metric: str, label_column: str, limit: int) -> tuple[pd.DataFrame, list[str]]:
    chart_frame = (
        frame[[label_column, metric]]
        .dropna()
        .sort_values(metric, ascending=False, kind="mergesort")
        .head(limit)
        .copy()
    )
    label_order = chart_frame[label_column].astype(str).tolist()
    return chart_frame, label_order


def render_ranked_bars(
    title: str,
    frame: pd.DataFrame,
    metric: str,
    label_column: str = "display_name",
    limit: int = 10,
    metric_title: str | None = None,
) -> None:
    st.subheader(title)
    if frame.empty or metric not in frame.columns or label_column not in frame.columns:
        st.caption("No rows to display.")
        return

    chart_frame, label_order = _prepare_ranked_bar_chart(frame, metric, label_column, limit)
    if chart_frame.empty:
        st.caption("No rows to display.")
        return

    chart_frame["direction"] = chart_frame[metric].apply(lambda value: "Positive" if value >= 0 else "Negative")
    metric_format = ".1f" if metric.endswith("_bps") else ".1%"
    resolved_metric_title = metric_title or WINDOW_LABELS.get(metric, metric)
    bar = (
        alt.Chart(chart_frame)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X(
                f"{metric}:Q",
                title=resolved_metric_title,
                axis=alt.Axis(format=metric_format, labelColor="#5B6B66", titleColor="#5B6B66", gridColor="#D7E0DB"),
            ),
            y=alt.Y(
                f"{label_column}:N",
                sort=label_order,
                title=None,
                axis=alt.Axis(labelColor="#5B6B66"),
            ),
            color=alt.Color(
                "direction:N",
                scale=alt.Scale(domain=["Positive", "Negative"], range=["#0B6E4F", "#C84B31"]),
                legend=None,
            ),
            tooltip=[alt.Tooltip(f"{label_column}:N", title="Series"), alt.Tooltip(f"{metric}:Q", format=metric_format, title=resolved_metric_title)],
        )
        .properties(height=max(260, limit * 28))
    )
    zero_rule = alt.Chart(pd.DataFrame({metric: [0]})).mark_rule(color="#9AA7A2").encode(x=f"{metric}:Q")
    labels = (
        alt.Chart(chart_frame)
        .mark_text(align="left", baseline="middle", dx=5, fontSize=11)
        .encode(
            x=alt.X(f"{metric}:Q"),
            y=alt.Y(f"{label_column}:N", sort=label_order),
            text=alt.Text(f"{metric}:Q", format=metric_format),
        )
    )
    st.altair_chart(bar + zero_rule + labels, use_container_width=True)


def render_heatmap(title: str, heatmap: pd.DataFrame, percent: bool = True, column_labels: dict[str, str] | None = None) -> None:
    st.subheader(title)
    if heatmap.empty:
        st.caption("No heatmap values available.")
        return

    display_heatmap = heatmap.rename(columns=column_labels or {})
    reset_heatmap = display_heatmap.reset_index()
    if "Group" not in reset_heatmap.columns:
        first_column = reset_heatmap.columns[0]
        reset_heatmap = reset_heatmap.rename(columns={first_column: "Group"})

    chart_frame = reset_heatmap.melt(id_vars="Group", var_name="Metric", value_name="Value").dropna()
    if chart_frame.empty:
        st.caption("No heatmap values available.")
        return

    chart = (
        alt.Chart(chart_frame)
        .mark_rect(cornerRadius=4)
        .encode(
            x=alt.X("Metric:N", title=None),
            y=alt.Y("Group:N", title=None),
            color=alt.Color("Value:Q", scale=alt.Scale(scheme="redyellowgreen", reverse=False), legend=None),
            tooltip=[
                alt.Tooltip("Group:N"),
                alt.Tooltip("Metric:N"),
                alt.Tooltip("Value:Q", format=".1%" if percent else ".1f"),
            ],
        )
        .properties(height=max(220, heatmap.shape[0] * 28))
    )
    text = alt.Chart(chart_frame).mark_text(fontSize=11).encode(
        x="Metric:N",
        y="Group:N",
        text=alt.Text("Value:Q", format=".1%" if percent else ".1f"),
        color=alt.value("#10221A"),
    )
    st.altair_chart(chart + text, use_container_width=True)


def render_normalized_line_chart(title: str, history: pd.DataFrame, event_date: pd.Timestamp | None = None) -> None:
    st.subheader(title)
    if history.empty:
        st.caption("No history available for the current selection.")
        return

    base = (
        alt.Chart(history)
        .mark_line(strokeWidth=2.2)
        .encode(
            x=alt.X(
                "date:T",
                title="Date",
                axis=alt.Axis(
                    format="%b %Y",
                    labelAngle=-20,
                    tickCount=8,
                    labelColor="#5B6B66",
                    titleColor="#5B6B66",
                    grid=False,
                ),
            ),
            y=alt.Y(
                "normalized:Q",
                title="Normalized to 100",
                axis=alt.Axis(labelColor="#5B6B66", titleColor="#5B6B66", gridColor="#D7E0DB"),
            ),
            color=alt.Color("display_name:N", title=None),
            tooltip=[
                alt.Tooltip("display_name:N", title="Series"),
                alt.Tooltip("date:T", title="Date"),
                alt.Tooltip("normalized:Q", format=".1f", title="Normalized"),
            ],
        )
        .properties(height=360)
    )
    layers = [base]
    if event_date is not None:
        rule_frame = pd.DataFrame({"event_date": [event_date]})
        rule = alt.Chart(rule_frame).mark_rule(color="#5B6B66", strokeDash=[6, 5]).encode(x="event_date:T")
        layers.append(rule)
    chart = alt.layer(*layers).resolve_scale(color="independent")
    st.altair_chart(chart, use_container_width=True)


def render_page_links() -> None:
    columns = st.columns(4)
    columns[0].page_link("pages/02_Equities.py", label="View Equities")
    columns[1].page_link("pages/03_Bonds.py", label="View Bonds")
    columns[2].page_link("pages/04_Commodities.py", label="View Commodities")
    columns[3].page_link("pages/05_Compare.py", label="View Compare")
