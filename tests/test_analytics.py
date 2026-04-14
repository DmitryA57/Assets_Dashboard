from __future__ import annotations

import pandas as pd

from src.analytics import apply_reference_window, build_heatmap_matrix, comparison_universe, format_reference_window_label, top_bottom


def test_top_bottom_sorts_descending_and_ascending() -> None:
    frame = pd.DataFrame(
        [
            {"display_name": "A", "ytd": 0.10},
            {"display_name": "B", "ytd": -0.20},
            {"display_name": "C", "ytd": 0.30},
        ]
    )

    top, bottom = top_bottom(frame, metric="ytd", n=1)

    assert top.iloc[0]["display_name"] == "C"
    assert bottom.iloc[0]["display_name"] == "B"


def test_build_heatmap_matrix_averages_by_group() -> None:
    frame = pd.DataFrame(
        [
            {"asset_class": "Equities", "ytd": 0.10},
            {"asset_class": "Equities", "ytd": 0.30},
            {"asset_class": "Bonds", "ytd": 0.05},
        ]
    )

    heatmap = build_heatmap_matrix(frame, metric="ytd", group_by="asset_class")

    assert heatmap.loc["ytd", "Equities"] == 0.20
    assert heatmap.loc["ytd", "Bonds"] == 0.05


def test_build_heatmap_matrix_preserves_group_name() -> None:
    frame = pd.DataFrame(
        [
            {"country": "United States", "ytd": 0.10},
            {"country": "Japan", "ytd": 0.20},
        ]
    )

    heatmap = build_heatmap_matrix(frame, metric="ytd", group_by="country")

    assert heatmap.columns.name == "country"


def test_comparison_universe_excludes_ineligible_rows() -> None:
    frame = pd.DataFrame(
        [
            {"asset_id": "fresh", "comparison_eligible": True},
            {"asset_id": "stale", "comparison_eligible": False},
        ]
    )

    filtered = comparison_universe(frame)

    assert filtered["asset_id"].tolist() == ["fresh"]


def test_format_reference_window_label_uses_event_name_for_default_date() -> None:
    label = format_reference_window_label(pd.Timestamp("2026-02-28"))
    assert label == "US vs Iran conflict (28.02.2026)"


def test_apply_reference_window_recomputes_percent_and_bps_metrics() -> None:
    frame = pd.DataFrame(
        [
            {"asset_id": "spx", "latest_value": 120.0, "series_type": "equity_price_index", "since_event": None, "since_event_bps": None, "base_event": None},
            {"asset_id": "us10y", "latest_value": 4.5, "series_type": "government_yield", "since_event": None, "since_event_bps": None, "base_event": None},
        ]
    )
    prices = pd.DataFrame(
        [
            {"asset_id": "spx", "date": "2026-02-28", "value": 100.0, "source_timestamp": "2026-02-28T18:00:00"},
            {"asset_id": "spx", "date": "2026-04-01", "value": 120.0, "source_timestamp": "2026-04-01T18:00:00"},
            {"asset_id": "us10y", "date": "2026-02-28", "value": 4.1, "source_timestamp": "2026-02-28T18:00:00"},
            {"asset_id": "us10y", "date": "2026-04-01", "value": 4.5, "source_timestamp": "2026-04-01T18:00:00"},
        ]
    )

    updated = apply_reference_window(frame, prices, pd.Timestamp("2026-02-28"))

    spx = updated.loc[updated["asset_id"] == "spx"].iloc[0]
    us10y = updated.loc[updated["asset_id"] == "us10y"].iloc[0]

    assert spx["since_event"] == 0.2
    assert pd.isna(spx["since_event_bps"])
    assert us10y["since_event_bps"] == 40.0
    assert pd.isna(us10y["since_event"])
