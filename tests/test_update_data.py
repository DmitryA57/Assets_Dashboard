from __future__ import annotations

import pandas as pd

from src.update_data import merge_prices, normalize_prices_frame


def test_normalize_prices_frame_fills_missing_timestamp() -> None:
    frame = pd.DataFrame(
        [
            {"date": "2026-01-01", "asset_id": "spx", "value": "100.5"},
        ]
    )

    normalized = normalize_prices_frame(frame)

    assert list(normalized.columns) == ["date", "asset_id", "value", "source_timestamp"]
    assert normalized.iloc[0]["asset_id"] == "spx"
    assert normalized.iloc[0]["value"] == 100.5
    assert pd.notna(normalized.iloc[0]["source_timestamp"])


def test_merge_prices_keeps_latest_timestamp_per_asset_date() -> None:
    existing = pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "asset_id": "spx",
                "value": 100.0,
                "source_timestamp": "2026-01-01T10:00:00",
            }
        ]
    )
    incoming = pd.DataFrame(
        [
            {
                "date": "2026-01-01",
                "asset_id": "spx",
                "value": 101.0,
                "source_timestamp": "2026-01-01T11:00:00",
            }
        ]
    )

    merged = merge_prices(existing, incoming)

    assert len(merged) == 1
    assert merged.iloc[0]["value"] == 101.0

