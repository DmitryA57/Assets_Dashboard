from __future__ import annotations

import pandas as pd

from src.compute_snapshot import build_snapshot


def test_build_snapshot_returns_empty_frame_without_prices() -> None:
    asset_master = pd.DataFrame(
        [{"asset_id": "spx", "series_type": "equity_price_index"}]
    )
    prices = pd.DataFrame(columns=["date", "asset_id", "value", "source_timestamp"])
    events = pd.DataFrame(columns=["event_id", "event_name", "event_date", "description", "is_active"])

    snapshot = build_snapshot(asset_master=asset_master, prices=prices, events=events)
    assert snapshot.empty


def test_build_snapshot_computes_yield_bps() -> None:
    asset_master = pd.DataFrame(
        [{"asset_id": "us10y", "series_type": "government_yield"}]
    )
    prices = pd.DataFrame(
        [
            {"date": "2025-12-31", "asset_id": "us10y", "value": 4.0, "source_timestamp": "2025-12-31T20:00:00"},
            {"date": "2026-02-27", "asset_id": "us10y", "value": 4.1, "source_timestamp": "2026-02-27T20:00:00"},
            {"date": "2026-04-01", "asset_id": "us10y", "value": 4.5, "source_timestamp": "2026-04-01T20:00:00"},
            {"date": "2025-04-01", "asset_id": "us10y", "value": 3.9, "source_timestamp": "2025-04-01T20:00:00"},
        ]
    )
    prices["date"] = pd.to_datetime(prices["date"])
    prices["source_timestamp"] = pd.to_datetime(prices["source_timestamp"])
    events = pd.DataFrame(
        [
            {
                "event_id": "IRAN_US_ISRAEL_OP_START",
                "event_name": "Event",
                "event_date": "2026-02-28",
                "description": "Test event",
                "is_active": True,
            }
        ]
    )
    events["event_date"] = pd.to_datetime(events["event_date"])

    snapshot = build_snapshot(asset_master=asset_master, prices=prices, events=events)
    row = snapshot.iloc[0]

    assert str(row["snapshot_date"].date()) == "2026-04-01"
    assert str(row["data_as_of"].date()) == "2026-04-01"
    assert row["lag_days"] == 0
    assert row["freshness_status"] == "Fresh"
    assert bool(row["comparison_eligible"]) is True
    assert row["ytd_bps"] == 50.0
    assert row["since_event_bps"] == 40.0
    assert row["yoy_bps"] == 60.0


def test_build_snapshot_marks_stale_series_as_ineligible() -> None:
    asset_master = pd.DataFrame(
        [
            {"asset_id": "brent", "series_type": "commodity_benchmark_price"},
            {"asset_id": "other", "series_type": "commodity_benchmark_price"},
        ]
    )
    prices = pd.DataFrame(
        [
            {"date": "2025-12-31", "asset_id": "brent", "value": 70.0, "source_timestamp": "2026-01-01T20:00:00"},
            {"date": "2026-01-05", "asset_id": "brent", "value": 72.0, "source_timestamp": "2026-01-05T20:00:00"},
            {"date": "2026-04-10", "asset_id": "other", "value": 100.0, "source_timestamp": "2026-04-10T20:00:00"},
        ]
    )
    prices["date"] = pd.to_datetime(prices["date"])
    prices["source_timestamp"] = pd.to_datetime(prices["source_timestamp"])
    events = pd.DataFrame(columns=["event_id", "event_name", "event_date", "description", "is_active"])

    snapshot = build_snapshot(asset_master=asset_master, prices=prices, events=events)
    row = snapshot.iloc[0]

    assert str(row["snapshot_date"].date()) == "2026-04-10"
    assert str(row["data_as_of"].date()) == "2026-01-05"
    assert row["lag_days"] == 95
    assert row["freshness_status"] == "Stale"
    assert bool(row["comparison_eligible"]) is False
