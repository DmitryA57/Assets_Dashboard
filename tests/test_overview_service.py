from __future__ import annotations

import pandas as pd

from src.services.overview_service import exclude_russia_rows, load_russia_overview_bundle


def test_exclude_russia_rows_removes_russia_by_country_region_source_and_asset_prefix() -> None:
    frame = pd.DataFrame(
        [
            {"asset_id": "eq_spx", "country": "United States", "region": "North America", "source": "Bloomberg"},
            {"asset_id": "ru_eq_imoex", "country": "Russia", "region": "Russia", "source": "T-Invest API"},
            {"asset_id": "eq_other", "country": "", "region": "Russia", "source": "Bloomberg"},
        ]
    )

    filtered = exclude_russia_rows(frame)

    assert filtered["asset_id"].tolist() == ["eq_spx"]


def test_load_russia_overview_bundle_keeps_only_rtsi() -> None:
    bundle = load_russia_overview_bundle(pd.Timestamp("2026-02-28"))

    assert bundle.snapshot["asset_id"].tolist() == ["ru_eq_rtsi"]
