from __future__ import annotations

import pandas as pd

from src.services.overview_service import exclude_russia_rows


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
