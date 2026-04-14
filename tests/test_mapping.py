from __future__ import annotations

import pandas as pd

from src.constants import ASSET_MASTER_COLUMNS
from src.validators import validate_asset_master, validate_snapshot_freshness


def test_validate_asset_master_accepts_expected_columns() -> None:
    frame = pd.DataFrame(columns=ASSET_MASTER_COLUMNS)
    assert validate_asset_master(frame) == []


def test_validate_asset_master_detects_duplicates() -> None:
    frame = pd.DataFrame(
        [
            {column: None for column in ASSET_MASTER_COLUMNS},
            {column: None for column in ASSET_MASTER_COLUMNS},
        ]
    )
    frame["asset_id"] = ["dup", "dup"]
    issues = validate_asset_master(frame)
    assert any("duplicate" in issue for issue in issues)


def test_validate_snapshot_freshness_flags_old_rows() -> None:
    snapshot = pd.DataFrame(
        [
            {"asset_id": "fresh", "data_as_of": "2026-04-13", "lag_days": 0},
            {"asset_id": "stale", "data_as_of": "2026-01-01", "lag_days": 103},
        ]
    )

    issues = validate_snapshot_freshness(snapshot, max_staleness_days=30)

    assert len(issues) == 1
    assert "stale" in issues[0]
