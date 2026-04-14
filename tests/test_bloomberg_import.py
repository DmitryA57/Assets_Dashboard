from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from src.update_data import parse_bloomberg_history_export, parse_bloomberg_history_workbook


def test_parse_bloomberg_history_export_maps_px_last_and_total_return() -> None:
    asset_master = pd.DataFrame(
        [
            {"asset_id": "spx_pr", "bbg_ticker": "SPX Index", "source_field": "PX_LAST"},
            {"asset_id": "spx_tr", "bbg_ticker": "SPX Index", "source_field": "TOT_RETURN_INDEX_GROSS_DVDS"},
        ]
    )

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as handle:
        handle.write(
            "\n".join(
                [
                    "Security,SPX Index,,,,,,",
                    "Start Date,12/31/2024 0:00,,,,,,",
                    "End Date,4/10/2026 0:00,,,,,,",
                    "Period,D,,,,,,",
                    "Currency,USD,,,,,,",
                    ",,,,,,,",
                    "Date,TOT_RETURN_INDEX_GROSS_DVDS,Change,% Change,PX_LAST,Change,% Change",
                    "10/04/2026,6925.8442,-7.2,-0.1,6816.89,-7.7,-0.1",
                    "09/04/2026,6933.0462,42.9,0.6,6824.66,41.8,0.6",
                    "31/12/2024,5881.63,,,5881.63,,",
                ]
            )
        )
        temp_path = Path(handle.name)

    try:
        parsed = parse_bloomberg_history_export(temp_path, asset_master)
    finally:
        temp_path.unlink(missing_ok=True)

    assert len(parsed) == 6
    assert set(parsed["asset_id"]) == {"spx_pr", "spx_tr"}
    assert parsed.loc[parsed["asset_id"] == "spx_pr", "value"].max() == 6824.66
    assert parsed.loc[parsed["asset_id"] == "spx_tr", "value"].max() == 6933.0462


def test_parse_bloomberg_history_workbook_reads_multiple_sheets() -> None:
    asset_master = pd.DataFrame(
        [
            {"asset_id": "spx_pr", "bbg_ticker": "SPX Index", "source_field": "PX_LAST"},
            {"asset_id": "spx_tr", "bbg_ticker": "SPX Index", "source_field": "TOT_RETURN_INDEX_GROSS_DVDS"},
            {"asset_id": "kospi_pr", "bbg_ticker": "KOSPI Index", "source_field": "PX_LAST"},
            {"asset_id": "kospi_tr", "bbg_ticker": "KOSPI Index", "source_field": "TOT_RETURN_INDEX_GROSS_DVDS"},
        ]
    )

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as handle:
        workbook_path = Path(handle.name)

    workbook = Workbook()
    ws1 = workbook.active
    ws1.title = "S&P"
    rows_1 = [
        ["Security", "SPX Index"],
        ["Start Date", "2024-12-31 00:00:00"],
        ["End Date", "2026-04-10 00:00:00"],
        ["Period", "D"],
        ["Currency", "USD"],
        [],
        ["Date", "TOT_RETURN_INDEX_GROSS_DVDS", "Change", "% Change", "PX_LAST", "Change", "% Change"],
        ["2026-04-10 00:00:00", 6925.8442, -7.2, -0.1, 6816.89, -7.7, -0.1],
        ["2024-12-31 00:00:00", 5881.63, None, None, 5881.63, None, None],
    ]
    for row in rows_1:
        ws1.append(row)

    ws2 = workbook.create_sheet("Kospi")
    rows_2 = [
        ["Security", "KOSPI Index"],
        ["Start Date", "2024-12-31 00:00:00"],
        ["End Date", "2026-04-10 00:00:00"],
        ["Period", "D"],
        ["Currency", "USD"],
        [],
        ["Date", "TOT_RETURN_INDEX_GROSS_DVDS", "Change", "% Change", "PX_LAST", "Change", "% Change"],
        ["2026-04-10 00:00:00", 4.0516, 0.0333, 0.82, 3.95111, 0.03249, 0.82],
        ["2024-12-31 00:00:00", 3.2000, None, None, 3.10000, None, None],
    ]
    for row in rows_2:
        ws2.append(row)
    workbook.save(workbook_path)

    try:
        parsed = parse_bloomberg_history_workbook(workbook_path, asset_master)
    finally:
        workbook_path.unlink(missing_ok=True)

    assert len(parsed) == 8
    assert set(parsed["asset_id"]) == {"spx_pr", "spx_tr", "kospi_pr", "kospi_tr"}
