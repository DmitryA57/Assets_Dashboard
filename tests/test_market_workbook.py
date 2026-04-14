from __future__ import annotations

import tempfile
from pathlib import Path

from openpyxl import Workbook

from src.market_workbook import COMMODITY_CATEGORY_BY_NAME, parse_market_workbook


def test_parse_market_workbook_extracts_equities_and_commodities() -> None:
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as handle:
        workbook_path = Path(handle.name)

    workbook = Workbook()
    equity = workbook.active
    equity.title = "Equity_data"
    equity_rows = [
        [],
        [None, "END DATE", 46125],
        [],
        [None, "S&P500", None, None, "Nasdaq Composite"],
        [],
        ["Security", "SPX Index", None, "Security", "CCMP Index"],
        ["Start Date", 40544, None, "Start Date", 40544],
        ["End Date", 46125, None, "End Date", 46125],
        ["Period", "D", None, "Period", "D"],
        ["Currency", "USD", None, "Currency", "USD"],
        [],
        ["Date", "PX_LAST", None, "Date", "PX_LAST"],
        [46125, 6816.89, None, 46125, 19000.0],
        [46122, 6824.66, None, 46122, 18900.0],
    ]
    for row in equity_rows:
        equity.append(row)

    commodity = workbook.create_sheet("Commodity_data")
    commodity_rows = [
        [],
        [None, "END DATE", 46125],
        [],
        [None, "Copper", None, None, "Gold"],
        [],
        ["Security", "LMCADY LME Comdty", None, "Security", "XAU BGN Curncy"],
        ["Start Date", 40544, None, "Start Date", 40544],
        ["End Date", 46125, None, "End Date", 46125],
        ["Period", "D", None, "Period", "D"],
        ["Currency", "USD", None, "Currency", "USD"],
        [],
        ["Date", "PX_LAST", None, "Date", "PX_LAST"],
        [46125, 9000.0, None, 46125, 3200.0],
        [46122, 8900.0, None, 46122, 3150.0],
    ]
    for row in commodity_rows:
        commodity.append(row)

    workbook.save(workbook_path)

    try:
        asset_master, prices = parse_market_workbook(workbook_path)
    finally:
        workbook_path.unlink(missing_ok=True)

    assert len(asset_master) == 4
    assert len(prices) == 8
    assert set(asset_master["asset_class"]) == {"Equities", "Commodities"}
    assert "Base metals" in asset_master["commodity_category"].tolist()
    assert "Precious metals" in asset_master["commodity_category"].tolist()


def test_battery_materials_are_folded_into_base_metals() -> None:
    assert COMMODITY_CATEGORY_BY_NAME["Cobalt"] == "Base metals"
    assert COMMODITY_CATEGORY_BY_NAME["Lithium, LCE"] == "Base metals"
