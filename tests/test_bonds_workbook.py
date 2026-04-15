from __future__ import annotations

import pandas as pd

from src.bonds_workbook import parse_bonds_sheet


def test_parse_bonds_sheet_imports_requested_bonds_and_excludes_loans() -> None:
    sheet = pd.DataFrame([[pd.NA] * 6 for _ in range(14)])
    sheet.iat[3, 0] = "Global Aggregate"
    sheet.iat[5, 0] = "Security"
    sheet.iat[5, 1] = "LEGATRUU Index"
    sheet.iat[3, 3] = "US Lev Loan Index"
    sheet.iat[5, 3] = "Security"
    sheet.iat[5, 4] = "I38932US Index"
    sheet.iat[12, 0] = "2026-04-14"
    sheet.iat[12, 1] = 504.5
    sheet.iat[12, 3] = "2026-04-14"
    sheet.iat[12, 4] = 100.0

    asset_master, prices = parse_bonds_sheet(sheet, pd.Timestamp("2026-04-15"))

    assert asset_master["asset_id"].tolist() == ["bond_legatruu"]
    assert asset_master.iloc[0]["display_name"] == "Global Aggregate"
    assert asset_master.iloc[0]["sub_asset_class"] == "Global"
    assert "loan" not in " ".join(asset_master["display_name"].str.lower())
    assert prices["asset_id"].tolist() == ["bond_legatruu"]
