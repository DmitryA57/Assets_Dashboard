from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from config import ASSET_MASTER_PATH, PRICES_PATH
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS


EQUITY_METADATA_BY_TICKER = {
    "SPX Index": {"asset_name": "S&P 500", "country": "United States", "region": "North America", "dm_em_flag": "DM"},
    "CCMP Index": {"asset_name": "Nasdaq Composite", "country": "United States", "region": "North America", "dm_em_flag": "DM"},
    "SPTSX Index": {"asset_name": "S&P/TSX Composite", "country": "Canada", "region": "North America", "dm_em_flag": "DM"},
    "MEXBOL Index": {"asset_name": "BMV IPC", "country": "Mexico", "region": "Latin America", "dm_em_flag": "EM"},
    "IBOV Index": {"asset_name": "Ibovespa", "country": "Brazil", "region": "Latin America", "dm_em_flag": "EM"},
    "IPSA Index": {"asset_name": "S&P IPSA", "country": "Chile", "region": "Latin America", "dm_em_flag": "EM"},
    "COLCAP Index": {"asset_name": "MSCI COLCAP", "country": "Colombia", "region": "Latin America", "dm_em_flag": "EM"},
    "MERVAL Index": {"asset_name": "S&P MERVAL", "country": "Argentina", "region": "Latin America", "dm_em_flag": "EM"},
    "SX5E Index": {"asset_name": "EURO STOXX 50", "country": "Europe", "region": "Europe", "dm_em_flag": "DM"},
    "UKX Index": {"asset_name": "FTSE 100", "country": "United Kingdom", "region": "Europe", "dm_em_flag": "DM"},
    "CAC Index": {"asset_name": "CAC 40", "country": "France", "region": "Europe", "dm_em_flag": "DM"},
    "DAX Index": {"asset_name": "DAX", "country": "Germany", "region": "Europe", "dm_em_flag": "DM"},
    "IBEX Index": {"asset_name": "IBEX 35", "country": "Spain", "region": "Europe", "dm_em_flag": "DM"},
    "FTSEMIB Index": {"asset_name": "FTSE MIB", "country": "Italy", "region": "Europe", "dm_em_flag": "DM"},
    "AEX Index": {"asset_name": "AEX", "country": "Netherlands", "region": "Europe", "dm_em_flag": "DM"},
    "OMX Index": {"asset_name": "OMX Stockholm 30", "country": "Sweden", "region": "Europe", "dm_em_flag": "DM"},
    "SMI Index": {"asset_name": "Swiss Market Index", "country": "Switzerland", "region": "Europe", "dm_em_flag": "DM"},
    "SXXP Index": {"asset_name": "STOXX Europe 600", "country": "Europe", "region": "Europe", "dm_em_flag": "DM"},
    "NKY Index": {"asset_name": "Nikkei 225", "country": "Japan", "region": "Asia Pacific", "dm_em_flag": "DM"},
    "HSI Index": {"asset_name": "Hang Seng Index", "country": "Hong Kong", "region": "Asia", "dm_em_flag": "EM"},
    "SHSZ300 Index": {"asset_name": "CSI 300", "country": "China", "region": "Asia", "dm_em_flag": "EM"},
    "AS51 Index": {"asset_name": "S&P/ASX 200", "country": "Australia", "region": "Asia Pacific", "dm_em_flag": "DM"},
    "KOSPI Index": {"asset_name": "KOSPI", "country": "South Korea", "region": "Asia", "dm_em_flag": "EM"},
    "NIFTY Index": {"asset_name": "Nifty 50", "country": "India", "region": "Asia", "dm_em_flag": "EM"},
    "TWSE Index": {"asset_name": "Taiwan Stock Exchange Index", "country": "Taiwan", "region": "Asia", "dm_em_flag": "EM"},
    "JCI Index": {"asset_name": "IDX Composite", "country": "Indonesia", "region": "Asia", "dm_em_flag": "EM"},
    "STI Index": {"asset_name": "Straits Times Index", "country": "Singapore", "region": "Asia", "dm_em_flag": "DM"},
}


COMMODITY_CATEGORY_BY_NAME = {
    "HCC FOB Australia Swap M1": "Ferrous / Steel chain",
    "China HRC": "Ferrous / Steel chain",
    "Iron Ore 62%": "Ferrous / Steel chain",
    "HRC US": "Ferrous / Steel chain",
    "Copper": "Base metals",
    "Zinc": "Base metals",
    "Aluminium": "Base metals",
    "Nickel LME": "Base metals",
    "Cobalt": "Base metals",
    "Lithium, LCE": "Base metals",
    "Gold": "Precious metals",
    "Silver": "Precious metals",
    "Platinum": "Precious metals",
    "Palladium": "Precious metals",
    "Brent": "Energy - crude oil & refined products",
    "WTI": "Energy - crude oil & refined products",
    "Urals": "Energy - crude oil & refined products",
    "Asia Naphtha FOB Singapore Cargo Spot": "Energy - crude oil & refined products",
    "European Gasoil 0.1% FOB Med Cargo Spot": "Energy - crude oil & refined products",
    "European ULSD 10ppm FOB Med Cargo Spot": "Energy - crude oil & refined products",
    "Fuel Oil 3,5% Rotterdam swap": "Energy - crude oil & refined products",
    "TTF spot": "Energy - gas, LNG, coal",
    "TTF year-ahead": "Energy - gas, LNG, coal",
    "LNG Australia to China": "Energy - gas, LNG, coal",
    "Henry Hub": "Energy - gas, LNG, coal",
    "NEWC index": "Energy - gas, LNG, coal",
    "API2": "Energy - gas, LNG, coal",
    "Uranium": "Energy materials",
    "Urea Black sea": "Fertilizers",
    "DAP US Gulf export price": "Fertilizers",
    "Potash Brazil CFR": "Fertilizers",
    "Corn": "Agriculture",
    "SOY": "Agriculture",
    "Wheat": "Agriculture",
}


def _clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _slugify(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return re.sub(r"_+", "_", lowered)


def _excel_date(value: object) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    if isinstance(value, (int, float)):
        return pd.Timestamp("1899-12-30") + pd.to_timedelta(int(value), unit="D")
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _extract_name(name_row: pd.Series, start_col: int, next_start: int | None) -> str:
    search_end = next_start if next_start is not None else len(name_row)
    for column in range(start_col, min(search_end, start_col + 4)):
        candidate = _clean_text(name_row.iloc[column])
        if candidate:
            return candidate
    return ""


def _extract_unit(unit_row: pd.Series, start_col: int, next_start: int | None) -> str:
    search_end = next_start if next_start is not None else len(unit_row)
    for column in range(start_col, min(search_end, start_col + 4)):
        candidate = _clean_text(unit_row.iloc[column])
        if candidate and candidate.upper() not in {"USD"}:
            return candidate
    return ""


def _security_starts(security_row: pd.Series) -> list[int]:
    return [index for index, value in security_row.items() if _clean_text(value) == "Security"]


def _empty_asset_master() -> pd.DataFrame:
    return pd.DataFrame(columns=ASSET_MASTER_COLUMNS)


def _empty_prices() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICES_COLUMNS)


def _build_equity_record(display_name: str, ticker: str) -> dict[str, object]:
    metadata = EQUITY_METADATA_BY_TICKER.get(ticker.strip(), {})
    asset_name = metadata.get("asset_name", display_name)
    asset_id = f"eq_{_slugify(asset_name)}"
    return {
        "asset_id": asset_id,
        "asset_name": asset_name,
        "display_name": display_name,
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "Equities",
        "sub_asset_class": "Headline Index",
        "country": metadata.get("country", ""),
        "region": metadata.get("region", ""),
        "dm_em_flag": metadata.get("dm_em_flag", ""),
        "commodity_category": "",
        "sector_name": "",
        "series_type": "equity_price_index",
        "return_variant": "Price Return",
        "currency": "USD",
        "unit": "index points",
        "is_active": True,
        "notes": "Rebuilt from Assets_data.xlsx",
    }


def _build_commodity_record(display_name: str, ticker: str, unit: str) -> dict[str, object]:
    cleaned_name = display_name.strip()
    asset_name = cleaned_name
    asset_id = f"cmd_{_slugify(asset_name)}"
    return {
        "asset_id": asset_id,
        "asset_name": asset_name,
        "display_name": cleaned_name,
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "Commodities",
        "sub_asset_class": "Benchmark",
        "country": "",
        "region": "",
        "dm_em_flag": "",
        "commodity_category": COMMODITY_CATEGORY_BY_NAME.get(cleaned_name, "Other commodities"),
        "sector_name": "",
        "series_type": "commodity_benchmark_price",
        "return_variant": "",
        "currency": "USD",
        "unit": unit,
        "is_active": True,
        "notes": "Rebuilt from Assets_data.xlsx",
    }


def _parse_sheet(
    sheet: pd.DataFrame,
    *,
    asset_class: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    name_row = sheet.iloc[3]
    security_row = sheet.iloc[5]
    unit_row = sheet.iloc[4]
    starts = _security_starts(security_row)

    asset_records: list[dict[str, object]] = []
    price_records: list[dict[str, object]] = []
    workbook_timestamp = pd.Timestamp.utcnow().tz_localize(None)

    for index, start_col in enumerate(starts):
        next_start = starts[index + 1] if index + 1 < len(starts) else None
        display_name = _extract_name(name_row, start_col, next_start)
        ticker = _clean_text(security_row.iloc[start_col + 1])
        if not ticker:
            continue

        if asset_class == "Equities":
            asset_record = _build_equity_record(display_name=display_name or ticker, ticker=ticker)
        else:
            unit = _extract_unit(unit_row, start_col, next_start)
            asset_record = _build_commodity_record(display_name=display_name or ticker, ticker=ticker, unit=unit)

        asset_records.append(asset_record)

        for row_index in range(12, len(sheet)):
            raw_date = sheet.iat[row_index, start_col] if start_col < sheet.shape[1] else None
            raw_value = sheet.iat[row_index, start_col + 1] if start_col + 1 < sheet.shape[1] else None

            parsed_date = _excel_date(raw_date)
            if parsed_date is None:
                continue

            value = pd.to_numeric(raw_value, errors="coerce")
            if pd.isna(value):
                continue

            price_records.append(
                {
                    "date": parsed_date,
                    "asset_id": asset_record["asset_id"],
                    "value": float(value),
                    "source_timestamp": workbook_timestamp,
                }
            )

    return asset_records, price_records


def parse_market_workbook(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    with pd.ExcelFile(path) as workbook:
        equity_sheet = pd.read_excel(workbook, sheet_name="Equity_data", header=None)
        commodity_sheet = pd.read_excel(workbook, sheet_name="Commodity_data", header=None)

    equity_assets, equity_prices = _parse_sheet(equity_sheet, asset_class="Equities")
    commodity_assets, commodity_prices = _parse_sheet(commodity_sheet, asset_class="Commodities")

    asset_master = pd.DataFrame(equity_assets + commodity_assets, columns=ASSET_MASTER_COLUMNS)
    asset_master = asset_master.drop_duplicates(subset=["asset_id"]).sort_values(["asset_class", "display_name"]).reset_index(drop=True)

    prices = pd.DataFrame(equity_prices + commodity_prices, columns=PRICES_COLUMNS)
    prices = prices.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return asset_master if not asset_master.empty else _empty_asset_master(), prices if not prices.empty else _empty_prices()


def replace_project_data(
    source_path: Path,
    *,
    asset_master_path: Path = ASSET_MASTER_PATH,
    prices_path: Path = PRICES_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    asset_master, prices = parse_market_workbook(source_path)
    asset_master.to_csv(asset_master_path, index=False)
    prices.to_parquet(prices_path, index=False)
    return asset_master, prices


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replace project data from a wide market workbook.")
    parser.add_argument("--source", required=True, help="Path to Assets_data.xlsx.")
    parser.add_argument("--asset-master", default=str(ASSET_MASTER_PATH), help="Output asset master CSV path.")
    parser.add_argument("--prices", default=str(PRICES_PATH), help="Output prices parquet path.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asset_master, prices = replace_project_data(
        Path(args.source),
        asset_master_path=Path(args.asset_master),
        prices_path=Path(args.prices),
    )
    print(f"Wrote {len(asset_master)} assets and {len(prices)} price rows.")


if __name__ == "__main__":
    main()
