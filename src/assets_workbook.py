from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from config import ASSET_MASTER_PATH, PRICES_PATH
from src.bonds_workbook import BOND_METADATA_BY_TICKER, EXCLUDED_TICKERS as EXCLUDED_BOND_TICKERS
from src.compute_snapshot import write_snapshot
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS
from src.load_data import load_events
from src.market_workbook import COMMODITY_CATEGORY_BY_NAME, EQUITY_METADATA_BY_TICKER


REPLACED_ASSET_CLASSES = {
    "Equities",
    "Commodities",
    "Bonds",
    "ETFs",
    "Crypto",
    "Top-10 Stocks",
}

ETF_METADATA_BY_TICKER = {
    "SPY US Equity": {
        "asset_name": "SPDR S&P 500 ETF Trust",
        "sub_asset_class": "Broad Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Broad Market",
        "commodity_category": "",
    },
    "QQQ US Equity": {
        "asset_name": "Invesco QQQ Trust Series I",
        "sub_asset_class": "Broad Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Nasdaq 100",
        "commodity_category": "",
    },
    "IWM US Equity": {
        "asset_name": "iShares Russell 2000 ETF",
        "sub_asset_class": "Broad Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Small Caps",
        "commodity_category": "",
    },
    "EWJ US Equity": {
        "asset_name": "iShares MSCI Japan ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "Japan",
        "region": "Asia Pacific",
        "dm_em_flag": "DM",
        "sector_name": "",
        "commodity_category": "",
    },
    "EWY US Equity": {
        "asset_name": "iShares MSCI South Korea ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "South Korea",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "EWH US Equity": {
        "asset_name": "iShares MSCI Hong Kong ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "Hong Kong",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "FXI US Equity": {
        "asset_name": "iShares China Large-Cap ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "China",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "INDA US Equity": {
        "asset_name": "iShares MSCI India ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "India",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "EWZ US Equity": {
        "asset_name": "iShares MSCI Brazil ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "Brazil",
        "region": "Latin America",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "EWW US Equity": {
        "asset_name": "iShares MSCI Mexico ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "Mexico",
        "region": "Latin America",
        "dm_em_flag": "EM",
        "sector_name": "",
        "commodity_category": "",
    },
    "VGK US Equity": {
        "asset_name": "Vanguard FTSE Europe ETF",
        "sub_asset_class": "Country Equity ETFs",
        "country": "Europe",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "",
        "commodity_category": "",
    },
    "XLF US Equity": {
        "asset_name": "Financial Select Sector SPDR ETF",
        "sub_asset_class": "Sector Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Financials",
        "commodity_category": "",
    },
    "XLK US Equity": {
        "asset_name": "Technology Select Sector SPDR ETF",
        "sub_asset_class": "Sector Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Technology",
        "commodity_category": "",
    },
    "SOXX US Equity": {
        "asset_name": "iShares Semiconductor ETF",
        "sub_asset_class": "Sector Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Semiconductors",
        "commodity_category": "",
    },
    "XLE US Equity": {
        "asset_name": "Energy Select Sector SPDR ETF",
        "sub_asset_class": "Sector Equity ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Energy",
        "commodity_category": "",
    },
    "IXC US Equity": {
        "asset_name": "iShares Global Energy ETF",
        "sub_asset_class": "Sector Equity ETFs",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Global Energy",
        "commodity_category": "",
    },
    "IBIT US Equity": {
        "asset_name": "iShares Bitcoin Trust ETF",
        "sub_asset_class": "Crypto ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Bitcoin",
        "commodity_category": "",
    },
    "ETHA US Equity": {
        "asset_name": "iShares Ethereum Trust ETF",
        "sub_asset_class": "Crypto ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Ethereum",
        "commodity_category": "",
    },
    "BITO US Equity": {
        "asset_name": "ProShares Bitcoin ETF",
        "sub_asset_class": "Crypto ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Bitcoin",
        "commodity_category": "",
    },
    "SLV US Equity": {
        "asset_name": "iShares Silver Trust",
        "sub_asset_class": "Commodity ETFs - Direct Exposure",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Silver",
        "commodity_category": "Precious metals",
    },
    "IAU US Equity": {
        "asset_name": "iShares Gold Trust",
        "sub_asset_class": "Commodity ETFs - Direct Exposure",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Gold",
        "commodity_category": "Precious metals",
    },
    "USO US Equity": {
        "asset_name": "United States Oil Fund",
        "sub_asset_class": "Commodity ETFs - Direct Exposure",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "WTI",
        "commodity_category": "Energy - crude oil & refined products",
    },
    "BNO US Equity": {
        "asset_name": "United States Brent Oil Fund",
        "sub_asset_class": "Commodity ETFs - Direct Exposure",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Brent",
        "commodity_category": "Energy - crude oil & refined products",
    },
    "GDX US Equity": {
        "asset_name": "VanEck Gold Miners ETF",
        "sub_asset_class": "Commodity-Related Equities ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Gold Miners",
        "commodity_category": "Precious metals",
    },
    "GDXJ US Equity": {
        "asset_name": "VanEck Junior Gold Miners ETF",
        "sub_asset_class": "Commodity-Related Equities ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Junior Gold Miners",
        "commodity_category": "Precious metals",
    },
    "XME US Equity": {
        "asset_name": "SPDR S&P Metals & Mining ETF",
        "sub_asset_class": "Commodity-Related Equities ETFs",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Metals & Mining",
        "commodity_category": "Base metals",
    },
    "PICK US Equity": {
        "asset_name": "iShares MSCI Global Metals & Mining Producers ETF",
        "sub_asset_class": "Commodity-Related Equities ETFs",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Global Metals & Mining",
        "commodity_category": "Base metals",
    },
}

CRYPTO_METADATA_BY_TICKER = {
    "XBT Curncy": {"asset_name": "Bitcoin"},
    "XET Curncy": {"asset_name": "Ethereum"},
    "XSO Curncy": {"asset_name": "Solana"},
}

TOP_STOCK_METADATA_BY_TICKER = {
    "AMZN US Equity": {
        "asset_name": "Amazon",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Consumer Internet",
    },
    "MSFT US Equity": {
        "asset_name": "Microsoft",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Software / Cloud",
    },
    "GOOGL US Equity": {
        "asset_name": "Google",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Internet",
    },
    "META US Equity": {
        "asset_name": "Meta",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Internet",
    },
    "2330 TT Equity": {
        "asset_name": "TSMC",
        "country": "Taiwan",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "Semiconductors",
    },
    "NVDA US Equity": {
        "asset_name": "NVIDIA",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Semiconductors",
    },
    "ASML NA Equity": {
        "asset_name": "ASML",
        "country": "Netherlands",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "Semicap Equipment",
    },
    "HSAI US Equity": {
        "asset_name": "Hesai",
        "country": "China",
        "region": "Asia",
        "dm_em_flag": "EM",
        "sector_name": "Autonomous Driving / LiDAR",
    },
    "PRY IM Equity": {
        "asset_name": "Prysmian",
        "country": "Italy",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "Electrical Infrastructure",
    },
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


def _find_text_in_window(row: pd.Series, start_col: int, next_start: int | None) -> str:
    search_end = next_start if next_start is not None else len(row)
    for column in range(start_col, min(search_end, start_col + 4)):
        candidate = _clean_text(row.iloc[column])
        if candidate:
            return candidate
    return ""


def _security_starts(security_row: pd.Series) -> list[int]:
    return [index for index, value in security_row.items() if _clean_text(value) == "Security"]


def _ticker_without_index(value: str) -> str:
    return value.replace(" Index", "").strip()


def _empty_asset_master() -> pd.DataFrame:
    return pd.DataFrame(columns=ASSET_MASTER_COLUMNS)


def _empty_prices() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICES_COLUMNS)


def _build_equity_record(display_name: str, ticker: str) -> dict[str, object]:
    metadata = EQUITY_METADATA_BY_TICKER.get(ticker.strip(), {})
    asset_name = metadata.get("asset_name", display_name.strip())
    return {
        "asset_id": f"eq_{_slugify(asset_name)}",
        "asset_name": asset_name,
        "display_name": display_name.strip() or asset_name,
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


def _build_commodity_record(display_name: str, ticker: str) -> dict[str, object]:
    cleaned_name = display_name.strip()
    return {
        "asset_id": f"cmd_{_slugify(cleaned_name)}",
        "asset_name": cleaned_name,
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
        "return_variant": "Spot / Benchmark",
        "currency": "USD",
        "unit": "USD",
        "is_active": True,
        "notes": "Rebuilt from Assets_data.xlsx",
    }


def _build_bond_record(ticker: str) -> dict[str, object] | None:
    cleaned_ticker = _ticker_without_index(ticker)
    metadata = BOND_METADATA_BY_TICKER.get(cleaned_ticker)
    if metadata is None or cleaned_ticker in EXCLUDED_BOND_TICKERS:
        return None

    return {
        "asset_id": f"bond_{_slugify(cleaned_ticker)}",
        "asset_name": metadata["asset_name"],
        "display_name": metadata["display_name"],
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "Bonds",
        "sub_asset_class": metadata["sub_asset_class"],
        "country": metadata["country"],
        "region": metadata["region"],
        "dm_em_flag": metadata["dm_em_flag"],
        "commodity_category": "",
        "sector_name": metadata["sector_name"],
        "series_type": metadata["series_type"],
        "return_variant": "Total Return",
        "currency": "USD",
        "unit": "index points",
        "is_active": True,
        "notes": "Imported from Assets_data.xlsx; leveraged loans excluded by specification.",
    }


def _build_etf_record(ticker: str) -> dict[str, object] | None:
    metadata = ETF_METADATA_BY_TICKER.get(ticker.strip())
    if metadata is None:
        return None
    return {
        "asset_id": f"etf_{_slugify(ticker)}",
        "asset_name": metadata["asset_name"],
        "display_name": metadata["asset_name"],
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "ETFs",
        "sub_asset_class": metadata["sub_asset_class"],
        "country": metadata["country"],
        "region": metadata["region"],
        "dm_em_flag": metadata["dm_em_flag"],
        "commodity_category": metadata["commodity_category"],
        "sector_name": metadata["sector_name"],
        "series_type": "etf_price",
        "return_variant": "Price Return",
        "currency": "USD",
        "unit": "USD per share",
        "is_active": True,
        "notes": "Imported from Assets_data.xlsx",
    }


def _build_crypto_record(display_name: str, ticker: str) -> dict[str, object] | None:
    metadata = CRYPTO_METADATA_BY_TICKER.get(ticker.strip())
    if metadata is None:
        return None
    asset_name = metadata["asset_name"]
    return {
        "asset_id": f"crypto_{_slugify(ticker)}",
        "asset_name": asset_name,
        "display_name": display_name.strip() or asset_name,
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "Crypto",
        "sub_asset_class": "",
        "country": "",
        "region": "Global",
        "dm_em_flag": "",
        "commodity_category": "",
        "sector_name": "",
        "series_type": "crypto_spot",
        "return_variant": "Spot Price",
        "currency": "USD",
        "unit": "USD",
        "is_active": True,
        "notes": "Imported from Assets_data.xlsx",
    }


def _build_top_stock_record(display_name: str, ticker: str) -> dict[str, object] | None:
    metadata = TOP_STOCK_METADATA_BY_TICKER.get(ticker.strip())
    if metadata is None:
        return None
    asset_name = metadata["asset_name"]
    return {
        "asset_id": f"stock_{_slugify(ticker)}",
        "asset_name": asset_name,
        "display_name": display_name.strip() or asset_name,
        "bbg_ticker": ticker.strip(),
        "source_field": "PX_LAST",
        "source": "Bloomberg",
        "asset_class": "Top-10 Stocks",
        "sub_asset_class": "",
        "country": metadata["country"],
        "region": metadata["region"],
        "dm_em_flag": metadata["dm_em_flag"],
        "commodity_category": "",
        "sector_name": metadata["sector_name"],
        "series_type": "single_stock_price",
        "return_variant": "Price Return",
        "currency": "USD",
        "unit": "USD per share",
        "is_active": True,
        "notes": "Imported from Assets_data.xlsx",
    }


def _build_record(sheet_name: str, display_name: str, ticker: str) -> dict[str, object] | None:
    if sheet_name == "Equity":
        return _build_equity_record(display_name, ticker)
    if sheet_name == "Commodities":
        return _build_commodity_record(display_name, ticker)
    if sheet_name == "Bonds":
        return _build_bond_record(ticker)
    if sheet_name == "ETFs":
        return _build_etf_record(ticker)
    if sheet_name == "Crypto":
        return _build_crypto_record(display_name, ticker)
    if sheet_name == "Top-10 stocks":
        return _build_top_stock_record(display_name, ticker)
    return None


def _parse_sheet(sheet: pd.DataFrame, sheet_name: str, workbook_timestamp: pd.Timestamp) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    name_row = sheet.iloc[3]
    security_row = sheet.iloc[5]
    starts = _security_starts(security_row)

    asset_records: list[dict[str, object]] = []
    price_records: list[dict[str, object]] = []

    for index, start_col in enumerate(starts):
        next_start = starts[index + 1] if index + 1 < len(starts) else None
        display_name = _find_text_in_window(name_row, start_col, next_start)
        ticker = _clean_text(security_row.iloc[start_col + 1])
        if not ticker:
            continue

        asset_record = _build_record(sheet_name, display_name, ticker)
        if asset_record is None:
            continue

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


def parse_assets_workbook(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    workbook_timestamp = pd.Timestamp(path.stat().st_mtime, unit="s")
    asset_records: list[dict[str, object]] = []
    price_records: list[dict[str, object]] = []

    with pd.ExcelFile(path) as workbook:
        for sheet_name in workbook.sheet_names:
            sheet = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
            parsed_assets, parsed_prices = _parse_sheet(sheet, sheet_name, workbook_timestamp)
            asset_records.extend(parsed_assets)
            price_records.extend(parsed_prices)

    asset_master = pd.DataFrame(asset_records, columns=ASSET_MASTER_COLUMNS)
    prices = pd.DataFrame(price_records, columns=PRICES_COLUMNS)

    if not asset_master.empty:
        asset_master = asset_master.drop_duplicates(subset=["asset_id"]).sort_values(["asset_class", "display_name"]).reset_index(drop=True)
    if not prices.empty:
        prices = prices.sort_values(["asset_id", "date"]).reset_index(drop=True)

    return asset_master if not asset_master.empty else _empty_asset_master(), prices if not prices.empty else _empty_prices()


def merge_assets_into_project(
    source_path: Path,
    *,
    asset_master_path: Path = ASSET_MASTER_PATH,
    prices_path: Path = PRICES_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    imported_asset_master, imported_prices = parse_assets_workbook(source_path)

    existing_asset_master = pd.read_csv(asset_master_path)
    existing_prices = pd.read_parquet(prices_path)

    replaced_asset_ids = existing_asset_master.loc[
        existing_asset_master["asset_class"].isin(REPLACED_ASSET_CLASSES),
        "asset_id",
    ].astype(str)

    preserved_asset_master = existing_asset_master[~existing_asset_master["asset_class"].isin(REPLACED_ASSET_CLASSES)].copy()
    preserved_prices = existing_prices[~existing_prices["asset_id"].astype(str).isin(replaced_asset_ids)].copy()

    merged_asset_master = pd.concat([preserved_asset_master, imported_asset_master], ignore_index=True)
    merged_prices = pd.concat([preserved_prices, imported_prices], ignore_index=True)

    merged_asset_master.to_csv(asset_master_path, index=False)
    merged_prices.to_parquet(prices_path, index=False)
    snapshot = write_snapshot(merged_asset_master, merged_prices, load_events())
    return merged_asset_master, merged_prices, snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import the full Assets_data.xlsx workbook into project data files.")
    parser.add_argument("--source", required=True, help="Path to Assets_data.xlsx.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asset_master, prices, snapshot = merge_assets_into_project(Path(args.source))
    print(
        f"Imported {len(asset_master)} assets across {asset_master['asset_class'].nunique()} asset classes; "
        f"stored {len(prices)} price rows and {len(snapshot)} snapshot rows."
    )


if __name__ == "__main__":
    main()
