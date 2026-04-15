from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from config import ASSET_MASTER_PATH, PRICES_PATH
from src.compute_snapshot import write_snapshot
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS
from src.load_data import load_events


BOND_METADATA_BY_TICKER = {
    "LEGATRUU": {
        "asset_name": "Bloomberg Global Aggregate Bond Index",
        "display_name": "Global Aggregate",
        "sub_asset_class": "Global",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "LGTRTRUU": {
        "asset_name": "Bloomberg Global Treasury Bond Index",
        "display_name": "Global Treasuries",
        "sub_asset_class": "Global",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Treasury",
        "series_type": "bond_total_return_index",
    },
    "LGDRTRUU": {
        "asset_name": "Bloomberg Global Aggregate Credit Index",
        "display_name": "Global Credit",
        "sub_asset_class": "Global",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Credit",
        "series_type": "credit_total_return_index",
    },
    "LG30TRUU": {
        "asset_name": "Bloomberg Global High Yield Index",
        "display_name": "Global High Yield",
        "sub_asset_class": "Global",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "High Yield",
        "series_type": "credit_total_return_index",
    },
    "LBUSTRUU": {
        "asset_name": "Bloomberg U.S. Aggregate Bond Index",
        "display_name": "U.S. Aggregate",
        "sub_asset_class": "U.S. fixed income",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "LUATTRUU": {
        "asset_name": "Bloomberg U.S. Treasury Bond Index",
        "display_name": "U.S. Treasury",
        "sub_asset_class": "U.S. fixed income",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Treasury",
        "series_type": "bond_total_return_index",
    },
    "LUACTRUU": {
        "asset_name": "Bloomberg U.S. Corporate Index",
        "display_name": "U.S. Corporate",
        "sub_asset_class": "U.S. fixed income",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Credit",
        "series_type": "credit_total_return_index",
    },
    "LUMSTRUU": {
        "asset_name": "Bloomberg U.S. MBS Index",
        "display_name": "U.S. MBS",
        "sub_asset_class": "U.S. fixed income",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "MBS",
        "series_type": "bond_total_return_index",
    },
    "LF98TRUU": {
        "asset_name": "Bloomberg U.S. Corporate High Yield Index",
        "display_name": "U.S. HY",
        "sub_asset_class": "U.S. fixed income",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "High Yield",
        "series_type": "credit_total_return_index",
    },
    "LP06TREU": {
        "asset_name": "Bloomberg Pan-European Aggregate Index",
        "display_name": "Pan-Euro Aggregate",
        "sub_asset_class": "Regional",
        "country": "Europe",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "LBEATREU": {
        "asset_name": "Bloomberg Euro Aggregate Bond Index",
        "display_name": "Euro-Aggregate",
        "sub_asset_class": "Regional",
        "country": "Euro Area",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "I00163JP": {
        "asset_name": "Bloomberg Asian Pacific Aggregate Index",
        "display_name": "Asian-Pacific Aggregate",
        "sub_asset_class": "Regional",
        "country": "Asia Pacific",
        "region": "Asia Pacific",
        "dm_em_flag": "DM",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "EMUSTRUU": {
        "asset_name": "Bloomberg Emerging Markets Hard Currency Aggregate Index",
        "display_name": "EM USD Aggregate",
        "sub_asset_class": "Regional",
        "country": "Emerging Markets",
        "region": "Emerging Markets",
        "dm_em_flag": "EM",
        "sector_name": "Aggregate",
        "series_type": "bond_total_return_index",
    },
    "I30740US": {
        "asset_name": "Bloomberg EM GCC USD Sukuk Index",
        "display_name": "EM GCC USD Sukuk",
        "sub_asset_class": "Regional",
        "country": "GCC",
        "region": "Middle East",
        "dm_em_flag": "EM",
        "sector_name": "Sukuk",
        "series_type": "bond_total_return_index",
    },
    "LP01TREU": {
        "asset_name": "Bloomberg Pan-European High Yield Index",
        "display_name": "Pan-European High Yield",
        "sub_asset_class": "Regional",
        "country": "Europe",
        "region": "Europe",
        "dm_em_flag": "DM",
        "sector_name": "High Yield",
        "series_type": "credit_total_return_index",
    },
    "LF94TRUU": {
        "asset_name": "Bloomberg Global Inflation-Linked Index",
        "display_name": "Global Inflation-Linked",
        "sub_asset_class": "Other",
        "country": "Global",
        "region": "Global",
        "dm_em_flag": "Global",
        "sector_name": "Inflation-Linked",
        "series_type": "bond_total_return_index",
    },
    "LMBITR": {
        "asset_name": "Bloomberg U.S. Municipal Index",
        "display_name": "Municipal Bond Index",
        "sub_asset_class": "Other",
        "country": "United States",
        "region": "North America",
        "dm_em_flag": "DM",
        "sector_name": "Municipal",
        "series_type": "bond_total_return_index",
    },
}

EXCLUDED_TICKERS = {"I38932US", "I39493EU", "I39494US", "I39569EU"}


def _clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _slugify(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return re.sub(r"_+", "_", lowered)


def _ticker_code(value: str) -> str:
    return value.replace(" Index", "").strip()


def _excel_date(value: object) -> pd.Timestamp | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _security_starts(security_row: pd.Series) -> list[int]:
    return [index for index, value in security_row.items() if _clean_text(value) == "Security"]


def _bond_record(ticker: str, metadata: dict[str, str]) -> dict[str, object]:
    return {
        "asset_id": f"bond_{_slugify(ticker)}",
        "asset_name": metadata["asset_name"],
        "display_name": metadata["display_name"],
        "bbg_ticker": f"{ticker} Index",
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
        "notes": "Imported from Bonds_data.xlsx; leveraged loans excluded by specification.",
    }


def parse_bonds_sheet(sheet: pd.DataFrame, workbook_timestamp: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    security_row = sheet.iloc[5]
    starts = _security_starts(security_row)

    asset_records: list[dict[str, object]] = []
    price_records: list[dict[str, object]] = []

    for start_col in starts:
        ticker = _ticker_code(_clean_text(security_row.iloc[start_col + 1]))
        if not ticker or ticker in EXCLUDED_TICKERS:
            continue
        metadata = BOND_METADATA_BY_TICKER.get(ticker)
        if metadata is None:
            continue

        asset_record = _bond_record(ticker, metadata)
        asset_records.append(asset_record)

        for row_index in range(12, len(sheet)):
            parsed_date = _excel_date(sheet.iat[row_index, start_col])
            if parsed_date is None:
                continue

            value = pd.to_numeric(sheet.iat[row_index, start_col + 1], errors="coerce")
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

    asset_master = pd.DataFrame(asset_records, columns=ASSET_MASTER_COLUMNS)
    prices = pd.DataFrame(price_records, columns=PRICES_COLUMNS)
    if not asset_master.empty:
        asset_master = asset_master.drop_duplicates(subset=["asset_id"]).sort_values("display_name").reset_index(drop=True)
    if not prices.empty:
        prices = prices.sort_values(["asset_id", "date"]).reset_index(drop=True)
    return asset_master, prices


def parse_bonds_workbook(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    sheet = pd.read_excel(path, sheet_name="Bonds_data", header=None)
    workbook_timestamp = pd.Timestamp(path.stat().st_mtime, unit="s")
    return parse_bonds_sheet(sheet, workbook_timestamp)


def merge_bonds_into_project(
    source_path: Path,
    *,
    asset_master_path: Path = ASSET_MASTER_PATH,
    prices_path: Path = PRICES_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bond_assets, bond_prices = parse_bonds_workbook(source_path)

    asset_master = pd.read_csv(asset_master_path)
    existing_prices = pd.read_parquet(prices_path)

    asset_master = asset_master[asset_master["asset_class"] != "Bonds"].copy()
    existing_prices = existing_prices[~existing_prices["asset_id"].astype(str).str.startswith("bond_")].copy()

    merged_asset_master = pd.concat([asset_master, bond_assets], ignore_index=True)
    merged_prices = pd.concat([existing_prices, bond_prices], ignore_index=True)

    merged_asset_master.to_csv(asset_master_path, index=False)
    merged_prices.to_parquet(prices_path, index=False)
    snapshot = write_snapshot(merged_asset_master, merged_prices, load_events())
    return merged_asset_master, merged_prices, snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import Bloomberg bond index workbook into project data.")
    parser.add_argument("--source", required=True, help="Path to Bonds_data.xlsx.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asset_master, prices, snapshot = merge_bonds_into_project(Path(args.source))
    bond_count = int((asset_master["asset_class"] == "Bonds").sum())
    print(f"Imported {bond_count} bond indices. Project now has {len(asset_master)} assets, {len(prices)} price rows, {len(snapshot)} snapshot rows.")


if __name__ == "__main__":
    main()
