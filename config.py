from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RUSSIA_DATA_DIR = DATA_DIR / "russia"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

ASSET_MASTER_PATH = DATA_DIR / "asset_master.csv"
EVENTS_PATH = DATA_DIR / "events.csv"
PRICES_PATH = DATA_DIR / "prices.parquet"
SNAPSHOT_PATH = DATA_DIR / "snapshot.parquet"
RUSSIA_SUMMARY_EQUITIES_PATH = RUSSIA_DATA_DIR / "summary_equities.csv"
RUSSIA_SUMMARY_BONDS_PATH = RUSSIA_DATA_DIR / "summary_bonds.csv"
RUSSIA_DAILY_LAST_PRICE_LONG_PATH = RUSSIA_DATA_DIR / "daily_last_price_long.csv"

DEFAULT_EVENT_ID = "IRAN_US_ISRAEL_OP_START"
DEFAULT_EVENT_LABEL = "US vs Iran conflict"
DEFAULT_EVENT_DATE = "2026-02-28"

DATE_DISPLAY_FORMAT = "%d %b %Y"
PERCENT_DECIMALS = 1

FRESH_MAX_LAG_DAYS = 7
AGED_MAX_LAG_DAYS = 30
COMPARISON_INCLUDED_FRESHNESS = {"Fresh", "Aged"}

COMMODITY_CATEGORY_DISPLAY_LABELS = {
    "Energy - crude oil & refined products": "Energy - Crude & Products",
    "Energy - gas, LNG, coal": "Energy - Gas/LNG/Coal",
    "Ferrous / Steel chain": "Steel Chain",
    "Energy materials": "Energy Materials",
    "Precious metals": "Precious Metals",
    "Base metals": "Base Metals",
}

SERIES_DISPLAY_ALIASES = {
    "Asia Naphtha FOB Singapore Cargo Spot": "Asia Naphtha",
    "European Gasoil 0.1% FOB Med Cargo Spot": "European Gasoil",
    "European ULSD 10ppm FOB Med Cargo Spot": "European ULSD",
    "Fuel Oil 3,5% Rotterdam swap": "Fuel Oil Rotterdam",
    "HCC FOB Australia Swap M1": "HCC Australia M1",
    "Korea Composite Stock Price Index": "KOSPI",
    "Taiwan Stock Exchange Index": "Taiwan Exchange",
}
