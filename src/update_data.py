from __future__ import annotations

import argparse
import csv
from pathlib import Path

import pandas as pd

from config import PRICES_PATH
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS


SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".xlsx"}


def empty_prices_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=PRICES_COLUMNS)


def load_asset_mapping(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=ASSET_MASTER_COLUMNS)

    asset_master = pd.read_csv(path)
    for column in ASSET_MASTER_COLUMNS:
        if column not in asset_master.columns:
            asset_master[column] = pd.NA
    return asset_master[ASSET_MASTER_COLUMNS]


def load_prices_store(path: Path = PRICES_PATH) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return empty_prices_frame()

    prices = pd.read_parquet(path)
    return normalize_prices_frame(prices)


def read_source_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file extension: {path.suffix}")

    if path.suffix.lower() == ".csv":
        if is_bloomberg_history_csv(path):
            raise ValueError(
                "Bloomberg history exports require asset mapping. "
                "Use ingest_bloomberg_history_export instead."
            )
        frame = pd.read_csv(path)
    elif path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        raise ValueError(
            "Excel files require asset mapping. Use ingest_bloomberg_history_workbook instead."
        )
    return normalize_prices_frame(frame)


def is_bloomberg_history_csv(path: Path) -> bool:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            first_value = (row[0] or "").strip()
            if not first_value:
                continue
            return first_value == "Security"
    return False


def parse_bloomberg_history_export(path: Path, asset_master: pd.DataFrame) -> pd.DataFrame:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    exported_at = pd.Timestamp(path.stat().st_mtime, unit="s")
    return parse_bloomberg_history_rows(rows=rows, asset_master=asset_master, exported_at=exported_at)


def parse_bloomberg_history_rows(
    rows: list[list[object]],
    asset_master: pd.DataFrame,
    exported_at: pd.Timestamp,
) -> pd.DataFrame:
    if not rows or not rows[0] or str(rows[0][0]).strip() != "Security":
        raise ValueError("File is not a supported Bloomberg history export.")

    security = str(rows[0][1]).strip()
    header_index = next(
        (index for index, row in enumerate(rows) if row and str(row[0]).strip() == "Date"),
        None,
    )
    if header_index is None:
        raise ValueError("Could not find the data header row in Bloomberg export.")

    headers = [str(value).strip() if value is not None else "" for value in rows[header_index]]
    interesting_columns = [
        (index, name)
        for index, name in enumerate(headers)
        if name and name not in {"Date", "Change", "% Change"}
    ]
    if not interesting_columns:
        raise ValueError("No Bloomberg data fields found in export.")

    mapping = asset_master.copy()
    if mapping.empty:
        raise ValueError("asset_master is empty. Add bbg_ticker/source_field mappings before import.")

    mapping["bbg_ticker"] = mapping["bbg_ticker"].astype(str).str.strip()
    mapping["source_field"] = mapping["source_field"].astype(str).str.strip()

    records: list[dict[str, object]] = []
    for row in rows[header_index + 1 :]:
        normalized_row = list(row)
        if not normalized_row or not any(str(cell).strip() for cell in normalized_row if cell is not None):
            continue

        raw_date = normalized_row[0]
        if raw_date in (None, ""):
            continue

        if isinstance(raw_date, pd.Timestamp):
            parsed_date = raw_date
        else:
            raw_date_str = str(raw_date).strip()
            date_format = None
            dayfirst = True
            if "-" in raw_date_str and ":" in raw_date_str:
                date_format = "%Y-%m-%d %H:%M:%S"
                dayfirst = False
            elif "/" in raw_date_str and ":" in raw_date_str:
                date_format = "%m/%d/%Y %H:%M"
                dayfirst = False
            elif "/" in raw_date_str:
                date_format = "%d/%m/%Y"

            parsed_date = pd.to_datetime(
                raw_date,
                format=date_format,
                dayfirst=dayfirst,
                errors="coerce",
            )
        if pd.isna(parsed_date):
            continue

        for column_index, field_name in interesting_columns:
            if column_index >= len(normalized_row):
                continue
            raw_value = normalized_row[column_index]
            if raw_value in (None, ""):
                continue

            matched = mapping[
                (mapping["bbg_ticker"] == security)
                & (mapping["source_field"] == field_name)
            ]
            if matched.empty:
                continue

            value = pd.to_numeric(raw_value, errors="coerce")
            if pd.isna(value):
                continue

            records.append(
                {
                    "date": parsed_date,
                    "asset_id": matched.iloc[0]["asset_id"],
                    "value": float(value),
                    "source_timestamp": exported_at,
                }
            )

    if not records:
        return empty_prices_frame()
    return normalize_prices_frame(pd.DataFrame(records))


def is_bloomberg_history_workbook(path: Path) -> bool:
    with pd.ExcelFile(path) as workbook:
        if not workbook.sheet_names:
            return False
        first_sheet = pd.read_excel(workbook, sheet_name=workbook.sheet_names[0], header=None, nrows=1)
        return not first_sheet.empty and str(first_sheet.iloc[0, 0]).strip() == "Security"


def parse_bloomberg_history_workbook(path: Path, asset_master: pd.DataFrame) -> pd.DataFrame:
    exported_at = pd.Timestamp(path.stat().st_mtime, unit="s")
    frames: list[pd.DataFrame] = []

    with pd.ExcelFile(path) as workbook:
        for sheet_name in workbook.sheet_names:
            sheet = pd.read_excel(workbook, sheet_name=sheet_name, header=None)
            rows = sheet.where(pd.notna(sheet), None).values.tolist()
            parsed = parse_bloomberg_history_rows(
                rows=rows,
                asset_master=asset_master,
                exported_at=exported_at,
            )
            if not parsed.empty:
                frames.append(parsed)

    if not frames:
        return empty_prices_frame()
    return merge_prices(empty_prices_frame(), pd.concat(frames, ignore_index=True))


def normalize_prices_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in PRICES_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized = normalized[PRICES_COLUMNS]
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["source_timestamp"] = pd.to_datetime(normalized["source_timestamp"], errors="coerce")
    normalized["value"] = pd.to_numeric(normalized["value"], errors="coerce")

    # Stamp incoming rows that do not yet have an ingestion timestamp.
    missing_timestamp = normalized["source_timestamp"].isna()
    if missing_timestamp.any():
        normalized.loc[missing_timestamp, "source_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)

    normalized = normalized.dropna(subset=["asset_id", "date", "value"])
    return normalized.sort_values(["asset_id", "date", "source_timestamp"]).reset_index(drop=True)


def merge_prices(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True)

    if combined.empty:
        return empty_prices_frame()

    combined = normalize_prices_frame(combined)
    combined = combined.drop_duplicates(subset=["asset_id", "date"], keep="last")
    return combined.sort_values(["asset_id", "date", "source_timestamp"]).reset_index(drop=True)


def ingest_source_file(source_path: Path, destination_path: Path = PRICES_PATH) -> pd.DataFrame:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_prices_store(destination_path)
    incoming = read_source_file(source_path)
    merged = merge_prices(existing, incoming)
    merged.to_parquet(destination_path, index=False)
    return merged


def ingest_bloomberg_history_export(
    source_path: Path,
    asset_master_path: Path,
    destination_path: Path = PRICES_PATH,
) -> pd.DataFrame:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    asset_master = load_asset_mapping(asset_master_path)
    existing = load_prices_store(destination_path)
    incoming = parse_bloomberg_history_export(source_path, asset_master)
    merged = merge_prices(existing, incoming)
    merged.to_parquet(destination_path, index=False)
    return merged


def ingest_bloomberg_history_workbook(
    source_path: Path,
    asset_master_path: Path,
    destination_path: Path = PRICES_PATH,
) -> pd.DataFrame:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    asset_master = load_asset_mapping(asset_master_path)
    existing = load_prices_store(destination_path)
    incoming = parse_bloomberg_history_workbook(source_path, asset_master)
    merged = merge_prices(existing, incoming)
    merged.to_parquet(destination_path, index=False)
    return merged


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import local time-series files into prices.parquet.")
    parser.add_argument("--source", required=True, help="Path to a CSV or parquet file with date/asset_id/value rows.")
    parser.add_argument(
        "--destination",
        default=str(PRICES_PATH),
        help="Target parquet file. Defaults to the project prices store.",
    )
    parser.add_argument(
        "--asset-master",
        default="data/asset_master.csv",
        help="Asset master mapping file. Used for Bloomberg-style exports.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    destination = Path(args.destination)
    source = Path(args.source)
    if source.suffix.lower() == ".csv" and is_bloomberg_history_csv(source):
        merged = ingest_bloomberg_history_export(
            source_path=source,
            asset_master_path=Path(args.asset_master),
            destination_path=destination,
        )
    elif source.suffix.lower() == ".xlsx" and is_bloomberg_history_workbook(source):
        merged = ingest_bloomberg_history_workbook(
            source_path=source,
            asset_master_path=Path(args.asset_master),
            destination_path=destination,
        )
    else:
        merged = ingest_source_file(source_path=source, destination_path=destination)
    print(f"Stored {len(merged)} normalized price rows in {destination}")


if __name__ == "__main__":
    main()
