# Assets Dashboard

Streamlit dashboard for cross-asset market monitoring across equities, bonds, commodities, ETFs, crypto, and a focused stock watchlist.

The repository is prepared for GitHub and Streamlit Community Cloud:
- `app.py` is the entrypoint.
- `.streamlit/config.toml` is included in the repo.
- required app data is stored in `data/prices.parquet` and `data/snapshot.parquet`.
- Python version is pinned in `.python-version`.
- runtime dependencies are listed in `requirements.txt`.

## Project Structure

```text
.
|-- app.py
|-- config.py
|-- requirements.txt
|-- .python-version
|-- .streamlit/
|   `-- config.toml
|-- data/
|   |-- asset_master.csv
|   |-- events.csv
|   |-- prices.parquet
|   |-- snapshot.parquet
|   |-- raw/
|   `-- processed/
|-- pages/
|-- src/
`-- tests/
```

## Local Run

Recommended Python version: `3.12`.

Install dependencies:

```bash
uv venv .venv --python 3.12
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
```

Run the app:

```bash
.venv/Scripts/streamlit.exe run app.py
```

Run tests:

```bash
.venv/Scripts/python.exe -m pytest
```

## Data Notes

The app relies on repository-local data files:
- `data/asset_master.csv`
- `data/events.csv`
- `data/prices.parquet`
- `data/snapshot.parquet`

`data/raw/` and `data/processed/` stay excluded from git except for `.gitkeep`.

## Data Import

The main project data can be rebuilt from the combined Bloomberg workbook:

```bash
.venv/Scripts/python.exe -m src.assets_workbook --source path/to/Assets_data.xlsx
```

This importer refreshes:
- `Equities`
- `Commodities`
- `Bonds`
- `ETFs`
- `Crypto`
- `Top-10 Stocks`

It also preserves the approved bond universe and still excludes leveraged loans by specification.

Bond-only updates remain available if needed:

```bash
.venv/Scripts/python.exe -m src.bonds_workbook --source path/to/Bonds_data.xlsx
```

## Streamlit Community Cloud

When creating the app in Streamlit Community Cloud:
- repository: this GitHub repo
- branch: `main`
- entrypoint: `app.py`
- Python: `3.12` if available, otherwise the dependency pins support Python `3.14`

No secrets are required for the current dashboard pages.

The `Russia` page and `Overview Rus & World` page read repository-local export files from `data/russia/`:
- `daily_last_price_long.csv`
- `summary_equities.csv`
- `summary_bonds.csv`

This keeps the Russia section independent from live T-Bank API connectivity, SSL certificates, and local proxy settings.

## Pre-Deploy Checklist

- `app.py` exists
- `requirements.txt` exists
- `.streamlit/config.toml` is committed
- `data/prices.parquet` is committed
- `data/snapshot.parquet` is committed
- no local secrets are stored in the repo
- tests pass locally
