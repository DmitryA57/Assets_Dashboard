<<<<<<< HEAD
# Assets_Dashboard
=======
# Global Cross-Asset Dashboard

Streamlit dashboard for cross-asset market monitoring across equities, bonds, and commodities.

The repository is already prepared for GitHub and Streamlit Community Cloud:
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

Recommended Python version: `3.12`

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

## Streamlit Community Cloud

When creating the app in Streamlit Community Cloud:
- repository: this GitHub repo
- branch: `main`
- entrypoint: `app.py`
- Python: `3.12`

No secrets are required for the current version of the app.

## Pre-Deploy Checklist

- `app.py` exists
- `requirements.txt` exists
- `.streamlit/config.toml` is committed
- `data/prices.parquet` is committed
- `data/snapshot.parquet` is committed
- no local secrets are stored in the repo
- tests pass locally
>>>>>>> 519b2d4 (Initial app upload)
