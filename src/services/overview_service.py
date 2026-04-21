from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.services.russia_bonds import load_russia_bonds_state
from src.services.russia_equities import load_russia_equities_state


def exclude_russia_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    mask = pd.Series(False, index=frame.index)
    for column in ("country", "region", "source", "asset_id"):
        if column not in frame.columns:
            continue
        values = frame[column].fillna("").astype(str)
        if column in {"country", "region"}:
            mask = mask | values.str.casefold().eq("russia")
        elif column == "source":
            source_values = values.str.casefold()
            mask = mask | source_values.eq("t-invest api") | source_values.eq("t-bank export")
        elif column == "asset_id":
            mask = mask | values.str.startswith("ru_")
    return frame.loc[~mask].copy()


@dataclass(slots=True)
class RussiaOverviewBundle:
    snapshot: pd.DataFrame
    prices: pd.DataFrame
    warnings: list[str]


def load_russia_overview_bundle(reference_date: pd.Timestamp) -> RussiaOverviewBundle:
    frames: list[pd.DataFrame] = []
    prices_frames: list[pd.DataFrame] = []
    warnings: list[str] = []

    for label, loader in (
        ("Russia Equities", load_russia_equities_state),
        ("Russia Bonds", load_russia_bonds_state),
    ):
        try:
            state = loader("", reference_date)
        except Exception as error:
            warnings.append(f"{label} could not be loaded: {error}")
            continue
        if not state.snapshot.empty:
            frames.append(state.snapshot.copy())
        if not state.prices.empty:
            prices_frames.append(state.prices.copy())
        warnings.extend(state.warnings)

    if not frames:
        return RussiaOverviewBundle(
            snapshot=pd.DataFrame(),
            prices=pd.DataFrame(columns=["date", "asset_id", "value", "source_timestamp"]),
            warnings=warnings,
        )

    combined_snapshot = pd.concat(frames, ignore_index=True, sort=False)
    combined_prices = pd.concat(prices_frames, ignore_index=True, sort=False) if prices_frames else pd.DataFrame(columns=["date", "asset_id", "value", "source_timestamp"])

    if "asset_id" in combined_snapshot.columns:
        combined_snapshot = combined_snapshot.drop_duplicates(subset=["asset_id"], keep="last")
    if not combined_prices.empty:
        combined_prices = combined_prices.drop_duplicates(subset=["asset_id", "date"], keep="last").reset_index(drop=True)

    return RussiaOverviewBundle(
        snapshot=combined_snapshot.reset_index(drop=True),
        prices=combined_prices,
        warnings=warnings,
    )
