from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.services.russia_equities import load_russia_equities_state


RUSSIA_OVERVIEW_ASSET_IDS = {"ru_eq_rtsi"}


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
    warnings: list[str] = []

    try:
        state = load_russia_equities_state("", reference_date)
    except Exception as error:
        warnings.append(f"Russia RTSI could not be loaded: {error}")
        return RussiaOverviewBundle(
            snapshot=pd.DataFrame(),
            prices=pd.DataFrame(columns=["date", "asset_id", "value", "source_timestamp"]),
            warnings=warnings,
        )

    filtered_snapshot = state.snapshot.copy()
    if "asset_id" in filtered_snapshot.columns:
        filtered_snapshot = filtered_snapshot[filtered_snapshot["asset_id"].isin(RUSSIA_OVERVIEW_ASSET_IDS)].copy()

    filtered_prices = state.prices.copy()
    if "asset_id" in filtered_prices.columns:
        filtered_prices = filtered_prices[filtered_prices["asset_id"].isin(RUSSIA_OVERVIEW_ASSET_IDS)].copy()

    if filtered_snapshot.empty:
        warnings.append("Russia RTSI is missing from the export data.")

    return RussiaOverviewBundle(
        snapshot=filtered_snapshot.reset_index(drop=True),
        prices=filtered_prices.reset_index(drop=True),
        warnings=warnings,
    )
