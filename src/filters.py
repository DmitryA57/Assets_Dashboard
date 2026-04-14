from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class DashboardFilters:
    page_fields: list[str]
    asset_classes: list[str]
    dm_em_flags: list[str]
    countries: list[str]
    regions: list[str]
    return_variants: list[str]
    sectors: list[str]
    commodity_categories: list[str]
    sub_asset_classes: list[str]


def apply_dashboard_filters(snapshot: pd.DataFrame, filters: DashboardFilters) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot

    frame = snapshot.copy()
    mapping = {
        "asset_class": filters.asset_classes,
        "dm_em_flag": filters.dm_em_flags,
        "country": filters.countries,
        "region": filters.regions,
        "return_variant": filters.return_variants,
        "sector_name": filters.sectors,
        "commodity_category": filters.commodity_categories,
        "sub_asset_class": filters.sub_asset_classes,
    }

    for column, selected in mapping.items():
        if column in frame.columns and selected:
            frame = frame[frame[column].isin(selected)]
    return frame
