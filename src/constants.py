from __future__ import annotations


ASSET_MASTER_COLUMNS = [
    "asset_id",
    "asset_name",
    "display_name",
    "bbg_ticker",
    "source_field",
    "source",
    "asset_class",
    "sub_asset_class",
    "country",
    "region",
    "dm_em_flag",
    "commodity_category",
    "sector_name",
    "series_type",
    "return_variant",
    "currency",
    "unit",
    "is_active",
    "notes",
]

EVENTS_COLUMNS = [
    "event_id",
    "event_name",
    "event_date",
    "description",
    "is_active",
]

PRICES_COLUMNS = [
    "date",
    "asset_id",
    "value",
    "source_timestamp",
]

SNAPSHOT_COLUMNS = [
    "snapshot_date",
    "data_as_of",
    "lag_days",
    "freshness_status",
    "comparison_eligible",
    "asset_id",
    "latest_value",
    "ytd",
    "since_event",
    "yoy",
    "ytd_bps",
    "since_event_bps",
    "yoy_bps",
    "base_ytd",
    "base_event",
    "base_yoy",
]

SERIES_TYPE_PERCENT = {
    "equity_price_index",
    "equity_total_return_index",
    "equity_sector_price_index",
    "equity_sector_total_return_index",
    "bond_total_return_index",
    "credit_total_return_index",
    "commodity_index",
    "commodity_spot",
    "commodity_future_front",
    "commodity_benchmark_price",
}

SERIES_TYPE_BPS = {
    "government_yield",
}
