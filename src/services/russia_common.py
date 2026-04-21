from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from src.analytics import KpiCard, comparison_exclusion_summary, comparison_universe, performance_breadth, top_bottom
from src.compute_snapshot import build_snapshot
from src.constants import ASSET_MASTER_COLUMNS, PRICES_COLUMNS
from src.data_sources.tbank_client import TBankApiError, TBankClient
from src.data_sources.tbank_instruments import RussiaUniverseItem, TBankInstrument, resolve_universe
from src.data_sources.tbank_market_data import (
    fetch_close_prices,
    fetch_daily_candles,
    fetch_last_prices,
    fetch_today_schedule_status,
    fetch_trading_statuses,
)


RUSSIA_COUNTRY = "Russia"
RUSSIA_REGION = "Russia"
RUSSIA_DM_EM = "EM"
RUSSIA_SOURCE = "T-Invest API"


RUSSIA_EQUITY_UNIVERSE = [
    RussiaUniverseItem("ru_eq_imoex", "IMOEX", "IMOEX", "Equities", "Russia Headline Index", "equity_price_index", "Price Return"),
    RussiaUniverseItem("ru_eq_rtsi", "RTSI", "RTSI", "Equities", "Russia Headline Index", "equity_price_index", "Price Return"),
    RussiaUniverseItem("ru_eq_moexbc", "MOEXBC", "MOEX Blue Chip", "Equities", "Russia Headline Index", "equity_price_index", "Price Return"),
    RussiaUniverseItem("ru_eq_moexbmi", "MOEXBMI", "MOEX Broad Market", "Equities", "Russia Headline Index", "equity_price_index", "Price Return"),
    RussiaUniverseItem("ru_eq_moexog", "MOEXOG", "MOEX Oil & Gas", "Equities", "Russia Sector Index", "equity_sector_price_index", "Price Return", sector_name="Oil & Gas"),
    RussiaUniverseItem("ru_eq_moexfn", "MOEXFN", "MOEX Financials", "Equities", "Russia Sector Index", "equity_sector_price_index", "Price Return", sector_name="Financials"),
    RussiaUniverseItem("ru_eq_moexmm", "MOEXMM", "MOEX Metals & Mining", "Equities", "Russia Sector Index", "equity_sector_price_index", "Price Return", sector_name="Metals & Mining"),
    RussiaUniverseItem("ru_eq_moexcn", "MOEXCN", "MOEX Consumer", "Equities", "Russia Sector Index", "equity_sector_price_index", "Price Return", sector_name="Consumer"),
    RussiaUniverseItem("ru_eq_moexit", "MOEXIT", "MOEX Information Technology", "Equities", "Russia Sector Index", "equity_sector_price_index", "Price Return", sector_name="Information Technology"),
]

RUSSIA_BOND_UNIVERSE = [
    RussiaUniverseItem("ru_bond_rgbi", "RGBI", "RGBI", "Bonds", "Russia Government Bonds", "bond_total_return_index", "Price Return", sector_name="Government"),
    RussiaUniverseItem("ru_bond_rgbitr", "RGBITR", "RGBI Total Return", "Bonds", "Russia Government Bonds", "bond_total_return_index", "Total Return", sector_name="Government"),
    RussiaUniverseItem("ru_bond_rucbtrns", "RUCBTRNS", "RUCBTRNS", "Bonds", "Russia Corporate Bonds", "bond_total_return_index", "Total Return", sector_name="Corporate"),
]

UNIVERSES = {
    "equities": RUSSIA_EQUITY_UNIVERSE,
    "bonds": RUSSIA_BOND_UNIVERSE,
}


@dataclass(slots=True)
class RussiaMarketState:
    snapshot: pd.DataFrame
    comparison_snapshot: pd.DataFrame
    prices: pd.DataFrame
    asset_master: pd.DataFrame
    warnings: list[str]
    schedule_status: str
    data_source: str = RUSSIA_SOURCE


@dataclass(slots=True)
class TInvestConnectionSettings:
    token: str | None
    trust_env: bool = False
    verify_ssl: bool = True
    ca_bundle_path: str | None = None


def read_tinvest_token(secrets: Mapping[str, object]) -> str | None:
    try:
        secret_values = dict(secrets)
    except Exception:
        return None

    for key in ("TINVEST_TOKEN",):
        if key in secret_values and secret_values[key]:
            return str(secret_values[key]).strip()
    tbank_section = secret_values.get("tbank")
    if isinstance(tbank_section, Mapping):
        for key in ("token", "TINVEST_TOKEN"):
            if key in tbank_section and tbank_section[key]:
                return str(tbank_section[key]).strip()
    return None


def _read_bool(mapping: Mapping[str, object], key: str, default: bool) -> bool:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def read_tinvest_connection_settings(secrets: Mapping[str, object]) -> TInvestConnectionSettings:
    try:
        secret_values = dict(secrets)
    except Exception:
        return TInvestConnectionSettings(token=None)

    token = read_tinvest_token(secret_values)
    tbank_section = secret_values.get("tbank")
    section = tbank_section if isinstance(tbank_section, Mapping) else {}

    ca_bundle_path = None
    for key in ("TINVEST_CA_BUNDLE", "ca_bundle_path", "ca_bundle"):
        if key in secret_values and secret_values[key]:
            ca_bundle_path = str(secret_values[key]).strip()
            break
        if key in section and section[key]:
            ca_bundle_path = str(section[key]).strip()
            break

    trust_env = _read_bool(secret_values, "TINVEST_TRUST_ENV", False)
    if "trust_env" in section:
        trust_env = _read_bool(section, "trust_env", trust_env)

    verify_ssl = _read_bool(secret_values, "TINVEST_VERIFY_SSL", True)
    if "verify_ssl" in section:
        verify_ssl = _read_bool(section, "verify_ssl", verify_ssl)

    return TInvestConnectionSettings(
        token=token,
        trust_env=trust_env,
        verify_ssl=verify_ssl,
        ca_bundle_path=ca_bundle_path,
    )


def universe_for_market(market_key: str) -> list[RussiaUniverseItem]:
    return list(UNIVERSES.get(market_key, []))


def _market_from_payload(market_key: str) -> str:
    if market_key not in UNIVERSES:
        raise ValueError(f"Unsupported Russia market: {market_key}")
    return market_key


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_resolve_universe(token: str, market_key: str) -> dict[str, object]:
    market = _market_from_payload(market_key)
    settings = read_tinvest_connection_settings(st.secrets)
    client = TBankClient(
        token,
        trust_env=settings.trust_env,
        verify_ssl=settings.verify_ssl,
        ca_bundle_path=settings.ca_bundle_path,
    )
    instruments, warnings = resolve_universe(client, universe_for_market(market))
    return {
        "instruments": [instrument.to_dict() for instrument in instruments],
        "warnings": warnings,
    }


@st.cache_data(ttl=21600, show_spinner=False)
def _cached_candles(token: str, instrument_ids: tuple[str, ...]) -> dict[str, object]:
    settings = read_tinvest_connection_settings(st.secrets)
    client = TBankClient(
        token,
        trust_env=settings.trust_env,
        verify_ssl=settings.verify_ssl,
        ca_bundle_path=settings.ca_bundle_path,
    )
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=730)
    records: list[dict[str, object]] = []
    warnings: list[str] = []
    for instrument_id in instrument_ids:
        try:
            candles = fetch_daily_candles(client, instrument_id, from_dt=start_utc, to_dt=now_utc)
            records.extend(candle.to_record() for candle in candles)
        except TBankApiError as error:
            warnings.append(f"Could not load candles for {instrument_id}: {error}")
    return {"records": records, "warnings": warnings}


@st.cache_data(ttl=300, show_spinner=False)
def _cached_live_market_fields(token: str, instrument_ids: tuple[str, ...]) -> dict[str, object]:
    settings = read_tinvest_connection_settings(st.secrets)
    client = TBankClient(
        token,
        trust_env=settings.trust_env,
        verify_ssl=settings.verify_ssl,
        ca_bundle_path=settings.ca_bundle_path,
    )
    warnings: list[str] = []
    last_prices: dict[str, float] = {}
    close_prices: dict[str, float] = {}
    trading_statuses: dict[str, str] = {}

    try:
        last_prices = fetch_last_prices(client, list(instrument_ids))
    except TBankApiError as error:
        warnings.append(f"Could not load last prices: {error}")
    try:
        close_prices = fetch_close_prices(client, list(instrument_ids))
    except TBankApiError as error:
        warnings.append(f"Could not load close prices: {error}")
    try:
        trading_statuses = fetch_trading_statuses(client, list(instrument_ids))
    except TBankApiError as error:
        warnings.append(f"Could not load trading statuses: {error}")

    return {
        "last_prices": last_prices,
        "close_prices": close_prices,
        "trading_statuses": trading_statuses,
        "warnings": warnings,
    }


@st.cache_data(ttl=1800, show_spinner=False)
def _cached_schedule_status(token: str) -> str:
    settings = read_tinvest_connection_settings(st.secrets)
    client = TBankClient(
        token,
        trust_env=settings.trust_env,
        verify_ssl=settings.verify_ssl,
        ca_bundle_path=settings.ca_bundle_path,
    )
    try:
        return fetch_today_schedule_status(client)
    except TBankApiError:
        return ""


def _instrument_from_dict(payload: dict[str, object]) -> TBankInstrument:
    return TBankInstrument(
        uid=str(payload.get("uid", "")),
        figi=str(payload.get("figi", "")),
        ticker=str(payload.get("ticker", "")),
        class_code=str(payload.get("class_code", "")),
        name=str(payload.get("name", "")),
        instrument_type=str(payload.get("instrument_type", "")),
        exchange=str(payload.get("exchange", "")),
        currency=str(payload.get("currency", "")),
        api_trade_available=payload.get("api_trade_available"),
        buy_available=payload.get("buy_available"),
        sell_available=payload.get("sell_available"),
        first_1day_candle_date=pd.to_datetime(payload.get("first_1day_candle_date"), errors="coerce"),
        maturity_date=pd.to_datetime(payload.get("maturity_date"), errors="coerce"),
    )


def build_asset_master(
    universe: list[RussiaUniverseItem],
    instruments: list[TBankInstrument],
) -> pd.DataFrame:
    instrument_by_ticker = {instrument.ticker.upper(): instrument for instrument in instruments}
    rows: list[dict[str, object]] = []
    for member in universe:
        instrument = instrument_by_ticker.get(member.ticker.upper())
        if instrument is None:
            continue
        rows.append(
            {
                "asset_id": member.asset_id,
                "asset_name": instrument.name or member.display_name,
                "display_name": member.display_name,
                "bbg_ticker": member.ticker,
                "source_field": "close",
                "source": RUSSIA_SOURCE,
                "asset_class": member.asset_class,
                "sub_asset_class": member.sub_asset_class,
                "country": RUSSIA_COUNTRY,
                "region": RUSSIA_REGION,
                "dm_em_flag": RUSSIA_DM_EM,
                "commodity_category": "",
                "sector_name": member.sector_name,
                "series_type": member.series_type,
                "return_variant": member.return_variant,
                "currency": instrument.currency or member.currency,
                "unit": member.unit,
                "is_active": True,
                "notes": member.notes,
            }
        )
    return pd.DataFrame(rows, columns=ASSET_MASTER_COLUMNS)


def normalize_prices(
    records: list[dict[str, object]],
) -> pd.DataFrame:
    normalized_rows: list[dict[str, object]] = []
    for record in records:
        source_asset_id = str(record.get("asset_id") or "")
        if not source_asset_id:
            continue
        normalized_rows.append(
            {
                "date": pd.to_datetime(record.get("date"), errors="coerce"),
                "asset_id": source_asset_id,
                "value": pd.to_numeric(record.get("value"), errors="coerce"),
                "source_timestamp": pd.to_datetime(record.get("source_timestamp"), errors="coerce"),
            }
        )
    prices = pd.DataFrame(normalized_rows, columns=PRICES_COLUMNS)
    if prices.empty:
        return prices
    prices = prices.dropna(subset=["date", "asset_id", "value"]).sort_values(["asset_id", "date", "source_timestamp"]).reset_index(drop=True)
    return prices


def remap_candle_asset_ids(
    universe: list[RussiaUniverseItem],
    instruments: list[TBankInstrument],
    candle_records: list[dict[str, object]],
) -> list[dict[str, object]]:
    asset_id_by_instrument_id: dict[str, str] = {}
    member_by_ticker = {member.ticker.upper(): member for member in universe}
    for instrument in instruments:
        member = member_by_ticker.get(instrument.ticker.upper())
        if member is not None:
            asset_id_by_instrument_id[instrument.instrument_id] = member.asset_id

    remapped: list[dict[str, object]] = []
    for record in candle_records:
        instrument_id = str(record.get("asset_id") or "")
        asset_id = asset_id_by_instrument_id.get(instrument_id)
        if asset_id is None:
            continue
        mapped = dict(record)
        mapped["asset_id"] = asset_id
        remapped.append(mapped)
    return remapped


def enrich_snapshot(
    snapshot: pd.DataFrame,
    universe: list[RussiaUniverseItem],
    instruments: list[TBankInstrument],
    live_fields: dict[str, object],
) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot

    last_prices = live_fields.get("last_prices", {})
    close_prices = live_fields.get("close_prices", {})
    trading_statuses = live_fields.get("trading_statuses", {})

    rows: list[dict[str, object]] = []
    instrument_by_asset_id: dict[str, TBankInstrument] = {}
    member_by_ticker = {member.ticker.upper(): member for member in universe}
    for instrument in instruments:
        member = member_by_ticker.get(instrument.ticker.upper())
        if member is not None:
            instrument_by_asset_id[member.asset_id] = instrument
    for _, row in snapshot.iterrows():
        asset_id = str(row["asset_id"])
        instrument = instrument_by_asset_id.get(asset_id)
        instrument_id = instrument.instrument_id if instrument else ""
        market_price = None
        if instrument_id:
            market_price = last_prices.get(instrument_id)
            if market_price is None:
                market_price = close_prices.get(instrument_id)
        if market_price is None:
            market_price = row.get("latest_value")

        enriched = row.to_dict()
        enriched["ticker"] = instrument.ticker if instrument else ""
        enriched["instrument_uid"] = instrument.uid if instrument else ""
        enriched["figi"] = instrument.figi if instrument else ""
        enriched["class_code"] = instrument.class_code if instrument else ""
        enriched["exchange"] = instrument.exchange if instrument else ""
        enriched["trading_status"] = trading_statuses.get(instrument_id, "")
        enriched["market_price"] = market_price
        enriched["last_update_date"] = row.get("data_as_of")
        enriched["maturity_date"] = instrument.maturity_date if instrument else pd.NaT
        rows.append(enriched)

    return pd.DataFrame(rows)


def build_russia_kpi_cards(frame: pd.DataFrame, metric: str, metric_title: str) -> list[KpiCard]:
    cards: list[KpiCard] = []
    comparison_frame = comparison_universe(frame)
    reference_frame = comparison_frame.dropna(subset=[metric]) if metric in comparison_frame.columns else comparison_frame.iloc[0:0]
    fallback_frame = frame.dropna(subset=[metric]) if metric in frame.columns else frame.iloc[0:0]
    top, bottom = top_bottom(reference_frame if not reference_frame.empty else frame, metric, n=1)
    if not top.empty:
        cards.append(KpiCard("Best Performer", str(top.iloc[0]["display_name"]), f"{top.iloc[0][metric]:+.1%}"))
    if not bottom.empty:
        cards.append(KpiCard("Worst Performer", str(bottom.iloc[0]["display_name"]), f"{bottom.iloc[0][metric]:+.1%}"))
    if not reference_frame.empty:
        cards.append(KpiCard("Median Return", f"{reference_frame[metric].median():+.1%}", metric_title))
    elif not fallback_frame.empty:
        cards.append(KpiCard("Median Return", f"{fallback_frame[metric].median():+.1%}", metric_title))
    breadth = performance_breadth(reference_frame, metric)
    if breadth is not None:
        cards.append(KpiCard("Positive Breadth", f"{breadth:.0%}", "Share of positive returns"))
    cards.append(KpiCard("Active Instruments", str(int(len(frame))), "Resolved Russia universe"))
    _, excluded = comparison_exclusion_summary(frame)
    cards.append(KpiCard("Excluded / Stale", str(excluded), "Not used in rankings"))
    return cards[:6]


def build_status_lines(state: RussiaMarketState) -> list[str]:
    eligible, excluded = comparison_exclusion_summary(state.snapshot)
    lines = [
        f"Loaded instruments: {len(state.snapshot)}",
        f"Used in rankings: {eligible}",
        f"Excluded as stale or missing: {excluded}",
    ]
    if state.schedule_status:
        lines.append(state.schedule_status)
    return lines


def load_russia_market_state(token: str, market_key: str, reference_date: pd.Timestamp) -> RussiaMarketState:
    market = _market_from_payload(market_key)
    universe = universe_for_market(market)
    resolved_payload = _cached_resolve_universe(token, market)
    instruments = [_instrument_from_dict(item) for item in resolved_payload["instruments"]]
    warnings = list(resolved_payload["warnings"])

    instrument_ids = tuple(instrument.instrument_id for instrument in instruments if instrument.instrument_id)
    candle_payload = _cached_candles(token, instrument_ids)
    warnings.extend(candle_payload["warnings"])

    live_payload = _cached_live_market_fields(token, instrument_ids)
    warnings.extend(live_payload["warnings"])

    asset_master = build_asset_master(universe, instruments)
    remapped_candles = remap_candle_asset_ids(universe, instruments, list(candle_payload["records"]))
    prices = normalize_prices(remapped_candles)

    snapshot = build_snapshot(
        asset_master=asset_master,
        prices=prices,
        events=pd.DataFrame(columns=["event_id", "event_name", "event_date", "description", "is_active"]),
    )
    if not snapshot.empty:
        from src.analytics import apply_reference_window

        snapshot = apply_reference_window(snapshot, prices, reference_date)
    snapshot = enrich_snapshot(snapshot, universe, instruments, live_payload)
    comparison_snapshot = comparison_universe(snapshot)

    return RussiaMarketState(
        snapshot=snapshot,
        comparison_snapshot=comparison_snapshot,
        prices=prices,
        asset_master=asset_master,
        warnings=warnings,
        schedule_status=_cached_schedule_status(token),
    )
