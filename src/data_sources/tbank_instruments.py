from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from src.data_sources.tbank_client import TBankClient, first_present, format_enum_label, parse_timestamp


INSTRUMENT_TYPE_INDEX = "INSTRUMENT_TYPE_INDEX"


@dataclass(frozen=True, slots=True)
class TBankInstrument:
    uid: str
    figi: str
    ticker: str
    class_code: str
    name: str
    instrument_type: str
    exchange: str
    currency: str
    api_trade_available: bool | None = None
    buy_available: bool | None = None
    sell_available: bool | None = None
    first_1day_candle_date: pd.Timestamp | None = None
    maturity_date: pd.Timestamp | None = None

    @property
    def instrument_id(self) -> str:
        return self.uid or self.figi or f"{self.ticker}_{self.class_code}".strip("_")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RussiaUniverseItem:
    asset_id: str
    ticker: str
    display_name: str
    asset_class: str
    sub_asset_class: str
    series_type: str
    return_variant: str
    sector_name: str = ""
    instrument_kind: str = INSTRUMENT_TYPE_INDEX
    expected_exchange_contains: str = "MOEX"
    currency: str = "RUB"
    unit: str = "index points"
    notes: str = "Loaded from T-Invest API Russia universe."


def instrument_from_payload(payload: dict[str, Any]) -> TBankInstrument:
    instrument_type_raw = first_present(payload, "instrumentType", "instrument_type", "instrumentKind", "instrument_kind")
    return TBankInstrument(
        uid=str(first_present(payload, "uid", "instrumentUid", "instrument_uid") or ""),
        figi=str(first_present(payload, "figi") or ""),
        ticker=str(first_present(payload, "ticker") or ""),
        class_code=str(first_present(payload, "classCode", "class_code") or ""),
        name=str(first_present(payload, "name") or ""),
        instrument_type=format_enum_label(instrument_type_raw),
        exchange=str(first_present(payload, "exchange") or ""),
        currency=str(first_present(payload, "currency") or ""),
        api_trade_available=first_present(payload, "apiTradeAvailableFlag", "api_trade_available_flag"),
        buy_available=first_present(payload, "buyAvailableFlag", "buy_available_flag"),
        sell_available=first_present(payload, "sellAvailableFlag", "sell_available_flag"),
        first_1day_candle_date=parse_timestamp(first_present(payload, "first1DayCandleDate", "first_1day_candle_date")),
        maturity_date=parse_timestamp(first_present(payload, "maturityDate", "maturity_date")),
    )


def list_indicatives(client: TBankClient) -> list[TBankInstrument]:
    payload = client.post(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/Indicatives",
        {},
    )
    return [instrument_from_payload(item) for item in list(first_present(payload, "instruments") or [])]


def find_instrument(client: TBankClient, query: str, instrument_kind: str = INSTRUMENT_TYPE_INDEX) -> list[TBankInstrument]:
    payload: dict[str, object] = {"query": query}
    if instrument_kind:
        payload["instrumentKind"] = instrument_kind
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument",
        payload,
    )
    return [instrument_from_payload(item) for item in list(first_present(response, "instruments") or [])]


def choose_best_instrument(
    instruments: list[TBankInstrument],
    *,
    ticker: str,
    expected_exchange_contains: str = "",
) -> TBankInstrument | None:
    if not instruments:
        return None

    normalized_ticker = ticker.strip().upper()
    exact_matches = [instrument for instrument in instruments if instrument.ticker.strip().upper() == normalized_ticker]
    candidates = exact_matches or instruments

    if expected_exchange_contains:
        exchange_matches = [
            instrument
            for instrument in candidates
            if expected_exchange_contains.upper() in instrument.exchange.upper()
        ]
        if exchange_matches:
            candidates = exchange_matches

    candidates = sorted(
        candidates,
        key=lambda instrument: (
            instrument.class_code == "",
            instrument.uid == "",
            instrument.name,
        ),
    )
    return candidates[0] if candidates else None


def resolve_universe(
    client: TBankClient,
    universe: list[RussiaUniverseItem],
) -> tuple[list[TBankInstrument], list[str]]:
    warnings: list[str] = []
    indicative_lookup = list_indicatives(client)
    resolved: list[TBankInstrument] = []

    for member in universe:
        direct_matches = [
            instrument
            for instrument in indicative_lookup
            if instrument.ticker.strip().upper() == member.ticker.upper()
        ]
        match = choose_best_instrument(
            direct_matches,
            ticker=member.ticker,
            expected_exchange_contains=member.expected_exchange_contains,
        )
        if match is None:
            search_matches = find_instrument(client, member.ticker, instrument_kind=member.instrument_kind)
            match = choose_best_instrument(
                search_matches,
                ticker=member.ticker,
                expected_exchange_contains=member.expected_exchange_contains,
            )

        if match is None:
            warnings.append(f"Instrument {member.ticker} was not found in T-Invest API.")
            continue
        resolved.append(match)

    return resolved, warnings

