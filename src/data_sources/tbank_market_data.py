from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from src.data_sources.tbank_client import TBankClient, first_present, format_enum_label, parse_timestamp, quotation_to_float


DAY_CANDLE_INTERVAL = "CANDLE_INTERVAL_DAY"


@dataclass(frozen=True, slots=True)
class TBankDailyCandle:
    instrument_id: str
    date: pd.Timestamp
    value: float
    source_timestamp: pd.Timestamp

    def to_record(self) -> dict[str, object]:
        return {
            "date": self.date,
            "asset_id": self.instrument_id,
            "value": self.value,
            "source_timestamp": self.source_timestamp,
        }


def fetch_daily_candles(
    client: TBankClient,
    instrument_id: str,
    *,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 2400,
) -> list[TBankDailyCandle]:
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
        {
            "instrumentId": instrument_id,
            "from": from_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "to": to_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "interval": DAY_CANDLE_INTERVAL,
            "limit": int(limit),
        },
    )

    fetched_at = pd.Timestamp.utcnow().tz_localize(None)
    candles: list[TBankDailyCandle] = []
    for candle in list(first_present(response, "candles") or []):
        timestamp = parse_timestamp(first_present(candle, "time"))
        close_price = quotation_to_float(first_present(candle, "close"))
        if timestamp is None or close_price is None:
            continue
        candles.append(
            TBankDailyCandle(
                instrument_id=instrument_id,
                date=timestamp.normalize(),
                value=close_price,
                source_timestamp=fetched_at,
            )
        )
    return candles


def fetch_last_prices(client: TBankClient, instrument_ids: list[str]) -> dict[str, float]:
    if not instrument_ids:
        return {}
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices",
        {
            "instrumentId": instrument_ids,
            "lastPriceType": "LAST_PRICE_EXCHANGE",
        },
    )
    prices: dict[str, float] = {}
    for item in list(first_present(response, "lastPrices", "last_prices") or []):
        instrument_id = str(first_present(item, "instrumentUid", "instrument_uid", "instrumentId", "instrument_id", "figi") or "")
        price = quotation_to_float(first_present(item, "price"))
        if instrument_id and price is not None:
            prices[instrument_id] = price
    return prices


def fetch_close_prices(client: TBankClient, instrument_ids: list[str]) -> dict[str, float]:
    if not instrument_ids:
        return {}
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.MarketDataService/GetClosePrices",
        {
            "instruments": [{"instrumentId": instrument_id} for instrument_id in instrument_ids],
        },
    )
    prices: dict[str, float] = {}
    for item in list(first_present(response, "closePrices", "close_prices") or []):
        instrument_id = str(first_present(item, "instrumentUid", "instrument_uid", "instrumentId", "instrument_id", "figi") or "")
        price = quotation_to_float(first_present(item, "price", "closePrice", "close_price"))
        if instrument_id and price is not None:
            prices[instrument_id] = price
    return prices


def fetch_trading_statuses(client: TBankClient, instrument_ids: list[str]) -> dict[str, str]:
    if not instrument_ids:
        return {}
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.MarketDataService/GetTradingStatuses",
        {
            "instrumentId": instrument_ids,
        },
    )
    statuses: dict[str, str] = {}
    items = list(first_present(response, "tradingStatuses", "trading_statuses") or [])
    for item in items:
        instrument_id = str(first_present(item, "instrumentUid", "instrument_uid", "instrumentId", "instrument_id", "figi") or "")
        raw_status = first_present(item, "tradingStatus", "trading_status")
        if instrument_id and raw_status is not None:
            statuses[instrument_id] = format_enum_label(raw_status)
    return statuses


def fetch_today_schedule_status(client: TBankClient) -> str:
    now_utc = datetime.now(timezone.utc)
    response = client.post(
        "tinkoff.public.invest.api.contract.v1.InstrumentsService/TradingSchedules",
        {
            "from": now_utc.isoformat().replace("+00:00", "Z"),
            "to": (now_utc + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
        },
    )
    exchanges = list(first_present(response, "exchanges") or [])
    for exchange in exchanges:
        exchange_name = str(first_present(exchange, "exchange") or "")
        if "MOEX" not in exchange_name.upper():
            continue
        for day in list(first_present(exchange, "days") or []):
            day_timestamp = parse_timestamp(first_present(day, "date"))
            if day_timestamp is None or day_timestamp.normalize() != pd.Timestamp.utcnow().tz_localize(None).normalize():
                continue
            is_trading_day = bool(first_present(day, "isTradingDay", "is_trading_day"))
            return f"{exchange_name}: {'Trading day' if is_trading_day else 'Closed'}"
    return ""

