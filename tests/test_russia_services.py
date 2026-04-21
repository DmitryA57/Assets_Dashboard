from __future__ import annotations

import pandas as pd

from src.data_sources.tbank_instruments import TBankInstrument
from src.services.russia_common import (
    RUSSIA_EQUITY_UNIVERSE,
    build_russia_kpi_cards,
    read_tinvest_connection_settings,
    read_tinvest_token,
    remap_candle_asset_ids,
)
from src.services.russia_equities import load_russia_equities_state


def test_remap_candle_asset_ids_matches_by_ticker_not_list_position() -> None:
    instruments = [
        TBankInstrument(
            uid="uid-rtsi",
            figi="figi-rtsi",
            ticker="RTSI",
            class_code="",
            name="RTSI",
            instrument_type="Index",
            exchange="MOEX",
            currency="RUB",
            first_1day_candle_date=pd.Timestamp("2020-01-01"),
        )
    ]
    candle_records = [
        {
            "date": pd.Timestamp("2026-04-20"),
            "asset_id": "uid-rtsi",
            "value": 1200.0,
            "source_timestamp": pd.Timestamp("2026-04-20T10:00:00"),
        }
    ]

    remapped = remap_candle_asset_ids(RUSSIA_EQUITY_UNIVERSE[:2], instruments, candle_records)

    assert remapped[0]["asset_id"] == "ru_eq_rtsi"


def test_build_russia_kpi_cards_counts_all_loaded_instruments() -> None:
    frame = pd.DataFrame(
        [
            {"display_name": "IMOEX", "ytd": 0.12, "comparison_eligible": True},
            {"display_name": "RTSI", "ytd": -0.03, "comparison_eligible": False},
        ]
    )

    cards = build_russia_kpi_cards(frame, metric="ytd", metric_title="YTD")
    card_map = {card.title: card.value for card in cards}

    assert card_map["Active Instruments"] == "2"
    assert card_map["Excluded / Stale"] == "1"


def test_read_tinvest_token_supports_nested_section() -> None:
    token = read_tinvest_token({"tbank": {"token": "secret-token"}})

    assert token == "secret-token"


def test_read_tinvest_connection_settings_supports_ssl_options() -> None:
    settings = read_tinvest_connection_settings(
        {
            "TINVEST_TOKEN": "secret-token",
            "TINVEST_TRUST_ENV": "true",
            "TINVEST_VERIFY_SSL": "true",
            "TINVEST_CA_BUNDLE": "C:/certs/root-ca.pem",
        }
    )

    assert settings.token == "secret-token"
    assert settings.trust_env is True
    assert settings.verify_ssl is True
    assert settings.ca_bundle_path == "C:/certs/root-ca.pem"


def test_load_russia_equities_state_from_export_files() -> None:
    state = load_russia_equities_state("", pd.Timestamp("2026-02-28"))

    assert state.data_source == "T-Bank export"
    assert len(state.snapshot) == 9
    assert "IMOEX" in state.snapshot["ticker"].tolist()
    assert "MOEXBMI" in state.snapshot["ticker"].tolist()
