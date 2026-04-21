from __future__ import annotations

import pandas as pd

from src.data_sources.tbank_instruments import TBankInstrument, choose_best_instrument


def test_choose_best_instrument_prefers_expected_exchange() -> None:
    moex = TBankInstrument(
        uid="uid-moex",
        figi="figi-moex",
        ticker="IMOEX",
        class_code="",
        name="MOEX Russia Index",
        instrument_type="Index",
        exchange="MOEX",
        currency="RUB",
        first_1day_candle_date=pd.Timestamp("2020-01-01"),
    )
    other = TBankInstrument(
        uid="uid-other",
        figi="figi-other",
        ticker="IMOEX",
        class_code="",
        name="Alternate IMOEX",
        instrument_type="Index",
        exchange="OTHER",
        currency="RUB",
        first_1day_candle_date=pd.Timestamp("2020-01-01"),
    )

    selected = choose_best_instrument([other, moex], ticker="IMOEX", expected_exchange_contains="MOEX")

    assert selected is not None
    assert selected.uid == "uid-moex"

