from __future__ import annotations

import pandas as pd

from src.services.russia_common import RussiaMarketState
from src.services.russia_export_service import load_russia_export_market_state


def load_russia_bonds_state(token: str, reference_date: pd.Timestamp) -> RussiaMarketState:
    return load_russia_export_market_state("bonds", reference_date)
