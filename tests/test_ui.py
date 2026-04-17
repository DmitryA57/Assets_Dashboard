from __future__ import annotations

import pandas as pd

from src.ui import _normalized_axis_domain, _prepare_ranked_bar_chart


def test_prepare_ranked_bar_chart_orders_labels_descending() -> None:
    frame = pd.DataFrame(
        [
            {"display_name": "Copper", "ytd": 0.044},
            {"display_name": "Lithium, LCE", "ytd": 0.346},
            {"display_name": "Cobalt", "ytd": 0.055},
            {"display_name": "Aluminium", "ytd": 0.235},
        ]
    )

    chart_frame, label_order = _prepare_ranked_bar_chart(frame, metric="ytd", label_column="display_name", limit=10)

    assert chart_frame["display_name"].tolist() == ["Lithium, LCE", "Aluminium", "Cobalt", "Copper"]
    assert label_order == ["Lithium, LCE", "Aluminium", "Cobalt", "Copper"]


def test_prepare_ranked_bar_chart_keeps_greater_negative_values_first() -> None:
    frame = pd.DataFrame(
        [
            {"display_name": "Henry Hub", "ytd": -0.187},
            {"display_name": "Hang Seng", "ytd": -0.005},
            {"display_name": "DAX", "ytd": -0.033},
        ]
    )

    _, label_order = _prepare_ranked_bar_chart(frame, metric="ytd", label_column="display_name", limit=10)

    assert label_order == ["Hang Seng", "DAX", "Henry Hub"]


def test_normalized_axis_domain_zooms_to_data_band_instead_of_zero() -> None:
    history = pd.DataFrame(
        [
            {"display_name": "A", "normalized": 101.2},
            {"display_name": "A", "normalized": 108.4},
            {"display_name": "B", "normalized": 99.5},
            {"display_name": "B", "normalized": 111.1},
        ]
    )

    domain = _normalized_axis_domain(history)

    assert domain is not None
    domain_min, domain_max = domain
    assert domain_min > 90
    assert domain_max < 120
    assert domain_min < 100 < domain_max
