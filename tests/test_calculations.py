from __future__ import annotations

from src.compute_snapshot import compute_bps_change, compute_percent_change


def test_compute_percent_change() -> None:
    assert compute_percent_change(110, 100) == 0.1


def test_compute_percent_change_rejects_zero_base() -> None:
    assert compute_percent_change(110, 0) is None


def test_compute_bps_change() -> None:
    assert compute_bps_change(4.5, 4.0) == 50.0

