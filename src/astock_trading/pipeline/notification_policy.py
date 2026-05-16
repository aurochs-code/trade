"""Notification noise-control policies for pipelines."""

from __future__ import annotations


def should_push_sector_heatmap(sectors: list[dict], *, phase: str) -> bool:
    """Return whether a sector heatmap deserves its own Discord card."""
    if not sectors:
        return False
    if phase in {"close", "evening"}:
        return True

    max_abs_change = max(abs(_to_float(item.get("change_pct"))) for item in sectors)
    return max_abs_change >= 3.0


def _to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
