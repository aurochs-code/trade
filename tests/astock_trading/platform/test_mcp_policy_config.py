"""MCP tool governance configuration tests."""

from __future__ import annotations

import re
from pathlib import Path

import yaml


def test_mcp_policy_classifies_all_trade_tools():
    root = Path(__file__).resolve().parents[3]
    source = (root / "src" / "astock_trading" / "platform" / "mcp_server.py").read_text()
    configured = yaml.safe_load((root / "config" / "mcp_server.yaml").read_text())

    tool_names = set(re.findall(r"^def (trade_[a-zA-Z0-9_]+)\(", source, flags=re.MULTILINE))
    categories = configured["tool_policy"]["categories"]
    classified = {
        tool
        for category in categories.values()
        for tool in category.get("tools", [])
    }

    assert tool_names <= classified


def test_mcp_policy_requires_confirmation_for_side_effect_tools():
    root = Path(__file__).resolve().parents[3]
    configured = yaml.safe_load((root / "config" / "mcp_server.yaml").read_text())

    categories = configured["tool_policy"]["categories"]
    require_confirm = set(configured["require_confirm"]["categories"])
    state_change = set(categories["state_change"]["tools"])
    high_risk = set(categories["high_risk"]["tools"])

    assert {"state_change", "high_risk"} <= require_confirm
    assert "trade_auto_trade" in high_risk
    assert {"trade_mock_buy", "trade_mock_sell", "trade_mock_cancel"} <= high_risk
    assert {"trade_run_pipeline", "trade_screener", "trade_fetch_history", "trade_watchlist_manage"} <= state_change
