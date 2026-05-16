"""Agent-facing MCP tool payload builders."""

from __future__ import annotations

from typing import Any

from astock_trading.platform.agent_diagnostics import (
    diagnose_health,
    explain_run,
    propose_agent_trade_plan,
)


def diagnose_health_payload(conn: Any) -> dict:
    """Return the payload for trade_diagnose_health."""
    return diagnose_health(conn)


def explain_run_payload(conn: Any, run_id: str) -> dict:
    """Return the payload for trade_explain_run."""
    return explain_run(conn, run_id)


def propose_plan_payload(conn: Any) -> dict:
    """Return the payload for trade_propose_plan."""
    return propose_agent_trade_plan(conn)
