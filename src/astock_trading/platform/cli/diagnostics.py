"""Agent-facing diagnostics CLI commands."""

from __future__ import annotations

import typer

from astock_trading.platform.agent_diagnostics import (
    diagnose_health,
    explain_run,
    propose_agent_trade_plan,
)
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db


diagnose_app = typer.Typer(name="diagnose", help="Agent 诊断命令")


@diagnose_app.command("health")
def diagnose_health_cmd(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """诊断运行健康、数据源和候选池状态，不执行交易。"""
    init_db()
    conn = connect()
    try:
        json_or_text(diagnose_health(conn), as_json)
    finally:
        conn.close()


def register_diagnostics_commands(app: typer.Typer) -> None:
    app.add_typer(diagnose_app)

    @app.command("explain-run")
    def explain_run_cmd(
        run_id: str = typer.Argument(..., help="run_id"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """解释单次 pipeline run 的状态、事件和失败原因。"""
        init_db()
        conn = connect()
        try:
            payload = explain_run(conn, run_id)
            json_or_text(payload, as_json)
            if payload.get("status") == "not_found":
                raise typer.Exit(1)
        finally:
            conn.close()

    @app.command("propose-plan")
    def propose_plan_cmd(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """生成只读 Agent 交易计划，不执行任何交易动作。"""
        init_db()
        conn = connect()
        try:
            json_or_text(propose_agent_trade_plan(conn), as_json)
        finally:
            conn.close()
