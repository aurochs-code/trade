"""Agent-facing diagnostics CLI commands."""

from __future__ import annotations

from pathlib import Path

import typer

from astock_trading.platform.agent_diagnostics import (
    diagnose_flow,
    diagnose_health,
    diagnose_schedule,
    diagnose_strategy,
    explain_run,
    propose_agent_trade_plan,
)
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.llm_context import build_llm_context, render_llm_context_markdown


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


@diagnose_app.command("strategy")
def diagnose_strategy_cmd(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """诊断选股、评分、决策门控和参数 profile，不执行交易。"""
    init_db()
    conn = connect()
    try:
        json_or_text(diagnose_strategy(conn), as_json)
    finally:
        conn.close()


@diagnose_app.command("flow")
def diagnose_flow_cmd(
    include_account: bool = typer.Option(False, "--include-account", help="读取 MX 模拟盘账户；默认只查本地证据"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """诊断候选召回、策略闸门、机会卡和模拟承接链路，不执行交易。"""
    from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
    from astock_trading.pipeline.context import build_context

    ctx = build_context()
    try:
        auto_readiness = build_auto_trade_readiness(ctx, include_account=include_account)
        json_or_text(diagnose_flow(ctx.conn, auto_readiness=auto_readiness), as_json)
    finally:
        ctx.conn.close()


@diagnose_app.command("schedule")
def diagnose_schedule_cmd(
    jobs_path: Path | None = typer.Option(None, "--jobs-path", help="Hermes jobs.json 路径"),
    env_file: Path | None = typer.Option(None, "--env-file", help="atrade 运行 .env 路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """诊断 Hermes trading 调度是否漏跑关键盘中任务，不执行补跑。"""
    init_db()
    conn = connect()
    try:
        json_or_text(diagnose_schedule(conn, jobs_path=jobs_path, env_file=env_file), as_json)
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

    @app.command("llm-context")
    def llm_context_cmd(
        mode: str = typer.Option("close", "--mode", help="morning / close / weekly"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """生成 Hermes/LLM 摘要用的只读上下文，不执行交易动作。"""
        if mode not in {"morning", "close", "weekly"}:
            raise typer.BadParameter("--mode must be morning, close, or weekly")
        init_db()
        conn = connect()
        try:
            payload = build_llm_context(conn, mode=mode)
            json_or_text(payload if as_json else render_llm_context_markdown(payload), as_json)
        finally:
            conn.close()
