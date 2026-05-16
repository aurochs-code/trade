"""Pipeline execution CLI commands."""

from __future__ import annotations

import json

import typer

from astock_trading.platform.pipeline_runner import PIPELINE_HELP, execute_pipeline
from astock_trading.platform.time import is_trading_day


def register_pipeline_commands(app: typer.Typer) -> None:
    @app.command("run-pipeline")
    def run_pipeline(
        pipeline_type: str = typer.Argument(..., help=PIPELINE_HELP),
        ignore_data_source_health: bool = typer.Option(
            False,
            "--ignore-data-source-health",
            help="忽略数据源健康 gate，强制运行依赖行情的 pipeline",
        ),
        json_output: bool = typer.Option(
            False,
            "--json",
            help="输出 JSON，便于自动化调用",
        ),
    ):
        """运行指定 pipeline（完整流程，带幂等检查）。

        Pipelines:
        morning
        noon
        intraday_monitor
        evening
        scoring
        weekly
        monthly
        sentiment
        auto_trade
        """
        from astock_trading.pipeline.context import build_context

        ctx = build_context()
        try:
            outcome = execute_pipeline(
                ctx,
                pipeline_type,
                is_trading_day=is_trading_day(),
                ignore_data_source_health=ignore_data_source_health,
                on_started=None if json_output else _echo_started,
                on_data_source_warning=None if json_output else _echo_data_source_warning,
            )

            if json_output:
                typer.echo(json.dumps(outcome, ensure_ascii=False, default=str))
            elif outcome["status"] == "skipped":
                typer.echo(outcome["message"])
            elif outcome["status"] == "completed":
                _echo_pipeline_summary(pipeline_type, outcome.get("result", {}))
                typer.echo(f"{pipeline_type} 完成")
            elif outcome.get("reason") == "data_source_health_failed":
                typer.echo(outcome["message"], err=True)
                raise typer.Exit(1)
            else:
                detail = outcome.get("error", outcome.get("message", "unknown"))
                typer.echo(f"{pipeline_type} 失败: {detail}", err=True)
                raise typer.Exit(1)

            if json_output and outcome["status"] == "failed":
                raise typer.Exit(1)
        finally:
            ctx.conn.close()

    @app.command("refresh-positions")
    def refresh_positions_cmd():
        """刷新持仓实时价格并写 DB（自动跳过缓存未过期的）。"""
        from astock_trading.pipeline.context import build_context

        ctx = build_context()
        try:
            from astock_trading.pipeline.helpers import refresh_position_prices

            prices = refresh_position_prices(ctx)
            if not prices:
                typer.echo("无持仓")
            else:
                typer.echo(f"已刷新 {len(prices)} 只持仓:")
                for code, price in prices.items():
                    typer.echo(f"  {code}  ¥{price:.2f}")
        finally:
            ctx.conn.close()


def _echo_started(pipeline_type: str, run_id: str, _payload: dict | None) -> None:
    typer.echo(f"{pipeline_type} 开始 (run_id={run_id})")


def _echo_data_source_warning(_pipeline_type: str, _run_id: str, payload: dict | None) -> None:
    typer.echo((payload or {}).get("message", "辅助数据源降级，继续运行"))


def _echo_pipeline_summary(pipeline_type: str, result: dict) -> None:
    if pipeline_type == "morning":
        typer.echo(
            f"  大盘={result['signal']} "
            f"持仓={result['positions']} 风控={len(result['risk_alerts'])}条"
        )
    elif pipeline_type == "noon":
        typer.echo(
            f"  大盘={result['signal']} "
            f"持仓={result['positions']} 风控={len(result['alerts'])}条"
        )
    elif pipeline_type == "intraday_monitor":
        typer.echo(
            f"  持仓={result['positions']} "
            f"新告警={len(result['alerts'])}条 去重={result['deduped']}条"
        )
    elif pipeline_type == "scoring":
        typer.echo(f"  评分 {result['scored']} 只股票")
    elif pipeline_type == "evening":
        typer.echo(
            f"  大盘={result['signal']} "
            f"持仓={result['positions']} 风控={len(result['risk_alerts'])}条"
        )
    elif pipeline_type == "weekly":
        typer.echo(
            f"  {result['buy_count']}买 "
            f"{result['sell_count']}卖 胜率{result['win_rate']:.0%}"
        )
    elif pipeline_type == "monthly":
        typer.echo("  月度复盘已生成")
    elif pipeline_type == "sentiment":
        typer.echo(f"  监控{result['monitored']}只 告警{len(result['alerts'])}条")
    elif pipeline_type == "auto_trade":
        if not result.get("enabled"):
            typer.echo("auto_trade 未启用")
        else:
            mode = "[DRY]" if result.get("dry_run") else ""
            typer.echo(f"  {mode} 买入{len(result['buys'])}笔 卖出{len(result['sells'])}笔")
