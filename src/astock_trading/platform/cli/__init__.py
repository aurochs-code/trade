"""Typer CLI entrypoint."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import typer

from astock_trading.platform.cli.agent import register_agent_context
from astock_trading.platform.cli.calibration import register_calibration_command
from astock_trading.platform.cli.config import config_app
from astock_trading.platform.cli.dashboard import dashboard_app
from astock_trading.platform.cli.data_sources import data_sources_app, register_check_data_sources
from astock_trading.platform.cli.db import db_app
from astock_trading.platform.cli.diagnostics import register_diagnostics_commands
from astock_trading.platform.cli.events import events_app
from astock_trading.platform.cli.health import register_health_commands
from astock_trading.platform.cli.hermes import register_hermes_commands
from astock_trading.platform.cli.history import history_app
from astock_trading.platform.cli.init import register_init_command
from astock_trading.platform.cli.manual_trades import manual_trades_app
from astock_trading.platform.cli.market_intel import market_intel_app
from astock_trading.platform.cli.market_data import register_market_data_commands
from astock_trading.platform.cli.notifications import notify_app
from astock_trading.platform.cli.ops import ops_app
from astock_trading.platform.cli.paper import paper_app
from astock_trading.platform.cli.pipelines import register_pipeline_commands
from astock_trading.platform.cli.research import register_research_commands
from astock_trading.platform.cli.review import review_app
from astock_trading.platform.cli.risk import risk_app
from astock_trading.platform.cli.runs import runs_app
from astock_trading.platform.cli.screener import screener_app
from astock_trading.platform.cli.stock import stock_app
from astock_trading.platform.cli.strategy_profiles import strategy_app
from astock_trading.platform.cli.trading import register_trading_commands
from astock_trading.platform.database import MissingDatabaseUrl
from astock_trading.platform.runtime_env import load_runtime_env


app = typer.Typer(name="trade", help="A-Stock Trading 交易系统 CLI")

app.add_typer(db_app)
app.add_typer(config_app)
app.add_typer(dashboard_app)
app.add_typer(runs_app)
app.add_typer(events_app)
app.add_typer(data_sources_app)
app.add_typer(manual_trades_app)
app.add_typer(paper_app)
app.add_typer(risk_app)
app.add_typer(screener_app)
app.add_typer(stock_app)
app.add_typer(strategy_app)
app.add_typer(notify_app)
app.add_typer(ops_app)
app.add_typer(market_intel_app)
app.add_typer(history_app)
app.add_typer(review_app)
register_agent_context(app)
register_calibration_command(app)
register_init_command(app)
register_diagnostics_commands(app)
register_health_commands(app)
register_hermes_commands(app)
register_market_data_commands(app)
register_check_data_sources(app)
register_pipeline_commands(app)
register_research_commands(app)
register_trading_commands(app)


@app.command("mcp")
def run_mcp():
    """启动 MCP Server（stdio transport）：atrade mcp"""
    from astock_trading.platform.mcp_server import main as mcp_main

    mcp_main()


def main():
    load_runtime_env()
    try:
        app(prog_name=Path(sys.argv[0]).name)
    except MissingDatabaseUrl as exc:
        if "--json" in sys.argv:
            typer.echo(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        else:
            typer.secho(str(exc), fg="red")
        sys.exit(1)


if __name__ == "__main__":
    main()
