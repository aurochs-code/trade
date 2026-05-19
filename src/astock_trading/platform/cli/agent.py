"""Agent-facing CLI context."""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text


def register_agent_context(app: typer.Typer) -> None:
    @app.command("agent-context")
    def agent_context(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """输出给 Agent 使用的安全入口和约束。"""
        payload = {
            "project": "a-stock-trading",
            "safe_entrypoints": ["atrade", "atrade mcp", "bin/trade", "bin/trade mcp"],
            "forbidden_entrypoints": ["src/astock_trading/**/*.py"],
            "database": {
                "runtime_env": "ASTOCK_DATABASE_URL",
                "runtime_required": True,
                "migration_source": "archived SQLite path only; not kept in checkout",
            },
            "recommended_commands": {
                "health": "atrade health --json",
                "diagnose_health": "atrade diagnose health --json",
                "diagnose_strategy": "atrade diagnose strategy --json",
                "events": "atrade events query --json",
                "runs": "atrade runs list --json",
                "portfolio": "atrade status --json",
                "screener": "atrade screener candidates --json",
                "screener_explain": "atrade screener explain --json",
                "screener_refresh": "atrade screener refresh --json",
                "screener_run": "atrade screener run --query '...' --json",
                "stock_analyze": "atrade stock analyze CODE_OR_NAME --json",
                "market_intel": "atrade market-intel brief --query '今天热点新闻和强势板块' --json",
                "market_news_search": "atrade market-intel search KEYWORD --json",
                "record_buy": "atrade record-buy CODE SHARES PRICE --yes --json",
                "record_sell": "atrade record-sell CODE SHARES PRICE --yes --json",
                "manual_trades": "atrade manual-trades list --json",
                "paper": "atrade paper status --json",
                "db_status": "atrade db status --json",
                "db_tables": "atrade db tables --json",
                "db_check": "atrade db check --json",
                "db_backup": (
                    "atrade db backup --output ~/.local/state/a-stock-trading/backups/astock.sql "
                    "--docker-container astock-mysql --yes --json"
                ),
            },
            "rules": [
                "Do not execute Python files under src/astock_trading directly.",
                "Use CLI/MCP commands for all reads and writes.",
                "Use --json for machine-readable command output.",
            ],
        }
        json_or_text(payload, as_json)
