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
            "safe_entrypoints": ["bin/trade", "bin/trade mcp"],
            "forbidden_entrypoints": ["src/astock_trading/**/*.py"],
            "database": {
                "runtime_env": "ASTOCK_DATABASE_URL",
                "runtime_required": True,
                "migration_source": "data/astock_trading.db",
            },
            "recommended_commands": {
                "health": "bin/trade health --json",
                "events": "bin/trade events query --json",
                "runs": "bin/trade runs list --json",
                "portfolio": "bin/trade status --json",
                "screener": "bin/trade screener candidates --json",
                "screener_run": "bin/trade screener run --query '...' --json",
                "record_buy": "bin/trade record-buy CODE SHARES PRICE --yes --json",
                "record_sell": "bin/trade record-sell CODE SHARES PRICE --yes --json",
                "manual_trades": "bin/trade manual-trades list --json",
                "paper": "bin/trade paper status --json",
                "db_status": "bin/trade db status --json",
                "db_tables": "bin/trade db tables --json",
                "db_check": "bin/trade db check --json",
                "db_backup": (
                    "bin/trade db backup --output data/backups/astock.sql "
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
