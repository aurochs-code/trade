"""Health and diagnostics CLI commands."""

from __future__ import annotations

import importlib.util
import json
from datetime import timedelta
from pathlib import Path
from typing import Optional

import typer
from sqlalchemy.engine import make_url

from astock_trading.platform.cli.common import json_or_text, project_root
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.database import MissingDatabaseUrl
from astock_trading.platform.db import connect, get_schema_version, init_db
from astock_trading.platform.runs import RunJournal
from astock_trading.platform.time import MARKET_TZ, utc_now


def _resolve_vault_path() -> Optional[Path]:
    paths_file = project_root() / "config" / "paths.yaml"
    if not paths_file.exists():
        return None

    try:
        import yaml

        with open(paths_file, encoding="utf-8") as f:
            paths = yaml.safe_load(f) or {}
    except Exception:
        return None

    raw_path = paths.get("vault_path")
    if not raw_path:
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        path = paths_file.parent.parent / path
    return path


def _diagnostic_database_url(value: object) -> str:
    raw = str(value)
    try:
        return make_url(raw).render_as_string(hide_password=True)
    except Exception:
        return raw


def _latest_run_status(conn, run_type: str) -> dict:
    row = conn.execute(
        "SELECT run_id, status, started_at, finished_at, error_message "
        "FROM run_log WHERE run_type = ? ORDER BY started_at DESC LIMIT 1",
        (run_type,),
    ).fetchone()
    return dict(row) if row else {}


def _stale_running_rows(conn, older_than_hours: int) -> list[dict]:
    cutoff = (utc_now() - timedelta(hours=older_than_hours)).isoformat()
    rows = conn.execute(
        "SELECT run_id, run_type, started_at FROM run_log "
        "WHERE status = 'running' AND started_at < ? "
        "ORDER BY started_at DESC",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def register_health_commands(app: typer.Typer) -> None:
    @app.command("doctor")
    def doctor(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """环境自检：数据库、配置、vault、MCP 依赖。"""
        try:
            path = init_db()
        except MissingDatabaseUrl as e:
            result = {"status": "failed", "error": str(e)}
            json_or_text(result, as_json)
            raise typer.Exit(1)
        conn = connect()
        try:
            version = get_schema_version(conn)
            event_count = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
            run_count = conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0]
            config_count = conn.execute("SELECT COUNT(*) FROM config_versions").fetchone()[0]

            registry = ConfigRegistry()
            snapshot = registry.freeze(conn)

            vault_path = _resolve_vault_path()
            result = {
                "status": "ok",
                "db": {
                    "path": _diagnostic_database_url(path),
                    "schema_version": version,
                    "events": event_count,
                    "runs": run_count,
                    "config_versions": config_count,
                },
                "config": {
                    "version": snapshot.version,
                    "hash": snapshot.hash,
                },
                "vault": {
                    "path": str(vault_path) if vault_path else "",
                    "exists": bool(vault_path and vault_path.exists()),
                },
                "mcp": {
                    "installed": importlib.util.find_spec("mcp.server.fastmcp") is not None,
                },
                "timezone": str(MARKET_TZ),
            }

            if as_json:
                typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
                return

            typer.echo("A-Stock Trading Doctor")
            typer.echo(f"  DB: {result['db']['path']}")
            typer.echo(f"  Schema version: {result['db']['schema_version']}")
            typer.echo(
                f"  Events/Runs/Configs: "
                f"{result['db']['events']}/{result['db']['runs']}/{result['db']['config_versions']}"
            )
            typer.echo(
                f"  Config: {result['config']['version']} "
                f"(hash={result['config']['hash']})"
            )
            typer.echo(
                f"  Vault: {result['vault']['path'] or '未配置'} "
                f"(exists={result['vault']['exists']})"
            )
            typer.echo(f"  MCP installed: {result['mcp']['installed']}")
            typer.echo(f"  Business timezone: {result['timezone']}")
        finally:
            conn.close()

    @app.command("health")
    def health(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """运行健康检查：DB、近期 run、失败记录、数据源探针。"""
        from astock_trading.market.health import evaluate_data_source_health

        path = init_db()
        conn = connect()
        try:
            journal = RunJournal(conn)
            failed = journal.get_failed_runs(days=3)
            active_running_rows = conn.execute(
                "SELECT run_id, run_type, started_at FROM run_log "
                "WHERE status = 'running' AND started_at >= ? "
                "ORDER BY started_at DESC LIMIT 20",
                ((utc_now() - timedelta(hours=6)).isoformat(),),
            ).fetchall()
            stale_running = _stale_running_rows(conn, older_than_hours=6)
            data_source_rows = conn.execute(
                "SELECT kind, symbol, MAX(observed_at) AS observed_at "
                "FROM market_observations GROUP BY kind, symbol "
                "ORDER BY observed_at DESC LIMIT 20"
            ).fetchall()
            required_runs = ["morning", "evening", "scoring", "intraday_monitor", "weekly"]
            latest_runs = {run_type: _latest_run_status(conn, run_type) for run_type in required_runs}
            data_source_health = evaluate_data_source_health(conn)
            result = {
                "status": "failed"
                if data_source_health["status"] == "failed"
                else "warning"
                if failed or active_running_rows or stale_running or data_source_health["status"] == "warning"
                else "ok",
                "db": {
                    "path": _diagnostic_database_url(path),
                    "schema_version": get_schema_version(conn),
                    "events": conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0],
                    "runs": conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0],
                },
                "runs": {
                    "latest": latest_runs,
                    "failed_3d": [dict(r) for r in failed[:10]],
                    "running": [dict(r) for r in active_running_rows],
                    "stale_running": stale_running[:20],
                },
                "data_sources": data_source_health
                | {"recent_observations": [dict(r) for r in data_source_rows]},
                "vault": {
                    "path": str(_resolve_vault_path() or ""),
                    "exists": bool(_resolve_vault_path() and _resolve_vault_path().exists()),
                },
            }
            if as_json:
                typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
                return
            typer.echo(f"Health: {result['status']}")
            typer.echo(f"DB: {result['db']['path']}")
            typer.echo(f"Failed runs in 3d: {len(result['runs']['failed_3d'])}")
            typer.echo(f"Running runs: {len(result['runs']['running'])}")
            typer.echo(f"Stale running runs: {len(result['runs']['stale_running'])}")
            typer.echo(f"Recent data observations: {len(result['data_sources']['recent_observations'])}")
        finally:
            conn.close()
