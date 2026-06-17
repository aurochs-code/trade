"""系统化运维 CLI。"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.ops_watchdog import build_ops_watchdog, build_ops_watchdog_context as build_context


ops_app = typer.Typer(name="ops", help="系统化运维与排障")


@ops_app.command("watchdog")
def ops_watchdog_cmd(
    include_account: bool = typer.Option(False, "--include-account", help="读取 MX 模拟盘账户；默认只查本地证据"),
    jobs_path: Path | None = typer.Option(None, "--jobs-path", help="Hermes jobs.json 路径"),
    env_file: Path | None = typer.Option(None, "--env-file", help="atrade 运行 .env 路径"),
    fail_on_critical: bool = typer.Option(False, "--fail-on-critical", help="发现 critical 时以退出码 2 结束"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """系统化运维 watchdog：聚合调度、数据源、候选池和模拟承接断点。"""
    ctx = build_context()
    try:
        payload = build_ops_watchdog(
            ctx,
            include_account=include_account,
            jobs_path=jobs_path,
            env_file=env_file,
        )
        json_or_text(payload, as_json)
        if fail_on_critical and payload.get("status") == "critical":
            raise typer.Exit(2)
    finally:
        ctx.conn.close()


@ops_app.command("watchdog-supervise")
def ops_watchdog_supervise_cmd(
    timeout_seconds: int = typer.Option(60, "--timeout-seconds", min=5, help="子进程总超时秒数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """监督运行 notify ops-watchdog；超时会杀掉子进程并返回失败 JSON。"""
    child_runtime = max(5, timeout_seconds - 5)
    command = [
        sys.argv[0],
        "notify",
        "ops-watchdog",
        "--max-runtime-seconds",
        str(child_runtime),
        "--json",
    ]
    env = os.environ.copy()
    env.setdefault("ASTOCK_DISCORD_TIMEOUT_SECONDS", "5")
    env.setdefault("ASTOCK_DISCORD_MAX_RETRIES", "1")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
        )
    except subprocess.TimeoutExpired:
        payload = {
            "command": "ops watchdog-supervise",
            "status": "failed",
            "reason": "ops_watchdog_timeout",
            "timeout_seconds": timeout_seconds,
            "child_command": " ".join(command),
            "guardrails": {
                "read_only": False,
                "writes_order": False,
                "runs_pipeline": False,
            },
        }
        json_or_text(payload, as_json)
        raise typer.Exit(124)

    payload = _supervised_payload(completed, command)
    json_or_text(payload, as_json)
    if completed.returncode != 0:
        raise typer.Exit(completed.returncode)


def _supervised_payload(completed: subprocess.CompletedProcess[str], command: list[str]) -> dict:
    try:
        child = json.loads(completed.stdout) if completed.stdout.strip() else {}
    except json.JSONDecodeError:
        child = {
            "status": "unparsed",
            "stdout_tail": completed.stdout[-2000:],
        }
    return {
        "command": "ops watchdog-supervise",
        "status": "ok" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "child_command": " ".join(command),
        "child": child,
        "stderr_tail": completed.stderr[-2000:],
        "guardrails": {
            "read_only": False,
            "writes_order": False,
            "runs_pipeline": False,
        },
    }
