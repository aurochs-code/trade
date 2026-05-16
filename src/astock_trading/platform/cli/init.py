"""Initialization commands for installed CLI use."""

from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path
from typing import Any

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.paths import (
    default_cache_dir,
    default_config_dir,
    default_data_dir,
    default_state_dir,
)


def _template_dir():
    return resources.files("astock_trading").joinpath("templates", "config")


def _copy_template(name: str, target: Path, force: bool) -> str:
    source = _template_dir().joinpath(name)
    existed = target.exists()
    if target.exists() and not force:
        return "exists"
    target.parent.mkdir(parents=True, exist_ok=True)
    with resources.as_file(source) as source_path:
        shutil.copyfile(source_path, target)
    return "updated" if existed else "created"


def _write_env_from_example(config_dir: Path, force: bool) -> str:
    env_path = config_dir / ".env"
    existed = env_path.exists()
    if env_path.exists() and not force:
        return "exists"
    source = _template_dir().joinpath(".env.example")
    with resources.as_file(source) as source_path:
        env_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    return "updated" if existed else "created"


def _init_result(config_dir: Path, force: bool) -> dict[str, Any]:
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir = default_data_dir()
    state_dir = default_state_dir()
    cache_dir = default_cache_dir()
    log_dir = state_dir / "logs"
    cron_log_dir = log_dir / "cron"
    launchd_log_dir = log_dir / "launchd"
    for path in (data_dir, state_dir, cache_dir, log_dir, cron_log_dir, launchd_log_dir):
        path.mkdir(parents=True, exist_ok=True)

    copied: dict[str, str] = {}
    for name in (
        ".env.example",
        "strategy.yaml",
        "stocks.yaml",
        "notification.yaml",
        "paths.yaml",
        "mcp_server.yaml",
        "profiles/trend_swing.yaml",
        "profiles/short_continuation.yaml",
        "profiles/defensive_watch.yaml",
    ):
        copied[name] = _copy_template(name, config_dir / name, force)
    copied[".env"] = _write_env_from_example(config_dir, force)

    return {
        "status": "ok",
        "config_dir": str(config_dir),
        "data_dir": str(data_dir),
        "state_dir": str(state_dir),
        "cache_dir": str(cache_dir),
        "files": copied,
        "commands": {
            "doctor": "atrade doctor --json",
            "health": "atrade health --json",
            "mcp": "atrade mcp",
        },
        "next_steps": [
            f"Edit {config_dir / '.env'} and set ASTOCK_DATABASE_URL.",
            "Run atrade doctor --json.",
        ],
    }


def register_init_command(app: typer.Typer) -> None:
    @app.command("init")
    def init_command(
        config_dir: Path = typer.Option(
            default_config_dir(),
            "--config-dir",
            help="配置目录，默认 ~/.config/a-stock-trading",
        ),
        force: bool = typer.Option(False, "--force", help="覆盖已存在模板文件"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """初始化全局 atrade 配置目录和默认模板。"""
        result = _init_result(config_dir.expanduser(), force)
        json_or_text(result, as_json)
