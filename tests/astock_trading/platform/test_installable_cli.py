"""Tests for installable/global CLI behavior."""

from __future__ import annotations

import json
import os
import subprocess
import tomllib
from pathlib import Path

import yaml


def test_pyproject_exposes_atrade_console_script():
    root = Path(__file__).resolve().parents[3]

    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    scripts = project["project"]["scripts"]
    assert scripts["atrade"] == "astock_trading.platform.cli:main"
    assert scripts["astock-trading"] == "astock_trading.platform.cli:main"


def test_runtime_env_loads_config_dir_env_without_overriding_process_env(tmp_path, monkeypatch):
    from astock_trading.platform.runtime_env import load_runtime_env

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / ".env").write_text(
        "ASTOCK_DATABASE_URL=mysql+pymysql://user:pass@127.0.0.1:3306/from_config\n"
        "MX_APIKEY=from-config\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ASTOCK_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("MX_APIKEY", "already-set")
    monkeypatch.delenv("ASTOCK_DATABASE_URL", raising=False)

    loaded = load_runtime_env()

    assert loaded == config_dir / ".env"
    assert os.environ["ASTOCK_DATABASE_URL"] == (
        "mysql+pymysql://user:pass@127.0.0.1:3306/from_config"
    )
    assert os.environ["MX_APIKEY"] == "already-set"


def test_runtime_env_uses_explicit_env_parent_as_config_dir(tmp_path, monkeypatch):
    from astock_trading.platform.runtime_env import load_runtime_env

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / ".env").write_text(
        "ASTOCK_DATABASE_URL=mysql+pymysql://user:pass@127.0.0.1:3306/from_config\n",
        encoding="utf-8",
    )
    (config_dir / "strategy.yaml").write_text(
        "scoring:\n"
        "  weights:\n"
        "    technical: 3\n"
        "    fundamental: 2\n"
        "    flow: 2\n"
        "    sentiment: 3\n"
        "  thresholds:\n"
        "    buy: 5.5\n"
        "    watch: 5.0\n"
        "    reject: 3.0\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ASTOCK_ENV_FILE", str(config_dir / ".env"))
    monkeypatch.delenv("ASTOCK_CONFIG_DIR", raising=False)
    monkeypatch.delenv("ASTOCK_DATABASE_URL", raising=False)

    loaded = load_runtime_env()

    assert loaded == config_dir / ".env"
    assert os.environ["ASTOCK_CONFIG_DIR"] == str(config_dir)
    assert os.environ["ASTOCK_DATABASE_URL"] == (
        "mysql+pymysql://user:pass@127.0.0.1:3306/from_config"
    )


def test_config_registry_uses_astock_config_dir(tmp_path, monkeypatch):
    from astock_trading.platform.config import ConfigRegistry

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "strategy.yaml").write_text(
        "scoring:\n"
        "  weights:\n"
        "    technical: 3\n"
        "    fundamental: 2\n"
        "    flow: 2\n"
        "    sentiment: 3\n"
        "  thresholds:\n"
        "    buy: 5.5\n"
        "    watch: 5.0\n"
        "    reject: 3.0\n"
        "risk:\n"
        "  position:\n"
        "    single_max: 0.2\n"
        "    total_max: 0.6\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ASTOCK_CONFIG_DIR", str(config_dir))

    registry = ConfigRegistry()
    data, errors = registry.load_and_validate()

    assert errors == []
    assert data["strategy"]["scoring"]["thresholds"]["buy"] == 5.5


def test_relative_runtime_paths_use_data_dir_for_global_config(tmp_path, monkeypatch):
    from astock_trading.platform.paths import resolve_path_from_config

    config_dir = tmp_path / "config"
    data_home = tmp_path / "data-home"
    config_dir.mkdir()
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))

    resolved = resolve_path_from_config("trade-vault", config_dir)

    assert resolved == data_home / "a-stock-trading" / "trade-vault"


def test_init_command_creates_xdg_config_templates(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    config_dir = tmp_path / "a-stock-config"
    env = os.environ.copy()
    env.pop("ASTOCK_DATABASE_URL", None)

    result = subprocess.run(
        [str(cli), "init", "--config-dir", str(config_dir), "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["config_dir"] == str(config_dir)
    assert (config_dir / ".env").exists()
    assert (config_dir / ".env.example").exists()
    assert (config_dir / "strategy.yaml").exists()
    assert (config_dir / "mcp_server.yaml").exists()
    assert (config_dir / "profiles" / "trend_swing.yaml").exists()
    assert (config_dir / "profiles" / "short_continuation.yaml").exists()
    assert (config_dir / "profiles" / "defensive_watch.yaml").exists()
    strategy = yaml.safe_load((config_dir / "strategy.yaml").read_text(encoding="utf-8"))
    assert strategy["auto_trade"]["enabled"] is False
    assert strategy["auto_trade"]["dry_run"] is True
    assert payload["commands"]["doctor"] == "atrade doctor --json"
