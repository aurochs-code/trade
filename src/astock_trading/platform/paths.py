"""Runtime path resolution for installed and source-tree CLI use."""

from __future__ import annotations

import os
from pathlib import Path

APP_NAME = "a-stock-trading"


def project_root() -> Path:
    """Return the source checkout root when running from this repository."""
    return Path(__file__).resolve().parents[3]


def xdg_config_home() -> Path:
    raw = os.environ.get("XDG_CONFIG_HOME")
    return Path(raw).expanduser() if raw else Path.home() / ".config"


def xdg_data_home() -> Path:
    raw = os.environ.get("XDG_DATA_HOME")
    return Path(raw).expanduser() if raw else Path.home() / ".local" / "share"


def xdg_state_home() -> Path:
    raw = os.environ.get("XDG_STATE_HOME")
    return Path(raw).expanduser() if raw else Path.home() / ".local" / "state"


def xdg_cache_home() -> Path:
    raw = os.environ.get("XDG_CACHE_HOME")
    return Path(raw).expanduser() if raw else Path.home() / ".cache"


def default_config_dir() -> Path:
    return xdg_config_home() / APP_NAME


def default_data_dir() -> Path:
    return xdg_data_home() / APP_NAME


def default_state_dir() -> Path:
    return xdg_state_home() / APP_NAME


def default_cache_dir() -> Path:
    return xdg_cache_home() / APP_NAME


def resolve_config_dir(explicit: Path | None = None) -> Path:
    """Resolve config directory for runtime commands.

    Priority:
    1. explicit argument
    2. ASTOCK_CONFIG_DIR
    3. ./config in current working directory
    4. source checkout config/
    5. ~/.config/a-stock-trading
    """
    if explicit is not None:
        return explicit.expanduser()

    env_dir = os.environ.get("ASTOCK_CONFIG_DIR", "").strip()
    if env_dir:
        return Path(env_dir).expanduser()

    cwd_config = Path.cwd() / "config"
    if (cwd_config / "strategy.yaml").exists():
        return cwd_config

    repo_config = project_root() / "config"
    if (repo_config / "strategy.yaml").exists():
        return repo_config

    return default_config_dir()


def resolve_path_from_config(raw_path: str | Path, config_dir: Path | None = None) -> Path:
    """Resolve a config path relative to its owning config directory."""
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    owner = config_dir or resolve_config_dir()
    repo_config = (project_root() / "config").resolve()
    base = project_root() if owner.resolve() == repo_config else default_data_dir()
    return (base / path).resolve()
