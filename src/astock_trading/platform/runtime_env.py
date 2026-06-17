"""Runtime environment loading for global CLI entrypoints."""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from astock_trading.platform.paths import project_root, resolve_config_dir


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()
    if "=" not in stripped:
        return None
    key, raw_value = stripped.split("=", 1)
    key = key.strip()
    if not key:
        return None
    value = raw_value.strip()
    if value:
        try:
            parts = shlex.split(value, comments=True, posix=True)
            value = parts[0] if parts else ""
        except ValueError:
            value = value.strip("\"'")
    return key, value


def _candidate_env_files() -> list[Path]:
    explicit = os.environ.get("ASTOCK_ENV_FILE", "").strip()
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.extend(
        [
            resolve_config_dir() / ".env",
            Path.cwd() / ".env",
            project_root() / ".env",
        ]
    )
    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved not in seen:
            deduped.append(candidate.expanduser())
            seen.add(resolved)
    return deduped


def candidate_env_files() -> list[Path]:
    """Return runtime .env lookup candidates in load order."""
    return _candidate_env_files()


def parse_env_file(env_file: Path) -> dict[str, str]:
    """Parse a simple shell-style .env file without mutating os.environ."""
    values: dict[str, str] = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(line)
        if parsed:
            key, value = parsed
            values[key] = value
    return values


def load_runtime_env() -> Path | None:
    """Load the first available runtime .env without overriding process env."""
    if os.environ.get("ASTOCK_NO_ENV_FILE", "").strip().lower() in {"1", "true", "yes"}:
        return None
    for env_file in _candidate_env_files():
        if not env_file.exists():
            continue
        values = parse_env_file(env_file)
        for key, value in values.items():
            os.environ.setdefault(key, value)
        if "ASTOCK_CONFIG_DIR" not in os.environ:
            env_config_dir = env_file.parent
            if (env_config_dir / "strategy.yaml").exists():
                os.environ["ASTOCK_CONFIG_DIR"] = str(env_config_dir)
        return env_file
    return None
