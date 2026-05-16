"""Shared CLI helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from astock_trading.platform.paths import project_root as _project_root


def project_root() -> Path:
    return _project_root()


def json_or_text(payload: Any, as_json: bool) -> None:
    if as_json:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        typer.echo(payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, default=str))
