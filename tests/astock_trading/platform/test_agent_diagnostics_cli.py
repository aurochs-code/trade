"""Agent diagnostics CLI contract tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_diagnose_health_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "diagnose", "health", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "health"
    assert payload["status"] in {"ok", "warning", "failed"}
    assert "findings" in payload
    assert "recommendations" in payload
    assert "data_sources" in payload["inputs"]


def test_explain_run_missing_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "explain-run", "missing-run-id", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload == {
        "status": "not_found",
        "run_id": "missing-run-id",
        "findings": ["run_id not found"],
    }


def test_propose_plan_json_is_non_executing_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "propose-plan", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "proposed"
    assert payload["execution_allowed"] is False
    assert payload["plan_type"] == "agent_trade_plan"
    assert "diagnostics" in payload
    assert "actions" in payload
