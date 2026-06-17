#!/usr/bin/env python3
"""launchd 用 watchdog 外部监督器，只调用稳定 atrade 入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Supervise atrade notify ops-watchdog")
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--atrade", default=str(Path.home() / ".local" / "bin" / "atrade"))
    parser.add_argument(
        "--python",
        default=str(Path.home() / ".local" / "share" / "uv" / "tools" / "a-stock-trading" / "bin" / "python"),
    )
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    if args.worker:
        return _run_worker()

    timeout_seconds = max(5, args.timeout_seconds)
    command = [
        args.python,
        str(Path(__file__).resolve()),
        "--worker",
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
    except subprocess.TimeoutExpired as exc:
        payload = {
            "command": "ops-watchdog-supervisor",
            "status": "failed",
            "reason": "ops_watchdog_timeout",
            "timeout_seconds": timeout_seconds,
            "child_command": " ".join(command),
            "stdout_tail": _tail_text(exc.stdout),
            "stderr_tail": _tail_text(exc.stderr),
        }
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 124

    try:
        child = json.loads(completed.stdout) if completed.stdout.strip() else {}
    except json.JSONDecodeError:
        child = {
            "status": "unparsed",
            "stdout_tail": completed.stdout[-2000:],
        }
    payload = {
        "command": "ops-watchdog-supervisor",
        "status": "ok" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "child_command": " ".join(command),
        "child": child,
        "stderr_tail": completed.stderr[-2000:],
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return completed.returncode


def _run_worker() -> int:
    _debug("worker:start")
    _debug("worker:import_context")
    from astock_trading.platform.ops_watchdog import (
        build_ops_watchdog,
        build_ops_watchdog_context,
        build_ops_watchdog_monitor,
        read_ops_watchdog_snapshot,
        resolve_ops_watchdog_state_file,
        write_ops_watchdog_snapshot,
    )
    from astock_trading.reporting.discord import format_ops_watchdog_embed
    from astock_trading.reporting.discord_sender import send_embed

    _debug("worker:load_runtime_env")
    _load_worker_env()
    _debug("worker:build_context")
    ctx = build_ops_watchdog_context()
    try:
        _debug("worker:build_report")
        report = build_ops_watchdog(ctx, include_account=False)
    finally:
        ctx.conn.close()

    _debug("worker:build_monitor")
    state_file = resolve_ops_watchdog_state_file(None)
    previous = read_ops_watchdog_snapshot(state_file)
    monitor = build_ops_watchdog_monitor(report, previous_snapshot=previous)
    monitor["state_file"] = str(state_file)
    should_notify = bool(monitor.get("should_notify"))
    embed = format_ops_watchdog_embed(monitor) if should_notify else {}
    if should_notify:
        _debug("worker:send_embed")
        ok, error = send_embed(embed, "A股运维 watchdog")
    else:
        _debug("worker:silent")
        ok, error = True, ""

    if ok or not should_notify:
        _debug("worker:write_state")
        write_ops_watchdog_snapshot(monitor, state_file)
        monitor["state_updated"] = True
    else:
        monitor["state_updated"] = False

    payload = {
        "status": "sent" if should_notify and ok else ("failed" if should_notify else "silent"),
        "notification": {
            "target": "discord",
            "ok": ok,
            "error": error,
            "skipped": not should_notify,
            "reason": "" if should_notify else monitor.get("summary", ""),
        },
        "embed": embed,
        "monitor": monitor,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))
    _debug("worker:done")
    return 0 if ok else 1


def _debug(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _tail_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")[-2000:]
    return str(value)[-2000:]


def _load_worker_env() -> None:
    if os.environ.get("ASTOCK_DATABASE_URL"):
        return
    candidates: list[Path] = []
    explicit = os.environ.get("ASTOCK_ENV_FILE", "").strip()
    if explicit:
        candidates.append(Path(explicit).expanduser())
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))).expanduser()
    candidates.append(config_home / "a-stock-trading" / ".env")
    for env_file in candidates:
        if not env_file.exists():
            continue
        for line in env_file.read_text(encoding="utf-8").splitlines():
            parsed = _parse_env_line(line)
            if parsed:
                key, value = parsed
                os.environ.setdefault(key, value)
        return


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[len("export "):].strip()
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


if __name__ == "__main__":
    sys.exit(main())
