"""P6-2 多策略 profile 对比。

只做配置和历史证据对比；不自动切换 ASTOCK_CONFIG_PROFILE。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from statistics import mean
from types import SimpleNamespace
from typing import Any

from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.domain_events import (
    DECISION_SUGGESTED,
    STRATEGY_CAPITAL_ALLOCATION_PROPOSED,
    STRATEGY_PROFILE_ACTIVATION_APPLIED,
    STRATEGY_PROFILE_ACTIVATION_REQUESTED,
    STRATEGY_PROFILE_COMPARISON_PROPOSED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.paths import resolve_config_dir
from astock_trading.platform.runtime_env import candidate_env_files, parse_env_file
from astock_trading.platform.time import utc_now_iso

DEFAULT_PROFILES = ("trend_swing", "short_continuation", "defensive_watch")
ACTIVE_STRATEGY_BUDGET_PCT = 0.60


def compare_strategy_profiles(
    conn: Any,
    *,
    config_dir: Path | None = None,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    record: bool = False,
) -> dict:
    """比较多个策略 profile 的配置差异和已有运行证据。"""
    config_root = config_dir or resolve_config_dir()
    store = EventStore(conn)
    rows = [
        _profile_summary(conn, store, config_root=config_root, profile=name)
        for name in profiles
    ]
    has_evidence = any(item["evidence_status"] == "has_profile_runs" for item in rows)
    payload = {
        "analysis": "strategy_profile_comparison",
        "status": "ok" if has_evidence else "needs_shadow_validation",
        "generated_at": utc_now_iso(),
        "current_profile": os.getenv("ASTOCK_CONFIG_PROFILE", "default"),
        "profiles": rows,
        "recommendations": _recommendations(rows),
        "guardrails": {
            "auto_switch_profile": False,
            "auto_allocate_capital": False,
            "manual_approval_required": True,
            "reason": "P6-2 只做多策略 profile 对比，不自动切换 ASTOCK_CONFIG_PROFILE，也不自动分配资金。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_profile_report(payload)

    if record:
        event_id = store.append(
            "strategy:profiles",
            "strategy",
            STRATEGY_PROFILE_COMPARISON_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_profiles"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def propose_strategy_allocation(
    conn: Any,
    *,
    config_dir: Path | None = None,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    total_capital: float = 500000.0,
    min_samples: int = 10,
    record: bool = False,
) -> dict:
    """生成多策略隔离资金桶和弱策略处理建议；不自动执行。"""
    comparison = compare_strategy_profiles(conn, config_dir=config_dir, profiles=profiles, record=False)
    total_capital_cents = int(round(max(total_capital, 0.0) * 100))
    buckets = _capital_buckets(
        comparison.get("profiles") or [],
        total_capital_cents=total_capital_cents,
        min_samples=min_samples,
    )
    weak_review = _weak_strategy_review(buckets, min_samples=min_samples)
    payload = {
        "analysis": "strategy_capital_allocation",
        "status": _allocation_status(buckets),
        "generated_at": utc_now_iso(),
        "current_profile": comparison.get("current_profile", "default"),
        "total_capital_cents": total_capital_cents,
        "capital_policy": {
            "mode": "advisory_only",
            "active_strategy_budget_pct": ACTIVE_STRATEGY_BUDGET_PCT,
            "reserve_pct": round(1 - ACTIVE_STRATEGY_BUDGET_PCT, 4),
            "min_review_samples": min_samples,
        },
        "capital_buckets": buckets,
        "weak_strategy_review": weak_review,
        "source_profile_comparison": {
            "status": comparison.get("status"),
            "profile_count": len(comparison.get("profiles") or []),
            "recommendations": comparison.get("recommendations", []),
        },
        "recommendations": _allocation_recommendations(buckets),
        "guardrails": {
            "auto_apply": False,
            "auto_switch_profile": False,
            "auto_allocate_capital": False,
            "manual_approval_required": True,
            "reason": "只输出隔离资金桶和弱策略处理建议，不改账户、不切换 profile、不自动分配资金。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_allocation_report(payload)

    if record:
        store = EventStore(conn)
        event_id = store.append(
            "strategy:allocation",
            "strategy",
            STRATEGY_CAPITAL_ALLOCATION_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_allocation"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(
            conn,
            event_id,
            payload["report_markdown"],
            report_type="strategy_capital_allocation",
            artifact_prefix="strategy_allocation",
        )

    return payload


def build_strategy_profile_activation_plan(
    conn: Any,
    *,
    config_dir: Path | None = None,
    target_profile: str = "trend_swing",
    record: bool = False,
) -> dict:
    """生成执行 profile 激活计划；只读，不修改环境变量或调度配置。"""
    config_root = config_dir or resolve_config_dir()
    available_profiles = _available_profiles(config_root)
    current_profile = os.getenv("ASTOCK_CONFIG_PROFILE", "default") or "default"
    target_profile = target_profile.strip()
    target_path = config_root / "profiles" / f"{target_profile}.yaml"
    if not target_profile:
        raise ValueError("target_profile is required")
    if not target_path.exists():
        raise ValueError(f"未知策略 profile: {target_profile}")

    target_config, config_errors = ConfigRegistry(config_dir=config_root, profile=target_profile).load_and_validate()
    target_hash = profile_config_hash(target_config)
    status = "already_active" if current_profile == target_profile else "requires_manual_confirmation"
    approval_gate = _activation_approval_gate(
        current_profile=current_profile,
        target_profile=target_profile,
        status=status,
    )
    after_approval_preview = _profile_activation_after_approval_preview(
        conn,
        approval_gate=approval_gate,
        config_dir=config_root,
    )
    payload = {
        "analysis": "strategy_profile_activation_plan",
        "status": status,
        "summary": _activation_plan_summary(current_profile, target_profile, status),
        "generated_at": utc_now_iso(),
        "current_profile": current_profile,
        "target_profile": target_profile,
        "available_profiles": available_profiles,
        "target_config_hash": target_hash,
        "target_key_parameters": _key_parameters(target_config.get("strategy", {})),
        "config_errors": config_errors,
        "activation": {
            "auto_apply": False,
            "manual_confirmation_required": True,
            "export_command": f"export ASTOCK_CONFIG_PROFILE={target_profile}",
            "verify_command": f"ASTOCK_CONFIG_PROFILE={target_profile} atrade paper auto-readiness --json",
            "run_command": f"ASTOCK_CONFIG_PROFILE={target_profile} atrade run-pipeline auto_trade --json",
            "run_command_requires_user_approval": True,
            "run_command_contract_id": "run_pipeline_auto_trade",
            "hermes_note": (
                "生产调度如需使用该 profile，必须由人工在 trading profile 运行环境中显式设置，"
                "agent 不应自行切换。"
            ),
        },
        "guardrails": {
            "auto_apply": False,
            "auto_switch_profile": False,
            "manual_approval_required": True,
            "modifies_environment": False,
            "modifies_schedule": False,
            "real_broker_integration": False,
            "reason": "该命令只生成可审计激活计划；不修改 .env、不改 Hermes、不提交真实券商订单。",
        },
        "recommendations": _activation_recommendations(current_profile, target_profile, status),
        "next_action": _activation_plan_next_action(target_profile=target_profile, status=status),
        "approval_gate": approval_gate,
        "after_approval_preview": after_approval_preview,
        "post_approval_checklist": _activation_post_approval_checklist(
            target_profile=target_profile,
            status=status,
        ),
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_profile_activation_report(payload)

    if record:
        store = EventStore(conn)
        event_id = store.append(
            "strategy:profile_activation",
            "strategy",
            STRATEGY_PROFILE_ACTIVATION_REQUESTED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_profile_activation_plan"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(
            conn,
            event_id,
            payload["report_markdown"],
            report_type="strategy_profile_activation_plan",
            artifact_prefix="strategy_profile_activation",
        )

    return payload


def apply_strategy_profile_activation(
    conn: Any,
    *,
    config_dir: Path | None = None,
    target_profile: str = "trend_swing",
    env_file: Path | None = None,
    confirm: bool = False,
) -> dict:
    """人工确认后写入运行 .env；默认只返回确认要求，不修改环境。"""
    plan = build_strategy_profile_activation_plan(
        conn,
        config_dir=config_dir,
        target_profile=target_profile,
        record=False,
    )
    resolved_env_file = _resolve_activation_env_file(env_file)
    before = _env_profile_snapshot(resolved_env_file)
    target_profile = plan["target_profile"]
    status = "confirmation_required"
    backup_path = ""
    recorded_event_id = ""
    guardrails = {
        "manual_approval_required": True,
        "requires_yes_flag": True,
        "modifies_environment": False,
        "modifies_schedule": False,
        "real_broker_integration": False,
        "auto_switch_profile": False,
        "reason": "只有显式 --apply-env --yes 才写入 ASTOCK_CONFIG_PROFILE；不改 Hermes 调度、不提交订单。",
    }
    runtime_env = {
        "env_file": str(resolved_env_file) if resolved_env_file else "",
        "env_file_exists_before": bool(resolved_env_file and resolved_env_file.exists()),
        "profile_key_present_before": before["profile_key_present"],
        "before_profile": before["profile"],
        "after_profile": before["profile"],
        "backup_path": "",
        "revert_command": _profile_revert_command(before["profile"], resolved_env_file),
    }

    if confirm:
        if resolved_env_file is None:
            status = "env_file_required"
        else:
            if resolved_env_file.exists():
                backup_path = _backup_env_file(resolved_env_file)
            _write_env_value(resolved_env_file, "ASTOCK_CONFIG_PROFILE", target_profile)
            after = _env_profile_snapshot(resolved_env_file)
            runtime_env.update({
                "env_file_exists_before": before["exists"],
                "profile_key_present_before": before["profile_key_present"],
                "before_profile": before["profile"],
                "after_profile": after["profile"],
                "backup_path": backup_path,
                "revert_command": _profile_revert_command(before["profile"], resolved_env_file),
            })
            status = "applied"
            guardrails["modifies_environment"] = True

    payload = {
        "analysis": "strategy_profile_activation_apply",
        "status": status,
        "generated_at": utc_now_iso(),
        "current_profile": plan["current_profile"],
        "target_profile": target_profile,
        "activation_plan": {
            "status": plan["status"],
            "target_config_hash": plan["target_config_hash"],
            "target_key_parameters": plan["target_key_parameters"],
            "config_errors": plan["config_errors"],
        },
        "runtime_env": runtime_env,
        "guardrails": guardrails,
        "next_action": _activation_apply_next_action(
            target_profile=target_profile,
            env_file=resolved_env_file,
            status=status,
        ),
        "after_approval_preview": plan.get("after_approval_preview", {}) or {"available": False},
        "post_approval_checklist": _activation_post_approval_checklist(
            target_profile=target_profile,
            status="already_active" if status == "applied" else "requires_manual_confirmation",
        ),
        "recorded_event_id": "",
    }

    if status == "applied":
        event_id = EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            STRATEGY_PROFILE_ACTIVATION_APPLIED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_profile_activation_apply"},
        )
        recorded_event_id = event_id
        payload["recorded_event_id"] = recorded_event_id
    payload["report_markdown"] = render_strategy_profile_activation_apply_report(payload)
    return payload


def latest_strategy_profile_activation_request(
    source: Any,
    *,
    target_profile: str | None = None,
    limit: int = 50,
) -> dict:
    """读取最近一次 profile 激活请求；只返回运营复核需要的字段。"""
    events = _query_profile_activation_events(source, limit=limit)
    for event in events:
        payload = event.get("payload") or {}
        if target_profile and payload.get("target_profile") != target_profile:
            continue
        return {
            "event_id": event.get("event_id", ""),
            "occurred_at": event.get("occurred_at", ""),
            "status": payload.get("status", ""),
            "current_profile": payload.get("current_profile", ""),
            "target_profile": payload.get("target_profile", ""),
            "activation": payload.get("activation", {}) or {},
            "guardrails": payload.get("guardrails", {}) or {},
            "recommendations": payload.get("recommendations", []) or [],
        }
    return {}


def _resolve_activation_env_file(env_file: Path | None) -> Path | None:
    if env_file is not None:
        return env_file.expanduser()
    for candidate in candidate_env_files():
        if candidate.exists():
            return candidate
    return None


def _env_profile_snapshot(env_file: Path | None) -> dict[str, Any]:
    if env_file is None or not env_file.exists():
        return {"exists": False, "profile": None, "profile_key_present": False}
    values = parse_env_file(env_file)
    return {
        "exists": True,
        "profile": values.get("ASTOCK_CONFIG_PROFILE"),
        "profile_key_present": "ASTOCK_CONFIG_PROFILE" in values,
    }


def _backup_env_file(env_file: Path) -> str:
    stamp = (
        utc_now_iso()
        .replace("-", "")
        .replace(":", "")
        .replace("+", "")
        .replace(".", "")
    )
    backup = env_file.with_name(f"{env_file.name}.bak_{stamp}")
    shutil.copy2(env_file, backup)
    return str(backup)


def _write_env_value(env_file: Path, key: str, value: str) -> None:
    env_file.parent.mkdir(parents=True, exist_ok=True)
    lines = env_file.read_text(encoding="utf-8").splitlines() if env_file.exists() else []
    replacement = f"{key}={value}"
    replaced = False
    result: list[str] = []
    for line in lines:
        stripped = line.strip()
        comparable = stripped[len("export ") :].strip() if stripped.startswith("export ") else stripped
        if comparable.startswith(f"{key}="):
            result.append(replacement)
            replaced = True
        else:
            result.append(line)
    if not replaced:
        result.append(replacement)
    env_file.write_text("\n".join(result).rstrip() + "\n", encoding="utf-8")


def _profile_revert_command(before_profile: str | None, env_file: Path | None) -> str:
    if env_file is None:
        return ""
    if before_profile:
        return f"atrade strategy profile-activation --target {before_profile} --apply-env --yes --env-file {env_file}"
    return f"手工从 {env_file} 删除 ASTOCK_CONFIG_PROFILE，或恢复 backup_path。"


def _activation_apply_next_action(
    *,
    target_profile: str,
    env_file: Path | None,
    status: str,
) -> dict[str, Any]:
    if status == "applied":
        return {
            "type": "verify_runtime_profile",
            "label": "复核运行 profile",
            "command": "atrade diagnose schedule --json",
            "safe_to_auto_apply": True,
            **_profile_activation_read_only_contract("diagnose_schedule"),
        }
    if status == "env_file_required":
        return {
            "type": "choose_runtime_env_file",
            "label": "指定运行 .env 文件",
            "command": f"atrade strategy profile-activation --target {target_profile} --apply-env --yes --env-file PATH --json",
            "safe_to_auto_apply": False,
            **_profile_activation_apply_contract(),
        }
    env_part = f" --env-file {env_file}" if env_file else ""
    return {
        "type": "confirm_profile_activation_apply",
        "label": "确认写入运行 profile",
        "command": f"atrade strategy profile-activation --target {target_profile} --apply-env --yes{env_part} --json",
        "safe_to_auto_apply": False,
        **_profile_activation_apply_contract(),
    }


def _activation_plan_summary(current_profile: str, target_profile: str, status: str) -> str:
    if status == "already_active":
        return f"当前执行 profile 已是 {target_profile}；继续复核模拟盘预检和调度状态。"
    return f"当前执行 profile 为 {current_profile}；目标 {target_profile} 需要人工确认后才能写入运行环境。"


def _activation_approval_gate(*, current_profile: str, target_profile: str, status: str) -> dict[str, Any]:
    if status == "already_active":
        return {
            "required": False,
            "reason": f"当前执行 profile 已是 {target_profile}；继续只读核查调度和模拟预检。",
            "verify_command": "atrade diagnose schedule --json",
            "verify_command_contract": _profile_activation_command_contract("diagnose_schedule"),
        }
    return {
        "required": True,
        "type": "profile_activation_apply",
        "label": "人工确认写入运行 profile",
        "reason": (
            f"当前 {current_profile} 混合配置阻断自动模拟；"
            f"需要人工批准后写入 ASTOCK_CONFIG_PROFILE={target_profile}。"
        ),
        "target_profile": target_profile,
        "review_command": f"atrade strategy profile-activation --target {target_profile} --json",
        "apply_command": f"atrade strategy profile-activation --target {target_profile} --apply-env --yes --json",
        "verify_command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": False,
        "modifies_environment_after_approval": True,
        "review_command_contract_id": "strategy_profile_activation_review",
        "review_command_contract": _profile_activation_command_contract("strategy_profile_activation_review"),
        "apply_command_contract_id": "strategy_profile_activation_apply",
        "apply_command_contract": _profile_activation_command_contract(
            "strategy_profile_activation_apply",
            writes_state=True,
            writes_environment=True,
            requires_user_approval=True,
            risk_level="environment_write",
            state_events=["strategy.profile_activation.applied"],
        ),
        "verify_command_contract_id": "diagnose_schedule",
        "verify_command_contract": _profile_activation_command_contract("diagnose_schedule"),
    }


def _profile_activation_after_approval_preview(
    conn: Any,
    *,
    approval_gate: dict[str, Any],
    config_dir: Path,
) -> dict[str, Any]:
    """生成 profile 审批后的只读预判；不写 .env、不读取账户、不提交委托。"""
    if approval_gate.get("required") is not True:
        return {"available": False}
    try:
        from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
        from astock_trading.platform.agent_diagnostics import _candidate_flow_after_approval_preview
        from astock_trading.platform.runs import RunJournal

        data, _errors = ConfigRegistry(config_dir=config_dir).load_and_validate()
        strategy_cfg = (data.get("strategy", {}) or {}) if isinstance(data, dict) else {}
        ctx = SimpleNamespace(
            conn=conn,
            cfg=strategy_cfg,
            event_store=EventStore(conn),
            run_journal=RunJournal(conn),
        )
        auto_readiness = build_auto_trade_readiness(ctx, include_account=False)
        return _candidate_flow_after_approval_preview(
            approval_gate=approval_gate,
            auto_readiness=auto_readiness,
        )
    except Exception as exc:
        return {
            "available": False,
            "status": "unavailable",
            "summary": f"审批后只读预演读取失败：{exc}",
            "recommended_command": "atrade diagnose flow --json",
            "safe_to_auto_apply": True,
            "writes_environment": False,
            "places_order": False,
        }


def _activation_post_approval_checklist(*, target_profile: str, status: str) -> dict[str, Any]:
    waiting = status != "already_active"
    steps: list[dict[str, Any]] = []
    if waiting:
        steps.append({
            "type": "apply_runtime_profile",
            "label": "人工确认写入运行 profile",
            "command": f"atrade strategy profile-activation --target {target_profile} --apply-env --yes --json",
            "reason": "把 ASTOCK_CONFIG_PROFILE 写入运行 .env；不改 Hermes 调度、不提交订单。",
            "safe_to_auto_apply": False,
            "command_contract": _profile_activation_command_contract(
                "strategy_profile_activation_apply",
                writes_state=True,
                writes_environment=True,
                requires_user_approval=True,
                risk_level="environment_write",
                state_events=["strategy.profile_activation.applied"],
            ),
        })
    steps.extend([
        {
            "type": "verify_schedule_profile",
            "label": "核查运行 profile 和调度",
            "command": "atrade diagnose schedule --json",
            "reason": "确认运行 .env 已生效，并检查下个窗口盘中候选/模拟承接任务。",
            "safe_to_auto_apply": True,
            "command_contract": _profile_activation_command_contract("diagnose_schedule"),
        },
        {
            "type": "verify_paper_readiness",
            "label": "核查模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "reason": "确认同日买入意向、买入窗口、候选新鲜度和账户读取状态。",
            "safe_to_auto_apply": True,
            "command_contract": _profile_activation_command_contract("paper_auto_readiness"),
        },
        {
            "type": "verify_trial_guard",
            "label": "核查试运行护栏",
            "command": "atrade risk trial-guard --json",
            "reason": "确认首轮试运行仓位和候选池入场信号边界。",
            "safe_to_auto_apply": True,
            "command_contract": _profile_activation_command_contract("risk_trial_guard"),
        },
    ])
    return {
        "status": "waiting_manual_approval" if waiting else "ready_for_verification",
        "summary": (
            f"人工批准写入 {target_profile} 后，先运行只读预检和调度核查；"
            "确认同日买入意向、买入窗口和调度首跑后，auto_trade 才能单独审批执行。"
        ),
        "steps": steps,
        "paper_order_execution": {
            "command": "atrade run-pipeline auto_trade --json",
            "allowed_only_after": [
                "运行 profile 已确认",
                "调度核查通过",
                "同日新鲜买入意向已形成",
                "买入窗口打开",
                "模拟预检和试运行护栏通过",
            ],
            "requires_separate_user_approval": True,
            "command_contract": _profile_activation_command_contract(
                "run_pipeline_auto_trade",
                writes_state=True,
                writes_order=True,
                requires_user_approval=True,
                risk_level="paper_order_execution",
                state_events=["auto_trade.diagnostic", "auto_trade.summary", "paper.order.submitted"],
            ),
        },
    }


def _activation_plan_next_action(*, target_profile: str, status: str) -> dict[str, Any]:
    if status == "already_active":
        return {
            "type": "verify_runtime_profile",
            "label": "复核运行 profile",
            "command": "atrade diagnose schedule --json",
            "safe_to_auto_apply": True,
            **_profile_activation_read_only_contract("diagnose_schedule"),
        }
    return {
        "type": "confirm_profile_activation_apply",
        "label": "确认写入运行 profile",
        "command": f"atrade strategy profile-activation --target {target_profile} --apply-env --yes --json",
        "safe_to_auto_apply": False,
        **_profile_activation_apply_contract(),
    }


def _profile_activation_apply_contract() -> dict[str, Any]:
    return {
        "writes_state": True,
        "writes_environment": True,
        "writes_order": False,
        "requires_user_approval": True,
        "risk_level": "environment_write",
        "command_contract_id": "strategy_profile_activation_apply",
    }


def _profile_activation_read_only_contract(command_contract_id: str) -> dict[str, Any]:
    return {
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": command_contract_id,
    }


def _profile_activation_command_contract(
    command_contract_id: str,
    *,
    writes_state: bool = False,
    writes_environment: bool = False,
    writes_order: bool = False,
    requires_user_approval: bool = False,
    risk_level: str = "read_only",
    state_events: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": command_contract_id,
        "risk_level": risk_level,
        "writes_state": writes_state,
        "writes_environment": writes_environment,
        "writes_order": writes_order,
        "requires_user_approval": requires_user_approval,
        "state_events": state_events or [],
    }


def _query_profile_activation_events(source: Any, *, limit: int) -> list[dict]:
    conn = getattr(source, "_conn", None)
    if conn is None and hasattr(source, "execute"):
        conn = source
    if conn is not None:
        rows = conn.execute(
            """SELECT * FROM event_log
               WHERE event_type = ?
               ORDER BY occurred_at DESC, stream_version DESC
               LIMIT ?""",
            (STRATEGY_PROFILE_ACTIVATION_REQUESTED, limit),
        ).fetchall()
        return [EventStore._row_to_dict(row) for row in rows]
    if hasattr(source, "query"):
        events = source.query(event_type=STRATEGY_PROFILE_ACTIVATION_REQUESTED, limit=limit)
        return sorted(
            events,
            key=lambda event: (
                str(event.get("occurred_at") or ""),
                int(event.get("stream_version") or 0),
            ),
            reverse=True,
        )
    return []


def profile_config_hash(config: dict) -> str:
    """返回与 ConfigRegistry.freeze() 一致的配置 hash 前缀。"""
    config_json = json.dumps(config, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(config_json.encode()).hexdigest()[:16]


def render_strategy_profile_report(payload: dict) -> str:
    """渲染中文多策略 profile 对比报告。"""
    status_label = {
        "ok": "已有运行证据",
        "needs_shadow_validation": "需要影子验证",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# P6-2 多策略 Profile 对比",
        "",
        f"- 状态：{status_label}",
        f"- 当前执行 profile：{payload.get('current_profile')}",
        "- 自动切换 profile：否",
        "- 自动资金分配：否",
        "",
        "## Profile 概览",
    ]
    for item in payload.get("profiles") or []:
        review = item.get("trade_review") or {}
        params = item.get("key_parameters") or {}
        lines.extend([
            f"- {item.get('name')}：{item.get('purpose')}",
            f"  - 买入阈值：{params.get('buy_threshold')}；观察阈值：{params.get('watch_threshold')}",
            f"  - 历史运行：{item.get('run_count')} 次；复盘样本：{review.get('sample_count')} 笔",
            f"  - 平均收益：{review.get('avg_return_pct', 0):.2%}；胜率：{review.get('win_rate_pct', 0):.2%}",
        ])
    lines.extend(["", "## 建议"])
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")
    return "\n".join(lines)


def render_strategy_allocation_report(payload: dict) -> str:
    """渲染中文多策略隔离资金建议报告。"""
    status_label = {
        "ok": "可人工复核",
        "review_required": "需要人工复核",
        "needs_shadow_validation": "需要影子验证",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# P6-2 多策略隔离资金建议",
        "",
        f"- 状态：{status_label}",
        f"- 总资金：¥{(payload.get('total_capital_cents', 0) or 0) / 100:,.2f}",
        "- 自动分配资金：否",
        "- 自动切换 profile：否",
        "",
        "## 隔离资金桶",
    ]
    for bucket in payload.get("capital_buckets") or []:
        lines.extend([
            f"- {bucket.get('profile')}（{bucket.get('scope')}）：{bucket.get('display_action')}",
            f"  - 建议资金：¥{bucket.get('suggested_capital_cents', 0) / 100:,.2f}"
            f"（{bucket.get('suggested_capital_pct', 0):.1%}）",
            f"  - 依据：{bucket.get('reason')}",
        ])
    lines.extend(["", "## 弱策略复核"])
    review = payload.get("weak_strategy_review") or {}
    lines.append(f"- 暂停候选：{', '.join(review.get('pause_candidates') or []) or '无'}")
    lines.append(f"- 影子验证：{', '.join(review.get('shadow_candidates') or []) or '无'}")
    lines.extend(["", "## 建议"])
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")
    return "\n".join(lines)


def render_strategy_profile_activation_report(payload: dict) -> str:
    """渲染中文 profile 激活计划。"""
    status_label = {
        "already_active": "已是当前执行 profile",
        "requires_manual_confirmation": "需要人工确认",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    activation = payload.get("activation") or {}
    next_action = payload.get("next_action") or {}
    checklist = payload.get("post_approval_checklist") or {}
    after_approval_preview = payload.get("after_approval_preview") or {}
    lines = [
        "# 策略 Profile 激活计划",
        "",
        f"- 状态：{status_label}",
        f"- 摘要：{payload.get('summary', '')}",
        f"- 当前 profile：{payload.get('current_profile')}",
        f"- 目标 profile：{payload.get('target_profile')}",
        "- 自动切换：否",
        "- 修改环境：否",
        "",
        "## 人工确认命令",
        f"- 设置：`{activation.get('export_command')}`",
        f"- 预检：`{activation.get('verify_command')}`",
        f"- 模拟承接命令需单独批准：`{activation.get('run_command')}`",
        "",
        "## 批准后核查",
        f"- {checklist.get('summary', '批准后先运行只读预检和调度核查。')}",
    ]
    for step in checklist.get("steps") or []:
        lines.append(f"- {step.get('label')}：`{step.get('command')}`")
    if after_approval_preview.get("available"):
        lines.extend([
            "",
            "## 批准后只读预判",
            f"- {after_approval_preview.get('summary', '')}",
            f"- 只读预演：`{after_approval_preview.get('preview_command')}`",
            f"- 审批后复核：`{after_approval_preview.get('post_approval_verify_command')}`",
        ])
        blockers = after_approval_preview.get("remaining_blockers_from_current_readiness") or []
        if blockers:
            labels = "、".join(str(item.get("label") or item.get("reason") or "未知阻断") for item in blockers)
            lines.append(f"- 当前剩余非 profile 阻断：{labels}")
    paper_execution = checklist.get("paper_order_execution") or {}
    if paper_execution:
        lines.extend([
            "",
            "## 单独审批的模拟承接",
            f"- 命令：`{paper_execution.get('command')}`",
            "- 需要单独人工批准：是",
        ])
    lines.extend([
        "",
        "## 下一步",
        f"- `{next_action.get('command', 'atrade diagnose schedule --json')}`",
        "",
        "## 建议",
    ])
    for item in payload.get("recommendations") or []:
        lines.append(f"- {item}")
    return "\n".join(lines)


def render_strategy_profile_activation_apply_report(payload: dict) -> str:
    """渲染中文 profile 运行环境写入结果。"""
    runtime_env = payload.get("runtime_env") or {}
    next_action = payload.get("next_action") or {}
    status_label = {
        "confirmation_required": "等待人工确认",
        "applied": "已写入运行环境",
        "env_file_required": "需要指定 .env 文件",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# 策略 Profile 运行环境写入",
        "",
        f"- 状态：{status_label}",
        f"- 目标 profile：{payload.get('target_profile')}",
        f"- .env：{runtime_env.get('env_file') or '未确定'}",
        f"- 写入前：{runtime_env.get('before_profile') or '未设置'}",
        f"- 写入后：{runtime_env.get('after_profile') or '未设置'}",
        "- 修改 Hermes 调度：否",
        "- 提交真实/模拟订单：否",
    ]
    if runtime_env.get("backup_path"):
        lines.append(f"- 备份：{runtime_env.get('backup_path')}")
    after_approval_preview = payload.get("after_approval_preview") or {}
    if after_approval_preview.get("available"):
        lines.extend([
            "",
            "## 当前只读预判",
            f"- {after_approval_preview.get('summary', '')}",
            f"- 复核：`{after_approval_preview.get('post_approval_verify_command', 'atrade paper auto-readiness --json')}`",
        ])
    lines.extend([
        "",
        "## 下一步",
        f"- `{next_action.get('command', 'atrade diagnose schedule --json')}`",
    ])
    checklist = payload.get("post_approval_checklist") or {}
    if checklist:
        lines.extend(["", "## 后续核查"])
        for step in checklist.get("steps") or []:
            if step.get("type") == "apply_runtime_profile" and payload.get("status") == "applied":
                continue
            lines.append(f"- {step.get('label')}：`{step.get('command')}`")
    return "\n".join(lines)


def _profile_summary(conn: Any, store: EventStore, *, config_root: Path, profile: str) -> dict:
    config, errors = ConfigRegistry(config_dir=config_root, profile=profile).load_and_validate()
    strategy = config.get("strategy", {})
    config_hash = profile_config_hash(config)
    versions = _matching_config_versions(conn, config_hash)
    run_count = _run_count(conn, versions)
    decisions = _decision_counts(store, versions)
    trade_review = _trade_review_stats(store, versions)
    evidence_status = "has_profile_runs" if run_count or sum(decisions.values()) or trade_review["sample_count"] else "no_profile_runs"
    return {
        "name": profile,
        "purpose": _profile_purpose(profile),
        "config_hash": config_hash,
        "matched_config_versions": versions,
        "config_errors": errors,
        "evidence_status": evidence_status,
        "run_count": run_count,
        "decision_counts": decisions,
        "trade_review": trade_review,
        "key_parameters": _key_parameters(strategy),
    }


def _available_profiles(config_root: Path) -> list[str]:
    profile_dir = config_root / "profiles"
    if not profile_dir.exists():
        return []
    return sorted(path.stem for path in profile_dir.glob("*.yaml"))


def _activation_recommendations(current_profile: str, target_profile: str, status: str) -> list[str]:
    if status == "already_active":
        return [
            f"当前已使用 {target_profile}，继续用 atrade paper auto-readiness --json 检查窗口、候选和风控。",
        ]
    return [
        f"人工确认后，将 ASTOCK_CONFIG_PROFILE 设置为 {target_profile}，再重新运行 paper auto-readiness。",
        "确认前不要让 agent 自动修改生产调度或运行环境。",
    ]


def _capital_buckets(profiles: list[dict], *, total_capital_cents: int, min_samples: int) -> list[dict]:
    active_profiles = [item for item in profiles if _allocation_action(item, min_samples) == "activate_candidate"]
    active_scores = {
        item["name"]: max(item["trade_review"]["avg_return_pct"], 0.001)
        * max(item["trade_review"]["win_rate_pct"], 0.001)
        for item in active_profiles
    }
    score_sum = sum(active_scores.values())
    active_budget_cents = int(round(total_capital_cents * ACTIVE_STRATEGY_BUDGET_PCT))

    buckets = []
    for item in profiles:
        action = _allocation_action(item, min_samples)
        if action == "activate_candidate" and score_sum > 0:
            capital_cents = int(round(active_budget_cents * active_scores[item["name"]] / score_sum))
        else:
            capital_cents = 0
        buckets.append(_capital_bucket(item, action=action, capital_cents=capital_cents, total_cents=total_capital_cents))
    return buckets


def _capital_bucket(profile: dict, *, action: str, capital_cents: int, total_cents: int) -> dict:
    review = profile.get("trade_review") or {}
    params = profile.get("key_parameters") or {}
    return {
        "profile": profile.get("name"),
        "scope": f"strategy_{_scope_slug(str(profile.get('name') or 'unknown'))}",
        "action": action,
        "display_action": _allocation_action_label(action),
        "suggested_capital_cents": capital_cents,
        "suggested_capital_pct": round(capital_cents / total_cents, 4) if total_cents > 0 else 0.0,
        "max_single_position_pct": params.get("single_max_pct", 0.0),
        "review_sample_count": review.get("sample_count", 0),
        "avg_return_pct": review.get("avg_return_pct", 0.0),
        "win_rate_pct": review.get("win_rate_pct", 0.0),
        "reason": _allocation_reason(profile, action),
    }


def _allocation_action(profile: dict, min_samples: int) -> str:
    review = profile.get("trade_review") or {}
    sample_count = int(review.get("sample_count") or 0)
    avg_return = float(review.get("avg_return_pct") or 0.0)
    win_rate = float(review.get("win_rate_pct") or 0.0)
    if sample_count < min_samples:
        return "shadow_validate"
    if avg_return < 0 or win_rate < 0.4:
        return "pause_candidate"
    return "activate_candidate"


def _allocation_action_label(action: str) -> str:
    return {
        "activate_candidate": "可作为人工复核后的启用候选",
        "pause_candidate": "建议暂停并列入弱策略复核",
        "shadow_validate": "仅影子验证，暂不分配执行资金",
    }.get(action, action)


def _allocation_reason(profile: dict, action: str) -> str:
    review = profile.get("trade_review") or {}
    samples = int(review.get("sample_count") or 0)
    avg_return = float(review.get("avg_return_pct") or 0.0)
    win_rate = float(review.get("win_rate_pct") or 0.0)
    if action == "activate_candidate":
        return f"已有 {samples} 笔复盘样本，平均收益 {avg_return:.2%}，胜率 {win_rate:.2%}。"
    if action == "pause_candidate":
        return f"已有 {samples} 笔复盘样本，但平均收益 {avg_return:.2%}、胜率 {win_rate:.2%} 未达标。"
    return f"复盘样本只有 {samples} 笔，先积累影子运行证据。"


def _weak_strategy_review(buckets: list[dict], *, min_samples: int) -> dict:
    return {
        "rules": {
            "min_review_samples": min_samples,
            "pause_when_avg_return_below": 0,
            "pause_when_win_rate_below": 0.4,
        },
        "active_candidates": [item["profile"] for item in buckets if item["action"] == "activate_candidate"],
        "pause_candidates": [item["profile"] for item in buckets if item["action"] == "pause_candidate"],
        "shadow_candidates": [item["profile"] for item in buckets if item["action"] == "shadow_validate"],
    }


def _allocation_status(buckets: list[dict]) -> str:
    if not buckets or all(item["action"] == "shadow_validate" for item in buckets):
        return "needs_shadow_validation"
    if any(item["action"] == "pause_candidate" for item in buckets):
        return "review_required"
    return "ok"


def _allocation_recommendations(buckets: list[dict]) -> list[str]:
    if not buckets:
        return ["没有可分配的策略 profile。"]
    recommendations = [
        "把每个 profile 的建议资金桶当作人工审批清单；当前实现不会自动改账户或真实资金。",
    ]
    if any(item["action"] == "shadow_validate" for item in buckets):
        recommendations.append("证据不足的 profile 只做影子运行，先补 run_log、decision.suggested 和 trade.review.recorded。")
    if any(item["action"] == "pause_candidate" for item in buckets):
        recommendations.append("暂停候选 profile 需要复核样本来源，确认不是行情阶段或数据质量造成的短期偏差。")
    return recommendations


def _matching_config_versions(conn: Any, config_hash: str) -> list[str]:
    rows = conn.execute(
        """SELECT config_version
           FROM config_versions
           WHERE config_hash = ?
           ORDER BY created_at DESC""",
        (config_hash,),
    ).fetchall()
    return [str(row["config_version"]) for row in rows]


def _run_count(conn: Any, config_versions: list[str]) -> int:
    if not config_versions:
        return 0
    placeholders = ", ".join("?" for _ in config_versions)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count
            FROM run_log
            WHERE config_version IN ({placeholders}) AND status = 'completed'""",
        tuple(config_versions),
    ).fetchone()
    return int(row["count"] or 0) if row else 0


def _decision_counts(store: EventStore, config_versions: list[str]) -> dict[str, int]:
    counts = {"BUY": 0, "WATCH": 0, "CLEAR": 0, "SELL": 0, "NO_TRADE": 0}
    for version in config_versions:
        for event in store.query(event_type=DECISION_SUGGESTED, metadata_filter={"config_version": version}, limit=5000):
            action = str((event.get("payload") or {}).get("action") or "NO_TRADE")
            counts[action] = counts.get(action, 0) + 1
    return counts


def _trade_review_stats(store: EventStore, config_versions: list[str]) -> dict:
    returns = []
    for version in config_versions:
        events = store.query(event_type=TRADE_REVIEW_RECORDED, metadata_filter={"config_version": version}, limit=5000)
        for event in events:
            returns.append(_float((event.get("payload") or {}).get("latest_return_pct")))
    return {
        "sample_count": len(returns),
        "avg_return_pct": round(mean(returns), 4) if returns else 0.0,
        "win_rate_pct": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else 0.0,
    }


def _key_parameters(strategy: dict) -> dict:
    scoring = strategy.get("scoring", {})
    thresholds = scoring.get("thresholds", {})
    gates = scoring.get("decision_gates", {})
    position = strategy.get("risk", {}).get("position", {})
    auto_trade = strategy.get("auto_trade", {})
    continuation = strategy.get("continuation", {})
    continuation_scoring = continuation.get("scoring", {})
    return {
        "buy_threshold": _float(thresholds.get("buy")),
        "watch_threshold": _float(thresholds.get("watch")),
        "reject_threshold": _float(thresholds.get("reject")),
        "require_entry_signal_for_buy": bool(gates.get("require_entry_signal_for_buy", False)),
        "min_data_quality_for_buy": str(gates.get("min_data_quality_for_buy", "degraded")),
        "max_missing_fields_for_buy": gates.get("max_missing_fields_for_buy"),
        "single_max_pct": _float(position.get("single_max")),
        "total_max_pct": _float(position.get("total_max")),
        "weekly_max": int(position.get("weekly_max", 0) or 0),
        "continuation_top_n": int(continuation_scoring.get("top_n", 0) or 0),
        "continuation_hold_days": continuation_scoring.get("hold_days", []),
        "auto_trade_enabled": bool(auto_trade.get("enabled", False)),
        "auto_trade_dry_run": bool(auto_trade.get("dry_run", True)),
    }


def _profile_purpose(profile: str) -> str:
    return {
        "trend_swing": "趋势波段候选，适合 5-20 个交易日的确认型机会。",
        "short_continuation": "短线续涨研究，适合 T+1 到 T+3 的强势延续样本验证。",
        "defensive_watch": "弱市观察模式，提高买入门槛，优先减少新开仓。",
    }.get(profile, "自定义策略 profile。")


def _recommendations(rows: list[dict]) -> list[str]:
    if not rows:
        return ["没有发现可比较的策略 profile。"]
    if not any(row["evidence_status"] == "has_profile_runs" for row in rows):
        return [
            "先做影子运行并积累每个 profile 的 run_log、decision.suggested 和 trade.review.recorded，再比较胜率与收益。",
            "在有足够样本前，不要自动切换 ASTOCK_CONFIG_PROFILE，也不要做自动资金隔离。",
        ]
    ranked = sorted(
        rows,
        key=lambda item: (
            item["trade_review"]["sample_count"],
            item["trade_review"]["avg_return_pct"],
            item["run_count"],
        ),
        reverse=True,
    )
    top = ranked[0]
    return [
        f"当前证据最多的是 {top['name']}，但仍需结合样本数量、市场状态和人工复核决定是否用于执行。",
        "profile 对比只产生建议；执行前必须显式确认 ASTOCK_CONFIG_PROFILE。",
    ]


def _write_report_artifact(
    conn: Any,
    event_id: str,
    markdown: str,
    *,
    report_type: str = "strategy_profile_comparison",
    artifact_prefix: str = "strategy_profiles",
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"{artifact_prefix}_{event_id}",
            event_id,
            report_type,
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _scope_slug(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value).strip("_") or "unknown"
