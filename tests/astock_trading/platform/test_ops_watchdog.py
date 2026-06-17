from __future__ import annotations


def test_ops_watchdog_escalates_schedule_failure_and_candidate_staleness():
    from astock_trading.platform.ops_watchdog import build_ops_watchdog_report

    report = build_ops_watchdog_report(
        schedule={
            "status": "warning",
            "failed_jobs": [
                {
                    "name": "A股候选池刷新",
                    "last_status": "error",
                    "failure_diagnosis": {
                        "error_type": "native_runtime_crash",
                        "exit_code": 133,
                        "log_path": "/tmp/screener_refresh.log",
                        "recovery_action": {
                            "command": "atrade screener refresh --json",
                            "writes_state": True,
                            "writes_order": False,
                        },
                    },
                }
            ],
        },
        health={
            "status": "warning",
            "inputs": {
                "candidate_pool": {
                    "total": 9,
                    "core_count": 0,
                    "execution_freshness": {
                        "fresh": False,
                        "age_hours": 27.4,
                        "max_age_hours": 24,
                        "blocker": "scoring_inputs_stale",
                    },
                }
            },
        },
        data_sources={
            "status": "warning",
            "provider_incidents": {
                "actionable_unresolved_recent": 0,
                "non_actionable_unresolved_recent": 4,
            },
            "data_source_blockers": [],
        },
        flow={
            "status": "warning",
            "flow_stage": {
                "auto_readiness": {
                    "status": "blocked",
                    "blockers": [
                        {"reason": "core_pool_empty", "label": "核心候选池为空"},
                        {"reason": "scoring_inputs_stale", "label": "候选评分超过 24 小时"},
                    ],
                }
            },
        },
    )

    reasons = {item["reason"] for item in report["incidents"]}

    assert report["status"] == "critical"
    assert report["guardrails"] == {
        "read_only": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "runs_pipeline": False,
    }
    assert "scheduled_job_failed" in reasons
    assert "candidate_scoring_stale" in reasons
    assert "core_pool_empty" in reasons
    assert report["next_actions"][0]["command"] == "atrade screener refresh --json"
    assert report["next_actions"][0]["writes_order"] is False
    assert "调度失败" in report["summary"]


def test_ops_watchdog_reports_ok_without_incidents():
    from astock_trading.platform.ops_watchdog import build_ops_watchdog_report

    report = build_ops_watchdog_report(
        schedule={"status": "ok", "failed_jobs": [], "missed_jobs": []},
        health={
            "status": "ok",
            "inputs": {
                "candidate_pool": {
                    "total": 12,
                    "core_count": 3,
                    "execution_freshness": {"fresh": True, "age_hours": 1.2, "max_age_hours": 24},
                }
            },
        },
        data_sources={
            "status": "ok",
            "provider_incidents": {
                "actionable_unresolved_recent": 0,
                "non_actionable_unresolved_recent": 0,
            },
            "data_source_blockers": [],
        },
        flow={
            "status": "ok",
            "flow_stage": {
                "auto_readiness": {
                    "status": "ready",
                    "blockers": [],
                }
            },
        },
    )

    assert report["status"] == "ok"
    assert report["incidents"] == []
    assert report["next_actions"] == []
    assert report["summary"] == "运维 watchdog 未发现流程断层。"


def test_ops_watchdog_monitor_notifies_on_new_incident_and_recovery():
    from astock_trading.platform.ops_watchdog import build_ops_watchdog_monitor

    report = {
        "status": "critical",
        "summary": "发现 1 个关键运维断点：调度失败。",
        "incidents": [
            {
                "severity": "critical",
                "component": "schedule",
                "reason": "scheduled_job_failed",
                "label": "调度失败",
                "evidence": {"name": "A股候选池刷新", "error_type": "native_runtime_crash"},
            }
        ],
    }

    first = build_ops_watchdog_monitor(report, previous_snapshot=None)
    second = build_ops_watchdog_monitor(report, previous_snapshot=first["snapshot"])
    recovered = build_ops_watchdog_monitor(
        {
            "status": "ok",
            "summary": "运维 watchdog 未发现流程断层。",
            "incidents": [],
        },
        previous_snapshot=first["snapshot"],
    )

    assert first["status"] == "changed"
    assert first["should_notify"] is True
    assert first["change_types"] == ["new_ops_incident"]
    assert second["status"] == "unchanged"
    assert second["should_notify"] is False
    assert recovered["status"] == "changed"
    assert recovered["should_notify"] is True
    assert recovered["change_types"] == ["ops_recovered"]
