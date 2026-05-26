"""Tests for automated manual follow-up summaries."""

from __future__ import annotations

from astock_trading.platform.manual_followup import build_manual_followup_payload


def test_manual_followup_prioritizes_health_and_classifies_positive_trials():
    payload = build_manual_followup_payload(
        opportunity={
            "status": "needs_health_check",
            "summary": "先修运行/数据问题，暂停新增交易判断。",
            "candidate_summary": {
                "total": 3,
                "core_count": 0,
                "watch_count": 2,
                "radar_count": 1,
                "entry_signal_count": 0,
                "summary": "候选池 3 只：核心 0、观察 2、强势观察 1；当前入场信号 0 只。",
            },
            "counts": {
                "buy_intents": 0,
                "stale_buy_intents": 2,
                "active_positive_trial_candidates": 2,
            },
            "blockers": ["L1 数据源失败未补齐", "核心池为空"],
            "next_action": {
                "type": "inspect_data_sources",
                "label": "检查数据覆盖",
                "command": "atrade data-sources diagnose --json",
                "reason": "L1 数据源失败未补齐，先诊断数据源再看新增交易。",
                "safe_to_auto_apply": True,
                "writes_state": False,
                "writes_environment": False,
                "writes_order": False,
                "requires_user_approval": False,
                "risk_level": "read_only",
                "command_contract_id": "data_sources_diagnose",
            },
            "stale_buy_intents": [
                {"code": "002384", "name": "东山精密", "stale_reason_label": "超过确认有效期"},
            ],
            "active_positive_trial_candidates": [
                {
                    "code": "600584",
                    "name": "长电科技",
                    "return_pct": 19.94,
                    "current_pool_tier": "watch",
                    "current_pool_tier_label": "观察",
                    "current_score": 5.0,
                    "current_entry_signal": False,
                    "current_data_quality": "ok",
                    "review_command": "atrade stock analyze 600584 --json",
                },
                {
                    "code": "600888",
                    "name": "新疆众和",
                    "return_pct": 17.93,
                    "current_pool_tier": "radar",
                    "current_pool_tier_label": "强势观察",
                    "current_score": 4.7,
                    "current_entry_signal": False,
                    "current_data_quality": "ok",
                    "review_command": "atrade stock analyze 600888 --json",
                },
            ],
        },
        trial_review={
            "review_summary": {"positive_count": 2, "price_anomaly_count": 0},
            "positive_reviews": [],
        },
        auto_readiness={
            "status": "blocked",
            "summary": "模拟盘自动交易预检未通过：核心候选池为空、没有新鲜买入意向。",
            "buy_side": {
                "ready": False,
                "blockers": [
                    {"reason": "core_pool_empty", "label": "核心候选池为空"},
                    {"reason": "no_fresh_buy_signal", "label": "没有新鲜买入意向"},
                ],
            },
            "next_action": {
                "command": "atrade paper auto-readiness --json",
                "writes_order": False,
                "requires_user_approval": False,
            },
        },
    )

    assert payload["command"] == "review manual-followup"
    assert payload["status"] == "needs_health_check"
    assert payload["summary"].startswith("先修运行/数据问题")
    assert payload["candidate_summary"]["core_count"] == 0
    assert payload["counts"]["positive_trial_candidates"] == 2
    assert payload["counts"]["stale_buy_intents"] == 1
    assert payload["next_action"]["command"] == "atrade data-sources diagnose --json"
    assert payload["guardrails"] == {
        "read_only": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "manual_confirmation_required_for_trade": True,
    }

    assert [item["classification"] for item in payload["candidate_reviews"]] == [
        "continue_observe",
        "continue_observe",
    ]
    assert payload["candidate_reviews"][0]["classification_label"] == "继续观察"
    assert payload["candidate_reviews"][0]["reason"] == "仍在观察层，且没有当前入场信号。"
    assert payload["candidate_reviews"][0]["next_action"]["command"] == "atrade stock analyze 600584 --json"
    assert payload["manual_actions"][0]["type"] == "review_stale_manual_confirmation"
    assert payload["source_commands"] == [
        "atrade opportunity --json",
        "atrade paper trial-review --json",
        "atrade paper auto-readiness --json",
        "atrade risk trial-guard --json",
    ]


def test_manual_followup_requires_approval_when_auto_trade_is_ready():
    payload = build_manual_followup_payload(
        opportunity={
            "status": "paper_auto_readiness",
            "summary": "核心候选已有入场信号，等待模拟承接预检。",
            "candidate_summary": {
                "total": 1,
                "core_count": 1,
                "watch_count": 0,
                "radar_count": 0,
                "entry_signal_count": 1,
                "summary": "候选池 1 只：核心 1、观察 0、强势观察 0；当前入场信号 1 只。",
            },
            "counts": {"buy_intents": 1, "stale_buy_intents": 0},
            "active_positive_trial_candidates": [
                {
                    "code": "688981",
                    "name": "中芯国际",
                    "return_pct": 5.2,
                    "current_pool_tier": "core",
                    "current_pool_tier_label": "核心",
                    "current_score": 6.4,
                    "current_entry_signal": True,
                    "current_primary_strategy_route_label": "资金确认趋势",
                    "current_data_quality": "ok",
                    "review_command": "atrade stock analyze 688981 --json",
                }
            ],
        },
        auto_readiness={
            "status": "ready",
            "summary": "模拟盘自动交易预检通过。",
            "buy_side": {"ready": True, "blockers": []},
            "next_action": {
                "type": "run_auto_trade",
                "label": "运行模拟盘自动交易",
                "command": "atrade run-pipeline auto_trade --json",
                "writes_order": True,
                "requires_user_approval": True,
                "risk_level": "paper_order_execution",
            },
        },
    )

    assert payload["status"] == "approval_required"
    assert payload["candidate_reviews"][0]["classification"] == "requires_paper_order_approval"
    assert payload["candidate_reviews"][0]["classification_label"] == "需要你确认"
    assert payload["candidate_reviews"][0]["next_action"]["command"] == "atrade paper auto-readiness --json"
    assert payload["manual_actions"][0] == {
        "type": "approve_paper_auto_trade",
        "label": "确认是否运行模拟盘自动交易",
        "command": "atrade run-pipeline auto_trade --json",
        "reason": "模拟承接预检已通过，但该命令可能提交 MX 模拟盘委托，必须由你明确批准。",
        "safe_to_auto_apply": False,
        "writes_state": True,
        "writes_environment": False,
        "writes_order": True,
        "requires_user_approval": True,
        "risk_level": "paper_order_execution",
    }
