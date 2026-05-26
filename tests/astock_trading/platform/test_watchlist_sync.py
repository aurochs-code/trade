"""Tests for MX watchlist synchronization planning."""

from astock_trading.platform.watchlist_sync import build_watchlist_sync_plan


def test_watchlist_sync_plan_preserves_mx_and_local_holdings():
    plan = build_watchlist_sync_plan(
        candidates=[
            {"code": "600001", "name": "核心票", "pool_tier": "core", "score": 6.8},
            {"code": "600002", "name": "观察票", "pool_tier": "watch", "score": 5.4},
            {"code": "600003", "name": "强势票", "pool_tier": "radar", "score": 4.9},
            {"code": "600004", "name": "本地持仓候选", "pool_tier": "watch", "score": 5.2},
        ],
        current_watchlist=[
            {"code": "600002", "name": "观察票"},
            {"code": "600004", "name": "本地持仓候选"},
            {"code": "600005", "name": "MX持仓"},
            {"code": "600999", "name": "旧自选"},
        ],
        mx_positions=[
            {"code": "600005", "name": "MX持仓", "shares": 100},
        ],
        local_positions=[
            {"code": "600004", "name": "本地持仓候选", "shares": 200},
        ],
    )

    assert [item["code"] for item in plan["add"]] == ["600001", "600003"]
    assert [item["code"] for item in plan["remove"]] == ["600999"]
    assert [item["code"] for item in plan["keep_positions"]] == ["600004", "600005"]
    assert [item["code"] for item in plan["keep_candidates"]] == ["600002"]
    assert [item["code"] for item in plan["skipped_candidate_holdings"]] == ["600004"]
    assert plan["target_count"] == 3
    assert plan["preserve_holdings"] is True


def test_watchlist_sync_plan_adds_missing_holdings_to_target_watchlist():
    plan = build_watchlist_sync_plan(
        candidates=[
            {"code": "600001", "name": "观察票", "pool_tier": "watch", "score": 5.4},
        ],
        current_watchlist=[
            {"code": "600999", "name": "旧自选"},
        ],
        mx_positions=[
            {"code": "600005", "name": "MX持仓", "shares": 100},
        ],
        local_positions=[
            {"code": "600006", "name": "本地持仓", "shares": 200},
        ],
    )

    assert [item["code"] for item in plan["remove"]] == ["600999"]
    assert [item["code"] for item in plan["add_positions"]] == ["600005", "600006"]
    assert [item["code"] for item in plan["add_candidates"]] == ["600001"]
    assert [item["code"] for item in plan["add"]] == ["600005", "600006", "600001"]
    assert [item["code"] for item in plan["desired_watchlist"]] == ["600005", "600006", "600001"]
    assert plan["add_position_count"] == 2
    assert plan["add_candidate_count"] == 1
