from __future__ import annotations

import asyncio
import json
import sqlite3

import astock_trading.platform.stock_analysis as stock_analysis
from astock_trading.market.models import StockQuote, StockSnapshot, TechnicalIndicators
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.history_mirror import archive_signal_history
from astock_trading.platform.stock_analysis import (
    build_stock_analysis_payload,
    lookup_stock_identifier_from_db,
    resolve_stock_identifier,
    _with_resolved_snapshot_name,
)
from astock_trading.strategy.models import (
    Action,
    DataQuality,
    DecisionIntent,
    DimensionScore,
    MarketSignal,
    MarketState,
    ScoreResult,
    Style,
    StrategyRouteDiagnostic,
    StrategyRouteEvidence,
)


def test_resolve_stock_identifier_supports_chinese_name():
    async def resolver(query: str) -> list[dict]:
        assert query == "三安光电"
        return [{"代码": "600703", "名称": "三安光电"}]

    result = asyncio.run(resolve_stock_identifier("三安光电", resolver=resolver))

    assert result == {"code": "600703", "name": "三安光电", "source": "screener"}


def test_resolve_stock_identifier_backfills_name_for_input_code():
    result = asyncio.run(
        resolve_stock_identifier(
            "600703",
            name_lookup=lambda value: {"code": value, "name": "三安光电", "source": "local_cache"},
        )
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "local_cache"}


def test_resolve_stock_identifier_uses_screener_for_input_code_when_local_cache_misses():
    async def resolver(query: str) -> list[dict]:
        assert query == "600703"
        return [{"代码": "600703", "名称": "三安光电"}]

    result = asyncio.run(
        resolve_stock_identifier("600703", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "screener"}


def test_resolve_stock_identifier_uses_basic_info_for_input_code_when_screener_misses(monkeypatch):
    async def resolver(query: str) -> list[dict]:
        assert query == "600519"
        return []

    async def basic_info_lookup(code: str) -> str | None:
        assert code == "600519"
        return "贵州茅台"

    monkeypatch.setattr(stock_analysis, "_lookup_stock_name_from_basic_info", basic_info_lookup)

    result = asyncio.run(
        resolve_stock_identifier("600519", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600519", "name": "贵州茅台", "source": "basic_info"}


def test_resolve_stock_identifier_uses_spot_snapshot_for_name_when_screener_misses(monkeypatch):
    async def resolver(query: str) -> list[dict]:
        assert query == "三安光电"
        return []

    async def spot_lookup(identifier: str) -> dict | None:
        assert identifier == "三安光电"
        return {"code": "600703", "name": "三安光电", "source": "spot"}

    monkeypatch.setattr(stock_analysis, "_lookup_stock_from_spot", spot_lookup)

    result = asyncio.run(
        resolve_stock_identifier("三安光电", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "spot"}


def test_lookup_stock_identifier_from_db_resolves_recent_observation_name():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE projection_candidate_pool (
            code TEXT,
            pool_tier TEXT,
            name TEXT,
            score REAL,
            added_at TEXT,
            last_scored_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE market_observations (
            observation_id TEXT,
            source TEXT,
            kind TEXT,
            symbol TEXT,
            observed_at TEXT,
            run_id TEXT,
            payload_json TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO market_observations
           (observation_id, source, kind, symbol, observed_at, payload_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "obs-1",
            "test",
            "stock_news",
            "600703",
            "2026-05-16T00:00:00+00:00",
            json.dumps({"quote": {"代码": "600703", "名称": "三安光电"}}, ensure_ascii=False),
        ),
    )

    assert lookup_stock_identifier_from_db(conn, "三安光电") == {
        "code": "600703",
        "name": "三安光电",
        "source": "local_cache",
    }
    assert lookup_stock_identifier_from_db(conn, "600703") == {
        "code": "600703",
        "name": "三安光电",
        "source": "local_cache",
    }


def test_with_resolved_snapshot_name_updates_quote_name_when_provider_returns_code():
    snapshot = StockSnapshot(
        code="600703",
        name="600703",
        quote=StockQuote(
            code="600703",
            name="600703",
            price=12.3,
            open=12.0,
            high=12.5,
            low=11.9,
            close=12.3,
            volume=1000000,
            amount=12300000,
            change_pct=1.2,
        ),
    )

    result = _with_resolved_snapshot_name(snapshot, "三安光电")

    assert result.name == "三安光电"
    assert result.quote.name == "三安光电"


def test_recent_history_signal_analysis_returns_real_miss_reason(tmp_path):
    db_path = tmp_path / "history.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id="hist_stock_analysis",
            run_id="screener_101500",
            phase="screener",
            market={"signal": "YELLOW"},
            candidates=[{"code": "600703", "name": "三安光电", "total_score": 5.8}],
            decisions=[{"code": "600703", "name": "三安光电", "action": "WATCH", "notes": ["缺少入场信号"]}],
        )

        payload = stock_analysis._recent_history_signal_analysis(conn, "600703", days=7)
    finally:
        conn.close()

    assert payload["history_group_id"] == "hist_stock_analysis"
    assert payload["decision_action"] == "WATCH"
    assert "观察" in payload["miss_reason"]


def test_build_stock_analysis_payload_marks_report_non_executable():
    snapshot = StockSnapshot(
        code="600703",
        name="三安光电",
        quote=StockQuote(
            code="600703",
            name="三安光电",
            price=12.3,
            open=12.0,
            high=12.5,
            low=11.9,
            close=12.3,
            volume=1000000,
            amount=12300000,
            change_pct=1.2,
        ),
        technical=TechnicalIndicators(
            ma5=12.0,
            ma10=11.8,
            ma20=11.5,
            ma60=10.8,
            above_ma20=True,
            volume_ratio=1.8,
            rsi=58,
            golden_cross=True,
            ma20_slope=0.01,
            momentum_5d=3.0,
            daily_volatility=0.02,
            deviation_rate=4.0,
            change_pct=1.2,
        ),
    )
    score = ScoreResult(
        code="600703",
        name="三安光电",
        total=6.3,
        dimensions=[
            DimensionScore("technical", 2.4, 3.0, "技术达标"),
            DimensionScore("fundamental", 1.4, 3.0, "基本面可用"),
            DimensionScore("flow", 1.0, 2.0, "资金一般"),
            DimensionScore("sentiment", 1.5, 3.0, "中性"),
        ],
        entry_signal=True,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="600703",
        name="三安光电",
        action=Action.BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.16,
        market_signal=MarketSignal.GREEN,
        market_multiplier=0.8,
    )

    payload = build_stock_analysis_payload(
        identifier="三安光电",
        resolved={"code": "600703", "name": "三安光电", "source": "screener"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=0.8, detail={"沪深300": "ok"}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={"pool_tier": "watch", "score": 6.1},
        history=[{"date": "2026-05-16", "total_score": 6.1}],
        history_signal={
            "source": "history_mirror",
            "snapshot_date": "2026-05-19",
            "history_group_id": "hist_stock_analysis",
            "miss_reason": "观察：缺少入场信号",
        },
    )

    assert payload["analysis"] == "stock"
    assert payload["execution_allowed"] is False
    assert payload["resolved"]["code"] == "600703"
    assert payload["score"]["total_score"] == 6.3
    assert payload["decision"]["action"] == "BUY"
    assert payload["market"]["signal"] == "GREEN"
    assert payload["candidate_pool"]["pool_tier"] == "watch"
    assert payload["history"][0]["total_score"] == 6.1
    assert payload["history_signal"]["miss_reason"] == "观察：缺少入场信号"
    assert "历史镜像：观察：缺少入场信号" in payload["findings"]
    assert "任何下单前都需要人工确认" in payload["recommendations"][0]


def test_build_stock_analysis_payload_flags_candidate_pool_mismatch():
    snapshot = StockSnapshot(
        code="688981",
        name="中芯国际",
        quote=StockQuote(
            code="688981",
            name="中芯国际",
            price=131.33,
            open=130.0,
            high=132.11,
            low=126.59,
            close=131.33,
            volume=1000000,
            amount=131330000,
            change_pct=0.02,
        ),
    )
    score = ScoreResult(
        code="688981",
        name="中芯国际",
        total=5.1,
        dimensions=[
            DimensionScore("technical", 1.8, 3.0, "技术转弱"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.0, 2.0, "资金一般"),
            DimensionScore("sentiment", 1.1, 3.0, "中性"),
        ],
        entry_signal=False,
        style=Style.MOMENTUM,
        style_confidence=0.52,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="688981",
        name="中芯国际",
        action=Action.WATCH,
        confidence=5.1,
        score=5.1,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    payload = build_stock_analysis_payload(
        identifier="688981",
        resolved={"code": "688981", "name": "中芯国际", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "688981",
            "name": "中芯国际",
            "pool_tier": "core",
            "score": 6.4,
            "last_scored_at": "2026-05-22",
        },
    )

    assert payload["candidate_pool_consistency"] == {
        "status": "current_analysis_weaker_than_pool",
        "summary": "当前单股即时判断为观察，但候选池仍显示核心；先刷新候选池证据，不把旧核心状态当作可模拟买入依据。",
        "pool_tier": "core",
        "pool_score": 6.4,
        "pool_last_scored_at": "2026-05-22",
        "current_action": "WATCH",
        "current_score": 5.1,
        "score_delta": -1.3,
        "requires_pool_refresh": True,
        "diagnostic_commands": {
            "refresh_candidate_pool": "atrade screener refresh --json",
            "diagnose_flow": "atrade diagnose flow --json",
        },
    }
    assert payload["next_action"] == {
        "type": "refresh_candidate_pool_state",
        "label": "刷新候选池证据",
        "command": "atrade screener refresh --json",
        "reason": "当前单股即时判断与候选池层级不一致，先重建候选池证据。",
        "safe_to_auto_apply": False,
        "writes_state": True,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "state_write",
        "command_contract_id": "screener_refresh",
    }
    assert any("候选池一致性：当前单股即时判断为观察，但候选池仍显示核心" in item for item in payload["findings"])


def test_build_stock_analysis_payload_guides_core_buy_to_auto_readiness():
    snapshot = StockSnapshot(
        code="002384",
        name="东山精密",
        quote=StockQuote(
            code="002384",
            name="东山精密",
            price=220.55,
            open=207.0,
            high=221.0,
            low=205.0,
            close=220.55,
            volume=1000000,
            amount=220550000,
            change_pct=6.56,
        ),
        technical=TechnicalIndicators(
            ma5=216.0,
            ma10=210.0,
            ma20=204.0,
            ma60=190.0,
            above_ma20=True,
            volume_ratio=0.92,
            rsi=67.6,
            golden_cross=True,
            ma20_slope=0.061,
            momentum_5d=8.46,
            daily_volatility=0.03,
            deviation_rate=8.18,
            change_pct=6.56,
        ),
    )
    score = ScoreResult(
        code="002384",
        name="东山精密",
        total=7.0,
        dimensions=[
            DimensionScore("technical", 2.5, 3.0, "资金趋势确认"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.8, 2.0, "偏强"),
        ],
        entry_signal=True,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
        strategy_routes=[
            StrategyRouteEvidence(
                route="flow_confirmed_trend",
                display_name="资金趋势确认",
                family="trend_swing",
                confidence=0.88,
                entry_signal=True,
            )
        ],
        primary_strategy_route="flow_confirmed_trend",
    )
    decision = DecisionIntent(
        code="002384",
        name="东山精密",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.2,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    payload = build_stock_analysis_payload(
        identifier="002384",
        resolved={"code": "002384", "name": "东山精密", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "score": 7.0,
            "last_scored_at": "2026-05-22",
        },
    )

    assert payload["candidate_pool_consistency"]["status"] == "aligned"
    assert payload["code"] == "002384"
    assert payload["name"] == "东山精密"
    assert payload["score_total"] == 7.0
    assert payload["score"]["primary_strategy_route"] == "flow_confirmed_trend"
    assert payload["score"]["primary_strategy_route_label"] == "资金趋势确认"
    assert payload["action"] == "BUY"
    assert payload["action_label"] == "买入意向"
    assert payload["entry_signal"] is True
    assert payload["summary"] == (
        "东山精密(002384) 评分 7.0，买入意向，入场信号已触发；"
        "候选池层级：核心。下一步：复核模拟承接预检。"
    )
    assert payload["decision"]["action"] == "BUY"
    assert payload["next_action"] == {
        "type": "paper_auto_readiness",
        "label": "复核模拟承接预检",
        "command": "atrade paper auto-readiness --json",
        "reason": "当前是核心候选且已形成买入意向；先检查 profile、买入窗口、风控和模拟盘承接状态，不自动下单。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "paper_auto_readiness",
    }


def test_build_stock_analysis_payload_surfaces_trial_buy_intent():
    snapshot = StockSnapshot(
        code="002384",
        name="东山精密",
        quote=StockQuote(
            code="002384",
            name="东山精密",
            price=223.99,
            open=220.0,
            high=232.02,
            low=219.22,
            close=223.99,
            volume=117493472,
            amount=183095697.72,
            change_pct=0.0,
        ),
        technical=TechnicalIndicators(
            ma5=211.62,
            ma10=215.61,
            ma20=214.02,
            ma60=158.49,
            above_ma20=True,
            volume_ratio=1.22,
            rsi=46.2,
            golden_cross=False,
            ma20_slope=0.0222,
            momentum_5d=-4.64,
            daily_volatility=0.0665,
            deviation_rate=-1.45,
        ),
    )
    score = ScoreResult(
        code="002384",
        name="东山精密",
        total=6.1,
        dimensions=[
            DimensionScore("technical", 1.8, 3.0, "技术偏强"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.6, 2.0, "情绪偏强"),
        ],
        entry_signal=False,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="002384",
        name="东山精密",
        action=Action.TRIAL_BUY,
        confidence=6.1,
        score=6.1,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
        notes=["大盘 RED，禁止新开仓；降级为试买意向"],
    )

    payload = build_stock_analysis_payload(
        identifier="002384",
        resolved={"code": "002384", "name": "东山精密", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.RED, multiplier=0.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "watch",
            "score": 5.9,
            "last_scored_at": "2026-06-03",
        },
    )

    assert payload["action"] == "TRIAL_BUY"
    assert payload["action_label"] == "试买意向"
    assert payload["execution_allowed"] is False
    assert "试买意向" in payload["summary"]
    assert payload["next_action"] == {
        "type": "trial_buy_risk_guard",
        "label": "计算试买仓位上限",
        "command": "atrade risk trial-guard --json",
        "reason": "当前是试买意向；系统只给低置信小仓判断，不写成交、不提交模拟盘订单。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "risk_trial_guard",
    }
    assert "试买意向不会触发自动下单" in payload["recommendations"][1]


def test_build_stock_analysis_payload_keeps_core_entry_signal_when_execution_gate_blocks_buy():
    snapshot = StockSnapshot(
        code="002384",
        name="东山精密",
        quote=StockQuote(
            code="002384",
            name="东山精密",
            price=220.55,
            open=207.0,
            high=221.0,
            low=205.0,
            close=220.55,
            volume=1000000,
            amount=220550000,
            change_pct=6.56,
        ),
        technical=TechnicalIndicators(
            ma5=216.0,
            ma10=210.0,
            ma20=204.0,
            ma60=190.0,
            above_ma20=True,
            volume_ratio=0.92,
            rsi=67.6,
            golden_cross=True,
            ma20_slope=0.061,
            momentum_5d=8.46,
            daily_volatility=0.03,
            deviation_rate=8.18,
            change_pct=6.56,
        ),
    )
    score = ScoreResult(
        code="002384",
        name="东山精密",
        total=7.0,
        dimensions=[
            DimensionScore("technical", 2.5, 3.0, "资金趋势确认"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.8, 2.0, "偏强"),
        ],
        entry_signal=True,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
        strategy_routes=[
            StrategyRouteEvidence(
                route="flow_confirmed_trend",
                display_name="资金趋势确认",
                family="trend_swing",
                confidence=0.88,
                entry_signal=True,
            )
        ],
        primary_strategy_route="flow_confirmed_trend",
    )
    decision = DecisionIntent(
        code="002384",
        name="东山精密",
        action=Action.WATCH,
        confidence=7.0,
        score=7.0,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
        notes=["大盘 RED，禁止新开仓"],
    )
    execution_readiness = {
        "status": "profile_review_required",
        "summary": "当前仍在 default 混合配置；其他阻断：当前不在模拟买入窗口、没有新鲜买入意向。",
        "next_action": {
            "type": "review_recorded_profile_activation",
            "label": "复核已记录的 profile 激活计划",
            "command": "atrade strategy profile-activation --target trend_swing --json",
            "safe_to_auto_apply": False,
        },
        "execution_profile": {
            "status": "review_required",
            "safe_to_auto_apply": False,
        },
        "buy_side": {
            "current_entry_signals": [
                {
                    "code": "002384",
                    "name": "东山精密",
                    "entry_signal": True,
                    "review_command": "atrade stock analyze 002384 --json",
                }
            ],
            "signal_gap": {
                "status": "entry_signal_without_fresh_buy_intent",
                "summary": "当前核心候选已有入场信号，但没有同日新鲜买入意向。",
            },
        },
    }

    payload = build_stock_analysis_payload(
        identifier="002384",
        resolved={"code": "002384", "name": "东山精密", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.RED, multiplier=0.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "score": 7.0,
            "last_scored_at": "2026-05-22",
        },
        execution_readiness=execution_readiness,
    )

    assert payload["candidate_pool_consistency"]["status"] == "execution_gate_blocked"
    assert payload["candidate_pool_consistency"]["requires_pool_refresh"] is False
    assert payload["candidate_pool_consistency"]["execution_gate"] == {
        "market_signal": "RED",
        "market_multiplier": 0.0,
        "notes": ["大盘 RED，禁止新开仓"],
    }
    assert payload["next_action"]["type"] == "review_recorded_profile_activation"
    assert payload["next_action"]["command"] == "atrade strategy profile-activation --target trend_swing --json"
    assert payload["next_action"]["writes_state"] is False
    assert "刷新候选池证据" not in payload["summary"]
    assert any("执行闸门" in item for item in payload["findings"])


def test_build_stock_analysis_payload_marks_instant_buy_without_fresh_decision_gap():
    snapshot = StockSnapshot(
        code="002384",
        name="东山精密",
        quote=StockQuote(
            code="002384",
            name="东山精密",
            price=220.55,
            open=207.0,
            high=221.0,
            low=205.0,
            close=220.55,
            volume=1000000,
            amount=220550000,
            change_pct=6.56,
        ),
        technical=TechnicalIndicators(
            ma5=216.0,
            ma10=210.0,
            ma20=204.0,
            ma60=190.0,
            above_ma20=True,
            volume_ratio=0.92,
            rsi=67.6,
            golden_cross=True,
            ma20_slope=0.061,
            momentum_5d=8.46,
            daily_volatility=0.03,
            deviation_rate=8.18,
            change_pct=6.56,
        ),
    )
    score = ScoreResult(
        code="002384",
        name="东山精密",
        total=7.0,
        dimensions=[
            DimensionScore("technical", 2.5, 3.0, "资金趋势确认"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.8, 2.0, "偏强"),
        ],
        entry_signal=True,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="002384",
        name="东山精密",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.2,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )
    execution_readiness = {
        "status": "profile_review_required",
        "summary": "当前仍在 default 混合配置；其他阻断：当前不在模拟买入窗口、没有新鲜买入意向。",
        "next_action": {
            "type": "review_recorded_profile_activation",
            "label": "复核已记录的 profile 激活计划",
            "command": "atrade strategy profile-activation --target trend_swing --json",
            "safe_to_auto_apply": False,
        },
        "fresh_buy_signal": {"count": 0, "top": {}},
        "buy_side": {
            "status": "blocked",
            "blockers": [
                {"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"},
                {"reason": "no_fresh_buy_signal", "label": "没有新鲜买入意向"},
            ],
            "current_entry_signals": [
                {
                    "code": "002384",
                    "name": "东山精密",
                    "entry_signal": True,
                    "review_command": "atrade stock analyze 002384 --json",
                }
            ],
            "signal_gap": {
                "status": "entry_signal_without_fresh_buy_intent",
                "summary": "当前核心候选已有入场信号，但没有同日新鲜买入意向。",
            },
        },
    }

    payload = build_stock_analysis_payload(
        identifier="002384",
        resolved={"code": "002384", "name": "东山精密", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "score": 7.0,
            "last_scored_at": "2026-05-22",
        },
        execution_readiness=execution_readiness,
    )

    assert payload["decision_scope"] == {
        "type": "read_only_instant_analysis",
        "summary": "本命令只做即时单股分析，不写入 decision.suggested；模拟承接以同日新鲜买入意向和 paper auto-readiness 为准。",
        "writes_state": False,
        "writes_decision_event": False,
        "execution_allowed": False,
    }
    assert payload["execution_readiness"]["status"] == "profile_review_required"
    assert payload["execution_readiness"]["fresh_buy_signal"]["count"] == 0
    assert payload["execution_readiness"]["buy_side"]["signal_gap"]["status"] == (
        "entry_signal_without_fresh_buy_intent"
    )
    assert payload["summary"] == (
        "东山精密(002384) 评分 7.0，买入意向，入场信号已触发；候选池层级：核心。"
        "该结论是只读即时判断，尚未形成可承接的同日买入意向。下一步：复核运行 profile 激活。"
    )
    assert payload["next_action"] == {
        "type": "review_recorded_profile_activation",
        "label": "复核运行 profile 激活",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "reason": "当前单股分析是只读买入意向，但运行 profile 仍需人工确认；先只读复核 profile 激活计划，不进入模拟承接。",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert any("尚未形成可承接的同日买入意向" in item for item in payload["findings"])


def test_build_stock_analysis_payload_guides_watch_candidate_to_shadow_risk_check():
    snapshot = StockSnapshot(
        code="600584",
        name="长电科技",
        quote=StockQuote(
            code="600584",
            name="长电科技",
            price=72.88,
            open=70.0,
            high=73.0,
            low=69.5,
            close=72.88,
            volume=1000000,
            amount=72880000,
            change_pct=2.4,
        ),
    )
    score = ScoreResult(
        code="600584",
        name="长电科技",
        total=5.8,
        dimensions=[
            DimensionScore("technical", 1.7, 3.0, "入场信号不足"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.4, 3.0, "中性"),
        ],
        entry_signal=False,
        style=Style.MOMENTUM,
        style_confidence=0.7,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="600584",
        name="长电科技",
        action=Action.WATCH,
        confidence=5.8,
        score=5.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    payload = build_stock_analysis_payload(
        identifier="600584",
        resolved={"code": "600584", "name": "长电科技", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "score": 5.8,
            "last_scored_at": "2026-05-22",
        },
    )

    assert payload["candidate_pool_consistency"]["status"] == "aligned"
    assert payload["next_action"] == {
        "type": "continue_shadow_trial",
        "label": "继续影子观察",
        "command": "atrade paper trial-plan --json",
        "reason": "当前仍是观察候选且入场信号未触发；继续用影子试运行清单跟踪，不进入模拟买入。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "paper_trial_plan",
    }


def test_build_stock_analysis_payload_explains_missing_entry_signal_causes():
    snapshot = StockSnapshot(
        code="600584",
        name="长电科技",
        quote=StockQuote(
            code="600584",
            name="长电科技",
            price=72.88,
            open=67.05,
            high=73.14,
            low=63.95,
            close=72.88,
            volume=1000000,
            amount=72880000,
            change_pct=9.04,
        ),
        technical=TechnicalIndicators(
            ma5=65.02,
            ma10=60.6,
            ma20=53.49,
            ma60=46.91,
            above_ma20=True,
            volume_ratio=1.09,
            rsi=89.5,
            golden_cross=False,
            ma20_slope=0.0924,
            momentum_5d=25.55,
            daily_volatility=0.1261,
            deviation_rate=36.24,
            change_pct=9.04,
        ),
    )
    score = ScoreResult(
        code="600584",
        name="长电科技",
        total=5.9,
        dimensions=[
            DimensionScore("technical", 1.7, 3.0, "入场信号不足"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.5, 3.0, "中性"),
        ],
        entry_signal=False,
        style=Style.MOMENTUM,
        style_confidence=1.0,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="600584",
        name="长电科技",
        action=Action.WATCH,
        confidence=5.9,
        score=5.9,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    payload = build_stock_analysis_payload(
        identifier="600584",
        resolved={"code": "600584", "name": "长电科技", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "score": 5.8,
            "last_scored_at": "2026-05-22",
        },
    )

    blocker = next(item for item in payload["findings"] if item.startswith("入场阻断："))
    assert "未出现金叉" in blocker
    assert "量能确认不足" in blocker
    assert "RSI 过热" in blocker
    assert "乖离率过高" in blocker
    assert "当日涨幅较大" in blocker


def test_build_stock_analysis_payload_explains_route_missing_conditions():
    snapshot = StockSnapshot(
        code="600584",
        name="长电科技",
        quote=StockQuote(
            code="600584",
            name="长电科技",
            price=72.88,
            open=70.0,
            high=73.0,
            low=69.5,
            close=72.88,
            volume=1000000,
            amount=72880000,
            change_pct=2.4,
        ),
        technical=TechnicalIndicators(
            ma5=72.0,
            ma10=70.5,
            ma20=68.0,
            ma60=60.0,
            above_ma20=True,
            volume_ratio=0.0,
            rsi=58.0,
            golden_cross=True,
            ma20_slope=0.02,
            momentum_5d=5.5,
            daily_volatility=0.03,
            deviation_rate=6.0,
            change_pct=2.4,
        ),
    )
    score = ScoreResult(
        code="600584",
        name="长电科技",
        total=5.8,
        dimensions=[
            DimensionScore("technical", 1.7, 3.0, "趋势观察"),
            DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
            DimensionScore("flow", 1.5, 2.0, "资金较强"),
            DimensionScore("sentiment", 1.4, 3.0, "中性"),
        ],
        entry_signal=False,
        style=Style.MOMENTUM,
        style_confidence=0.7,
        data_quality=DataQuality.OK,
        strategy_routes=[
            StrategyRouteEvidence(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                confidence=0.62,
                entry_signal=False,
                status="watch",
                route_score=0.75,
                matched_conditions=["above_ma20", "ma20_slope", "momentum_5d"],
                missing_conditions=["volume_ratio"],
            )
        ],
        route_diagnostics=[
            StrategyRouteDiagnostic(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                status="watch",
                route_score=0.75,
                matched_conditions=["above_ma20", "ma20_slope", "momentum_5d"],
                missing_conditions=["volume_ratio"],
                entry_signal=False,
                confidence=0.62,
            )
        ],
        primary_strategy_route="trend_watch",
    )
    decision = DecisionIntent(
        code="600584",
        name="长电科技",
        action=Action.WATCH,
        confidence=5.8,
        score=5.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    payload = build_stock_analysis_payload(
        identifier="600584",
        resolved={"code": "600584", "name": "长电科技", "source": "local_cache"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "score": 5.8,
            "last_scored_at": "2026-05-22",
        },
    )

    assert payload["score"]["route_diagnostics"][0]["missing_conditions"] == ["volume_ratio"]
    route_gap = next(item for item in payload["findings"] if item.startswith("路线缺口："))
    assert "趋势观察" in route_gap
    assert "量比确认" in route_gap
