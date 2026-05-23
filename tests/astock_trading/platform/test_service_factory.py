"""Service composition root contracts."""

from __future__ import annotations

from astock_trading.platform.db import connect, init_db


def test_build_market_service_uses_common_provider_order(tmp_path):
    from astock_trading.market.baostock_adapters import BaoStockMarketAdapter
    from astock_trading.market.hk_adapters import AkShareHKFinancialAdapter, AkShareHKMarketAdapter
    from astock_trading.market.service import MarketService
    from astock_trading.platform.service_factory import build_market_service

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        service = build_market_service(conn)
    finally:
        conn.close()

    assert isinstance(service, MarketService)
    assert any(isinstance(provider, AkShareHKMarketAdapter) for provider in service._market)
    assert any(isinstance(provider, BaoStockMarketAdapter) for provider in service._market)
    assert any(isinstance(provider, AkShareHKFinancialAdapter) for provider in service._financial)


def test_pipeline_context_uses_shared_market_service_builder(tmp_path, monkeypatch):
    from astock_trading.market.service import MarketService
    from astock_trading.platform import service_factory
    from astock_trading.pipeline.context import build_context

    calls = []

    def fake_build_market_service(conn):
        calls.append(conn)
        return MarketService()

    monkeypatch.setattr(service_factory, "build_market_service", fake_build_market_service)

    ctx = build_context(tmp_path / "test.db")
    try:
        assert calls == [ctx.conn]
        assert isinstance(ctx.market_svc, MarketService)
    finally:
        ctx.conn.close()


def test_load_config_snapshot_uses_file_config_when_freeze_fails(tmp_path, monkeypatch):
    from astock_trading.platform import service_factory

    class FakeRegistry:
        def freeze(self, conn):
            raise RuntimeError("duplicate config version")

        def load_and_validate(self):
            return {"strategy": {"entry_signal": {"volume_ratio_min": 1.2}}}, []

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    monkeypatch.setattr(service_factory, "ConfigRegistry", FakeRegistry)

    try:
        snapshot, cfg = service_factory.load_config_snapshot(conn)
    finally:
        conn.close()

    assert snapshot.version == "unversioned"
    assert snapshot.data["strategy"]["entry_signal"]["volume_ratio_min"] == 1.2
    assert cfg["entry_signal"]["volume_ratio_min"] == 1.2


def test_build_strategy_service_wires_manual_confirmation_notifier(tmp_path, monkeypatch):
    from astock_trading.market.models import (
        FinancialReport,
        FundFlow,
        SentimentData,
        StockQuote,
        StockSnapshot,
        TechnicalIndicators,
    )
    from astock_trading.platform import service_factory
    from astock_trading.platform.events import EventStore
    from astock_trading.strategy.models import MarketSignal, MarketState

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    calls = []
    monkeypatch.setattr(
        service_factory,
        "notify_manual_confirmation_requested",
        calls.append,
    )
    try:
        service = service_factory.build_strategy_service(EventStore(conn), {
            "scoring": {"thresholds": {"buy": 6.5, "watch": 5.0}},
        })
        service.evaluate(
            [
                StockSnapshot(
                    code="002138",
                    name="双环传动",
                    quote=StockQuote(
                        code="002138",
                        name="双环传动",
                        price=15.0,
                        open=14.8,
                        high=15.2,
                        low=14.7,
                        close=15.0,
                        volume=5000000,
                        amount=7.5e8,
                        change_pct=1.5,
                    ),
                    technical=TechnicalIndicators(
                        ma5=15.0,
                        ma10=14.5,
                        ma20=14.0,
                        ma60=13.0,
                        above_ma20=True,
                        volume_ratio=1.8,
                        rsi=55.0,
                        golden_cross=True,
                        ma20_slope=0.01,
                        momentum_5d=3.0,
                        daily_volatility=0.025,
                    ),
                    financial=FinancialReport(roe=12.0, revenue_growth=15.0, operating_cash_flow=1e8),
                    flow=FundFlow(net_inflow_1d=6e8, northbound_net_positive=True),
                    sentiment=SentimentData(score=2.0, detail="研报3篇"),
                )
            ],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_notify_factory",
            config_version="v_test",
        )
    finally:
        conn.close()

    assert len(calls) == 1
    assert calls[0]["manual_trade"]["code"] == "002138"


def test_mcp_init_uses_shared_runtime_factory(monkeypatch):
    from types import SimpleNamespace

    import astock_trading.platform.mcp_server as srv
    from astock_trading.platform import service_factory

    fake = SimpleNamespace(
        conn=object(),
        event_store=object(),
        run_journal=object(),
        exec_svc=object(),
        reporter=object(),
        market_svc=object(),
        strategy_svc=object(),
        config_snapshot=object(),
    )

    monkeypatch.setattr(service_factory, "build_runtime_services", lambda: fake)
    srv._conn = None

    srv._init()

    try:
        assert srv._conn is fake.conn
        assert srv._event_store is fake.event_store
        assert srv._run_journal is fake.run_journal
        assert srv._exec_svc is fake.exec_svc
        assert srv._report_gen is fake.reporter
        assert srv._market_svc is fake.market_svc
        assert srv._strategy_svc is fake.strategy_svc
        assert srv._config_snapshot is fake.config_snapshot
    finally:
        srv._conn = None
        srv._event_store = None
        srv._run_journal = None
        srv._exec_svc = None
        srv._report_gen = None
        srv._market_svc = None
        srv._strategy_svc = None
        srv._config_snapshot = None


def test_paper_mcp_tools_have_focused_registrar():
    from astock_trading.platform.mcp_tools.paper import register_paper_tools

    assert callable(register_paper_tools)
