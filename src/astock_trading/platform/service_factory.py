"""Shared service composition for CLI, MCP, and pipeline entrypoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from astock_trading.execution.service import ExecutionService
from astock_trading.execution.trade_logger import TradeLogger
from astock_trading.market.adapters import (
    AStockSignalAdapter,
    AkShareFinancialAdapter,
    AkShareFlowAdapter,
    AkShareHKFinancialAdapter,
    AkShareHKMarketAdapter,
    AkShareMarketAdapter,
    BaoStockMarketAdapter,
    BaiduFundFlowAdapter,
    MootdxMarketAdapter,
    MXMarketAdapter,
    MXSentimentAdapter,
    OpenCliFinanceAdapter,
    TencentFinancialAdapter,
    TushareClient,
    TushareFinancialAdapter,
    TushareFlowAdapter,
    TushareMarketAdapter,
)
from astock_trading.market.service import MarketService
from astock_trading.market.store import MarketStore
from astock_trading.platform.config import ConfigRegistry, ConfigSnapshot
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.paths import resolve_config_dir, resolve_path_from_config
from astock_trading.platform.runs import RunJournal
from astock_trading.reporting.obsidian import ObsidianProjector
from astock_trading.reporting.manual_confirmation import notify_manual_confirmation_requested
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.risk.service import RiskService
from astock_trading.strategy.decider import build_decider_from_config
from astock_trading.strategy.models import ScoringWeights
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.service import StrategyService

_logger = logging.getLogger(__name__)


@dataclass
class RuntimeServices:
    """Fully initialized runtime graph for operational entrypoints."""

    conn: Any
    event_store: EventStore
    run_journal: RunJournal
    config_snapshot: Optional[ConfigSnapshot]
    market_svc: MarketService
    strategy_svc: StrategyService
    risk_svc: RiskService
    exec_svc: ExecutionService
    projector: ProjectionUpdater
    reporter: ReportGenerator
    obsidian: ObsidianProjector
    vault_path: Optional[str] = None


def resolve_vault_path() -> Optional[str]:
    """Resolve the configured trade-vault path."""
    try:
        import yaml

        config_dir = resolve_config_dir()
        paths_file = config_dir / "paths.yaml"
        if not paths_file.exists():
            return None
        with open(paths_file, encoding="utf-8") as f:
            paths = yaml.safe_load(f) or {}
        vault_path = paths.get("vault_path", "")
        if not vault_path:
            return None
        return str(resolve_path_from_config(vault_path, config_dir))
    except Exception as exc:
        _logger.debug("[service_factory] vault path resolution skipped: %s", exc)
        return None


def load_config_snapshot(conn: Any) -> tuple[Optional[ConfigSnapshot], dict]:
    """Load and freeze strategy config, falling back to file config if versioning fails."""
    registry = ConfigRegistry()
    try:
        snapshot = registry.freeze(conn)
        return snapshot, snapshot.data.get("strategy", {})
    except Exception as exc:
        _logger.warning("[service_factory] 配置快照写入失败，使用未版本化文件配置: %s", exc)
        try:
            data, errors = registry.load_and_validate()
            if errors:
                _logger.warning("[service_factory] 配置校验警告: %s", errors)
            snapshot = ConfigSnapshot(
                version="unversioned",
                hash="unversioned",
                data=data,
            )
            return snapshot, data.get("strategy", {})
        except Exception as fallback_exc:
            _logger.error("[service_factory] 配置文件加载失败，使用空配置: %s", fallback_exc)
            return None, {}


def build_market_service(conn: Any, store: Optional[MarketStore] = None) -> MarketService:
    """Build the canonical market service provider chain."""
    market_store = store or MarketStore(conn)
    tushare_client = TushareClient.from_env()
    tushare_market_providers = (
        [TushareMarketAdapter(client=tushare_client)] if tushare_client.enabled else []
    )
    tushare_financial_providers = (
        [TushareFinancialAdapter(client=tushare_client)] if tushare_client.enabled else []
    )
    tushare_flow_providers = (
        [TushareFlowAdapter(client=tushare_client)] if tushare_client.enabled else []
    )
    return MarketService(
        market_providers=[
            *tushare_market_providers,
            MXMarketAdapter(),
            AStockSignalAdapter(),
            OpenCliFinanceAdapter(),
            MootdxMarketAdapter(),
            AkShareHKMarketAdapter(),
            AkShareMarketAdapter(),
            BaoStockMarketAdapter(),
        ],
        financial_providers=[
            *tushare_financial_providers,
            TencentFinancialAdapter(),
            AkShareHKFinancialAdapter(),
            AkShareFinancialAdapter(),
        ],
        flow_providers=[
            *tushare_flow_providers,
            BaiduFundFlowAdapter(),
            AkShareFlowAdapter(),
        ],
        sentiment_providers=[MXSentimentAdapter()],
        store=market_store,
    )


def build_strategy_service(event_store: EventStore, cfg: dict) -> StrategyService:
    """Build strategy scoring and decision services from config."""
    weights_cfg = cfg.get("scoring", {}).get("weights", {})
    scorer = Scorer(
        weights=ScoringWeights(
            technical=weights_cfg.get("technical", 3),
            fundamental=weights_cfg.get("fundamental", 2),
            flow=weights_cfg.get("flow", 2),
            sentiment=weights_cfg.get("sentiment", 3),
        ),
        veto_rules=cfg.get("scoring", {}).get("veto", []),
        entry_cfg=cfg.get("entry_signal", {}),
        continuation_cfg=cfg.get("continuation", {}),
        score_adjustments=cfg.get("scoring", {}).get("score_adjustments", {}),
    )
    decider = build_decider_from_config(cfg)
    return StrategyService(
        scorer,
        decider,
        event_store,
        manual_trade_notifier=notify_manual_confirmation_requested,
        false_breakout_cfg=cfg.get("risk", {}).get("false_breakout_cooldown", {}),
    )


def build_trade_hooks(event_store: EventStore, conn: Any, vault_path: Optional[str] = None) -> list:
    """Build optional hooks invoked after manual trade fills."""
    if not vault_path:
        return []
    try:
        return [TradeLogger(event_store, conn, vault_path)]
    except Exception as exc:
        _logger.debug("[service_factory] trade logger hook skipped: %s", exc)
        return []


def build_execution_service(
    event_store: EventStore,
    conn: Any,
    *,
    vault_path: Optional[str] = None,
) -> ExecutionService:
    """Build the execution service with configured side-effect hooks."""
    return ExecutionService(
        event_store,
        conn,
        on_trade=build_trade_hooks(event_store, conn, vault_path),
    )


def build_runtime_services() -> RuntimeServices:
    """Build the full service graph for a single operational runtime."""
    init_db()
    conn = connect()
    event_store = EventStore(conn)
    run_journal = RunJournal(conn)
    config_snapshot, cfg = load_config_snapshot(conn)
    vault_path = resolve_vault_path()

    market_svc = build_market_service(conn)
    strategy_svc = build_strategy_service(event_store, cfg)
    risk_svc = RiskService(event_store)
    exec_svc = build_execution_service(event_store, conn, vault_path=vault_path)
    projector = ProjectionUpdater(event_store, conn)
    reporter = ReportGenerator(event_store, conn)
    obsidian = ObsidianProjector(event_store, conn, vault_path)

    return RuntimeServices(
        conn=conn,
        event_store=event_store,
        run_journal=run_journal,
        config_snapshot=config_snapshot,
        market_svc=market_svc,
        strategy_svc=strategy_svc,
        risk_svc=risk_svc,
        exec_svc=exec_svc,
        projector=projector,
        reporter=reporter,
        obsidian=obsidian,
        vault_path=vault_path,
    )
