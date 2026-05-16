"""
pipeline/context.py — Pipeline 运行上下文

所有 pipeline 共享的 service 初始化和配置加载。
一次初始化，整个 pipeline 复用。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.config import ConfigRegistry, ConfigSnapshot
from astock_trading.platform.runs import RunJournal
from astock_trading.market.adapters import (
    AkShareMarketAdapter, AkShareFinancialAdapter, AkShareFlowAdapter,
    AkShareHKMarketAdapter, AkShareHKFinancialAdapter,
    MXMarketAdapter, MXSentimentAdapter, AStockSignalAdapter, BaiduFundFlowAdapter, MootdxMarketAdapter, TencentFinancialAdapter,
)
from astock_trading.market.service import MarketService
from astock_trading.market.store import MarketStore
from astock_trading.strategy.models import ScoringWeights
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.service import RiskService
from astock_trading.execution.service import ExecutionService
from astock_trading.execution.trade_logger import TradeLogger
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.reporting.obsidian import ObsidianProjector

_logger = logging.getLogger(__name__)


def _resolve_vault_path() -> Optional[str]:
    try:
        import yaml
        p = Path(__file__).parent.parent.parent.parent / "config" / "paths.yaml"
        if p.exists():
            with open(p) as f:
                paths = yaml.safe_load(f) or {}
            vp = paths.get("vault_path", "")
            if vp:
                resolved = Path(vp)
                if not resolved.is_absolute():
                    resolved = Path(__file__).parent.parent.parent.parent / vp
                return str(resolved)
    except Exception:
        pass
    return None


@dataclass
class PipelineContext:
    """所有 pipeline 共享的运行上下文。"""
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

    @property
    def cfg(self) -> dict:
        if self.config_snapshot:
            return self.config_snapshot.data.get("strategy", {})
        return {}

    @property
    def config_version(self) -> str:
        return self.config_snapshot.version if self.config_snapshot else "unknown"

    @property
    def capital(self) -> float:
        return self.cfg.get("capital", 450000)


def build_context(db_path: Optional[Path] = None) -> PipelineContext:
    """构建完整的 pipeline 上下文。"""
    init_db(db_path)
    conn = connect(db_path)
    event_store = EventStore(conn)
    run_journal = RunJournal(conn)

    # Config
    try:
        registry = ConfigRegistry()
        config_snapshot = registry.freeze(conn)
        cfg = config_snapshot.data.get("strategy", {})
    except Exception as e:
        _logger.warning(f"[context] 配置加载失败，使用空配置: {e}")
        config_snapshot = None
        cfg = {}

    # Market
    store = MarketStore(conn)
    market_svc = MarketService(
        market_providers=[AStockSignalAdapter(), MXMarketAdapter(), MootdxMarketAdapter(), AkShareHKMarketAdapter(), AkShareMarketAdapter()],
        financial_providers=[TencentFinancialAdapter(), AkShareHKFinancialAdapter(), AkShareFinancialAdapter()],
        flow_providers=[BaiduFundFlowAdapter(), AkShareFlowAdapter()],
        sentiment_providers=[MXSentimentAdapter()],
        store=store,
    )

    # Strategy
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
    )
    thresholds = cfg.get("scoring", {}).get("thresholds", {})
    pos_cfg = cfg.get("risk", {}).get("position", {})
    decider = Decider(
        buy_threshold=thresholds.get("buy", 5.5),
        watch_threshold=thresholds.get("watch", 5.0),
        single_max_pct=pos_cfg.get("single_max", 0.20),
        total_max_pct=pos_cfg.get("total_max", 0.60),
        weekly_max=pos_cfg.get("weekly_max", 2),
    )
    strategy_svc = StrategyService(scorer, decider, event_store)

    # Risk / Execution
    risk_svc = RiskService(event_store)
    vault_path = _resolve_vault_path()
    trade_hooks = []
    if vault_path:
        trade_hooks.append(TradeLogger(event_store, conn, vault_path))
    exec_svc = ExecutionService(event_store, conn, on_trade=trade_hooks)

    # Reporting
    projector = ProjectionUpdater(event_store, conn)
    reporter = ReportGenerator(event_store, conn)
    obsidian = ObsidianProjector(event_store, conn, vault_path)

    return PipelineContext(
        conn=conn, event_store=event_store, run_journal=run_journal,
        config_snapshot=config_snapshot, market_svc=market_svc,
        strategy_svc=strategy_svc, risk_svc=risk_svc, exec_svc=exec_svc,
        projector=projector, reporter=reporter, obsidian=obsidian,
        vault_path=vault_path,
    )
