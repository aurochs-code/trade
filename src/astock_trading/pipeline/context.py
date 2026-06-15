"""
pipeline/context.py — Pipeline 运行上下文

所有 pipeline 共享的 service 初始化和配置加载。
一次初始化，整个 pipeline 复用。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from astock_trading.platform.events import EventStore
from astock_trading.platform.config import ConfigSnapshot
from astock_trading.platform.runs import RunJournal
from astock_trading.market.service import MarketService
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.service import RiskService
from astock_trading.execution.service import ExecutionService
from astock_trading.platform import service_factory
from astock_trading.reporting.projectors import ProjectionUpdater
from astock_trading.reporting.reports import ReportGenerator
from astock_trading.reporting.obsidian import ObsidianProjector

_logger = logging.getLogger(__name__)


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


def build_context() -> PipelineContext:
    """构建完整的 pipeline 上下文。"""
    services = service_factory.build_runtime_services()

    return PipelineContext(
        conn=services.conn,
        event_store=services.event_store,
        run_journal=services.run_journal,
        config_snapshot=services.config_snapshot,
        market_svc=services.market_svc,
        strategy_svc=services.strategy_svc,
        risk_svc=services.risk_svc,
        exec_svc=services.exec_svc,
        projector=services.projector,
        reporter=services.reporter,
        obsidian=services.obsidian,
        vault_path=services.vault_path,
    )
