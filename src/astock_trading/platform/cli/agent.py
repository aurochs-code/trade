"""Agent-facing CLI context."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import typer

from astock_trading.platform.cli.common import json_or_text

RUNTIME_FOLLOW_UP_COMMANDS = [
    "atrade diagnose flow --json",
    "atrade opportunity --json",
    "atrade review manual-followup --json",
    "atrade digest --json",
    "atrade paper auto-readiness --json",
    "atrade risk trial-guard --json",
]

RUNTIME_GUARDRAILS = [
    "agent-context 是只读入口，不提交模拟盘订单，也不记录人工成交。",
    "不要在用户明确批准前执行带 --apply-env --yes 的 profile 写入命令。",
    "旧买入意向不会跨日自动提交；下个买入窗口前需要重新形成当日买入意向。",
]
CATALOG_VERSION = 1


def register_agent_context(app: typer.Typer) -> None:
    @app.command("agent-context")
    def agent_context(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """输出给 Agent 使用的安全入口和约束。"""
        payload = {
            "project": "a-stock-trading",
            "safe_entrypoints": ["atrade", "atrade mcp", "bin/trade", "bin/trade mcp"],
            "forbidden_entrypoints": ["src/astock_trading/**/*.py"],
            "database": {
                "runtime_env": "ASTOCK_DATABASE_URL",
                "runtime_required": True,
                "migration_source": "archived SQLite path only; not kept in checkout",
            },
            "recommended_commands": {
                "commands": "atrade commands --json",
                "doctor": "atrade doctor --json",
                "health": "atrade health --json",
                "diagnose_health": "atrade diagnose health --json",
                "diagnose_strategy": "atrade diagnose strategy --json",
                "diagnose_flow": "atrade diagnose flow --json",
                "diagnose_schedule": "atrade diagnose schedule --json",
                "data_sources_diagnose": "atrade data-sources diagnose --json",
                "events": "atrade events query --json",
                "events_backfill_evidence": "atrade events backfill-evidence --json",
                "runs": "atrade runs list --json",
                "portfolio": "atrade status --json",
                "opportunity": "atrade opportunity --json",
                "opportunity_watch": "atrade opportunity-watch --json",
                "manual_followup": "atrade review manual-followup --json",
                "screener": "atrade screener candidates --json",
                "screener_explain": "atrade screener explain --json",
                "screener_iterate": "atrade screener iterate --json",
                "screener_refresh": "atrade screener refresh --json",
                "screener_run": "atrade screener run --query '...' --json",
                "stock_analyze": "atrade stock analyze CODE_OR_NAME --json",
                "risk_check": "atrade risk check CODE --json",
                "risk_portfolio": "atrade risk portfolio --json",
                "risk_position": "atrade risk position CODE SCORE PRICE --json",
                "risk_trial_guard": "atrade risk trial-guard --json",
                "strategy_profile_activation": "atrade strategy profile-activation --target trend_swing --json",
                "market_intel": "atrade market-intel brief --query '今天热点新闻和强势板块' --json",
                "market_news_search": "atrade market-intel search KEYWORD --json",
                "market_hot_stocks": "atrade market-intel hot-stocks --json",
                "market_northbound": "atrade market-intel northbound --json",
                "market_fund_flow": "atrade market-intel fund-flow CODE --json",
                "market_watchlist_sync": (
                    "atrade market-intel watchlist-sync --source candidate-pool "
                    "--preserve-holdings --dry-run --json"
                ),
                "record_buy": "atrade record-buy CODE SHARES PRICE --yes --json",
                "adjust_position_cost": "atrade adjust-position-cost CODE --cost-price COST_PRICE --yes --json",
                "record_sell": "atrade record-sell CODE SHARES PRICE --yes --json",
                "manual_trades": "atrade manual-trades list --json",
                "manual_trades_stale": "atrade manual-trades list --status stale --json",
                "manual_trades_expire_stale": "atrade manual-trades expire-stale --yes --json",
                "paper": "atrade paper status --json",
                "paper_auto_readiness": "atrade paper auto-readiness --json",
                "paper_trial_plan": "atrade paper trial-plan --json",
                "paper_trial_review": "atrade paper trial-review --json",
                "llm_context_morning": "atrade llm-context --mode morning --json",
                "llm_context_close": "atrade llm-context --mode close --json",
                "llm_context_weekly": "atrade llm-context --mode weekly --json",
                "db_status": "atrade db status --json",
                "db_tables": "atrade db tables --json",
                "db_check": "atrade db check --json",
                "db_backup": (
                    "atrade db backup --output ~/.local/state/a-stock-trading/backups/astock.sql "
                    "--docker-container astock-mysql --yes --json"
                ),
            },
            "operator_attention": _operator_attention(),
            "rules": [
                "不要直接执行 src/astock_trading 下的 Python 模块。",
                "优先使用 CLI 命令；MCP 只是 agent-client 薄适配层。",
                "自动化读取必须使用 --json 输出。",
                "需要写入 .env、记录成交或提交模拟盘订单时，必须先拿到用户明确批准。",
            ],
        }
        json_or_text(payload, as_json)

    @app.command("commands")
    def commands(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """输出给 Agent 使用的机器可读命令契约。"""
        json_or_text(_command_catalog(), as_json)


def _command_catalog() -> dict[str, Any]:
    return {
        "command": "commands",
        "status": "ok",
        "catalog_version": CATALOG_VERSION,
        "summary": "Agent 可调用命令契约；默认只读，写状态或写环境的命令必须显式标注。",
        "guardrails": {
            "stable_entrypoints": ["atrade", "bin/trade"],
            "forbidden_entrypoints": ["src/astock_trading/**/*.py"],
            "json_required_for_automation": True,
            "manual_confirmation_required_for_real_trade": True,
            "agent_must_not_auto_apply_profile": True,
        },
        "commands": _command_catalog_entries(),
    }


def _command_catalog_entries() -> list[dict[str, Any]]:
    return [
        _catalog_entry(
            id="agent_context",
            title="Agent 自检上下文",
            argv=["atrade", "agent-context", "--json"],
            category="agent",
            description="读取安全入口、当前运行态动作和后续只读命令。",
        ),
        _catalog_entry(
            id="commands",
            title="命令契约目录",
            argv=["atrade", "commands", "--json"],
            category="agent",
            description="读取 agent 可调用命令、参数和风险级别；只读。",
        ),
        _catalog_entry(
            id="doctor",
            title="运行环境自检",
            argv=["atrade", "doctor", "--json"],
            category="diagnostics",
            description="检查运行环境、数据库配置和基础可用性；不写状态。",
        ),
        _catalog_entry(
            id="health",
            title="运行健康检查",
            argv=["atrade", "health", "--json"],
            category="diagnostics",
            description="读取 DB、近期运行、失败记录和数据源探针摘要；不写状态。",
        ),
        _catalog_entry(
            id="diagnose_health",
            title="健康诊断",
            argv=["atrade", "diagnose", "health", "--json"],
            category="diagnostics",
            description="诊断数据源、候选池、失败运行和运行中任务；不写状态。",
        ),
        _catalog_entry(
            id="diagnose_flow",
            title="候选流诊断",
            argv=["atrade", "diagnose", "flow", "--json"],
            category="diagnostics",
            description="汇总候选池、买入意向、机会卡、模拟预检和调度；不运行 pipeline。",
            options={
                "--include-account": {
                    "type": "flag",
                    "default": False,
                    "effect": "额外读取 MX 模拟盘账户；默认只查本地证据",
                },
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="diagnose_schedule",
            title="Hermes 调度诊断",
            argv=["atrade", "diagnose", "schedule", "--json"],
            category="diagnostics",
            description="检查 trading profile 调度、运行 .env profile 和关键任务下次运行时间。",
            options={
                "--jobs-path": {"placeholder": "PATH", "required": False},
                "--env-file": {"placeholder": "PATH", "required": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="diagnose_strategy",
            title="策略与候选流诊断",
            argv=["atrade", "diagnose", "strategy", "--json"],
            category="diagnostics",
            description="诊断当前候选池、评分、入场信号、买入意向和执行 profile；不运行 pipeline。",
        ),
        _catalog_entry(
            id="data_sources_diagnose",
            title="数据源诊断",
            argv=["atrade", "data-sources", "diagnose", "--json"],
            category="diagnostics",
            description="检查核心数据源、候选池新鲜度、provider fallback 和未补齐失败；不写状态。",
        ),
        _catalog_entry(
            id="digest",
            title="运行摘要",
            argv=["atrade", "digest", "--json"],
            category="summary",
            description="读取当前待确认、过期待复核、持仓、失败运行和重点信号；不写状态。",
        ),
        _catalog_entry(
            id="llm_context_morning",
            title="LLM 盘前摘要上下文",
            argv=["atrade", "llm-context", "--mode", "morning", "--json"],
            category="summary",
            description="生成 Hermes/LLM 盘前摘要上下文；只读，不执行交易。",
            options=_llm_context_options(default_mode="morning"),
        ),
        _catalog_entry(
            id="llm_context_close",
            title="LLM 收盘复盘上下文",
            argv=["atrade", "llm-context", "--mode", "close", "--json"],
            category="summary",
            description="生成 Hermes/LLM 收盘复盘上下文，包含候选漏斗和模拟承接链路；只读。",
            options=_llm_context_options(default_mode="close"),
        ),
        _catalog_entry(
            id="llm_context_weekly",
            title="LLM 周复盘上下文",
            argv=["atrade", "llm-context", "--mode", "weekly", "--json"],
            category="summary",
            description="生成 Hermes/LLM 周复盘补充上下文；只读，不执行交易。",
            options=_llm_context_options(default_mode="weekly"),
        ),
        _catalog_entry(
            id="screener_candidates",
            title="候选池列表",
            argv=["atrade", "screener", "candidates", "--json"],
            category="screener",
            description="读取当前候选池投影，区分核心、观察和强势观察；不刷新、不写状态。",
            options={
                "--tier": {"allowed_values": ["all", "core", "watch"], "default": "all"},
                "--limit": {"type": "int", "default": 100, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="screener_refresh",
            title="刷新候选池",
            argv=["atrade", "screener", "refresh", "--json"],
            category="screener",
            description="执行调度型候选池刷新，写入评分、候选池事件和候选池投影；不下单。",
            writes_state=True,
            risk_level="state_write",
            state_events=[
                "candidate.added",
                "candidate.updated",
                "candidate.promoted",
                "candidate.rejected",
                "pool.demoted",
            ],
            options={
                "--query": {"placeholder": "QUERY", "required": False, "default": ""},
                "--limit": {
                    "type": "int",
                    "required": False,
                    "default": "strategy.screening.refresh_scan_limit",
                },
                "--watch-threshold": {
                    "type": "float",
                    "required": False,
                    "default": "config.strategy.screening.watch_threshold",
                },
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="screener_explain",
            title="候选漏斗解释",
            argv=["atrade", "screener", "explain", "--json"],
            category="screener",
            description="解释近期评分、决策、否决原因和临界候选；只读。",
            options={
                "--since": {"placeholder": "ISO", "required": False},
                "--days": {"type": "int", "default": 7, "min": 1},
                "--run-id": {"placeholder": "RUN_ID", "required": False},
                "--limit": {"type": "int", "default": 1000, "min": 1},
                "--near-miss-margin": {"type": "float", "default": 1.0},
                "--follow-up-limit": {"type": "int", "default": 10, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="screener_iterate",
            title="候选策略迭代建议",
            argv=["atrade", "screener", "iterate", "--json"],
            category="screener",
            description="生成候选漏斗迭代建议；默认记录 strategy.iteration.proposed 证据事件。",
            writes_state=True,
            risk_level="state_write",
            state_events=["strategy.iteration.proposed"],
            options={
                "--record/--no-record": {
                    "type": "flag",
                    "default": True,
                    "writes_state": True,
                    "effect": "默认写入迭代建议证据；--no-record 只预览",
                },
                "--since": {"placeholder": "ISO", "required": False},
                "--days": {"type": "int", "default": 7, "min": 1},
                "--run-id": {"placeholder": "RUN_ID", "required": False},
                "--limit": {"type": "int", "default": 1000, "min": 1},
                "--near-miss-margin": {"type": "float", "default": 1.0},
                "--follow-up-limit": {"type": "int", "default": 10, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="screener_run",
            title="执行候选筛选",
            argv=["atrade", "screener", "run", "--query", "'...'", "--json"],
            category="screener",
            description="执行筛选和评分，并把达标结果加入观察池；不下单。",
            writes_state=True,
            risk_level="state_write",
            state_events=["score.calculated", "decision.suggested", "candidate.*"],
            options={
                "--query": {"placeholder": "QUERY", "required": False, "default": ""},
                "--limit": {"type": "int", "required": False},
                "--watch-threshold": {"type": "float", "required": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="stock_analyze",
            title="个股分析",
            argv=["atrade", "stock", "analyze", "CODE_OR_NAME", "--json"],
            category="stock",
            description="分析单只股票的评分、入场门控、候选池和历史记录；不执行交易。",
            arguments={
                "CODE_OR_NAME": {"description": "股票代码或名称"},
            },
            options={
                "--history-days": {"type": "int", "default": 7, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="explain",
            title="解释买入意向",
            argv=["atrade", "explain", "CODE", "--json"],
            category="stock",
            description="解释单只股票最近评分和决策证据；不执行交易。",
            arguments={
                "CODE": {"description": "股票代码"},
            },
            options={
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="risk_trial_guard",
            title="试运行风控护栏",
            argv=["atrade", "risk", "trial-guard", "--json"],
            category="risk",
            description="审计首轮试运行仓位上限，并展示候选池入场信号和 profile 阻断；只读，不下单。",
            options={
                "--capital": {"type": "float", "required": False},
                "--amount": {"type": "float", "required": False},
                "--trial-ratio": {"type": "float", "required": False},
                "--single-max-pct": {"type": "float", "required": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="risk_check",
            title="持仓离场风控",
            argv=["atrade", "risk", "check", "CODE", "--json"],
            category="risk",
            description="检查单只本地持仓的止损、止盈和风控信号；只读。",
            arguments={"CODE": {"description": "股票代码"}},
        ),
        _catalog_entry(
            id="risk_portfolio",
            title="组合风控检查",
            argv=["atrade", "risk", "portfolio", "--json"],
            category="risk",
            description="检查组合级风控限制；只读。",
            options={
                "--daily-pnl-pct": {"type": "float", "default": 0.0},
                "--consecutive-loss-days": {"type": "int", "default": 0},
                "--max-sector-exposure-pct": {"type": "float", "default": 0.0},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="risk_position",
            title="仓位建议计算",
            argv=["atrade", "risk", "position", "CODE", "SCORE", "PRICE", "--json"],
            category="risk",
            description="按评分、价格和风控参数计算建议仓位；只读，不写交易事实。",
            arguments={
                "CODE": {"description": "股票代码"},
                "SCORE": {"type": "float", "description": "评分"},
                "PRICE": {"type": "float", "description": "当前价格"},
            },
            options={
                "--capital": {"type": "float", "required": False},
                "--current-exposure-pct": {"type": "float", "default": 0.0},
                "--market-multiplier": {"type": "float", "default": 1.0},
                "--single-max-pct": {"type": "float", "required": False},
                "--total-max-pct": {"type": "float", "required": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="opportunity",
            title="今日机会卡",
            argv=["atrade", "opportunity", "--json"],
            category="opportunity",
            description="生成当前机会、阻断项、审批门和下一步；不执行交易。",
        ),
        _catalog_entry(
            id="opportunity_watch",
            title="机会变化监控",
            argv=["atrade", "opportunity-watch", "--json"],
            category="opportunity",
            description="对比机会监控状态；候选或当前动作变化时触发提醒。",
            writes_state=True,
            risk_level="state_write",
            state_events=[],
            options={
                "--state-file": {"placeholder": "PATH", "required": False},
                "--no-write": {
                    "type": "flag",
                    "default": False,
                    "effect": "只比较，不更新机会监控状态文件",
                },
                "--reset-state": {"type": "flag", "default": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="manual_followup",
            title="人工复核自动汇总",
            argv=["atrade", "review", "manual-followup", "--json"],
            category="review",
            description="聚合机会卡、影子试运行复盘和模拟承接预检，生成只读人工复核清单；不写状态、不下单。",
            options={
                "--skip-account": {"type": "flag", "default": False, "effect": "不请求 MX 账户"},
                "--limit": {"type": "int", "default": 100, "min": 1, "max": 1000},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="notify_opportunity_watch",
            title="推送机会变化提醒",
            argv=["atrade", "notify", "opportunity-watch", "--json"],
            category="notification",
            description="机会变化时推送 Discord；无变化返回 silent。",
            writes_state=True,
            risk_level="notification_write",
            options={
                "--dry-run": {"type": "flag", "default": False, "effect": "只生成卡片，不发送且不更新状态"},
                "--state-file": {"placeholder": "PATH", "required": False},
                "--reset-state": {"type": "flag", "default": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="notify_manual_followup",
            title="推送人工复核自动汇总",
            argv=["atrade", "notify", "manual-followup", "--json"],
            category="notification",
            description="生成人工复核自动汇总并推送 Discord；不提交委托，不记录成交。",
            risk_level="notification_write",
            options={
                "--dry-run": {"type": "flag", "default": False, "effect": "只生成卡片，不发送 Discord"},
                "--skip-account": {"type": "flag", "default": False, "effect": "不请求 MX 账户"},
                "--limit": {"type": "int", "default": 100, "min": 1, "max": 1000},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="paper_auto_readiness",
            title="模拟承接预检",
            argv=["atrade", "paper", "auto-readiness", "--json"],
            category="paper",
            description="检查 auto_trade 是否具备提交 MX 模拟盘委托条件；不下单。",
            options={
                "--skip-account": {"type": "flag", "default": False, "effect": "不请求 MX 账户"},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="paper_status",
            title="模拟盘状态",
            argv=["atrade", "paper", "status", "--json"],
            category="paper",
            description="读取 MX 模拟盘持仓和资金；只读，不提交委托。",
        ),
        _catalog_entry(
            id="paper_trial_plan",
            title="影子试运行计划",
            argv=["atrade", "paper", "trial-plan", "--json"],
            category="paper",
            description="把 watch/radar/core 候选转成只读影子试运行清单；不下单。",
            options={
                "--limit": {"type": "int", "default": 10, "min": 1, "max": 20},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="paper_trial_plan_record",
            title="记录影子试运行",
            argv=["atrade", "paper", "trial-plan", "--record", "--json"],
            category="paper",
            description="写入 paper.trial.recorded 影子事件；不提交模拟盘订单。",
            writes_state=True,
            writes_order=False,
            risk_level="state_write",
            state_events=["paper.trial.recorded"],
            options={
                "--record": {"type": "flag", "writes_state": True},
                "--limit": {"type": "int", "default": 10, "min": 1, "max": 20},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="paper_trial_review",
            title="影子试运行复盘",
            argv=["atrade", "paper", "trial-review", "--json"],
            category="paper",
            description="复盘影子候选表现；默认只读，不晋级、不下单。",
            options={
                "--trial-date": {"placeholder": "YYYY-MM-DD", "required": False},
                "--as-of": {"placeholder": "YYYY-MM-DD", "required": False},
                "--min-age-days": {"type": "int", "default": 1, "min": 0},
                "--limit": {"type": "int", "default": 100, "min": 1, "max": 1000},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="paper_trial_review_record",
            title="记录影子试运行复盘",
            argv=["atrade", "paper", "trial-review", "--record", "--json"],
            category="paper",
            description="写入 paper.trial.reviewed 影子复盘事件；不晋级、不提交模拟盘订单。",
            writes_state=True,
            writes_order=False,
            risk_level="state_write",
            state_events=["paper.trial.reviewed"],
            options={
                "--record": {"type": "flag", "writes_state": True},
                "--trial-date": {"placeholder": "YYYY-MM-DD", "required": False},
                "--as-of": {"placeholder": "YYYY-MM-DD", "required": False},
                "--min-age-days": {"type": "int", "default": 1, "min": 0},
                "--limit": {"type": "int", "default": 100, "min": 1, "max": 1000},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="run_pipeline_auto_trade",
            title="运行模拟承接 pipeline",
            argv=["atrade", "run-pipeline", "auto_trade", "--json"],
            category="pipeline",
            description="执行 auto_trade pipeline；满足条件时可能提交 MX 模拟盘委托，不能由 agent 自行触发。",
            writes_state=True,
            writes_order=True,
            requires_user_approval=True,
            risk_level="paper_order_execution",
            state_events=[
                "auto_trade.diagnostic",
                "auto_trade.summary",
                "paper.order.submitted",
            ],
            options={
                "--ignore-data-source-health": {
                    "type": "flag",
                    "default": False,
                    "requires_user_approval": True,
                },
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="strategy_profile_activation_review",
            title="复核 profile 激活计划",
            argv=["atrade", "strategy", "profile-activation", "--target", "trend_swing", "--json"],
            category="strategy",
            description="生成或复核运行 profile 激活计划；默认不写环境。",
            options=_profile_activation_options(include_apply=False),
        ),
        _catalog_entry(
            id="strategy_profile_activation_apply",
            title="写入运行 profile",
            argv=[
                "atrade",
                "strategy",
                "profile-activation",
                "--target",
                "trend_swing",
                "--apply-env",
                "--yes",
                "--json",
            ],
            category="strategy",
            description="人工批准后写入 ASTOCK_CONFIG_PROFILE；不改 Hermes 调度，不提交订单。",
            writes_state=True,
            writes_environment=True,
            requires_user_approval=True,
            risk_level="environment_write",
            state_events=["strategy.profile_activation.applied"],
            options=_profile_activation_options(include_apply=True),
        ),
        _catalog_entry(
            id="events_query",
            title="事件查询",
            argv=["atrade", "events", "query", "--json"],
            category="events",
            description="查询事件日志；只读。",
            options={
                "--type": {"placeholder": "EVENT_TYPE", "required": False},
                "--stream": {"placeholder": "STREAM", "required": False},
                "--since": {"placeholder": "ISO", "required": False},
                "--limit": {"type": "int", "default": 50, "min": 1},
                "--order": {"allowed_values": ["asc", "desc"], "default": "desc"},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="events_backfill_evidence_preview",
            title="证据回填预览",
            argv=["atrade", "events", "backfill-evidence", "--json"],
            category="events",
            description="预览哪些历史旧事件可以追加证据回填；不改写原始事件，不写状态。",
            options=_events_backfill_evidence_options(include_apply=False),
        ),
        _catalog_entry(
            id="events_backfill_evidence_apply",
            title="写入证据回填事件",
            argv=["atrade", "events", "backfill-evidence", "--apply", "--json"],
            category="events",
            description="人工确认后为历史旧事件追加 evidence.backfilled 事件；不改写原始事件，不提交订单。",
            writes_state=True,
            writes_order=False,
            requires_user_approval=True,
            risk_level="state_write",
            state_events=["evidence.backfilled"],
            options=_events_backfill_evidence_options(include_apply=True),
        ),
        _catalog_entry(
            id="runs_list",
            title="运行记录列表",
            argv=["atrade", "runs", "list", "--json"],
            category="runs",
            description="读取 run_log 运行记录；只读。",
            options={
                "--run-type": {"placeholder": "RUN_TYPE", "required": False},
                "--status": {"placeholder": "STATUS", "required": False},
                "--limit": {"type": "int", "default": 20, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="portfolio_status",
            title="本地持仓状态",
            argv=["atrade", "status", "--json"],
            category="portfolio",
            description="读取本地持仓、资金和投影；只读。",
        ),
        _catalog_entry(
            id="market_intel_brief",
            title="市场情报简报",
            argv=["atrade", "market-intel", "brief", "--query", "'今天热点新闻和强势板块'", "--json"],
            category="market_intel",
            description="采集并汇总热点新闻、强势板块和跨平台热股；可能写入行情缓存，不下单。",
            writes_state=True,
            risk_level="market_data_write",
            state_events=["market_observation.*"],
            options={
                "--query": {"placeholder": "QUERY", "required": False},
                "--limit": {"type": "int", "default": 5, "min": 1, "max": 20},
                "--include-global/--no-global": {"type": "flag", "default": True},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="market_news_search",
            title="财经新闻检索",
            argv=["atrade", "market-intel", "search", "KEYWORD", "--json"],
            category="market_intel",
            description="按关键词检索财经新闻；可能写入行情缓存，不下单。",
            writes_state=True,
            risk_level="market_data_write",
            state_events=["market_observation.*"],
            arguments={"KEYWORD": {"description": "新闻关键词或问题"}},
            options={
                "--limit": {"type": "int", "default": 10, "min": 1, "max": 40},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="market_hot_stocks",
            title="强势股列表",
            argv=["atrade", "market-intel", "hot-stocks", "--json"],
            category="market_intel",
            description="采集强势股和题材归因；可能写入行情缓存，不下单。",
            writes_state=True,
            risk_level="market_data_write",
            state_events=["market_observation.*"],
            options={
                "--trade-date": {"placeholder": "YYYY-MM-DD", "required": False},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="market_northbound",
            title="北向资金",
            argv=["atrade", "market-intel", "northbound", "--json"],
            category="market_intel",
            description="采集北向资金分钟流向；可能写入行情缓存，不下单。",
            writes_state=True,
            risk_level="market_data_write",
            state_events=["market_observation.*"],
        ),
        _catalog_entry(
            id="market_fund_flow",
            title="个股资金流",
            argv=["atrade", "market-intel", "fund-flow", "CODE", "--json"],
            category="market_intel",
            description="读取个股资金流和实时尾部数据；不提交交易。",
            arguments={"CODE": {"description": "股票代码"}},
            options={
                "--days": {"type": "int", "default": 5, "min": 1},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="market_watchlist_sync_preview",
            title="MX 自选同步预演",
            argv=[
                "atrade",
                "market-intel",
                "watchlist-sync",
                "--source",
                "candidate-pool",
                "--preserve-holdings",
                "--dry-run",
                "--json",
            ],
            category="market_intel",
            description=(
                "按最新核心池、观察池和强势观察生成 MX 自选同步计划；"
                "保留 MX 模拟盘持仓和本地持仓记录，不修改外部状态。"
            ),
            options={
                "--source": {"allowed_values": ["candidate-pool"], "default": "candidate-pool"},
                "--include-radar/--no-include-radar": {"type": "flag", "default": True},
                "--preserve-holdings/--no-preserve-holdings": {"type": "flag", "default": True},
                "--dry-run": {"type": "flag", "default": True, "effect": "只生成同步计划"},
                "--operation-delay": {"type": "float", "default": 1.5, "effect": "执行时每次 MX 写入后的等待秒数"},
                "--max-retries": {"type": "int", "default": 3, "effect": "遇到 MX 限频时每个操作最多重试次数"},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="market_watchlist_sync_apply",
            title="MX 自选同步执行",
            argv=[
                "atrade",
                "market-intel",
                "watchlist-sync",
                "--source",
                "candidate-pool",
                "--preserve-holdings",
                "--yes",
                "--json",
            ],
            category="market_intel",
            description=(
                "人工确认后清理 MX 非持仓旧自选，并加入最新核心池、观察池和强势观察；"
                "不会提交 MX 模拟盘订单。"
            ),
            writes_state=True,
            writes_order=False,
            requires_user_approval=True,
            risk_level="external_state_write",
            state_events=["mx.watchlist.updated"],
            options={
                "--source": {"allowed_values": ["candidate-pool"], "default": "candidate-pool"},
                "--include-radar/--no-include-radar": {"type": "flag", "default": True},
                "--preserve-holdings/--no-preserve-holdings": {"type": "flag", "default": True},
                "--yes": {"required": True, "effect": "确认写入 MX 自选"},
                "--operation-delay": {"type": "float", "default": 1.5, "effect": "每次 MX 写入后的等待秒数"},
                "--max-retries": {"type": "int", "default": 3, "effect": "遇到 MX 限频时每个操作最多重试次数"},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="record_buy",
            title="记录人工买入",
            argv=["atrade", "record-buy", "CODE", "SHARES", "PRICE", "--yes", "--json"],
            category="manual_trade",
            description="人工确认真实成交后记录本地买入事实；不是自动下单。",
            arguments={
                "CODE": {"description": "股票代码"},
                "SHARES": {"type": "int", "description": "买入股数"},
                "PRICE": {"type": "float", "description": "成交价"},
            },
            writes_state=True,
            requires_user_approval=True,
            risk_level="manual_trade_record",
            state_events=["order.*", "position.*", "trade.hypothesis.recorded"],
            options={
                "--cost-price": {"type": "float", "effect": "按券商显示的每股成本价写入总成本"},
                "--cost-basis": {"type": "float", "effect": "按券商显示的总成本写入"},
                "--yes": {"required": True},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="adjust_position_cost",
            title="校准持仓成本",
            argv=["atrade", "adjust-position-cost", "CODE", "--cost-price", "COST_PRICE", "--yes", "--json"],
            category="manual_trade",
            description="按券商 App 的成本价校准本地持仓总成本；只修正本地记录，不下单。",
            arguments={
                "CODE": {"description": "股票代码"},
            },
            writes_state=True,
            requires_user_approval=True,
            risk_level="manual_trade_record",
            state_events=["position.cost_basis_adjusted"],
            options={
                "--cost-price": {"type": "float", "effect": "券商显示的每股成本价"},
                "--cost-basis": {"type": "float", "effect": "券商显示的总成本"},
                "--yes": {"required": True},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="record_sell",
            title="记录人工卖出",
            argv=["atrade", "record-sell", "CODE", "SHARES", "PRICE", "--yes", "--json"],
            category="manual_trade",
            description="人工确认真实成交后记录本地卖出事实；不是自动下单。",
            arguments={
                "CODE": {"description": "股票代码"},
                "SHARES": {"type": "int", "description": "卖出股数，支持部分卖出"},
                "PRICE": {"type": "float", "description": "成交价"},
            },
            writes_state=True,
            requires_user_approval=True,
            risk_level="manual_trade_record",
            state_events=["order.*", "position.*", "trade.outcome.recorded"],
            options={"--yes": {"required": True}, "--json": {"required_for_automation": True}},
        ),
        _catalog_entry(
            id="db_status",
            title="数据库状态",
            argv=["atrade", "db", "status", "--json"],
            category="database",
            description="读取 schema version 和关键表计数；只读。",
        ),
        _catalog_entry(
            id="db_tables",
            title="数据库表状态",
            argv=["atrade", "db", "tables", "--json"],
            category="database",
            description="读取 MySQL 表大小和行数估算；只读。",
        ),
        _catalog_entry(
            id="db_check",
            title="数据库一致性检查",
            argv=["atrade", "db", "check", "--json"],
            category="database",
            description="执行 MySQL CHECK TABLE；不修改交易状态。",
        ),
        _catalog_entry(
            id="db_backup",
            title="数据库备份",
            argv=[
                "atrade",
                "db",
                "backup",
                "--output",
                "~/.local/state/a-stock-trading/backups/astock.sql",
                "--docker-container",
                "astock-mysql",
                "--yes",
                "--json",
            ],
            category="database",
            description="人工确认后导出 MySQL 备份文件；不修改交易事实，不提交订单。",
            requires_user_approval=True,
            risk_level="filesystem_write",
            options={
                "--output": {"placeholder": "PATH", "required": True, "writes_filesystem": True},
                "--docker-container": {"placeholder": "NAME", "required": False},
                "--yes": {"type": "flag", "required": True, "confirms_filesystem_write": True},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="manual_trades_list",
            title="待人工确认清单",
            argv=["atrade", "manual-trades", "list", "--json"],
            category="manual_trade",
            description="读取仍可操作的人工确认单；不记录成交、不下单。",
        ),
        _catalog_entry(
            id="manual_trades_stale",
            title="过期待复核清单",
            argv=["atrade", "manual-trades", "list", "--status", "stale", "--json"],
            category="manual_trade",
            description="读取已经过期、跨日或非交易日不可承接的人工确认单；不写状态。",
            options={
                "--status": {"required": True, "allowed_values": ["stale"]},
                "--json": {"required_for_automation": True},
            },
        ),
        _catalog_entry(
            id="manual_trades_expire_stale",
            title="结案过期待复核",
            argv=["atrade", "manual-trades", "expire-stale", "--yes", "--json"],
            category="manual_trade",
            description="人工确认后追加 manual_trade.expired 审计事件；不记录成交、不提交订单。",
            writes_state=True,
            writes_order=False,
            requires_user_approval=True,
            risk_level="state_write",
            state_events=["manual_trade.expired"],
            options={"--yes": {"required": True}, "--json": {"required_for_automation": True}},
        ),
    ]


def _llm_context_options(*, default_mode: str) -> dict[str, Any]:
    return {
        "--mode": {
            "required": True,
            "default": default_mode,
            "allowed_values": ["morning", "close", "weekly"],
        },
        "--json": {"required_for_automation": True},
    }


def _profile_activation_options(*, include_apply: bool) -> dict[str, Any]:
    options: dict[str, Any] = {
        "--target": {
            "required": True,
            "default": "trend_swing",
            "allowed_values": ["trend_swing", "short_continuation", "defensive_watch"],
        },
        "--record": {"type": "flag", "default": False, "writes_state": True},
        "--env-file": {"placeholder": "PATH", "required": False},
        "--json": {"required_for_automation": True},
    }
    if include_apply:
        options["--apply-env"] = {"type": "flag", "required": True, "writes_environment": True}
        options["--yes"] = {"type": "flag", "required": True, "confirms_environment_write": True}
    return options


def _events_backfill_evidence_options(*, include_apply: bool) -> dict[str, Any]:
    options: dict[str, Any] = {
        "--code": {"placeholder": "CODE", "required": False},
        "--limit": {"type": "int", "default": 5000, "min": 1},
        "--json": {"required_for_automation": True},
    }
    if include_apply:
        options["--apply"] = {
            "type": "flag",
            "required": True,
            "writes_state": True,
            "effect": "追加 evidence.backfilled 审计事件，不改写原始事件",
        }
    return options


def _catalog_entry(
    *,
    id: str,
    title: str,
    argv: list[str],
    category: str,
    description: str,
    arguments: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    writes_state: bool = False,
    writes_environment: bool = False,
    writes_order: bool = False,
    requires_user_approval: bool = False,
    risk_level: str = "read_only",
    state_events: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": id,
        "title": title,
        "category": category,
        "description": description,
        "argv": argv,
        "command": " ".join(argv),
        "arguments": arguments or {},
        "options": options or {"--json": {"required_for_automation": True}},
        "risk_level": risk_level,
        "writes_state": writes_state,
        "writes_environment": writes_environment,
        "writes_order": writes_order,
        "requires_user_approval": requires_user_approval,
        "state_events": state_events or [],
        "stable_entrypoint": "atrade",
    }


def _command_contract_by_command() -> dict[str, dict[str, Any]]:
    return {
        entry["command"]: _compact_command_contract(entry)
        for entry in _command_catalog_entries()
    }


def _compact_command_contract(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": entry["id"],
        "risk_level": entry["risk_level"],
        "writes_state": entry["writes_state"],
        "writes_environment": entry["writes_environment"],
        "writes_order": entry["writes_order"],
        "requires_user_approval": entry["requires_user_approval"],
        "state_events": entry["state_events"],
    }


def _action_with_command_contract(action: dict[str, Any]) -> dict[str, Any]:
    contract = _command_contract_for_command(str(action.get("command") or ""))
    if not contract:
        return action
    enriched = dict(action)
    for key in (
        "writes_state",
        "writes_environment",
        "writes_order",
        "requires_user_approval",
        "risk_level",
    ):
        enriched.setdefault(key, contract[key])
    enriched["command_contract_id"] = contract["id"]
    enriched["command_contract"] = contract
    return enriched


def _command_contract_for_command(command: str) -> dict[str, Any] | None:
    contracts = _command_contract_by_command()
    normalized = " ".join(str(command or "").split())
    if normalized in contracts:
        return contracts[normalized]

    dynamic_templates = (
        ("atrade stock analyze ", " --json", "atrade stock analyze CODE_OR_NAME --json"),
        ("atrade explain ", " --json", "atrade explain CODE --json"),
    )
    for prefix, suffix, template in dynamic_templates:
        if normalized.startswith(prefix) and normalized.endswith(suffix):
            value = normalized.removeprefix(prefix).removesuffix(suffix).strip()
            if value:
                return contracts.get(template)
    if (
        normalized.startswith("atrade paper trial-review ")
        and " --record" in f" {normalized} "
        and normalized.endswith(" --json")
    ):
        return contracts.get("atrade paper trial-review --record --json")
    if normalized.startswith("atrade events backfill-evidence ") and normalized.endswith(" --json"):
        if " --apply " in f" {normalized} ":
            return contracts.get("atrade events backfill-evidence --apply --json")
        return contracts.get("atrade events backfill-evidence --json")
    return None


def _approval_gate_with_command_contracts(approval_gate: dict[str, Any]) -> dict[str, Any]:
    if not approval_gate:
        return {}
    contracts = _command_contract_by_command()
    enriched = dict(approval_gate)
    for prefix in ("review", "apply", "verify"):
        command = str(enriched.get(f"{prefix}_command") or "")
        contract = contracts.get(command)
        if contract:
            enriched[f"{prefix}_command_contract_id"] = contract["id"]
            enriched[f"{prefix}_command_contract"] = contract
    return enriched


def _operator_attention() -> dict[str, Any]:
    """读取当前运行态给 agent 的下一步动作；失败时不影响静态入口清单。"""
    try:
        from astock_trading.platform.db import connect, init_db
        from astock_trading.platform.hermes_commands import build_digest, build_opportunity_card

        init_db()
        conn = connect()
        try:
            digest = build_digest(conn)
            opportunity = build_opportunity_card(conn)
            after_approval_preview = _after_approval_preview_for_attention(conn, opportunity)
            runtime_contract = _runtime_contract_for_attention(opportunity)
            if not runtime_contract.get("status"):
                runtime_contract = _runtime_contract_from_schedule(conn)
        finally:
            conn.close()
    except Exception as exc:
        return _runtime_unavailable_attention(exc)

    attention = digest.get("attention", {}) or {}
    approval_gate = _approval_gate_with_command_contracts(opportunity.get("approval_gate", {}) or {})
    next_window_plan = opportunity.get("next_window_plan", {}) or {}
    next_action = opportunity.get("next_action", {}) or {}
    evidence_actions = [
        _action_with_command_contract(action)
        for action in opportunity.get("evidence_actions", []) or []
        if isinstance(action, dict)
    ]
    status = str(opportunity.get("status") or digest.get("status") or "unknown")
    return {
        "status": status,
        "summary": opportunity.get("summary") or digest.get("summary") or "",
        "current_action": _current_action(
            status=status,
            next_action=next_action,
            attention=attention,
            approval_gate=approval_gate,
            summary=opportunity.get("summary") or digest.get("summary") or "",
        ),
        "evidence_actions": evidence_actions,
        "approval_gate": approval_gate,
        "after_approval_preview": after_approval_preview,
        "next_window_plan": next_window_plan,
        "runtime_contract": runtime_contract,
        "follow_up_commands": RUNTIME_FOLLOW_UP_COMMANDS,
        "source_commands": [
            "atrade agent-context --json",
            "atrade commands --json",
            "atrade digest --json",
            "atrade opportunity --json",
            "atrade review manual-followup --json",
        ],
        "guardrails": RUNTIME_GUARDRAILS,
    }


def _runtime_contract_for_attention(opportunity: dict[str, Any]) -> dict[str, Any]:
    """从 opportunity 的调度诊断里提取脚本合约结论，供 agent-context 直接判断卡点。"""
    diagnostics = opportunity.get("diagnostics", {}) or {}
    schedule = diagnostics.get("schedule", {}) or {}
    contract = schedule.get("runtime_contract", {}) or {}
    return _compact_runtime_contract_for_attention(contract)


def _runtime_contract_from_schedule(conn: Any) -> dict[str, Any]:
    try:
        from astock_trading.platform.agent_diagnostics import diagnose_schedule

        schedule = diagnose_schedule(conn)
    except Exception as exc:
        return {
            "status": "unavailable",
            "summary": f"调度脚本运行合约读取失败：{exc}",
            "scope": "next_window_simulation_scripts",
            "script_checks": [],
            "blocking_issues": [{"script": "", "reason": "schedule_diagnosis_unavailable"}],
            "guardrails": {
                "read_only": True,
                "modifies_scripts": False,
                "runs_jobs": False,
            },
        }
    return _compact_runtime_contract_for_attention(schedule.get("runtime_contract", {}) or {})


def _compact_runtime_contract_for_attention(contract: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(contract, dict) or not contract:
        return {}
    return {
        "status": contract.get("status", ""),
        "summary": contract.get("summary", ""),
        "scope": contract.get("scope", ""),
        "script_dir_exists": bool(contract.get("script_dir_exists", False)),
        "script_checks": [
            {
                "script": item.get("script", ""),
                "profile_env_file_loading_possible": bool(
                    item.get("profile_env_file_loading_possible", False)
                ),
                "issues": item.get("issues", []) or [],
            }
            for item in (contract.get("script_checks", []) or [])
            if isinstance(item, dict)
        ],
        "blocking_issues": contract.get("blocking_issues", []) or [],
        "guardrails": contract.get("guardrails", {}) or {},
    }


def _after_approval_preview_for_attention(conn: Any, opportunity: dict[str, Any]) -> dict[str, Any]:
    """复用候选流诊断口径生成审批后只读预演，不查外部账户也不写状态。"""
    approval_gate = opportunity.get("approval_gate", {}) or {}
    if approval_gate.get("required") is not True:
        return {"available": False}
    try:
        from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
        from astock_trading.platform.agent_diagnostics import diagnose_flow
        from astock_trading.platform.config import ConfigRegistry
        from astock_trading.platform.events import EventStore
        from astock_trading.platform.runs import RunJournal

        data, _ = ConfigRegistry().load_and_validate()
        ctx = SimpleNamespace(
            conn=conn,
            cfg=data.get("strategy", {}) or {},
            event_store=EventStore(conn),
            run_journal=RunJournal(conn),
        )
        auto_readiness = build_auto_trade_readiness(ctx, include_account=False)
        flow = diagnose_flow(conn, opportunity=opportunity, auto_readiness=auto_readiness)
        return flow.get("after_approval_preview", {}) or {"available": False}
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


def _current_action(
    *,
    status: str,
    next_action: dict[str, Any],
    attention: dict[str, Any],
    approval_gate: dict[str, Any],
    summary: str,
) -> dict[str, Any]:
    command = str(next_action.get("command") or attention.get("command") or "atrade diagnose flow --json")
    safe_to_auto_apply = _safe_to_auto_apply(next_action, attention)
    contract = _command_contract_for_command(command)
    return _action_with_command_contract({
        "type": str(next_action.get("type") or attention.get("status") or "refresh_runtime_diagnostics"),
        "label": str(next_action.get("label") or attention.get("label") or "复核当前交易系统状态"),
        "command": command,
        "reason": str(next_action.get("reason") or attention.get("summary") or summary),
        "safe_to_auto_apply": safe_to_auto_apply,
        "writes_state": contract["writes_state"] if contract else _command_writes_state(command),
        "requires_user_approval": (
            contract["requires_user_approval"]
            if contract
            else _requires_user_approval(
                status=status,
                approval_gate=approval_gate,
                safe_to_auto_apply=safe_to_auto_apply,
            )
        ),
    })


def _safe_to_auto_apply(next_action: dict[str, Any], attention: dict[str, Any]) -> bool:
    if "safe_to_auto_apply" in next_action:
        return bool(next_action.get("safe_to_auto_apply"))
    if "safe_to_auto_apply" in attention:
        return bool(attention.get("safe_to_auto_apply"))
    return False


def _requires_user_approval(
    *,
    status: str,
    approval_gate: dict[str, Any],
    safe_to_auto_apply: bool,
) -> bool:
    if approval_gate.get("required") is True:
        return True
    if status in {"profile_review_required", "manual_confirmation_required"}:
        return True
    return not safe_to_auto_apply


def _command_writes_state(command: str) -> bool:
    write_markers = (
        "--apply-env",
        "--yes",
        "--record",
        "record-buy",
        "adjust-position-cost",
        "record-sell",
        "expire-stale",
        "trial-plan --record",
    )
    return any(marker in command for marker in write_markers)


def _runtime_unavailable_attention(exc: Exception) -> dict[str, Any]:
    return {
        "status": "runtime_unavailable",
        "summary": "未能读取运行库状态；静态命令清单仍可用，先检查 ASTOCK_DATABASE_URL 和运行库健康。",
        "current_action": {
            "type": "check_runtime_database",
            "label": "检查运行库连接",
            "command": "atrade doctor --json",
            "reason": "agent-context 读取当前候选流需要运行库；当前只能返回静态入口和约束。",
            "safe_to_auto_apply": False,
            "writes_state": False,
            "requires_user_approval": False,
        },
        "approval_gate": {},
        "after_approval_preview": {"available": False},
        "next_window_plan": {},
        "follow_up_commands": [
            "atrade doctor --json",
            "atrade db check --json",
            *RUNTIME_FOLLOW_UP_COMMANDS,
        ],
        "source_commands": ["atrade agent-context --json", "atrade commands --json"],
        "guardrails": RUNTIME_GUARDRAILS,
        "diagnostic": {
            "error_type": exc.__class__.__name__,
        },
    }
