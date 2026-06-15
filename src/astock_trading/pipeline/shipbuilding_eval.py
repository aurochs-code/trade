"""
pipeline/shipbuilding_eval.py — 造船板块专项评分

内部研究模块；需要从稳定 CLI 入口接入后再作为操作命令使用。

流程:
1. 用 akshare 获取船舶制造/造船相关板块成分股
2. MarketService 批量抓取行情 + 技术指标
3. Scoring pipeline 评分并输出结果
"""

from __future__ import annotations

import logging
from datetime import datetime

from astock_trading.platform.time import local_today_str
from astock_trading.pipeline.context import build_context
from astock_trading.market.service import MarketService
from astock_trading.market.adapters import (
    AkShareMarketAdapter, AkShareFinancialAdapter, AkShareFlowAdapter,
    TushareClient, TushareMarketAdapter, TushareFinancialAdapter, TushareFlowAdapter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# 1. 用 akshare 获取造船板块成分股
# ------------------------------------------------------------------

SHIPBUILDING_STOCKS_FALLBACK = [
    {"code": "600150", "name": "中国船舶"},
    {"code": "601989", "name": "中国重工"},
    {"code": "600685", "name": "中船防务"},
    {"code": "600482", "name": "中国动力"},
    {"code": "601890", "name": "亚星锚链"},
    {"code": "300008", "name": "天海防务"},
    {"code": "300589", "name": "江龙船艇"},
    {"code": "300600", "name": "国瑞科技"},
    {"code": "603268", "name": "松发股份"},
    {"code": "300065", "name": "海兰信"},
    {"code": "600734", "name": "实达集团"},
    {"code": "002423", "name": "中原特钢"},
    {"code": "000039", "name": "中集集团"},
]


def get_shipbuilding_stocks() -> list[dict]:
    """用 akshare 获取船舶制造板块成分股，失败则返回备用列表。"""
    try:
        import akshare as ak
        _logger.info("[akshare] 正在获取船舶制造板块成分股...")

        # 方式1：东财船舶制造板块
        df = ak.stock_board_industry_cons_em(symbol="船舶制造")
        if df is not None and not df.empty:
            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("名称", "")).strip()
                if code and name:
                    stocks.append({"code": code, "name": name})
            _logger.info(f"[akshare] 获取到 {len(stocks)} 只船舶制造板块股票")
            return stocks

    except Exception as e:
        _logger.warning(f"[akshare] 船舶制造板块获取失败: {e}")

    try:
        import akshare as ak
        # 方式2：东财海工装备板块
        df = ak.stock_board_industry_cons_em(symbol="海工装备")
        if df is not None and not df.empty:
            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("名称", "")).strip()
                if code and name:
                    stocks.append({"code": code, "name": name})
            _logger.info(f"[akshare] 获取到 {len(stocks)} 只海工装备板块股票")
            return stocks
    except Exception as e:
        _logger.warning(f"[akshare] 海工装备板块获取失败: {e}")

    _logger.warning("[akshare] 使用内置备用列表")
    return SHIPBUILDING_STOCKS_FALLBACK


# ------------------------------------------------------------------
# 2. 初始化简化的 MarketService（用于独立运行）
# ------------------------------------------------------------------

def build_shipbuilding_market_service() -> MarketService:
    """构建仅用于造船板块数据抓取的 MarketService。"""
    tushare_client = TushareClient.from_env()
    tushare_market = [TushareMarketAdapter(client=tushare_client)] if tushare_client.enabled else []
    tushare_financial = [TushareFinancialAdapter(client=tushare_client)] if tushare_client.enabled else []
    tushare_flow = [TushareFlowAdapter(client=tushare_client)] if tushare_client.enabled else []
    return MarketService(
        market_providers=[*tushare_market, AkShareMarketAdapter()],
        financial_providers=[*tushare_financial, AkShareFinancialAdapter()],
        flow_providers=[*tushare_flow, AkShareFlowAdapter()],
        sentiment_providers=[],
        store=None,
        concurrency=5,
    )


# ------------------------------------------------------------------
# 3. 评分输出
# ------------------------------------------------------------------

def format_score_row(code, name, result) -> str:
    total = result.total
    tech = next((d for d in result.dimensions if d.name == "technical"), None)
    fund = next((d for d in result.dimensions if d.name == "fundamental"), None)
    flow = next((d for d in result.dimensions if d.name == "flow"), None)
    sent = next((d for d in result.dimensions if d.name == "sentiment"), None)

    emoji = "✅" if total >= 7 else ("🟡" if total >= 5 else "❌")
    veto = " 🔴VETO" if result.veto_triggered else ""

    tech_s = f"{tech.score:.1f}" if tech else "N/A"
    fund_s = f"{fund.score:.1f}" if fund else "N/A"
    flow_s = f"{flow.score:.1f}" if flow else "N/A"
    sent_s = f"{sent.score:.1f}" if sent else "N/A"

    entry = " 📈入场" if result.entry_signal else ""
    style = result.style.value if result.style else ""

    return (
        f"{emoji} {name}({code}) 总分:{total:.1f} "
        f"[技术:{tech_s} 财务:{fund_s} 资金:{flow_s} 舆情:{sent_s}]"
        f"{veto}{entry} {style}"
    )


async def run_shipbuilding_scoring():
    """主流程：获取股票列表 → 批量抓取 → 评分 → 输出"""
    run_id = f"ship_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 1. 获取造船板块股票
    stocks = get_shipbuilding_stocks()
    if not stocks:
        _logger.error("未获取到任何造船板块股票")
        return

    _logger.info(f"[step1] 共 {len(stocks)} 只股票待评分")
    for s in stocks:
        _logger.info(f"  {s['code']} {s['name']}")

    # 2. 初始化 MarketService 并批量抓取
    market_svc = build_shipbuilding_market_service()
    _logger.info("[step2] 开始批量抓取行情和技术指标...")

    snapshots = await market_svc.collect_batch(stocks, run_id)

    # 过滤有效的 snapshots
    valid = [s for s in snapshots if s.quote is not None or s.technical is not None]
    _logger.info(f"[step2] 抓取完成，有效数据 {len(valid)}/{len(snapshots)} 只")

    # 3. 初始化 PipelineContext（用于获取 Scorer）
    _logger.info("[step3] 初始化评分器...")
    buy_threshold = 5.5
    try:
        ctx = build_context()
        scorer = ctx.strategy_svc._scorer
        buy_threshold = float(ctx.cfg.get("scoring", {}).get("thresholds", {}).get("buy", 5.5))
        _logger.info(f"[step3] 评分器就绪，权重: tech={scorer.weights.technical} "
                     f"fund={scorer.weights.fundamental} flow={scorer.weights.flow} "
                     f"sent={scorer.weights.sentiment}")
    except Exception as e:
        _logger.warning(f"[step3] PipelineContext 初始化失败，使用默认权重: {e}")
        from astock_trading.strategy.models import ScoringWeights
        from astock_trading.strategy.scorer import Scorer
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=["below_ma20", "limit_up_today", "consecutive_outflow"],
            entry_cfg={"rsi_max": 70, "volume_ratio_min": 1.5},
        )

    # 4. 评分
    _logger.info("[step4] 开始评分...")
    results = scorer.score_batch(valid)
    results.sort(key=lambda r: r.total, reverse=True)

    # 5. 输出结果
    print("\n" + "=" * 80)
    print(f"🚢 造船板块评分报告 — {local_today_str()} (run_id: {run_id})")
    print("=" * 80)

    for i, r in enumerate(results, 1):
        print(f"{i}. {format_score_row(r.code, r.name, r)}")
        if r.veto_signals:
            print(f"   ⚠️ 否决信号: {', '.join(r.veto_signals)}")
        if r.data_missing_fields:
            print(f"   ⚠️ 数据缺失: {', '.join(r.data_missing_fields)}")

    # 汇总统计
    totals = [r.total for r in results]
    avg = sum(totals) / len(totals) if totals else 0
    passed = sum(1 for r in results if r.total >= buy_threshold)
    vetoed = sum(1 for r in results if r.veto_triggered)

    print("-" * 80)
    print(f"📊 汇总: 共 {len(results)} 只 | 平均分 {avg:.1f} | "
          f"达标(≥{buy_threshold:.1f}) {passed} 只 | 否决 {vetoed} 只")

    # 入场信号股票
    entry_signals = [r for r in results if r.entry_signal]
    if entry_signals:
        print(f"\n📈 入场信号 ({len(entry_signals)} 只):")
        for r in entry_signals:
            print(f"   {r.name}({r.code}) 评分:{r.total:.1f}")

    return results
