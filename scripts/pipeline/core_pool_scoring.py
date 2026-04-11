#!/usr/bin/env python3
"""
pipeline/core_pool_scoring.py — 核心池每日评分（15:40 执行）

职责：
  1. 读 config/stocks.yaml 的核心池列表
  2. 批量拉取：实时价格 + 技术指标 + 基本面 + 资金流向 + TrendRadar 舆情
  3. 四维评分（技术/基本面/资金/舆情）
  4. 输出到 vault/04-选股/评分报告/核心池_评分_YYYYMMDD.md
  5. 更新 vault/04-选股/核心池.md 的评分列

用法（CLI）：
  python -m scripts.pipeline.core_pool_scoring

用法（导入）：
  from scripts.pipeline.core_pool_scoring import run
  scores = run()  # 返回评分列表
"""

import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.engine.scorer import batch_score, get_recommendation, split_veto_signals
from scripts.state import save_candidate_snapshot_history
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.config_loader import get_stocks
from scripts.utils.logger import get_logger
from scripts.utils.pool_manager import build_pool_snapshot_entries, load_pool_snapshot, save_pool_snapshot
from scripts.utils.runtime_state import update_pipeline_state

_logger = get_logger("pipeline.core_pool_scoring")


def _build_report_content(scores: list, date_str: str) -> str:
    """生成评分报告 markdown 内容"""
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = weekday_names[dt.weekday()]

    lines = [
        f"# 核心池评分报告 — {date_str}（{weekday}）",
        "",
        f"评分时间：{datetime.now().strftime('%H:%M')}",
        "",
        "---",
        "",
        "| 股票 | 代码 | 技术(2) | 基本面(3) | 资金(2) | 舆情(3) | **总分(10)** | 建议 | 数据 |",
        "|------|------|---------|---------|---------|---------|------------|------|------|",
    ]

    for s in scores:
        name = s.get("name", "")
        code = s.get("code", "")
        tech = s.get("technical_score", 0)
        fin = s.get("fundamental_score", 0)
        flow = s.get("flow_score", 0)
        sentiment = s.get("sentiment_score", 0)
        total = s.get("total_score", 0)
        suggestion = get_recommendation(s)
        dq = s.get("data_quality", "ok")
        dq_mark = "✅" if dq == "ok" else "⛔错误" if dq == "error" else "⚠️降级"

        lines.append(
            f"| {name} | {code} | {tech:.1f} | {fin:.1f} | {flow:.1f} | "
            f"{sentiment:.1f} | **{total:.1f}** | {suggestion} | {dq_mark} |"
        )

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 详细数据")
    lines.append("")

    for s in scores:
        lines.append(f"### {s.get('name', '')}（{s.get('code', '')}）")
        lines.append(f"- **总分：{s.get('total_score', 0):.1f}**")
        lines.append(f"- 技术面：{s.get('technical_detail', '')}")
        lines.append(f"- 基本面：{s.get('fundamental_detail', '')}")
        lines.append(f"- 资金流：{s.get('flow_detail', '')}")
        lines.append(f"- 舆情：{s.get('sentiment_detail', '')}")
        dq = s.get("data_quality", "ok")
        if dq != "ok":
            missing = s.get("data_missing_fields", [])
            lines.append(f"- **⚠️ 数据降级**：缺失 {', '.join(missing) if missing else '部分字段'}，评分可能偏低")
        hard_veto, warning_signals = split_veto_signals(s.get("veto_signals", []))
        if hard_veto:
            lines.append(f"- **一票否决：{', '.join(hard_veto)}**")
        elif warning_signals:
            lines.append(f"- **预警：{', '.join(warning_signals)}**")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run() -> list:
    """
    执行核心池评分

    Returns:
        list of dict，每个元素包含评分详情
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[SCORING] 核心池评分 {date_str}")
    try:
        vault = ObsidianVault()
        stocks_cfg = get_stocks()
        current_snapshot = load_pool_snapshot()

        core_pool = [
            {
                "code": str(item.get("code", "")).strip(),
                "name": str(item.get("name", "")).strip(),
            }
            for item in current_snapshot.get("entries", [])
            if str(item.get("bucket", "")).strip() == "core"
        ]
        if not core_pool:
            _logger.warning("结构化核心池为空，从 config/stocks.yaml 回退")
            core_pool = [
                {"code": str(item.get("code", "")).strip(), "name": str(item.get("name", "")).strip()}
                for item in stocks_cfg.get("core_pool", [])
                if str(item.get("code", "")).strip()
            ]

        if not core_pool:
            _logger.warning("核心池为空，退出")
            update_pipeline_state(
                "core_pool_scoring",
                "skipped",
                {"reason": "core_pool_empty"},
                date_str,
            )
            return []

        _logger.info(f">> 核心池: {len(core_pool)} 只")

        for item in core_pool:
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()
            if code:
                _logger.info(f">> 评分 {name}({code})...")

        scores = batch_score(core_pool)
        history_group_id = f"core_pool_scoring:{date_str}:{datetime.now().strftime('%H%M%S')}"
        snapshot_entries, snapshot_meta = build_pool_snapshot_entries(
            scores,
            current_snapshot=current_snapshot,
            strategy_cfg=None,
            source="core_pool_scoring",
        )
        snapshot_meta.update({
            "pipeline": "core_pool_scoring",
            "history_group_id": history_group_id,
            "scored_count": len(scores),
            "source": "core_pool_scoring",
        })
        snapshot_path = save_pool_snapshot(snapshot_entries, snapshot_meta)
        snapshot_meta["snapshot_path"] = snapshot_path
        save_candidate_snapshot_history(
            scores,
            snapshot_date=date_str,
            pipeline="core_pool_scoring",
            history_group_id=history_group_id,
            pool="core",
            universe="tracked",
            source="core_pool_scoring",
            actionable_count=len(scores),
            metadata=snapshot_meta,
        )

        _logger.info(">> 写入评分报告...")
        report_content = _build_report_content(scores, date_str)
        report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
        report_dir.mkdir(parents=True, exist_ok=True)
        time_str = datetime.now().strftime("%H%M%S")
        report_path = report_dir / f"核心池_评分报告_{date_str.replace('-', '')}_{time_str}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        _logger.info(f"  已写入: {report_path.name}")

        _logger.info(">> 同步核心池/观察池投影...")
        core_pool_updated = False
        try:
            vault.sync_pool_projection(snapshot_entries, snapshot_meta)
            core_pool_updated = True
            _logger.info("  核心池/观察池 投影已更新")
        except Exception as e:
            _logger.warning(f"  更新池子投影失败: {e}")

        _logger.info(f"[SCORING] 评分完成，共 {len(scores)} 只")

        update_pipeline_state(
            "core_pool_scoring",
            "success",
            {
                "scored_count": len(scores),
                "report_path": str(report_path),
                "core_pool_updated": core_pool_updated,
                "snapshot_path": snapshot_path,
                "history_group_id": history_group_id,
                "pool_snapshot_summary": snapshot_meta.get("summary", {}),
            },
            date_str,
        )

        return scores
    except Exception as e:
        update_pipeline_state(
            "core_pool_scoring",
            "error",
            {"error": str(e)},
            date_str,
        )
        raise


if __name__ == "__main__":
    result = run()
    print(f"\n核心池评分完成，共 {len(result)} 只")
