"""隔离逐票评分的子进程入口。"""

from __future__ import annotations

import contextlib
import json
import sys
import traceback

from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.screener import (
    _build_source_quality_summary,
    _score_stock_batch,
)


def main() -> int:
    try:
        request = json.loads(sys.stdin.read() or "{}")
    except json.JSONDecodeError:
        print("评分 worker 收到非 JSON 请求。", file=sys.stderr)
        return 2

    stock_list = request.get("stock_list") or []
    run_id = str(request.get("run_id") or "")
    if not isinstance(stock_list, list) or not run_id:
        print("评分 worker 请求缺少 stock_list 或 run_id。", file=sys.stderr)
        return 2
    requested_codes = {
        str(item.get("code") or "").strip()
        for item in stock_list
        if isinstance(item, dict) and str(item.get("code") or "").strip()
    }

    ctx = build_context()
    try:
        with contextlib.redirect_stdout(sys.stderr):
            score_batch = _score_stock_batch(ctx, stock_list, run_id)
        scores = score_batch.get("scores", []) or []
        if requested_codes:
            scores = [
                score
                for score in scores
                if str(score.get("code") or "").strip() in requested_codes
            ]
        snapshots = score_batch.get("snapshots", []) or []
        payload = {
            "scores": scores,
            "snapshots": [],
            "source_quality": _build_source_quality_summary(snapshots, scores),
        }
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return 0
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1
    finally:
        ctx.conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
