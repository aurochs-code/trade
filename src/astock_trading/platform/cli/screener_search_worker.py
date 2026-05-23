"""隔离执行妙想粗筛搜索的子进程入口。"""

from __future__ import annotations

import json
import sys

from astock_trading.market.adapters import MXScreenerAdapter


def main() -> int:
    query = sys.argv[1] if len(sys.argv) > 1 else ""
    rows = MXScreenerAdapter()._sync(query)
    sys.stdout.write(json.dumps(rows, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
