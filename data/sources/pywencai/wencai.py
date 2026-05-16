"""iwencai 选股数据源 — 直接请求 iwencai API"""
import json
import math
import logging
import os
import time
from urllib.parse import parse_qsl, urlparse

import requests as rq
import pandas as pd
import pydash as _

try:
    from .headers import build_headers
except ImportError:
    try:
        from headers import build_headers
    except ImportError:
        def build_headers(cookie: str = "") -> dict:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
                "Referer": "http://www.iwencai.com/",
            }
            if cookie:
                headers["Cookie"] = cookie
            return headers

logger = logging.getLogger(__name__)

# ── cookie 保存路径 ─────────────────────────────────────────────────────────
_COOKIE_PATH = os.path.join(os.path.dirname(__file__), "cookie.txt")

def load_cookie() -> str:
    if not os.path.exists(_COOKIE_PATH):
        return ""
    with open(_COOKIE_PATH) as f:
        return f.read().strip()

def save_cookie(cookie: str) -> None:
    with open(_COOKIE_PATH, "w") as f:
        f.write(cookie.strip())

# ── 低层请求工具 ─────────────────────────────────────────────────────────────

def _while_do(do, retry=3, sleep=1):
    for i in range(retry):
        try:
            return do()
        except Exception as exc:
            logger.warning(f"[iwencai] 请求失败（{i+1}/{retry}）: {exc}")
            if i < retry - 1:
                time.sleep(sleep)
    return None

# ── Step 1: get-robot-data → 获取 condition / url_params ───────────────────

def _get_robot_data(query: str, query_type: str = "stock", cookie: str = "") -> dict:
    """返回 {'url_params': dict, 'condition': str, 'row_count': int}"""
    url = "http://www.iwencai.com/customized/chart/get-robot-data"
    payload = {
        "add_info": '{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}',
        "perpage": 10,
        "page": 1,
        "source": "Ths_iwencai_Xuangu",
        "log_info": '{"input_type":"click"}',
        "version": "2.0",
        "secondary_intent": query_type,
        "question": query,
    }

    def do():
        res = rq.post(
            url,
            json=payload,
            headers=build_headers(cookie or load_cookie()),
            timeout=(5, 15),
        )
        return _parse_robot_response(res)

    return _while_do(do)

def _parse_robot_response(res: rq.Response) -> dict:
    result = json.loads(res.text)
    content_str = _.get(result, "data.answer.0.txt.0.content")
    if isinstance(content_str, str):
        content = json.loads(content_str)
    else:
        content = content_str

    components = content.get("components", [])
    if not components:
        raise ValueError("components 为空")

    first = components[0]
    show_type = first.get("show_type")

    if show_type == "xuangu_tableV1":
        footer_url = _.get(first, "config.other_info.footer_info.url", "")
        row_count = _.get(first, "data.meta.extra.row_count", 0)
        condition = _.get(first, "data.meta.extra.condition", "")

        # 解析 footer_url 的 query string（保留所有参数，包括空值）
        if footer_url:
            qs_part = footer_url.split("?", 1)[1] if "?" in footer_url else ""
            params_list = parse_qsl(qs_part, keep_blank_values=True)
            url_params = {}
            for k, v in params_list:
                if k in url_params:
                    existing = url_params[k]
                    url_params[k] = [existing, v] if not isinstance(existing, list) else existing + [v]
                else:
                    url_params[k] = v
        else:
            url_params = {}

        return {
            "condition": condition,
            "row_count": int(row_count) if row_count else 0,
            "url_params": url_params,
        }
    else:
        # 通用 handler：取所有 components 的 datas
        datas_list = []
        for comp in components:
            ds = _.get(comp, "data.datas", [])
            if isinstance(ds, list):
                datas_list.extend(ds)
        if datas_list:
            return {
                "condition": None,
                "row_count": len(datas_list),
                "url_params": {},
                "inline_datas": datas_list,
            }
        raise ValueError(f"不支持的 show_type: {show_type}，且无 inline_datas")

# ── Step 2: getDataList → 分页取数据 ───────────────────────────────────────

def _get_data_list(url_params: dict, cookie: str = "", page: int = 1,
                   perpage: int = 100) -> pd.DataFrame:
    url = "http://www.iwencai.com/gateway/urp/v7/landing/getDataList"
    payload = {**url_params, "page": page, "perpage": perpage}

    def do():
        res = rq.post(
            url,
            data=payload,
            headers={**build_headers(cookie or load_cookie()),
                     "Content-Type": "application/x-www-form-urlencoded"},
            timeout=(5, 15),
        )
        result = json.loads(res.text)
        # 正确路径：answer.components.0.data.datas
        datas = _.get(result, "answer.components.0.data.datas", [])
        if not datas:
            raise ValueError(f"datas 为空: {result.get('status_msg')}")
        return pd.DataFrame.from_dict(datas)

    return _while_do(do)

# ── 公开 API ────────────────────────────────────────────────────────────────

def query(query: str, loop: bool = True, cookie: str = "") -> pd.DataFrame:
    """
    查询 iwencai，支持自然语言选股条件。

    Args:
        query:  问财语句，如 "A股今日涨跌幅>3%"
        loop:   是否自动分页（默认 True）
        cookie: 可选，传入则覆盖默认 cookie

    Returns:
        DataFrame 或 None（失败时）
    """
    params = _get_robot_data(query, cookie=cookie)
    if params is None:
        logger.error("[iwencai] get_robot_data 失败")
        return None

    condition = params.get("condition")
    url_params = params.get("url_params", {})
    row_count = params.get("row_count", 0)

    if not condition:
        # 尝试通用 inline_datas 路径
        inline = params.get("inline_datas", [])
        if inline:
            result = pd.DataFrame.from_dict(inline)
            logger.info(f"[iwencai] inline 模式成功，{len(result)} 条")
            return result
        logger.warning("[iwencai] 无 condition 且无 inline_datas，返回空")
        return None

    if loop and row_count > 100:
        pages = math.ceil(row_count / 100)
        frames = []
        for p in range(1, pages + 1):
            df = _get_data_list(url_params, cookie=cookie, page=p, perpage=100)
            frames.append(df)
        result = pd.concat(frames, ignore_index=True) if frames else None
    else:
        result = _get_data_list(url_params, cookie=cookie, page=1, perpage=100)

    if result is not None and not result.empty:
        logger.info(f"[iwencai] 查询成功，{len(result)} 条结果（总 {row_count}）")
    return result


# ── 兼容性 alias ─────────────────────────────────────────────────────────────
def fetch_stocks(codes: list, cookie: str = "") -> pd.DataFrame:
    """
    用股票代码列表批量拉取数据（通过 find 接口）。

    注意：find 接口需要完整参数，目前通过 query() 通用查询更可靠。
    这里用 query() 对代码列表做模糊查询作为替代。
    """
    if not codes:
        return None
    # 用逗号拼接代码列表查询
    return query(",".join(codes), loop=False, cookie=cookie)
