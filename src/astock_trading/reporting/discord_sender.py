"""
reporting/discord_sender.py — Discord 消息发送

通过 Bot Token DM 指定用户，或发送到频道。
格式化由 discord.py 负责，这里只负责发送。
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

_logger = logging.getLogger(__name__)

API_BASE = "https://discord.com/api/v10"

# 模块级加载 .env，只读一次
_env_loaded = False


def _load_env_once():
    """从 .env 加载环境变量（仅首次调用时读文件）。"""
    global _env_loaded
    if _env_loaded:
        return
    _env_loaded = True
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


def _get_token() -> str:
    _load_env_once()
    return os.environ.get("DISCORD_BOT_TOKEN", "").strip()


def _get_dm_user_id() -> str:
    _load_env_once()
    return os.environ.get("DISCORD_DM_USER_ID", "").strip()


def _get_channel_id() -> str:
    _load_env_once()
    return os.environ.get("DISCORD_CHANNEL_ID", "").strip()


def _api_request(
    method: str,
    endpoint: str,
    token: str,
    payload: Optional[dict] = None,
    max_retries: int = 3,
) -> dict:
    """发送 Discord API 请求，带指数退避重试。"""
    url = f"{API_BASE}{endpoint}"
    request_timeout = _env_float("ASTOCK_DISCORD_TIMEOUT_SECONDS", 15.0, minimum=1.0)
    retry_count = _env_int("ASTOCK_DISCORD_MAX_RETRIES", max_retries, minimum=1)
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
        "User-Agent": "A-Stock Trading/2.0",
    }
    data = json.dumps(payload).encode("utf-8") if payload else None

    last_error: dict = {}
    for attempt in range(retry_count):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=request_timeout) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")[:300]
            last_error = {"error": f"HTTP {e.code}", "detail": body}
            # 429 (rate limit) 或 5xx → 重试
            if e.code == 429 or e.code >= 500:
                retry_after = 1.0
                try:
                    err_data = json.loads(body)
                    retry_after = float(err_data.get("retry_after", retry_after))
                except Exception:
                    pass
                if attempt < retry_count - 1:
                    wait = max(retry_after, (2 ** attempt) * 0.5)
                    _logger.warning(f"Discord API {e.code}, retry {attempt+1}/{retry_count} in {wait:.1f}s")
                    time.sleep(wait)
                    continue
                return last_error
            # 4xx 非 429 → 不重试
            _logger.error(f"Discord API error: {e.code} {body}")
            return last_error
        except Exception as e:
            last_error = {"error": str(e)}
            if attempt < retry_count - 1:
                time.sleep((2 ** attempt) * 0.5)
                continue
            _logger.error(f"Discord request failed: {e}")
            return last_error

    return last_error


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float, *, minimum: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, float(raw))
    except ValueError:
        return default


def _get_dm_channel(token: str, user_id: str) -> Optional[str]:
    """创建或获取 DM 频道 ID。"""
    result = _api_request("POST", "/users/@me/channels", token, {"recipient_id": user_id})
    return result.get("id")


def send_embed(embed: dict, content: str = "") -> tuple[bool, str]:
    """
    发送 Discord Rich Embed。

    优先 DM 用户，其次发到频道。
    返回 (success, error_message)。
    """
    token = _get_token()
    if not token:
        return False, "DISCORD_BOT_TOKEN not configured"

    # 确定目标频道
    dm_user = _get_dm_user_id()
    channel_id = _get_channel_id()

    if dm_user:
        channel_id = _get_dm_channel(token, dm_user)
        if not channel_id:
            return False, f"Failed to create DM channel for user {dm_user}"
    elif not channel_id:
        return False, "Neither DISCORD_DM_USER_ID nor DISCORD_CHANNEL_ID configured"

    # 发送
    payload = {"embeds": [embed]}
    if content:
        payload["content"] = content[:2000]

    result = _api_request("POST", f"/channels/{channel_id}/messages", token, payload)

    if "id" in result:
        _logger.info(f"Discord message sent: {result['id']}")
        return True, ""
    else:
        return False, result.get("error", result.get("detail", "Unknown error"))


def send_text(text: str) -> tuple[bool, str]:
    """发送纯文本消息。"""
    token = _get_token()
    if not token:
        return False, "DISCORD_BOT_TOKEN not configured"

    dm_user = _get_dm_user_id()
    channel_id = _get_channel_id()

    if dm_user:
        channel_id = _get_dm_channel(token, dm_user)
        if not channel_id:
            return False, "Failed to create DM channel"
    elif not channel_id:
        return False, "No target configured"

    payload = {"content": text[:2000]}
    result = _api_request("POST", f"/channels/{channel_id}/messages", token, payload)

    if "id" in result:
        return True, ""
    return False, result.get("error", "Unknown error")
