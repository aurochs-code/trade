"""Tests for reporting/discord_sender.py — retry logic and env loading"""

import os
from unittest.mock import patch, MagicMock
import urllib.error

from astock_trading.reporting.discord_sender import (
    _api_request,
    _load_env_once,
    send_embed,
)


class TestEnvLoading:
    def test_load_env_once_flag(self):
        """_load_env_once should only read file once."""
        import astock_trading.reporting.discord_sender as mod
        mod._env_loaded = False
        _load_env_once()
        assert mod._env_loaded is True
        # Second call should be a no-op (no file read)
        _load_env_once()
        assert mod._env_loaded is True

    def test_no_token_returns_error(self):
        """send_embed should fail gracefully without token."""
        with patch.dict(os.environ, {}, clear=True):
            import astock_trading.reporting.discord_sender as mod
            mod._env_loaded = True  # skip file loading
            ok, err = send_embed({"title": "test"})
            assert ok is False
            assert "not configured" in err


class TestRetryLogic:
    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    def test_success_first_try(self, mock_urlopen):
        """Successful request on first try."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"id": "123"}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = _api_request("POST", "/test", "fake_token", {"data": 1})
        assert result["id"] == "123"
        assert mock_urlopen.call_count == 1

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    @patch("astock_trading.reporting.discord_sender.time.sleep")
    def test_retry_on_500(self, mock_sleep, mock_urlopen):
        """Should retry on 500 errors."""
        error_resp = MagicMock()
        error_resp.read.return_value = b'{"message": "server error"}'
        http_error = urllib.error.HTTPError(
            "http://test", 500, "Internal Server Error", {}, error_resp
        )

        success_resp = MagicMock()
        success_resp.read.return_value = b'{"id": "456"}'
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [http_error, success_resp]

        result = _api_request("POST", "/test", "fake_token", max_retries=3)
        assert result["id"] == "456"
        assert mock_urlopen.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    @patch("astock_trading.reporting.discord_sender.time.sleep")
    def test_retry_on_429(self, mock_sleep, mock_urlopen):
        """Should retry on 429 rate limit with retry_after."""
        error_resp = MagicMock()
        error_resp.read.return_value = b'{"retry_after": 0.5}'
        http_error = urllib.error.HTTPError(
            "http://test", 429, "Too Many Requests", {}, error_resp
        )

        success_resp = MagicMock()
        success_resp.read.return_value = b'{"id": "789"}'
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [http_error, success_resp]

        result = _api_request("POST", "/test", "fake_token", max_retries=3)
        assert result["id"] == "789"

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    def test_no_retry_on_400(self, mock_urlopen):
        """Should NOT retry on 4xx (non-429) errors."""
        error_resp = MagicMock()
        error_resp.read.return_value = b'{"message": "bad request"}'
        http_error = urllib.error.HTTPError(
            "http://test", 400, "Bad Request", {}, error_resp
        )
        mock_urlopen.side_effect = http_error

        result = _api_request("POST", "/test", "fake_token", max_retries=3)
        assert "error" in result
        assert mock_urlopen.call_count == 1  # no retry

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    @patch("astock_trading.reporting.discord_sender.time.sleep")
    def test_all_retries_exhausted(self, mock_sleep, mock_urlopen):
        """Should return error after all retries exhausted."""
        error_resp = MagicMock()
        error_resp.read.return_value = b'{"message": "server error"}'
        http_error = urllib.error.HTTPError(
            "http://test", 500, "Internal Server Error", {}, error_resp
        )
        mock_urlopen.side_effect = http_error

        result = _api_request("POST", "/test", "fake_token", max_retries=3)
        assert "error" in result
        assert mock_urlopen.call_count == 3

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    @patch("astock_trading.reporting.discord_sender.time.sleep")
    def test_retry_on_connection_error(self, mock_sleep, mock_urlopen):
        """Should retry on connection errors."""
        success_resp = MagicMock()
        success_resp.read.return_value = b'{"id": "ok"}'
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [ConnectionError("timeout"), success_resp]

        result = _api_request("POST", "/test", "fake_token", max_retries=3)
        assert result["id"] == "ok"
        assert mock_urlopen.call_count == 2

    @patch("astock_trading.reporting.discord_sender.urllib.request.urlopen")
    @patch("astock_trading.reporting.discord_sender.time.sleep")
    def test_env_overrides_timeout_and_retry_count(self, mock_sleep, mock_urlopen):
        """Ops watchdog can keep Discord sends bounded through env overrides."""
        mock_urlopen.side_effect = TimeoutError("network timeout")

        with patch.dict(os.environ, {
            "ASTOCK_DISCORD_TIMEOUT_SECONDS": "4",
            "ASTOCK_DISCORD_MAX_RETRIES": "1",
        }):
            result = _api_request("POST", "/test", "fake_token", max_retries=3)

        assert "network timeout" in result["error"]
        assert mock_urlopen.call_count == 1
        assert mock_urlopen.call_args.kwargs["timeout"] == 4
        assert mock_sleep.call_count == 0
