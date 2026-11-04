# -*- coding: utf-8 -*-
"""独立 DEMO（模拟盘）只读客户端 + 纯 mock 传输（阶段 1，按独立复核收紧）。

硬边界（阶段 1）：
- 凭据只读 `OKX_DEMO_API_KEY / OKX_DEMO_API_SECRET / OKX_DEMO_API_PASSPHRASE`：
  进程环境优先，缺失时只读 Windows 用户环境（HKCU\\Environment）；**永不回退 `OKX_API_*`**，
  不打印/不落盘/不走命令行参数。
- 固定官方域名 `https://www.okx.com`（与已验收本地工具一致，不自动切地区域名）；
  固定请求头 `x-simulated-trading: 1`；拒绝重定向；路径不得改变 URL authority。
- 阶段 1 只允许 GET；写接口一律 `DEMO_WRITE_NOT_RELEASED`（实际开仓未放行）。
- 请求带完整 OK-ACCESS-* 鉴权签名（GET 空 body，path 含 query，UTC 毫秒时间戳）。
- 错误只输出固定代码/数字 API 码，不输出 URL、异常正文、headers、raw data；响应读取有上限；
  payload/data 结构严格校验，None 页不得当空。
"""
from __future__ import annotations

import base64
import errno
import hmac
import hashlib
import json
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

DEMO_BASE = "https://openapi.okx.com"
DEMO_HOST = "openapi.okx.com"
DEMO_HEADER = "x-simulated-trading"
DEMO_HEADER_VALUE = "1"
INST = "SOL-USDT-SWAP"
CREDENTIAL_NAMES = ("OKX_DEMO_API_KEY", "OKX_DEMO_API_SECRET", "OKX_DEMO_API_PASSPHRASE")
MAX_TIMEOUT = 8
MAX_BODY = 262_144                     # 256 KiB
DEFAULT_PATH_PREFIX = "/api/v5/"
# Same fixed read-only evidence endpoints as demo_verified_risk.SAFE_GET_PATHS.
# This is a retry allowlist, not an expansion of the client's GET scope.
RETRY_GET_PATHS = frozenset({
    "/api/v5/account/config", "/api/v5/account/balance",
    "/api/v5/account/positions", "/api/v5/account/leverage-info",
    "/api/v5/trade/orders-pending", "/api/v5/trade/orders-algo-pending",
    "/api/v5/account/bills-archive", "/api/v5/trade/fills-history",
    "/api/v5/account/positions-history", "/api/v5/trade/order",
    "/api/v5/public/time", "/api/v5/public/instruments",
})
MAX_RETRY_EVENTS = 64
NETWORK_RETRY_DELAY = 0.1
# Bounded local failure diagnostics. Only exact TLS_EOF may enter the existing
# opt-in GET retry policy; categories never extend evidence time or admit entry.
NETWORK_FAILURE_CATEGORIES = frozenset({
    "DNS", "TLS_CERTIFICATE", "TLS_EOF", "TLS", "CONNECT_TIMEOUT",
    "READ_TIMEOUT", "POOL_WAIT_TIMEOUT", "CONNECTION", "PROXY",
    "PROTOCOL", "TIMEOUT", "UNKNOWN",
})


class DemoCredentialError(Exception):
    def __init__(self, code: str = "DEMO_CREDENTIALS_INCOMPLETE"):
        super().__init__(code)
        self.code = code


class DemoTransportError(Exception):
    def __init__(self, code: str, numeric: int | None = None):
        text = code if numeric is None else f"{code}:{numeric}"
        super().__init__(text)
        self.code = code
        self.numeric = numeric


class DemoWriteRefused(Exception):
    def __init__(self, code: str = "DEMO_WRITE_NOT_RELEASED"):
        super().__init__(code)
        self.code = code


# --------------------------------------------------------------------------- 凭据


def _read_user_env(name: str) -> str | None:
    """只读 Windows 用户环境变量（HKCU\\Environment）；失败返回 None，不抛异常细节。"""
    try:
        import winreg  # type: ignore
    except ImportError:
        return None
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment", 0,
                            winreg.KEY_READ | winreg.KEY_QUERY_VALUE) as key:
            value, _ = winreg.QueryValueEx(key, name)
        return str(value) if value else None
    except Exception:  # noqa: BLE001 - 任何注册表错误都视为缺失，不输出细节
        return None


def load_demo_credentials(env: dict | None = None) -> dict:
    """一次性读取三项 DEMO 凭据；缺项拒绝；绝不回退实盘 OKX_API_*。"""
    source = env if env is not None else os.environ
    values = {}
    for name in CREDENTIAL_NAMES:
        value = source.get(name) if hasattr(source, "get") else None
        if not value:
            if env is None:
                value = _read_user_env(name)
        if not value:
            raise DemoCredentialError(f"DEMO_CREDENTIALS_INCOMPLETE:{name}")
        values[name] = str(value)
    return values


# --------------------------------------------------------------------------- 签名


def _demo_sign(secret: str, timestamp: str, method: str, request_path: str, body: str) -> str:
    message = f"{timestamp}{method}{request_path}{body}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _now_iso() -> str:
    """OKX REST 要求 ISO8601 UTC 毫秒格式，例如 2026-10-19T00:00:00.000Z。"""
    now = datetime.now(tz=timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


# --------------------------------------------------------------------------- HTTP


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401,ARG002
        raise DemoTransportError("DEMO_REDIRECT_FORBIDDEN")


def _temporary_network_type(error: urllib.error.URLError) -> str | None:
    """Classify only explicit transient reasons; never retain exception text.

    HTTPError is a URLError subclass, so it must be excluded before examining
    its reason. TLS verification and unknown TLS errors also fail immediately.
    """
    if isinstance(error, urllib.error.HTTPError):
        return None
    reason = error.reason
    if isinstance(reason, (urllib.error.HTTPError, ssl.SSLCertVerificationError)):
        return None
    if isinstance(reason, ssl.SSLEOFError):
        return "SSLEOFError"
    if isinstance(reason, ssl.SSLError):
        return None
    if isinstance(reason, socket.gaierror):
        return "TemporaryDNS" if reason.errno == socket.EAI_AGAIN else None
    for kind, label in ((TimeoutError, "TimeoutError"),
                        (ConnectionResetError, "ConnectionResetError"),
                        (ConnectionAbortedError, "ConnectionAbortedError"),
                        (ConnectionRefusedError, "ConnectionRefusedError"),
                        (BrokenPipeError, "BrokenPipeError"),
                        (EOFError, "EOFError")):
        if isinstance(reason, kind):
            return label
    if isinstance(reason, OSError):
        return {errno.ETIMEDOUT: "TimeoutError",
                errno.ECONNRESET: "ConnectionResetError",
                errno.ECONNABORTED: "ConnectionAbortedError",
                errno.ECONNREFUSED: "ConnectionRefusedError",
                errno.EPIPE: "BrokenPipeError"}.get(reason.errno)
    return None


def _pooled_temporary_network_type(error: DemoTransportError) -> str | None:
    """Recognize only the pool's sanitized, exact SSL EOF failure.

    Other diagnostic categories remain observations, not retry permissions.
    The original exception is retained if this physical attempt is final.
    """
    if type(error) is not DemoTransportError:
        return None
    category = getattr(error, "network_category", None)
    if (type(error.code) is str and error.code == "DEMO_NETWORK_ERROR"
            and error.numeric is None and type(category) is str and category == "TLS_EOF"):
        return "SSLEOFError"
    return None


def _real_http_get(url: str, headers: dict, timeout: int) -> tuple[int, object]:
    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, headers=headers, method="GET")
    with opener.open(req, timeout=timeout) as resp:
        status = int(getattr(resp, "status", 0) or 0)
        raw = resp.read(MAX_BODY + 1)
    if not isinstance(raw, (bytes, bytearray)):
        raise DemoTransportError("DEMO_BAD_RESPONSE")
    if len(raw) > MAX_BODY:
        raise DemoTransportError("DEMO_RESPONSE_TOO_LARGE")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise DemoTransportError("DEMO_BAD_JSON") from None
    return status, payload


# --------------------------------------------------------------------------- 只读客户端


class DemoReadOnlyClient:
    """Fixed-DEMO reads, with optional bounded transport retries.

    ``network_retries=1`` requires a callable ``retry_budget`` that reserves the
    same evidence provider's per-path quota for the second physical attempt.
    The hook runs after backoff and before re-signing; it must raise if
    reservation is unavailable. A transport retry alone is not a rate-limit
    repair, and HTTP/API failures are never retried.
    """

    def __init__(self, credentials: dict, http_get: Callable | None = None,
                 base: str = DEMO_BASE, timeout: int = MAX_TIMEOUT,
                 network_retries: int = 0,
                 retry_budget: Callable[[str], None] | None = None):
        if base != DEMO_BASE:
            raise DemoTransportError("DEMO_BASE_LOCKED")
        if type(network_retries) is not int or network_retries not in (0, 1):
            raise DemoTransportError("DEMO_NETWORK_RETRY_POLICY_INVALID")
        if retry_budget is not None and not callable(retry_budget):
            raise DemoTransportError("DEMO_NETWORK_RETRY_BUDGET_INVALID")
        if network_retries == 1 and retry_budget is None:
            raise DemoTransportError("DEMO_NETWORK_RETRY_POLICY_INVALID")
        if not isinstance(credentials, dict) or any(not credentials.get(n) for n in CREDENTIAL_NAMES):
            raise DemoCredentialError("DEMO_CREDENTIALS_INCOMPLETE")
        self._creds = credentials
        self._http_get = http_get or _real_http_get
        self._timeout = int(timeout)
        self._network_retries = network_retries
        self._retry_budget = retry_budget
        self._retry_events = deque(maxlen=MAX_RETRY_EVENTS)
        self._retry_events_lock = threading.Lock()

    def retry_events(self) -> list[dict]:
        """Return a bounded, detached observation of actual GET retries only."""
        with self._retry_events_lock:
            return [dict(event) for event in self._retry_events]

    def _request_path(self, path: str, params: dict | None) -> str:
        if not isinstance(path, str) or not path.startswith(DEFAULT_PATH_PREFIX):
            raise DemoTransportError("DEMO_PATH_NOT_ALLOWED")
        if "://" in path or "@" in path or "\\" in path or path.startswith("//"):
            raise DemoTransportError("DEMO_PATH_NOT_ALLOWED")
        if any(seg in path for seg in ("/../", "/./")):
            raise DemoTransportError("DEMO_PATH_NOT_ALLOWED")
        query = urllib.parse.urlencode(params or {}, doseq=True)
        return path + (f"?{query}" if query else "")

    def _headers(self, method: str, request_path: str, body: str = "") -> dict:
        timestamp = _now_iso()
        signature = _demo_sign(self._creds[CREDENTIAL_NAMES[1]], timestamp,
                               method.upper(), request_path, body)
        return {
            "OK-ACCESS-KEY": self._creds[CREDENTIAL_NAMES[0]],
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._creds[CREDENTIAL_NAMES[2]],
            DEMO_HEADER: DEMO_HEADER_VALUE,
            "Accept": "application/json",
            "User-Agent": "sol-signal-demo-client/1.0",
        }

    def _schedule_get_retry(self, path: str, attempt: int, reason_type: str | None) -> bool:
        """Use the existing one-retry policy and same-path quota reservation."""
        if (attempt != 0 or self._network_retries != 1
                or path not in RETRY_GET_PATHS or reason_type is None):
            return False
        time.sleep(NETWORK_RETRY_DELAY)
        if self._retry_budget is not None:
            try:
                self._retry_budget(path)
            except Exception:  # noqa: BLE001 - budget failures are also sanitized
                raise DemoTransportError("DEMO_NETWORK_RETRY_BUDGET_FAILED") from None
        with self._retry_events_lock:
            self._retry_events.append({"path": path, "code": "DEMO_NETWORK_RETRY",
                                       "attempt": 2, "type": reason_type})
        return True

    def get(self, path: str, params: dict | None = None) -> object:
        request_path = self._request_path(path, params)
        url = f"{DEMO_BASE}{request_path}"
        if urllib.parse.urlsplit(url).netloc != DEMO_HOST:
            raise DemoTransportError("DEMO_BASE_LOCKED")
        for attempt in range(self._network_retries + 1):
            # Rebuild the UTC timestamp and signature for every network attempt.
            headers = self._headers("GET", request_path, "")
            try:
                status, payload = self._http_get(url, headers, self._timeout)
            except DemoTransportError as error:
                if self._schedule_get_retry(path, attempt, _pooled_temporary_network_type(error)):
                    continue
                raise
            except urllib.error.URLError as error:
                reason_type = _temporary_network_type(error)
                if self._schedule_get_retry(path, attempt, reason_type):
                    continue
                raise DemoTransportError("DEMO_NETWORK_ERROR") from None
            except Exception:  # noqa: BLE001 - 网络/HTTP 异常一律脱敏
                raise DemoTransportError("DEMO_NETWORK_ERROR") from None
            break
        if not isinstance(status, int) or status != 200:
            numeric = status if isinstance(status, int) and 100 <= status <= 599 else None
            raise DemoTransportError("DEMO_HTTP_ERROR", numeric)
        if not isinstance(payload, dict):
            raise DemoTransportError("DEMO_BAD_RESPONSE")
        raw_code = payload.get("code")
        code_text = str(raw_code) if raw_code is not None else ""
        if code_text != "0":
            numeric = int(code_text) if code_text.isascii() and code_text.isdigit() and 1 <= len(code_text) <= 8 else None
            raise DemoTransportError("DEMO_API_ERROR", numeric)
        data = payload.get("data")
        if not isinstance(data, list):
            raise DemoTransportError("DEMO_BAD_RESPONSE")
        return data

    def post(self, path: str, body: dict | None = None) -> object:  # noqa: ARG002
        raise DemoWriteRefused()


# --------------------------------------------------------------------------- preflight


def demo_preflight(client: "DemoReadOnlyClient") -> dict:
    config = client.get("/api/v5/account/config")
    balance = client.get("/api/v5/account/balance", {"ccy": "USDT"})
    positions = client.get("/api/v5/account/positions", {"instType": "SWAP"})
    pending = client.get("/api/v5/trade/orders-pending", {"instType": "SWAP"})
    algos = {}
    for kind in ("oco", "conditional", "trigger"):
        algos[kind] = len(client.get("/api/v5/trade/orders-algo-pending",
                                     {"instType": "SWAP", "instId": INST, "ordType": kind}) or [])
    lever = client.get("/api/v5/account/leverage-info", {"instId": INST, "mgnMode": "cross"})
    spec = client.get("/api/v5/public/instruments", {"instType": "SWAP", "instId": INST})
    equity = None
    try:
        details = balance[0].get("details") or []
        usdt = next((d for d in details if str(d.get("ccy")) == "USDT"), None)
        equity = float(usdt.get("eq")) if usdt else None
        if not (equity == equity and abs(equity) != float("inf")):
            equity = None
    except (TypeError, ValueError, IndexError, KeyError):
        equity = None
    return {
        "transport": "demo-readonly",
        "simulated_header": DEMO_HEADER_VALUE,
        "pos_mode": (config[0].get("posMode") if config else None),
        "acct_lv": (config[0].get("acctLv") if config else None),
        "equity_usdt": equity,
        "positions": len(positions or []),
        "pending_orders": len(pending or []),
        "algo_orders": algos,
        "sol_cross_lever": (lever[0].get("lever") if lever else None),
        "sol_min_sz": (spec[0].get("minSz") if spec else None),
        "sol_ct_val": (spec[0].get("ctVal") if spec else None),
        "release_ready": False,
        "orders_sent": False,
        "write_allowed": False,
    }


# --------------------------------------------------------------------------- 纯 mock


@dataclass
class MockDemoTransport:
    scenario: str = "protected"
    events: list = field(default_factory=list)

    def render_card(self, signal: dict) -> dict:
        card = {
            "venue": "DEMO-MOCK（非真实网络）",
            "signal_id": signal["id"],
            "side": signal["side"],
            "entry": signal["entry"],
            "stop": signal["stop"],
            "tp1": signal["tp1"],
            "qty": signal.get("qty", "0.01"),
            "equity": 100.0,
            "simulated": True,
        }
        self.events.append({"event": "card_shown", "card": card})
        return card

    def execute(self, card: dict, confirmed: bool) -> dict:  # noqa: ARG002
        if not confirmed:
            self.events.append({"event": "operator_rejected"})
            return {"status": "REJECTED", "code": "OPERATOR_REJECTED", "steps": [], "halt": False}
        scenario = self.scenario
        if scenario == "protected":
            steps = ["submit", "fill", "protect", "verify"]
            self.events.extend({"event": s} for s in steps)
            return {"status": "PROTECTED", "code": "OK", "steps": steps, "halt": False}
        if scenario == "timeout":
            steps = ["submit", "cancel", "verify_cancel"]
            self.events.extend({"event": s} for s in steps)
            return {"status": "CANCELED", "code": "CANCELED", "steps": steps, "halt": False}
        if scenario == "cancel-race":
            steps = ["submit", "cancel", "fill_during_cancel", "protect", "verify"]
            self.events.extend({"event": s} for s in steps)
            return {"status": "PROTECTED", "code": "OK", "steps": steps, "halt": False}
        if scenario == "protection-fail":
            steps = ["submit", "fill", "protect", "protect_failed", "close", "flat_verified"]
            self.events.extend({"event": s} for s in steps)
            return {"status": "CLOSED", "code": "PROTECTION_FAILED_CLOSED", "steps": steps,
                    "halt": True}
        if scenario == "exception":
            steps = ["submit"]
            self.events.extend({"event": s} for s in steps)
            return {"status": "UNKNOWN", "code": "MOCK_EXCEPTION_HALT", "steps": steps,
                    "halt": True}
        return {"status": "REJECTED", "code": "UNKNOWN_SCENARIO", "steps": [], "halt": True}
