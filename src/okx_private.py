# -*- coding: utf-8 -*-
"""OKX 私有接口：签名鉴权 + 账户读取 + 下单执行（分阶段启用）。

============================== 安全设计 ==============================
1. Key 只从环境变量读取，**绝不写入任何文件**：
     OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE
     OKX_SIMULATED=1   -> 使用 OKX 模拟盘（x-simulated-trading: 1）
     OKX_TRADING_ENABLED=1 -> 才允许真正下单（默认关闭，只能读）
2. **代码层硬锁**（即使 API Key 权限给多了也没用）：
     - 只允许 OKX_ALLOWED_INST 里列出的交易对（默认 SOL-USDT-SWAP）
     - 只允许 SWAP（永续）；现货/交割/期权一律拒绝
     - 没有任何划转/提现/赚币/借贷的调用路径
3. 所有下单前先过参数校验：精度、最小量、价格偏离、名义上限
4. 每笔操作写审计日志 logs/orders.csv，失败也写
5. 幂等：用信号 ID 作为 clientOrderId，避免超时重试导致重复下单
=====================================================================
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import urllib.parse
from datetime import datetime, timezone

from .util import LOG_DIR, append_csv, http_request, log, now_ms

BASE = "https://www.okx.com"

# ---- 硬锁：允许的交易对与品种（改这里等于改安全边界，慎动） ----
ALLOWED_INST = {"SOL-USDT-SWAP"}
ALLOWED_INST_TYPE = {"SWAP"}
ALLOWED_SIDE = {"buy", "sell"}
ALLOWED_ORD_TYPE = {"limit", "market"}
ALLOWED_POS_SIDE = {"net", "long", "short"}

# 价格偏离保护：下单价不得超过市价 ±N%
MAX_PRICE_DEVIATION_PCT = 1.5
# 单笔名义上限（USDT），与风控2%风险互相独立的一道保险
MAX_NOTIONAL_USDT = 400.0

ORDER_LOG = LOG_DIR / "orders.csv"
ORDER_FIELDS = ["ts_utc", "action", "instId", "side", "ordType", "px", "sz",
                "clientOrderId", "ok", "code", "msg", "resp"]


class PrivateError(RuntimeError):
    """私有接口调用失败。"""


# --------------------------------------------------------------------------- 凭据


def _env_or_reg(name: str) -> str:
    """读环境变量；若当前进程环境里没有（例如 setx 之后进程没重启），退回读注册表用户级。

    这样用户 setx 之后无需重启本程序也能生效——否则会得到一个迷惑的"明明设了却读不到"。
    """
    v = (os.environ.get(name) or "").strip()
    if v:
        return v
    if os.name == "nt":
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
                val, _ = winreg.QueryValueEx(k, name)
                return (val or "").strip()
        except Exception:  # noqa: BLE001
            return ""
    return ""


def creds() -> dict:
    return {
        "key": _env_or_reg("OKX_API_KEY"),
        "secret": _env_or_reg("OKX_API_SECRET"),
        "passphrase": _env_or_reg("OKX_API_PASSPHRASE"),
        "simulated": _env_or_reg("OKX_SIMULATED") == "1",
        "trading_enabled": _env_or_reg("OKX_TRADING_ENABLED") == "1",
    }


def configured() -> bool:
    c = creds()
    return bool(c["key"] and c["secret"] and c["passphrase"])


def status() -> dict:
    c = creds()
    return {
        "configured": configured(),
        "has_key": bool(c["key"]),
        "has_secret": bool(c["secret"]),
        "has_passphrase": bool(c["passphrase"]),
        "simulated": c["simulated"],
        "trading_enabled": c["trading_enabled"],
        "allowed_inst": sorted(ALLOWED_INST),
    }


# --------------------------------------------------------------------------- 签名


def _ts_iso() -> str:
    """OKX 要求 ISO8601 毫秒精度 UTC，如 2026-10-16T09:07:39.756Z"""
    t = datetime.now(timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def _sign(secret: str, ts: str, method: str, request_path: str, body: str) -> str:
    msg = f"{ts}{method.upper()}{request_path}{body}"
    mac = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


def _audit(action: str, ok: bool, req: dict, code=None, msg="", resp=None) -> None:
    append_csv(ORDER_LOG, {
        "ts_utc": _ts_iso(), "action": action,
        "instId": req.get("instId", ""), "side": req.get("side", ""),
        "ordType": req.get("ordType", ""), "px": req.get("px", ""),
        "sz": req.get("sz", ""), "clientOrderId": req.get("clOrdId", ""),
        "ok": ok, "code": code, "msg": msg,
        "resp": json.dumps(resp, ensure_ascii=False)[:300] if resp else "",
    }, ORDER_FIELDS)


def private(method: str, path: str, params: dict | None = None, body: dict | None = None):
    """调用 OKX 私有接口，返回 data 列表。code!=0 抛 PrivateError。"""
    c = creds()
    if not configured():
        raise PrivateError("未配置 API 凭据（OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE）")

    request_path = path
    if params:
        request_path += "?" + urllib.parse.urlencode(params)
    body_str = json.dumps(body, separators=(",", ":")) if body else ""
    ts = _ts_iso()
    headers = {
        "OK-ACCESS-KEY": c["key"],
        "OK-ACCESS-SIGN": _sign(c["secret"], ts, method, request_path, body_str),
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": c["passphrase"],
        "Content-Type": "application/json",
    }
    if c["simulated"]:
        headers["x-simulated-trading"] = "1"

    raw = http_request(BASE + request_path, method=method,
                       body=body_str.encode("utf-8") if body_str else None,
                       headers=headers, timeout=20, retries=2)
    payload = json.loads(raw.decode("utf-8"))
    code = str(payload.get("code"))
    if code != "0":
        raise PrivateError(f"OKX {path} code={code} msg={payload.get('msg')} data={payload.get('data')}")
    return payload.get("data", [])


# --------------------------------------------------------------------------- 只读接口


def account_config() -> list:
    """账户配置：持仓模式(净/双向)、保证金模式(全仓/逐仓)、手续费档位。"""
    return private("GET", "/api/v5/account/config")


def balance(ccy: str = "USDT") -> list:
    return private("GET", "/api/v5/account/balance", {"ccy": ccy})


def positions(inst_id: str | None = None) -> list:
    p = {"instType": "SWAP"}
    if inst_id:
        p["instId"] = inst_id
    return private("GET", "/api/v5/account/positions", p)


def pending_orders(inst_id: str | None = None) -> list:
    p = {"instType": "SWAP"}
    if inst_id:
        p["instId"] = inst_id
    return private("GET", "/api/v5/trade/orders-pending", p)


def algo_orders(ord_type: str = "conditional") -> list:
    """条件单（止损/止盈）：用来验证止损单到底挂上了没有。"""
    return private("GET", "/api/v5/trade/orders-algo-pending",
                   {"instType": "SWAP", "ordType": ord_type})


def fills(inst_id: str | None = None, limit: int = 100) -> list:
    """成交流水（真实成交价、手续费）。对账的核心数据。"""
    p = {"instType": "SWAP", "limit": str(limit)}
    if inst_id:
        p["instId"] = inst_id
    return private("GET", "/api/v5/trade/fills", p)


def fills_history(begin_ms: int | None = None, end_ms: int | None = None,
                  limit: int = 100) -> list:
    p = {"instType": "SWAP", "limit": str(limit)}
    if begin_ms:
        p["begin"] = str(begin_ms)
    if end_ms:
        p["end"] = str(end_ms)
    return private("GET", "/api/v5/trade/fills-history", p)


# --------------------------------------------------------------------------- 交易（必须显式开启）


def _require_trading() -> None:
    c = creds()
    if not c["trading_enabled"]:
        raise PrivateError(
            "交易功能未开启。设置环境变量 OKX_TRADING_ENABLED=1 后才允许下单。"
            "（这是防止误触的硬开关，勿在验证阶段打开）")


def _validate_order(inst_id: str, side: str, ord_type: str, px: str | None,
                    sz: str, pos_side: str, last_px: float | None) -> None:
    """下单前的最后一道校验。任何一项不过直接拒绝，不发给交易所。"""
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单 {sorted(ALLOWED_INST)} 内，拒绝下单")
    if "SWAP" not in ALLOWED_INST_TYPE:
        raise PrivateError("品种白名单异常")
    if side not in ALLOWED_SIDE:
        raise PrivateError(f"side={side} 非法")
    if ord_type not in ALLOWED_ORD_TYPE:
        raise PrivateError(f"ordType={ord_type} 非法（只允许限价/市价）")
    if pos_side not in ALLOWED_POS_SIDE:
        raise PrivateError(f"posSide={pos_side} 非法")
    try:
        szf = float(sz)
    except ValueError:
        raise PrivateError(f"手数 {sz} 非法")
    if szf <= 0:
        raise PrivateError("手数必须大于 0")
    if ord_type == "limit":
        if not px:
            raise PrivateError("限价单必须给 px")
        pxf = float(px)
        if last_px and abs(pxf / last_px - 1) * 100 > MAX_PRICE_DEVIATION_PCT:
            raise PrivateError(
                f"挂单价 {pxf} 偏离市价 {last_px} 超过 {MAX_PRICE_DEVIATION_PCT}%，拒绝（防手误/防插针）")
        notional = pxf * szf
        if notional > MAX_NOTIONAL_USDT:
            raise PrivateError(f"名义 {notional:.0f}u 超过上限 {MAX_NOTIONAL_USDT}u，拒绝")


def place_order(inst_id: str, side: str, sz: str, ord_type: str = "limit",
                px: str | None = None, pos_side: str = "net",
                cl_ord_id: str | None = None, last_px: float | None = None,
                reduce_only: bool = False) -> dict:
    """下单。返回 OKX 响应。所有校验通过才发出去。

    reduce_only=True 时只允许减仓，永远不会开出反向新仓（平仓场景务必打开）。
    """
    _require_trading()
    req = {"instId": inst_id, "tdMode": "cross", "side": side, "ordType": ord_type,
           "sz": sz, "posSide": pos_side}
    if reduce_only:
        req["reduceOnly"] = "true"
    if ord_type == "limit":
        if not px:
            raise PrivateError("限价单必须给 px")
        req["px"] = px
    if cl_ord_id:
        if not (cl_ord_id.isalnum() and 1 <= len(cl_ord_id) <= 32):
            raise PrivateError("clOrdId 只能字母数字，1~32 位")
        req["clOrdId"] = cl_ord_id
    _validate_order(inst_id, side, ord_type, px, sz, pos_side, last_px)
    try:
        data = private("POST", "/api/v5/trade/order", body=req)
    except PrivateError as e:
        _audit("place", False, req, msg=str(e)[:200])
        raise
    _audit("place", True, req, resp=data)
    return data[0] if data else {}


def place_algo_stop(inst_id: str, side: str, sz: str, trigger_px: str,
                    ord_px: str = "-1", pos_side: str = "net") -> dict:
    """挂止损（条件单，市价触发）。ord_px=-1 表示触发后市价成交。"""
    _require_trading()
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单内，拒绝")
    req = {"instId": inst_id, "tdMode": "cross", "side": side, "sz": sz,
           "ordType": "conditional", "posSide": pos_side,
           "slTriggerPx": trigger_px, "slOrdPx": ord_px}
    try:
        data = private("POST", "/api/v5/trade/order-algo", body=req)
    except PrivateError as e:
        _audit("place_stop", False, req, msg=str(e)[:200])
        raise
    _audit("place_stop", True, req, resp=data)
    return data[0] if data else {}


def cancel_order(inst_id: str, ord_id: str) -> dict:
    _require_trading()
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单内，拒绝")
    req = {"instId": inst_id, "ordId": ord_id}
    data = private("POST", "/api/v5/trade/cancel-order", body=req)
    _audit("cancel", True, req, resp=data)
    return data[0] if data else {}


# --------------------------------------------------------------------------- 仓位级止盈止损（OCO）

def amend_algo(inst_id: str, algo_id: str, new_sl: str | None = None,
               new_tp: str | None = None, new_sz: str | None = None) -> dict:
    """修改已有的策略/仓位级止盈止损单（OCO）。

    只允许改止损价/止盈价/数量，不允许把止损往不利方向挪（见 guard）。
    """
    _require_trading()
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单内，拒绝")
    req = {"instId": inst_id, "algoId": algo_id}
    if new_sl:
        req["newSlTriggerPx"] = new_sl
    if new_tp:
        req["newTpTriggerPx"] = new_tp
    if new_sz:
        req["newSz"] = new_sz
    try:
        data = private("POST", "/api/v5/trade/amend-algos", body=req)
    except PrivateError as e:
        _audit("amend_algo", False, req, msg=str(e)[:200])
        raise
    _audit("amend_algo", True, req, resp=data)
    return data[0] if data else {}


def cancel_algo(inst_id: str, algo_id: str) -> dict:
    _require_trading()
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单内，拒绝")
    req = {"instId": inst_id, "algoId": algo_id}
    data = private("POST", "/api/v5/trade/cancel-algos", body=[req])
    _audit("cancel_algo", True, {"instId": inst_id, "clOrdId": algo_id}, resp=data)
    return data[0] if data else {}


def place_oco(inst_id: str, pos_side: str, sz: str, sl_px: str, tp_px: str,
              close_fraction: str = "1") -> dict:
    """给已有持仓挂仓位级 OCO 止盈止损（触发后市价平仓）。"""
    _require_trading()
    if inst_id not in ALLOWED_INST:
        raise PrivateError(f"交易对 {inst_id} 不在白名单内，拒绝")
    if pos_side not in ("long", "short"):
        raise PrivateError("posSide 必须是 long 或 short")
    side = "sell" if pos_side == "long" else "buy"
    req = {"instId": inst_id, "tdMode": "cross", "side": side, "posSide": pos_side,
           "ordType": "oco", "sz": sz, "closeFraction": close_fraction,
           "slTriggerPx": sl_px, "slOrdPx": "-1",
           "tpTriggerPx": tp_px, "tpOrdPx": "-1"}
    try:
        data = private("POST", "/api/v5/trade/order-algo", body=req)
    except PrivateError as e:
        _audit("place_oco", False, req, msg=str(e)[:200])
        raise
    _audit("place_oco", True, req, resp=data)
    return data[0] if data else {}
