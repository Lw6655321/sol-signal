"""Controlled OKX transport. Live use still requires separate release approval.

Writes have one HTTP attempt. Reads distinguish absence from unknown outcomes.
Exchange fields are never filled from request assumptions to verify protection.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from . import okx_private as P
from .util import LOG_DIR, http_request


ALLOWED_INST = {"SOL-USDT-SWAP"}
NATIVE_LOG = LOG_DIR / "native_orders.csv"
FIELDS = ["ts_utc", "action", "instId", "clOrdId", "ok", "code", "msg", "resp"]
MAX_NOTIONAL_USDT = Decimal("400")
ALGO_TYPES = ("oco", "conditional", "trigger", "move_order_stop", "iceberg",
              "twap", "smart_iceberg", "chase")
TERMINAL = {"filled", "canceled", "mmp_canceled"}
READ_ATTEMPTS = 3


class NativeError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _audit(action: str, ok: bool, inst_id: str = "", cl_ord_id: str = "", *, code=""):
    """Allowlisted metadata only; never record raw replies or exception text."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    exists = NATIVE_LOG.exists() and NATIVE_LOG.stat().st_size > 0
    with NATIVE_LOG.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(dict(ts_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                             action=action, instId=inst_id, clOrdId=cl_ord_id,
                             ok=ok, code=code))


def _record(*args, **kwargs):
    # A failed audit must not turn a completed request into another write.
    try:
        _audit(*args, **kwargs)
    except OSError:
        pass


def _require_trading():
    if os.environ.get("OKX_TRADING_ENABLED") != "1":
        raise NativeError("TRADING_DISABLED")


def _check_inst(inst_id):
    if inst_id not in ALLOWED_INST:
        raise NativeError("INST_NOT_ALLOWED")


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9]{1,32}", value):
        raise NativeError("INVALID_ID")
    return value


def _number(value, *, positive=False, signed=False):
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        number = Decimal(str(value))
        if not number.is_finite() or (positive and number <= 0) or (not signed and number < 0):
            raise ValueError
        return number
    except (ValueError, InvalidOperation, TypeError):
        raise NativeError("INVALID_NUMBER") from None


def _request(method, path, params=None, body=None):
    """Sign with existing helpers, but never inherit private()'s POST retry."""
    method = method.upper()
    if method not in {"GET", "POST"}:
        raise NativeError("METHOD_NOT_ALLOWED")
    try:
        credentials = P.creds()
        if not all(credentials.get(k) for k in ("key", "secret", "passphrase")):
            raise NativeError("CREDENTIALS_UNAVAILABLE")
        request_path = path + ("?" + urllib.parse.urlencode(params) if params else "")
        body_text = json.dumps(body, separators=(",", ":")) if body is not None else ""
        stamp = P._ts_iso()
        headers = {"OK-ACCESS-KEY": credentials["key"],
                   "OK-ACCESS-SIGN": P._sign(credentials["secret"], stamp, method,
                                             request_path, body_text),
                   "OK-ACCESS-TIMESTAMP": stamp,
                   "OK-ACCESS-PASSPHRASE": credentials["passphrase"],
                   "Content-Type": "application/json"}
        if credentials.get("simulated"):
            headers["x-simulated-trading"] = "1"
        raw = http_request(P.BASE + request_path, method=method,
                           body=body_text.encode("utf-8") if body is not None else None,
                           headers=headers, timeout=5, retries=1 if method == "POST" else 2)
        payload = json.loads(raw.decode("utf-8"))
    except NativeError:
        raise
    except Exception:
        raise NativeError("REQUEST_UNKNOWN") from None
    if not isinstance(payload, dict):
        raise NativeError("INVALID_RESPONSE")
    if str(payload.get("code")) != "0":
        if method == "GET" and str(payload.get("code")) == "51603":
            raise NativeError("ORDER_NOT_FOUND")
        raise NativeError("API_REJECTED")
    data = payload.get("data")
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise NativeError("INVALID_RESPONSE")
    if method == "POST" and not data:
        raise NativeError("INVALID_RESPONSE")
    for row in data:
        if (method == "POST" and "sCode" not in row) or (
                "sCode" in row and str(row["sCode"]) != "0"):
            raise NativeError("ROW_REJECTED")
    return data


def _rows(action, method, path, params=None, body=None, *, inst_id="", cl_ord_id=""):
    try:
        data = _request(method, path, params, body)
    except NativeError as error:
        _record(action, False, inst_id, cl_ord_id, code=error.code)
        raise
    except Exception:
        _record(action, False, inst_id, cl_ord_id, code="REQUEST_UNKNOWN")
        raise NativeError("REQUEST_UNKNOWN") from None
    _record(action, True, inst_id, cl_ord_id, code="0")
    return data


def _call(action, method, path, params=None, body=None, *, inst_id="", cl_ord_id=""):
    rows = _rows(action, method, path, params, body, inst_id=inst_id, cl_ord_id=cl_ord_id)
    return rows[0] if rows else None


def get_order(inst_id, cl_ord_id="", *, ord_id=""):
    _check_inst(inst_id)
    params = {"instId": inst_id}
    params["ordId" if ord_id else "clOrdId"] = _id(ord_id or cl_ord_id)
    try:
        return _call("get_order", "GET", "/api/v5/trade/order", params,
                     inst_id=inst_id, cl_ord_id=cl_ord_id)
    except NativeError as error:
        if error.code == "ORDER_NOT_FOUND":
            return None
        raise


def get_algo_order(inst_id, algo_cl_ord_id="", *, algo_id=""):
    _check_inst(inst_id)
    params = {"algoId" if algo_id else "algoClOrdId": _id(algo_id or algo_cl_ord_id)}
    try:
        return _call("get_algo", "GET", "/api/v5/trade/order-algo",
                     params, inst_id=inst_id,
                     cl_ord_id=algo_cl_ord_id)
    except NativeError as error:
        if error.code == "ORDER_NOT_FOUND":
            return None
        raise


def _pending(action, path, inst_id, **params):
    _check_inst(inst_id)
    rows = _rows(action, "GET", path, {"instType": "SWAP", "instId": inst_id,
                                      "limit": "100", **params}, inst_id=inst_id)
    if len(rows) >= 100 or any(row.get("instId") != inst_id for row in rows):
        raise NativeError("PENDING_UNKNOWN")
    return rows


def pending_orders(inst_id):
    return _pending("pending_orders", "/api/v5/trade/orders-pending", inst_id)


def pending_algos(inst_id, ord_type="oco"):
    if ord_type not in ALGO_TYPES:
        raise NativeError("ALGO_TYPE_NOT_ALLOWED")
    return _pending("pending_algos", "/api/v5/trade/orders-algo-pending", inst_id,
                    ordType=ord_type)


def get_position(inst_id):
    _check_inst(inst_id)
    rows = _rows("positions", "GET", "/api/v5/account/positions",
                 {"instType": "SWAP", "instId": inst_id}, inst_id=inst_id)
    positions = []
    for row in rows:
        if row.get("instId") != inst_id:
            raise NativeError("POSITION_UNKNOWN")
        if _number(row.get("pos"), signed=True):
            if row.get("posSide") != "net":
                raise NativeError("POSITION_MODE_UNKNOWN")
            positions.append(row)
    if len(positions) > 1:
        raise NativeError("POSITION_UNKNOWN")
    return positions[0] if positions else None


def place_limit_order(inst_id, side, px, sz, cl_ord_id, post_only=True):
    _require_trading()
    _check_inst(inst_id)
    _id(cl_ord_id)
    if side not in {"buy", "sell"} or not isinstance(post_only, bool):
        raise NativeError("INVALID_SIDE_OR_TYPE")
    size, price = _number(sz, positive=True), _number(px, positive=True)
    # SOL-USDT-SWAP contract value is 1 SOL in this project's fixed whitelist.
    # This is an independent hard ceiling, even outside OrderRouter.
    if size * price > MAX_NOTIONAL_USDT:
        raise NativeError("NOTIONAL_LIMIT")
    body = dict(instId=inst_id, tdMode="cross", side=side,
                ordType="post_only" if post_only else "limit",
                sz=str(size), px=str(price),
                clOrdId=cl_ord_id)
    return _call("place_limit", "POST", "/api/v5/trade/order", body=body,
                 inst_id=inst_id, cl_ord_id=cl_ord_id)


def cancel_order(inst_id, cl_ord_id="", *, ord_id=""):
    _require_trading()
    _check_inst(inst_id)
    body = {"instId": inst_id, "ordId" if ord_id else "clOrdId": _id(ord_id or cl_ord_id)}
    return _call("cancel_order", "POST", "/api/v5/trade/cancel-order", body=body,
                 inst_id=inst_id, cl_ord_id=cl_ord_id)


def place_oco_close(inst_id, close_side, sz, sl_px, tp_px, algo_cl_ord_id):
    _require_trading()
    _check_inst(inst_id)
    _id(algo_cl_ord_id)
    if close_side not in {"buy", "sell"}:
        raise NativeError("SIDE_INVALID")
    body = dict(instId=inst_id, tdMode="cross", side=close_side, ordType="oco",
                sz=str(_number(sz, positive=True)), reduceOnly=True,
                slTriggerPx=str(_number(sl_px, positive=True)), slOrdPx="-1",
                tpTriggerPx=str(_number(tp_px, positive=True)), tpOrdPx="-1",
                algoClOrdId=algo_cl_ord_id)
    return _call("place_oco", "POST", "/api/v5/trade/order-algo", body=body,
                 inst_id=inst_id, cl_ord_id=algo_cl_ord_id)


def amend_oco_size(inst_id, algo_id, qty, req_id):
    _require_trading()
    _check_inst(inst_id)
    body = dict(instId=inst_id, algoId=_id(algo_id), newSz=str(_number(qty, positive=True)),
                cxlOnFail=False, reqId=_id(req_id))
    return _call("amend_oco", "POST", "/api/v5/trade/amend-algos", body=body,
                 inst_id=inst_id)


def cancel_algo(inst_id, algo_id):
    _require_trading()
    _check_inst(inst_id)
    return _call("cancel_algo", "POST", "/api/v5/trade/cancel-algos",
                 body=[dict(instId=inst_id, algoId=_id(algo_id))], inst_id=inst_id)


def market_close(inst_id, side, sz, cl_ord_id):
    _require_trading()
    _check_inst(inst_id)
    _id(cl_ord_id)
    if side not in {"buy", "sell"}:
        raise NativeError("SIDE_INVALID")
    body = dict(instId=inst_id, tdMode="cross", side=side, ordType="market",
                sz=str(_number(sz, positive=True)), reduceOnly=True, clOrdId=cl_ord_id)
    return _call("market_close", "POST", "/api/v5/trade/order", body=body,
                 inst_id=inst_id, cl_ord_id=cl_ord_id)


def _wait_order(inst_id, client_id="", *, ord_id="", close_side=None, qty=None):
    previous_fill = Decimal("0")
    for attempt in range(READ_ATTEMPTS):
        row = get_order(inst_id, client_id, ord_id=ord_id)
        if row is not None:
            if (row.get("instId") != inst_id or not row.get("ordId")
                    or (ord_id and str(row["ordId"]) != ord_id)
                    or (client_id and row.get("clOrdId") != client_id)):
                raise NativeError("ORDER_MISMATCH")
            size = _number(row.get("sz"), positive=True)
            filled = _number(row.get("accFillSz"))
            if filled > size or filled < previous_fill or (row.get("state") == "filled" and filled != size):
                raise NativeError("INVALID_SNAPSHOT")
            previous_fill = filled
            if close_side is None and row.get("state") in TERMINAL:
                return row
            if (close_side is not None and row.get("state") == "filled"
                    and row.get("side") == close_side and row.get("posSide") == "net"
                    and row.get("ordType") == "market"
                    and str(row.get("reduceOnly", "")).lower() == "true"
                    and size == qty and filled == qty):
                return row
        if attempt + 1 < READ_ATTEMPTS:
            time.sleep(0.1)
    raise NativeError("ORDER_UNCONFIRMED")


def _all_algos(inst_id):
    rows = []
    for kind in ALGO_TYPES:
        rows.extend(pending_algos(inst_id, kind))
    return rows


def _wait_algo_terminal(inst_id, algo_id, *, reduce_only):
    for attempt in range(READ_ATTEMPTS):
        row = get_algo_order(inst_id, algo_id=algo_id)
        if row is not None:
            if row.get("instId") != inst_id or str(row.get("algoId")) != algo_id:
                raise NativeError("ALGO_MISMATCH")
            if row.get("state") in {"canceled", "order_failed"}:
                return
            # A reduce-only strategy that already triggered cannot reopen a
            # position. Opening strategies require explicit cancel confirmation.
            if reduce_only and row.get("state") == "effective":
                return
        if attempt + 1 < READ_ATTEMPTS:
            time.sleep(0.1)
    raise NativeError("ALGO_CANCEL_UNCONFIRMED")


def _cancel_ordinary(inst_id, out):
    for order in pending_orders(inst_id):
        order_id = _id(str(order.get("ordId") or ""))
        client = str(order.get("clOrdId") or "")
        if client:
            _id(client)
        try:
            cancel_order(inst_id, client, ord_id=order_id)
        except NativeError:
            pass  # A timeout/rejection is not terminal; independently query.
        _wait_order(inst_id, client, ord_id=order_id)
        out["cancelled_orders"] += 1


def flatten(inst_id="SOL-USDT-SWAP"):
    """Keep protection until entry orders are terminal and reduction is verified.

    Each mutation is attempted once; unknown outcomes require reconciliation.
    ``flat`` means positions AND ordinary AND every supported algo list are empty.
    Unsupported/unavailable list queries deliberately prevent a success claim.
    """
    _require_trading()
    _check_inst(inst_id)
    out = dict(cancelled_algos=0, cancelled_orders=0, closed=None, flat=False,
               code="UNCONFIRMED", unknown=True)
    try:
        _cancel_ordinary(inst_id, out)
        algos = _all_algos(inst_id)
        # An omitted role is unknown, never proof that an algo is protection.
        if any(str(a.get("reduceOnly", "")).lower() not in {"true", "false"} for a in algos):
            raise NativeError("ALGO_ROLE_UNKNOWN")
        opening_algos = [a for a in algos if str(a["reduceOnly"]).lower() == "false"]
        for algo in opening_algos:
            algo_id = _id(str(algo.get("algoId") or ""))
            try:
                cancel_algo(inst_id, algo_id)
            except NativeError:
                pass
            _wait_algo_terminal(inst_id, algo_id, reduce_only=False)
            out["cancelled_algos"] += 1
        if opening_algos:
            _cancel_ordinary(inst_id, out)  # Any child generated before cancellation.
        if pending_orders(inst_id):
            raise NativeError("ENTRY_REMAINS")
        if any(str(a.get("reduceOnly", "")).lower() != "true" for a in _all_algos(inst_id)):
            raise NativeError("ENTRY_ALGO_REMAINS")
        position = get_position(inst_id)
        if position is not None:
            size = _number(position.get("pos"), signed=True)
            side = "sell" if size > 0 else "buy"
            client = "FLAT" + uuid.uuid4().hex[:28]
            response = None
            try:
                response = market_close(inst_id, side, abs(size), client)
            except NativeError:
                pass  # Query this same id; never send a second market close.
            close_id = str((response or {}).get("ordId") or "")
            closed = _wait_order(inst_id, client, ord_id=close_id,
                                 close_side=side, qty=abs(size))
            out["closed"] = dict(ordId=str(closed["ordId"]), clOrdId=client)
        if get_position(inst_id) is not None or pending_orders(inst_id):
            raise NativeError("ACCOUNT_NOT_FLAT")
        for algo in _all_algos(inst_id):
            if str(algo.get("reduceOnly", "")).lower() != "true":
                raise NativeError("ENTRY_ALGO_REMAINS")
            algo_id = _id(str(algo.get("algoId") or ""))
            try:
                cancel_algo(inst_id, algo_id)
            except NativeError:
                pass
            _wait_algo_terminal(inst_id, algo_id, reduce_only=True)
            out["cancelled_algos"] += 1
        # Check positions last, after confirming every source of future entry
        # has disappeared. Any failed query leaves flat=False.
        if _all_algos(inst_id):
            raise NativeError("ALGO_REMAINS")
        if pending_orders(inst_id) or get_position(inst_id) is not None:
            raise NativeError("ACCOUNT_NOT_FLAT")
        out.update(flat=True, code="OK", unknown=False)
    except NativeError as error:
        out["code"] = error.code
    except Exception:
        out["code"] = "FLATTEN_UNKNOWN"
    return out


def _norm_order(order):
    return dict(order) if isinstance(order, dict) else None


def _norm_algo(algo, protection_id):
    # protection_id is a correlation requirement, never a missing-field default.
    return dict(algo) if isinstance(algo, dict) else None


def _valid_protection(row, intent, protection_id, *, qty=None, algo_id=""):
    if not isinstance(row, dict):
        return False
    if (row.get("instId") != intent.inst_id or row.get("algoClOrdId") != protection_id
            or not row.get("algoId") or (algo_id and str(row["algoId"]) != algo_id)
            or row.get("state") != "live" or row.get("ordType") != "oco"
            or row.get("side") != ("sell" if intent.side == "long" else "buy")
            or row.get("posSide") != "net" or str(row.get("slOrdPx")) != "-1"
            or str(row.get("tpOrdPx")) != "-1"
            or str(row.get("reduceOnly", "")).lower() != "true"):
        return False
    try:
        size = _number(row.get("sz"), positive=True)
        return ((qty is None or size == qty)
                and _number(row.get("slTriggerPx"), positive=True) == intent.stop
                and _number(row.get("tpTriggerPx"), positive=True) == intent.tp1)
    except NativeError:
        return False


class NativeOkxTransport:
    mode = "native-live"

    def __init__(self):
        self._protection_attempted = set()

    def submit_entry(self, intent, cl_ord_id):
        row = place_limit_order(intent.inst_id, "buy" if intent.side == "long" else "sell",
                                intent.entry, intent.qty, cl_ord_id,
                                post_only=intent.require_maker)
        return {"sCode": "0", "ordId": str(row.get("ordId") or "")}

    def get_order(self, inst_id, cl_ord_id):
        # Router owns bounded read retries; avoid multiplying nested retry loops.
        return _norm_order(get_order(inst_id, cl_ord_id))

    def cancel_entry(self, inst_id, cl_ord_id):
        return cancel_order(inst_id, cl_ord_id)

    def set_protection(self, intent, qty, protection_id, *, reduce_only):
        if reduce_only is not True:
            raise NativeError("REDUCE_ONLY_REQUIRED")
        _id(protection_id)
        qty = _number(qty, positive=True)
        existing = get_algo_order(intent.inst_id, protection_id)
        algo_id = ""
        if existing is not None:
            if not _valid_protection(existing, intent, protection_id):
                raise NativeError("PROTECTION_MISMATCH")
            algo_id = str(existing["algoId"])
            if _number(existing["sz"], positive=True) == qty:
                return {"sCode": "0", "algoId": algo_id}
        elif protection_id in self._protection_attempted:
            raise NativeError("PROTECTION_UNKNOWN")
        self._protection_attempted.add(protection_id)
        try:
            if algo_id:
                req_id = "a" + hashlib.sha256((protection_id + ":" + str(qty)).encode()).hexdigest()[:31]
                result = amend_oco_size(intent.inst_id, algo_id, qty, req_id)
            else:
                result = place_oco_close(intent.inst_id,
                                         "sell" if intent.side == "long" else "buy",
                                         qty, intent.stop, intent.tp1, protection_id)
            returned_id = str(result.get("algoId") or "")
            if not returned_id or (algo_id and returned_id != algo_id):
                raise NativeError("PROTECTION_MISMATCH")
            algo_id = returned_id
        except NativeError:
            # A POST timeout is uncertain, not permission to create another OCO.
            pass
        observed = get_algo_order(intent.inst_id, protection_id)
        if not _valid_protection(observed, intent, protection_id, qty=qty, algo_id=algo_id):
            raise NativeError("PROTECTION_UNCONFIRMED")
        return {"sCode": "0", "algoId": str(observed["algoId"])}

    def get_protection(self, inst_id, protection_id):
        return _norm_algo(get_algo_order(inst_id, protection_id), protection_id)

    def emergency_close(self, inst_id, side, qty, cl_ord_id, *, reduce_only):
        if reduce_only is not True:
            raise NativeError("REDUCE_ONLY_REQUIRED")
        row = market_close(inst_id, side, qty, cl_ord_id)
        return {"sCode": "0", "ordId": str(row.get("ordId") or "")}
