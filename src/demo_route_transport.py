"""Explicitly enabled SOL demo transport; never imports a live credential path.

POSTs have one network attempt. The router's persistent journal owns cross-process
idempotency; this adapter also refuses repeated or conflicting in-process writes.
Exchange snapshots are returned without inventing missing verification fields.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import threading
import time
import urllib.request
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Callable, Mapping

from .demo_transport import (
    CREDENTIAL_NAMES, MAX_BODY, MAX_TIMEOUT, DemoReadOnlyClient,
    DemoTransportError, _NoRedirect, load_demo_credentials,
)
from .execution import Intent


BASE = "https://openapi.okx.com"
INST = "SOL-USDT-SWAP"
MAX_NOTIONAL = Decimal("400")
POST_PATHS = frozenset({"/api/v5/trade/order", "/api/v5/trade/cancel-order",
                        "/api/v5/trade/order-algo", "/api/v5/trade/amend-algos",
                        "/api/v5/trade/cancel-algos"})
ALGO_TYPES = ("oco", "conditional", "trigger", "move_order_stop", "iceberg",
              "twap", "smart_iceberg", "chase")
AUDIT_FIELDS = ("timestamp_utc", "mode", "action", "instId", "client_id",
                "ok", "code", "numeric")
LIFECYCLE_EVIDENCE_SECONDS = 5
LIFECYCLE_LOOKBACK_MS = 28 * 86_400_000


class DemoRouteError(RuntimeError):
    """Fixed error codes only; raw server/error text never reaches callers."""

    def __init__(self, code: str, numeric: int | None = None):
        self.code, self.numeric = code, numeric
        super().__init__(code)


class DemoEntryNotSent(DemoRouteError):
    """Only a local admission refusal before entry HTTP may use this type."""


def _id(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9]{1,32}", value):
        raise DemoRouteError("DEMO_INVALID_ID")
    return value


def _number(value: object, *, positive: bool = False, signed: bool = False) -> Decimal:
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        result = Decimal(str(value))
        if not result.is_finite() or (not signed and result < 0) or (positive and result <= 0):
            raise ValueError
        return result
    except (ValueError, TypeError, InvalidOperation):
        raise DemoRouteError("DEMO_INVALID_NUMBER") from None


def _numeric(value: object) -> int | None:
    text = str(value) if value is not None else ""
    return int(text) if text.isascii() and text.isdigit() and 1 <= len(text) <= 8 else None


def _inst(value: object):
    if value != INST:
        raise DemoRouteError("DEMO_INSTRUMENT")


def _millisecond(value: object) -> int:
    number = _number(value, positive=True)
    if number != number.to_integral_value():
        raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
    return int(number)


def _real_http_post(url: str, headers: dict, body: bytes, timeout: int) -> tuple[int, object]:
    """A single bounded request; no retry middleware and no redirect following."""
    if not isinstance(url, str) or url not in {BASE + path for path in POST_PATHS}:
        raise DemoRouteError("DEMO_PATH_NOT_ALLOWED")
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    opener = urllib.request.build_opener(_NoRedirect)
    with opener.open(request, timeout=timeout) as response:
        status = getattr(response, "status", None)
        raw = response.read(MAX_BODY + 1)
    if not isinstance(raw, (bytes, bytearray)):
        raise DemoRouteError("DEMO_BAD_RESPONSE")
    if len(raw) > MAX_BODY:
        raise DemoRouteError("DEMO_RESPONSE_TOO_LARGE")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise DemoRouteError("DEMO_BAD_JSON") from None
    return status, payload


class DemoRouteTransport:
    """Only demo credentials and the fixed simulated-trading header are used.

    ``http_get`` / ``http_post`` are injectable for offline contract tests. They
    receive (url, headers, timeout) / (url, headers, body_bytes, timeout). No global
    trading or simulation environment variable is consulted or changed.
    """

    mode = "demo"

    def __init__(self, credentials: dict | None = None, *, enable_writes: bool = False,
                 http_get: Callable | None = None, http_post: Callable | None = None,
                 log_root: str | Path = "logs", timeout: int = MAX_TIMEOUT):
        if not isinstance(enable_writes, bool):
            raise DemoRouteError("DEMO_ENABLE_BOOLEAN_REQUIRED")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= MAX_TIMEOUT:
            raise DemoRouteError("DEMO_TIMEOUT")
        creds = load_demo_credentials() if credentials is None else credentials
        if (not isinstance(creds, dict) or set(creds) != set(CREDENTIAL_NAMES)
                or any(not isinstance(creds.get(name), str) or not creds[name].strip()
                       for name in CREDENTIAL_NAMES)):
            raise DemoRouteError("DEMO_CREDENTIALS_INCOMPLETE")
        self._reader = DemoReadOnlyClient(dict(creds), http_get=http_get,
                                        base=BASE, timeout=timeout)
        self._enabled = enable_writes
        self._timeout = timeout
        self._http_post = http_post or _real_http_post
        self._attempts: dict[str, tuple[str, Mapping | None]] = {}
        self._cancel_protection_lock = threading.Lock()
        root = Path(log_root) / "demo-transport"
        root.mkdir(parents=True, exist_ok=True)
        self.run_dir = root / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-")
                               + uuid.uuid4().hex)
        self.run_dir.mkdir(exist_ok=False)
        self.audit_path = self.run_dir / "account-ops.csv"
        with self.audit_path.open("x", encoding="utf-8", newline="") as handle:
            csv.DictWriter(handle, fieldnames=AUDIT_FIELDS).writeheader()

    @property
    def enable_writes(self) -> bool:
        return self._enabled

    def assert_demo_enabled(self):
        if self._enabled is not True:
            raise DemoRouteError("DEMO_WRITES_DISABLED")
        if (type(self._reader) is not DemoReadOnlyClient or BASE != "https://openapi.okx.com"
                or self.mode != "demo"):
            raise DemoRouteError("DEMO_ISOLATION_INVALID")
        headers = self._reader._headers("GET", "/api/v5/account/config", "")
        if headers.get("x-simulated-trading") != "1":
            raise DemoRouteError("DEMO_ISOLATION_INVALID")

    def _audit(self, action: str, client_id: str, ok: bool, code: str,
               numeric: int | None = None):
        # Audit failures must not cause a completed POST to be sent a second time.
        try:
            with self.audit_path.open("a", encoding="utf-8", newline="") as handle:
                csv.DictWriter(handle, fieldnames=AUDIT_FIELDS).writerow(dict(
                    timestamp_utc=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    mode="demo", action=action, instId=INST, client_id=client_id,
                    ok=ok, code=code, numeric=numeric))
        except OSError:
            pass

    def _get(self, path: str, params: dict, *, absent: bool = False,
             _deadline: float | None = None) -> list[dict]:
        reader = self._reader
        if _deadline is not None:
            remaining = _deadline - time.monotonic()
            if remaining <= 0:
                raise DemoRouteError("DEMO_LIFECYCLE_STALE")
            # A separate immutable client view avoids changing a shared reader's
            # timeout while independent GETs run concurrently.
            reader = DemoReadOnlyClient(dict(self._reader._creds),
                        http_get=self._reader._http_get, base=BASE,
                        timeout=min(self._timeout, max(1, math.ceil(remaining))))
        try:
            rows = reader.get(path, params)
        except DemoTransportError as error:
            if absent and error.numeric == 51603:
                return []
            raise DemoRouteError("DEMO_READ_UNKNOWN", error.numeric) from None
        except Exception:
            raise DemoRouteError("DEMO_READ_UNKNOWN") from None
        if (not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows)
                or any("sCode" in row and str(row["sCode"]) != "0" for row in rows)):
            raise DemoRouteError("DEMO_BAD_RESPONSE")
        if _deadline is not None and time.monotonic() > _deadline:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        return deepcopy(rows)

    @staticmethod
    def _parallel_gets(jobs: Mapping, _deadline: float | None = None) -> dict:
        """Bounded independent reads only. Failed waves drain before returning."""
        if len(jobs) > 10:
            raise DemoRouteError("DEMO_READ_SCOPE_INVALID")
        with ThreadPoolExecutor(max_workers=max(1, len(jobs)), thread_name_prefix="demo-read") as pool:
            futures = {pool.submit(job): name for name, job in jobs.items()}
            results = {}
            try:
                for future in as_completed(futures):
                    results[futures[future]] = future.result()
            except Exception:
                for future in futures:
                    future.cancel()
                raise
        if _deadline is not None and time.monotonic() > _deadline:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        return results

    def _post(self, action: str, identity: str, path: str,
              body: dict | list[dict], *, before_send: Callable | None = None) -> Mapping:
        self.assert_demo_enabled()
        if path not in POST_PATHS:
            raise DemoRouteError("DEMO_PATH_NOT_ALLOWED")
        if before_send is not None and (action != "entry" or not callable(before_send)):
            raise DemoRouteError("DEMO_ENTRY_GUARD_INVALID")
        # Only this reviewed cancellation operation uses OKX's array body. It
        # cannot become a batch/general POST escape hatch or alter existing
        # dictionary request fingerprints.
        if path == "/api/v5/trade/cancel-algos":
            if (action != "cancel_protection" or not isinstance(body, list)
                    or len(body) != 1 or not isinstance(body[0], dict)
                    or set(body[0]) != {"instId", "algoId"}
                    or body[0].get("instId") != INST):
                raise DemoRouteError("DEMO_CANCEL_SCOPE_INVALID")
            _id(body[0]["algoId"])
        elif not isinstance(body, dict):
            raise DemoRouteError("DEMO_BAD_REQUEST")
        body_text = json.dumps(body, separators=(",", ":"), sort_keys=True, allow_nan=False)
        fingerprint = hashlib.sha256((path + body_text).encode()).hexdigest()
        key = action + ":" + identity
        if key in self._attempts:
            previous, ack = self._attempts[key]
            if previous != fingerprint:
                raise DemoRouteError("DEMO_IDEMPOTENCY_CONFLICT")
            if ack is None:
                raise DemoRouteError("DEMO_WRITE_UNKNOWN")
            return deepcopy(ack)
        headers = self._reader._headers("POST", path, body_text)
        if headers.get("x-simulated-trading") != "1":
            raise DemoRouteError("DEMO_ISOLATION_INVALID")
        headers["Content-Type"] = "application/json"
        self._audit(action, identity, False, "REQUEST")
        body_bytes = body_text.encode()
        # This is entry-only: stale entry evidence must never block a protective
        # order, cancellation or reduce-only exit. Sign/audit first so their
        # elapsed time is included; no account/network operation happens here.
        if before_send is not None:
            try:
                before_send()
            except Exception as error:
                code = getattr(error, "code", None)
                if code not in {"DEMO_STATE_STALE", "MARKET_STALE", "EXPIRED",
                                "DEMO_ENTRY_CLOCK_INVALID", "DEMO_ENTRY_ADMISSION_FAILED",
                                "MACRO_BLACKOUT", "DEMO_CALENDAR_STALE", "DATA_STALE"}:
                    code = "DEMO_ENTRY_ADMISSION_FAILED"
                self._audit(action, identity, False, code)
                raise DemoEntryNotSent(code) from None
        # Persisting attempts across process restarts is the router journal's job.
        self._attempts[key] = (fingerprint, None)
        try:
            status, payload = self._http_post(BASE + path, headers, body_bytes, self._timeout)
            if not isinstance(status, int) or isinstance(status, bool) or status != 200:
                raise DemoRouteError("DEMO_HTTP_ERROR", _numeric(status))
            if not isinstance(payload, dict):
                raise DemoRouteError("DEMO_BAD_RESPONSE")
            if str(payload.get("code", "")) != "0":
                # OKX can wrap a single rejected operation in aggregate code
                # 1. Preserve its numeric row reason without retaining sMsg,
                # changing the failed outcome, or allowing another POST.
                rows = payload.get("data")
                if (str(payload.get("code")) == "1" and isinstance(rows, list)
                        and len(rows) == 1 and isinstance(rows[0], dict)):
                    numeric = _numeric(rows[0].get("sCode"))
                    if numeric is not None and numeric != 0:
                        raise DemoRouteError("DEMO_ROW_REJECTED", numeric)
                raise DemoRouteError("DEMO_API_REJECTED", _numeric(payload.get("code")))
            rows = payload.get("data")
            if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
                raise DemoRouteError("DEMO_BAD_RESPONSE")
            if str(rows[0].get("sCode", "")) != "0":
                raise DemoRouteError("DEMO_ROW_REJECTED", _numeric(rows[0].get("sCode")))
            if (path == "/api/v5/trade/cancel-algos"
                    and rows[0].get("algoId") != body[0]["algoId"]):
                # Do not cache a malformed success as a successful cancellation.
                # The already-recorded unknown attempt forbids another POST.
                raise DemoRouteError("DEMO_PROTECTION_MISMATCH")
            result = deepcopy(rows[0])
        except DemoEntryNotSent:
            # A callback/adapter throwing this type after HTTP was entered is
            # no longer proof of zero send. Preserve the original UNKNOWN path.
            self._audit(action, identity, False, "DEMO_WRITE_UNKNOWN")
            raise DemoRouteError("DEMO_WRITE_UNKNOWN") from None
        except DemoRouteError as error:
            self._audit(action, identity, False, error.code, error.numeric)
            raise
        except DemoTransportError:
            self._audit(action, identity, False, "DEMO_WRITE_UNKNOWN")
            raise DemoRouteError("DEMO_WRITE_UNKNOWN") from None
        except Exception:
            self._audit(action, identity, False, "DEMO_WRITE_UNKNOWN")
            raise DemoRouteError("DEMO_WRITE_UNKNOWN") from None
        self._attempts[key] = (fingerprint, result)
        self._audit(action, identity, True, "0")
        return result

    @staticmethod
    def _intent(intent: Intent):
        if not isinstance(intent, Intent):
            raise DemoRouteError("DEMO_INVALID_INTENT")
        _inst(intent.inst_id)
        if intent.side not in {"long", "short"} or not isinstance(intent.require_maker, bool):
            raise DemoRouteError("DEMO_INVALID_INTENT")
        for name in ("entry", "qty", "stop", "tp1", "contract_value", "lot_size", "tick_size"):
            if not isinstance(getattr(intent, name), Decimal):
                raise DemoRouteError("DEMO_INVALID_INTENT")
            _number(getattr(intent, name), positive=True)
        # The fixed instrument currently represents one SOL per contract. A
        # changed contract specification needs a separate reviewed adapter.
        if intent.contract_value != Decimal("1"):
            raise DemoRouteError("DEMO_CONTRACT_VALUE")
        if intent.qty % intent.lot_size or any(value % intent.tick_size for value in
                                               (intent.entry, intent.stop, intent.tp1)):
            raise DemoRouteError("DEMO_INVALID_INCREMENT")
        if ((intent.side == "long" and not intent.stop < intent.entry < intent.tp1)
                or (intent.side == "short" and not intent.tp1 < intent.entry < intent.stop)):
            raise DemoRouteError("DEMO_INVALID_PRICES")
        actual = intent.qty * intent.contract_value * intent.entry
        if intent.notional != actual or actual > MAX_NOTIONAL:
            raise DemoRouteError("DEMO_NOTIONAL_LIMIT")

    def submit_entry(self, intent: Intent, cl_ord_id: str, *,
                     before_send: Callable | None = None) -> Mapping:
        self.assert_demo_enabled()
        self._intent(intent)
        _id(cl_ord_id)
        body = dict(instId=INST, tdMode="cross", posSide="net",
                    side="buy" if intent.side == "long" else "sell",
                    ordType="post_only" if intent.require_maker else "limit",
                    sz=str(intent.qty), px=str(intent.entry), clOrdId=cl_ord_id)
        return self._post("entry", cl_ord_id, "/api/v5/trade/order", body,
                          before_send=before_send)

    def get_order(self, inst_id: str, cl_ord_id: str, *, _deadline: float | None = None) -> Mapping | None:
        _inst(inst_id)
        _id(cl_ord_id)
        rows = self._get("/api/v5/trade/order", {"instId": INST, "clOrdId": cl_ord_id},
                         absent=True, _deadline=_deadline)
        if not rows:
            return None
        if (len(rows) != 1 or rows[0].get("instId") != INST
                or rows[0].get("clOrdId") != cl_ord_id or not rows[0].get("ordId")
                or rows[0].get("posSide") != "net" or rows[0].get("tdMode") != "cross"
                or rows[0].get("ordType") not in {"limit", "post_only", "market"}):
            raise DemoRouteError("DEMO_ORDER_MISMATCH")
        return rows[0]

    def cancel_entry(self, inst_id: str, cl_ord_id: str) -> Mapping:
        self.assert_demo_enabled()
        _inst(inst_id)
        _id(cl_ord_id)
        return self._post("cancel", cl_ord_id, "/api/v5/trade/cancel-order",
                          dict(instId=INST, clOrdId=cl_ord_id))

    def get_protection(self, inst_id: str, protection_id: str) -> Mapping | None:
        _inst(inst_id)
        _id(protection_id)
        rows = self._get("/api/v5/trade/order-algo", {"algoClOrdId": protection_id}, absent=True)
        if not rows:
            return None
        if (len(rows) != 1 or rows[0].get("instId") != INST
                or rows[0].get("algoClOrdId") != protection_id or not rows[0].get("algoId")):
            raise DemoRouteError("DEMO_PROTECTION_MISMATCH")
        return rows[0]

    def cancel_protection(self, inst_id: str, protection_id: str, algo_id: str) -> Mapping:
        """Cancel exactly the route's independently correlated reduce-only OCO.

        The caller must first prove that its position is flat. This transport
        method proves ownership/shape of the specified protection only; neither
        an absent algo nor a successful cancel acknowledgement proves flatness.
        Cross-process cancellation checkpoints belong to the router journal.
        """
        self.assert_demo_enabled()
        _inst(inst_id)
        _id(protection_id)
        _id(algo_id)
        current = self.get_protection(INST, protection_id)
        result = dict(instId=INST, algoClOrdId=protection_id, algoId=algo_id,
                      cancel_post_sent=False)
        if current is None:
            # Only get_protection's explicit empty/51603 result is absent.
            # Timeouts and all other API/read errors propagate as unknown.
            return dict(result, state="not_found", status="ABSENT")
        if (current.get("algoId") != algo_id or current.get("ordType") != "oco"
                or current.get("posSide") != "net" or current.get("tdMode") != "cross"
                or str(current.get("reduceOnly", "")).lower() != "true"
                or current.get("side") not in {"buy", "sell"}
                or str(current.get("slOrdPx")) != "-1"
                or str(current.get("tpOrdPx")) != "-1"):
            raise DemoRouteError("DEMO_PROTECTION_MISMATCH")
        _number(current.get("sz"), positive=True)
        _number(current.get("slTriggerPx"), positive=True)
        _number(current.get("tpTriggerPx"), positive=True)
        state = current.get("state")
        if state in {"canceled", "effective", "order_failed"}:
            return dict(result, state=state, status="TERMINAL")
        if state != "live":
            raise DemoRouteError("DEMO_PROTECTION_STATE_UNKNOWN")
        with self._cancel_protection_lock:
            key = "cancel_protection:" + protection_id
            previously_attempted = key in self._attempts
            self._post("cancel_protection", protection_id, "/api/v5/trade/cancel-algos",
                       [dict(instId=INST, algoId=algo_id)])
            # ACKNOWLEDGED is deliberately distinct from TERMINAL. A subsequent
            # correlated GET/full exposure snapshot must prove cancellation/flatness.
            return dict(result, state="cancel_requested", status="CANCEL_ACKNOWLEDGED",
                        cancel_post_sent=not previously_attempted)

    def get_protection_exit_orders(self, inst_id: str, protection_id: str,
                                   algo_id: str) -> list[dict]:
        """Read bounded real child receipts for this route's effective OCO.

        OKX documents ``ordIdList`` as the actual triggered order ID list and
        each ordinary order's ``algoId`` as its triggering parent. The deprecated
        latest ``ordId`` alone is insufficient evidence of the complete list.
        This method neither infers a fill from ``effective`` nor proves flatness.
        """
        self.assert_demo_enabled()
        _inst(inst_id)
        _id(protection_id)
        _id(algo_id)
        parent = self.get_protection(INST, protection_id)
        if (not isinstance(parent, Mapping) or parent.get("algoId") != algo_id
                or parent.get("ordType") != "oco" or parent.get("state") != "effective"
                or parent.get("posSide") != "net" or parent.get("tdMode") != "cross"
                or str(parent.get("reduceOnly", "")).lower() != "true"
                or parent.get("side") not in {"buy", "sell"}
                or str(parent.get("slOrdPx")) != "-1"
                or str(parent.get("tpOrdPx")) != "-1"):
            raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
        parent_size = _number(parent.get("sz"), positive=True)
        _number(parent.get("slTriggerPx"), positive=True)
        _number(parent.get("tpTriggerPx"), positive=True)
        ids = parent.get("ordIdList")
        if (not isinstance(ids, list) or not 1 <= len(ids) <= 4
                or any(not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,32}", value)
                       for value in ids) or len(set(ids)) != len(ids)
                or parent.get("ordId", "") not in ("", *ids)):
            raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
        children = []
        total_filled = Decimal("0")
        for order_id in ids:
            # There is no arbitrary path/query passthrough and no fabricated
            # child clOrdId: exchange-generated orders are read by their ordId.
            rows = self._get("/api/v5/trade/order", {"instId": INST, "ordId": order_id})
            if len(rows) != 1:
                raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
            child = rows[0]
            if (child.get("instId") != INST or child.get("ordId") != order_id
                    or child.get("algoId") != algo_id
                    or child.get("algoClOrdId") not in (None, "", protection_id)
                    or child.get("posSide") != "net" or child.get("tdMode") != "cross"
                    or child.get("side") != parent["side"]
                    or str(child.get("reduceOnly", "")).lower() != "true"
                    or child.get("ordType") not in {"market", "limit"}
                    or child.get("state") not in {"live", "partially_filled", "filled", "canceled", "mmp_canceled"}):
                raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
            size = _number(child.get("sz"), positive=True)
            filled = _number(child.get("accFillSz"))
            state = child["state"]
            if (size > parent_size or filled > size
                    or (state == "live" and filled != 0)
                    or (state == "partially_filled" and not 0 < filled < size)
                    or (state == "filled" and filled != size)):
                raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
            total_filled += filled
            if total_filled > parent_size:
                raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
            children.append(child)
        after = self.get_protection(INST, protection_id)
        # Refuse list/identity/shape changes during this bounded evidence read.
        # Do not hide an additional triggered child behind an earlier snapshot.
        keys = ("instId", "algoClOrdId", "algoId", "ordType", "state", "posSide", "tdMode",
                "reduceOnly", "side", "sz", "slTriggerPx", "slOrdPx", "tpTriggerPx", "tpOrdPx",
                "ordIdList", "ordId")
        if (not isinstance(after, Mapping)
                or any(parent.get(key) != after.get(key) for key in keys)):
            raise DemoRouteError("DEMO_PROTECTION_EXIT_UNKNOWN")
        return deepcopy(children)

    @staticmethod
    def _valid_protection(row: Mapping | None, intent: Intent, protection_id: str,
                          qty: Decimal | None = None, algo_id: str = "") -> bool:
        if (not isinstance(row, Mapping) or row.get("instId") != INST
                or row.get("algoClOrdId") != protection_id or not row.get("algoId")
                or (algo_id and row.get("algoId") != algo_id)
                or row.get("ordType") != "oco" or row.get("state") != "live"
                or row.get("side") != ("sell" if intent.side == "long" else "buy")
                or row.get("posSide") != "net" or row.get("tdMode") != "cross"
                or str(row.get("reduceOnly", "")).lower() != "true"
                or any(row.get(key, "") not in ("", "last") for key in ("slTriggerPxType", "tpTriggerPxType"))
                or str(row.get("slOrdPx")) != "-1" or str(row.get("tpOrdPx")) != "-1"):
            return False
        try:
            size = _number(row.get("sz"), positive=True)
            return ((qty is None or size == qty)
                    and _number(row.get("slTriggerPx"), positive=True) == intent.stop
                    and _number(row.get("tpTriggerPx"), positive=True) == intent.tp1)
        except DemoRouteError:
            return False

    def set_protection(self, intent: Intent, qty: Decimal, protection_id: str, *, reduce_only: bool) -> Mapping:
        self.assert_demo_enabled()
        self._intent(intent)
        _id(protection_id)
        if reduce_only is not True:
            raise DemoRouteError("DEMO_REDUCE_ONLY_REQUIRED")
        size = _number(qty, positive=True)
        if size > intent.qty or size % intent.lot_size:
            raise DemoRouteError("DEMO_PROTECTION_SIZE")
        current = self.get_protection(INST, protection_id)
        algo_id = ""
        if current is not None:
            if not self._valid_protection(current, intent, protection_id):
                raise DemoRouteError("DEMO_PROTECTION_MISMATCH")
            algo_id = _id(current["algoId"])
            old_size = _number(current["sz"], positive=True)
            if old_size == size:
                return current
            if old_size > size:
                raise DemoRouteError("DEMO_PROTECTION_SHRINK")
        elif any(key.startswith("protect:" + protection_id + ":") for key in self._attempts):
            raise DemoRouteError("DEMO_PROTECTION_UNKNOWN")
        request_id = "a" + hashlib.sha256((protection_id + ":" + str(size.normalize())).encode()).hexdigest()[:31]
        if algo_id:
            path = "/api/v5/trade/amend-algos"
            body = dict(instId=INST, algoId=algo_id, newSz=str(size),
                        cxlOnFail=False, reqId=request_id)
        else:
            path = "/api/v5/trade/order-algo"
            body = dict(instId=INST, tdMode="cross", posSide="net",
                        side="sell" if intent.side == "long" else "buy", ordType="oco",
                        sz=str(size), reduceOnly=True, slTriggerPx=str(intent.stop),
                        slOrdPx="-1", tpTriggerPx=str(intent.tp1), tpOrdPx="-1",
                        slTriggerPxType="last", tpTriggerPxType="last",
                        algoClOrdId=protection_id)
        try:
            ack = self._post("protect", protection_id + ":" + str(size.normalize()), path, body)
            returned = ack.get("algoId")
            if not isinstance(returned, str) or not returned or (algo_id and returned != algo_id):
                raise DemoRouteError("DEMO_PROTECTION_MISMATCH")
            algo_id = returned
        except DemoRouteError:
            # Unknown, rejected or malformed acknowledgements never authorize a retry.
            # A correlated, complete GET snapshot is the only proof of protection.
            pass
        observed = self.get_protection(INST, protection_id)
        if not self._valid_protection(observed, intent, protection_id, size, algo_id):
            raise DemoRouteError("DEMO_PROTECTION_UNCONFIRMED")
        return observed

    def emergency_close(self, inst_id: str, side: str, qty: Decimal, cl_ord_id: str,
                        *, reduce_only: bool) -> Mapping:
        self.assert_demo_enabled()
        _inst(inst_id)
        _id(cl_ord_id)
        if side not in {"buy", "sell"}:
            raise DemoRouteError("DEMO_INVALID_SIDE")
        if reduce_only is not True:
            raise DemoRouteError("DEMO_REDUCE_ONLY_REQUIRED")
        body = dict(instId=INST, tdMode="cross", posSide="net", side=side,
                    ordType="market", sz=str(_number(qty, positive=True)),
                    reduceOnly=True, clOrdId=cl_ord_id)
        return self._post("close", cl_ord_id, "/api/v5/trade/order", body)

    def read_exposure(self, *, _deadline: float | None = None) -> dict:
        """Full-account positions, ordinary orders and supported algo families.

        A full page is not proof of completeness. This intentionally refuses
        capped/unknown queries rather than interpreting them as a clear account.
        """
        return self._exposure_result(self._parallel_gets(self._exposure_jobs(_deadline), _deadline))

    def _exposure_jobs(self, deadline: float | None) -> dict:
        """The same ten full-account reads, also usable in one bounded DAG."""
        jobs = {"positions": lambda: self._get("/api/v5/account/positions", {}, _deadline=deadline),
                "pending": lambda: self._get("/api/v5/trade/orders-pending", {"limit": "100"}, _deadline=deadline)}
        for kind in ALGO_TYPES:
            jobs["algo:"+kind] = lambda kind=kind: self._get(
                "/api/v5/trade/orders-algo-pending", {"ordType": kind, "limit": "100"}, _deadline=deadline)
        return jobs

    @staticmethod
    def _exposure_result(data: Mapping) -> dict:
        positions = data["positions"]
        active = []
        for row in positions:
            if (not isinstance(row.get("instId"), str) or not row["instId"]
                    or row.get("posSide") not in {"net", "long", "short"}):
                raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
            if _number(row.get("pos"), signed=True):
                active.append(row)
        if len(positions) >= 100:
            raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
        pending = data["pending"]
        if len(pending) >= 100:
            raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
        for row in pending:
            if (not isinstance(row.get("instId"), str) or not row["instId"]
                    or not isinstance(row.get("ordId"), str) or not row["ordId"]
                    or row.get("state") not in {"live", "partially_filled"}):
                raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
            if _number(row.get("accFillSz")) > _number(row.get("sz"), positive=True):
                raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
        algos = []
        seen = set()
        for kind in ALGO_TYPES:
            rows = data["algo:"+kind]
            if len(rows) >= 100:
                raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
            for row in rows:
                if (not isinstance(row.get("instId"), str) or not row["instId"]
                        or row.get("ordType") != kind
                        or not isinstance(row.get("algoId"), str) or not row["algoId"]
                        or row.get("algoId") in seen):
                    raise DemoRouteError("DEMO_EXPOSURE_UNKNOWN")
                seen.add(row["algoId"])
                algos.append(row)
        return {"positions": active, "pending": pending, "algos": algos}

    def verify_closed_position_lifecycle(self, intent: Intent, order_id: str, client_id: str,
                                         filled_qty: Decimal, flat_observed_at_ms: int,
                                         protection_id: str, algo_id: str) -> dict:
        """GET-only proof of an already closed, precisely owned OCO lifecycle.

        This capability is deliberately independent of the write switch. Neither
        a terminal algorithm receipt nor a historical position alone proves an
        exit: both full-account exposure waves must be empty, every unfiltered
        SOL fill must join the exact owned orders, and one real history row must
        match the execution/record intervals. Evidence has one five-second budget.
        No route, client ID, position ID or source timestamp is manufactured.
        """
        if (type(self._reader) is not DemoReadOnlyClient or self.mode != "demo"
                or BASE != "https://openapi.okx.com"
                or self._reader._headers("GET", "/api/v5/account/config", "").get("x-simulated-trading") != "1"):
            raise DemoRouteError("DEMO_ISOLATION_INVALID")
        self._intent(intent)
        for identity in (order_id, client_id, protection_id, algo_id):
            _id(identity)
        expected_protection = "r" + hashlib.sha256((intent.signal_id+":protection").encode()).hexdigest()[:31]
        if protection_id != expected_protection:
            raise DemoRouteError("DEMO_CLOSED_PROTECTION_MISMATCH")
        size = _number(filled_qty, positive=True)
        if size != intent.qty or size % intent.lot_size:
            raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
        if type(flat_observed_at_ms) is not int or flat_observed_at_ms <= 0:
            raise DemoRouteError("DEMO_FLAT_EVIDENCE_MISSING")
        deadline = time.monotonic() + LIFECYCLE_EVIDENCE_SECONDS

        def stamp(value):
            if not (type(value) is int or (isinstance(value, str) and value.isascii() and value.isdigit())):
                raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
            return _millisecond(value)

        def server_time():
            rows = self._get("/api/v5/public/time", {}, _deadline=deadline)
            if len(rows) != 1:
                raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
            return stamp(rows[0].get("ts"))

        def parent_read():
            rows = self._get("/api/v5/trade/order-algo", {"algoClOrdId": protection_id}, _deadline=deadline)
            if len(rows) != 1:
                raise DemoRouteError("DEMO_CLOSED_PROTECTION_MISMATCH")
            return rows[0]

        def fills_read(begin, end):
            rows = self._get("/api/v5/trade/fills-history", {"instType": "SWAP", "instId": INST,
                       "begin": str(begin), "end": str(end), "limit": "100"}, _deadline=deadline)
            if len(rows) >= 100:
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")
            if any(not begin <= stamp(row.get("fillTime")) <= end for row in rows):
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW")
            return rows

        def history_read(end):
            rows = self._get("/api/v5/account/positions-history", {"instType": "SWAP", "instId": INST,
                       "after": str(end+1), "limit": "100"}, _deadline=deadline)
            if len(rows) >= 100:
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")
            selected = []
            seen = set()
            for row in rows:
                opened, closed = stamp(row.get("cTime")), stamp(row.get("uTime"))
                key = (_id(row.get("posId")), opened, closed)
                if (row.get("instId") != INST or row.get("instType") != "SWAP"
                        or opened > closed or closed > end or key in seen):
                    raise DemoRouteError("DEMO_CLOSED_HISTORY_MISMATCH")
                seen.add(key)
                if closed >= flat_observed_at_ms:
                    selected.append(row)
            if len(selected) != 1:
                raise DemoRouteError("DEMO_CLOSED_HISTORY_MISMATCH")
            return selected[0]

        def require_flat(exposure):
            if (not isinstance(exposure, Mapping) or any(exposure.get(key) != []
                    for key in ("positions", "pending", "algos"))):
                raise DemoRouteError("DEMO_CLOSED_EXPOSURE_NOT_FLAT")

        exit_side = "sell" if intent.side == "long" else "buy"

        def parent_ids(parent):
            # Children are never guessed or dispatched before every original
            # parent identity/parameter check and its complete ordIdList pass.
            if (parent.get("instId") != INST or parent.get("algoClOrdId") != protection_id
                    or parent.get("algoId") != algo_id or parent.get("state") != "effective"
                    or parent.get("ordType") != "oco" or parent.get("side") != exit_side
                    or parent.get("posSide") != "net" or parent.get("tdMode") != "cross"
                    or str(parent.get("reduceOnly", "")).lower() != "true"
                    or str(parent.get("slOrdPx")) != "-1" or str(parent.get("tpOrdPx")) != "-1"
                    or _number(parent.get("sz"), positive=True) != size
                    or _number(parent.get("slTriggerPx"), positive=True) != intent.stop
                    or _number(parent.get("tpTriggerPx"), positive=True) != intent.tp1
                    or parent.get("actualSide") not in {"sl", "tp"}
                    or any(parent.get(key, "last") != "last" for key in ("slTriggerPxType", "tpTriggerPxType"))):
                raise DemoRouteError("DEMO_CLOSED_PROTECTION_MISMATCH")
            ids = parent.get("ordIdList")
            if (not isinstance(ids, list) or not 1 <= len(ids) <= 4
                    or any(not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,32}", value) for value in ids)
                    or len(set(ids)) != len(ids)
                    or parent.get("ordId", "") not in ("", *ids) or order_id in ids):
                raise DemoRouteError("DEMO_CLOSED_PROTECTION_MISMATCH")
            return ids

        def child_read(identity):
            rows = self._get("/api/v5/trade/order", {"instId": INST, "ordId": identity}, _deadline=deadline)
            if len(rows) != 1:
                raise DemoRouteError("DEMO_CLOSED_ORDER_MISMATCH")
            return rows[0]

        # One bounded pool schedules only independent/dependency-ready GETs.
        # public/time is a history window boundary, not the earliest exposure
        # observation: the caller's wall/monotonic interval covers all reads.
        exposure_jobs = self._exposure_jobs(deadline)
        jobs = {"exposure:"+key: job for key, job in exposure_jobs.items()}
        jobs.update(start=server_time, entry=lambda: self.get_order(INST, client_id, _deadline=deadline),
                    parent=parent_read,
                    config=lambda: self._get("/api/v5/account/config", {}, _deadline=deadline))
        first, children_map = {}, {}
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="demo-closed-read") as pool:
            pending = {pool.submit(job): key for key, job in jobs.items()}
            try:
                while pending:
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        raise DemoRouteError("DEMO_LIFECYCLE_STALE")
                    done, _ = wait(tuple(pending), timeout=remaining, return_when=FIRST_COMPLETED)
                    if not done:
                        raise DemoRouteError("DEMO_LIFECYCLE_STALE")
                    # Observe every completed failure before dispatching newly
                    # ready dependants; cancel pending work on any refusal.
                    completed = [(pending.pop(future), future.result()) for future in done]
                    for key, value in completed:
                        if key.startswith("child:"):
                            children_map[key[6:]] = value
                            continue
                        first[key] = value
                        if key == "start":
                            if not 0 <= value-flat_observed_at_ms <= LIFECYCLE_LOOKBACK_MS:
                                raise DemoRouteError("DEMO_FLAT_EVIDENCE_INVALID")
                            pending[pool.submit(fills_read, flat_observed_at_ms, value)] = "fills"
                            pending[pool.submit(history_read, value)] = "history"
                        elif key == "parent":
                            for identity in parent_ids(value):
                                pending[pool.submit(child_read, identity)] = "child:"+identity
            except Exception:
                for future in pending:
                    future.cancel()
                # Exiting the executor joins every in-flight read before this
                # proof returns or a later proof can start another wave.
                raise
        if time.monotonic() > deadline:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        start, parent = first["start"], first["parent"]
        ids = parent["ordIdList"]
        first["exposure"] = self._exposure_result({key: first["exposure:"+key] for key in exposure_jobs})
        require_flat(first["exposure"])
        config = first["config"]
        uid = config[0].get("uid") if len(config) == 1 else None
        if (not isinstance(uid, str) or not uid.isascii() or not uid.isdigit()
                or config[0].get("posMode") != "net_mode"):
            raise DemoRouteError("DEMO_CLOSED_ACCOUNT_UNVERIFIED")
        account_hash = hashlib.sha256(("DEMO\0"+uid).encode()).hexdigest()
        second = self._parallel_gets({"exposure": lambda: self.read_exposure(_deadline=deadline),
                                     "parent": parent_read}, deadline)
        require_flat(second["exposure"])
        if second["parent"] != parent:
            raise DemoRouteError("DEMO_CLOSED_PROTECTION_CHANGED")
        end = server_time()
        if not 0 <= end-start <= LIFECYCLE_EVIDENCE_SECONDS*1000:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        fills = first["fills"]
        if end > start:
            tail = self._parallel_gets({"fills": lambda: fills_read(start+1, end),
                                      "history": lambda: history_read(end)}, deadline)
            fills += tail["fills"]
            if tail["history"] != first["history"]:
                raise DemoRouteError("DEMO_CLOSED_HISTORY_CHANGED")
        entry, history = first["entry"], first["history"]
        entry_side = "buy" if intent.side == "long" else "sell"
        if (not isinstance(entry, Mapping) or entry.get("ordId") != order_id
                or entry.get("side") != entry_side or entry.get("state") != "filled"
                or entry.get("ordType") != ("post_only" if intent.require_maker else "limit")
                or _number(entry.get("sz"), positive=True) != size
                or _number(entry.get("accFillSz")) != size
                or _number(entry.get("px"), positive=True) != intent.entry
                or str(entry.get("reduceOnly", "")).lower() != "false"
                or entry.get("algoId", "") != ""):
            raise DemoRouteError("DEMO_CLOSED_ORDER_MISMATCH")
        children = [children_map[identity] for identity in ids]
        orders = {order_id: entry}
        for identity, child in zip(ids, children):
            if (child.get("instId") != INST or child.get("ordId") != identity
                    or child.get("algoId") != algo_id
                    or child.get("algoClOrdId") not in (None, "", protection_id)
                    or child.get("posSide") != "net" or child.get("tdMode") != "cross"
                    or child.get("side") != exit_side or child.get("state") != "filled"
                    or child.get("ordType") != "market"
                    or str(child.get("reduceOnly", "")).lower() != "true"
                    or _number(child.get("sz"), positive=True) != _number(child.get("accFillSz"), positive=True)
                    or _number(child.get("sz"), positive=True) > size
                    or not isinstance(child.get("clOrdId"), str)):
                raise DemoRouteError("DEMO_CLOSED_ORDER_MISMATCH")
            orders[identity] = child
        if sum((_number(child["accFillSz"]) for child in children), Decimal("0")) != size:
            raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
        grouped = {identity: [] for identity in orders}
        seen_trades, seen_bills = set(), set()
        for fill in fills:
            identity = fill.get("ordId")
            detail = orders.get(identity)
            if (detail is None or fill.get("instId") != INST or fill.get("instType") != "SWAP"
                    or fill.get("clOrdId") != detail["clOrdId"] or fill.get("posSide") != "net"
                    or fill.get("side") != detail["side"] or fill.get("feeCcy") != "USDT"
                    or fill.get("subType") not in ({"1", "3"} if identity == order_id and intent.side == "long" else
                        {"2", "4"} if identity == order_id else {"2", "5"} if intent.side == "long" else {"1", "6"})
                    or fill.get("algoId") not in (None, "", "" if identity == order_id else algo_id)):
                raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
            trade, bill = _id(fill.get("tradeId")), _id(fill.get("billId"))
            if trade in seen_trades or bill in seen_bills:
                raise DemoRouteError("DEMO_LIFECYCLE_DUPLICATE_FILL")
            seen_trades.add(trade)
            seen_bills.add(bill)
            execution, recorded = stamp(fill.get("fillTime")), stamp(fill.get("ts"))
            if not execution <= recorded <= min(end, execution+5000):
                raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
            quantity, price = _number(fill.get("fillSz"), positive=True), _number(fill.get("fillPx"), positive=True)
            if quantity % intent.lot_size:
                raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
            _number(fill.get("fee"), signed=True)
            pnl = _number(fill.get("fillPnl"), signed=True)
            if identity == order_id and pnl != 0:
                raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
            grouped[identity].append((execution, recorded, quantity, price))
        for identity, detail in orders.items():
            executions = grouped[identity]
            quantity = sum((item[2] for item in executions), Decimal("0"))
            if not executions or quantity != _number(detail["accFillSz"]):
                raise DemoRouteError("DEMO_LIFECYCLE_FILL_UNPROVEN")
            if sum((item[2]*item[3] for item in executions), Decimal("0")) != _number(detail.get("avgPx"), positive=True)*quantity:
                raise DemoRouteError("DEMO_CLOSED_PRICE_MISMATCH")
            last = max(item[0] for item in executions)
            recorded = min(item[1] for item in executions if item[0] == last)
            if (stamp(detail.get("fillTime")) != last or not last <= stamp(detail.get("uTime")) <= recorded
                    or not flat_observed_at_ms <= stamp(detail.get("cTime")) <= min(item[0] for item in executions)):
                raise DemoRouteError("DEMO_CLOSED_ORDER_TIME_MISMATCH")
        entries = grouped[order_id]
        exits = [item for identity in ids for item in grouped[identity]]
        first_fill, last_fill = min(item[0] for item in entries), max(item[0] for item in exits)
        first_record = min(item[1] for item in entries if item[0] == first_fill)
        last_record = min(item[1] for item in exits if item[0] == last_fill)
        opened, closed = stamp(history.get("cTime")), stamp(history.get("uTime"))
        trigger = stamp(parent.get("triggerTime"))
        if (not first_fill <= opened <= first_record or not last_fill <= closed <= last_record
                or max(item[0] for item in entries) > min(item[0] for item in exits)
                or not max(item[0] for item in entries) <= stamp(parent.get("cTime")) <= trigger <= min(item[0] for item in exits)
                or stamp(parent.get("uTime")) != trigger):
            raise DemoRouteError("DEMO_CLOSED_HISTORY_MISMATCH")
        position_id = _id(history.get("posId"))
        # OKX positions-history uses an empty settledPnl for SWAP: settlement
        # PnL applies only to cross FUTURES. Keep the raw receipt unchanged and
        # normalize only this exact inapplicable field in the owned SWAP scope.
        settled_pnl = history.get("settledPnl", "0")
        if (history.get("instType") == "SWAP" and history.get("instId") == INST
                and history.get("posSide") == "net" and history.get("mgnMode") == "cross"
                and type(settled_pnl) is str and settled_pnl == ""):
            settled_pnl = "0"
        if (history.get("posSide") != "net" or history.get("mgnMode") != "cross"
                or history.get("direction") != intent.side or history.get("ccy") != "USDT"
                or _number(history.get("openMaxPos"), positive=True) != size
                or _number(history.get("closeTotalPos"), positive=True) != size
                or _number(history.get("liqPenalty")) != 0
                or _number(settled_pnl, signed=True) != 0
                or any(fill.get("posId") not in (None, "", position_id) for fill in fills)):
            raise DemoRouteError("DEMO_CLOSED_HISTORY_MISMATCH")
        for records, field in ((entries, "openAvgPx"), (exits, "closeAvgPx")):
            if sum((item[2]*item[3] for item in records), Decimal("0")) != _number(history.get(field), positive=True)*size:
                raise DemoRouteError("DEMO_CLOSED_PRICE_MISMATCH")
        if time.monotonic() > deadline:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        return dict(position_binding_verified=True, closed_lifecycle_verified=True, exit_reconciled=True,
                    account_hash=account_hash, position_id=position_id, position_opened_at_ms=opened,
                    position_closed_at_ms=closed, flat_observed_at_ms=flat_observed_at_ms,
                    observed_at_ms=start, history_through_ms=end, confirmed_closed_qty=str(size),
                    exit_order_ids=deepcopy(ids), order=deepcopy(entry), parent=deepcopy(parent),
                    children=deepcopy(children), fills=deepcopy(fills), position_history=deepcopy(history),
                    exposure_before=deepcopy(first["exposure"]), exposure_after=deepcopy(second["exposure"]))

    def verify_emergency_closed_lifecycle(self, intent: Intent, order_id: str, client_id: str,
                                          filled_qty: Decimal, flat_observed_at_ms: int,
                                          close_order_ids: list[str], *, expected_account_hash: str) -> dict:
        """GET-only, typed receipt for a completed emergency market exit.

        The caller supplies the immutable intent and retained exchange identities.
        Every market close must use the router's exact deterministic emergency
        role; no OCO parent is invented. This narrow capability currently requires
        a completely filled entry and one completely filled emergency close.
        Multiple closes require a separate model for interleaved entry fills.
        It never updates a journal, clears HALT, admits trading, or grants resume.
        One five-second wall/monotonic interval covers all GETs and validation.
        """
        with localcontext() as context:
            context.prec = 200
            if (type(self._reader) is not DemoReadOnlyClient or self.mode != "demo"
                    or BASE != "https://openapi.okx.com"
                    or self._reader._headers("GET", "/api/v5/account/config", "").get("x-simulated-trading") != "1"):
                raise DemoRouteError("DEMO_ISOLATION_INVALID")
            self._intent(intent)
            _id(order_id)
            _id(client_id)
            signal = intent.signal_id
            if not isinstance(signal, str) or not signal.strip():
                raise DemoRouteError("DEMO_EMERGENCY_INTENT_INVALID")
            expected_client = (signal if re.fullmatch(r"[A-Za-z0-9]{1,32}", signal)
                               else "h" + hashlib.sha256(signal.encode()).hexdigest()[:31])
            if client_id != expected_client:
                raise DemoRouteError("DEMO_EMERGENCY_ENTRY_ID_MISMATCH")
            if (not isinstance(expected_account_hash, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", expected_account_hash)):
                raise DemoRouteError("DEMO_EMERGENCY_ACCOUNT_UNVERIFIED")
            if (type(close_order_ids) is not list or len(close_order_ids) != 1
                    or any(type(value) is not str or not re.fullmatch(r"[0-9]{1,32}", value)
                           for value in close_order_ids)
                    or len(set(close_order_ids)) != len(close_order_ids) or order_id in close_order_ids):
                raise DemoRouteError("DEMO_EMERGENCY_CLOSE_IDS_INVALID")
            ids = list(close_order_ids)
            size = _number(filled_qty, positive=True)
            if size != intent.qty or size % intent.lot_size:
                raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
            if (type(flat_observed_at_ms) is not int or flat_observed_at_ms <= 0
                    or type(intent.created_at_ms) is not int or type(intent.expires_at_ms) is not int
                    or not 0 < intent.created_at_ms < intent.expires_at_ms):
                raise DemoRouteError("DEMO_FLAT_EVIDENCE_MISSING")
            started_wall, started_mono = time.time_ns() // 1_000_000, time.monotonic_ns()
            deadline = started_mono / 1_000_000_000 + LIFECYCLE_EVIDENCE_SECONDS

            def stamp(value):
                if not (type(value) is int or (type(value) is str and value.isascii() and value.isdigit())):
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
                return _millisecond(value)

            def server_time():
                rows = self._get("/api/v5/public/time", {}, _deadline=deadline)
                if len(rows) != 1:
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
                return stamp(rows[0].get("ts"))

            def exposure():
                raw = self._parallel_gets(self._exposure_jobs(deadline), deadline)
                normalized = self._exposure_result(raw)
                if any(normalized[key] != [] for key in ("positions", "pending", "algos")):
                    raise DemoRouteError("DEMO_EMERGENCY_EXPOSURE_NOT_FLAT")
                return normalized, raw

            def closes_read():
                def read(identity):
                    rows = self._get("/api/v5/trade/order", {"instId": INST, "ordId": identity}, _deadline=deadline)
                    if len(rows) != 1:
                        raise DemoRouteError("DEMO_EMERGENCY_CLOSE_MISMATCH")
                    return rows[0]
                return self._parallel_gets({identity: lambda identity=identity: read(identity) for identity in ids}, deadline)

            def fills_read(begin, end):
                rows = self._get("/api/v5/trade/fills-history", {"instType": "SWAP", "instId": INST,
                    "begin": str(begin), "end": str(end), "limit": "100"}, _deadline=deadline)
                if len(rows) >= 100:
                    raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")
                if any(not begin <= stamp(row.get("fillTime")) <= end for row in rows):
                    raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW")
                return rows

            def history_read(end):
                rows = self._get("/api/v5/account/positions-history", {"instType": "SWAP", "instId": INST,
                    "after": str(end+1), "limit": "100"}, _deadline=deadline)
                if len(rows) >= 100:
                    raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")
                selected, seen = [], set()
                for row in rows:
                    opened, closed = stamp(row.get("cTime")), stamp(row.get("uTime"))
                    key = (_id(row.get("posId")), opened, closed)
                    if (row.get("instId") != INST or row.get("instType") != "SWAP"
                            or opened > closed or closed > end or key in seen):
                        raise DemoRouteError("DEMO_EMERGENCY_HISTORY_MISMATCH")
                    seen.add(key)
                    if closed >= flat_observed_at_ms:
                        selected.append(row)
                if len(selected) != 1:
                    raise DemoRouteError("DEMO_EMERGENCY_HISTORY_MISMATCH")
                return selected[0]

            start = server_time()
            if not 0 <= start-flat_observed_at_ms <= LIFECYCLE_LOOKBACK_MS:
                raise DemoRouteError("DEMO_FLAT_EVIDENCE_INVALID")
            first = self._parallel_gets({"exposure": exposure,
                "entry": lambda: self.get_order(INST, client_id, _deadline=deadline), "closes": closes_read,
                "config": lambda: self._get("/api/v5/account/config", {}, _deadline=deadline),
                "fills": lambda: fills_read(flat_observed_at_ms, start), "history": lambda: history_read(start)}, deadline)
            second = self._parallel_gets({"exposure": exposure,
                "entry": lambda: self.get_order(INST, client_id, _deadline=deadline), "closes": closes_read,
                "config": lambda: self._get("/api/v5/account/config", {}, _deadline=deadline)}, deadline)
            for key in ("entry", "closes", "config"):
                if first[key] != second[key]:
                    raise DemoRouteError("DEMO_EMERGENCY_RECEIPTS_CHANGED")
            config = first["config"]
            uid = config[0].get("uid") if len(config) == 1 else None
            if (not isinstance(uid, str) or not uid.isascii() or not uid.isdigit()
                    or config[0].get("posMode") != "net_mode"
                    or hashlib.sha256(("DEMO\0"+uid).encode()).hexdigest() != expected_account_hash):
                raise DemoRouteError("DEMO_EMERGENCY_ACCOUNT_UNVERIFIED")
            end = server_time()
            if not 0 <= end-start <= 5000:
                raise DemoRouteError("DEMO_LIFECYCLE_STALE")
            fills = first["fills"]
            if end > start:
                tail = self._parallel_gets({"fills": lambda: fills_read(start+1, end),
                    "history": lambda: history_read(end)}, deadline)
                fills += tail["fills"]
                if tail["history"] != first["history"]:
                    raise DemoRouteError("DEMO_EMERGENCY_HISTORY_CHANGED")
            entry, history = first["entry"], first["history"]
            entry_side, exit_side = ("buy", "sell") if intent.side == "long" else ("sell", "buy")
            if (not isinstance(entry, Mapping) or entry.get("ordId") != order_id
                    or entry.get("side") != entry_side or entry.get("state") != "filled"
                    or entry.get("ordType") != ("post_only" if intent.require_maker else "limit")
                    or _number(entry.get("sz"), positive=True) != size or _number(entry.get("accFillSz")) != size
                    or _number(entry.get("px"), positive=True) != intent.entry
                    or str(entry.get("reduceOnly", "")).lower() != "false"
                    or entry.get("algoId", "") != "" or entry.get("algoClOrdId") not in (None, "")):
                raise DemoRouteError("DEMO_EMERGENCY_ENTRY_MISMATCH")
            orders, closes, reduced = {order_id: entry}, [], Decimal("0")
            for identity in ids:
                child = first["closes"][identity]
                quantity = _number(child.get("sz"), positive=True)
                # The first emergency starts from the durable default "0" and
                # requests the retained filled quantity. Exchange size spelling
                # may normalize zeros; the immutable retained quantity, rather
                # than a guessed representation, determines the actual role.
                role = "emergency:0:" + str(size)
                expected_close = "r" + hashlib.sha256((signal+":"+role).encode()).hexdigest()[:31]
                if (child.get("instId") != INST or child.get("ordId") != identity
                        or child.get("clOrdId") != expected_close or child.get("posSide") != "net"
                        or child.get("tdMode") != "cross" or child.get("side") != exit_side
                        or child.get("state") != "filled" or child.get("ordType") != "market"
                        or str(child.get("reduceOnly", "")).lower() != "true"
                        or child.get("algoId") not in (None, "") or child.get("algoClOrdId") not in (None, "")
                        or _number(child.get("accFillSz"), positive=True) != quantity
                        or quantity % intent.lot_size or quantity != size):
                    raise DemoRouteError("DEMO_EMERGENCY_CLOSE_MISMATCH")
                reduced += quantity
                orders[identity] = child
                closes.append(child)
            if reduced != size:
                raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
            grouped = {identity: [] for identity in orders}
            seen_trades, seen_bills = set(), set()
            fee_total, pnl_total = Decimal("0"), Decimal("0")
            for fill in fills:
                identity, detail = fill.get("ordId"), orders.get(fill.get("ordId"))
                allowed_subtypes = ({"1", "3"} if intent.side == "long" else {"2", "4"}) if identity == order_id else (
                                    {"2", "5"} if intent.side == "long" else {"1", "6"})
                if (detail is None or fill.get("instId") != INST or fill.get("instType") != "SWAP"
                        or fill.get("clOrdId") != detail["clOrdId"] or fill.get("posSide") != "net"
                        or fill.get("side") != detail["side"] or fill.get("feeCcy") != "USDT"
                        or fill.get("subType") not in allowed_subtypes or fill.get("algoId") not in (None, "")):
                    raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
                trade, bill = _id(fill.get("tradeId")), _id(fill.get("billId"))
                if trade in seen_trades or bill in seen_bills:
                    raise DemoRouteError("DEMO_LIFECYCLE_DUPLICATE_FILL")
                seen_trades.add(trade)
                seen_bills.add(bill)
                execution, recorded = stamp(fill.get("fillTime")), stamp(fill.get("ts"))
                if not flat_observed_at_ms <= execution <= recorded <= min(end, execution+5000):
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
                quantity, price = _number(fill.get("fillSz"), positive=True), _number(fill.get("fillPx"), positive=True)
                if quantity % intent.lot_size:
                    raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
                fee_total += _number(fill.get("fee"), signed=True)
                pnl = _number(fill.get("fillPnl"), signed=True)
                if identity == order_id and pnl != 0:
                    raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
                pnl_total += pnl
                grouped[identity].append((execution, recorded, quantity, price))
            for identity, detail in orders.items():
                records = grouped[identity]
                quantity = sum((item[2] for item in records), Decimal("0"))
                if not records or quantity != _number(detail["accFillSz"]):
                    raise DemoRouteError("DEMO_LIFECYCLE_FILL_UNPROVEN")
                if sum((item[2]*item[3] for item in records), Decimal("0")) != _number(detail.get("avgPx"), positive=True)*quantity:
                    raise DemoRouteError("DEMO_EMERGENCY_PRICE_MISMATCH")
                last = max(item[0] for item in records)
                recorded = min(item[1] for item in records if item[0] == last)
                if (stamp(detail.get("fillTime")) != last or not last <= stamp(detail.get("uTime")) <= recorded
                        or not max(flat_observed_at_ms, intent.created_at_ms) <= stamp(detail.get("cTime")) <= min(item[0] for item in records)):
                    raise DemoRouteError("DEMO_EMERGENCY_ORDER_TIME_MISMATCH")
            entries = grouped[order_id]
            exits = [item for identity in ids for item in grouped[identity]]
            previous = max(item[0] for item in entries)
            for identity in ids:
                if stamp(orders[identity].get("cTime")) < previous:
                    raise DemoRouteError("DEMO_EMERGENCY_ORDER_TIME_MISMATCH")
                previous = max(item[0] for item in grouped[identity])
            first_fill, last_fill = min(item[0] for item in entries), max(item[0] for item in exits)
            first_record = min(item[1] for item in entries if item[0] == first_fill)
            last_record = min(item[1] for item in exits if item[0] == last_fill)
            opened, closed = stamp(history.get("cTime")), stamp(history.get("uTime"))
            position_id = _id(history.get("posId"))
            if (not first_fill <= opened <= first_record or not last_fill <= closed <= last_record
                    or history.get("posSide") != "net" or history.get("mgnMode") != "cross"
                    or history.get("direction") != intent.side or history.get("ccy") != "USDT"
                    or _number(history.get("openMaxPos"), positive=True) != size
                    or _number(history.get("closeTotalPos"), positive=True) != size
                    or _number(history.get("liqPenalty")) != 0
                    or _number("0" if history.get("settledPnl") == "" else history.get("settledPnl"), signed=True) != 0
                    or any(fill.get("posId") not in (None, "", position_id) for fill in fills)):
                raise DemoRouteError("DEMO_EMERGENCY_HISTORY_MISMATCH")
            for records, field in ((entries, "openAvgPx"), (exits, "closeAvgPx")):
                if sum((item[2]*item[3] for item in records), Decimal("0")) != _number(history.get(field), positive=True)*size:
                    raise DemoRouteError("DEMO_EMERGENCY_PRICE_MISMATCH")
            history_pnl = _number(history.get("pnl"), signed=True)
            history_fee = _number(history.get("fee"), signed=True)
            funding = _number(history.get("fundingFee"), signed=True)
            realized = _number(history.get("realizedPnl"), signed=True)
            if history_pnl != pnl_total or history_fee != fee_total or realized != history_pnl+history_fee+funding:
                raise DemoRouteError("DEMO_EMERGENCY_ACCOUNTING_MISMATCH")
            finished_wall, finished_mono = time.time_ns() // 1_000_000, time.monotonic_ns()
            if end > finished_wall:
                # Wait for an observed subsecond server/local clock difference;
                # never relabel the source time or extend the original budget.
                delay_ms = end - finished_wall
                if (delay_ms > 1000 or time.monotonic() + delay_ms / 1000 > deadline
                        or not 0 <= finished_wall-started_wall <= 5000
                        or not 0 <= finished_mono-started_mono <= 5_000_000_000):
                    raise DemoRouteError("DEMO_LIFECYCLE_STALE")
                time.sleep(delay_ms / 1000)
                finished_wall, finished_mono = time.time_ns() // 1_000_000, time.monotonic_ns()
            if (not 0 <= finished_wall-started_wall <= 5000
                    or not 0 <= finished_mono-started_mono <= 5_000_000_000
                    or not 0 <= finished_wall-start <= 5000 or not start <= end <= finished_wall
                    or time.monotonic() > deadline):
                raise DemoRouteError("DEMO_LIFECYCLE_STALE")
            return dict(schema=1, environment="DEMO", purpose="HALTED_EMERGENCY_CLOSURE_RECEIPT_ONLY",
                diagnostic_only=True, can_resume=False, can_activate=False,
                emergency_closed_lifecycle_verified=True, position_binding_verified=True,
                closed_lifecycle_verified=True, account_hash=expected_account_hash,
                intent_sha256=hashlib.sha256(json.dumps(intent.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                entry_order_id=order_id, entry_client_id=client_id,
                retained_close_order_ids=deepcopy(ids), position_id=position_id,
                position_opened_at_ms=opened, position_closed_at_ms=closed,
                flat_observed_at_ms=flat_observed_at_ms, observed_at_ms=start, history_through_ms=end,
                collection_started_at_ms=started_wall, collection_completed_at_ms=finished_wall,
                collection_elapsed_monotonic_ns=finished_mono-started_mono,
                confirmed_closed_qty=str(size), order=deepcopy(entry), closes=deepcopy(closes),
                fills=deepcopy(fills), position_history=deepcopy(history),
                exposure_before=deepcopy(first["exposure"][0]), exposure_after=deepcopy(second["exposure"][0]),
                exposure_raw_before=deepcopy(first["exposure"][1]), exposure_raw_after=deepcopy(second["exposure"][1]),
                exposure_algo_types=list(ALGO_TYPES))

    def verify_position_lifecycle(self, intent: Intent, order_id: str, client_id: str,
                                  filled_qty: Decimal, flat_observed_at_ms: int) -> dict:
        """Prove a first position binding from a persisted, verified flat snapshot.

        All SOL SWAP fills in the bounded window are fetched without an order-ID
        filter. Any exit/external fill, full page, missing field, stale evidence,
        changed exposure or unidentified protection refuses the binding. This
        proves ownership, not OCO quantity/health: caller must independently check
        the returned raw algoId against its persisted protection order identity.

        Five seconds is a rejection deadline, not a relaxed freshness rule.
        Per-request timeout is reduced to the remaining budget (rounded up to
        one second); failed read waves drain, so no previous wave leaks into a
        future proof. There are at most 25 GETs, no retry, and no POST.
        """
        self._intent(intent)
        _id(order_id)
        _id(client_id)
        size = _number(filled_qty, positive=True)
        if size > intent.qty or size % intent.lot_size:
            raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
        if (not isinstance(flat_observed_at_ms, int) or isinstance(flat_observed_at_ms, bool)
                or flat_observed_at_ms <= 0):
            raise DemoRouteError("DEMO_FLAT_EVIDENCE_MISSING")
        deadline = time.monotonic() + LIFECYCLE_EVIDENCE_SECONDS

        def server_time():
            rows = self._get("/api/v5/public/time", {}, _deadline=deadline)
            if len(rows) != 1:
                raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
            return _millisecond(rows[0].get("ts"))

        start = server_time()
        if not 0 <= start-flat_observed_at_ms <= LIFECYCLE_LOOKBACK_MS:
            raise DemoRouteError("DEMO_FLAT_EVIDENCE_INVALID")

        def fills_between(begin, end):
            rows = self._get("/api/v5/trade/fills-history",
                        {"instType": "SWAP", "instId": INST, "begin": str(begin),
                         "end": str(end), "limit": "100"}, _deadline=deadline)
            if len(rows) >= 100:
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")
            if any(not begin <= _millisecond(row.get("fillTime")) <= end for row in rows):
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW")
            return rows

        # One exposure wave uses ten simultaneous independent endpoint GETs;
        # own-order and unfiltered fills reads run alongside that wave.
        first = self._parallel_gets({
            "exposure": lambda: self.read_exposure(_deadline=deadline),
            "order": lambda: self.get_order(INST, client_id, _deadline=deadline),
            "fills": lambda: fills_between(flat_observed_at_ms, start)}, deadline)
        after = self.read_exposure(_deadline=deadline)
        end = server_time()
        if not 0 <= end-start <= LIFECYCLE_EVIDENCE_SECONDS * 1000:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        fills = first["fills"]
        if end > start:
            fills += fills_between(start+1, end)
        order = first["order"]
        side = "buy" if intent.side == "long" else "sell"
        if (not isinstance(order, Mapping) or order.get("ordId") != order_id
                or order.get("side") != side
                or order.get("ordType") != ("post_only" if intent.require_maker else "limit")
                or order.get("state") not in {"live", "partially_filled", "filled", "canceled", "mmp_canceled"}
                or _number(order.get("sz"), positive=True) != intent.qty
                or _number(order.get("accFillSz")) != size
                or (order.get("state") == "filled" and size != intent.qty)):
            raise DemoRouteError("DEMO_LIFECYCLE_ORDER_MISMATCH")
        seen, seen_trades, seen_bills, total, times = {}, set(), set(), Decimal("0"), []
        record_times = []
        for fill in fills:
            if (fill.get("instId") != INST or fill.get("instType") != "SWAP"
                    or fill.get("ordId") != order_id or fill.get("clOrdId") != client_id
                    or fill.get("posSide") != "net" or fill.get("side") != side
                    or fill.get("feeCcy") != "USDT"
                    or fill.get("subType") in {"5", "6"}
                    or _number(fill.get("fillPnl"), signed=True) != 0):
                raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
            identity = (_id(fill.get("tradeId")), _id(fill.get("billId")))
            if identity[0] in seen_trades or identity[1] in seen_bills:
                raise DemoRouteError("DEMO_LIFECYCLE_DUPLICATE_FILL")
            seen[identity] = fill
            seen_trades.add(identity[0])
            seen_bills.add(identity[1])
            stamp = _millisecond(fill.get("fillTime"))
            if not flat_observed_at_ms <= stamp <= end:
                raise DemoRouteError("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW")
            record_stamp = None
            if "ts" in fill:
                raw_record_stamp = fill["ts"]
                if (type(raw_record_stamp) is not int and not
                        (isinstance(raw_record_stamp, str) and raw_record_stamp.isascii()
                         and raw_record_stamp.isdigit())):
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
                try:
                    record_stamp = _millisecond(raw_record_stamp)
                except DemoRouteError:
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID") from None
                if (not stamp <= record_stamp <= end
                        or record_stamp - stamp > LIFECYCLE_EVIDENCE_SECONDS * 1000):
                    raise DemoRouteError("DEMO_LIFECYCLE_TIME_INVALID")
            _number(fill.get("fee"), signed=True)
            fill_size = _number(fill.get("fillSz"), positive=True)
            if fill_size % intent.lot_size:
                raise DemoRouteError("DEMO_LIFECYCLE_QUANTITY")
            total += fill_size
            times.append(stamp)
            record_times.append((stamp, record_stamp))
        if not times or total != size:
            raise DemoRouteError("DEMO_LIFECYCLE_FILL_UNPROVEN")
        first_fill = min(times)
        # Execution and record-generation timestamps need not be identical.
        # Only the earliest execution's own record can bound its creation;
        # later executions cannot widen this interval. Missing record evidence
        # retains the old exact-match contract, never an inferred timestamp.
        first_record = (min(record for stamp, record in record_times if stamp == first_fill)
                        if all(record is not None for _, record in record_times) else first_fill)
        protection_id = "r" + hashlib.sha256((intent.signal_id+":protection").encode()).hexdigest()[:31]

        def ownership_view(exposure):
            if len(exposure["positions"]) != 1:
                raise DemoRouteError("DEMO_LIFECYCLE_POSITION_MISMATCH")
            position = exposure["positions"][0]
            quantity = _number(position.get("pos"), signed=True)
            opened = _millisecond(position.get("cTime"))
            if (position.get("instId") != INST or position.get("posSide") != "net"
                    or position.get("mgnMode") != "cross" or abs(quantity) != size
                    or (quantity > 0) != (intent.side == "long")
                    or not first_fill <= opened <= first_record):
                raise DemoRouteError("DEMO_LIFECYCLE_POSITION_MISMATCH")
            position_id = _id(position.get("posId"))
            if len(exposure["pending"]) > 1:
                raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
            pending_key = []
            for pending in exposure["pending"]:
                if (pending.get("instId") != INST or pending.get("ordId") != order_id
                        or pending.get("clOrdId") != client_id or pending.get("side") != side
                        or pending.get("posSide") != "net" or pending.get("tdMode") != "cross"
                        or pending.get("ordType") != order["ordType"]
                        or _number(pending.get("accFillSz")) != size
                        or _number(pending.get("sz"), positive=True) != intent.qty
                        or order["state"] not in {"live", "partially_filled"}):
                    raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
                pending_key.append((pending["ordId"], pending["state"], str(size)))
            if len(exposure["algos"]) > 1:
                raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
            algo_key = []
            for algo in exposure["algos"]:
                if (not self._valid_protection(algo, intent, protection_id)
                        or _number(algo["sz"], positive=True) > size):
                    raise DemoRouteError("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
                algo_key.append((algo["algoId"], algo["algoClOrdId"], str(_number(algo["sz"]))))
            return (position_id, opened, str(quantity), pending_key, algo_key)

        before_key = ownership_view(first["exposure"])
        after_key = ownership_view(after)
        if before_key != after_key:
            raise DemoRouteError("DEMO_LIFECYCLE_POSITION_CHANGED")
        if time.monotonic() > deadline:
            raise DemoRouteError("DEMO_LIFECYCLE_STALE")
        return dict(position_binding_verified=True, position_id=after_key[0],
                    position_opened_at_ms=after_key[1], flat_observed_at_ms=flat_observed_at_ms,
                    observed_at_ms=start, history_through_ms=end,
                    position_observed_not_before_ms=start,
                    order=deepcopy(order), fills=deepcopy(fills), exposure=deepcopy(after))
