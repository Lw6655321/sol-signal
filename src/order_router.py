"""Bounded order state machine with injected transport and fail-closed preflight.

The default NativeTransport remains locked. Explicit native-live adapters need
the trading gate and a separately verified, fresh account-risk snapshot.

Each router has a fresh audit directory; a separate, persistent SQLite journal
owns account-level deduplication, the single position slot, and a sticky HALT.
Mutation requests are never blindly retried. Only reads have bounded retries.
Cancellation uses the same fill/protection state machine as ordinary polling.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
import time
import uuid
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .execution import ExecutionRejected, Intent, build_intent, notional_limit


ZERO = Decimal("0")
TERMINAL_ENTRY = {"filled", "canceled", "mmp_canceled"}
ACTIVE_ROUTES = {"RESERVED", "SUBMITTING", "PENDING", "PROTECTING", "PROTECTED", "CLOSING", "UNKNOWN", "HALTED"}
DEMO_ENTRY_CONTEXT_CODES = {"MACRO_BLACKOUT", "DEMO_CALENDAR_STALE", "MARKET_STALE", "DATA_STALE"}


class RouterError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def client_order_id(signal_id: str) -> str:
    """Preserve valid wire IDs; persist a deterministic mapping for engine IDs."""
    if not isinstance(signal_id, str) or not signal_id.strip():
        raise RouterError("SIGNAL_ID")
    if re.fullmatch(r"[A-Za-z0-9]{1,32}", signal_id):
        return signal_id
    return "h" + hashlib.sha256(signal_id.encode("utf-8")).hexdigest()[:31]


def _derived_id(signal_id: str, role: str) -> str:
    return "r" + hashlib.sha256((signal_id + ":" + role).encode("utf-8")).hexdigest()[:31]


def _decimal(value, *, positive: bool = False) -> Decimal:
    if value is None or isinstance(value, bool):
        raise RouterError("INVALID_SNAPSHOT")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise RouterError("INVALID_SNAPSHOT") from None
    if not result.is_finite() or result < ZERO or (positive and result <= ZERO):
        raise RouterError("INVALID_SNAPSHOT")
    return result


@dataclass(frozen=True)
class RouteResult:
    status: str
    code: str
    signal_id: str
    client_order_id: str
    filled_qty: Decimal
    protected_qty: Decimal
    run_dir: Path
    emergency_status: str = ""


@dataclass(frozen=True)
class _DemoEntryAdmission:
    """Immutable source evidence, scoped to this exact DEMO entry intent."""
    intent: Intent
    account_ms: int
    quote_ms: int
    checked_ms: int
    monotonic_ns: int
    bid: Decimal
    ask: Decimal
    context_valid_until_ms: int | None = None
    context_expiry_code: str = "DEMO_ENTRY_ADMISSION_FAILED"


class Transport(Protocol):
    """Adapter contract: responses must be correlated, cumulative snapshots.

    set_protection creates/amends ONE bracket under the same protection_id;
    it must not append overlapping brackets. Snapshots use OKX field names.
    emergency_close must honor reduce_only; its acknowledgement is not a fill.
    """
    mode: str

    def submit_entry(self, intent: Intent, cl_ord_id: str) -> Mapping: ...
    def get_order(self, inst_id: str, cl_ord_id: str) -> Mapping | None: ...
    def cancel_entry(self, inst_id: str, cl_ord_id: str) -> Mapping: ...
    def set_protection(self, intent: Intent, qty: Decimal, protection_id: str, *, reduce_only: bool) -> Mapping: ...
    def get_protection(self, inst_id: str, protection_id: str) -> Mapping | None: ...
    def emergency_close(self, inst_id: str, side: str, qty: Decimal,
                        cl_ord_id: str, *, reduce_only: bool) -> Mapping: ...


class NativeTransport:
    """Locked placeholder, not a live adapter. It never accesses credentials."""
    mode = "native"

    @staticmethod
    def _blocked(*args, **kwargs):
        if os.environ.get("OKX_TRADING_ENABLED") != "1":
            raise RouterError("TRADING_DISABLED")
        raise RouterError("NATIVE_UNAVAILABLE")

    submit_entry = get_order = cancel_entry = set_protection = get_protection = emergency_close = _blocked


class OrderRouter:
    AUDIT_FIELDS = ("timestamp_ms", "mode", "account_key", "signal_id", "client_order_id",
                    "event", "status", "code", "filled_qty", "protected_qty", "emergency_status",
                    "order_id", "entry_state", "protection_id", "protection_order_id",
                    "protection_requested_qty", "close_id", "close_order_id", "close_qty", "confirmed_closed_qty")

    def __init__(self, transport: Transport | None = None, *, config: Mapping,
                 state_provider: Callable[[], Mapping], market_provider: Callable[[], Mapping],
                 log_root: str | Path = "logs", journal_path: str | Path | None = None,
                 clock: Callable[[], int] | None = None, max_read_attempts: int = 3,
                 account_key: str = "T2-mock", account_identity: str | None = None,
                 require_demo_entry_admission: bool = False,
                 final_entry_check: Callable[[int], Mapping] | None = None):
        if not isinstance(max_read_attempts, int) or isinstance(max_read_attempts, bool) or not 1 <= max_read_attempts <= 10:
            raise ValueError("max_read_attempts must be between 1 and 10")
        if not isinstance(account_key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", account_key):
            raise ValueError("account_key must be a non-secret account alias")
        self.transport = transport if transport is not None else NativeTransport()
        self.config = deepcopy(config)
        self.state_provider, self.market_provider = state_provider, market_provider
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.max_read_attempts = max_read_attempts
        self.account_key = account_key
        if account_identity is not None and (not isinstance(account_identity, str)
                                            or not re.fullmatch(r"[0-9a-f]{64}", account_identity)):
            raise ValueError("account_identity must be a hashed DEMO account identity")
        self.account_identity = account_identity
        if type(require_demo_entry_admission) is not bool:
            raise ValueError("require_demo_entry_admission must be a bool")
        if require_demo_entry_admission:
            # This adds a stricter admission contract; it never grants a new
            # transport capability or changes the real account's trading gate.
            from .demo_route_transport import DemoRouteTransport
            if (type(self.transport) is not DemoRouteTransport or self.transport.mode != "demo" or account_identity is None
                    or not account_key.startswith("DEMO-") or not callable(final_entry_check)):
                raise ValueError("strict DEMO admission requires a bound DEMO transport and final context check")
        elif final_entry_check is not None:
            raise ValueError("final_entry_check requires strict DEMO admission")
        self.require_demo_entry_admission = require_demo_entry_admission
        self.final_entry_check = final_entry_check
        self._last_verified_account_time = None
        self._entry_not_sent_fingerprints: dict[str, str] = {}
        self.mode = getattr(self.transport, "mode", "unknown")
        self._owner = uuid.uuid4().hex
        self._local_halted = False
        root = Path(log_root) / "router"
        root.mkdir(parents=True, exist_ok=True)
        self.run_dir = root / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex)
        self.run_dir.mkdir(exist_ok=False)
        self.audit_path = self.run_dir / "orders.csv"
        with self.audit_path.open("x", encoding="utf-8", newline="") as handle:
            csv.DictWriter(handle, fieldnames=self.AUDIT_FIELDS).writeheader()
        self.journal_path = Path(journal_path) if journal_path is not None else root / "journal.sqlite3"
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        self.halt_path = self.journal_path.parent / ("HALT-" + hashlib.sha256(account_key.encode()).hexdigest()[:12])
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS routes (account TEXT, signal TEXT, fingerprint TEXT NOT NULL, "
                       "client TEXT NOT NULL, status TEXT NOT NULL, owner TEXT NOT NULL, data TEXT NOT NULL, "
                       "PRIMARY KEY(account,signal), UNIQUE(account,client))")
            db.execute("CREATE TABLE IF NOT EXISTS controls (account TEXT PRIMARY KEY, code TEXT NOT NULL)")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.journal_path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    def _now(self) -> int:
        number = _decimal(self.clock())
        if number != number.to_integral_value():
            raise RouterError("CLOCK")
        return int(number)

    def _audit(self, row: Mapping, event: str):
        # No raw responses, exception strings, headers, credentials or config.
        record = {name: row.get(name, "") for name in self.AUDIT_FIELDS}
        record.update(timestamp_ms=self._now(), mode=self.mode, account_key=self.account_key, event=event)
        with self.audit_path.open("a", encoding="utf-8", newline="") as handle:
            csv.DictWriter(handle, fieldnames=self.AUDIT_FIELDS).writerow(record)

    def _save(self, row: dict):
        with self._db() as db:
            db.execute("UPDATE routes SET status=?,data=? WHERE account=? AND signal=? AND owner=?",
                       (row["status"], json.dumps(row, sort_keys=True), self.account_key,
                        row["signal_id"], self._owner))

    def _result(self, row: Mapping) -> RouteResult:
        return RouteResult(row["status"], row["code"], row["signal_id"], row["client_order_id"],
                           _decimal(row.get("filled_qty", 0)), _decimal(row.get("protected_qty", 0)),
                           self.run_dir, row.get("emergency_status", ""))

    def _finish(self, row: dict, status: str, code: str) -> RouteResult:
        row.update(status=status, code=code)
        self._save(row)
        self._audit(row, "result")
        return self._result(row)

    def _halt(self, row: dict, code: str):
        # Storage failure must never prevent an attempt to reduce known exposure.
        self._local_halted = True
        try:
            with self._db() as db:
                db.execute("INSERT OR REPLACE INTO controls(account,code) VALUES(?,?)", (self.account_key, code))
        except (OSError, sqlite3.Error):
            pass
        try:
            self.halt_path.write_text(code + "\n", encoding="utf-8")
        except OSError:
            pass
        row["code"] = code
        self._safety_record(row, "halt")

    def _safety_record(self, row: dict, event: str):
        for operation in (lambda: self._save(row), lambda: self._audit(row, event)):
            try:
                operation()
            except Exception:
                self._local_halted = True

    def _claim(self, intent: Intent, fingerprint: str, client: str) -> tuple[dict, bool]:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT fingerprint,owner,data FROM routes WHERE account=? AND signal=?",
                                  (self.account_key, intent.signal_id)).fetchone()
            if existing:
                if existing[0] != fingerprint:
                    raise RouterError("IDEMPOTENCY_CONFLICT")
                if existing[1]:
                    raise RouterError("IN_PROGRESS")
                row = json.loads(existing[2])
                if self.mode == "demo" and (self.account_identity is None
                        or row.get("account_hash") != self.account_identity):
                    raise RouterError("DEMO_ACCOUNT_CHANGED")
                db.execute("UPDATE routes SET owner=? WHERE account=? AND signal=?",
                           (self._owner, self.account_key, intent.signal_id))
                return row, False
            if (self._local_halted or self.halt_path.exists()
                    or db.execute("SELECT 1 FROM controls WHERE account=?", (self.account_key,)).fetchone()):
                raise RouterError("HALTED")
            statuses = db.execute("SELECT status FROM routes WHERE account=?", (self.account_key,)).fetchall()
            if any(status[0] in ACTIVE_ROUTES for status in statuses):
                raise RouterError("POSITION_LIMIT")
            row = dict(signal_id=intent.signal_id, client_order_id=client, status="RESERVED", code="OK",
                       filled_qty="0", protected_qty="0", avg_price="0", order_id="",
                       entry_state="", protection_id=_derived_id(intent.signal_id, "protection"),
                       protection_order_id="", protection_requested_qty="", cancel_requested=False,
                       close_id="", close_order_id="", close_qty="0", confirmed_closed_qty="0", submit_attempted=False,
                       emergency_status="", close_order_ids=[], side=intent.side, inst_id=intent.inst_id,
                       created_at_ms=intent.created_at_ms, expires_at_ms=intent.expires_at_ms,
                       account_hash=self.account_identity,
                       flat_observed_at_ms=self._last_verified_account_time)
            try:
                db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
                           (self.account_key, intent.signal_id, fingerprint, client, row["status"],
                            self._owner, json.dumps(row, sort_keys=True)))
            except sqlite3.IntegrityError:
                raise RouterError("CLIENT_ID_COLLISION") from None
            return row, True

    def _preflight(self, intent: Intent):
        try:
            state = dict(self.state_provider())
        except Exception:
            raise RouterError("STATE_UNAVAILABLE") from None
        try:
            market = self.market_provider()
            last = _decimal(market["last"], positive=True)
            stamp = _decimal(market["timestamp_ms"])
        except Exception:
            raise RouterError("MARKET_UNAVAILABLE") from None
        checked_monotonic_ns = time.monotonic_ns()
        now = self._now()
        if self.mode in {"native-live", "demo"}:
            label = "DEMO" if self.mode == "demo" else "LIVE"
            verified_field = "demo_verified" if self.mode == "demo" else "live_verified"
            try:
                observed = _decimal(state.get("observed_at_ms"))
            except RouterError:
                raise RouterError(label + "_STATE_UNVERIFIED") from None
            if state.get(verified_field) is not True:
                raise RouterError(label + "_STATE_UNVERIFIED")
            if self.mode == "demo" and state.get("account_hash") != self.account_identity:
                raise RouterError("DEMO_ACCOUNT_CHANGED")
            if observed > now or Decimal(now) - observed > 5000:
                raise RouterError(label + "_STATE_STALE")
        max_age = _decimal(self.config.get("data", {}).get("stale_seconds", {}).get("ticker", 60), positive=True)
        if stamp > now or Decimal(now) - stamp > max_age * 1000:
            raise RouterError("MARKET_STALE")
        admission = None
        if self.mode == "demo" and (self.require_demo_entry_admission
                                    or intent.signal_id.startswith("DEMO-TEST-")):
            if type(state.get("observed_at_ms")) is not int or observed <= 0:
                raise RouterError("DEMO_STATE_UNVERIFIED")
            if (type(market.get("timestamp_ms")) is not int or stamp <= 0
                    or Decimal(now) - stamp > 5000):
                raise RouterError("MARKET_STALE")
            try:
                bid = _decimal(market.get("bid"), positive=True)
                ask = _decimal(market.get("ask"), positive=True)
            except RouterError:
                raise RouterError("MAKER_QUOTE_INVALID") from None
            if bid > ask or (intent.signal_id.startswith("DEMO-TEST-")
                             and intent.require_maker is not True):
                raise RouterError("MAKER_QUOTE_INVALID")
            if intent.require_maker and ((intent.side == "long" and intent.entry >= ask)
                    or (intent.side == "short" and intent.entry <= bid)):
                raise RouterError("MAKER_QUOTE_MOVED")
            valid_until_ms, expiry_code = self._demo_entry_context_window(now)
            admission = _DemoEntryAdmission(intent, int(observed), int(stamp), now,
                        checked_monotonic_ns, bid, ask, valid_until_ms, expiry_code)
        if abs(intent.entry - last) * 100 > last * Decimal("1.5"):
            raise RouterError("PRICE_DEVIATION")
        try:
            limit = notional_limit(state.get("equity_estimate"), self.config)
            actual = intent.qty * intent.contract_value * intent.entry
            if actual > limit or intent.notional != actual:
                raise RouterError("NOTIONAL_LIMIT")
            signal = dict(id=intent.signal_id, instId=intent.inst_id, kind=intent.kind, side=intent.side,
                          entry=intent.entry, stop=intent.stop, tp1=intent.tp1, tp2=intent.tp2,
                          qty=intent.qty, generated_at_ms=intent.created_at_ms,
                          expires_at_ms=intent.expires_at_ms, require_maker=intent.require_maker,
                          slippage_bps=intent.slippage_bps)
            state["now_ms"] = now
            checked = build_intent(signal, state, self.config)
        except ExecutionRejected as error:
            raise RouterError(error.code) from None
        for field in ("qty", "entry", "stop", "tp1", "tp2", "contract_value", "lot_size", "tick_size",
                      "maker_fee_rate", "taker_fee_rate", "require_maker", "slippage_bps"):
            if getattr(checked, field) != getattr(intent, field):
                raise RouterError("RISK_CHANGED")
        if self.mode == "demo":
            self._last_verified_account_time = int(observed)
        return admission

    def _demo_entry_context_window(self, effective_now_ms: int) -> tuple[int | None, str]:
        """Check trusted local source evidence, without refreshing its clocks.

        The service owns the original calendar/source records. Its pure callback
        must also return their earliest exclusive expiry/blackout deadline so
        callback processing time cannot carry the entry over that boundary.
        """
        if not self.require_demo_entry_admission:
            return None, "DEMO_ENTRY_ADMISSION_FAILED"
        try:
            window = self.final_entry_check(effective_now_ms)
        except RouterError:
            raise
        except Exception:
            raise RouterError("DEMO_ENTRY_ADMISSION_FAILED") from None
        if (not isinstance(window, Mapping)
                or type(window.get("valid_until_ms")) is not int
                or window["valid_until_ms"] <= 0
                or type(window.get("code")) is not str
                or window.get("code") not in DEMO_ENTRY_CONTEXT_CODES):
            raise RouterError("DEMO_ENTRY_ADMISSION_FAILED")
        if effective_now_ms >= window["valid_until_ms"]:
            raise RouterError(window["code"])
        return window["valid_until_ms"], window["code"]

    def _demo_entry_effective_time(self, intent: Intent, admission: _DemoEntryAdmission) -> int:
        if type(admission) is not _DemoEntryAdmission or admission.intent is not intent:
            raise RouterError("DEMO_ENTRY_ADMISSION_FAILED")
        now = self._now()
        elapsed_ns = time.monotonic_ns() - admission.monotonic_ns
        if now < admission.checked_ms or elapsed_ns < 0:
            raise RouterError("DEMO_ENTRY_CLOCK_INVALID")
        # Check wall time AND elapsed monotonic time. A clock correction may
        # shorten the window, but must never make old account/quote evidence new.
        elapsed_ms = Decimal(elapsed_ns) / 1_000_000
        for source_ms, code in ((admission.account_ms, "DEMO_STATE_STALE"),
                                (admission.quote_ms, "MARKET_STALE")):
            if (source_ms > now or now - source_ms > 5000
                    or Decimal(admission.checked_ms - source_ms) + elapsed_ms > 5000):
                raise RouterError(code)
        if (now >= intent.expires_at_ms
                or Decimal(admission.checked_ms) + elapsed_ms >= intent.expires_at_ms):
            raise RouterError("EXPIRED")
        # Round elapsed fractions upwards at an inclusive macro-window start.
        # The rollback check above is independent of this conservative maximum.
        return max(now, admission.checked_ms + (elapsed_ns + 999_999) // 1_000_000)

    def _check_demo_entry_admission(self, intent: Intent, admission: _DemoEntryAdmission):
        effective = self._demo_entry_effective_time(intent, admission)
        deadline, code = self._demo_entry_context_window(effective)
        # A callback cannot lengthen the original preflight's trusted window.
        windows = ((admission.context_valid_until_ms, admission.context_expiry_code), (deadline, code))
        effective = self._demo_entry_effective_time(intent, admission)
        for valid_until_ms, expiry_code in windows:
            if valid_until_ms is not None and effective >= valid_until_ms:
                raise RouterError(expiry_code)

    @staticmethod
    def _intent_fingerprint(intent: Intent) -> str:
        return hashlib.sha256(json.dumps(intent.to_dict(), sort_keys=True,
                                        separators=(",", ":")).encode()).hexdigest()

    def _finish_entry_not_sent(self, intent: Intent, row: dict, code: str) -> RouteResult:
        # The transport proves its HTTP callback was never entered. Keep the
        # durable submit-attempt marker; never erase/retry an uncertain write.
        # Keep exact immutable-intent proof even when both the subsequent
        # journal save and the CLI's cleanup encounter a local storage fault.
        self._entry_not_sent_fingerprints[intent.signal_id] = self._intent_fingerprint(intent)
        row.update(entry_not_sent=True, entry_not_sent_code=code)
        try:
            return self._finish(row, "REJECTED", code)
        except Exception:
            # A failed local refusal write must not trigger account cancellation
            # or emergency closing when no entry request has been sent.
            row.update(status="HALTED", code="ENTRY_REFUSAL_PERSIST_FAILED")
            self._halt(row, row["code"])
            self._safety_record(row, "entry_not_sent_persist_failed")
            return self._result(row)

    @staticmethod
    def _ack(response: Mapping) -> bool:
        return isinstance(response, Mapping) and str(response.get("sCode", "")) == "0"

    def _read(self, method: str, *args):
        for _ in range(self.max_read_attempts):
            try:
                response = getattr(self.transport, method)(*args)
            except Exception:
                continue
            if isinstance(response, Mapping) and response:
                return response
        return None

    def _entry_snapshot(self, intent: Intent, row: dict, *, safety: bool = False) -> Mapping:
        snapshot = self._read("get_order", intent.inst_id, row["client_order_id"])
        if snapshot is None:
            raise RouterError("SUBMISSION_UNKNOWN")
        expected_side = "buy" if intent.side == "long" else "sell"
        if (snapshot.get("instId") != intent.inst_id or snapshot.get("clOrdId") != row["client_order_id"]
                or snapshot.get("side") != expected_side or not snapshot.get("ordId")
                or snapshot.get("posSide") != "net"
                or snapshot.get("ordType") != ("post_only" if intent.require_maker else "limit")):
            raise RouterError("ORDER_MISMATCH")
        if row["order_id"] and str(snapshot["ordId"]) != row["order_id"]:
            raise RouterError("ORDER_MISMATCH")
        state = snapshot.get("state")
        if state not in TERMINAL_ENTRY | {"live", "partially_filled"}:
            raise RouterError("INVALID_SNAPSHOT")
        filled = _decimal(snapshot.get("accFillSz"))
        if (_decimal(snapshot.get("sz"), positive=True) != intent.qty or filled > intent.qty
                or filled < _decimal(row["filled_qty"]) or filled % intent.lot_size):
            raise RouterError("INVALID_SNAPSHOT")
        if state == "filled" and filled != intent.qty:
            raise RouterError("INVALID_SNAPSHOT")
        if filled:
            average = _decimal(snapshot.get("avgPx"), positive=True)
            row["avg_price"] = str(average)
        row.update(order_id=str(snapshot["ordId"]), entry_state=state, filled_qty=str(filled))
        if safety:
            self._safety_record(row, "entry_observed")
        else:
            self._save(row)
            self._audit(row, "entry_observed")
        return snapshot

    def _protection_valid(self, snapshot, intent: Intent, row: dict, qty: Decimal) -> bool:
        if not isinstance(snapshot, Mapping):
            return False
        expected_side = "sell" if intent.side == "long" else "buy"
        if (snapshot.get("instId") != intent.inst_id or snapshot.get("algoClOrdId") != row["protection_id"]
                or not snapshot.get("algoId") or snapshot.get("state") != "live"
                or snapshot.get("ordType") != "oco" or snapshot.get("side") != expected_side
                or snapshot.get("posSide") != "net" or str(snapshot.get("slOrdPx")) != "-1"
                or str(snapshot.get("tpOrdPx")) != "-1"
                or any(snapshot.get(key, "") not in ("", "last") for key in ("slTriggerPxType", "tpTriggerPxType"))
                or str(snapshot.get("reduceOnly", "")).lower() != "true"):
            return False
        if row["protection_order_id"] and str(snapshot["algoId"]) != row["protection_order_id"]:
            return False
        try:
            return (_decimal(snapshot.get("sz"), positive=True) == qty
                    and _decimal(snapshot.get("slTriggerPx"), positive=True) == intent.stop
                    and _decimal(snapshot.get("tpTriggerPx"), positive=True) == intent.tp1)
        except RouterError:
            return False

    def _protect(self, intent: Intent, row: dict) -> bool:
        qty = _decimal(row["filled_qty"])
        if qty <= ZERO:
            return True
        requested = row["protection_requested_qty"]
        if requested != str(qty):
            row.update(status="PROTECTING", protection_requested_qty=str(qty))
            self._save(row)
            self._audit(row, "protection_request")
            try:
                response = self.transport.set_protection(intent, qty, row["protection_id"], reduce_only=True)
                if isinstance(response, Mapping) and "sCode" in response and str(response["sCode"]) != "0":
                    return False
                if self._ack(response) and response.get("algoId"):
                    row["protection_order_id"] = str(response["algoId"])
                    self._save(row)
            except Exception:
                pass  # The request may have succeeded; verify by its stable id.
        snapshot = self._read("get_protection", intent.inst_id, row["protection_id"])
        if not self._protection_valid(snapshot, intent, row, qty):
            return False
        row.update(protected_qty=str(qty), protection_order_id=str(snapshot["algoId"]))
        self._save(row)
        self._audit(row, "protection_verified")
        return True

    def _cancel(self, intent: Intent, row: dict, *, safety: bool = False):
        if not row["cancel_requested"]:
            row["cancel_requested"] = True
            if safety:
                self._safety_record(row, "cancel_request")
            else:
                self._save(row)
                self._audit(row, "cancel_request")
            try:
                self.transport.cancel_entry(intent.inst_id, row["client_order_id"])
            except Exception:
                pass  # ACK, timeout and rejection all require independent re-query.

    def _prove_demo_lifecycle(self, intent: Intent, row: dict, position_id: str, opened: int):
        proof = self.transport.verify_position_lifecycle(
            intent, row["order_id"], row["client_order_id"], _decimal(row["filled_qty"]),
            row.get("flat_observed_at_ms"))
        observed = proof.get("observed_at_ms") if isinstance(proof, Mapping) else None
        if (not isinstance(proof, Mapping) or proof.get("position_binding_verified") is not True
                or proof.get("position_id") != position_id or proof.get("position_opened_at_ms") != opened
                or proof.get("flat_observed_at_ms") != row.get("flat_observed_at_ms")
                or type(observed) is not int or not 0 <= self._now()-observed <= 5000):
            raise RouterError("POSITION_LIFECYCLE_UNPROVEN")
        payload = json.dumps(proof, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        row.update(position_binding_observed_at_ms=observed, position_binding_sha256=digest)
        try:
            (self.run_dir / ("lifecycle-" + digest + ".json")).write_text(payload, encoding="utf-8")
        except OSError:
            # A proven exposure must still be reducible when its local audit
            # disk fails. Freeze new entries without suppressing emergency IO.
            self._halt(row, "LIFECYCLE_AUDIT_UNAVAILABLE")

    def _demo_reduction_scope(self, intent: Intent, row: dict, remaining: Decimal) -> str:
        """An old entry fill is not authority to reduce an unrelated new slot."""
        exposure = self.transport.read_exposure()
        if (not isinstance(exposure, Mapping) or any(not isinstance(exposure.get(key), list)
                for key in ("positions", "pending", "algos"))):
            raise RouterError("DEMO_REDUCTION_SCOPE_UNKNOWN")
        positions = []
        for position in exposure["positions"]:
            if not isinstance(position, Mapping):
                raise RouterError("DEMO_REDUCTION_SCOPE_UNKNOWN")
            size = Decimal(str(position.get("pos")))
            if not size.is_finite():
                raise RouterError("DEMO_REDUCTION_SCOPE_UNKNOWN")
            if size:
                positions.append((position, size))
        for pending in exposure["pending"]:
            if (not isinstance(pending, Mapping) or pending.get("instId") != intent.inst_id
                    or pending.get("clOrdId") != row["client_order_id"]):
                raise RouterError("DEMO_REDUCTION_EXTERNAL_EXPOSURE")
        if not positions:
            return "FLAT" if (row["entry_state"] in TERMINAL_ENTRY
                              and not exposure["pending"] and not exposure["algos"]) else "UNKNOWN"
        if len(positions) != 1:
            raise RouterError("DEMO_REDUCTION_EXTERNAL_EXPOSURE")
        position, size = positions[0]
        expected = remaining if intent.side == "long" else -remaining
        position_id, opened_raw = position.get("posId"), position.get("cTime")
        if (position.get("instId") != intent.inst_id or position.get("posSide") != "net"
                or position.get("mgnMode") != "cross" or size != expected
                or not isinstance(position_id, str) or not position_id
                or not isinstance(opened_raw, str) or not opened_raw.isascii() or not opened_raw.isdigit()):
            raise RouterError("DEMO_REDUCTION_SCOPE_UNKNOWN")
        opened = int(opened_raw)
        if (not intent.created_at_ms - 2000 <= opened <= self._now()
                or row.get("position_id") not in (None, position_id)
                or row.get("position_opened_at_ms") not in (None, opened)):
            raise RouterError("DEMO_REDUCTION_EXTERNAL_EXPOSURE")
        if row.get("position_id") is None or row.get("position_opened_at_ms") is None:
            self._prove_demo_lifecycle(intent, row, position_id, opened)
        row.update(position_id=position_id, position_opened_at_ms=opened)
        self._safety_record(row, "emergency_lifecycle_verified")
        return "OPEN"

    def _demo_account_clear(self) -> bool:
        """Current complete account evidence, never entry/close arithmetic."""
        try:
            exposure = self.transport.read_exposure()
            if (not isinstance(exposure, Mapping) or any(not isinstance(exposure.get(key), list)
                    for key in ("positions", "pending", "algos"))):
                return False
            for position in exposure["positions"]:
                if not isinstance(position, Mapping):
                    return False
                quantity = Decimal(str(position.get("pos")))
                if not quantity.is_finite() or quantity != ZERO:
                    return False
            return not exposure["pending"] and not exposure["algos"]
        except Exception:
            return False

    def _emergency(self, intent: Intent, row: dict, code: str, *, status: str = "HALTED") -> RouteResult:
        if (self.mode == "demo" and self._entry_not_sent_fingerprints.get(intent.signal_id)
                == self._intent_fingerprint(intent)):
            # The CLI/controller may re-enter cleanup using the older durable
            # SUBMITTING row after refusal persistence failed. No entry HTTP
            # was called for this exact intent: cleanup is local HALT only.
            row.update(status="HALTED", code="ENTRY_REFUSAL_PERSIST_FAILED",
                       entry_not_sent=True)
            self._halt(row, row["code"])
            self._safety_record(row, "entry_not_sent_cleanup")
            return self._result(row)
        self._halt(row, code)
        if self.mode == "demo" and row.get("planned_close_code") and row.get("close_attempted"):
            return self._planned_emergency_observe(intent, row, code)
        if row["entry_state"] not in TERMINAL_ENTRY:
            self._cancel(intent, row, safety=True)
        row["emergency_status"] = "NO_CONFIRMED_FILL"
        for _ in range(self.max_read_attempts):
            known = False
            try:
                self._entry_snapshot(intent, row, safety=True)
                known = True
            except RouterError:
                pass
            filled = _decimal(row["filled_qty"])
            reduced = _decimal(row.get("confirmed_closed_qty", "0"))
            if filled <= reduced:
                if known and row["entry_state"] in TERMINAL_ENTRY:
                    if self.mode != "demo" or self._demo_account_clear():
                        row["emergency_status"] = "FLAT_CONFIRMED"
                        row["protected_qty"] = "0"
                    else:
                        row["emergency_status"] = "EXPOSURE_REQUIRES_REVIEW"
                    break
                if not known:
                    break
                continue
            side = "sell" if intent.side == "long" else "buy"
            if not row["close_id"]:
                qty = filled - reduced
                if self.mode == "demo":
                    try:
                        scope = self._demo_reduction_scope(intent, row, qty)
                    except Exception:
                        row["emergency_status"] = "EXPOSURE_UNVERIFIED"
                        break
                    if scope == "FLAT":
                        row.update(emergency_status="FLAT_CONFIRMED", protected_qty="0")
                        break
                    if scope != "OPEN":
                        row["emergency_status"] = "EXPOSURE_UNVERIFIED"
                        break
                role = "emergency:" + str(reduced) + ":" + str(qty)
                row.update(close_id=_derived_id(intent.signal_id, role), close_order_id="", close_qty=str(qty))
                self._safety_record(row, "emergency_close_request")
                try:
                    response = self.transport.emergency_close(intent.inst_id, side, qty, row["close_id"], reduce_only=True)
                    if self._ack(response) and response.get("ordId"):
                        row["close_order_id"] = str(response["ordId"])
                        self._safety_record(row, "emergency_close_ack")
                except Exception:
                    pass
            closed = self._read("get_order", intent.inst_id, row["close_id"])
            confirmed = False
            if isinstance(closed, Mapping):
                try:
                    confirmed = (closed.get("instId") == intent.inst_id
                                 and closed.get("clOrdId") == row["close_id"]
                                 and closed.get("side") == side and closed.get("state") == "filled"
                                 and bool(closed.get("ordId")) and closed.get("posSide") == "net"
                                 and (not row["close_order_id"] or str(closed["ordId"]) == row["close_order_id"])
                                 and closed.get("ordType") == "market"
                                 and str(closed.get("reduceOnly", "")).lower() == "true"
                                 and _decimal(closed.get("sz")) == _decimal(row["close_qty"])
                                 and _decimal(closed.get("accFillSz")) == _decimal(row["close_qty"]))
                except RouterError:
                    pass
            if not confirmed:
                row["emergency_status"] = "CLOSE_UNCONFIRMED"
                break  # Never resend a close whose result is uncertain.
            reduced += _decimal(row["close_qty"])
            confirmed_close_ids = row.setdefault("close_order_ids", [])
            if str(closed["ordId"]) not in confirmed_close_ids:
                confirmed_close_ids.append(str(closed["ordId"]))
            row.update(confirmed_closed_qty=str(reduced), close_id="", close_order_id="", close_qty="0",
                       emergency_status="CLOSE_UNCONFIRMED")
            self._safety_record(row, "emergency_close_verified")
            if known and row["entry_state"] in TERMINAL_ENTRY and filled == reduced:
                if self.mode != "demo" or self._demo_account_clear():
                    row.update(emergency_status="FLAT_CONFIRMED", protected_qty="0")
                else:
                    row["emergency_status"] = "EXPOSURE_REQUIRES_REVIEW"
                break
            # Cancellation can race with another entry fill. Re-query and reduce
            # only an additionally confirmed remainder under a different stable id.
        row.update(status=status, code=code)
        self._safety_record(row, "result")
        return self._result(row)

    def _advance(self, intent: Intent, row: dict, *, cancel_entry: bool = False) -> RouteResult:
        for _ in range(self.max_read_attempts):
            try:
                self._entry_snapshot(intent, row)
            except RouterError as error:
                return self._emergency(intent, row, error.code, status="UNKNOWN")
            filled = _decimal(row["filled_qty"])
            if filled and not self._protect(intent, row):
                return self._emergency(intent, row, "PROTECTION_FAILED")
            if row["entry_state"] in TERMINAL_ENTRY:
                return self._finish(row, "PROTECTED" if filled else "CANCELED", "OK" if filled else "CANCELED")
            if cancel_entry or filled or self._now() >= intent.expires_at_ms:
                self._cancel(intent, row)
                continue
            return self._finish(row, "PENDING", "PENDING")
        return self._emergency(intent, row, "CANCEL_UNKNOWN", status="UNKNOWN")

    def execute(self, intent: Intent) -> RouteResult:
        """Execute/reconcile an approved intent; never infer human approval."""
        return self._execute(intent)

    def reconcile_existing(self, intent: Intent) -> RouteResult:
        """Reconcile a durable route without ever creating a new entry."""
        return self._execute(intent, existing_only=True)

    def recover_demo_owners(self) -> int:
        """Release dead process ownership only under the service's OS lock.

        The dedicated service calls this after obtaining its exclusive
        process.lock and binding the DEMO UID. Status, intent fingerprint,
        submit checkpoint and all HALTs remain intact. Recovery must next use
        reconcile_existing/cancel_and_reconcile, never submit a replacement.
        """
        from .demo_route_transport import DemoRouteTransport
        if (self.mode != "demo" or type(self.transport) is not DemoRouteTransport
                or not self.account_key.startswith("DEMO-") or self.account_identity is None):
            raise RouterError("DEMO_RECOVERY_BINDING_REQUIRED")
        self.transport.assert_demo_enabled()
        recovered = []
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            records = db.execute("SELECT signal,client,status,data FROM routes WHERE account=? AND owner<>''",
                                 (self.account_key,)).fetchall()
            # Validate the whole set before releasing even one owner.
            for signal, client, status, raw in records:
                row = json.loads(raw)
                if (not isinstance(row, dict) or row.get("account_hash") != self.account_identity
                        or row.get("signal_id") != signal or row.get("client_order_id") != client
                        or client != client_order_id(signal) or row.get("status") != status
                        or row.get("inst_id") != "SOL-USDT-SWAP"
                        or row.get("side") not in {"long", "short"}):
                    raise RouterError("DEMO_RECOVERY_JOURNAL_INVALID")
                recovered.append(row)
            db.execute("UPDATE routes SET owner='' WHERE account=? AND owner<>''", (self.account_key,))
        for row in recovered:
            try:
                self._audit(row, "orphan_owner_released_for_reconciliation")
            except Exception:
                self._local_halted = True  # Existing orders still get reconciled.
        return len(recovered)

    def _demo_existing_claim(self, intent: Intent) -> dict:
        if self.mode != "demo" or not isinstance(intent, Intent):
            raise RouterError("DEMO_EXISTING_REQUIRED")
        from .demo_route_transport import DemoRouteTransport
        if type(self.transport) is not DemoRouteTransport:
            raise RouterError("DEMO_TRANSPORT_REQUIRED")
        self.transport.assert_demo_enabled()
        if not self.account_key.startswith("DEMO-"):
            raise RouterError("DEMO_ACCOUNT_NAMESPACE")
        with self._db() as db:
            found = db.execute("SELECT 1 FROM routes WHERE account=? AND signal=?",
                               (self.account_key, intent.signal_id)).fetchone()
        if not found:
            raise RouterError("RECONCILE_NOT_FOUND")
        fingerprint = hashlib.sha256(json.dumps(intent.to_dict(), sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
        row, new = self._claim(intent, fingerprint, client_order_id(intent.signal_id))
        if new:
            self._halt(row, "JOURNAL_CHANGED")
            raise RouterError("JOURNAL_CHANGED")
        return row

    def _demo_halted_readonly_claim(self, intent: Intent) -> dict:
        """Claim an existing frozen route without acquiring any trading capability."""
        from .demo_route_transport import DemoRouteTransport
        if (self.mode != "demo" or type(self.transport) is not DemoRouteTransport
                or not isinstance(intent, Intent) or not self.account_key.startswith("DEMO-")
                or not isinstance(self.account_identity, str)
                or not re.fullmatch(r"[0-9a-f]{64}", self.account_identity)):
            raise RouterError("DEMO_HALTED_READONLY_SCOPE")
        fingerprint = hashlib.sha256(json.dumps(intent.to_dict(), sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT fingerprint,client,status,owner,data FROM routes WHERE account=? AND signal=?",
                                  (self.account_key, intent.signal_id)).fetchone()
            if not existing:
                raise RouterError("RECONCILE_NOT_FOUND")
            if existing[0] != fingerprint or existing[1] != client_order_id(intent.signal_id):
                raise RouterError("IDEMPOTENCY_CONFLICT")
            if existing[3]:
                raise RouterError("IN_PROGRESS")
            row = json.loads(existing[4])
            if (not isinstance(row, dict) or row.get("status") != existing[2]
                    or row.get("status") not in {"HALTED", "UNKNOWN"}
                    or row.get("account_hash") != self.account_identity
                    or row.get("signal_id") != intent.signal_id
                    or row.get("client_order_id") != existing[1] or row.get("side") != intent.side
                    or row.get("inst_id") != intent.inst_id
                    or row.get("created_at_ms") != intent.created_at_ms
                    or row.get("expires_at_ms") != intent.expires_at_ms
                    or not row.get("submit_attempted") or not row.get("code")):
                raise RouterError("DEMO_HALTED_ROUTE_MISMATCH")
            db.execute("UPDATE routes SET owner=? WHERE account=? AND signal=? AND owner=''",
                       (self._owner, self.account_key, intent.signal_id))
        return row

    def _store_halted_exit_proof(self, payload: str, digest: str) -> Path:
        """Publish complete immutable proof bytes; never truncate an old proof."""
        expected = payload.encode("utf-8")
        if hashlib.sha256(expected).hexdigest() != digest:
            raise RouterError("DEMO_HALTED_EXIT_PROOF_INVALID")
        destination = self.run_dir / ("halted-exit-" + digest + ".json")

        def reuse_existing():
            if destination.is_symlink():
                raise RouterError("DEMO_HALTED_EXIT_PROOF_INVALID")
            actual = destination.read_bytes()
            if actual != expected or hashlib.sha256(actual).hexdigest() != digest:
                raise RouterError("DEMO_HALTED_EXIT_PROOF_INVALID")

        if destination.exists() or destination.is_symlink():
            reuse_existing()
            return destination
        temporary = self.run_dir / ("halted-exit-" + uuid.uuid4().hex + ".tmp")
        try:
            with temporary.open("xb") as handle:
                if handle.write(expected) != len(expected):
                    raise RouterError("DEMO_HALTED_EXIT_PROOF_PERSIST_FAILED")
                handle.flush()
                os.fsync(handle.fileno())
            try:
                # Hard-link publication is atomic and refuses an existing name.
                # os.replace would overwrite a proof published by another caller.
                os.link(temporary, destination)
            except FileExistsError:
                reuse_existing()
            return destination
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def reconcile_halted_exit(self, intent: Intent) -> RouteResult:
        """Archive GET-only closure evidence while preserving the sticky HALT.

        This never invokes emergency handling, creates an entry, clears controls,
        or releases a halted route's position slot. Failures retain the original
        route status/code; only a separate reconciliation result may be recorded.
        """
        from .demo_route_transport import DemoRouteError
        row, original = None, None
        stage = "CLAIM"
        try:
            row = self._demo_halted_readonly_claim(intent)
            original = deepcopy(row)
            # This caller interval starts before every transport GET. Server
            # public/time splits history queries but may follow an exposure
            # observation, so it cannot be the only freshness anchor.
            stage = "START"
            collection_started_wall = self._now()
            collection_started_mono = time.monotonic_ns()
            if collection_started_wall <= 0:
                raise RouterError("DEMO_HALTED_EXIT_START_WALL_INVALID")
            if type(collection_started_mono) is not int or collection_started_mono < 0:
                raise RouterError("DEMO_HALTED_EXIT_START_MONO_INVALID")
            stage = "COLLECT"
            proof = self.transport.verify_closed_position_lifecycle(
                intent, row.get("order_id"), row["client_order_id"], _decimal(row.get("filled_qty"), positive=True),
                row.get("flat_observed_at_ms"), row.get("protection_id"), row.get("protection_order_id"))
            stage = "END"
            collection_completed_wall = self._now()
            collection_completed_mono = time.monotonic_ns()
            stage = "CHECK"
            observed = proof.get("observed_at_ms") if isinstance(proof, Mapping) else None
            if not isinstance(proof, Mapping):
                raise RouterError("DEMO_HALTED_EXIT_PROOF_SHAPE_INVALID")
            if proof.get("closed_lifecycle_verified") is not True:
                raise RouterError("DEMO_HALTED_EXIT_CLOSED_FLAG_INVALID")
            if proof.get("position_binding_verified") is not True:
                raise RouterError("DEMO_HALTED_EXIT_BINDING_FLAG_INVALID")
            if proof.get("exit_reconciled") is not True:
                raise RouterError("DEMO_HALTED_EXIT_RECONCILED_FLAG_INVALID")
            if proof.get("account_hash") != self.account_identity:
                raise RouterError("DEMO_HALTED_EXIT_ACCOUNT_MISMATCH")
            if proof.get("flat_observed_at_ms") != row.get("flat_observed_at_ms"):
                raise RouterError("DEMO_HALTED_EXIT_FLAT_MISMATCH")
            if not 0 <= collection_completed_wall-collection_started_wall <= 5000:
                raise RouterError("DEMO_HALTED_EXIT_CALLER_WALL_STALE")
            if type(collection_completed_mono) is not int:
                raise RouterError("DEMO_HALTED_EXIT_CALLER_MONO_TYPE_INVALID")
            if not 0 <= collection_completed_mono-collection_started_mono <= 5_000_000_000:
                raise RouterError("DEMO_HALTED_EXIT_CALLER_MONO_STALE")
            if type(observed) is not int:
                raise RouterError("DEMO_HALTED_EXIT_SOURCE_TIME_TYPE_INVALID")
            if not 0 <= collection_completed_wall-observed <= 5000:
                raise RouterError("DEMO_HALTED_EXIT_SOURCE_TIME_STALE")
            if row.get("position_id") not in (None, "", proof.get("position_id")):
                raise RouterError("DEMO_HALTED_EXIT_POSITION_ID_MISMATCH")
            if row.get("position_opened_at_ms") not in (None, proof.get("position_opened_at_ms")):
                raise RouterError("DEMO_HALTED_EXIT_POSITION_TIME_MISMATCH")
            stage = "BIND"
            binding = {key: proof[key] for key in ("position_id", "position_opened_at_ms",
                       "position_closed_at_ms", "exit_order_ids", "confirmed_closed_qty")}
            if row.get("exit_reconciled") is True and any(row.get(key) != value for key, value in binding.items()):
                raise RouterError("DEMO_HALTED_EXIT_BINDING_CHANGED")
            stage = "SERIALIZE"
            payload = json.dumps(proof, sort_keys=True, default=str)
            digest = hashlib.sha256(payload.encode()).hexdigest()
            stage = "ARCHIVE"
            proof_path = self._store_halted_exit_proof(payload, digest)
            row.update(binding, exit_reconciled=True, protected_qty="0",
                       exit_reconcile_code="DEMO_HALTED_EXIT_VERIFIED",
                       exit_reconciled_observed_at_ms=observed, exit_reconcile_sha256=digest,
                       exit_reconcile_proof_path=str(proof_path),
                       exit_reconcile_collection_started_at_ms=collection_started_wall,
                       exit_reconcile_collection_completed_at_ms=collection_completed_wall,
                       exit_reconcile_collection_elapsed_monotonic_ns=collection_completed_mono-collection_started_mono)
            # Compare against the complete claimed record in the same commit.
            # Even a local conflict must never overwrite a newer binding.
            stage = "JOURNAL"
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                found = db.execute("SELECT data,status,owner FROM routes WHERE account=? AND signal=?",
                                   (self.account_key, intent.signal_id)).fetchone()
                if (not found or found[1] != original["status"] or found[2] != self._owner
                        or json.loads(found[0]) != original):
                    raise RouterError("DEMO_HALTED_EXIT_JOURNAL_CHANGED")
                self._audit(row, "halted_exit_verified_no_resume")
                changed = db.execute("UPDATE routes SET data=? WHERE account=? AND signal=? AND owner=?",
                           (json.dumps(row, sort_keys=True), self.account_key, intent.signal_id, self._owner))
                if changed.rowcount != 1:
                    raise RouterError("DEMO_HALTED_EXIT_PERSIST_FAILED")
            stage = "RESULT"
            return self._result(dict(row, emergency_status="FLAT_EXIT_RECONCILED_HALTED"))
        except Exception as error:
            if isinstance(error, (RouterError, DemoRouteError)):
                code = error.code
            else:
                # Both parts are fixed local enums. Never inspect exception
                # text, class names, response bodies or account identifiers.
                if isinstance(error, OSError):
                    category = "IO"
                elif isinstance(error, sqlite3.Error):
                    category = "SQLITE"
                elif isinstance(error, TypeError):
                    category = "TYPE"
                elif isinstance(error, ValueError):
                    category = "VALUE"
                elif isinstance(error, LookupError):
                    category = "LOOKUP"
                elif isinstance(error, ArithmeticError):
                    category = "ARITHMETIC"
                else:
                    category = "OTHER"
                code = "DEMO_HALTED_EXIT_" + stage + "_" + category + "_ERROR"
            if not isinstance(code, str) or not re.fullmatch(r"[A-Z0-9_]{1,64}", code):
                code = "DEMO_HALTED_EXIT_UNPROVEN"
            fallback = deepcopy(original) if original is not None else dict(
                signal_id=getattr(intent, "signal_id", ""), client_order_id="", status="UNKNOWN",
                code=code, filled_qty="0", protected_qty="0")
            if row is not None and original is not None:
                # No _halt/_emergency and no write-enabled claim on this path.
                # Failure metadata is best effort and cannot erase prior proof.
                failed = dict(original, exit_reconcile_code=code)
                try:
                    with self._db() as db:
                        found = db.execute("SELECT data FROM routes WHERE account=? AND signal=? AND owner=?",
                                   (self.account_key, intent.signal_id, self._owner)).fetchone()
                        if found and json.loads(found[0]) == original:
                            db.execute("UPDATE routes SET data=? WHERE account=? AND signal=? AND owner=?",
                                (json.dumps(failed, sort_keys=True), self.account_key, intent.signal_id, self._owner))
                    self._audit(failed, "halted_exit_unverified_no_account_write")
                except Exception:
                    pass
            return self._result(dict(fallback, emergency_status="EXIT_RECONCILIATION_UNVERIFIED"))
        finally:
            if row is not None:
                self._release_owner(intent)

    def _release_owner(self, intent: Intent):
        try:
            with self._db() as db:
                db.execute("UPDATE routes SET owner='' WHERE account=? AND signal=? AND owner=?",
                           (self.account_key, intent.signal_id, self._owner))
        except (OSError, sqlite3.Error):
            self._local_halted = True

    def emergency_reduce(self, intent: Intent, code: str = "DEMO_PROTECTION_LOST") -> RouteResult:
        """Reduce only the already owned DEMO route, keeping a sticky HALT."""
        row = None
        safe_code = code if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,64}", code) else "DEMO_SAFETY_HALT"
        try:
            row = self._demo_existing_claim(intent)
            if row["status"] in {"CLOSED", "CANCELED", "REJECTED"}:
                # A historical fill must never reduce a later position. The
                # closed lifecycle has already released its account slot.
                return self._result(row)
            return self._emergency(intent, row, safe_code)
        except Exception:
            fallback = row or dict(signal_id=getattr(intent, "signal_id", ""), client_order_id="",
                                   status="UNKNOWN", code="DEMO_SAFETY_UNKNOWN", filled_qty="0", protected_qty="0")
            self._halt(fallback, "DEMO_SAFETY_UNKNOWN")
            fallback.update(status="UNKNOWN", code="DEMO_SAFETY_UNKNOWN")
            return self._result(fallback)
        finally:
            if row is not None:
                self._release_owner(intent)

    def resume_emergency(self, intent: Intent) -> RouteResult:
        """Query an existing emergency receipt; never send a write on HALT."""
        row = None
        try:
            row = self._demo_existing_claim(intent)
            if row["status"] in {"CLOSED", "CANCELED", "REJECTED"}:
                return self._result(row)
            if row.get("planned_close_code") and row.get("close_attempted"):
                return self._planned_emergency_observe(intent, row, row.get("code", "DEMO_PLANNED_SAFETY_HALT"))
            close_id = row.get("close_id")
            if close_id:
                closed = self._read("get_order", intent.inst_id, close_id)
                side = "sell" if intent.side == "long" else "buy"
                confirmed = (isinstance(closed, Mapping) and closed.get("instId") == intent.inst_id
                             and closed.get("clOrdId") == close_id and closed.get("side") == side
                             and closed.get("state") == "filled" and bool(closed.get("ordId"))
                             and closed.get("posSide") == "net" and closed.get("ordType") == "market"
                             and str(closed.get("reduceOnly", "")).lower() == "true"
                             and (not row.get("close_order_id") or str(closed["ordId"]) == row["close_order_id"])
                             and _decimal(closed.get("sz")) == _decimal(row["close_qty"])
                             and _decimal(closed.get("accFillSz")) == _decimal(row["close_qty"]))
                if confirmed:
                    reduced = _decimal(row.get("confirmed_closed_qty", "0")) + _decimal(row["close_qty"])
                    ids = row.setdefault("close_order_ids", [])
                    if str(closed["ordId"]) not in ids:
                        ids.append(str(closed["ordId"]))
                    row.update(confirmed_closed_qty=str(reduced), close_id="", close_order_id="", close_qty="0")
                    self._entry_snapshot(intent, row, safety=True)
                    if row["entry_state"] in TERMINAL_ENTRY and reduced == _decimal(row["filled_qty"]):
                        row.update(emergency_status="FLAT_CONFIRMED", protected_qty="0")
                    else:
                        row["emergency_status"] = "REMAINDER_REQUIRES_REVIEW"
                else:
                    row["emergency_status"] = "CLOSE_UNCONFIRMED"
            if row.get("emergency_status") == "FLAT_CONFIRMED" and not self._demo_account_clear():
                row["emergency_status"] = "EXPOSURE_REQUIRES_REVIEW"
            row.update(status="HALTED")
            self._safety_record(row, "halted_emergency_receipt_check")
            return self._result(row)
        except Exception:
            fallback = row or dict(signal_id=getattr(intent, "signal_id", ""), client_order_id="",
                                   status="UNKNOWN", code="EMERGENCY_QUERY_UNVERIFIED",
                                   filled_qty="0", protected_qty="0")
            self._halt(fallback, "EMERGENCY_QUERY_UNVERIFIED")
            fallback.update(status="UNKNOWN", code="EMERGENCY_QUERY_UNVERIFIED")
            return self._result(fallback)
        finally:
            if row is not None:
                self._release_owner(intent)

    def reconcile_exit(self, intent: Intent) -> RouteResult:
        """Release a DEMO slot only after independent flat/no-orders evidence.

        An exchange ACK, vanished OCO, or a local timer is never proof of exit.
        Unrelated exposure is reported and never flattened by this method.
        """
        row = None
        try:
            row = self._demo_existing_claim(intent)
            if row["status"] == "CLOSING":
                return self._planned_close_claimed(intent, row, row.get("planned_close_code", "DEMO_TECHNICAL_TEST_EXIT"))
            if row["status"] not in {"PROTECTED", "CLOSED"}:
                raise RouterError("EXIT_ROUTE_STATE")
            exposure = self.transport.read_exposure()
            if not isinstance(exposure, Mapping) or any(
                    not isinstance(exposure.get(key), list) or
                    any(not isinstance(item, Mapping) for item in exposure[key])
                    for key in ("positions", "pending", "algos")):
                raise RouterError("EXIT_EVIDENCE_UNKNOWN")
            positions = []
            for position in exposure["positions"]:
                size = Decimal(str(position["pos"]))
                if not size.is_finite():
                    raise RouterError("EXIT_EVIDENCE_UNKNOWN")
                if size:
                    positions.append((position, size))
            pending, algos = exposure["pending"], exposure["algos"]
            if not positions and not pending and not algos:
                row.update(protected_qty="0")
                return self._finish(row, "CLOSED", "EXIT_CONFIRMED")
            expected = _decimal(row["filled_qty"]) * (1 if intent.side == "long" else -1)
            own_position = (len(positions) == 1 and positions[0][0].get("instId") == intent.inst_id
                            and positions[0][0].get("posSide") == "net"
                            and positions[0][0].get("mgnMode") == "cross")
            if not own_position:
                raise RouterError("EXIT_EXTERNAL_OR_ORPHAN_EXPOSURE")
            if positions[0][1] != expected or pending or len(algos) != 1:
                return self._emergency(intent, row, "DEMO_EXPOSURE_MISMATCH")
            # Preserve exchange-observed lifecycle identity for the accounting
            # join. Never invent posId/cTime from local request assumptions.
            observed_position = positions[0][0]
            position_id = observed_position.get("posId")
            position_opened = observed_position.get("cTime")
            if (isinstance(position_id, str) and position_id
                    and isinstance(position_opened, str) and position_opened.isascii()
                    and position_opened.isdigit()):
                opened = int(position_opened)
                if not intent.created_at_ms - 2000 <= opened <= self._now():
                    raise RouterError("POSITION_LIFECYCLE_MISMATCH")
                if (row.get("position_id") not in (None, position_id)
                        or row.get("position_opened_at_ms") not in (None, opened)):
                    raise RouterError("POSITION_LIFECYCLE_MISMATCH")
                if row.get("position_id") is None or row.get("position_opened_at_ms") is None:
                    self._prove_demo_lifecycle(intent, row, position_id, opened)
                row.update(position_id=position_id, position_opened_at_ms=opened)
                self._save(row)
            try:
                protection = self.transport.get_protection(intent.inst_id, row["protection_id"])
            except Exception:
                return self._emergency(intent, row, "DEMO_PROTECTION_UNKNOWN")
            if (not self._protection_valid(protection, intent, row, abs(expected))
                    or str(algos[0].get("algoId")) != str(protection.get("algoId"))):
                return self._emergency(intent, row, "DEMO_PROTECTION_LOST")
            return self._finish(row, "PROTECTED", "OK")
        except Exception:
            fallback = row or dict(signal_id=getattr(intent, "signal_id", ""), client_order_id="",
                                   status="UNKNOWN", code="EXIT_STATE_UNKNOWN", filled_qty="0", protected_qty="0")
            self._halt(fallback, "EXIT_STATE_UNKNOWN")
            fallback.update(status="UNKNOWN", code="EXIT_STATE_UNKNOWN")
            self._safety_record(fallback, "exit_unknown")
            return self._result(fallback)
        finally:
            if row is not None:
                self._release_owner(intent)

    def _planned_protection(self, snapshot, intent: Intent, row: dict, qty: Decimal) -> bool:
        """Match the complete immutable bracket, including a terminal snapshot."""
        if not isinstance(snapshot, Mapping) or snapshot.get("state") not in {
                "live", "effective", "canceled", "order_failed"}:
            return False
        check = dict(snapshot, state="live")
        return (snapshot.get("tdMode") == "cross"
                and self._protection_valid(check, intent, row, qty))

    def _planned_exposure(self, intent: Intent, row: dict) -> dict:
        """Every account product/family must be known and owned before a write."""
        exposure = self.transport.read_exposure()
        if (not isinstance(exposure, Mapping) or any(
                not isinstance(exposure.get(key), list) or
                any(not isinstance(item, Mapping) for item in exposure[key])
                for key in ("positions", "pending", "algos"))):
            raise RouterError("DEMO_PLANNED_EXPOSURE_UNKNOWN")
        positions = []
        for position in exposure["positions"]:
            try:
                size = Decimal(str(position.get("pos")))
            except (InvalidOperation, TypeError):
                raise RouterError("DEMO_PLANNED_EXPOSURE_UNKNOWN") from None
            if not size.is_finite():
                raise RouterError("DEMO_PLANNED_EXPOSURE_UNKNOWN")
            if size:
                positions.append((position, size))
        if len(positions) > 1:
            raise RouterError("DEMO_PLANNED_EXTERNAL_EXPOSURE")
        for position, size in positions:
            opened = position.get("cTime")
            position_id = position.get("posId")
            if (position.get("instId") != intent.inst_id or position.get("posSide") != "net"
                    or position.get("mgnMode") != "cross" or not isinstance(position_id, str)
                    or not position_id or not isinstance(opened, str) or not opened.isascii()
                    or not opened.isdigit() or (size > ZERO) != (intent.side == "long")
                    or abs(size) > _decimal(row["filled_qty"])
                    or row.get("position_id") not in (None, position_id)
                    or row.get("position_opened_at_ms") not in (None, int(opened))
                    or not intent.created_at_ms - 2000 <= int(opened) <= self._now()):
                raise RouterError("DEMO_PLANNED_LIFECYCLE_MISMATCH")
        for pending in exposure["pending"]:
            is_entry = (pending.get("clOrdId") == row["client_order_id"]
                        and str(pending.get("ordId")) == row["order_id"]
                        and row["entry_state"] not in TERMINAL_ENTRY)
            is_close = (row.get("close_attempted") is True
                        and pending.get("clOrdId") == row.get("close_id")
                        and pending.get("ordType") == "market"
                        and str(pending.get("reduceOnly", "")).lower() == "true"
                        and pending.get("side") == ("sell" if intent.side == "long" else "buy")
                        and (not row.get("close_order_id")
                             or str(pending.get("ordId")) == row["close_order_id"]))
            if (pending.get("instId") != intent.inst_id or pending.get("posSide") != "net"
                    or pending.get("tdMode") != "cross" or not (is_entry or is_close)):
                raise RouterError("DEMO_PLANNED_EXTERNAL_EXPOSURE")
        if len(exposure["algos"]) > 1:
            raise RouterError("DEMO_PLANNED_EXTERNAL_EXPOSURE")
        for algo in exposure["algos"]:
            # Ownership and safety are separate questions. A corrupt bracket
            # under the already-bound IDs must not strand its proven position;
            # it is handled by the protection check and emergency path below.
            if (not row.get("protection_order_id") or algo.get("instId") != intent.inst_id
                    or str(algo.get("algoId")) != row["protection_order_id"]
                    or algo.get("algoClOrdId") != row["protection_id"]):
                raise RouterError("DEMO_PLANNED_EXTERNAL_PROTECTION")
        return dict(exposure, active_positions=positions)

    def _planned_prepare_protection(self, intent: Intent, row: dict):
        """Protect race fills or demand an owned emergency; never re-enter."""
        quantity = _decimal(row["filled_qty"])
        try:
            snapshot = self.transport.get_protection(intent.inst_id, row["protection_id"])
        except Exception:
            return None  # Unknown protection requires owned emergency reduction.
        if snapshot is not None:
            if (not isinstance(snapshot, Mapping) or snapshot.get("instId") != intent.inst_id
                    or snapshot.get("algoClOrdId") != row["protection_id"]
                    or (row.get("protection_order_id")
                        and str(snapshot.get("algoId")) != row["protection_order_id"])):
                raise RouterError("DEMO_PLANNED_EXTERNAL_PROTECTION")
            state = snapshot.get("state")
            if state == "effective":
                if not self._planned_protection(snapshot, intent, row, quantity):
                    raise RouterError("DEMO_PLANNED_PROTECTION_CHILDREN_UNKNOWN")
                # A triggered algo is not a live bracket. Every real child must
                # be terminal before any distinct emergency reduction is safe.
                actual = self._planned_oco_receipts(intent, row, snapshot)
                row["confirmed_closed_qty"] = str(actual)
                self._save(row)
                return None
            if state != "live":
                if snapshot.get("ordIdList") or snapshot.get("ordId"):
                    raise RouterError("DEMO_PLANNED_PROTECTION_CHILDREN_UNKNOWN")
                return None  # Canceled, failed or unknown: no effective protection.
            if self._planned_protection(snapshot, intent, row, quantity):
                row.update(protected_qty=str(quantity), protection_order_id=str(snapshot["algoId"]))
                self._save(row)
                return snapshot
            previous = _decimal(row["protected_qty"])
            if (previous <= ZERO or not self._planned_protection(snapshot, intent, row, previous)
                    or previous >= quantity):
                return None  # Bad qty/side/prices/reduceOnly is not protection.
        if row.get("protection_requested_qty") == str(quantity):
            # The old process may have sent this exact amendment. Never retry.
            return None
        row.update(protection_requested_qty=str(quantity), status="CLOSING")
        self._save(row)
        self._audit(row, "planned_race_protection_request")
        try:
            self.transport.set_protection(intent, quantity, row["protection_id"], reduce_only=True)
        except Exception:
            pass
        try:
            snapshot = self.transport.get_protection(intent.inst_id, row["protection_id"])
        except Exception:
            return None
        if not self._planned_protection(snapshot, intent, row, quantity):
            return None
        if snapshot.get("state") != "live":
            # Resolve a successful but immediately triggered bracket using its
            # actual children before another reduction, instead of assuming it.
            if snapshot.get("state") == "effective":
                actual = self._planned_oco_receipts(intent, row, snapshot)
                row["confirmed_closed_qty"] = str(actual)
                self._save(row)
            return None
        row.update(protected_qty=str(quantity), protection_order_id=str(snapshot["algoId"]))
        self._save(row)
        self._audit(row, "planned_race_protection_verified")
        return snapshot

    def _planned_close_receipt(self, intent: Intent, row: dict) -> Decimal | None:
        if row.get("close_attempted") is not True:
            return ZERO
        receipt = self._read("get_order", intent.inst_id, row["close_id"])
        if receipt is None:
            return None
        quantity = _decimal(row["close_qty"], positive=True)
        actual = _decimal(receipt.get("accFillSz"))
        if (receipt.get("instId") != intent.inst_id or receipt.get("clOrdId") != row["close_id"]
                or receipt.get("side") != ("sell" if intent.side == "long" else "buy")
                or receipt.get("posSide") != "net" or receipt.get("tdMode") != "cross"
                or receipt.get("ordType") != "market"
                or str(receipt.get("reduceOnly", "")).lower() != "true"
                or not receipt.get("ordId") or _decimal(receipt.get("sz")) != quantity
                or actual > quantity or actual % intent.lot_size
                or receipt.get("state") not in {"live", "partially_filled", "filled", "canceled", "mmp_canceled"}
                or (receipt.get("state") == "filled" and actual != quantity)
                or (row.get("close_order_id") and row["close_order_id"] != str(receipt["ordId"]))):
            raise RouterError("DEMO_PLANNED_CLOSE_RECEIPT_MISMATCH")
        if actual < _decimal(row.get("confirmed_closed_qty", "0")):
            raise RouterError("DEMO_PLANNED_CLOSE_RECEIPT_REGRESSION")
        row.update(close_order_id=str(receipt["ordId"]), confirmed_closed_qty=str(actual),
                   planned_close_receipt_state=receipt["state"])
        if actual and str(receipt["ordId"]) not in row.setdefault("close_order_ids", []):
            row["close_order_ids"].append(str(receipt["ordId"]))
        self._save(row)
        self._audit(row, "planned_close_receipt")
        return actual

    def _planned_emergency_observe(self, intent: Intent, row: dict, code: str) -> RouteResult:
        """An in-flight planned close is never replaced by an emergency POST."""
        try:
            observed = self._planned_close_receipt(intent, row)
            clear = self._demo_account_clear()
            if observed == _decimal(row["filled_qty"]) and clear:
                row["emergency_status"] = "FLAT_CONFIRMED"
            else:
                row["emergency_status"] = "PLANNED_EXIT_REQUIRES_REVIEW"
        except Exception:
            row["emergency_status"] = "PLANNED_EXIT_REQUIRES_REVIEW"
        row.update(status="HALTED", code=code)
        self._safety_record(row, "planned_emergency_receipt_only")
        return self._result(row)

    def _planned_oco_receipts(self, intent: Intent, row: dict, protection) -> Decimal:
        if not isinstance(protection, Mapping) or protection.get("state") != "effective":
            return ZERO
        if not self._planned_protection(protection, intent, row, _decimal(row["filled_qty"])):
            raise RouterError("DEMO_PLANNED_OCO_RECEIPT_MISMATCH")
        row["planned_protection_effective_observed"] = True
        self._save(row)
        children = self.transport.get_protection_exit_orders(
            intent.inst_id, row["protection_id"], row["protection_order_id"])
        if not isinstance(children, list) or not children:
            raise RouterError("DEMO_PLANNED_OCO_RECEIPT_UNKNOWN")
        total, seen = ZERO, set()
        for child in children:
            size = _decimal(child.get("accFillSz")) if isinstance(child, Mapping) else ZERO
            if (not isinstance(child, Mapping) or child.get("instId") != intent.inst_id
                    or child.get("algoId") != row["protection_order_id"]
                    or child.get("posSide") != "net" or child.get("tdMode") != "cross"
                    or child.get("side") != ("sell" if intent.side == "long" else "buy")
                    or str(child.get("reduceOnly", "")).lower() != "true"
                    or child.get("ordType") not in {"market", "limit"}
                    or child.get("state") not in {"filled", "canceled", "mmp_canceled"}
                    or not isinstance(child.get("ordId"), str) or not child["ordId"]
                    or child["ordId"] in seen or child["ordId"] == row.get("close_order_id")
                    or size > _decimal(child.get("sz"), positive=True)
                    or size % intent.lot_size):
                raise RouterError("DEMO_PLANNED_OCO_RECEIPT_MISMATCH")
            seen.add(child["ordId"])
            total += size
            if size and child["ordId"] not in row.setdefault("close_order_ids", []):
                row["close_order_ids"].append(child["ordId"])
        if total > _decimal(row["filled_qty"]):
            raise RouterError("DEMO_PLANNED_OCO_RECEIPT_MISMATCH")
        row["planned_oco_closed_qty"] = str(total)
        self._save(row)
        self._audit(row, "planned_oco_receipts")
        return total

    def _planned_close_claimed(self, intent: Intent, row: dict, code: str) -> RouteResult:
        if row["status"] in {"CLOSED", "CANCELED", "REJECTED", "HALTED", "UNKNOWN"}:
            return self._result(row)
        if row["status"] not in {"PROTECTED", "PENDING", "CLOSING"}:
            raise RouterError("DEMO_PLANNED_ROUTE_STATE")
        with self._db() as db:
            halted = db.execute("SELECT 1 FROM controls WHERE account=?", (self.account_key,)).fetchone()
        if self._local_halted or self.halt_path.exists() or halted:
            return self._finish(row, "HALTED", "DEMO_ALREADY_HALTED")
        row.update(status="CLOSING", planned_close_code=row.get("planned_close_code", code))
        self._save(row)
        self._audit(row, "planned_close_start")
        self._entry_snapshot(intent, row)
        self._planned_exposure(intent, row)
        if row["entry_state"] not in TERMINAL_ENTRY:
            # Protect an already observed fill before canceling any entry remainder.
            if _decimal(row["filled_qty"]) and self._planned_prepare_protection(intent, row) is None:
                return self._emergency(intent, row, "DEMO_PLANNED_PROTECTION_UNVERIFIED")
            self._cancel(intent, row)
            for _ in range(self.max_read_attempts):
                self._entry_snapshot(intent, row)
                if row["entry_state"] in TERMINAL_ENTRY:
                    break
            if row["entry_state"] not in TERMINAL_ENTRY:
                return self._finish(row, "CLOSING", "DEMO_PLANNED_ENTRY_PENDING")
        filled = _decimal(row["filled_qty"])
        if filled == ZERO:
            if not self._demo_account_clear():
                raise RouterError("DEMO_PLANNED_UNFILLED_EXPOSURE")
            return self._finish(row, "CANCELED", "DEMO_TEST_UNFILLED_CANCELED")
        exposure = self._planned_exposure(intent, row)
        if row.get("close_attempted") is not True and exposure["active_positions"]:
            position, size = exposure["active_positions"][0]
            if exposure["pending"]:
                raise RouterError("DEMO_PLANNED_QUANTITY_MISMATCH")
            if abs(size) != filled:
                # A previously bound position may have been partly exited by
                # a terminal OCO child. Prove those actual exits before reducing
                # only its remaining quantity; unrelated size changes still fail.
                if row.get("position_id") and type(row.get("position_opened_at_ms")) is int:
                    if self._planned_prepare_protection(intent, row) is None:
                        return self._emergency(intent, row, "DEMO_PLANNED_PROTECTION_UNVERIFIED")
                raise RouterError("DEMO_PLANNED_QUANTITY_MISMATCH")
            opened = int(position["cTime"])
            if row.get("position_id") is None or row.get("position_opened_at_ms") is None:
                self._prove_demo_lifecycle(intent, row, position["posId"], opened)
                row.update(position_id=position["posId"], position_opened_at_ms=opened)
                self._save(row)
                if self._local_halted:
                    raise RouterError("DEMO_ALREADY_HALTED")
            protection = self._planned_prepare_protection(intent, row)
            if protection is None:
                return self._emergency(intent, row, "DEMO_PLANNED_PROTECTION_UNVERIFIED")
            exposure = self._planned_exposure(intent, row)
            if exposure["active_positions"]:
                if (abs(exposure["active_positions"][0][1]) != filled or exposure["pending"]
                        or len(exposure["algos"]) != 1 or protection.get("state") != "live"):
                    raise RouterError("DEMO_PLANNED_PROTECTION_NOT_LIVE")
                row.update(close_id=_derived_id(intent.signal_id, "planned-close"), close_qty=str(filled),
                           close_order_id="", close_attempted=True)
                self._save(row)  # Durable before POST, including process/power loss.
                self._audit(row, "planned_close_request")
                try:
                    response = self.transport.emergency_close(intent.inst_id,
                        "sell" if intent.side == "long" else "buy", filled, row["close_id"], reduce_only=True)
                    if self._ack(response) and response.get("ordId"):
                        row["close_order_id"] = str(response["ordId"])
                        self._save(row)
                except Exception:
                    pass  # Only the same stable GET receipt may resolve this attempt.
        if not row.get("position_id") or type(row.get("position_opened_at_ms")) is not int:
            raise RouterError("DEMO_PLANNED_LIFECYCLE_UNPROVEN")
        reduced = self._planned_close_receipt(intent, row)
        protection = self.transport.get_protection(intent.inst_id, row["protection_id"])
        oco_reduced = self._planned_oco_receipts(intent, row, protection)
        # An unknown close can still be in flight after an OCO exit: do not
        # discard it, and never infer a second close is safe from flatness alone.
        if (reduced is None or (row.get("close_attempted") is True
                and row.get("planned_close_receipt_state") not in {"filled", "canceled", "mmp_canceled"})):
            return self._finish(row, "CLOSING", "DEMO_PLANNED_CLOSE_PENDING")
        exposure = self._planned_exposure(intent, row)
        if exposure["active_positions"] or exposure["pending"]:
            return self._finish(row, "CLOSING", "DEMO_PLANNED_EXIT_PENDING")
        if reduced + oco_reduced != filled:
            return self._finish(row, "CLOSING", "DEMO_PLANNED_EXIT_UNPROVEN")
        if protection is not None:
            if not self._planned_protection(protection, intent, row, filled):
                raise RouterError("DEMO_PLANNED_PROTECTION_MISMATCH")
            if protection.get("state") == "live" and not row.get("cancel_protection_attempted"):
                row["cancel_protection_attempted"] = True
                self._save(row)
                self._audit(row, "planned_protection_cancel_request")
                try:
                    response = self.transport.cancel_protection(intent.inst_id, row["protection_id"],
                                                                row["protection_order_id"])
                    if isinstance(response, Mapping) and response.get("state") == "effective":
                        row["planned_protection_effective_observed"] = True
                        self._save(row)
                except Exception:
                    pass
        # A cancel may lose a race to OCO activation. Read the exact parent
        # again: an empty aggregate snapshot must never override a known live
        # parent or an effective parent's still-live ordinary child.
        terminal_parent = self.transport.get_protection(intent.inst_id, row["protection_id"])
        if terminal_parent is None:
            if row.get("planned_protection_effective_observed") is True:
                return self._finish(row, "CLOSING", "DEMO_PLANNED_OCO_CHILDREN_UNKNOWN")
        else:
            if not self._planned_protection(terminal_parent, intent, row, filled):
                raise RouterError("DEMO_PLANNED_PROTECTION_MISMATCH")
            if terminal_parent.get("state") == "live":
                return self._finish(row, "CLOSING", "DEMO_PLANNED_CLEANUP_PENDING")
            if terminal_parent.get("state") == "effective":
                row["planned_protection_effective_observed"] = True
                self._save(row)
                # Every child must now have a fresh, correlated terminal GET
                # receipt, including zero-fill children that can still reduce
                # a later position if they are merely missing from aggregates.
                oco_reduced = self._planned_oco_receipts(intent, row, terminal_parent)
                if reduced + oco_reduced != filled:
                    return self._finish(row, "CLOSING", "DEMO_PLANNED_EXIT_UNPROVEN")
        # ACK, canceled OCO, or fill arithmetic alone never frees the slot.
        if not self._demo_account_clear():
            return self._finish(row, "CLOSING", "DEMO_PLANNED_CLEANUP_PENDING")
        row.update(protected_qty="0", emergency_status="", planned_exit_confirmed_at_ms=self._now())
        self._audit(row, "planned_close_complete")
        return self._finish(row, "CLOSED", row["planned_close_code"])

    def planned_close(self, intent: Intent, code: str = "DEMO_TECHNICAL_TEST_EXIT") -> RouteResult:
        """One owned DEMO exit with protection retained until independently flat.

        It never creates an entry, clears HALT, or retries a mutation after an
        uncertain receipt or process restart. CLOSING owns the position slot.
        """
        row = None
        safe_code = code if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,64}", code) else "DEMO_TECHNICAL_TEST_EXIT"
        try:
            row = self._demo_existing_claim(intent)
            return self._planned_close_claimed(intent, row, safe_code)
        except Exception as error:
            fallback = row or dict(signal_id=getattr(intent, "signal_id", ""), client_order_id="",
                status="UNKNOWN", code="DEMO_PLANNED_EXIT_UNKNOWN", filled_qty="0", protected_qty="0")
            failure = error.code if isinstance(error, RouterError) else "DEMO_PLANNED_EXIT_UNKNOWN"
            self._halt(fallback, failure)
            fallback.update(status="HALTED", code=failure)
            self._safety_record(fallback, "planned_close_halted")
            return self._result(fallback)
        finally:
            if row is not None:
                self._release_owner(intent)

    def cancel_and_reconcile(self, intent: Intent) -> RouteResult:
        """Cancel only an existing entry and confirm every final fill is safe.

        Never submits a new entry. A cancel acknowledgement alone is not a
        terminal result: race fills are protected or emergency-reduced, while
        unknown cancellation/fill state persists as a sticky HALT.
        """
        return self._execute(intent, cancel_entry=True)

    def _execute(self, intent: Intent, *, cancel_entry: bool = False,
                 existing_only: bool = False) -> RouteResult:
        """Shared claim, cancellation, reconciliation and owner-release path.

        Call only after the future confirmation UI has approved exact parameters.
        Replays reconcile existing entries; they never submit a second entry.
        A crashed owner's row stays locked for
        explicit operator recovery rather than being stolen by another process.
        """
        client = ""
        row = dict(signal_id=getattr(intent, "signal_id", ""), client_order_id=client,
                   status="REJECTED", code="INVALID_INTENT", filled_qty="0", protected_qty="0")
        claimed = False
        try:
            if not isinstance(intent, Intent):
                raise RouterError("INVALID_INTENT")
            for name in ("entry", "stop", "tp1", "tp2", "qty", "notional", "contract_value", "lot_size",
                         "tick_size", "risk_budget", "effective_risk", "rr", "net_rr"):
                value = getattr(intent, name)
                if not isinstance(value, Decimal) or not value.is_finite() or value <= ZERO:
                    raise RouterError("INVALID_INTENT")
            for name in ("maker_fee_rate", "taker_fee_rate", "slippage_bps"):
                if not isinstance(getattr(intent, name), Decimal):
                    raise RouterError("INVALID_INTENT")
                _decimal(getattr(intent, name))
            if (not isinstance(intent.require_maker, bool) or not isinstance(intent.kind, str)
                    or not isinstance(intent.created_at_ms, int) or isinstance(intent.created_at_ms, bool)
                    or not isinstance(intent.expires_at_ms, int) or isinstance(intent.expires_at_ms, bool)):
                raise RouterError("INVALID_INTENT")
            client = client_order_id(intent.signal_id)
            row["client_order_id"] = client
            if self.mode == "native":
                NativeTransport._blocked()
            if self.mode not in {"mock", "paper", "native-live", "demo"}:
                raise RouterError("TRANSPORT_MODE")
            if self.mode == "demo":
                # DEMO has its own explicit capability. Never simulate it by
                # re-labelling a network adapter as mock/paper or opening the
                # real account's environment gate.
                from .demo_route_transport import DemoRouteTransport
                if type(self.transport) is not DemoRouteTransport:
                    raise RouterError("DEMO_TRANSPORT_REQUIRED")
                try:
                    self.transport.assert_demo_enabled()
                except Exception:
                    raise RouterError("DEMO_TRADING_DISABLED") from None
                if not self.account_key.startswith("DEMO-"):
                    raise RouterError("DEMO_ACCOUNT_NAMESPACE")
                if self.account_identity is None:
                    raise RouterError("DEMO_ACCOUNT_UNBOUND")
            # T2B：真实适配器必须同时满足"显式注入 native-live transport"+
            # "进程级 OKX_TRADING_ENABLED=1"两道闸门，缺一不可。
            if self.mode == "native-live" and os.environ.get("OKX_TRADING_ENABLED") != "1":
                raise RouterError("TRADING_DISABLED")
            fingerprint = self._intent_fingerprint(intent)
            # Check persistent identity before potentially rejecting an expired replay.
            with self._db() as db:
                exists = db.execute("SELECT 1 FROM routes WHERE account=? AND signal=?",
                                    (self.account_key, intent.signal_id)).fetchone()
            if (cancel_entry or existing_only) and not exists:
                raise RouterError("CANCEL_NOT_FOUND" if cancel_entry else "RECONCILE_NOT_FOUND")
            if not exists:
                self._preflight(intent)
            row, new = self._claim(intent, fingerprint, client)
            claimed = True
            if new and existing_only:
                raise RouterError("RECONCILE_NOT_FOUND")
            if not new and row["status"] == "CLOSING":
                return self._planned_close_claimed(intent, row, row.get("planned_close_code", "DEMO_TECHNICAL_TEST_EXIT"))
            if not new and row["status"] in {"CANCELED", "REJECTED", "UNKNOWN", "HALTED", "CLOSED"}:
                self._audit(row, "replay_cached")
                return self._result(row)
            if new:
                # Repeat immediately before the write; failure releases the reserved slot.
                admission = self._preflight(intent)
                if admission is not None:
                    row["entry_admission"] = dict(account_ms=admission.account_ms,
                        quote_ms=admission.quote_ms, checked_ms=admission.checked_ms,
                        bid=str(admission.bid), ask=str(admission.ask))
                    if admission.context_valid_until_ms is not None:
                        row["entry_admission"].update(context_valid_until_ms=admission.context_valid_until_ms,
                            context_expiry_code=admission.context_expiry_code)
                row.update(status="SUBMITTING", code="OK")
                self._save(row)
                self._audit(row, "entry_request")
                row["submit_attempted"] = True
                self._save(row)
                try:
                    if admission is None:
                        response = self.transport.submit_entry(intent, client)
                    else:
                        from .demo_route_transport import DemoEntryNotSent
                        try:
                            response = self.transport.submit_entry(intent, client,
                                before_send=lambda: self._check_demo_entry_admission(intent, admission))
                        except DemoEntryNotSent as error:
                            return self._finish_entry_not_sent(intent, row, error.code)
                except Exception:
                    response = None
                    self._audit(row, "entry_response_unknown")
                if isinstance(response, Mapping) and "sCode" in response and str(response["sCode"]) != "0":
                    return self._finish(row, "REJECTED", "ENTRY_REJECTED")
                if self._ack(response) and response.get("ordId"):
                    row["order_id"] = str(response["ordId"])
                    self._save(row)
            return self._advance(intent, row, cancel_entry=cancel_entry)
        except (RouterError, ExecutionRejected) as error:
            if claimed and row.get("status") == "RESERVED":
                return self._finish(row, "REJECTED", error.code)
            if claimed:
                return self._emergency(intent, row, error.code, status="UNKNOWN")
            row.update(status="REJECTED", code=error.code)
            self._audit(row, "preflight_rejected")
            return self._result(row)
        except Exception:
            # IO failures after submitting/observing a fill must not strand the
            # position. Emergency operations tolerate failed audit persistence.
            if claimed and (row.get("submit_attempted") or _decimal(row.get("filled_qty", 0)) > ZERO):
                return self._emergency(intent, row, "ROUTER_FAILURE")
            row.update(status="HALTED", code="ROUTER_FAILURE")
            self._halt(row, "ROUTER_FAILURE")
            self._safety_record(row, "result")
            return self._result(row)
        finally:
            if claimed:
                try:
                    with self._db() as db:
                        db.execute("UPDATE routes SET owner='' WHERE account=? AND signal=? AND owner=?",
                                   (self.account_key, intent.signal_id, self._owner))
                except (OSError, sqlite3.Error):
                    self._local_halted = True  # A stale owner remains fail-closed.
