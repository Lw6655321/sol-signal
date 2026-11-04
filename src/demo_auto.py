"""Persistent, demo-only coordinator; all account IO is injected.

Strategy execution accepts only a current scan's explicit ``new_signals``. A
separate, default-disabled technical-test entry records its explicit DEMO-only
purpose and still uses the same account risk, router and accounting journals.
This module
does not collect market data, read credentials, change account settings, or
interpret historical CSV rows as new signals. A second durable journal remembers
each signal before calling the router. Unknown execution outcomes stay halted.

The service must serialize calls and hold a process lock. The router remains the
authority for exchange reconciliation, protection, emergency reductions and its
own durable single-position slot. No position is released by guessing from an
empty list or from an acknowledgement.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from .execution import ExecutionRejected, Intent, build_intent


DEMO_ACCOUNT_KEY = "DEMO-AUTO-SOL-V1"
_ACTIVE = {"RESERVED", "SUBMITTING", "PENDING", "PROTECTED", "CLOSING", "UNKNOWN", "HALTED"}
_TERMINAL = {"REJECTED", "CANCELED", "CLOSED"}
_TEST_PURPOSE = "DEMO_TECHNICAL_TEST"
_TEST_AUTHORIZATION = "user-approved-demo-technical-tests-20261019"
_TEST_KINDS = {"smoke": "scalp_5m", "swing": "swing_1h4h"}
_NATURAL_EXIT_CODES = {"MAX_HOLD_REACHED", "NO_OVERNIGHT_EXIT"}
_NATURAL_EXIT_PURPOSE = "NATURAL_HOLDING_EXIT"
_NATURAL_EXIT_LOCAL_REFUSALS = {"NATURAL_EXIT_SCOPE_INVALID", "NATURAL_EXIT_NOT_DUE",
                              "NATURAL_EXIT_REGISTRATION_INVALID", "NATURAL_EXIT_NOT_REGISTERED",
                              "NATURAL_EXIT_REGISTRATION_FAILED"}
_DECIMAL_FIELDS = {"entry", "stop", "tp1", "tp2", "qty", "notional", "contract_value",
                   "lot_size", "tick_size", "maker_fee_rate", "taker_fee_rate", "slippage_bps",
                   "risk_budget", "effective_risk", "rr", "net_rr"}


class DemoAutoError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _code(value, fallback="CONTROLLER_FAILURE"):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", value) else fallback


def _integer(value):
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DemoAutoError("INVALID_TIMESTAMP")
    return value


def _quantity(value):
    if isinstance(value, bool) or value is None:
        raise DemoAutoError("INVALID_ROUTE_RESULT")
    try:
        value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise DemoAutoError("INVALID_ROUTE_RESULT") from None
    if not value.is_finite() or value < 0:
        raise DemoAutoError("INVALID_ROUTE_RESULT")
    return value


def _restore_intent(raw):
    """Reject damaged state; do not regenerate a different plan after restart."""
    values = json.loads(raw)
    if not isinstance(values, dict) or set(values) != {item.name for item in fields(Intent)}:
        raise DemoAutoError("JOURNAL_INVALID")
    for name in _DECIMAL_FIELDS:
        values[name] = _quantity(values[name])
    if (not isinstance(values["require_maker"], bool)
            or not isinstance(values["signal_id"], str)
            or not isinstance(values["kind"], str)
            or values["inst_id"] != "SOL-USDT-SWAP"
            or values["side"] not in {"long", "short"}):
        raise DemoAutoError("JOURNAL_INVALID")
    _integer(values["created_at_ms"])
    _integer(values["expires_at_ms"])
    return Intent(**values)


class DemoAutoController:
    def __init__(self, config, router, risk_provider, market_provider, storage_root, clock=None,
                 *, enable_demo_tests=False):
        if (getattr(router, "mode", None) != "demo"
                or getattr(router, "account_key", None) != DEMO_ACCOUNT_KEY):
            raise DemoAutoError("DEMO_ROUTER_REQUIRED")
        if type(enable_demo_tests) is not bool:
            raise DemoAutoError("DEMO_TEST_ENABLE_INVALID")
        self.enable_demo_tests = enable_demo_tests
        self.config = deepcopy(config)
        self.router = router
        self.risk_provider = risk_provider
        self.market_provider = market_provider
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._lock = threading.RLock()
        self._local_halt = ""
        root = Path(storage_root)
        state_root = root / "state" / "demo-auto"
        state_root.mkdir(parents=True, exist_ok=True)
        self.journal_path = state_root / "controller.sqlite3"
        self.halt_path = state_root / "CONTROLLER_HALT"
        log_root = root / "logs" / "demo-auto"
        log_root.mkdir(parents=True, exist_ok=True)
        self.run_dir = log_root / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex)
        self.run_dir.mkdir(exist_ok=False)
        self.audit_path = self.run_dir / "controller.jsonl"
        with self._db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS controller_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS controller_signals (signal TEXT PRIMARY KEY,status TEXT NOT NULL,"
                       "code TEXT NOT NULL,intent TEXT,updated_ms INTEGER NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS technical_tests (signal TEXT PRIMARY KEY,purpose TEXT NOT NULL,"
                       "scenario TEXT NOT NULL,authorization_id TEXT NOT NULL,plan_fingerprint TEXT NOT NULL,"
                       "signal_json TEXT NOT NULL,registered_at_ms INTEGER NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS natural_exits (signal TEXT PRIMARY KEY,record TEXT NOT NULL)")
            db.execute("INSERT OR IGNORE INTO controller_meta VALUES('account',?)", (DEMO_ACCOUNT_KEY,))
            db.execute("INSERT OR IGNORE INTO controller_meta VALUES('schema','1')")
            meta = dict(db.execute("SELECT key,value FROM controller_meta"))
            if meta.get("account") != DEMO_ACCOUNT_KEY or meta.get("schema") != "1":
                raise DemoAutoError("JOURNAL_INVALID")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.journal_path, timeout=5)
        try:
            with db:
                yield db
        finally:
            db.close()

    def _now(self):
        return _integer(self.clock())

    def _audit(self, status, code, signal_id=""):
        # Deliberately exclude raw exceptions, configuration, API payloads and keys.
        with self.audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"timestamp_ms": self._now(), "account": DEMO_ACCOUNT_KEY,
                                     "status": status, "code": _code(code), "signal_id": signal_id},
                                    sort_keys=True) + "\n")

    def _halt(self, code):
        code = _code(code)
        self._local_halt = code
        try:
            with self._db() as db:
                db.execute("INSERT OR IGNORE INTO controller_meta VALUES('halt',?)", (code,))
        except Exception:
            pass
        try:
            self.halt_path.write_text(code + "\n", encoding="utf-8")
        except OSError:
            pass
        try:
            self._audit("HALTED", code)
        except Exception:
            pass
        return {"status": "HALTED", "code": code}

    def _halt_code(self):
        if self._local_halt:
            return self._local_halt
        with self._db() as db:
            record = db.execute("SELECT value FROM controller_meta WHERE key='halt'").fetchone()
        if record:
            return _code(record[0])
        if self.halt_path.exists():
            return "PERSISTENT_HALT"
        return ""

    def _active(self):
        with self._db() as db:
            rows = db.execute("SELECT signal,status,code,intent FROM controller_signals "
                              "WHERE status NOT IN ('REJECTED','CANCELED','CLOSED')").fetchall()
        if len(rows) > 1:
            raise DemoAutoError("MULTIPLE_ACTIVE_INTENTS")
        if rows and rows[0][1] not in _ACTIVE:
            raise DemoAutoError("JOURNAL_INVALID")
        return rows[0] if rows else None

    def status(self):
        with self._lock:
            try:
                halt = self._halt_code()
                active = self._active()
                return {"status": "HALTED" if halt else (active[1] if active else "IDLE"),
                        "code": halt or (active[2] if active else "READY_FOR_SCAN"),
                        "signal_id": active[0] if active else "", "account": DEMO_ACCOUNT_KEY}
            except Exception:
                return self._halt("JOURNAL_UNAVAILABLE")

    def safety_halt(self, code="SERVICE_FAILURE"):
        """Latch a service failure and reduce only its persisted, owned route.

        A reporting/scan exception after entry must not let the service exit
        while an unmonitored pending entry can fill. No account-wide flatten or
        invented plan is allowed. Router reconciliation owns cancel races and
        uncertain reduction acknowledgements; this method never clears a HALT.
        """
        with self._lock:
            safe_code = _code(code, "SERVICE_FAILURE")
            try:
                halt = self._halt_code()
                if halt in _NATURAL_EXIT_LOCAL_REFUSALS:
                    # A failed local exit registration grants no alternate
                    # account write authority, including service-finally cleanup.
                    return {"status": "HALTED", "code": halt}
                active = self._active()
                if active is None:
                    return self._halt(safe_code)
                signal_id, _, _, raw_intent = active
                if raw_intent is None:
                    return self._halt("INCOMPLETE_INTENT")
                intent = _restore_intent(raw_intent)
                if intent.signal_id != signal_id:
                    return self._halt("JOURNAL_IDENTITY_MISMATCH")
                if active[1] == "CLOSING":
                    # A planned market exit may already have reached OKX. A
                    # reporting/service failure must never invent a second
                    # emergency close under a different idempotency key.
                    return self._planned_exit_failure(intent, safe_code)
                return self._execution_failure(intent, safe_code)
            except Exception:
                return self._halt("JOURNAL_UNAVAILABLE")

    def _claim(self, signal_id):
        """Atomically consume an ID and reserve this account before account IO."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM controller_signals WHERE signal=?", (signal_id,)).fetchone():
                return "DUPLICATE"
            if db.execute("SELECT 1 FROM controller_signals WHERE status NOT IN ('REJECTED','CANCELED','CLOSED')").fetchone():
                # A skipped signal is consumed too. It must not be queued and
                # unexpectedly enter after the existing position exits.
                db.execute("INSERT INTO controller_signals VALUES(?,?,?,?,?)",
                           (signal_id, "REJECTED", "POSITION_SLOT_BUSY", None, self._now()))
                return "POSITION_SLOT_BUSY"
            if db.execute("SELECT 1 FROM controller_meta WHERE key='halt'").fetchone():
                return "PERSISTENT_HALT"
            db.execute("INSERT INTO controller_signals VALUES(?,?,?,?,?)",
                       (signal_id, "RESERVED", "CHECKING", None, self._now()))
        return ""

    def _save(self, signal_id, status, code, intent=None):
        with self._db() as db:
            if intent is None:
                db.execute("UPDATE controller_signals SET status=?,code=?,updated_ms=? WHERE signal=?",
                           (status, _code(code), self._now(), signal_id))
            else:
                db.execute("UPDATE controller_signals SET status=?,code=?,intent=?,updated_ms=? WHERE signal=?",
                           (status, _code(code), json.dumps(intent.to_dict(), sort_keys=True), self._now(), signal_id))
        self._audit(status, code, signal_id)

    def _snapshot(self, payload):
        market = self.market_provider()
        if not isinstance(market, Mapping):
            raise DemoAutoError("MARKET_UNAVAILABLE")
        market = {**market, "data_fresh": payload["data_fresh"],
                  "macro_blackout": payload["macro_blackout"], "observed_at_ms": self._now()}
        provider = self.risk_provider
        state = provider.snapshot(market) if hasattr(provider, "snapshot") else provider(market)
        if not isinstance(state, Mapping):
            raise DemoAutoError("STATE_UNAVAILABLE")
        if state.get("demo_verified") is not True:
            raise DemoAutoError("DEMO_STATE_UNVERIFIED")
        observed = _integer(state.get("observed_at_ms"))
        now = self._now()
        if observed > now or now - observed > 5000:
            raise DemoAutoError("DEMO_STATE_STALE")
        if state.get("refusal_reasons", []) != []:
            raise DemoAutoError("DEMO_RISK_NOT_READY")
        if state.get("data_fresh") is not True or state.get("macro_blackout") is not False:
            raise DemoAutoError("DEMO_RISK_NOT_READY")
        # Weekly accounting is mandatory in automated mode; execution's paper
        # fallback must not silently omit the configured weekly limit.
        for required in ("week_start_equity", "realized_pnl_week", "entries_today"):
            if required not in state:
                raise DemoAutoError("DEMO_RISK_INCOMPLETE")
        return {**state, "now_ms": now}

    def _apply_result(self, intent, result, *, exit_checked=False):
        status = getattr(result, "status", None)
        code = _code(getattr(result, "code", None), "INVALID_ROUTE_RESULT")
        if getattr(result, "signal_id", None) != intent.signal_id:
            raise DemoAutoError("ROUTE_IDENTITY_MISMATCH")
        filled = _quantity(getattr(result, "filled_qty", None))
        protected = _quantity(getattr(result, "protected_qty", None))
        if status == "PROTECTED" and not (filled > 0 and protected == filled):
            raise DemoAutoError("PROTECTION_UNVERIFIED")
        if status == "CLOSED" and (not exit_checked or protected):
            raise DemoAutoError("EXIT_UNVERIFIED")
        if status == "CLOSING" and not exit_checked:
            raise DemoAutoError("EXIT_UNVERIFIED")
        if status in {"PENDING", "CANCELED", "REJECTED"} and (filled or protected):
            raise DemoAutoError("PROTECTION_UNVERIFIED")
        if status not in (_ACTIVE | _TERMINAL) or status in {"RESERVED", "SUBMITTING"}:
            raise DemoAutoError("INVALID_ROUTE_RESULT")
        self._save(intent.signal_id, status, code)
        if status in {"HALTED", "UNKNOWN"}:
            return self._halt(code)
        return {"status": status, "code": code, "signal_id": intent.signal_id,
                "filled_qty": str(filled), "protected_qty": str(protected)}

    def _execution_failure(self, intent, code):
        # Router owns cancellation/fill reconciliation and reduce-only behavior.
        # Never invent a quantity or flatten unrelated account positions here.
        self._halt(code)
        try:
            self.router.emergency_reduce(intent, _code(code))
        except Exception:
            pass
        try:
            self._save(intent.signal_id, "HALTED", code)
        except Exception:
            pass
        return {"status": "HALTED", "code": _code(code), "signal_id": intent.signal_id}

    def _planned_exit_failure(self, intent, code):
        """Retain a planned exit checkpoint without sending any alternate IO."""
        safe_code = _code(code)
        try:
            self._save(intent.signal_id, "CLOSING", safe_code)
        except Exception:
            # The already-durable route/checkpoint and exchange protection are
            # left intact. Storage failure cannot authorize an emergency POST.
            return {**self._halt("JOURNAL_UNAVAILABLE"), "signal_id": intent.signal_id}
        return {"status": "CLOSING", "code": safe_code, "signal_id": intent.signal_id}

    def _holding_exit_code(self, intent, *, now_ms=None):
        """Honor existing playbook limits without loosening any stop.

        Intent creation is a conservative upper bound for holding time when the
        exchange fill time is unavailable. ``no_overnight`` uses UTC date
        boundaries, consistent with the risk provider's trading day.
        """
        books = self.config.get("playbooks", {})
        book = books.get(intent.kind)
        if not isinstance(book, Mapping):
            raise DemoAutoError("HOLD_POLICY_INVALID")
        now = self._now() if now_ms is None else _integer(now_ms)
        if now < intent.created_at_ms:
            raise DemoAutoError("CLOCK_REGRESSION")
        if "max_hold_hours" in book:
            hours = _quantity(book["max_hold_hours"])
            if hours <= 0:
                raise DemoAutoError("HOLD_POLICY_INVALID")
            if Decimal(now - intent.created_at_ms) >= hours * 3_600_000:
                return "MAX_HOLD_REACHED"
        overnight = book.get("no_overnight", False)
        if type(overnight) is not bool:
            raise DemoAutoError("HOLD_POLICY_INVALID")
        if overnight and now // 86_400_000 != intent.created_at_ms // 86_400_000:
            return "NO_OVERNIGHT_EXIT"
        return ""

    @staticmethod
    def _intent_fingerprint(intent):
        return hashlib.sha256(json.dumps(intent.to_dict(), sort_keys=True, separators=(",", ":"),
                                         allow_nan=False).encode()).hexdigest()

    def _natural_exit_scope(self, intent, db):
        if (intent.inst_id != "SOL-USDT-SWAP" or intent.kind not in _TEST_KINDS.values()
                or intent.signal_id.upper().startswith(("RSCH-", "DEMO-TEST-"))
                or db.execute("SELECT 1 FROM technical_tests WHERE signal=?", (intent.signal_id,)).fetchone()):
            raise DemoAutoError("NATURAL_EXIT_SCOPE_INVALID")

    @staticmethod
    def _natural_exit_rows(db, signal_id):
        schema = [(row[1], row[2], row[3], row[5]) for row in db.execute("PRAGMA table_info(natural_exits)")]
        if schema != [("signal", "TEXT", 0, 1), ("record", "TEXT", 1, 0)]:
            raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
        if db.execute("SELECT 1 FROM natural_exits n LEFT JOIN controller_signals c ON c.signal=n.signal "
                      "WHERE c.signal IS NULL").fetchone():
            raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
        return db.execute("SELECT record FROM natural_exits WHERE signal=?", (signal_id,)).fetchall()

    def _begin_natural_exit(self, intent, reason):
        """Atomically bind a due natural plan to CLOSING before any exit IO."""
        requested = self._now()
        if reason not in _NATURAL_EXIT_CODES or self._holding_exit_code(intent, now_ms=requested) != reason:
            raise DemoAutoError("NATURAL_EXIT_NOT_DUE")
        record = {"schema": 1, "purpose": _NATURAL_EXIT_PURPOSE, "signal_id": intent.signal_id,
                  "reason": reason, "intent_sha256": self._intent_fingerprint(intent), "requested_at_ms": requested}
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._natural_exit_scope(intent, db)
            row = db.execute("SELECT status,intent FROM controller_signals WHERE signal=?",
                             (intent.signal_id,)).fetchone()
            if (not row or row[0] != "PROTECTED" or row[1] is None
                    or _restore_intent(row[1]) != intent
                    or self._natural_exit_rows(db, intent.signal_id)):
                raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
            db.execute("INSERT INTO natural_exits VALUES(?,?)",
                       (intent.signal_id, json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)))
            db.execute("UPDATE controller_signals SET status='CLOSING',code=?,updated_ms=? WHERE signal=?",
                       (reason, requested, intent.signal_id))

    def _registered_natural_exit(self, intent):
        """A CLOSING row alone never authorizes a natural exit after restart."""
        def unique_pairs(items):
            record = {}
            for key, value in items:
                if key in record:
                    raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
                record[key] = value
            return record
        with self._db() as db:
            self._natural_exit_scope(intent, db)
            rows = self._natural_exit_rows(db, intent.signal_id)
        if not rows:
            raise DemoAutoError("NATURAL_EXIT_NOT_REGISTERED")
        if len(rows) != 1:
            raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
        try:
            record = json.loads(rows[0][0], object_pairs_hook=unique_pairs)
            if (not isinstance(record, dict)
                    or set(record) != {"schema", "purpose", "signal_id", "reason", "intent_sha256", "requested_at_ms"}
                    or type(record["schema"]) is not int or record["schema"] != 1
                    or record["purpose"] != _NATURAL_EXIT_PURPOSE or record["signal_id"] != intent.signal_id
                    or json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) != rows[0][0]):
                raise ValueError
        except (ValueError, TypeError):
            raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID") from None
        reason, fingerprint, requested = (record[key] for key in ("reason", "intent_sha256", "requested_at_ms"))
        if (not isinstance(reason, str) or reason not in _NATURAL_EXIT_CODES
                or fingerprint != self._intent_fingerprint(intent)
                or type(requested) is not int or not intent.created_at_ms <= requested <= self._now()
                or self._holding_exit_code(intent, now_ms=requested) != reason):
            raise DemoAutoError("NATURAL_EXIT_REGISTRATION_INVALID")
        return reason

    def _process_signal(self, signal, payload):
        if not isinstance(signal, Mapping):
            return {"status": "REJECTED", "code": "INVALID_SIGNAL"}
        signal_id = signal.get("id")
        if not isinstance(signal_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", signal_id):
            return {"status": "REJECTED", "code": "INVALID_SIGNAL_ID"}
        if signal_id.upper().startswith("RSCH-") or signal.get("research") is True:
            return {"status": "REJECTED", "code": "RESEARCH_SIGNAL", "signal_id": signal_id}
        claimed = self._claim(signal_id)
        if claimed:
            return {"status": "SKIPPED", "code": claimed, "signal_id": signal_id}
        intent = None
        submitted = False
        try:
            state = self._snapshot(payload)
            intent = build_intent(signal, state, self.config)
            if intent.created_at_ms < payload["generated_at_ms"] - 5000:
                raise DemoAutoError("SIGNAL_NOT_FROM_CURRENT_SCAN")
            # Reserve the exact plan durably before the router can write.
            self._save(signal_id, "SUBMITTING", "SUBMITTING", intent)
            submitted = True
            result = self.router.execute(intent)
            return self._apply_result(intent, result)
        except (ExecutionRejected, DemoAutoError) as error:
            if submitted:
                return self._execution_failure(intent, _code(error.code))
            self._save(signal_id, "REJECTED", _code(error.code))
            return {"status": "REJECTED", "code": _code(error.code), "signal_id": signal_id}
        except Exception:
            if submitted:
                return self._execution_failure(intent, "EXECUTION_UNCERTAIN")
            self._save(signal_id, "REJECTED", "STATE_UNAVAILABLE")
            return {"status": "REJECTED", "code": "STATE_UNAVAILABLE", "signal_id": signal_id}

    def _test_plan(self, plan):
        if (not isinstance(plan, Mapping)
                or set(plan) != {"purpose", "scenario", "authorization_id", "signal"}
                or plan.get("purpose") != _TEST_PURPOSE
                or plan.get("authorization_id") != _TEST_AUTHORIZATION
                or not isinstance(plan.get("scenario"), str)
                or plan.get("scenario") not in _TEST_KINDS):
            raise DemoAutoError("DEMO_TEST_PLAN_INVALID")
        signal = plan.get("signal")
        required = {"id", "instId", "kind", "side", "entry", "stop", "tp1", "tp2", "qty",
                    "generated_at_ms", "expires_at_ms"}
        if (not isinstance(signal, Mapping) or not required <= set(signal)
                or set(signal) - required - {"require_maker", "slippage_bps", "entry_type"}
                or signal.get("instId") != "SOL-USDT-SWAP"
                or signal.get("kind") != _TEST_KINDS[plan["scenario"]]):
            raise DemoAutoError("DEMO_TEST_SIGNAL_INVALID")
        signal_id = signal.get("id")
        prefix = "DEMO-TEST-" + plan["scenario"].upper() + "-"
        if (not isinstance(signal_id, str) or not signal_id.startswith(prefix)
                or len(signal_id) <= len(prefix)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", signal_id)):
            raise DemoAutoError("DEMO_TEST_ID_INVALID")
        specs = self.config.get("symbols")
        if not isinstance(specs, list):
            raise DemoAutoError("DEMO_TEST_MINIMUM_INVALID")
        matches = [spec for spec in specs if isinstance(spec, Mapping)
                   and spec.get("instId") == "SOL-USDT-SWAP"]
        if len(matches) != 1:
            raise DemoAutoError("DEMO_TEST_MINIMUM_INVALID")
        minimum = _quantity(matches[0].get("minSz"))
        if minimum <= 0 or _quantity(signal.get("qty")) != minimum:
            raise DemoAutoError("DEMO_TEST_MINIMUM_REQUIRED")
        # Canonical immutable registration; numeric Decimal inputs are written
        # as exact strings, never rounded through a binary float conversion.
        try:
            normalized = json.loads(json.dumps(dict(plan), sort_keys=True,
                                                allow_nan=False, default=str))
        except (TypeError, ValueError):
            raise DemoAutoError("DEMO_TEST_PLAN_INVALID") from None
        return normalized

    @staticmethod
    def _test_fingerprint(plan):
        return hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":"),
                                         allow_nan=False).encode()).hexdigest()

    def _registered_test(self, signal_id, intent=None):
        with self._db() as db:
            row = db.execute("SELECT purpose,scenario,authorization_id,plan_fingerprint,signal_json "
                             "FROM technical_tests WHERE signal=?", (signal_id,)).fetchone()
        if row is None:
            raise DemoAutoError("DEMO_TEST_NOT_REGISTERED")
        purpose, scenario, authorization, fingerprint, signal_json = row
        try:
            signal = json.loads(signal_json)
            plan = {"purpose": purpose, "scenario": scenario,
                    "authorization_id": authorization, "signal": signal}
            prefix = "DEMO-TEST-" + str(scenario).upper() + "-"
            if (purpose != _TEST_PURPOSE or authorization != _TEST_AUTHORIZATION
                    or scenario not in _TEST_KINDS or not isinstance(signal, dict)
                    or signal.get("id") != signal_id or not signal_id.startswith(prefix)
                    or signal.get("instId") != "SOL-USDT-SWAP"
                    or signal.get("kind") != _TEST_KINDS[scenario]
                    or fingerprint != self._test_fingerprint(plan)):
                raise DemoAutoError("DEMO_TEST_REGISTRATION_INVALID")
            if intent is not None and (intent.signal_id != signal_id
                    or intent.kind != signal["kind"] or intent.inst_id != signal["instId"]
                    or intent.side != signal.get("side") or intent.qty > _quantity(signal.get("qty"))):
                raise DemoAutoError("DEMO_TEST_REGISTRATION_INVALID")
        except (TypeError, ValueError, KeyError):
            raise DemoAutoError("DEMO_TEST_REGISTRATION_INVALID") from None
        return plan

    def process_demo_test(self, plan, market_evidence):
        """Submit one explicitly authorized technical plan, never a fake scan.

        The service supplies real collection freshness/calendar observations.
        Alpha scores and volatility selection are deliberately absent; account
        risk and router admission use exactly the ordinary verified path.
        """
        with self._lock:
            try:
                if not self.enable_demo_tests:
                    raise DemoAutoError("DEMO_TESTS_DISABLED")
                halt = self._halt_code()
                if halt:
                    return {"status": "HALTED", "code": halt}
                plan = self._test_plan(plan)
                if not isinstance(market_evidence, Mapping):
                    raise DemoAutoError("DEMO_TEST_MARKET_INVALID")
                stamp = _integer(market_evidence.get("generated_at_ms"))
                now = self._now()
                max_age = _quantity(self.config.get("data", {}).get("stale_seconds", {}).get("ticker", 60)) * 1000
                if stamp > now or max_age <= 0 or now - stamp > max_age:
                    raise DemoAutoError("DEMO_TEST_MARKET_STALE")
                if market_evidence.get("data_fresh") is not True:
                    raise DemoAutoError("STALE_DATA")
                if market_evidence.get("macro_blackout") is not False:
                    raise DemoAutoError("MACRO_BLACKOUT")
                signal_id = plan["signal"]["id"]
                values = (plan["purpose"], plan["scenario"], plan["authorization_id"],
                          self._test_fingerprint(plan),
                          json.dumps(plan["signal"], sort_keys=True, separators=(",", ":")))
                with self._db() as db:
                    db.execute("BEGIN IMMEDIATE")
                    existing = db.execute("SELECT purpose,scenario,authorization_id,plan_fingerprint,signal_json "
                                          "FROM technical_tests WHERE signal=?", (signal_id,)).fetchone()
                    if existing is not None and existing != values:
                        raise DemoAutoError("DEMO_TEST_IDEMPOTENCY_CONFLICT")
                    if existing is None:
                        if db.execute("SELECT 1 FROM controller_signals WHERE signal=?", (signal_id,)).fetchone():
                            raise DemoAutoError("DEMO_TEST_IDENTITY_CONFLICT")
                        db.execute("INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)",
                                   (signal_id, *values, now))
                self._audit("REGISTERED", "DEMO_TECHNICAL_TEST", signal_id)
                return self._process_signal(plan["signal"], market_evidence)
            except DemoAutoError as error:
                return {"status": "REJECTED", "code": _code(error.code)}
            except Exception:
                return self._halt("JOURNAL_UNAVAILABLE")

    def close_demo_test(self, signal_id):
        """Request an owned technical exit; never close an arbitrary signal."""
        with self._lock:
            intent = None
            try:
                if not self.enable_demo_tests:
                    raise DemoAutoError("DEMO_TESTS_DISABLED")
                halt = self._halt_code()
                if halt:
                    return {"status": "HALTED", "code": halt}
                if not isinstance(signal_id, str):
                    raise DemoAutoError("DEMO_TEST_ID_INVALID")
                self._registered_test(signal_id)
                active = self._active()
                if active is None or active[0] != signal_id:
                    raise DemoAutoError("DEMO_TEST_NOT_ACTIVE")
                if active[1] not in {"PENDING", "PROTECTED", "CLOSING"} or active[3] is None:
                    raise DemoAutoError("DEMO_TEST_CLOSE_STATE")
                restored = _restore_intent(active[3])
                self._registered_test(signal_id, restored)
                intent = restored
                self._save(signal_id, "CLOSING", "DEMO_TECHNICAL_TEST_EXIT")
                result = self.router.planned_close(intent, code="DEMO_TECHNICAL_TEST_EXIT")
                if getattr(result, "status", None) not in {"CLOSING", "CLOSED", "CANCELED", "UNKNOWN", "HALTED"}:
                    raise DemoAutoError("EXIT_UNVERIFIED")
                return self._apply_result(intent, result, exit_checked=True)
            except DemoAutoError as error:
                return self._planned_exit_failure(intent, error.code) if intent else {"status": "REJECTED", "code": _code(error.code)}
            except Exception:
                return self._planned_exit_failure(intent, "PLANNED_EXIT_UNCERTAIN") if intent else self._halt("JOURNAL_UNAVAILABLE")

    def process_scan(self, scan_payload):
        """Process exactly one current engine result; never reload old signals."""
        with self._lock:
            try:
                halt = self._halt_code()
                if halt:
                    return {"status": "HALTED", "code": halt, "results": []}
                if not isinstance(scan_payload, Mapping):
                    raise DemoAutoError("INVALID_SCAN")
                stamp = _integer(scan_payload.get("generated_at_ms"))
                now = self._now()
                max_age = _quantity(self.config.get("data", {}).get("stale_seconds", {}).get("ticker", 60)) * 1000
                if stamp > now or now - stamp > max_age or max_age <= 0:
                    raise DemoAutoError("SCAN_STALE")
                gates = scan_payload.get("gates")
                if not isinstance(gates, Mapping) or gates.get("pass") is not True:
                    raise DemoAutoError("SCAN_GATES_REJECTED")
                if scan_payload.get("data_fresh") is not True:
                    raise DemoAutoError("STALE_DATA")
                if scan_payload.get("macro_blackout") is not False:
                    raise DemoAutoError("MACRO_BLACKOUT")
                signals = scan_payload.get("new_signals")
                if not isinstance(signals, list):
                    raise DemoAutoError("NEW_SIGNALS_REQUIRED")
                results = []
                for signal in signals:
                    if (isinstance(signal, Mapping) and isinstance(signal.get("id"), str)
                            and signal["id"].upper().startswith("DEMO-TEST-")):
                        result = {"status": "REJECTED", "code": "DEMO_TEST_REQUIRES_SEPARATE_ENTRY",
                                  "signal_id": signal["id"]}
                    else:
                        result = self._process_signal(signal, scan_payload)
                    results.append(result)
                    if result["status"] == "HALTED":
                        break
                return {"status": "SCANNED", "code": "OK" if signals else "NO_SIGNAL", "results": results}
            except DemoAutoError as error:
                return {"status": "REJECTED", "code": _code(error.code), "results": []}
            except Exception:
                return {**self._halt("JOURNAL_UNAVAILABLE"), "results": []}

    def tick(self):
        """Monitor the persisted route even with no new signal or stale market.

        Reconciliation never uses ``execute``: a missing router journal must
        not cause a repeated entry. Expiry cancels only the pending entry;
        an already protected position keeps its exchange protection.
        """
        with self._lock:
            intent = None
            status = None
            try:
                halt = self._halt_code()
                if halt:
                    outcome = {"status": "HALTED", "code": halt}
                    if halt in _NATURAL_EXIT_LOCAL_REFUSALS:
                        return outcome
                    active = self._active()
                    if active is not None and active[3] is not None:
                        saved_intent = _restore_intent(active[3])
                        if saved_intent.signal_id != active[0]:
                            raise DemoAutoError("JOURNAL_IDENTITY_MISMATCH")
                        # Only query already persisted emergency receipts. A
                        # HALT never grants authority to reduce a replacement
                        # position or retry a mutation whose ACK was lost.
                        receipt = self.router.resume_emergency(saved_intent)
                        outcome["emergency_status"] = getattr(receipt, "emergency_status", "UNVERIFIED")
                    return outcome
                active = self._active()
                if active is None:
                    return {"status": "IDLE", "code": "NO_ACTIVE_INTENT"}
                signal_id, status, _, raw_intent = active
                if status in {"HALTED", "UNKNOWN"}:
                    return self._halt("EXECUTION_UNCERTAIN")
                if raw_intent is None:
                    # A crash before durable planning cannot be safely guessed.
                    return self._halt("INCOMPLETE_INTENT")
                intent = _restore_intent(raw_intent)
                if intent.signal_id != signal_id:
                    raise DemoAutoError("JOURNAL_IDENTITY_MISMATCH")
                if status == "CLOSING":
                    if signal_id.upper().startswith("DEMO-TEST-"):
                        self._registered_test(signal_id, intent)
                        close_code = "DEMO_TECHNICAL_TEST_EXIT"
                    else:
                        try:
                            close_code = self._registered_natural_exit(intent)
                        except Exception as error:
                            code = (error.code if isinstance(error, DemoAutoError)
                                    and error.code in _NATURAL_EXIT_LOCAL_REFUSALS else "NATURAL_EXIT_REGISTRATION_INVALID")
                            outcome = self._halt(code)
                            self._save(signal_id, "HALTED", code)
                            return outcome
                    result = self.router.planned_close(intent, code=close_code)
                elif status == "PROTECTED":
                    result = self.router.reconcile_exit(intent)
                elif self._now() >= intent.expires_at_ms:
                    result = self.router.cancel_and_reconcile(intent)
                else:
                    result = self.router.reconcile_existing(intent)
                if getattr(result, "status", None) == "REJECTED":
                    raise DemoAutoError("ROUTE_RECONCILIATION_FAILED")
                if status == "CLOSING" and getattr(result, "status", None) not in {"CLOSING", "CLOSED", "CANCELED", "HALTED", "UNKNOWN"}:
                    raise DemoAutoError("EXIT_UNVERIFIED")
                if status == "PROTECTED" and getattr(result, "status", None) not in {"PROTECTED", "CLOSED", "HALTED", "UNKNOWN"}:
                    raise DemoAutoError("EXIT_UNVERIFIED")
                outcome = self._apply_result(intent, result, exit_checked=status in {"PROTECTED", "CLOSING"})
                if status == "PROTECTED" and outcome["status"] == "PROTECTED":
                    exit_code = self._holding_exit_code(intent)
                    if exit_code:
                        if signal_id.upper().startswith("DEMO-TEST-"):
                            # Technical exits retain their separate explicit plan
                            # authorization and existing safety deadline behavior.
                            return self._execution_failure(intent, exit_code)
                        try:
                            self._begin_natural_exit(intent, exit_code)
                        except Exception as error:
                            # No exit request was authorized. Preserve the
                            # transaction's PROTECTED row and its live bracket;
                            # a local failure cannot authorize another POST.
                            code = (error.code if isinstance(error, DemoAutoError)
                                    and error.code in _NATURAL_EXIT_LOCAL_REFUSALS else "NATURAL_EXIT_REGISTRATION_FAILED")
                            return self._halt(code)
                        status = "CLOSING"
                        self._audit("CLOSING", exit_code, signal_id)
                        result = self.router.planned_close(intent, code=exit_code)
                        if getattr(result, "status", None) not in {"CLOSING", "CLOSED", "CANCELED", "HALTED", "UNKNOWN"}:
                            raise DemoAutoError("EXIT_UNVERIFIED")
                        return self._apply_result(intent, result, exit_checked=True)
                return outcome
            except DemoAutoError as error:
                if status == "CLOSING" and intent is not None:
                    return self._planned_exit_failure(intent, error.code)
                return self._execution_failure(intent, error.code) if intent else self._halt(error.code)
            except Exception:
                if status == "CLOSING" and intent is not None:
                    return self._planned_exit_failure(intent, "PLANNED_EXIT_UNCERTAIN")
                return self._execution_failure(intent, "EXECUTION_UNCERTAIN") if intent else self._halt("JOURNAL_UNAVAILABLE")
