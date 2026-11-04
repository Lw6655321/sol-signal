"""Dedicated DEMO automatic service. Default is diagnostic, never execution.

The user authorized DEMO automation on 2026-10-19. Real-account gates remain
unchanged. --enable-demo-auto is an explicit process capability, never an
environment variable. Unverified accounting keeps the service waiting.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from contextlib import closing, contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DEMO_CALENDAR_MAX_AGE_MS = 1_800_000
DEMO_CALENDAR_REFRESH_SECONDS = 1500
DEMO_QUOTE_CLOCK_WAIT_MS = 1000


def _now():
    return time.time_ns() // 1_000_000


def _write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)


def bind_demo_account(client, state_root, *, require_trade=False):
    rows = client.get("/api/v5/account/config")
    if (len(rows) != 1 or not isinstance(rows[0].get("uid"), str)
            or not rows[0]["uid"].isascii() or not rows[0]["uid"].isdigit()):
        raise RuntimeError("DEMO_ACCOUNT_ID_UNVERIFIED")
    identity = hashlib.sha256(("DEMO\0" + rows[0]["uid"]).encode()).hexdigest()
    if require_trade and "trade" not in str(rows[0].get("perm", "")).split(","):
        raise RuntimeError("DEMO_TRADE_PERMISSION_MISSING")
    binding = state_root / "account-binding.json"
    if binding.exists():
        stored = json.loads(binding.read_text(encoding="utf-8"))
        if stored != {"environment": "DEMO", "account_hash": identity}:
            raise RuntimeError("DEMO_ACCOUNT_CHANGED")
    else:
        # Never attach an unbound pre-existing order journal to a new account.
        if any((state_root / name).exists() for name in ("controller.sqlite3", "router.sqlite3")):
            raise RuntimeError("DEMO_JOURNAL_UNBOUND")
        _write(binding, {"environment": "DEMO", "account_hash": identity})
    return identity


def record_demo_authorization(state_root, account_identity):
    """Record activation of the user's existing DEMO-only authorization.

    This is the deployment record time, not the exact time the user spoke.
    It is a conservative boundary for excluding legacy account activity.
    """
    path = state_root / "auto-authorization.json"
    if path.exists():
        record = json.loads(path.read_text(encoding="utf-8"))
    else:
        record = {"environment": "DEMO", "account_hash": account_identity,
                  "authorization_recorded_at_ms": _now(),
                  "source": "user-approved-demo-activation",
                  "scope": "SOL-USDT-SWAP_EXISTING_RISK"}
        _write(path, record)
    stamp = record.get("authorization_recorded_at_ms") if isinstance(record, dict) else None
    if (not isinstance(record, dict) or record.get("environment") != "DEMO"
            or record.get("account_hash") != account_identity
            or record.get("source") != "user-approved-demo-activation"
            or record.get("scope") != "SOL-USDT-SWAP_EXISTING_RISK"
            or type(stamp) is not int or not 0 < stamp <= _now()):
        raise RuntimeError("DEMO_AUTHORIZATION_INVALID")
    return record


def read_execution_status(state_root):
    """Read execution journals without initializing or changing either one."""
    found = []
    try:
        for filename, query in (
                ("controller.sqlite3", "SELECT status FROM controller_signals"),
                ("router.sqlite3", "SELECT status FROM routes")):
            path = state_root / filename
            if not path.exists():
                continue
            with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
                found.extend(row[0] for row in db.execute(query))
        active = [status for status in found if status not in {"REJECTED", "CANCELED", "CLOSED"}]
        return {"status": "EXISTING_EXECUTION_REQUIRES_ENABLED_RECOVERY" if active else "IDLE",
                "active_records": len(active), "writes_allowed": False}
    except Exception:
        return {"status": "UNKNOWN", "code": "EXECUTION_JOURNAL_UNREADABLE", "writes_allowed": False}


def diagnostic_once(client, config, state_root, logs):
    # A read-only diagnostic must not open the controller or router for writing,
    # latch a HALT, or try to clean up an existing execution with writes disabled.
    from src.collect import collect
    from src.demo_risk import DemoRiskProvider
    authorization_path = state_root / "auto-authorization.json"
    if authorization_path.exists() and (state_root / "router.sqlite3").exists():
        from src.demo_verified_risk import VerifiedDemoRiskProvider
        provider = VerifiedDemoRiskProvider(client, config, state_root / "verified-risk-v2",
                    authorization=json.loads(authorization_path.read_text(encoding="utf-8")),
                    router_journal_path=state_root / "router.sqlite3")
    else:
        provider = DemoRiskProvider(client, config, state_root / "risk")
    risk = provider.snapshot(market_context(collect(
        config, calendar_max_age_s=DEMO_CALENDAR_REFRESH_SECONDS)))
    status = {"observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "environment": "OKX_DEMO_ONLY", "automatic_enabled": False,
              "ready_for_new_entries": False, "execution": read_execution_status(state_root),
              "risk": {key: risk.get(key) for key in (
                  "demo_verified", "observed_at_ms", "equity_estimate", "refusal_reasons",
                  "baseline_trusted", "history_complete", "account_snapshot_consistent",
                  "collection_started_ms", "collection_completed_ms", "request_timings")}}
    path = logs / ("diagnostic-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-")
                   + os.urandom(6).hex() + ".json")
    _write(path, status)
    print(json.dumps(status, ensure_ascii=False, default=str), flush=True)
    return 0


@contextmanager
def process_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("DEMO_SERVICE_ALREADY_RUNNING") from None
        yield
    finally:
        handle.close()


def assert_service_recovery_ready(state_root):
    """Inspect existing local execution state before granting recovery writes.

    Both absent journals are a fresh installation. An existing incomplete,
    unreadable or halted execution must never be reinterpreted as a fresh run.
    This function does not construct execution objects, release owners, change
    HALT/STOP, or contact an account. The caller holds process.lock.
    """
    state_root = Path(state_root)
    try:
        if (state_root / "CONTROLLER_HALT").exists() or any(state_root.glob("HALT-*")):
            raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
        paths = [state_root / name for name in ("controller.sqlite3", "router.sqlite3")]
        exists = [path.exists() for path in paths]
        if not any(exists):
            return
        if not all(exists):
            raise RuntimeError("DEMO_SERVICE_JOURNAL_INCOMPLETE")
        with closing(sqlite3.connect(paths[0].resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            meta = dict(db.execute("SELECT key,value FROM controller_meta"))
            signals = list(db.execute("SELECT signal,status,intent FROM controller_signals"))
        with closing(sqlite3.connect(paths[1].resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            controls = list(db.execute("SELECT account,code FROM controls"))
            routes = list(db.execute("SELECT account,signal,status,data FROM routes"))
    except RuntimeError:
        raise
    except Exception:
        raise RuntimeError("DEMO_SERVICE_JOURNAL_UNREADABLE") from None
    if "halt" in meta or controls:
        raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
    if meta.get("account") != "DEMO-AUTO-SOL-V1" or meta.get("schema") != "1":
        raise RuntimeError("DEMO_SERVICE_JOURNAL_INVALID")
    terminal = {"REJECTED", "CANCELED", "CLOSED"}
    active = {"RESERVED", "SUBMITTING", "PENDING", "PROTECTING", "PROTECTED", "CLOSING"}
    signal_ids, route_ids, active_signals, active_routes = set(), set(), set(), set()
    try:
        for signal, status, raw in signals:
            if status in {"HALTED", "UNKNOWN"}:
                raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
            if not isinstance(signal, str) or not signal or status not in terminal | (active - {"PROTECTING"}):
                raise ValueError
            signal_ids.add(signal)
            if status in active:
                intent = json.loads(raw)
                if not isinstance(intent, dict) or intent.get("signal_id") != signal:
                    raise ValueError
                active_signals.add(signal)
        for account, signal, status, raw in routes:
            if status in {"HALTED", "UNKNOWN"}:
                raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
            row = json.loads(raw)
            if (account != "DEMO-AUTO-SOL-V1" or signal not in signal_ids
                    or status not in terminal | active or not isinstance(row, dict)
                    or row.get("signal_id") != signal or row.get("status") != status
                    or row.get("inst_id") != "SOL-USDT-SWAP"):
                raise ValueError
            route_ids.add(signal)
            if status in active:
                active_routes.add(signal)
        if (len(active_signals) > 1 or len(active_routes) > 1
                or not active_signals <= route_ids or not active_routes <= active_signals):
            raise ValueError
    except RuntimeError:
        raise
    except Exception:
        raise RuntimeError("DEMO_SERVICE_JOURNAL_INVALID") from None


class _ServiceSafetyScope:
    """Cleanup authority starts only after local recovery and owner checks."""
    def __init__(self, controller):
        self.controller = controller
        self.armed = False

    def arm(self):
        status = self.controller.status().get("status")
        if status in {"HALTED", "UNKNOWN"}:
            raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
        if status not in {"IDLE", "RESERVED", "SUBMITTING", "PENDING", "PROTECTED", "CLOSING"}:
            raise RuntimeError("DEMO_SERVICE_CONTROLLER_UNVERIFIED")
        self.armed = True


@contextmanager
def service_safety_guard(controller, *, defer_cleanup=False):
    """Retain cleanup for owned work, without re-authorizing an old HALT.

    Once armed, a new HALTED result does not disable this process's safety
    cleanup. The controller retains its existing CLOSING/idempotency rules.
    Production defers arming until persistent startup and owner checks pass.
    """
    scope = _ServiceSafetyScope(controller)
    if not defer_cleanup:
        scope.arm()
    try:
        yield scope
    finally:
        if scope.armed and controller.status().get("status") != "IDLE":
            controller.safety_halt("SERVICE_STOPPED")


def market_context(snapshot):
    """Use original calendar evidence age, including when a cache is rewrapped."""
    sources = snapshot.get("sources") or {}
    calendar = sources.get("economic_calendar") or {}
    source_stamp = calendar.get("source_ts")
    envelope_stamp = calendar.get("fetched_at_ms")
    now = _now()
    known_calendar = (calendar.get("ok") is True
                      and type(source_stamp) is int and type(envelope_stamp) is int
                      and 0 < source_stamp <= envelope_stamp <= now
                      and now - source_stamp <= DEMO_CALENDAR_MAX_AGE_MS)
    blackout = (snapshot.get("features") or {}).get("macro", {}).get("blackout")
    if not known_calendar or type(blackout) is not bool:
        blackout = None
    return {"observed_at_ms": now, "data_fresh": (snapshot.get("health") or {}).get("usable") is True,
            "macro_blackout": blackout}


def service_quote(client, *, clock=None):
    """One real ticker observation; source timestamps are never rewrapped."""
    now = (clock or _now)()
    rows = client.get("/api/v5/market/ticker", {"instId": "SOL-USDT-SWAP"})
    try:
        if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
                or rows[0].get("instId") != "SOL-USDT-SWAP"):
            raise ValueError
        row = rows[0]
        prices = []
        for name in ("last", "bidPx", "askPx"):
            raw = row.get(name)
            if isinstance(raw, bool) or raw is None:
                raise ValueError
            price = Decimal(str(raw))
            if not price.is_finite() or price <= 0:
                raise ValueError
            prices.append(price)
        last, bid, ask = prices
        raw_stamp = row.get("ts")
        if (type(raw_stamp) is not int and not
                (isinstance(raw_stamp, str) and raw_stamp.isascii() and raw_stamp.isdigit())):
            raise ValueError
        stamp = int(raw_stamp)
        completed = (clock or _now)()
        if (type(now) is not int or type(completed) is not int or completed < now
                or bid > ask or stamp <= 0):
            raise ValueError
        if stamp > completed:
            # A small exchange/local clock difference is handled by waiting,
            # never by accepting a future timestamp or rewriting its age.
            # One bounded wait makes no additional request. All caller clocks
            # and original scan/calendar/entry deadlines keep running.
            delay_ms = stamp - completed
            if delay_ms > DEMO_QUOTE_CLOCK_WAIT_MS:
                raise ValueError
            began = time.monotonic_ns()
            time.sleep(delay_ms / 1000)
            ended, current = time.monotonic_ns(), (clock or _now)()
            if (type(began) is not int or type(ended) is not int or began < 0
                    or not began <= ended <= began + DEMO_QUOTE_CLOCK_WAIT_MS * 1_000_000
                    or type(current) is not int or current < completed):
                raise ValueError
            completed = current
        if not 0 < stamp <= completed or completed - stamp > 5000:
            raise ValueError
    except (ValueError, TypeError, OverflowError, InvalidOperation):
        raise RuntimeError("DEMO_MARKET_UNAVAILABLE") from None
    return {"last": str(last), "bid": str(bid), "ask": str(ask), "timestamp_ms": stamp}


class ServiceEntryEvidence:
    """Original scan/calendar evidence, re-evaluated at each admission clock.

    Quote refreshes only its own source time. Original critical-source times,
    scan generation and calendar events remain immutable until the next actual
    collection. The final check is pure local work and returns the next exact
    refusal boundary so callback/signing delays cannot cross it unnoticed.
    """
    def __init__(self, config, quote, *, clock=None):
        self.config = deepcopy(config)
        self.quote = quote
        self.clock = clock or _now
        self.snapshot = None

    def update_scan(self, snapshot):
        self.snapshot = deepcopy(snapshot)

    def _boundaries(self, now):
        from src.order_router import RouterError

        if type(now) is not int or now <= 0:
            raise RouterError("DEMO_ENTRY_CLOCK_INVALID")
        snapshot = self.snapshot
        if not isinstance(snapshot, dict) or (snapshot.get("health") or {}).get("usable") is not True:
            raise RouterError("MARKET_STALE")
        stamp = snapshot.get("generated_at_ms")
        stale = self.config.get("data", {}).get("stale_seconds", {})
        try:
            scan_age = Decimal(str(stale.get("ticker", 60))) * 1000
            if (not scan_age.is_finite() or scan_age <= 0 or scan_age != int(scan_age)
                    or type(stamp) is not int or not 0 < stamp <= now):
                raise ValueError
            boundaries = [(stamp + int(scan_age) + 1, "MARKET_STALE")]
            sources = snapshot.get("sources")
            if not isinstance(sources, dict):
                raise ValueError
            for name in self.config.get("data", {}).get("critical_sources", []):
                env = sources.get(name)
                source = env.get("source_ts") if isinstance(env, dict) else None
                age = Decimal(str(stale.get(name, 900))) * 1000
                if (not isinstance(env, dict) or env.get("ok") is not True
                        or type(source) is not int or not 0 < source <= now
                        or not age.is_finite() or age <= 0 or age != int(age)):
                    raise ValueError
                boundaries.append((source + int(age) + 1, "MARKET_STALE"))
        except (ValueError, TypeError, OverflowError, InvalidOperation):
            raise RouterError("MARKET_STALE") from None
        if any(now >= deadline for deadline, _ in boundaries):
            raise RouterError("MARKET_STALE")
        calendar = sources.get("economic_calendar")
        source = calendar.get("source_ts") if isinstance(calendar, dict) else None
        fetched = calendar.get("fetched_at_ms") if isinstance(calendar, dict) else None
        events = calendar.get("data") if isinstance(calendar, dict) else None
        if (not isinstance(calendar, dict) or calendar.get("ok") is not True
                or type(source) is not int or type(fetched) is not int
                or not 0 < source <= fetched <= now or not isinstance(events, list)
                or now - source > DEMO_CALENDAR_MAX_AGE_MS):
            raise RouterError("DEMO_CALENDAR_STALE")
        boundaries.append((source + DEMO_CALENDAR_MAX_AGE_MS + 1, "DEMO_CALENDAR_STALE"))
        try:
            macro = self.config["macro"]
            before, after = (macro[name] for name in ("blackout_minutes_before", "blackout_minutes_after"))
            if (type(before) is not int or before < 0 or type(after) is not int or after < 0
                    or not isinstance(macro["block_impact"], list)
                    or not isinstance(macro["block_currencies"], list)):
                raise ValueError
            for event in events:
                if (not isinstance(event, dict) or type(event.get("ts")) is not int or event["ts"] <= 0
                        or not isinstance(event.get("impact"), str) or not isinstance(event.get("country"), str)):
                    raise ValueError
                if event["impact"] in macro["block_impact"] and event["country"] in macro["block_currencies"]:
                    start, end = event["ts"] - before * 60_000, event["ts"] + after * 60_000
                    if start <= now <= end:
                        raise RouterError("MACRO_BLACKOUT")
                    if start > now:
                        boundaries.append((start, "MACRO_BLACKOUT"))
        except RouterError:
            raise
        except (ValueError, TypeError, KeyError, OverflowError):
            raise RouterError("DEMO_CALENDAR_STALE") from None
        return boundaries

    def assert_entry_current(self, effective_now_ms):
        boundaries = self._boundaries(effective_now_ms)
        deadline, code = min(boundaries, key=lambda row: row[0])
        return {"valid_until_ms": deadline, "code": code}

    def fresh_context(self):
        from src.order_router import RouterError

        quote = self.quote()
        stamp = quote.get("timestamp_ms") if isinstance(quote, dict) else None
        now = self.clock()
        if type(stamp) is not int or type(now) is not int or not 0 < stamp <= now or now - stamp > 5000:
            raise RuntimeError("DEMO_MARKET_UNAVAILABLE")
        try:
            self._boundaries(now)
        except RouterError as error:
            return {"observed_at_ms": stamp, "data_fresh": False,
                    "macro_blackout": True if error.code == "MACRO_BLACKOUT" else None}
        return {"observed_at_ms": stamp, "data_fresh": True, "macro_blackout": False}


class ServiceRiskProvider:
    """Controller payloads never replace the service's original market evidence."""
    def __init__(self, provider, evidence):
        self.provider, self.evidence = provider, evidence

    def snapshot(self, _market=None):
        return self.provider.snapshot(self.evidence.fresh_context())


def assert_service_entry_current(state_root, evidence, effective_now_ms):
    """Recheck local entry-only flags at every strict router/transport boundary.

    This callback is not used by protection, cancellation or planned exits.
    Only an absent individual flag is clear; unreadable state fails closed.
    """
    from src.order_router import RouterError

    def flags_clear():
        try:
            for name in ("STOP_NEW_ENTRIES", "CONTROLLER_HALT"):
                try:
                    (state_root / name).lstat()
                except FileNotFoundError:
                    continue
                raise RouterError("DEMO_ENTRY_ADMISSION_FAILED")
            for path in state_root.iterdir():
                if not path.name.startswith("HALT-"):
                    continue
                try:
                    path.lstat()
                except FileNotFoundError:
                    continue
                raise RouterError("DEMO_ENTRY_ADMISSION_FAILED")
        except OSError:
            raise RouterError("DEMO_ENTRY_ADMISSION_FAILED") from None

    flags_clear()
    window = evidence.assert_entry_current(effective_now_ms)
    # A STOP created while the original evidence was checked must also win.
    flags_clear()
    return window


def current_scan_signal_filter(seen, *, now_ms=None):
    """Preview current-scan eligibility without consuming persistent cooldowns.

    Each preview has its own working copy, so same-kind/side candidates cannot
    crowd an independent candidate out of the daily quota. Only scan_to_payload
    records the final, quota-bounded selection in the caller's seen mapping.
    Missing expiry is left to controller validation for legacy payload callers;
    an explicitly expired or invalid expiry never consumes a preview slot.
    """
    now = _now() if now_ms is None else now_ms
    if not isinstance(seen, dict) or type(now) is not int or now <= 0:
        raise ValueError("DEMO_SCAN_STATE_INVALID")
    working = dict(seen)
    signal_ids = set()

    def eligible(signal):
        if not isinstance(signal, dict):
            return False
        signal_id = signal.get("id")
        if (not isinstance(signal_id, str) or not signal_id or signal_id in signal_ids
                or signal_id.upper().startswith(("RSCH-", "DEMO-TEST-"))
                or signal.get("research") is True):
            return False
        kind, side = signal.get("kind"), signal.get("side")
        if not isinstance(kind, str) or not kind or side not in {"long", "short"}:
            return False
        if "expires_at_ms" in signal or "expires_at_utc" in signal:
            try:
                if "expires_at_ms" in signal:
                    expiry = signal["expires_at_ms"]
                    if type(expiry) is not int:
                        return False
                else:
                    stamp = datetime.fromisoformat(signal["expires_at_utc"].replace("Z", "+00:00"))
                    if stamp.tzinfo is None:
                        return False
                    expiry = int(stamp.timestamp() * 1000)
                if expiry <= now:
                    return False
            except (AttributeError, TypeError, ValueError, OverflowError, OSError):
                return False
        key = f"{kind}|{side}"
        previous = working.get(key, 0)
        if type(previous) is not int or previous < 0:
            raise ValueError("DEMO_SCAN_STATE_INVALID")
        cool = max(30, int(signal.get("expiry_min", 30))) * 60_000
        if previous and now - previous < cool:
            return False
        working[key] = now
        signal_ids.add(signal_id)
        return True

    return eligible


def scan_to_payload(snapshot, evaluation, seen, config):
    context = market_context(snapshot)
    now = _now()
    eligible = current_scan_signal_filter(seen, now_ms=now)
    current = []
    for signal in evaluation.get("signals", []):
        if not eligible(signal):
            continue
        # Only the current engine output is eligible. This is not historical
        # replay and never reads the paper scanner's CSV as execution input.
        current.append(signal)
        seen[f"{signal['kind']}|{signal['side']}"] = now
    return {**context, "generated_at_ms": now,
            "gates": evaluation.get("gates"), "new_signals": current}


def service_risk_summary(risk):
    summary = {key: risk.get(key) for key in (
        "demo_verified", "observed_at_ms", "equity_estimate", "day_start_equity",
        "week_start_equity", "entries_today", "realized_pnl_today", "realized_pnl_week",
        "refusal_reasons", "equity_scope", "baseline_trusted", "risk_adapter",
        "ledger_verified", "pause_evidence_verified", "history_complete",
        "account_snapshot_consistent", "collection_completed_ms")}
    summary["request_faults"] = [
        {key: fault.get(key) for key in ("path", "code", "numeric", "source_fault")}
        for fault in risk.get("request_faults", []) if isinstance(fault, dict)]
    return summary


def run_service(*, enabled=False, continuous=False, scan_interval=60, poll_interval=2):
    if enabled and not continuous:
        raise RuntimeError("DEMO_AUTO_REQUIRES_CONTINUOUS_MONITOR")
    # Lock before credentials/account binding or either order journal is opened.
    # A second process must not touch the first process's account namespace.
    with process_lock(ROOT / "state" / "demo-auto" / "process.lock"):
        if enabled:
            assert_service_recovery_ready(ROOT / "state" / "demo-auto")
            from src.demo_successor_gate import assert_activated_generation
            assert_activated_generation(ROOT)
            from src.demo_runtime_get import runtime_demo_get
            # The locked service's safety guard finishes before the pool closes,
            # and the process lock outlives both. Diagnostics keep their old path.
            with runtime_demo_get() as http_get:
                return _run_locked_service(enabled=enabled, continuous=continuous,
                    scan_interval=scan_interval, poll_interval=poll_interval, http_get=http_get)
        return _run_locked_service(enabled=enabled, continuous=continuous,
                                   scan_interval=scan_interval, poll_interval=poll_interval)


def _run_locked_service(*, enabled=False, continuous=False, scan_interval=60, poll_interval=2,
                        http_get=None):
    from src import engine
    from src.collect import collect, trimmed
    from src.demo_auto import DEMO_ACCOUNT_KEY, DemoAutoController
    from src.demo_verified_risk import VerifiedDemoRiskProvider
    from src.demo_route_transport import DemoRouteTransport
    from src.demo_transport import DemoReadOnlyClient, load_demo_credentials
    from src.order_router import OrderRouter
    from src.util import load_config

    config = load_config()
    if config.get("account", {}).get("signal_creates_position") is not False:
        raise RuntimeError("PAPER_POSITION_SETTING_INVALID")
    state_root = ROOT / "state" / "demo-auto"
    state_root.mkdir(parents=True, exist_ok=True)
    logs = ROOT / "logs" / "demo-auto"
    if enabled:
        assert_service_recovery_ready(state_root)
        from src.demo_successor_gate import assert_activated_generation
        assert_activated_generation(ROOT)
    credentials = load_demo_credentials()
    # Binding has one physical GET. Only the verified evidence provider opts
    # into bounded transient GET recovery with its own same-path rate budget.
    client = DemoReadOnlyClient(credentials, http_get=http_get, network_retries=0)
    account_identity = bind_demo_account(client, state_root, require_trade=enabled)
    if not enabled:
        return diagnostic_once(client, config, state_root, logs)
    authorization = record_demo_authorization(state_root, account_identity)
    transport = DemoRouteTransport(credentials, enable_writes=enabled, http_get=http_get, log_root=logs)
    provider = None

    def reserve_retry(path):
        if provider is None:
            raise RuntimeError("DEMO_RETRY_BUDGET_UNAVAILABLE")
        provider._reserve_get(path)

    evidence_client = DemoReadOnlyClient(credentials, http_get=http_get,
                                       network_retries=1, retry_budget=reserve_retry)
    provider = VerifiedDemoRiskProvider(evidence_client, config, state_root / "verified-risk-v2",
                authorization=authorization, router_journal_path=state_root / "router.sqlite3")
    market = lambda: service_quote(evidence_client)
    evidence = ServiceEntryEvidence(config, market)
    risk_provider = ServiceRiskProvider(provider, evidence)

    router = OrderRouter(transport, config=config, state_provider=risk_provider.snapshot,
                         market_provider=market, log_root=logs,
                         journal_path=state_root / "router.sqlite3", account_key=DEMO_ACCOUNT_KEY,
                         account_identity=account_identity, require_demo_entry_admission=True,
                          final_entry_check=lambda now: assert_service_entry_current(state_root, evidence, now))
    controller = DemoAutoController(config, router, risk_provider, market, ROOT)
    with service_safety_guard(controller, defer_cleanup=True) as safety:
        # run_service owns this directory's process.lock before reaching here.
        # A hard-killed former process cannot retain its SQLite row owner and
        # block protection of an exchange fill after a verified restart.
        assert_service_recovery_ready(state_root)
        assert_activated_generation(ROOT)
        router.recover_demo_owners()
        assert_service_recovery_ready(state_root)
        if getattr(router, "_local_halted", False):
            raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
        safety.arm()
        controller.tick()
        seen_path = state_root / "scan-seen.json"
        seen = json.loads(seen_path.read_text(encoding="utf-8")) if seen_path.exists() else {}
        if not isinstance(seen, dict):
            raise RuntimeError("DEMO_SCAN_STATE_INVALID")
        snapshot = None
        next_scan = 0.0
        count = 0
        while True:
            count += 1
            tick = controller.tick() if enabled else {"status": "DIAGNOSTIC", "code": "WRITES_DISABLED"}
            result = None
            risk = None
            controller_state = controller.status()
            active = controller_state.get("status") != "IDLE"
            if (time.monotonic() >= next_scan or not continuous) and not active:
                # Reconciliation precedes the relatively slow public scan.
                snapshot = collect(config, snapshot,
                                   calendar_max_age_s=DEMO_CALENDAR_REFRESH_SECONDS)
                evidence.update_scan(snapshot)
                risk = risk_provider.snapshot()
                engine_risk = dict(risk)
                engine_risk["halted"] = risk.get("demo_verified") is not True or controller.status().get("status") == "HALTED"
                engine_risk["entries_left"] = max(0, int(config["risk"]["max_entries_per_day"]) - int(risk.get("entries_today", 0)))
                if risk.get("equity_estimate") is not None:
                    snapshot["equity_estimate"] = risk["equity_estimate"]
                evaluation = engine.evaluate(snapshot, config, engine_risk,
                    signal_filter=current_scan_signal_filter(seen))
                payload = scan_to_payload(snapshot, evaluation, seen, config)
                payload.update(evidence.fresh_context())
                _write(seen_path, seen)
                stop_requested = (state_root / "STOP_NEW_ENTRIES").exists()
                if enabled and not stop_requested:
                    result = controller.process_scan(payload)
                else:
                    result = {"status": "WAITING", "code": "STOP_REQUESTED" if stop_requested else "WRITES_DISABLED"}
                _write(state_root / "market.json", {"snapshot": trimmed(snapshot), "eval": evaluation,
                                                      "updated_ms": _now()})
                next_scan = time.monotonic() + scan_interval
            previous = {}
            status_path = state_root / "service.json"
            if status_path.exists():
                previous = json.loads(status_path.read_text(encoding="utf-8"))
            risk_summary = previous.get("risk", {}) if risk is None else service_risk_summary(risk)
            status = {"observed_at_utc": datetime.now(timezone.utc).isoformat(), "observed_at_ms": _now(),
                      "pid": os.getpid(), "environment": "OKX_DEMO_ONLY", "automatic_enabled": enabled,
                      "ready_for_new_entries": enabled and risk_summary.get("demo_verified") is True
                          and controller.status().get("status") == "IDLE"
                          and not risk_summary.get("refusal_reasons")
                          and type(risk_summary.get("observed_at_ms")) is int
                          and 0 <= _now() - risk_summary["observed_at_ms"] <= 5000
                          and not (state_root / "STOP_NEW_ENTRIES").exists(),
                      "controller": controller.status(), "risk": risk_summary,
                      "tick": tick, "scan_result": result or previous.get("scan_result"), "cycles": count}
            _write(status_path, status)
            print(json.dumps(status, ensure_ascii=False, default=str), flush=True)
            if not continuous:
                return 0
            # process_scan may have opened a route after this loop's old IDLE
            # tick. Keep supervising owned work until the current journal is
            # terminal; HALTED/UNKNOWN are not proof that draining completed.
            if ((state_root / "STOP_NEW_ENTRIES").exists()
                    and controller.status().get("status") == "IDLE"):
                return 0
            time.sleep(poll_interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description="独立OKX模拟盘自动执行；默认只读诊断")
    parser.add_argument("--run", action="store_true", help="持续运行（须显式允许DEMO自动模式）")
    parser.add_argument("--enable-demo-auto", action="store_true", help="允许已授权模拟盘自动执行，仍须真实风控ready")
    parser.add_argument("--scan-interval", type=float, default=60)
    parser.add_argument("--poll-interval", type=float, default=2)
    parser.add_argument("--stop", action="store_true", help="停止新开仓并继续核对既有保护，空仓后退出")
    args = parser.parse_args(argv)
    if args.stop:
        path = ROOT / "state" / "demo-auto" / "STOP_NEW_ENTRIES"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("OPERATOR_STOP\n", encoding="utf-8")
        print("DEMO_STOP_REQUESTED")
        return 0
    if args.run and not args.enable_demo_auto:
        print("DEMO_AUTO_EXPLICIT_ENABLE_REQUIRED")
        return 2
    if args.enable_demo_auto and not args.run:
        print("DEMO_AUTO_REQUIRES_CONTINUOUS_MONITOR")
        return 2
    if not (10 <= args.scan_interval <= 300 and 0.5 <= args.poll_interval <= 5):
        print("DEMO_INTERVAL_INVALID")
        return 2
    try:
        return run_service(enabled=args.enable_demo_auto, continuous=args.run,
                           scan_interval=args.scan_interval, poll_interval=args.poll_interval)
    except Exception:
        print("DEMO_SERVICE_ERROR: stopped; inspect sanitized local diagnostics; never retry a write blindly")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
