"""Evidence-backed DEMO admission, separate from the frozen diagnostic provider.

Only injected fixed-DEMO GET transport is used. Authorization is a trusted local
activation record supplied by the service, not a timestamp attributed to the
human's message. The first UID-bound record is frozen locally; changing it never
selects a newer legacy anchor or clears a router HALT.

The parent provider supplies before/after account observations, all eight algo
types, coverage tails, real GET timings and the unchanged five-second admission
deadline. This adapter collects the actual historical reset anchor, complete
post-anchor bills/fills and strong order details, then calls the pure ledger.
Only a verified ledger and flat UTC boundaries can reconstruct starting equity.
Nothing here enables an executor or sends account writes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from . import demo_risk as D
from .demo_ledger import validate_demo_ledger
from .demo_transport import DemoTransportError, NETWORK_FAILURE_CATEGORIES

DAY_MS = 86_400_000
ANCHOR_SEARCH_MS = 7 * DAY_MS
# A conservative subset of the historical endpoints' retention. Beyond this
# horizon a separately reviewed checkpoint/retention adapter is required.
MAX_PROVEN_LOOKBACK_MS = 28 * DAY_MS
AUTH_SOURCE = "user-approved-demo-activation"
AUTH_SCOPE = "SOL-USDT-SWAP_EXISTING_RISK"
FROZEN_PARENT_FILLS = {"SWAP", "FUTURES", "OPTION", "SPOT", "MARGIN"}
VERIFIED_FILL_TYPES = (*D.FILL_TYPES, "EVENTS")
REQUIRED_FILLS = FROZEN_PARENT_FILLS | {"EVENTS"}
REQUIRED_POSITIONS = {"SWAP", "FUTURES", "OPTION", "MARGIN"}
REQUIRED_ALGOS = {"conditional", "oco", "trigger", "move_order_stop", "iceberg", "twap",
                  "smart_iceberg", "chase"}
FUTURES_NA_FIELDS = ("liab", "crossLiab", "isoLiab", "interest", "borrowFroz")
# Official endpoint limits, checked 2026-10-19, https://www.okx.com/docs-v5/en/ .
# Private limits below use UID; treating order-details as one UID bucket is more
# conservative than its documented UID+instrument bucket. Other processes can
# still consume the same UID budget: 429/50011 remain refusals, never retries.
GET_LIMITS_PER_TWO_SECONDS = {
    "/api/v5/account/config": 5, "/api/v5/account/balance": 10,
    "/api/v5/account/positions": 10, "/api/v5/account/leverage-info": 20,
    "/api/v5/trade/orders-pending": 60, "/api/v5/trade/orders-algo-pending": 20,
    "/api/v5/account/bills-archive": 5, "/api/v5/trade/fills-history": 10,
    "/api/v5/account/positions-history": 10, "/api/v5/trade/order": 60}
SAFE_GET_PATHS = set(GET_LIMITS_PER_TWO_SECONDS) | {"/api/v5/public/time", "/api/v5/public/instruments"}
SAFE_SOURCE_CODES = {"DEMO_BASE_LOCKED", "DEMO_PATH_NOT_ALLOWED", "DEMO_REDIRECT_FORBIDDEN",
                     "DEMO_BAD_RESPONSE", "DEMO_RESPONSE_TOO_LARGE", "DEMO_BAD_JSON",
                     "DEMO_NETWORK_ERROR", "DEMO_HTTP_ERROR", "DEMO_API_ERROR"}
RATE_WINDOW_SECONDS = 2.01  # Conservative 10ms guard for request-dispatch/clock granularity.


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        raise D.EvidenceRefused("VERIFIED_HISTORY_ID_INVALID")
    return value


class VerifiedDemoRiskProvider(D.DemoRiskProvider):
    """Collect and verify account evidence; no credentials or trading functions.

    ``authorization`` requires environment/account_hash,
    authorization_recorded_at_ms, source and scope. ``router_journal_path`` is
    opened mode=ro only; a missing journal is a refusal, never a newly created DB.
    ``state_dir`` should be a dedicated verified-risk directory. No diagnostic
    state, production state, or HALT file is modified.

    ``diagnostic_halted_ledger=True`` is an explicit read-only forensic channel:
    canonical same-account HALT controls do not stop history collection, but
    every returned snapshot retains ROUTER_HALTED and can never admit trading.
    It does not clear, replace, or reinterpret any router status or HALT flag.
    """

    def __init__(self, client, config, state_dir, *, authorization,
                 router_journal_path, account_key="DEMO-AUTO-SOL-V1", clock=None,
                 rate_clock=None, rate_sleep=None, diagnostic_halted_ledger=False):
        if type(diagnostic_halted_ledger) is not bool:
            raise D.EvidenceRefused("DIAGNOSTIC_HALTED_LEDGER_INVALID")
        super().__init__(client, config, state_dir, clock=clock)
        self.diagnostic_halted_ledger = diagnostic_halted_ledger
        self.authorization = copy.deepcopy(authorization)
        self.router_journal_path = Path(router_journal_path)
        self.account_key = account_key
        self._rate_lock = threading.Lock()
        self._rate_windows = {}
        self._rate_clock = rate_clock or time.monotonic
        self._rate_sleep = rate_sleep or time.sleep
        self._request_faults = []

    def _reserve_get(self, path):
        capacity = GET_LIMITS_PER_TWO_SECONDS.get(path)
        if capacity is None:
            return
        while True:
            with self._rate_lock:
                now = self._rate_clock()
                window = self._rate_windows.setdefault(path, deque())
                while window and now-window[0] >= RATE_WINDOW_SECONDS:
                    window.popleft()
                if len(window) < capacity:
                    window.append(now)
                    return
                wait = max(0.001, window[0]+RATE_WINDOW_SECONDS-now)
            self._rate_sleep(wait)

    def _get(self, path, params=None):
        self._reserve_get(path)
        try:
            # Parent timestamps are actual GET starts, after any budget queue.
            return super()._get(path, params)
        except DemoTransportError as error:
            code = error.code if error.code in SAFE_SOURCE_CODES else "DEMO_SOURCE_REQUEST_FAILED"
            numeric = error.numeric if type(error.numeric) is int and 0 <= error.numeric <= 99_999_999 else None
            fault = {"path": path if path in SAFE_GET_PATHS else "UNKNOWN_SOURCE", "code": code,
                     "numeric": numeric,
                     "source_fault": ("DEMO_SOURCE_RATE_LIMITED" if numeric in (429, 50011)
                                      else "DEMO_SOURCE_REQUEST_FAILED")}
            # Optional diagnostics must not replace the original refusal or
            # disclose exception text, account identity, URLs or headers.
            if code == "DEMO_NETWORK_ERROR":
                try:
                    category = getattr(error, "network_category", None)
                except Exception:
                    category = None
                if category is not None:
                    fault["network_category"] = (
                        category if type(category) is str
                        and category in NETWORK_FAILURE_CATEGORIES else "UNKNOWN")
            with self._timing_lock:
                self._request_faults.append(fault)
            raise

    @staticmethod
    def _parallel(jobs):
        results = {}
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="demo-verified-evidence") as pool:
            futures = {pool.submit(job): name for name, job in jobs.items()}
            try:
                for future in as_completed(futures):
                    results[futures[future]] = future.result()
            except Exception:
                for future in futures:
                    future.cancel()
                raise
        return results

    def _snapshot(self, market_snapshot=None):
        self._views = []
        self._request_faults = []
        self._account_get_batches = []
        self._compatibility_evidence = []
        self._anchor = None
        self._details = {}
        self._source_ranges = []
        self._fill_coverage = []
        self._bill_coverage = []
        self._position_coverage = []
        self._source_routes = None
        self._router_halt_observations = []
        self._source_router_halt_evidence = None
        self._router_halt_evidence = None
        self._ledger = None
        self._cache = None
        self._auth = None
        self._authorization_error = None
        try:
            self._auth = self._authorization_record()
        except D.EvidenceRefused as error:
            self._authorization_error = error.code
        result = super()._snapshot(market_snapshot)
        if self._authorization_error:
            result["refusal_reasons"].append(self._authorization_error)
        if self._ledger is not None and self._ledger.get("verified") is True:
            # Replace only the parent's explicit unintegrated-adapter gate.
            # Market/account/history/refusal codes are never silently removed.
            result["refusal_reasons"] = [reason for reason in result["refusal_reasons"]
                                         if reason != "LEDGER_ADAPTER_UNVERIFIED"]
            result["ledger_verified"] = True
            result["pause_evidence_verified"] = True
        result["refusal_reasons"] = list(dict.fromkeys(result["refusal_reasons"]))
        finished = D._integer(self.clock(), "CLOCK_INVALID")
        observed = result.get("observed_at_ms")
        if observed is None or not 0 <= finished - observed <= D.MAX_ACCOUNT_AGE_MS:
            result["refusal_reasons"].append("ACCOUNT_STALE")
        result["refusal_reasons"] = list(dict.fromkeys(result["refusal_reasons"]))
        if self.diagnostic_halted_ledger:
            # This opt-in is a permanently non-admitting diagnostic channel.
            # Keep ledger_verified's existing pure-ledger meaning separate from
            # a complete collection that also passed every original gate.
            result["diagnostic_only"] = True
            result["diagnostic_ledger_complete"] = (
                result["ledger_verified"] is True
                and result["history_complete"] is True
                and result["account_snapshot_consistent"] is True
                and not result["refusal_reasons"])
            result["router_halt_observations"] = copy.deepcopy(self._router_halt_observations)
            result["refusal_reasons"].append("ROUTER_HALTED")
            result["refusal_reasons"] = list(dict.fromkeys(result["refusal_reasons"]))
        result["demo_verified"] = not result["refusal_reasons"]
        result["halted"] = not result["demo_verified"]
        result["live_verified"] = False
        result["risk_adapter"] = "verified-demo-ledger-v2"
        result["request_faults"] = copy.deepcopy(self._request_faults)
        result["account_get_batches"] = copy.deepcopy(self._account_get_batches)
        result["account_observation_basis"] = "conservative-batch-start-includes-rate-queue"
        result["collection_completed_ms"] = finished
        if self._auth:
            result["authorization_recorded_at_ms"] = self._auth["authorization_recorded_at_ms"]
            result["authorization_time_meaning"] = "local-activation-record-upper-bound"
        return result

    def _authorization_record(self):
        raw = self.authorization
        if not isinstance(raw, Mapping):
            raise D.EvidenceRefused("DEMO_AUTHORIZATION_MISSING")
        account = raw.get("account_hash")
        stamp = D._integer(raw.get("authorization_recorded_at_ms"), "DEMO_AUTHORIZATION_INVALID")
        if (raw.get("environment") != "DEMO" or raw.get("source") != AUTH_SOURCE
                or raw.get("scope") != AUTH_SCOPE
                or not isinstance(account, str) or not re.fullmatch(r"[0-9a-f]{64}", account)
                or not 0 < stamp <= D._integer(self.clock(), "CLOCK_INVALID")):
            raise D.EvidenceRefused("DEMO_AUTHORIZATION_INVALID")
        if "service_started_at_ms" in raw:
            started = D._integer(raw["service_started_at_ms"], "DEMO_AUTHORIZATION_INVALID")
            if not stamp <= started <= D._integer(self.clock()):
                raise D.EvidenceRefused("DEMO_AUTHORIZATION_INVALID")
        return {key: raw[key] for key in ("environment", "account_hash",
                "authorization_recorded_at_ms", "source", "scope")}

    @staticmethod
    def _write(path, payload):
        temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with temp.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, allow_nan=False)
                handle.flush()
                D.os.fsync(handle.fileno())
            D.os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)

    def _account_view(self):
        queued_at = D._integer(self.clock(), "CLOCK_INVALID")
        view, start, end = super()._account_view()
        self._account_get_batches.append({"queued_at_ms": queued_at,
                                         "earliest_get_started_ms": start, "finished_at_ms": end})
        self._views.append(copy.deepcopy(view))
        if self._authorization_error:
            raise D.EvidenceRefused(self._authorization_error)
        uid = view["config"].get("uid")
        if not isinstance(uid, str) or not uid.isascii() or not uid.isdigit():
            raise D.EvidenceRefused("ACCOUNT_ID_UNVERIFIED")
        account = hashlib.sha256(("DEMO\0" + uid).encode()).hexdigest()
        if account != self._auth["account_hash"]:
            raise D.EvidenceRefused("DEMO_AUTHORIZATION_ACCOUNT_MISMATCH")
        directory = self.state_dir / account
        with D._state_lock(directory):
            binding = directory / "authorization-binding.json"
            if binding.exists():
                try:
                    if json.loads(binding.read_text(encoding="utf-8")) != self._auth:
                        raise D.EvidenceRefused("DEMO_AUTHORIZATION_CHANGED")
                except (ValueError, OSError):
                    raise D.EvidenceRefused("VERIFIED_STATE_INVALID") from None
            else:
                self._write(binding, self._auth)
            cache_path = directory / "verified.json"
            if cache_path.exists():
                try:
                    cache = json.loads(cache_path.read_text(encoding="utf-8"))
                    if (cache.get("schema") != 2 or cache.get("account_hash") != account
                            or cache.get("authorization_digest") != _digest(self._auth)
                            or not isinstance(cache.get("anchor"), dict)):
                        raise ValueError()
                    self._validate_cache(cache)
                    self._cache = cache
                except (ValueError, OSError, AttributeError):
                    raise D.EvidenceRefused("VERIFIED_STATE_INVALID") from None
        # Official Get-balance applicability matrix:
        # https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-balance
        # In Futures mode these five fields are not applicable and the API uses
        # the empty string. Preserve that raw evidence; only the compatibility
        # view consumed by the frozen diagnostic number parser uses the implied
        # no-borrowing constraint. Missing, malformed or nonzero is not N/A.
        compatible = copy.deepcopy(view)
        na_evidence = []
        if view["config"].get("acctLv") == "2" and view["config"].get("posMode") == "net_mode":
            details = compatible["balance"].get("details")
            if not isinstance(details, list) or any(not isinstance(row, dict) for row in details):
                raise D.EvidenceRefused("ACCOUNT_DATA_INVALID")
            for detail in details:
                for field in FUTURES_NA_FIELDS:
                    if field not in detail:
                        raise D.EvidenceRefused("FUTURES_APPLICABILITY_FIELD_MISSING")
                    if detail[field] == "":
                        detail[field] = "0"
                        na_evidence.append({"ccy": detail.get("ccy"), "field": field,
                                            "raw_value": "", "account_level": "2",
                                            "meaning": "official-not-applicable"})
                    elif D._number(detail[field], "FUTURES_APPLICABILITY_FIELD_INVALID") != 0:
                        raise D.EvidenceRefused("ACCOUNT_LIABILITY_UNSUPPORTED")
        raw_fingerprint = hashlib.sha256(D.DemoRiskProvider._account_fingerprint(view).encode()).hexdigest()
        compatible["_raw_account_fingerprint"] = raw_fingerprint
        self._compatibility_evidence.append({"raw_fingerprint": raw_fingerprint,
                                             "not_applicable_fields": na_evidence})
        return compatible, min(start, queued_at), end

    def _validate_cache(self, cache):
        """A prior period proof may not be relabeled to bypass cash continuity."""
        try:
            through = D._integer(cache.get("complete_through_ms"))
            began = D._integer(cache.get("collection_started_ms"))
            observed = D._integer(cache.get("account_observed_at_ms"))
            day, week = D._boundaries(through)
            anchor_close = D._integer(cache["anchor"].get("uTime"))
            if (cache.get("environment") != "DEMO"
                    or set(cache.get("fill_product_scopes", [])) != REQUIRED_FILLS
                    or cache.get("day_start_ms") != day or cache.get("week_start_ms") != week
                    or cache.get("coverage_start_ms") != anchor_close+1
                    or D._integer(cache.get("anchor_lookback_start_ms")) > anchor_close-DAY_MS
                    or not 0 <= through-began <= D.MAX_SNAPSHOT_WINDOW_MS
                    or not began-D.MAX_SKEW_MS <= observed <= through+D.MAX_SKEW_MS
                    or through > D._integer(self.clock())+D.MAX_SKEW_MS
                    or min(D._number(cache.get("day_start_equity")),
                           D._number(cache.get("week_start_equity"))) <= 0
                    or _digest(cache.get("evidence")) != cache.get("ledger_evidence_digest")):
                raise ValueError()
            sources = cache.get("source_coverage")
            if not isinstance(sources, dict):
                raise ValueError()
            expected = anchor_close+1
            windows = []
            for row in sources["bills"]:
                start, end = D._integer(row["start_ms"]), D._integer(row["end_ms"])
                if start != expected or end < start or row["currency_filter"] is not None:
                    raise ValueError()
                windows.append((start, end))
                expected = end+1
            if expected != through+1:
                raise ValueError()
            fill_windows = [(D._integer(row["start_ms"]), D._integer(row["end_ms"]))
                            for row in sources["fills"] if set(row["products"]) == REQUIRED_FILLS]
            if fill_windows != windows:
                raise ValueError()
            observations = cache.get("balance_observations")
            if (not isinstance(observations, list) or len(observations) != 2
                    or {row.get("stage") for row in observations} != {"account-before", "account-after"}):
                raise ValueError()
            for row in observations:
                start, end = D._integer(row["started_at_ms"]), D._integer(row["finished_at_ms"])
                if (row.get("path") != "/api/v5/account/balance" or not start <= end
                        or not began-D.MAX_SKEW_MS <= start <= end <= through+D.MAX_SKEW_MS):
                    raise ValueError()
        except (D.EvidenceRefused, ValueError, TypeError, KeyError, OverflowError, OSError):
            raise D.EvidenceRefused("VERIFIED_STATE_INVALID") from None

    def _account_fingerprint(self, view):
        return _digest({"compatibility": D.DemoRiskProvider._account_fingerprint(view),
                        "raw_fingerprint": view.get("_raw_account_fingerprint")})

    def _read_routes(self):
        if not self.router_journal_path.is_file():
            raise D.EvidenceRefused("ROUTER_JOURNAL_MISSING")
        try:
            uri = self.router_journal_path.resolve().as_uri() + "?mode=ro"
            with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
                db.execute("PRAGMA query_only=ON")
                db.execute("BEGIN")
                controls = db.execute("SELECT account,code FROM controls").fetchall()
                rows = db.execute("SELECT account,signal,status,client,data FROM routes").fetchall()
            if controls and not self.diagnostic_halted_ledger:
                raise D.EvidenceRefused("ROUTER_HALTED")
            if self.diagnostic_halted_ledger:
                if (len(controls) > 1 or any(account != self.account_key
                        or not isinstance(code, str)
                        or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", code)
                        for account, code in controls)):
                    raise D.EvidenceRefused("ROUTER_JOURNAL_INVALID")
            if len(rows) > 10000:
                raise D.EvidenceRefused("ROUTER_JOURNAL_TOO_LARGE")
            routes = []
            for account, signal, status, client, text in rows:
                if account != self.account_key or not isinstance(text, str) or len(text) > 1_000_000:
                    raise D.EvidenceRefused("ROUTER_JOURNAL_INVALID")
                route = json.loads(text)
                if (not isinstance(route, dict) or route.get("account_hash") != self._auth["account_hash"]
                        or route.get("signal_id") != signal or route.get("status") != status
                        or route.get("client_order_id") != client):
                    raise D.EvidenceRefused("ROUTER_JOURNAL_INVALID")
                if self.diagnostic_halted_ledger and status not in {
                        "RESERVED", "SUBMITTING", "PENDING", "PROTECTING", "PROTECTED",
                        "CLOSING", "UNKNOWN", "HALTED", "CLOSED", "CANCELED", "REJECTED"}:
                    raise D.EvidenceRefused("ROUTER_JOURNAL_INVALID")
                if (D._number(route.get("filled_qty")) > 0
                        and D._integer(route.get("position_opened_at_ms"), "VERIF_ROUTE_POSITION_TIME_INVALID") <
                        self._auth["authorization_recorded_at_ms"]):
                    raise D.EvidenceRefused("ROUTE_PRECEDES_ACTIVATION")
                routes.append(route)
            if self.diagnostic_halted_ledger:
                counts = {}
                for route in routes:
                    counts[route["status"]] = counts.get(route["status"], 0) + 1
                halt_path = self.router_journal_path.parent / (
                    "HALT-" + hashlib.sha256(self.account_key.encode()).hexdigest()[:12])
                self._router_halt_evidence = {
                    "control_codes": sorted(code for _, code in controls),
                    "route_status_counts": dict(sorted(counts.items())),
                    "halt_flag_present": halt_path.is_file()}
                self._router_halt_observations.append(copy.deepcopy(self._router_halt_evidence))
            return sorted(routes, key=lambda row: row["signal_id"])
        except D.EvidenceRefused:
            raise
        except (sqlite3.Error, OSError, ValueError, TypeError):
            raise D.EvidenceRefused("ROUTER_JOURNAL_INVALID") from None

    def _anchor_candidate(self, row, end):
        try:
            opened = D._integer(row.get("cTime"))
            closed = D._integer(row.get("uTime"))
            day, week = D._boundaries(end)
            maximum = D._number(row.get("openMaxPos"))
            net = D._number(row.get("realizedPnl"))
            summed = sum((D._number(row.get(key)) for key in
                          ("pnl", "fee", "fundingFee", "liqPenalty")), Decimal(0))
            return (opened <= closed < min(day, week)
                    and closed <= self._auth["authorization_recorded_at_ms"]
                    and row.get("ccy") == "USDT" and row.get("instType") in REQUIRED_POSITIONS
                    and maximum > 0 and D._number(row.get("closeTotalPos")) == maximum
                    and min(net, summed) >= 0 and abs(net-summed) <= Decimal("0.00000001")
                    and bool(_safe_id(row.get("posId"))) and bool(_safe_id(row.get("instId")))
                    and bool(_safe_id(row.get("ccy"))))
        except D.EvidenceRefused:
            return False

    def _position_scope(self, start, end):
        jobs = {kind: (lambda kind=kind: self._positions_history(start, end, kind))
                for kind in D.POSITION_TYPES}
        rows = self._parallel(jobs)
        self._position_coverage.append({"start_ms": start, "end_ms": end,
                                        "products": sorted(rows)})
        return [row for kind in D.POSITION_TYPES for row in rows[kind]]

    def _positions_history(self, start, end, kind):
        """Refuse unprovable timestamp page boundaries; never skip unseen ties.

        A full page whose oldest uTime is inside the desired window cannot prove
        that another row at that exclusive cursor was not omitted. A future
        overlap adapter may exhaust those ties; this version refuses instead.
        """
        rows = self._get("/api/v5/account/positions-history",
                         {"instType": kind, "limit": str(D.PAGE_LIMIT), "after": str(end + 1)})
        if len(rows) > D.PAGE_LIMIT:
            raise D.EvidenceRefused("POSITION_HISTORY_INCOMPLETE")
        seen, stamps = {}, []
        for row in rows:
            stamp = D._integer(row.get("uTime"), "POSITION_HISTORY_TIME_INVALID")
            created = D._integer(row.get("cTime"), "POSITION_HISTORY_TIME_INVALID")
            if created > stamp or stamp > end:
                raise D.EvidenceRefused("POSITION_HISTORY_CURSOR_INVALID")
            if row.get("instType") != kind:
                raise D.EvidenceRefused("VERIFIED_POSITION_SCOPE_MISMATCH")
            key = (_safe_id(row.get("instId")), _safe_id(row.get("posId")), created, stamp)
            if key in seen and seen[key] != row:
                raise D.EvidenceRefused("POSITION_HISTORY_DUPLICATE_CONFLICT")
            seen[key] = row
            stamps.append(stamp)
        if any(left < right for left, right in zip(stamps, stamps[1:])):
            raise D.EvidenceRefused("POSITION_HISTORY_CURSOR_INVALID")
        if len(rows) == D.PAGE_LIMIT and min(stamps) >= start:
            raise D.EvidenceRefused("POSITION_HISTORY_BOUNDARY_UNPROVEN")
        return [row for key, row in seen.items() if key[3] >= start]

    def _order_details(self, fills):
        wanted = {}
        for fill in fills:
            if fill.get("instId") != D.INST or fill.get("instType") != "SWAP":
                raise D.EvidenceRefused("LEDGER_FILL_SCOPE")
            order = _safe_id(fill.get("ordId"))
            wanted[order] = fill["instId"]
        jobs = {order: (lambda order=order, inst=inst: self._one(
                    "/api/v5/trade/order", {"instId": inst, "ordId": order}))
                for order, inst in wanted.items() if order not in self._details}
        if jobs:
            self._details.update(self._parallel(jobs))

    def _fills_scope(self, start, end, kind):
        rows = self._history("/api/v5/trade/fills-history", start, end, {"instType": kind})
        if any(row.get("instType") != kind for row in rows):
            raise D.EvidenceRefused("VERIFIED_FILL_SCOPE_MISMATCH")
        return rows

    def _history_evidence(self, start, end):
        if self._authorization_error:
            raise D.EvidenceRefused(self._authorization_error)
        if (set(D.FILL_TYPES) != FROZEN_PARENT_FILLS or set(VERIFIED_FILL_TYPES) != REQUIRED_FILLS
                or len(VERIFIED_FILL_TYPES) != len(REQUIRED_FILLS)
                or set(D.POSITION_TYPES) != REQUIRED_POSITIONS
                or set(D.ALGO_TYPES) != REQUIRED_ALGOS):
            raise D.EvidenceRefused("VERIFIED_ACCOUNT_SCOPE_UNPROVEN")
        if self._stage == "history":
            self._source_routes = self._read_routes()
            self._source_router_halt_evidence = copy.deepcopy(self._router_halt_evidence)
            search_start = max(0, end - ANCHOR_SEARCH_MS)
            if self._cache:
                cached_close = D._integer(self._cache["anchor"].get("uTime"))
                search_start = min(search_start, cached_close - DAY_MS)
            if search_start < 0 or end - search_start > MAX_PROVEN_LOOKBACK_MS:
                raise D.EvidenceRefused("ANCHOR_RETENTION_UNPROVEN")
            histories = self._position_scope(search_start, end)
            candidates = [row for row in histories if self._anchor_candidate(row, end)]
            if self._cache:
                candidates = [row for row in candidates if row == self._cache["anchor"]]
            if not candidates:
                raise D.EvidenceRefused("LEDGER_LOSS_ANCHOR_UNVERIFIED")
            self._anchor = max(candidates, key=lambda row: D._integer(row["uTime"]))
            closed = D._integer(self._anchor["uTime"])
            lookback = closed - DAY_MS
            if lookback < 0 or end - lookback > MAX_PROVEN_LOOKBACK_MS:
                raise D.EvidenceRefused("ANCHOR_RETENTION_UNPROVEN")
            if search_start > lookback:
                histories = self._position_scope(lookback, end)
            histories = [row for row in histories if D._integer(row["uTime"]) >= lookback]
            lower = closed + 1
            jobs = {"bills": lambda: self._history("/api/v5/account/bills-archive", lower, end)}
            for kind in VERIFIED_FILL_TYPES:
                jobs["fills:" + kind] = lambda kind=kind: self._fills_scope(lower, end, kind)
            batches = self._parallel(jobs)
            fills = [row for kind in VERIFIED_FILL_TYPES for row in batches["fills:"+kind]]
            bills = batches["bills"]
            self._source_ranges.append((lower, end))
            self._bill_coverage.append({"start_ms": lower, "end_ms": end, "currency_filter": None})
            self._fill_coverage.append({"start_ms": lower, "end_ms": end,
                                      "products": sorted(key.split(":", 1)[1] for key in batches
                                                         if key.startswith("fills:"))})
            self._lookback_start = lookback
        else:
            jobs = {"bills": lambda: self._history("/api/v5/account/bills-archive", start, end)}
            for kind in VERIFIED_FILL_TYPES:
                jobs["fills:"+kind] = lambda kind=kind: self._fills_scope(start, end, kind)
            for kind in D.POSITION_TYPES:
                jobs["positions:"+kind] = lambda kind=kind: self._positions_history(start, end, kind)
            batches = self._parallel(jobs)
            bills = batches["bills"]
            fills = [row for kind in VERIFIED_FILL_TYPES for row in batches["fills:"+kind]]
            histories = [row for kind in D.POSITION_TYPES for row in batches["positions:"+kind]]
            self._source_ranges.append((start, end))
            self._bill_coverage.append({"start_ms": start, "end_ms": end, "currency_filter": None})
            self._fill_coverage.append({"start_ms": start, "end_ms": end,
                                      "products": sorted(key.split(":", 1)[1] for key in batches
                                                         if key.startswith("fills:"))})
            self._position_coverage.append({"start_ms": start, "end_ms": end,
                                           "products": sorted(key.split(":", 1)[1] for key in batches
                                                              if key.startswith("positions:"))})
        self._order_details(fills)
        return bills, fills, histories

    def _flat_views(self):
        if len(self._views) != 2:
            return False
        for view in self._views:
            if set(view["algos"]) != REQUIRED_ALGOS:
                return False
            if view["pending"] or any(view["algos"][kind] for kind in D.ALGO_TYPES):
                return False
            if any(D._number(row.get("pos")) != 0 for row in view["positions"]):
                return False
        return True

    def _account_history(self, result, bills, fills, lifecycle, account_hash, equity, day, week, server):
        if not self._anchor or self._source_routes is None:
            raise D.EvidenceRefused("VERIFIED_EVIDENCE_MISSING")
        routes = self._read_routes()
        if (routes != self._source_routes or (self.diagnostic_halted_ledger
                and self._router_halt_evidence != self._source_router_halt_evidence)):
            raise D.EvidenceRefused("ROUTER_CHANGED_DURING_SNAPSHOT")
        lower = D._integer(self._anchor["uTime"]) + 1
        expected = lower
        for start, end in self._source_ranges:
            if start != expected or end < start:
                raise D.EvidenceRefused("VERIFIED_COVERAGE_GAP")
            expected = end + 1
        if expected != server + 1:
            raise D.EvidenceRefused("VERIFIED_COVERAGE_GAP")
        fill_windows = {(row["start_ms"], row["end_ms"]) for row in self._fill_coverage
                        if set(row["products"]) == REQUIRED_FILLS}
        bill_windows = {(row["start_ms"], row["end_ms"]) for row in self._bill_coverage
                        if row["currency_filter"] is None}
        first_end = self._source_ranges[0][1]
        position_prefix = any(row["start_ms"] <= self._lookback_start and row["end_ms"] == first_end
                              and set(row["products"]) == REQUIRED_POSITIONS
                              for row in self._position_coverage)
        position_tails = {(row["start_ms"], row["end_ms"]) for row in self._position_coverage
                          if set(row["products"]) == REQUIRED_POSITIONS}
        histories_complete = position_prefix and set(self._source_ranges[1:]) <= position_tails
        windows = set(self._source_ranges)
        fills_complete = windows == fill_windows
        bills_complete = windows == bill_windows
        if not (fills_complete and bills_complete and histories_complete):
            raise D.EvidenceRefused("VERIFIED_ACCOUNT_SCOPE_UNPROVEN")
        flat = self._flat_views()
        coverage = {"complete": fills_complete and bills_complete and histories_complete,
                    "all_account_products": fills_complete and histories_complete,
                    "all_account_currencies": bills_complete,
                    "fills_complete": fills_complete, "bills_complete": bills_complete,
                    "positions_history_complete": histories_complete,
                    "owned_routes_complete": routes == self._source_routes}
        coverage.update(environment="DEMO", account_hash=account_hash,
                        account_config=copy.deepcopy(self._views[-1]["config"]),
                        start_ms=lower, end_ms=server,
                        authorization_at_ms=self._auth["authorization_recorded_at_ms"],
                        anchor_lookback_complete=position_prefix,
                        anchor_lookback_start_ms=self._lookback_start,
                        account_flat_and_empty=flat,
                        loss_anchor={"source": "completed-position-reset", "environment": "DEMO",
                                     "account_hash": account_hash, "record": copy.deepcopy(self._anchor)})
        risk = self.config["risk"]
        ledger = validate_demo_ledger(fills=fills, bills=bills, positions_history=lifecycle,
                    routes=routes, order_details=self._details, now_ms=server,
                    day_start_ms=day, week_start_ms=week, coverage=coverage,
                    consecutive_loss_limit=risk["consecutive_loss_pause_count"],
                    pause_hours=risk["consecutive_loss_pause_hours"])
        self._ledger = ledger
        result["history_from_ms"] = lower
        result["anchor_lookback_start_ms"] = self._lookback_start
        result["ledger_refusals"] = list(ledger["refusals"])
        if ledger.get("verified") is not True:
            result["refusal_reasons"].extend(ledger["refusals"])
            return
        result.update(realized_pnl_today=str(ledger["realized_pnl_today"]),
                      realized_pnl_week=str(ledger["realized_pnl_week"]),
                      entries_today=ledger["entries_today"],
                      consecutive_losses=ledger["consecutive_losses"],
                      # The DEMO admission policy does not apply a loss-streak
                      # cooldown. Keep ledger evidence and shared risk policy intact.
                      consecutive_loss_cooldown_enabled=False,
                      paused_until_ms=0, paused=False)
        if not flat:
            raise D.EvidenceRefused("BASELINE_FLAT_ACCOUNT_REQUIRED")
        for position in ledger["completed_positions"]:
            if any(position["opened_at_ms"] < boundary <= position["closed_at_ms"]
                   for boundary in (day, week)):
                raise D.EvidenceRefused("BASELINE_BOUNDARY_NOT_FLAT")
        day_equity = equity - ledger["realized_pnl_today"]
        week_equity = equity - ledger["realized_pnl_week"]
        if min(day_equity, week_equity) <= 0:
            raise D.EvidenceRefused("BASELINE_EQUITY_INVALID")
        if self._cache:
            for period, boundary, derived in (("day", day, day_equity), ("week", week, week_equity)):
                if (self._cache.get(period+"_start_ms") == boundary
                        and D._number(self._cache.get(period+"_start_equity"), "VERIFIED_STATE_INVALID") != derived):
                    # Never silently rebase a period after an unexplained cash
                    # change, delayed bill, or damaged previous proof.
                    raise D.EvidenceRefused("BASELINE_RECONSTRUCTION_CHANGED")
        result.update(day_start_equity=str(day_equity), week_start_equity=str(week_equity),
                      baseline_trusted=True, baseline_source="complete-ledger-flat-boundary-reconstruction")
        result["daily_loss_hit"] = (ledger["realized_pnl_today"] <=
                                  -day_equity * D._number(risk["daily_loss_limit_pct"]) / 100)
        result["weekly_loss_hit"] = (ledger["realized_pnl_week"] <=
                                   -week_equity * D._number(risk["weekly_loss_limit_pct"]) / 100)
        if result["daily_loss_hit"]:
            result["refusal_reasons"].append("DAILY_LOSS_LIMIT")
        if result["weekly_loss_hit"]:
            result["refusal_reasons"].append("WEEKLY_LOSS_LIMIT")
        if ledger["entries_today"] >= int(risk["max_entries_per_day"]):
            result["refusal_reasons"].append("DAILY_ENTRY_LIMIT")
        directory = self.state_dir / account_hash
        with D._state_lock(directory):
            evidence = {"fills": fills, "bills": bills, "histories": lifecycle,
                        "routes": routes, "order_details": self._details}
            payload = {"schema": 2, "environment": "DEMO", "account_hash": account_hash,
                       "authorization_digest": _digest(self._auth), "anchor": self._anchor,
                       "complete_through_ms": server, "coverage_start_ms": lower,
                       "collection_started_ms": result["collection_started_ms"],
                       "anchor_lookback_start_ms": self._lookback_start,
                       "ledger_evidence_digest": _digest(evidence), "evidence": evidence,
                       "day_start_ms": day, "week_start_ms": week,
                       "day_start_equity": str(day_equity), "week_start_equity": str(week_equity),
                       "baseline_source": result["baseline_source"],
                       "account_observed_at_ms": result["observed_at_ms"],
                       "balance_observations": [timing for timing in self._request_timings
                                                if timing["path"] == "/api/v5/account/balance"],
                       "fill_product_scopes": sorted(REQUIRED_FILLS),
                       "position_product_scopes": sorted(REQUIRED_POSITIONS),
                       "algo_scopes": sorted(REQUIRED_ALGOS), "bills_currency_filter": None,
                       "source_coverage": {"fills": self._fill_coverage, "bills": self._bill_coverage,
                                           "positions": self._position_coverage},
                       "account_mode_applicability": self._compatibility_evidence,
                       "equity_usdt_observed": str(equity),
                       "account_mode": {"acctLv": self._views[-1]["config"].get("acctLv"),
                                        "posMode": self._views[-1]["config"].get("posMode")}}
            self._write(directory / "verified.json", payload)
