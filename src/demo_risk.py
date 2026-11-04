"""Read-only, fail-closed DEMO account diagnostics and boundary baselines.

This module never reads credentials and never sends writes. ``client.get`` must
be the dedicated, fixed-header DEMO client supplied by the application. A fresh
account balance is NOT a day/week baseline. Only this provider's own flat, empty
activity snapshots within 120 seconds of the corresponding UTC boundary can
create baselines. Only account level 2's isolated USDT futures wallet is supported;
idle positive cash in other currencies is excluded, and liabilities or activity
in any other instrument refuse admission.

Trade lifecycle attribution and consecutive *completed position* accounting are
not integrated yet: any fill or non-zero funding activity blocks admission with
LEDGER_UNVERIFIED. Diagnostic sums include fees/funding, and entry orders are
deduplicated, but those numbers must not be presented as a verified trade ledger.
The provider can therefore be run continuously while safely waiting for evidence.
Admission additionally stays locked by LEDGER_ADAPTER_UNVERIFIED until the full
lifecycle/consecutive-loss validator is independently reviewed and integrated;
an empty rolling window alone never proves the historical loss streak.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

INST = "SOL-USDT-SWAP"
BOUNDARY_WINDOW_MS = 120_000
MAX_AGE_MS = 60_000
MAX_ACCOUNT_AGE_MS = 5_000
MAX_SKEW_MS = 2_000
MAX_SNAPSHOT_WINDOW_MS = 10_000
PAUSE_LOOKBACK_MS = 24 * 60 * 60 * 1000
PAGE_LIMIT = 100
MAX_PAGES = 20
ALGO_TYPES = ("conditional", "oco", "trigger", "move_order_stop", "iceberg", "twap",
              "smart_iceberg", "chase")
FILL_TYPES = ("SWAP", "FUTURES", "OPTION", "SPOT", "MARGIN")
POSITION_TYPES = ("SWAP", "FUTURES", "OPTION", "MARGIN")


class EvidenceRefused(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _number(value, code="ACCOUNT_DATA_INVALID") -> Decimal:
    if value is None or isinstance(value, bool):
        raise EvidenceRefused(code)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise EvidenceRefused(code) from None
    if not result.is_finite():
        raise EvidenceRefused(code)
    return result


def _integer(value, code="ACCOUNT_DATA_INVALID") -> int:
    result = _number(value, code)
    if result != result.to_integral_value() or result < 0:
        raise EvidenceRefused(code)
    return int(result)


def _boundaries(now):
    dt = datetime.fromtimestamp(now / 1000, timezone.utc)
    day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    week = day - timedelta(days=day.weekday())
    return int(day.timestamp() * 1000), int(week.timestamp() * 1000)


@contextmanager
def _state_lock(directory):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / ".snapshot.lock"
    try:
        handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise EvidenceRefused("LOCAL_STATE_BUSY") from None
    try:
        os.close(handle)
        yield
    finally:
        path.unlink(missing_ok=True)


class DemoRiskProvider:
    """Return canonical execution state, with explicit evidence refusals.

    ``clock`` returns Unix milliseconds. State is namespaced by SHA256(DEMO+uid),
    not by API key or a shared production file. No arbitrary imported baseline is
    trusted: only the provider's schema, account binding and source are accepted.
    Local state is operational evidence, not tamper-proof attestation.
    """

    def __init__(self, client, config, state_dir, clock=None):
        self.client = client
        self.config = config
        self.state_dir = Path(state_dir)
        self.clock = clock or (lambda: int(time.time() * 1000))
        self._snapshot_lock = threading.Lock()
        self._timing_lock = threading.Lock()
        self._request_timings = []
        self._stage = ""

    def _get(self, path, params=None):
        start = _integer(self.clock(), "CLOCK_INVALID")
        stage = self._stage
        try:
            rows = self.client.get(path, params or {})
        finally:
            end = _integer(self.clock(), "CLOCK_INVALID")
            timing = {"path": path, "stage": stage, "started_at_ms": start,
                      "finished_at_ms": end,
                      "scope": {key: value for key, value in (params or {}).items()
                                if key in ("instType", "ordType", "begin", "end", "after")}}
            with self._timing_lock:
                self._request_timings.append(timing)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise EvidenceRefused("ACCOUNT_RESPONSE_INVALID")
        return rows

    def _one(self, path, params=None):
        rows = self._get(path, params)
        if len(rows) != 1:
            raise EvidenceRefused("ACCOUNT_RESPONSE_INVALID")
        return rows[0]

    @staticmethod
    def _parallel(jobs):
        """Only independent GET jobs. Workers never mutate account/risk state.

        On failure the context waits for in-flight read jobs to drain before
        raising; no timed-out previous wave may overlap the next snapshot.
        HTTP deadlines are owned by the injected fixed-DEMO client.
        """
        results = {}
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="demo-evidence") as pool:
            futures = {pool.submit(job): name for name, job in jobs.items()}
            try:
                for future in as_completed(futures):
                    results[futures[future]] = future.result()
            except Exception:
                for future in futures:
                    future.cancel()
                raise
        return results

    def _history(self, path, start, end, extra=None):
        """Fixed window, strict decreasing billId cursor, conflict detection."""
        seen = {}
        cursor = None
        for _ in range(MAX_PAGES):
            params = {"begin": str(start), "end": str(end), "limit": str(PAGE_LIMIT)}
            params.update(extra or {})
            if cursor is not None:
                params["after"] = str(cursor)
            rows = self._get(path, params)
            if len(rows) > PAGE_LIMIT:
                raise EvidenceRefused("HISTORY_PAGE_INVALID")
            ids = []
            for row in rows:
                bid = _integer(row.get("billId"), "HISTORY_ID_INVALID")
                if bid <= 0 or (cursor is not None and bid >= cursor):
                    raise EvidenceRefused("HISTORY_CURSOR_INVALID")
                ids.append(bid)
                if bid in seen and seen[bid] != row:
                    raise EvidenceRefused("HISTORY_DUPLICATE_CONFLICT")
                ts = _integer(row.get("fillTime") if "fills" in path else row.get("ts"),
                              "HISTORY_TIME_INVALID")
                if not start <= ts <= end:
                    raise EvidenceRefused("HISTORY_OUTSIDE_WINDOW")
                seen[bid] = row
            if any(left < right for left, right in zip(ids, ids[1:])):
                raise EvidenceRefused("HISTORY_CURSOR_INVALID")
            if len(rows) < PAGE_LIMIT:
                return list(seen.values())
            if len(set(ids)) != len(ids):
                raise EvidenceRefused("HISTORY_CURSOR_INVALID")
            next_cursor = min(ids)
            if cursor is not None and next_cursor >= cursor:
                raise EvidenceRefused("HISTORY_CURSOR_INVALID")
            cursor = next_cursor
        raise EvidenceRefused("HISTORY_INCOMPLETE")

    def _positions_history(self, start, end, kind):
        """Lifecycle coverage, ordered by uTime (posId is not a cursor/identity)."""
        seen = {}
        cursor = end + 1
        for _ in range(MAX_PAGES):
            rows = self._get("/api/v5/account/positions-history",
                             {"instType": kind, "limit": str(PAGE_LIMIT), "after": str(cursor)})
            if len(rows) > PAGE_LIMIT:
                raise EvidenceRefused("POSITION_HISTORY_INCOMPLETE")
            stamps = []
            for row in rows:
                stamp = _integer(row.get("uTime"), "POSITION_HISTORY_TIME_INVALID")
                created = _integer(row.get("cTime"), "POSITION_HISTORY_TIME_INVALID")
                if stamp >= cursor or created > stamp:
                    raise EvidenceRefused("POSITION_HISTORY_CURSOR_INVALID")
                identity = (row.get("instId"), row.get("posId"), stamp)
                if not all(isinstance(value, str) and value for value in identity[:2]):
                    raise EvidenceRefused("POSITION_HISTORY_ID_INVALID")
                if identity in seen and seen[identity] != row:
                    raise EvidenceRefused("POSITION_HISTORY_DUPLICATE_CONFLICT")
                stamps.append(stamp)
                seen[identity] = row
            if any(left < right for left, right in zip(stamps, stamps[1:])):
                raise EvidenceRefused("POSITION_HISTORY_CURSOR_INVALID")
            if len(rows) < PAGE_LIMIT or (stamps and min(stamps) < start):
                return [row for row in seen.values() if start <= int(row["uTime"]) <= end]
            if not stamps or min(stamps) >= cursor:
                raise EvidenceRefused("POSITION_HISTORY_CURSOR_INVALID")
            # Equal uTime across page boundaries cannot be exhausted with the
            # endpoint's exclusive timestamp cursor; don't silently omit ties.
            if stamps.count(min(stamps)) > 1:
                raise EvidenceRefused("POSITION_HISTORY_BOUNDARY_AMBIGUOUS")
            cursor = min(stamps)
        raise EvidenceRefused("POSITION_HISTORY_INCOMPLETE")

    def _history_evidence(self, start, end):
        jobs = {"bills": lambda: self._history("/api/v5/account/bills-archive", start, end)}
        for kind in FILL_TYPES:
            jobs["fills:" + kind] = lambda kind=kind: self._history(
                "/api/v5/trade/fills-history", start, end, {"instType": kind})
        for kind in POSITION_TYPES:
            jobs["positions:" + kind] = lambda kind=kind: self._positions_history(start, end, kind)
        result = self._parallel(jobs)
        fills = [row for kind in FILL_TYPES for row in result["fills:" + kind]]
        lifecycle = [row for kind in POSITION_TYPES for row in result["positions:" + kind]]
        return result["bills"], fills, lifecycle

    def _account_view(self):
        jobs = {"config": lambda: self._one("/api/v5/account/config"),
                "balance": lambda: self._one("/api/v5/account/balance"),
                "positions": lambda: self._get("/api/v5/account/positions"),
                "pending": lambda: self._get("/api/v5/trade/orders-pending", {"limit": str(PAGE_LIMIT)}),
                "leverage": lambda: self._one("/api/v5/account/leverage-info",
                                               {"instId": INST, "mgnMode": "cross"}),
                "spec": lambda: self._one("/api/v5/public/instruments",
                                           {"instType": "SWAP", "instId": INST})}
        for kind in ALGO_TYPES:
            jobs["algo:" + kind] = lambda kind=kind: self._get(
                "/api/v5/trade/orders-algo-pending", {"ordType": kind, "limit": str(PAGE_LIMIT)})
        offset = len(self._request_timings)
        rows = self._parallel(jobs)
        rows["algos"] = {kind: rows.pop("algo:" + kind) for kind in ALGO_TYPES}
        timings = self._request_timings[offset:]
        return rows, min(row["started_at_ms"] for row in timings), max(row["finished_at_ms"] for row in timings)

    @staticmethod
    def _account_fingerprint(view):
        """Ignore balance refresh clocks and idle assets' changing USD prices."""
        balance = view["balance"]
        details = balance.get("details")
        if not isinstance(details, list) or any(not isinstance(row, dict) for row in details):
            raise EvidenceRefused("ACCOUNT_DATA_INVALID")
        fields = ("ccy", "eq", "liab", "crossLiab", "isoLiab", "interest", "borrowFroz")
        wallets = sorted(({key: row.get(key) for key in fields} for row in details),
                         key=lambda row: str(row["ccy"]))
        stable = {"config": {key: view["config"].get(key) for key in ("uid", "posMode", "acctLv")},
                  "wallets": wallets, "positions": view["positions"], "pending": view["pending"],
                  "algos": view["algos"], "leverage": view["leverage"],
                  "spec": {key: view["spec"].get(key) for key in
                           ("instId", "state", "ctType", "settleCcy", "ctValCcy",
                            "ctVal", "lotSz", "minSz", "tickSz")}}
        return json.dumps(stable, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def _load(self, directory, account_hash):
        path = directory / "risk.json"
        if not path.exists():
            return {"schema": 1, "environment": "DEMO", "account_hash": account_hash,
                    "baselines": {}, "funding_flow_latched": False,
                    "ledger_unverified_latched": False, "observed_bill_hashes": {}}
        try:
            if path.stat().st_size > 2_000_000:
                raise ValueError()
            data = json.loads(path.read_text(encoding="utf-8"))
            if (not isinstance(data, dict) or data.get("schema") != 1
                    or data.get("environment") != "DEMO"
                    or data.get("account_hash") != account_hash
                    or not isinstance(data.get("baselines"), dict)
                    or not isinstance(data.get("observed_bill_hashes"), dict)
                    or not isinstance(data.get("funding_flow_latched"), bool)
                    or not isinstance(data.get("ledger_unverified_latched"), bool)):
                raise ValueError()
            return data
        except (ValueError, OSError, TypeError):
            raise EvidenceRefused("LOCAL_STATE_INVALID") from None

    def _save(self, directory, data):
        path = directory / "risk.json"
        temp = directory / ("risk-" + uuid.uuid4().hex + ".tmp")
        try:
            with temp.open("x", encoding="utf-8") as handle:
                json.dump(data, handle, sort_keys=True, allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)

    def snapshot(self, market_snapshot=None):
        with self._snapshot_lock:
            self._request_timings = []
            self._stage = "collection-start"
            return self._snapshot(market_snapshot)

    def _snapshot(self, market_snapshot=None):
        now = _integer(self.clock(), "CLOCK_INVALID")
        day, week = _boundaries(now)
        result = {"environment": "DEMO", "now_ms": now, "observed_at_ms": None,
                  "equity_estimate": None, "day_start_equity": None,
                  "week_start_equity": None, "realized_pnl_today": None,
                  "realized_pnl_week": None, "entries_today": 0,
                  "open_positions": [], "pending_orders": [], "protection_orders": [],
                  "data_fresh": False, "macro_blackout": True, "halted": True,
                  "daily_loss_hit": False, "weekly_loss_hit": False,
                  "paused": False, "paused_until_ms": 0, "demo_verified": False,
                  "live_verified": False, "refusal_reasons": [],
                  "day_start_ms": day, "week_start_ms": week,
                  "ledger_verified": False, "baseline_trusted": False,
                  "pause_evidence_verified": False, "history_complete": False,
                  "account_snapshot_consistent": False, "activity_free_coverage": False}
        reasons = result["refusal_reasons"]
        market = market_snapshot if isinstance(market_snapshot, Mapping) else {}
        if (market.get("data_fresh") is not True
                or not isinstance(market.get("macro_blackout"), bool)):
            reasons.append("MARKET_EVIDENCE_MISSING")
        else:
            try:
                age = now - _integer(market.get("observed_at_ms"), "MARKET_TIME_INVALID")
                if not 0 <= age <= MAX_AGE_MS:
                    raise EvidenceRefused("MARKET_STALE")
                result["data_fresh"] = True
                result["macro_blackout"] = market["macro_blackout"]
                if market["macro_blackout"]:
                    reasons.append("MACRO_BLACKOUT")
            except EvidenceRefused as exc:
                reasons.append(exc.code)
        try:
            self._collect(result, day, week)
        except EvidenceRefused as exc:
            reasons.append(exc.code)
        except Exception:
            # Never forward client exception text, URLs, credentials or raw data.
            reasons.append("ACCOUNT_READ_FAILED")
        if result["day_start_equity"] is None or result["week_start_equity"] is None:
            reasons.append("BASELINE_MISSING")
        finished = _integer(self.clock(), "CLOCK_INVALID")
        if finished < now or finished - now > MAX_AGE_MS:
            reasons.append("ACCOUNT_STALE")
        observed = result.get("observed_at_ms")
        if observed is not None and (finished < observed or finished - observed > MAX_ACCOUNT_AGE_MS):
            reasons.append("ACCOUNT_STALE")
        # Diagnostic account/history coverage is not a known lifecycle anchor.
        # Keep this hard lock until the separately reviewed ledger is integrated.
        reasons.append("LEDGER_ADAPTER_UNVERIFIED")
        result["ledger_verified"] = False
        result["pause_evidence_verified"] = False
        result["refusal_reasons"] = list(dict.fromkeys(reasons))
        result["demo_verified"] = not result["refusal_reasons"]
        result["halted"] = not result["demo_verified"]
        result["collection_completed_ms"] = finished
        result["request_timings"] = sorted(self._request_timings,
                                           key=lambda row: (row["started_at_ms"], row["path"]))
        return result

    def _collect(self, result, day, week):
        now, reasons = result["now_ms"], result["refusal_reasons"]
        server = _integer(self._one("/api/v5/public/time").get("ts"), "SERVER_TIME_INVALID")
        result["observed_at_ms"] = server
        result["collection_started_ms"] = server
        result["snapshot_window_start_ms"] = server
        if abs(server - _integer(self.clock(), "CLOCK_INVALID")) > MAX_SKEW_MS:
            raise EvidenceRefused("CLOCK_SKEW")
        if _boundaries(server) != (day, week):
            raise EvidenceRefused("BOUNDARY_CHANGED")
        self._stage = "account-before"
        before, before_started, before_finished = self._account_view()
        result["account_before_started_ms"] = before_started
        result["account_before_finished_ms"] = before_finished
        before_fingerprint = self._account_fingerprint(before)
        cfg = before["config"]
        uid = cfg.get("uid")
        if not isinstance(uid, str) or not uid.isascii() or not uid.isdigit():
            raise EvidenceRefused("ACCOUNT_ID_UNVERIFIED")
        account_hash = hashlib.sha256(("DEMO\0" + uid).encode()).hexdigest()
        result["account_hash"] = account_hash
        if cfg.get("posMode") != "net_mode":
            reasons.append("POSITION_MODE_MISMATCH")
        if str(cfg.get("acctLv")) != "2":
            reasons.append("ACCOUNT_MODE_UNSUPPORTED")
        balance = before["balance"]
        details = balance.get("details")
        if not isinstance(details, list) or any(not isinstance(d, dict) for d in details):
            raise EvidenceRefused("ACCOUNT_DATA_INVALID")
        usdt = [d for d in details if d.get("ccy") == "USDT"]
        if len(usdt) != 1:
            raise EvidenceRefused("USDT_EQUITY_MISSING")
        equity = _number(usdt[0].get("eq"))
        total = _number(balance.get("totalEq"))
        if equity <= 0 or total <= 0 or equity > Decimal("1000000000000"):
            raise EvidenceRefused("EQUITY_INVALID")
        result["equity_estimate"] = float(equity)
        result["equity_scope"] = "DEMO-USDT-FUTURES-WALLET"
        result["account_total_equity_diagnostic"] = str(total)
        result["multi_asset_collateral_supported"] = False
        for detail in details:
            if not isinstance(detail.get("ccy"), str) or _number(detail.get("eq")) < 0:
                reasons.append("ACCOUNT_ASSET_UNSUPPORTED")
            for field in ("liab", "crossLiab", "isoLiab", "interest", "borrowFroz"):
                value = detail.get(field) if field == "liab" else detail.get(field, "0")
                if _number(value) != 0:
                    reasons.append("ACCOUNT_LIABILITY_UNSUPPORTED")
        stamp = _integer(balance.get("uTime"), "BALANCE_TIME_INVALID")
        if stamp > _integer(self.clock(), "CLOCK_INVALID") + MAX_SKEW_MS:
            reasons.append("BALANCE_TIME_FUTURE")
        for position in before["positions"]:
            qty = _number(position.get("pos"))
            if not isinstance(position.get("instId"), str):
                raise EvidenceRefused("POSITION_DATA_INVALID")
            if qty != 0:
                safe = {key: position.get(key) for key in
                        ("instId", "pos", "posSide", "mgnMode", "lever", "avgPx")}
                result["open_positions"].append(safe)
                reasons.append("OPEN_POSITION_PRESENT" if position["instId"] == INST
                               else "UNSUPPORTED_ACCOUNT_POSITION")
        pending = before["pending"]
        if len(pending) >= PAGE_LIMIT:
            reasons.append("PENDING_PAGINATION_UNPROVEN")
        for order in pending:
            result["pending_orders"].append({key: order.get(key) for key in
                                              ("instId", "ordId", "clOrdId", "state")})
        if pending:
            reasons.append("UNKNOWN_OPEN_ORDER")
        for kind in ALGO_TYPES:
            algos = before["algos"][kind]
            if len(algos) >= PAGE_LIMIT:
                reasons.append("PENDING_PAGINATION_UNPROVEN")
            for order in algos:
                result["protection_orders"].append({key: order.get(key) for key in
                                                    ("instId", "algoId", "ordType", "state")})
            if algos:
                reasons.append("UNKNOWN_ALGO_ORDER")
        lever = before["leverage"]
        if (lever.get("instId") != INST or lever.get("mgnMode") != "cross"
                or lever.get("posSide") != "net" or _number(lever.get("lever")) != 8
                or _number(self.config["account"]["leverage_setting"]) != 8
                or self.config["account"].get("margin_mode") != "cross"
                or self.config["account"].get("position_mode") != "net"):
            reasons.append("LEVERAGE_MARGIN_MISMATCH")
        spec = before["spec"]
        matches = [s for s in self.config["symbols"] if s.get("instId") == INST]
        if len(matches) != 1 or spec.get("instId") != INST or spec.get("state") != "live":
            raise EvidenceRefused("INSTRUMENT_SPEC_MISMATCH")
        if (spec.get("ctType") != "linear" or spec.get("settleCcy") != "USDT"
                or spec.get("ctValCcy") != "SOL"):
            reasons.append("INSTRUMENT_SPEC_MISMATCH")
        for field in ("ctVal", "lotSz", "minSz", "tickSz"):
            if _number(spec.get(field)) != _number(matches[0].get(field)):
                reasons.append("INSTRUMENT_SPEC_MISMATCH")
        # A UTC Monday reset cannot erase a pause triggered on Sunday. Lifecycle
        # evidence independently catches a close whose fills/bills are absent.
        lookback = min(week, server - PAUSE_LOOKBACK_MS)
        result["history_from_ms"] = lookback
        self._stage = "history"
        bills, fills, lifecycle = self._history_evidence(lookback, server)
        self._stage = "account-after"
        after, after_started, after_finished = self._account_view()
        result["account_after_started_ms"] = after_started
        result["account_after_finished_ms"] = after_finished
        self._stage = "collection-end"
        end_server = _integer(self._one("/api/v5/public/time").get("ts"), "SERVER_TIME_INVALID")
        result["snapshot_window_end_ms"] = end_server
        if abs(end_server - _integer(self.clock(), "CLOCK_INVALID")) > MAX_SKEW_MS:
            raise EvidenceRefused("CLOCK_SKEW")
        if _boundaries(end_server) != (day, week):
            raise EvidenceRefused("BOUNDARY_CHANGED")
        if end_server < server or end_server - server > MAX_SNAPSHOT_WINDOW_MS:
            raise EvidenceRefused("ACCOUNT_SNAPSHOT_UNSTABLE")
        # The second balance/positions/order read happens after the first history
        # end. Reject any change, and independently cover the intervening window.
        if self._account_fingerprint(after) != before_fingerprint:
            raise EvidenceRefused("ACCOUNT_CHANGED_DURING_SNAPSHOT")
        # These are new GET observations, not a timestamp applied to old rows.
        # Admission freshness starts at the earliest actual post-view GET start.
        result["observed_at_ms"] = after_started
        second_stamp = _integer(after["balance"].get("uTime"), "BALANCE_TIME_INVALID")
        if second_stamp > end_server + MAX_SKEW_MS:
            raise EvidenceRefused("BALANCE_TIME_FUTURE")
        if end_server > server:
            self._stage = "history-gap"
            gap_bills, gap_fills, gap_lifecycle = self._history_evidence(server + 1, end_server)
            bills.extend(gap_bills)
            fills.extend(gap_fills)
            lifecycle.extend(gap_lifecycle)
        result["account_snapshot_consistent"] = True
        result["history_through_ms"] = end_server
        result["history_complete"] = True
        self._account_history(result, bills, fills, lifecycle, account_hash, equity,
                              day, week, end_server)

    def _account_history(self, result, bills, fills, lifecycle, account_hash, equity, day, week, server):
        reasons = result["refusal_reasons"]
        pnl_day = pnl_week = Decimal(0)
        entries = set()
        fill_keys = {}
        activity_times = []
        nontrading_flow = False
        for fill in fills:
            key = (fill.get("instId"), fill.get("tradeId"))
            if (not all(isinstance(k, str) and k for k in key)
                    or not isinstance(fill.get("ordId"), str) or not fill["ordId"]):
                raise EvidenceRefused("FILL_ID_INVALID")
            if key in fill_keys:
                if fill_keys[key] != fill:
                    raise EvidenceRefused("HISTORY_DUPLICATE_CONFLICT")
                continue
            fill_keys[key] = fill
            stamp = _integer(fill.get("fillTime"), "HISTORY_TIME_INVALID")
            activity_times.append(stamp)
            if fill.get("instId") != INST or fill.get("feeCcy") != "USDT":
                reasons.append("UNSUPPORTED_ACCOUNT_ACTIVITY")
                continue
            pnl = _number(fill.get("fillPnl"), "FILL_PNL_INVALID")
            fee = _number(fill.get("fee"), "FILL_FEE_INVALID")
            net = pnl + fee
            if stamp >= week:
                pnl_week += net
            if stamp >= day:
                pnl_day += net
                # Explicitly diagnostic: a reduction fill with zero realized PnL
                # cannot be classified as entry without owned lifecycle evidence.
                entries.add(fill["ordId"])
        for bill in bills:
            stamp = _integer(bill.get("ts"), "HISTORY_TIME_INVALID")
            amount = _number(bill.get("balChg"), "BILL_AMOUNT_INVALID")
            kind, ccy = bill.get("type"), bill.get("ccy")
            if not isinstance(kind, str) or not isinstance(ccy, str) or not ccy:
                raise EvidenceRefused("BILL_DATA_INVALID")
            activity_times.append(stamp)
            if kind == "8":
                if ccy != "USDT" or bill.get("instId") != INST:
                    reasons.append("UNSUPPORTED_ACCOUNT_ACTIVITY")
                else:
                    if stamp >= week:
                        pnl_week += amount
                    if stamp >= day:
                        pnl_day += amount
            elif kind == "2":
                # Trade bills and fills overlap; never double charge. Matching is
                # not yet integrated, so even unmatched trade bills block.
                reasons.append("LEDGER_UNVERIFIED")
            elif amount != 0 or kind not in ("1", "8", "2"):
                nontrading_flow = True
        if fills or bills or lifecycle:
            reasons.append("LEDGER_UNVERIFIED")
        if lifecycle:
            reasons.append("TRADE_LIFECYCLE_UNVERIFIED")
            activity_times.extend(_integer(row["uTime"]) for row in lifecycle)
        result["realized_pnl_today"] = float(pnl_day)
        result["realized_pnl_week"] = float(pnl_week)
        result["entries_today"] = len(entries)
        directory = self.state_dir / account_hash
        with _state_lock(directory):
            local = self._load(directory, account_hash)
            for bill in bills:
                key = str(bill["billId"])
                digest = hashlib.sha256(json.dumps(bill, sort_keys=True, separators=(",", ":"))
                                        .encode()).hexdigest()
                old = local["observed_bill_hashes"].get(key)
                if old is not None and old != digest:
                    raise EvidenceRefused("PERSISTED_BILL_CONFLICT")
                local["observed_bill_hashes"][key] = digest
            if nontrading_flow:
                local["funding_flow_latched"] = True
            if bills or fills or lifecycle:
                local["ledger_unverified_latched"] = True
            if local["funding_flow_latched"]:
                reasons.append("ACCOUNT_FUNDING_FLOW_DETECTED")
            if local["ledger_unverified_latched"]:
                reasons.append("LEDGER_UNVERIFIED")
            # An unknown market/macro state blocks trading, but does not turn a
            # fully observed flat account balance into an invalid cash baseline.
            account_reasons = [reason for reason in reasons
                               if not reason.startswith("MARKET_") and reason != "MACRO_BLACKOUT"]
            capture_safe = not account_reasons and not activity_times
            for period, boundary in (("day", day), ("week", week)):
                baseline = local["baselines"].get(period)
                if baseline is not None:
                    if not isinstance(baseline, dict):
                        raise EvidenceRefused("LOCAL_STATE_INVALID")
                    if baseline.get("boundary_ms") != boundary:
                        baseline = None
                if baseline is None and (capture_safe
                                         and 0 <= server - boundary <= BOUNDARY_WINDOW_MS):
                    baseline = {"boundary_ms": boundary, "observed_at_ms": server,
                                "equity": str(equity), "source": "demo-flat-empty-boundary"}
                    local["baselines"][period] = baseline
                if baseline is None:
                    reasons.append("DAY_BASELINE_MISSING" if period == "day"
                                   else "WEEK_BASELINE_MISSING")
                    continue
                stamp = _integer(baseline.get("observed_at_ms"), "BASELINE_INVALID")
                value = _number(baseline.get("equity"), "BASELINE_INVALID")
                if (baseline.get("source") != "demo-flat-empty-boundary"
                        or not boundary <= stamp <= boundary + BOUNDARY_WINDOW_MS
                        or stamp > server or value <= 0):
                    raise EvidenceRefused("BASELINE_INVALID")
                result["day_start_equity" if period == "day" else "week_start_equity"] = float(value)
                if not bills and not fills and not lifecycle and value != equity:
                    reasons.append("UNEXPLAINED_EQUITY_CHANGE")
            local["last_observed_at_ms"] = server
            self._save(directory, local)
        if result["day_start_equity"] is None or result["week_start_equity"] is None:
            reasons.append("BASELINE_MISSING")
        else:
            result["baseline_trusted"] = True
            risk = self.config["risk"]
            day_limit = _number(risk["daily_loss_limit_pct"]) / 100
            week_limit = _number(risk["weekly_loss_limit_pct"]) / 100
            result["daily_loss_hit"] = pnl_day <= -Decimal(str(result["day_start_equity"])) * day_limit
            result["weekly_loss_hit"] = pnl_week <= -Decimal(str(result["week_start_equity"])) * week_limit
            if result["daily_loss_hit"]:
                reasons.append("DAILY_LOSS_LIMIT")
            if result["weekly_loss_hit"]:
                reasons.append("WEEKLY_LOSS_LIMIT")
            if len(entries) >= int(risk["max_entries_per_day"]):
                reasons.append("DAILY_ENTRY_LIMIT")
        result["activity_free_coverage"] = not local["ledger_unverified_latched"]
