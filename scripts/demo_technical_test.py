#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""DEMO 技术测试入口（DSH 独占实现；默认只解释、零凭据零请求）。

执行必须同时给出 `--run-test --enable-demo-test --scenario smoke|swing`，且只在
独立 DEMO 固定归档中使用；本脚本不包含任何实盘路径。

新版生产入口仅消费已经受控激活的后继 generation；不复制旧状态、不回退
到 fresh/legacy 运行。旧迁移辅助方法仅保留给历史离线验证。

安全要点：
- 默认模式不导入凭据/客户端、不发起任何网络请求。
- 串行：同时持有旧 runtime 与新 runtime 的 process.lock；检查旧 supervisor 已安全退出。
- 保留前任 account-binding / auto-authorization / router+controller journals /
  verified-risk-v2 / scan-seen；不迁移、不清除 STOP/HALT。
- 实际公开采集（calendar_max_age_s=1500，保留真实 source_ts），使用新鲜 bid/ask/last。
- 技术计划精确四键，smoke/swing 使用最小 0.01 张、maker、原风控计算；两个场景都进入
  同一 router journal 与账户风险账本。
- 成交后验证 OCO；观察后走 close_demo_test→planned_close；核对完整空仓与账本后才算完成。
- 报告脱敏，不输出凭据/HTTP 头/账户身份；本地原始证据留在独立 run 目录。
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import deque
from collections.abc import Mapping
from copy import deepcopy
import uuid
from decimal import Context, Decimal, Inexact, InvalidOperation, ROUND_HALF_UP, localcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CODE_ROOT = Path(__file__).resolve().parents[1]
# Isolated script launches omit the deployment root from Python's import path.
sys.path.insert(0, str(CODE_ROOT))


def runtime_demo_get():
    # Keep the explanation-only entry free of transport/credential imports.
    from src.demo_runtime_get import runtime_demo_get as session
    return session()


PURPOSE = "DEMO_TECHNICAL_TEST"
AUTHORIZATION_ID = "user-approved-demo-technical-tests-20261019"
EXPECTED_PREDECESSOR = "8381b547b8215ca7d72f80f796ef855ce8698957"
SCENARIOS = {
    "smoke": {"kind": "scalp_5m", "prefix": "DEMO-TEST-SMOKE-",
              "stop_pct": Decimal("0.009"), "tp1_r": Decimal("2.5"),
              "tp2_r": Decimal("3.0"), "observe_s": 30,
              "stop_range": (Decimal("0.0030"), Decimal("0.0120"))},
    "swing": {"kind": "swing_1h4h", "prefix": "DEMO-TEST-SWING-",
              "stop_pct": Decimal("0.012"), "tp1_r": Decimal("2.6"),
              "tp2_r": Decimal("3.2"), "observe_s": 120,
              "stop_range": (Decimal("0.0080"), Decimal("0.0350"))},
}
ENTRY_WAIT_S = 180
POLL_S = 2.0
CLOSE_WAIT_S = 120
LEDGER_WAIT_S = 120
TERMINAL_ENTRY = {"CANCELED", "CLOSED", "REJECTED", "HALTED", "UNKNOWN"}
WAITING = {"RESERVED", "SUBMITTING", "PENDING", "PROTECTING"}
PRESERVED_STATE = ("account-binding.json", "auto-authorization.json",
                   "controller.sqlite3", "router.sqlite3", "scan-seen.json",
                   "verified-risk-v2")
LOCAL_TERMINAL = {"REJECTED", "CANCELED", "CLOSED"}
DIAGNOSTIC_PATHS = frozenset({
    "/api/v5/account/config", "/api/v5/account/balance", "/api/v5/account/positions",
    "/api/v5/account/leverage-info", "/api/v5/trade/orders-pending",
    "/api/v5/trade/orders-algo-pending", "/api/v5/account/bills-archive",
    "/api/v5/trade/fills-history", "/api/v5/account/positions-history",
    "/api/v5/trade/order", "/api/v5/public/time", "/api/v5/public/instruments",
})
# Frozen diagnostic vocabulary: unknown values are replaced, never echoed.
DIAGNOSTIC_CODES = frozenset("""
ACCOUNT_ASSET_UNSUPPORTED ACCOUNT_CHANGED_DURING_SNAPSHOT ACCOUNT_DATA_INVALID
ACCOUNT_FUNDING_FLOW_DETECTED ACCOUNT_ID_UNVERIFIED ACCOUNT_LIABILITY_UNSUPPORTED
ACCOUNT_MODE_UNSUPPORTED ACCOUNT_READ_FAILED ACCOUNT_RESPONSE_INVALID
ACCOUNT_SNAPSHOT_UNSTABLE ACCOUNT_STALE BALANCE_TIME_FUTURE BALANCE_TIME_INVALID
BASELINE_INVALID BASELINE_MISSING BILL_AMOUNT_INVALID BILL_DATA_INVALID
BOUNDARY_CHANGED CLOCK_INVALID CLOCK_SKEW DAILY_ENTRY_LIMIT DAILY_LOSS_LIMIT
DAY_BASELINE_MISSING EQUITY_INVALID FILL_FEE_INVALID FILL_ID_INVALID FILL_PNL_INVALID
HISTORY_CURSOR_INVALID HISTORY_DUPLICATE_CONFLICT HISTORY_ID_INVALID HISTORY_INCOMPLETE
HISTORY_OUTSIDE_WINDOW HISTORY_PAGE_INVALID HISTORY_TIME_INVALID INSTRUMENT_SPEC_MISMATCH
LEDGER_ADAPTER_UNVERIFIED LEDGER_UNVERIFIED LEVERAGE_MARGIN_MISMATCH LOCAL_STATE_BUSY
LOCAL_STATE_INVALID MACRO_BLACKOUT MARKET_EVIDENCE_MISSING MARKET_STALE MARKET_TIME_INVALID
OPEN_POSITION_PRESENT PENDING_PAGINATION_UNPROVEN PERSISTED_BILL_CONFLICT POSITION_DATA_INVALID
POSITION_HISTORY_BOUNDARY_AMBIGUOUS POSITION_HISTORY_CURSOR_INVALID
POSITION_HISTORY_DUPLICATE_CONFLICT POSITION_HISTORY_ID_INVALID POSITION_HISTORY_INCOMPLETE
POSITION_HISTORY_TIME_INVALID POSITION_MODE_MISMATCH SERVER_TIME_INVALID TRADE_LIFECYCLE_UNVERIFIED
UNEXPLAINED_EQUITY_CHANGE UNKNOWN_ALGO_ORDER UNKNOWN_OPEN_ORDER UNSUPPORTED_ACCOUNT_ACTIVITY
UNSUPPORTED_ACCOUNT_POSITION USDT_EQUITY_MISSING WEEK_BASELINE_MISSING WEEKLY_LOSS_LIMIT
ANCHOR_RETENTION_UNPROVEN BASELINE_BOUNDARY_NOT_FLAT BASELINE_EQUITY_INVALID
BASELINE_FLAT_ACCOUNT_REQUIRED BASELINE_RECONSTRUCTION_CHANGED CONSECUTIVE_LOSS_PAUSE
DEMO_API_ERROR DEMO_AUTHORIZATION_ACCOUNT_MISMATCH DEMO_AUTHORIZATION_CHANGED
DEMO_AUTHORIZATION_INVALID DEMO_AUTHORIZATION_MISSING DEMO_BAD_JSON DEMO_BAD_RESPONSE
DEMO_BASE_LOCKED DEMO_HTTP_ERROR DEMO_NETWORK_ERROR DEMO_PATH_NOT_ALLOWED
DEMO_NETWORK_RETRY_BUDGET_FAILED
DEMO_REDIRECT_FORBIDDEN DEMO_RESPONSE_TOO_LARGE DEMO_SOURCE_RATE_LIMITED DEMO_SOURCE_REQUEST_FAILED
FUTURES_APPLICABILITY_FIELD_INVALID FUTURES_APPLICABILITY_FIELD_MISSING
POSITION_HISTORY_BOUNDARY_UNPROVEN ROUTE_PRECEDES_ACTIVATION ROUTER_CHANGED_DURING_SNAPSHOT
ROUTER_HALTED ROUTER_JOURNAL_INVALID ROUTER_JOURNAL_MISSING ROUTER_JOURNAL_TOO_LARGE
VERIFIED_ACCOUNT_SCOPE_UNPROVEN VERIFIED_COVERAGE_GAP VERIFIED_EVIDENCE_MISSING
VERIFIED_FILL_SCOPE_MISMATCH VERIFIED_HISTORY_ID_INVALID VERIFIED_POSITION_SCOPE_MISMATCH
VERIFIED_STATE_INVALID LEDGER_ACCOUNT_BINDING LEDGER_ACCOUNT_SCOPE LEDGER_ANCHOR_COOLDOWN_UNPROVEN
LEDGER_BILL_DUPLICATE_CONFLICT LEDGER_BILL_OUTSIDE_COVERAGE LEDGER_BILL_SCOPE
LEDGER_BOUNDARIES_INVALID LEDGER_COVERAGE_WINDOW LEDGER_ENVIRONMENT LEDGER_EVIDENCE_INVALID
LEDGER_EXTERNAL_CASH_FLOW LEDGER_EXTERNAL_POSITION LEDGER_FILL_DUPLICATE_CONFLICT
LEDGER_FILL_OUTSIDE_COVERAGE LEDGER_FILL_OWNERSHIP_UNVERIFIED LEDGER_FILL_ROLE_MISMATCH
LEDGER_FILL_SCOPE LEDGER_FUNDING_LINK_UNVERIFIED LEDGER_HISTORY_INCOMPLETE LEDGER_ID_INVALID
LEDGER_LIFECYCLE_OVERLAP LEDGER_LIFECYCLE_TIME_MISMATCH LEDGER_LOSS_ANCHOR_RECORD_MISSING
LEDGER_LOSS_ANCHOR_UNVERIFIED LEDGER_NUMBER_INVALID LEDGER_ORDER_DETAIL_INVALID
LEDGER_ORDER_DETAIL_SIZE_MISMATCH LEDGER_ORDER_DETAIL_UNMATCHED LEDGER_ORDER_DETAIL_UNVERIFIED
LEDGER_OWNERSHIP_AMBIGUOUS LEDGER_PARTIAL_POSITION_HISTORY LEDGER_POLICY_INVALID
LEDGER_POSITION_DUPLICATE_CONFLICT LEDGER_POSITION_HISTORY_AMBIGUOUS LEDGER_POSITION_HISTORY_MISSING
LEDGER_POSITION_LINK_UNVERIFIED LEDGER_POSITION_OUTSIDE_COVERAGE LEDGER_POSITION_PNL_MISMATCH
LEDGER_POSITION_SCOPE LEDGER_ROUTE_DUPLICATE LEDGER_ROUTE_INVALID LEDGER_ROWS_INVALID
LEDGER_TIME_INVALID LEDGER_TRADE_BILL_MISMATCH LEDGER_TRADE_BILL_MISSING LEDGER_TRADE_BILL_UNMATCHED
LEDGER_UNRESOLVED_OWNED_ROUTE LEDGER_UNSUPPORTED_POSITION_ADJUSTMENT
""".split())
DIAGNOSTIC_NETWORK_CATEGORIES = frozenset({
    "DNS", "TLS_CERTIFICATE", "TLS_EOF", "TLS", "CONNECT_TIMEOUT", "READ_TIMEOUT",
    "POOL_WAIT_TIMEOUT", "CONNECTION", "PROXY", "PROTOCOL", "TIMEOUT", "UNKNOWN",
})


class TestRefused(Exception):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class ObservedRiskProvider:
    """Observe the existing provider without substituting evidence or results."""

    def __init__(self, provider, observer):
        self.provider = provider
        self.observer = observer

    def snapshot(self, market_snapshot=None):
        started = time.time_ns() // 1_000_000
        try:
            result = self.provider.snapshot(market_snapshot)
        except Exception:
            self._notify(None, started)
            raise
        self._notify(result, started)
        return result

    def _notify(self, result, started):
        try:
            self.observer(result, started, time.time_ns() // 1_000_000)
        except Exception:
            # Diagnostics must never replace the provider's result/exception.
            pass


class ObservedQuoteProvider:
    """Observe the existing GET result without another GET or altered outcome."""

    def __init__(self, provider, observer):
        self.provider, self.observer = provider, observer

    def __call__(self):
        started = self._time_ms()
        try:
            result = self.provider()
        except Exception:
            self._notify(None, started)
            raise
        self._notify(result, started)
        return result

    def _notify(self, result, started):
        completed = self._time_ms()
        try:
            self.observer(self._quote_snapshot(result), started, completed)
        except Exception:
            # Diagnostic failure must not replace a quote or its exception.
            pass

    @staticmethod
    def _time_ms():
        try:
            return time.time_ns() // 1_000_000
        except Exception:
            return None

    @staticmethod
    def _quote_snapshot(quote):
        # The validated service returns a plain dict. Never hand an observer the
        # live return object or invoke arbitrary Mapping/value implementations.
        if type(quote) is not dict:
            return None
        return {**{key: ProductionDeps._diagnostic_price(quote.get(key))
                   for key in ("bid", "ask", "last")},
                "timestamp_ms": ProductionDeps._diagnostic_time(quote.get("timestamp_ms"))}


# --------------------------------------------------------------------------- 纯函数


def _dec(value) -> Decimal:
    try:
        d = Decimal(str(value))
    except Exception:  # noqa: BLE001
        raise TestRefused("DECIMAL_INVALID") from None
    if not d.is_finite() or d <= 0:
        raise TestRefused("DECIMAL_INVALID")
    return d


def _tick_round(price: Decimal, tick: Decimal) -> Decimal:
    steps = (price / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return (steps * tick).quantize(tick)


def build_signal(scenario: str, side: str, bid, ask, tick_size, min_sz,
                 now_ms: int) -> dict:
    if scenario not in SCENARIOS:
        raise TestRefused("SCENARIO_INVALID")
    if side not in {"long", "short"}:
        raise TestRefused("SIDE_INVALID")
    spec = SCENARIOS[scenario]
    tick = _dec(tick_size)
    minimum = _dec(min_sz)
    bid = _dec(bid)
    ask = _dec(ask)
    if ask < bid:
        raise TestRefused("BOOK_INVALID")
    if side == "long":
        entry = _tick_round(bid, tick)
        stop = _tick_round(entry * (1 - spec["stop_pct"]), tick)
        tp1 = _tick_round(entry * (1 + spec["stop_pct"] * spec["tp1_r"]), tick)
        tp2 = _tick_round(entry * (1 + spec["stop_pct"] * spec["tp2_r"]), tick)
    else:
        entry = _tick_round(ask, tick)
        stop = _tick_round(entry * (1 + spec["stop_pct"]), tick)
        tp1 = _tick_round(entry * (1 - spec["stop_pct"] * spec["tp1_r"]), tick)
        tp2 = _tick_round(entry * (1 - spec["stop_pct"] * spec["tp2_r"]), tick)
    if not (entry > 0 and 0 < stop < entry < tp1 < tp2):
        if side == "short":
            if not (stop > entry > tp1 > tp2 > 0):
                raise TestRefused("PRICES_INVALID")
        else:
            raise TestRefused("PRICES_INVALID")
    stop_distance = abs(entry - stop)
    tp_distance = abs(tp1 - entry)
    stop_pct = stop_distance / entry
    low, high = spec["stop_range"]
    if not (low <= stop_pct <= high):
        raise TestRefused("STOP_RANGE_INVALID")
    gross_rr = tp_distance / stop_distance
    if gross_rr < 2:
        raise TestRefused("RR_INVALID")
    # 费用保守估计：maker 入场 0.02%，taker 退出 0.05%；净 RR 必须 >=1.55
    fee = entry * (Decimal("0.0002") + Decimal("0.0005"))
    net_rr = (tp_distance - fee) / (stop_distance + fee)
    if net_rr < Decimal("1.55"):
        raise TestRefused("NET_RR_INVALID")
    signal = {
        "id": spec["prefix"] + str(int(now_ms)),
        "instId": "SOL-USDT-SWAP",
        "kind": spec["kind"],
        "side": side,
        "entry": str(entry),
        "stop": str(stop),
        "tp1": str(tp1),
        "tp2": str(tp2),
        "qty": str(minimum),
        "generated_at_ms": int(now_ms),
        "expires_at_ms": int(now_ms) + ENTRY_WAIT_S * 1000,
        "require_maker": True,
        "entry_type": "limit",
        "slippage_bps": 0,
    }
    return signal


def build_plan(scenario: str, signal: dict) -> dict:
    if scenario not in SCENARIOS:
        raise TestRefused("SCENARIO_INVALID")
    return {"purpose": PURPOSE, "scenario": scenario,
            "authorization_id": AUTHORIZATION_ID, "signal": signal}


def validate_plan(plan: dict) -> None:
    if (not isinstance(plan, dict) or set(plan) != {"purpose", "scenario", "authorization_id", "signal"}
            or plan.get("purpose") != PURPOSE or plan.get("authorization_id") != AUTHORIZATION_ID
            or plan.get("scenario") not in SCENARIOS):
        raise TestRefused("PLAN_INVALID")
    signal = plan.get("signal")
    required = {"id", "instId", "kind", "side", "entry", "stop", "tp1", "tp2", "qty",
                "generated_at_ms", "expires_at_ms"}
    if (not isinstance(signal, dict) or not required <= set(signal)
            or set(signal) - required - {"require_maker", "slippage_bps", "entry_type"}):
        raise TestRefused("SIGNAL_INVALID")
    if signal["instId"] != "SOL-USDT-SWAP" or signal["kind"] != SCENARIOS[plan["scenario"]]["kind"]:
        raise TestRefused("SIGNAL_INVALID")
    prefix = SCENARIOS[plan["scenario"]]["prefix"]
    if not str(signal["id"]).startswith(prefix) or len(str(signal["id"])) <= len(prefix):
        raise TestRefused("SIGNAL_ID_INVALID")
    if _dec(signal["qty"]) <= 0:
        raise TestRefused("QTY_INVALID")
    if not 0 < int(signal["expires_at_ms"]) - int(signal["generated_at_ms"]) <= 600_000:
        raise TestRefused("EXPIRY_INVALID")


def _parse(argv):
    parser = argparse.ArgumentParser(description="DEMO 技术测试入口（默认只解释）")
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--run-test", action="store_true")
    parser.add_argument("--enable-demo-test", action="store_true")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--side", choices=("long", "short"), default="long")
    parser.add_argument("--predecessor-root", default=None)
    parser.add_argument("--new-root", default=str(ROOT))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--observe-seconds", type=int, default=None)
    parser.add_argument("--allow-short-observation", action="store_true")
    parser.add_argument("--entry-wait-seconds", type=int, default=ENTRY_WAIT_S)
    return parser.parse_args(argv)


# --------------------------------------------------------------------------- 流程


def _status_of(status) -> dict:
    if not isinstance(status, dict):
        raise TestRefused("CONTROLLER_STATUS_INVALID")
    return {"status": status.get("status"), "code": status.get("code"),
            "signal_id": status.get("signal_id"),
            "filled_qty": str(status.get("filled_qty", "")),
            "protected_qty": str(status.get("protected_qty", ""))}


def _qty(value):
    try:
        return Decimal(str(value or "0"))
    except Exception:  # noqa: BLE001
        return Decimal("0")


def _wait_entry(deps, runtime, signal_id, scenario, entry_wait_s):
    deadline = time.monotonic() + max(1, entry_wait_s)
    while time.monotonic() < deadline:
        state = _status_of(deps.status(runtime, signal_id))
        if state["status"] == "PROTECTED":
            return state, "PROTECTED"
        if state["status"] in TERMINAL_ENTRY:
            return state, state["status"]
        deps.tick(runtime)
        time.sleep(deps.poll_seconds)
    # 超时未成交：必须驱动控制器撤到终态，不能单撤单就退出
    deps.cancel_pending(runtime, signal_id)
    deadline = time.monotonic() + max(5, entry_wait_s)
    while time.monotonic() < deadline:
        state = _status_of(deps.status(runtime, signal_id))
        if state["status"] == "PROTECTED":
            return state, "PROTECTED"
        if state["status"] in TERMINAL_ENTRY:
            return state, state["status"]
        deps.tick(runtime)
        time.sleep(deps.poll_seconds)
    return _status_of(deps.status(runtime, signal_id)), "UNKNOWN"


def _observe(deps, runtime, signal_id, scenario, seconds):
    deadline = time.monotonic() + max(1, seconds)
    last = _status_of(deps.status(runtime, signal_id))
    while time.monotonic() < deadline:
        if last["status"] in TERMINAL_ENTRY:
            return last
        deps.tick(runtime)
        last = _status_of(deps.status(runtime, signal_id))
        time.sleep(deps.poll_seconds)
    return last


def _wait_close(deps, runtime, signal_id, timeout):
    deadline = time.monotonic() + max(1, timeout)
    state = _status_of(deps.status(runtime, signal_id))
    while time.monotonic() < deadline:
        if state["status"] in {"CLOSED", "HALTED", "UNKNOWN", "REJECTED"}:
            return state
        deps.tick(runtime)
        state = _status_of(deps.status(runtime, signal_id))
        time.sleep(deps.poll_seconds)
    return state


def _ledger_ok(risk: dict) -> bool:
    return (isinstance(risk, dict) and risk.get("demo_verified") is True
            and risk.get("ledger_verified") is True
            and risk.get("baseline_trusted") is True
            and risk.get("history_complete") is True
            and risk.get("account_snapshot_consistent") is True
            and not risk.get("refusal_reasons")
            and not risk.get("request_faults"))


def _verified_post_exit_pause(risk: dict) -> bool:
    """Recognize a complete ordinary exit proof with a sole policy pause.

    This is only a reporting/stop-polling condition after CLOSED. It never
    grants entry, changes _ledger_ok, or produces a successful test receipt.
    """
    if type(risk) is not dict:
        return False
    for key, expected in (("environment", "DEMO"), ("risk_adapter", "verified-demo-ledger-v2")):
        if type(risk.get(key)) is not str or risk[key] != expected:
            return False
    if (risk.get("demo_verified") is not False or risk.get("paused") is not True
            or risk.get("diagnostic_only", False) is not False):
        return False
    if not all(risk.get(key) is True for key in (
            "ledger_verified", "baseline_trusted", "history_complete",
            "pause_evidence_verified", "account_snapshot_consistent", "data_fresh")):
        return False
    if not all(risk.get(key) is False for key in ("macro_blackout", "daily_loss_hit", "weekly_loss_hit")):
        return False
    reasons = risk.get("refusal_reasons")
    if (type(reasons) is not list or len(reasons) != 1 or type(reasons[0]) is not str
            or reasons[0] != "CONSECUTIVE_LOSS_PAUSE"):
        return False
    for key in ("request_faults", "open_positions", "pending_orders", "protection_orders"):
        if type(risk.get(key)) is not list or len(risk[key]) != 0:
            return False
    keys = ("now_ms", "observed_at_ms", "collection_started_ms", "collection_completed_ms",
            "snapshot_window_start_ms", "snapshot_window_end_ms", "paused_until_ms", "consecutive_losses")
    if not all(type(risk.get(key)) is int and 0 < risk[key] <= 2**63-1 for key in keys):
        return False
    try:
        clock_ns = time.time_ns()
    except Exception:
        return False
    if type(clock_ns) is not int or clock_ns <= 0:
        return False
    now = clock_ns // 1_000_000
    # The server window and the second-view local observation are distinct
    # clocks. Retain the provider's 2s skew, 10s window and 5s local age bounds.
    return (risk["now_ms"] <= risk["observed_at_ms"] <= risk["collection_completed_ms"]
            <= now < risk["paused_until_ms"]
            and risk["snapshot_window_start_ms"] == risk["collection_started_ms"]
            and 0 <= risk["snapshot_window_end_ms"]-risk["snapshot_window_start_ms"] <= 10000
            and risk["collection_started_ms"] >= risk["now_ms"]-2000
            and risk["snapshot_window_end_ms"] <= risk["collection_completed_ms"]+2000
            and now-risk["observed_at_ms"] <= 5000)


def _wait_ledger(deps, runtime, timeout):
    deadline = time.monotonic() + max(1, timeout)
    risk = deps.risk_view(runtime)
    while time.monotonic() < deadline:
        if _ledger_ok(risk) or _verified_post_exit_pause(risk):
            return risk
        time.sleep(deps.poll_seconds)
        risk = deps.risk_view(runtime)
    return risk


def _account_clear(risk: dict) -> bool:
    return (isinstance(risk, dict) and _ledger_ok(risk)
            and not (risk.get("open_positions") or [])
            and not (risk.get("pending_orders") or [])
            and not (risk.get("protection_orders") or []))


def execute_test(deps, options) -> dict:
    """执行一个 scenario；锁、清理、报告都在双锁有效期内完成。"""
    report = {
        "purpose": "DEMO_TECHNICAL_TEST_REPORT",
        "scenario": options.scenario,
        "fixed_commit": deps.fixed_commit(),
        "run_id": options.run_id or "",
        "started_at_utc": deps.utc_now(),
        "started_at_beijing": deps.beijing_now(),
        "side": options.side,
        "stages": {},
        "result": "FAIL",
        "uncovered": [],
        "manual_review_required": True,
        "orders_sent": "UNKNOWN",
        "trading_state_changed": "UNKNOWN",
        "credentials_accessed": "UNKNOWN",
        "submit_attempted": False,
    }

    def _fail(code, detail="", runtime=None):
        report["refusal"] = {"code": code, "detail": detail}
        report["stages"]["failed"] = {"code": code}
        if runtime is not None and code in {"PROTECTION_UNVERIFIED", "CLOSE_NOT_CONFIRMED",
                                            "RECOVERY_ENTRY_NOT_PROTECTED",
                                            "ENTRY_HALTED", "ENTRY_UNKNOWN",
                                            "ENTRY_CANCEL_UNCONFIRMED",
                                            "OBSERVE_HALTED", "OBSERVE_UNKNOWN",
                                            "MARKET_REFRESH_FAILED",
                                            "UNKNOWN_ACTIVE_STATE", "ACTIVE_PLAN_MISSING"}:
            try:
                deps.safety_halt(runtime, code)
            except Exception:  # noqa: BLE001
                pass
        return deps.write_report(options.new_root, report, "FAIL")

    # 版本/路径/前任检查在凭据与锁之前；失败不写状态
    try:
        deps.validate_roots(options.predecessor_root, options.new_root)
        deps.check_predecessor(options.predecessor_root)
    except TestRefused as exc:
        return {"result": "FAIL", "refusal": {"code": exc.code, "detail": exc.detail},
                "report_path": ""}

    runtime = None
    try:
        with deps.locks(options.predecessor_root, options.new_root):
            try:
                deps.preserve_state(options.predecessor_root, options.new_root)
                runtime = deps.build_runtime(options.new_root)
                report["credentials_accessed"] = True
                deps.recover_owners(runtime)
                prepared = deps.prepare_run(runtime, options.new_root, options)
                natural_closed = False
                if prepared.get("mode") == "resume":
                    plan = prepared["plan"]
                    validate_plan(plan)
                    signal_id = plan["signal"]["id"]
                    report["plan"] = plan
                    report["run_id"] = deps.run_id(options.new_root, plan)
                    # 真实 controller.status 不带成交/保护数量：先 tick/reconcile
                    # 取得本轮完整证据，不能拿 plan.qty 或旧 _last 冒充。
                    reconciled = deps.reconcile(runtime)
                    if (not isinstance(reconciled, dict) or not reconciled.get("status")
                            or reconciled.get("signal_id") not in {None, signal_id}):
                        raise TestRefused("RECOVERY_EVIDENCE_MISSING")
                    status = _status_of(reconciled)
                    if not status.get("signal_id"):
                        status["signal_id"] = signal_id
                    report["stages"]["recovery"] = {"status": status["status"],
                                                    "signal_id": signal_id}
                    if status["status"] in WAITING:
                        state, entry_code = _wait_entry(deps, runtime, signal_id,
                                                        options.scenario, options.entry_wait_seconds)
                        if entry_code == "CLOSED":
                            natural_closed = True
                            status = state
                            if deps.verify_terminal(runtime, signal_id) is not True:
                                raise TestRefused("TERMINAL_UNVERIFIED", "CLOSED")
                        elif entry_code != "PROTECTED":
                            if entry_code == "UNKNOWN":
                                return _fail("ENTRY_CANCEL_UNCONFIRMED", entry_code, runtime)
                            return _fail("RECOVERY_ENTRY_NOT_PROTECTED", entry_code, runtime)
                        else:
                            status = state
                    elif status["status"] == "CLOSED":
                        # resume 的首次 reconcile 可能直接看到原 PROTECTED 已被天然
                        # OCO 退出（或原 CLOSING 刚完成）：真实 CLOSED 直接核验终态
                        # 与完整账本，不 close、不重新开仓。
                        natural_closed = True
                        if deps.verify_terminal(runtime, signal_id) is not True:
                            raise TestRefused("TERMINAL_UNVERIFIED", "CLOSED")
                    elif status["status"] not in {"PROTECTED", "CLOSING"}:
                        raise TestRefused("RECOVERY_STATE_UNSUPPORTED", str(status["status"]))
                    report["stages"]["entry"] = {"status": status["status"],
                                                 "code": status["code"], "recovered": True}
                    recovered_closing = status["status"] == "CLOSING"
                else:
                    snapshot = deps.collect_snapshot(options.new_root)
                    fields = deps.market_fields(snapshot)
                    plan = deps.resume_or_create_plan(options.new_root, options.scenario,
                                                      options.side, fields, report)
                    signal_id = plan["signal"]["id"]
                    validate_plan(plan)
                    report["plan"] = plan
                    report["run_id"] = deps.run_id(options.new_root, plan)
                    report["stages"]["plan"] = "PASS"
                    report["submit_attempted"] = True
                    result = _status_of(deps.start_test(runtime, plan, fields["evidence"]))
                    report["stages"]["submit"] = result
                    report["orders_sent"] = "UNKNOWN"
                    report["trading_state_changed"] = "UNKNOWN"
                    if result["status"] in {"REJECTED", "HALTED", "UNKNOWN"}:
                        raise TestRefused("ENTRY_" + str(result["status"]), str(result.get("code")))
                    state, entry_code = _wait_entry(deps, runtime, signal_id,
                                                    options.scenario, options.entry_wait_seconds)
                    report["stages"]["entry"] = {"status": state["status"],
                                                 "code": state["code"], "entry_result": entry_code}
                    status = state
                    if entry_code == "CLOSED":
                        natural_closed = True
                        if deps.verify_terminal(runtime, signal_id) is not True:
                            raise TestRefused("TERMINAL_UNVERIFIED", "CLOSED")
                    elif entry_code != "PROTECTED":
                        if entry_code == "UNKNOWN":
                            raise TestRefused("ENTRY_CANCEL_UNCONFIRMED", entry_code)
                        raise TestRefused("ENTRY_NOT_PROTECTED", entry_code)
                    else:
                        status = state
                    recovered_closing = False

                if status["status"] == "PROTECTED":
                    filled = _qty(status.get("filled_qty"))
                    protected = _qty(status.get("protected_qty"))
                    if not (filled > 0 and protected == filled):
                        raise TestRefused("PROTECTION_UNVERIFIED")
                    report["stages"]["protection"] = {"filled_qty": str(filled),
                                                      "protected_qty": str(protected),
                                                      "verified": True}
                    observed = _observe(deps, runtime, signal_id, options.scenario,
                                        options.observe_seconds)
                    report["stages"]["observe"] = {"seconds": options.observe_seconds,
                                                   "status": observed["status"]}
                    if observed["status"] == "CLOSED":
                        natural_closed = True
                        if deps.verify_terminal(runtime, signal_id) is not True:
                            raise TestRefused("TERMINAL_UNVERIFIED", "CLOSED")
                    elif observed["status"] in {"HALTED", "UNKNOWN"}:
                        raise TestRefused("OBSERVE_" + observed["status"])
                if not (recovered_closing or natural_closed):
                    close_state = _status_of(deps.close_test(runtime, signal_id))
                    report["stages"]["close_request"] = close_state
                closed = _wait_close(deps, runtime, signal_id,
                                     getattr(deps, "close_wait_seconds", CLOSE_WAIT_S))
                report["stages"]["close"] = closed
                if closed["status"] != "CLOSED":
                    raise TestRefused("CLOSE_NOT_CONFIRMED", str(closed.get("code")))
                if (natural_closed or recovered_closing) and deps.verify_terminal(runtime, signal_id) is not True:
                    raise TestRefused("TERMINAL_UNVERIFIED", "CLOSED")
                risk = _wait_ledger(deps, runtime,
                                    getattr(deps, "ledger_wait_seconds", LEDGER_WAIT_S))
                report["stages"]["ledger"] = {
                    "demo_verified": risk.get("demo_verified"),
                    "ledger_verified": risk.get("ledger_verified"),
                    "baseline_trusted": risk.get("baseline_trusted"),
                    "entries_today": risk.get("entries_today"),
                    "realized_pnl_today": risk.get("realized_pnl_today"),
                    "request_faults": len(risk.get("request_faults") or []),
                    "refusal_reasons": list(risk.get("refusal_reasons") or []),
                }
                if _verified_post_exit_pause(risk):
                    report["stages"]["post_exit_risk"] = {
                        "exit_ledger_verified": True,
                        "technical_closure_verified": (
                            report["stages"].get("protection", {}).get("verified") is True),
                        "entry_admissible": False,
                        "refusal_reasons": ["CONSECUTIVE_LOSS_PAUSE"],
                        "consecutive_losses": risk["consecutive_losses"],
                        "paused_until_ms": risk["paused_until_ms"],
                    }
                    raise TestRefused("POST_EXIT_RISK_PAUSED")
                if not _ledger_ok(risk):
                    raise TestRefused("LEDGER_NOT_VERIFIED")
                if not _account_clear(risk):
                    raise TestRefused("ACCOUNT_NOT_CLEAR")
                report["stages"]["clear"] = {"positions": 0, "pending_orders": 0,
                                             "protection_orders": 0}
                report["result"] = "PASS"
                report["manual_review_required"] = False
                report["orders_sent"] = True
                report["trading_state_changed"] = True
                return deps.write_report(options.new_root, report, "PASS")
            except TestRefused as exc:
                return _fail(exc.code, exc.detail, runtime)
            except Exception:  # noqa: BLE001 - 未知异常安全失败，不吞掉
                if runtime is not None:
                    try:
                        deps.safety_halt(runtime, "TECHNICAL_TEST_FAILURE")
                    except Exception:  # noqa: BLE001
                        pass
                return _fail("UNEXPECTED_EXCEPTION", "", runtime)
    except TestRefused as exc:
        # 锁失败/构造前失败：零凭据写、零状态写
        return {"result": "FAIL", "refusal": {"code": exc.code, "detail": exc.detail},
                "report_path": ""}


# --------------------------------------------------------------------------- 生产依赖（真实固定 DEMO 归档）


class ProductionDeps:
    """真实固定 DEMO 归档所用依赖；仅由 Codex 固定本地程序执行。

    所有网络写权限由 DemoRouteTransport(enable_writes=True) 与
    controller(enable_demo_tests=True) 双重显式开启。离线测试不触发本类；
    本类的局部接线由测试用 patch/子类验证。
    """

    poll_seconds = POLL_S

    def __init__(self):
        self.run_id_hint = None
        self._config = None
        self._root = ROOT
        self._context = {}
        self._context_ms = 0
        self._calendar = None
        self._macro_quotes = None
        self._entry_evidence = None
        self._fixed_commit = ""
        self._last = {}
        self._runtime = None
        self._http_get = None
        self._risk_observations = deque(maxlen=16)
        self._last_failed_risk = None
        self._active_entry_plan = None
        self._initial_plan_quote = None
        self._entry_quote_observations = deque(maxlen=16)

    # --- 基础信息
    def fixed_commit(self):
        return self._fixed_commit or self._read_commit(ROOT) or "UNKNOWN"

    @staticmethod
    def _read_commit(root):
        try:
            return (Path(root) / "DEPLOYED_COMMIT").read_text(encoding="utf-8").strip()
        except OSError:
            return ""

    @staticmethod
    def utc_now():
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def beijing_now():
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    # --- 根路径/部署清单/run-id
    @staticmethod
    def _sha(path):
        import hashlib
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _verify_archive(self, root, expected_commit=None, *, require_entry=False):
        root = Path(root).resolve()
        sys.path.insert(0, str(CODE_ROOT / "scripts"))
        from demo_auto_supervisor import verify_deployment
        commit = self._read_commit(root)
        if expected_commit is None:
            if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-f]{40}", commit):
                raise TestRefused("DEPLOYED_COMMIT_UNKNOWN")
            expected_commit = commit
        try:
            verify_deployment(root, expected_commit)
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "code", None) or "DEPLOYMENT_INVALID"
            raise TestRefused(str(code)) from None
        if require_entry:
            manifest = json.loads((root / "DEPLOYMENT_MANIFEST.json").read_text(encoding="utf-8"))
            files = set(manifest.get("files") or {})
            required = {"scripts/demo_auto_run.py", "scripts/demo_auto_supervisor.py",
                        "scripts/demo_technical_test.py", "config.json",
                        "src/order_router.py", "src/demo_route_transport.py",
                        "src/demo_auto.py"}
            missing = required - files
            if missing:
                raise TestRefused("DEPLOYMENT_REQUIRED_MISSING", ",".join(sorted(missing)))
        return commit

    def validate_roots(self, predecessor_root, new_root):
        new = Path(new_root).resolve()
        if new != ROOT.resolve():
            raise TestRefused("NEW_ROOT_MISMATCH")
        old = Path(predecessor_root).resolve()
        if old == new:
            raise TestRefused("SAME_RUNTIME_ROOT")
        self._verify_archive(old, EXPECTED_PREDECESSOR)
        self._fixed_commit = self._verify_archive(new, require_entry=True)
        if self._fixed_commit == EXPECTED_PREDECESSOR:
            raise TestRefused("TECHNICAL_TEST_SAME_AS_PREDECESSOR")
        return self._fixed_commit

    @staticmethod
    def validate_run_id(run_id):
        import re
        if run_id is None:
            return
        if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id):
            raise TestRefused("RUN_ID_INVALID")

    # --- 前置检查
    def check_predecessor(self, predecessor_root):
        root = Path(predecessor_root)
        if not root.is_dir():
            raise TestRefused("PREDECESSOR_MISSING")
        commit = self._read_commit(root)
        if commit != EXPECTED_PREDECESSOR:
            raise TestRefused("PREDECESSOR_VERSION_MISMATCH")
        supervisor = root / "state" / "demo-auto" / "supervisor.json"
        try:
            data = json.loads(supervisor.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise TestRefused("PREDECESSOR_SUPERVISOR_MISSING") from None
        if (data.get("status") != "EXITED" or data.get("code") != "CHILD_EXITED_NORMALLY"
                or data.get("return_code") != 0 or data.get("restarts") != 0):
            raise TestRefused("PREDECESSOR_NOT_EXITED")
        if not (root / "state" / "demo-auto" / "STOP_NEW_ENTRIES").exists():
            raise TestRefused("PREDECESSOR_STOP_MISSING")

    # --- 双锁（byte0，与已验收 process_lock 一致）
    @contextlib.contextmanager
    def locks(self, predecessor_root, new_root):
        old = Path(predecessor_root).resolve()
        new = Path(new_root).resolve()
        if old == new:
            raise TestRefused("SAME_RUNTIME_ROOT")
        paths = sorted({old / "state" / "demo-auto" / "process.lock",
                        new / "state" / "demo-auto" / "process.lock"})
        handles = []
        try:
            for path in paths:
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
                        raise TestRefused("PROCESS_LOCKED") from None
                except Exception:
                    handle.close()
                    raise
                handles.append(handle)
            yield
        finally:
            for handle in handles:
                try:
                    handle.close()
                except OSError:
                    pass

    # --- 状态保留（不重写已有，不迁移 STOP/HALT，拒绝不可读/被替换历史）
    def preserve_state(self, predecessor_root, new_root):
        src = Path(predecessor_root) / "state" / "demo-auto"
        dst = Path(new_root).resolve() / "state" / "demo-auto"
        dst.mkdir(parents=True, exist_ok=True)
        for name in sorted(p.name for p in src.glob("*")):
            if name == "CONTROLLER_HALT" or name.startswith("HALT-"):
                raise TestRefused("TECH_TEST_PREDECESSOR_HALT", name)
        for name in PRESERVED_STATE:
            source = src / name
            target = dst / name
            if not source.exists():
                raise TestRefused("PREDECESSOR_STATE_MISSING", name)
            source_rows = None
            if name in ("controller.sqlite3", "router.sqlite3"):
                source_rows = self._journal_snapshot(source)
                if source_rows is None:
                    raise TestRefused("JOURNAL_UNREADABLE", name)
            if target.exists():
                if name in ("account-binding.json", "auto-authorization.json"):
                    if self._sha(source) != self._sha(target):
                        raise TestRefused("ACCOUNT_OR_AUTHORIZATION_ROTATED", name)
                elif source_rows is not None:
                    target_rows = self._journal_snapshot(target)
                    if target_rows is None:
                        raise TestRefused("JOURNAL_UNREADABLE", name)
                    if not source_rows <= target_rows:
                        raise TestRefused("JOURNAL_HISTORY_MISSING", name)
                continue
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copy2(source, target)
            if source_rows is not None:
                copied = self._journal_snapshot(target)
                if copied is None or not source_rows <= copied:
                    raise TestRefused("JOURNAL_MIGRATION_INCOMPLETE", name)
        for forbidden in ("STOP_NEW_ENTRIES", "CONTROLLER_HALT"):
            if (dst / forbidden).exists():
                raise TestRefused("TECHNICAL_TEST_STATE_CONFLICT", forbidden)

    @staticmethod
    def _journal_snapshot(path):
        """返回全部表的行签名；不可读返回 None（必须拒绝，不得放行）。"""
        try:
            con = sqlite3.connect(f"file:{Path(path).resolve().as_posix()}?mode=ro", uri=True)
            try:
                rows = set()
                for (table,) in con.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                    for row in con.execute(f"SELECT * FROM {table}"):
                        rows.add((table, tuple(str(value) for value in row)))
                return rows
            finally:
                con.close()
        except Exception:  # noqa: BLE001
            return None

    # --- 真实构造（生产路径；测试用局部 patch 验证）
    def build_runtime(self, new_root):
        sys.path.insert(0, str(ROOT))
        sys.path.insert(0, str(ROOT / "scripts"))
        from demo_auto_run import ServiceEntryEvidence, bind_demo_account, market_context, service_quote
        from src.collect import _macro_state, collect
        from src.demo_auto import DemoAutoController, DEMO_ACCOUNT_KEY
        from src.demo_route_transport import DemoRouteTransport
        from src.demo_transport import DemoReadOnlyClient, load_demo_credentials
        from src.demo_verified_risk import VerifiedDemoRiskProvider
        from src.order_router import OrderRouter
        from src.util import load_config

        new_root = Path(new_root).resolve()
        state = new_root / "state" / "demo-auto"
        from src.demo_successor_gate import SuccessorGateRefused, assert_activated_generation
        from demo_auto_run import assert_service_recovery_ready
        try:
            assert_service_recovery_ready(state)
            assert_activated_generation(new_root)
        except SuccessorGateRefused as error:
            raise TestRefused(error.code) from None
        except RuntimeError:
            raise TestRefused("DEMO_SERVICE_RECOVERY_REFUSED") from None
        config = load_config()
        creds = load_demo_credentials()
        # Identity binding stays a single GET attempt. Only risk evidence opts
        # into a retry after reserving the same provider/path dispatch budget.
        binding_client = DemoReadOnlyClient(creds, http_get=self._http_get, network_retries=0)
        identity = bind_demo_account(binding_client, state, require_trade=True)
        auth = json.loads((state / "auto-authorization.json").read_text(encoding="utf-8"))
        if (auth.get("environment") != "DEMO" or auth.get("account_hash") != identity
                or auth.get("source") != "user-approved-demo-activation"
                or auth.get("scope") != "SOL-USDT-SWAP_EXISTING_RISK"
                or not isinstance(auth.get("authorization_recorded_at_ms"), int)
                or not 0 < auth["authorization_recorded_at_ms"] <= int(time.time() * 1000)):
            raise TestRefused("DEMO_AUTHORIZATION_INVALID")
        provider = None

        def reserve_retry(path):
            if provider is None:
                raise TestRefused("DEMO_RETRY_BUDGET_UNAVAILABLE")
            provider._reserve_get(path)

        client = DemoReadOnlyClient(creds, http_get=self._http_get,
                                   network_retries=1, retry_budget=reserve_retry)
        transport = DemoRouteTransport(creds, enable_writes=True, http_get=self._http_get,
                                       log_root=new_root / "logs")
        provider = VerifiedDemoRiskProvider(
            client, config, state / "verified-risk-v2",
            authorization=auth,
            router_journal_path=state / "router.sqlite3",
            account_key=DEMO_ACCOUNT_KEY)
        risk = ObservedRiskProvider(provider, self._observe_risk)
        self._config = config
        self._root = new_root
        self._market_context = market_context
        self._collect = collect
        self._macro_state = _macro_state
        self._context = {}
        self._context_ms = 0
        self._calendar = None
        self._macro_quotes = None

        def current_quote():
            # Same source-time and local-clock checks as activation/service.
            try:
                return service_quote(client)
            except RuntimeError:
                raise TestRefused("DEMO_MARKET_UNAVAILABLE") from None

        quote = ObservedQuoteProvider(current_quote, self._observe_entry_quote)
        self._quote = quote
        self._entry_evidence = ServiceEntryEvidence(config, quote)
        router = OrderRouter(
            transport=transport, config=config,
            state_provider=lambda: risk.snapshot(self._fresh_risk_context()),
            market_provider=quote,
            log_root=new_root / "logs",
            journal_path=state / "router.sqlite3",
            account_key=DEMO_ACCOUNT_KEY,
            account_identity=identity,
            require_demo_entry_admission=True,
            final_entry_check=self._assert_entry_current)
        controller = DemoAutoController(
            config, router, risk, quote, new_root,
            enable_demo_tests=True)
        self._runtime = {"controller": controller, "router": router, "risk": risk,
                         "transport": transport, "client": client, "config": config,
                         "root": str(new_root)}
        return self._runtime

    # --- 启动/恢复
    def prepare_run(self, runtime, new_root, options):
        status = runtime["controller"].status()
        if status.get("status") == "HALTED":
            raise TestRefused("CONTROLLER_HALTED", str(status.get("code")))
        # A prior process may have persisted the router HALT before it could
        # persist the controller's HALTED result. Never reinterpret that older
        # SUBMITTING row as permission for recovery/cleanup account mutations.
        self._assert_router_not_halted(runtime, new_root)
        base = Path(new_root).resolve() / "logs" / "technical-tests"
        plans = []
        if base.exists():
            for path in sorted(base.glob("*/plan.json")):
                try:
                    plan = json.loads(path.read_text(encoding="utf-8"))
                    validate_plan(plan)
                except (OSError, ValueError, TestRefused):
                    raise TestRefused("LOCAL_PLAN_INVALID") from None
                plans.append((path.parent.name, plan))
        active = status.get("status") not in {"IDLE", "HALTED"} or bool(status.get("signal_id"))
        if active:
            signal_id = str(status.get("signal_id") or "")
            if not signal_id.startswith("DEMO-TEST-"):
                raise TestRefused("FOREIGN_ACTIVE_INTENT", signal_id)
            matches = [(rid, plan) for rid, plan in plans
                       if plan["signal"]["id"] == signal_id]
            if not matches:
                raise TestRefused("ACTIVE_PLAN_MISSING")
            if len(matches) > 1:
                raise TestRefused("PLAN_CONFLICT", signal_id)
            plan = matches[0][1]
            run_id = matches[0][0]
            if options.run_id and options.run_id != run_id:
                raise TestRefused("RUN_ID_MISMATCH")
            if plan.get("scenario") != options.scenario:
                raise TestRefused("PLAN_SCENARIO_MISMATCH", str(plan.get("scenario")))
            if plan["signal"].get("side") != options.side:
                raise TestRefused("PLAN_SIDE_MISMATCH", str(plan["signal"].get("side")))
            if status.get("status") not in {"PENDING", "PROTECTED", "CLOSING"}:
                raise TestRefused("UNKNOWN_ACTIVE_STATE", str(status.get("status")))
            if not self._registration_matches(new_root, signal_id, plan):
                raise TestRefused("ACTIVE_PLAN_MISMATCH", signal_id)
            return {"mode": "resume", "plan": plan, "status": status}
        # No account/history IO before public collection supplies context.
        # Controller admission and router recheck retain their complete risk
        # snapshots; this only establishes local journal consistency.
        self._assert_local_idle(runtime, new_root, status, plans)
        if options.run_id and any(rid == options.run_id for rid, _ in plans):
            raise TestRefused("DEMO_TEST_PLAN_EXISTS")
        return {"mode": "new"}

    @staticmethod
    def _assert_router_not_halted(runtime, new_root):
        state = Path(new_root).resolve() / "state" / "demo-auto"
        try:
            if (getattr(runtime.get("router"), "_local_halted", False)
                    or list(state.glob("HALT-*"))):
                raise TestRefused("LOCAL_HALT_PRESENT")
            with contextlib.closing(sqlite3.connect(
                    f"file:{(state / 'router.sqlite3').as_posix()}?mode=ro", uri=True)) as db:
                halted = db.execute("SELECT 1 FROM controls LIMIT 1").fetchone()
        except (sqlite3.Error, OSError, ValueError):
            raise TestRefused("LOCAL_JOURNAL_UNREADABLE") from None
        if halted is not None:
            raise TestRefused("LOCAL_HALT_PRESENT")

    @staticmethod
    def _assert_local_idle(runtime, new_root, status, plans):
        state = Path(new_root).resolve() / "state" / "demo-auto"
        if (state / "STOP_NEW_ENTRIES").exists():
            raise TestRefused("TECHNICAL_TEST_STATE_CONFLICT")
        if ((state / "CONTROLLER_HALT").exists() or list(state.glob("HALT-*"))
                or getattr(runtime.get("router"), "_local_halted", False)):
            raise TestRefused("LOCAL_HALT_PRESENT")
        if status.get("status") != "IDLE" or status.get("signal_id"):
            raise TestRefused("LOCAL_STATE_NOT_IDLE")
        try:
            with contextlib.closing(sqlite3.connect(
                    f"file:{(state / 'controller.sqlite3').as_posix()}?mode=ro", uri=True)) as db:
                meta = dict(db.execute("SELECT key,value FROM controller_meta"))
                signals = list(db.execute("SELECT signal,status,intent FROM controller_signals"))
                registered = list(db.execute(
                    "SELECT signal,purpose,scenario,authorization_id,plan_fingerprint,signal_json "
                    "FROM technical_tests"))
            with contextlib.closing(sqlite3.connect(
                    f"file:{(state / 'router.sqlite3').as_posix()}?mode=ro", uri=True)) as db:
                controls = list(db.execute("SELECT account,code FROM controls"))
                routes = list(db.execute("SELECT account,signal,client,status,owner,data FROM routes"))
        except (sqlite3.Error, OSError, ValueError):
            raise TestRefused("LOCAL_JOURNAL_UNREADABLE") from None
        if meta.get("account") != "DEMO-AUTO-SOL-V1" or meta.get("schema") != "1":
            raise TestRefused("LOCAL_JOURNAL_INVALID")
        if "halt" in meta or controls:
            raise TestRefused("LOCAL_HALT_PRESENT")
        if any(row[1] not in LOCAL_TERMINAL for row in signals):
            raise TestRefused("LOCAL_CONTROLLER_NOT_TERMINAL")
        if any(row[3] not in LOCAL_TERMINAL or row[4] for row in routes):
            raise TestRefused("LOCAL_ROUTER_NOT_TERMINAL")
        signal_rows = {row[0]: row for row in signals}
        route_rows = {}
        for account, signal, client, route_status, owner, raw in routes:
            try:
                row = json.loads(raw)
            except (ValueError, TypeError):
                raise TestRefused("LOCAL_ROUTE_INVALID") from None
            if (account != "DEMO-AUTO-SOL-V1" or signal in route_rows
                    or not isinstance(row, dict) or row.get("signal_id") != signal
                    or row.get("status") != route_status or row.get("client_order_id") != client
                    or row.get("inst_id") != "SOL-USDT-SWAP"
                    or signal not in signal_rows or signal_rows[signal][1] != route_status):
                raise TestRefused("LOCAL_ROUTE_MISMATCH")
            route_rows[signal] = row
        plan_rows = {}
        for _, plan in plans:
            plan_rows.setdefault(plan["signal"]["id"], []).append(plan)
        registrations = {}
        for signal, purpose, scenario, authorization, fingerprint, raw in registered:
            try:
                plan = {"purpose": purpose, "scenario": scenario,
                        "authorization_id": authorization, "signal": json.loads(raw)}
                validate_plan(plan)
                canonical = json.dumps(plan, sort_keys=True, separators=(",", ":"), allow_nan=False)
            except (ValueError, TypeError, TestRefused):
                raise TestRefused("LOCAL_REGISTRATION_INVALID") from None
            if (plan["signal"]["id"] != signal
                    or hashlib.sha256(canonical.encode()).hexdigest() != fingerprint
                    or signal not in signal_rows
                    or len(plan_rows.get(signal, [])) != 1 or plan_rows[signal][0] != plan):
                raise TestRefused("LOCAL_REGISTRATION_MISMATCH")
            registrations[signal] = plan
        for signal, signal_status, raw in signals:
            if signal.startswith("DEMO-TEST-") and signal not in registrations:
                raise TestRefused("LOCAL_REGISTRATION_MISSING")
            if signal_status in {"CLOSED", "CANCELED"} and signal not in route_rows:
                raise TestRefused("LOCAL_ROUTE_MISSING")
            if raw is not None:
                try:
                    intent = json.loads(raw)
                except (TypeError, ValueError):
                    raise TestRefused("LOCAL_INTENT_INVALID") from None
                if (not isinstance(intent, dict) or intent.get("signal_id") != signal
                        or intent.get("inst_id") != "SOL-USDT-SWAP"):
                    raise TestRefused("LOCAL_INTENT_INVALID")

    @staticmethod
    def _registration_matches(new_root, signal_id, plan):
        """核对 technical_tests 登记与不可变 plan 指纹（只读）。"""
        import hashlib
        path = Path(new_root).resolve() / "state" / "demo-auto" / "controller.sqlite3"
        try:
            con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            try:
                row = con.execute("SELECT purpose,scenario,authorization_id,plan_fingerprint,"
                                  "signal_json FROM technical_tests WHERE signal=?",
                                  (signal_id,)).fetchone()
            finally:
                con.close()
        except Exception:  # noqa: BLE001
            return False
        if row is None:
            return False
        purpose, scenario, authorization, fingerprint, signal_json = row
        canonical = json.dumps(plan, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        return (purpose == plan["purpose"] and scenario == plan["scenario"]
                and authorization == plan["authorization_id"]
                and fingerprint == digest
                and json.loads(signal_json) == plan["signal"])

    # --- 市场
    def collect_snapshot(self, new_root):
        path = Path(new_root).resolve() / "state" / "demo-auto" / "market.json"
        try:
            prev = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            prev = None
        snapshot = self._collect(self._config, prev, calendar_max_age_s=1500)
        ticker = (snapshot.get("sources") or {}).get("ticker") or {}
        fetched = ticker.get("fetched_at_ms")
        generated = snapshot.get("generated_at_ms")
        now = int(time.time() * 1000)
        if type(fetched) is int and fetched > now:
            raise TestRefused("TICKER_NOT_FRESH")
        if type(generated) is int and generated > now:
            raise TestRefused("MARKET_CONTEXT_STALE")
        if any(type(stamp) is not int or not 0 < stamp <= now or now - stamp > 5_000
               for stamp in (generated, fetched)):
            # One bounded incremental collection, keeping every other source's
            # original envelope. Omitting only ticker forces an actual new GET;
            # collect itself rebuilds features/facts/health from source clocks.
            incremental = deepcopy(snapshot)
            incremental["sources"] = dict(incremental.get("sources") or {})
            incremental["sources"].pop("ticker", None)
            snapshot = self._collect(self._config, incremental, calendar_max_age_s=1500)
        try:
            path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass
        return snapshot

    def _update_context(self, snapshot, evidence):
        facts = snapshot.get("facts") or {}
        ticker = (snapshot.get("sources") or {}).get("ticker") or {}
        self._context = {
            "last": str(facts.get("price")),
            "timestamp_ms": int(ticker.get("fetched_at_ms") or 0),
            "generated_at_ms": int(snapshot.get("generated_at_ms") or 0),
            "data_fresh": bool(evidence.get("data_fresh")),
            "macro_blackout": bool(evidence.get("macro_blackout")) if isinstance(evidence.get("macro_blackout"), bool) else None,
            "observed_at_ms": int(evidence.get("observed_at_ms") or 0),
        }
        self._context_ms = int(time.time() * 1000)

    def market_fields(self, snapshot):
        facts = snapshot.get("facts") or {}
        symbol = snapshot.get("symbol") or {}
        ticker = (snapshot.get("sources") or {}).get("ticker") or {}
        fetched = ticker.get("fetched_at_ms")
        now = int(time.time() * 1000)
        if not isinstance(fetched, int) or now - fetched > 5_000 or fetched > now:
            raise TestRefused("TICKER_NOT_FRESH")
        evidence = self._market_context(snapshot)
        generated = snapshot.get("generated_at_ms")
        if not isinstance(generated, int) or generated > now or now - generated > 5_000:
            raise TestRefused("MARKET_CONTEXT_STALE")
        if evidence.get("data_fresh") is not True or evidence.get("macro_blackout") is not False:
            raise TestRefused("MARKET_EVIDENCE_UNVERIFIED")
        evidence = {**evidence, "generated_at_ms": generated,
                    "last": str(facts.get("price")), "timestamp_ms": fetched}
        self._update_context(snapshot, evidence)
        calendar = (snapshot.get("sources") or {}).get("economic_calendar") or {}
        self._calendar = {"source_ts": calendar.get("source_ts"),
                          "fetched_at_ms": calendar.get("fetched_at_ms"),
                          "events": calendar.get("data") or [],
                          "collected_at_ms": generated}
        self._macro_quotes = ((snapshot.get("features") or {}).get("macro") or {}).get("quotes")
        try:
            bid = facts["bid"]
            ask = facts["ask"]
            last = facts["price"]
            tick_size = symbol["tickSz"]
            min_sz = symbol["minSz"]
        except KeyError:
            raise TestRefused("MARKET_FIELDS_MISSING") from None
        if self._entry_evidence is not None:
            # The common service evidence owns a deep copy of every original
            # source envelope; fresh quotes never retimestamp those sources.
            self._entry_evidence.update_scan(snapshot)
        return {"bid": bid, "ask": ask, "last": last,
                "tick_size": tick_size, "min_sz": min_sz,
                "evidence": evidence, "snapshot": snapshot}

    # --- 计划
    @staticmethod
    def _plan_now_ms():
        try:
            value = time.time_ns()
        except Exception:
            raise TestRefused("PLAN_CLOCK_INVALID") from None
        if type(value) is not int or value < 1_000_000:
            raise TestRefused("PLAN_CLOCK_INVALID")
        return value // 1_000_000

    def _assert_plan_scan_current(self, fields, now):
        # A new execution quote cannot refresh the original scan or calendar.
        try:
            evidence, snapshot = fields["evidence"], fields["snapshot"]
            ticker = snapshot["sources"]["ticker"]
            stamps = (evidence["generated_at_ms"], evidence["timestamp_ms"],
                      snapshot["generated_at_ms"], ticker["fetched_at_ms"])
            fresh, blackout = evidence["data_fresh"], evidence["macro_blackout"]
        except (KeyError, TypeError):
            raise TestRefused("PLAN_MARKET_CONTEXT_INVALID") from None
        if fresh is not True or blackout is not False:
            raise TestRefused("MARKET_EVIDENCE_UNVERIFIED")
        if any(type(stamp) is not int or not 0 < stamp <= now or now - stamp > 5000
               for stamp in stamps):
            raise TestRefused("PLAN_MARKET_CONTEXT_STALE")
        # Reuse the existing pure STOP/HALT, critical-source and macro window
        # guard. Public source budgets are neither extended nor tightened here.
        from src.order_router import RouterError
        try:
            self._assert_entry_current(now)
        except RouterError as error:
            raise TestRefused(error.code) from None

    @staticmethod
    def _plan_quote_fields(quote, now):
        try:
            if type(quote) is not dict:
                raise ValueError
            stamp = quote["timestamp_ms"]
            if type(stamp) is not int or not 0 < stamp <= now or now - stamp > 5000:
                raise ValueError
            bid, ask, last = (_dec(quote[name]) for name in ("bid", "ask", "last"))
            if bid > ask:
                raise ValueError
        except (KeyError, TypeError, ValueError, TestRefused):
            raise TestRefused("PLAN_EXECUTION_QUOTE_INVALID") from None
        return {"bid": str(bid), "ask": str(ask), "last": str(last), "timestamp_ms": stamp}

    def _new_plan_execution_fields(self, fields):
        began = self._plan_now_ms()
        self._assert_plan_scan_current(fields, began)
        provider = getattr(self, "_quote", None)
        if not callable(provider):
            raise TestRefused("PLAN_EXECUTION_QUOTE_UNAVAILABLE")
        try:
            raw = provider()  # Exactly one existing fixed-DEMO native ticker GET.
        except Exception:
            raise TestRefused("PLAN_EXECUTION_QUOTE_UNAVAILABLE") from None
        completed = self._plan_now_ms()
        if completed < began:
            raise TestRefused("PLAN_CLOCK_INVALID")
        self._assert_plan_scan_current(fields, completed)
        quote = self._plan_quote_fields(raw, completed)
        return {**fields, **{name: quote[name] for name in ("bid", "ask", "last")},
                "execution_quote": {**quote, "observed_at_ms": completed}}, completed

    def resume_or_create_plan(self, new_root, scenario, side, fields, report):
        self.validate_run_id(self.run_id_hint)
        base = (Path(new_root).resolve() / "logs" / "technical-tests").resolve()
        run_id = self.run_id_hint or ("test-" + self.utc_now().replace(":", "").replace("-", "")
                                      + "-" + uuid.uuid4().hex[:8])
        run_dir = (base / run_id).resolve()
        if not run_dir.is_relative_to(base):
            raise TestRefused("RUN_ID_PATH_ESCAPE")
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "plan.json"
        if path.exists():
            plan = json.loads(path.read_text(encoding="utf-8"))
            validate_plan(plan)
            if plan.get("scenario") != scenario:
                raise TestRefused("PLAN_SCENARIO_MISMATCH")
            return plan
        execution, now = self._new_plan_execution_fields(fields)
        signal = build_signal(scenario, side, execution["bid"], execution["ask"],
                              execution["tick_size"], execution["min_sz"], now)
        plan = build_plan(scenario, signal)
        validate_plan(plan)
        # Pure plan construction must not carry either source over its original
        # deadline. Never retimestamp a source, reuse risk, or alter an old plan.
        committing = self._plan_now_ms()
        if committing < now:
            raise TestRefused("PLAN_CLOCK_INVALID")
        self._assert_plan_scan_current(fields, committing)
        self._plan_quote_fields(execution["execution_quote"], committing)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
        self._notify_plan_quote(plan, execution)
        return plan

    def run_id(self, new_root, plan):
        base = (Path(new_root).resolve() / "logs" / "technical-tests").resolve()
        if base.exists():
            for path in sorted(base.glob("*/plan.json")):
                try:
                    if json.loads(path.read_text(encoding="utf-8")) == plan:
                        return path.parent.name
                except ValueError:
                    continue
        return self.run_id_hint or ""

    # --- 执行状态
    @staticmethod
    def _assert_entry_flags(new_root):
        """Dynamic entry-only flags; only a missing individual flag is clear."""
        state = Path(new_root) / "state/demo-auto"
        for path, code in ((state / "STOP_NEW_ENTRIES", "TECHNICAL_TEST_STATE_CONFLICT"),
                           (state / "CONTROLLER_HALT", "LOCAL_HALT_PRESENT")):
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            except OSError:
                raise TestRefused("LOCAL_ENTRY_FLAGS_UNREADABLE") from None
            raise TestRefused(code)
        try:
            # Enumerate names only, so an unreadable directory cannot hide a
            # router HALT. lstat also detects dangling links and reparse flags.
            for path in state.iterdir():
                if not path.name.startswith("HALT-"):
                    continue
                try:
                    path.lstat()
                except FileNotFoundError:
                    continue
                raise TestRefused("LOCAL_HALT_PRESENT")
        except OSError:
            raise TestRefused("LOCAL_ENTRY_FLAGS_UNREADABLE") from None

    def _assert_entry_current(self, effective_now_ms):
        from src.order_router import RouterError
        try:
            self._assert_entry_flags(self._root)
            if self._entry_evidence is None:
                raise RouterError("DEMO_ENTRY_ADMISSION_FAILED")
            window = self._entry_evidence.assert_entry_current(effective_now_ms)
            # A STOP raised while the pure evidence callback was processing
            # must also be visible before its trusted deadline is returned.
            self._assert_entry_flags(self._root)
        except TestRefused:
            # Existing transport vocabulary records a proved zero-send entry
            # refusal. This hook never runs for protection, cancel or exit IO.
            raise RouterError("DEMO_ENTRY_ADMISSION_FAILED") from None
        return window

    def start_test(self, runtime, plan, evidence):
        try:
            root = Path(runtime["root"]).resolve(strict=True)
        except (KeyError, TypeError, ValueError, OSError):
            raise TestRefused("NEW_ROOT_MISMATCH") from None
        if root != self._root:
            raise TestRefused("NEW_ROOT_MISMATCH")
        self._assert_entry_flags(root)
        # Only diagnostic numeric facts are copied. The immutable plan, its
        # registration and every ordinary risk/admission check stay unchanged.
        try:
            self._active_entry_plan = self._diagnostic_plan(plan)
        except Exception:
            self._active_entry_plan = None
        try:
            result = runtime["controller"].process_demo_test(plan, evidence)
        finally:
            # Tick, protection, cancellation and exit quotes are not entry facts.
            self._active_entry_plan = None
        self._last = dict(result or {})
        return result

    def tick(self, runtime):
        # 价格新鲜度由 router/controller 的有界 quote provider 在调用时获取；
        # 这里不做全量 32 源采集，避免阻塞退出/观察。
        result = runtime["controller"].tick()
        if isinstance(result, dict):
            self._last = dict(result)
        return result

    def status(self, runtime, signal_id):
        status = runtime["controller"].status()
        if status.get("signal_id") == signal_id or status.get("status") != "IDLE":
            return {**status, **{k: v for k, v in self._last.items()
                                 if k in {"filled_qty", "protected_qty"}}}
        if self._last.get("signal_id") == signal_id:
            return dict(self._last)
        return status

    def cancel_pending(self, runtime, signal_id):
        result = runtime["controller"].close_demo_test(signal_id)
        self._last = dict(result or {})
        return result

    def close_test(self, runtime, signal_id):
        result = runtime["controller"].close_demo_test(signal_id)
        self._last = dict(result or {})
        return result

    def _fresh_risk_context(self):
        """每次风险调用都由真实新 quote + 原始日历 source_ts + 当前宏观窗口生成。

        demo_risk 的 MARKET_STALE 用 context.observed_at_ms（60s）；这里用报价来源
        ts（且 ≤5s），不把旧 quote 换 now。日历 source_ts 保持采集时原值，超过
        1800s 保守上限即拒绝（data_fresh=False / macro=None）。不做 32 源全量采集。
        """
        quote = self._quote()
        stamp = quote.get("timestamp_ms")
        now = time.time_ns() // 1_000_000
        if (not isinstance(stamp, int) or stamp > now or now - stamp > 5_000):
            raise TestRefused("MARKET_REFRESH_FAILED")
        cal = self._calendar or {}
        source = cal.get("source_ts")
        fetched = cal.get("fetched_at_ms")
        if (not isinstance(source, int) or not isinstance(fetched, int)
                or not 0 < source <= fetched <= now or now - source > 1_800_000):
            return {"observed_at_ms": stamp, "data_fresh": False, "macro_blackout": None}
        try:
            macro = self._macro_state(self._macro_quotes, cal.get("events") or [],
                                      self._config)
        except Exception:  # noqa: BLE001
            return {"observed_at_ms": stamp, "data_fresh": False, "macro_blackout": None}
        blackout = macro.get("blackout")
        if type(blackout) is not bool:
            return {"observed_at_ms": stamp, "data_fresh": False, "macro_blackout": None}
        return {"observed_at_ms": stamp, "data_fresh": True, "macro_blackout": blackout}

    def risk_view(self, runtime):
        # 每次风险调用重新取真实 quote，并基于原始日历 source_ts/当前宏观窗口重建 context。
        return runtime["risk"].snapshot(self._fresh_risk_context())

    @staticmethod
    def _diagnostic_code(value):
        return value if isinstance(value, str) and value in DIAGNOSTIC_CODES else "UNKNOWN_CODE"

    @staticmethod
    def _diagnostic_time(value):
        return value if type(value) is int and 0 <= value <= 99_999_999_999_999 else None

    @staticmethod
    def _diagnostic_price(value):
        # No arbitrary __str__/repr, exception text or unbounded decimal output.
        if type(value) not in {str, int, float, Decimal}:
            return None
        if type(value) is int and value.bit_length() > 128:
            return None
        try:
            if type(value) is Decimal:
                number = value
            else:
                text = str(value)
                if len(text) > 64:
                    return None
                number = Decimal(text)
            if (not number.is_finite() or not 0 < number <= Decimal("1000000000")
                    or number.adjusted() < -18):
                return None
            if type(value) is Decimal:
                # Bound the coefficient before any formatting or as_tuple()
                # copying. Inexact values are refused rather than rounded.
                with localcontext(Context(prec=28, traps=[Inexact, InvalidOperation])):
                    bounded = number.quantize(Decimal("1E-18"))
                if bounded != number:
                    return None
                number = bounded
            parts = number.as_tuple()
            if parts.exponent < -18 or len(parts.digits) > 28:
                return None
        except Exception:
            return None
        return str(number)

    def _diagnostic_plan(self, plan):
        signal = plan.get("signal") if isinstance(plan, Mapping) else None
        signal = signal if isinstance(signal, Mapping) else {}
        side = signal.get("side")
        return {"plan_side": side if type(side) is str and side in {"long", "short"} else None,
                "plan_entry": self._diagnostic_price(signal.get("entry")),
                "plan_generated_at_ms": self._diagnostic_time(signal.get("generated_at_ms"))}

    def _observe_plan_quote(self, plan, fields):
        """Keep the initial quote independently of the bounded entry history."""
        try:
            evidence = fields.get("evidence") or {}
            sources = (fields.get("snapshot") or {}).get("sources") or {}
            ticker = sources.get("ticker") or {}
            execution = fields.get("execution_quote")
            self._initial_plan_quote = {
                **self._diagnostic_plan(plan),
                "bid": self._diagnostic_price(fields.get("bid")),
                "ask": self._diagnostic_price(fields.get("ask")),
                "last": self._diagnostic_price(fields.get("last")),
                # Collection's fetched_at is not relabeled as exchange time.
                "quote_source_timestamp_ms": self._diagnostic_time(ticker.get("source_ts")),
                "quote_fetched_at_ms": self._diagnostic_time(evidence.get("timestamp_ms")),
            }
            if type(execution) is dict:
                self._initial_plan_quote.update({
                    "quote_source": "DEMO_SERVICE_QUOTE",
                    "quote_source_timestamp_ms": self._diagnostic_time(execution.get("timestamp_ms")),
                    "quote_fetched_at_ms": self._diagnostic_time(execution.get("observed_at_ms")),
                    "scan_source_timestamp_ms": self._diagnostic_time(ticker.get("source_ts")),
                    "scan_fetched_at_ms": self._diagnostic_time(evidence.get("timestamp_ms")),
                    "scan_generated_at_ms": self._diagnostic_time(
                        (fields.get("snapshot") or {}).get("generated_at_ms")),
                })
        except Exception:
            # Neither plan creation nor registration depends on diagnostics.
            self._initial_plan_quote = None

    def _notify_plan_quote(self, plan, fields):
        try:
            self._observe_plan_quote(plan, fields)
        except Exception:
            self._initial_plan_quote = None

    def _observe_entry_quote(self, quote, started, completed):
        if self._active_entry_plan is None:
            return
        value = quote if isinstance(quote, Mapping) else {}
        self._entry_quote_observations.append({
            **self._active_entry_plan,
            "provider_returned": isinstance(quote, Mapping),
            "bid": self._diagnostic_price(value.get("bid")),
            "ask": self._diagnostic_price(value.get("ask")),
            "last": self._diagnostic_price(value.get("last")),
            "quote_source_timestamp_ms": self._diagnostic_time(value.get("timestamp_ms")),
            "observer_started_at_ms": self._diagnostic_time(started),
            "observer_completed_at_ms": self._diagnostic_time(completed),
        })

    def _observe_risk(self, snapshot, started, completed):
        """Store only diagnostic facts, never account/credential/raw payloads."""
        value = snapshot if isinstance(snapshot, Mapping) else {}
        summary = {name: value.get(name) if type(value.get(name)) is bool else None
                   for name in ("demo_verified", "baseline_trusted", "history_complete",
                                "ledger_verified", "account_snapshot_consistent",
                                "data_fresh", "macro_blackout")}
        summary.update({name: self._diagnostic_time(value.get(name))
                        for name in ("now_ms", "observed_at_ms", "collection_started_ms", "collection_completed_ms",
                                     "snapshot_window_start_ms", "snapshot_window_end_ms")})
        summary.update(observer_started_at_ms=started, observer_completed_at_ms=completed,
                       provider_returned=isinstance(snapshot, Mapping))
        reasons = value.get("refusal_reasons")
        summary["refusal_codes"] = [self._diagnostic_code(code) for code in reasons[:64]] if isinstance(reasons, list) else []
        faults = value.get("request_faults")
        summary["request_faults"] = []
        for fault in faults[:128] if isinstance(faults, list) else []:
            if not isinstance(fault, Mapping):
                continue
            number = fault.get("numeric")
            safe_fault = {
                "path": fault.get("path") if fault.get("path") in DIAGNOSTIC_PATHS else "UNKNOWN_SOURCE",
                "code": self._diagnostic_code(fault.get("code")),
                "numeric": number if type(number) is int and 0 <= number <= 99_999_999 else None,
                "source_fault": self._diagnostic_code(fault.get("source_fault")),
            }
            if fault.get("code") == "DEMO_NETWORK_ERROR" and "network_category" in fault:
                category = fault.get("network_category")
                safe_fault["network_category"] = (category if type(category) is str
                    and category in DIAGNOSTIC_NETWORK_CATEGORIES else "UNKNOWN")
            summary["request_faults"].append(safe_fault)
        timings = value.get("request_timings")
        summary["request_timings"] = [
            {"path": row.get("path") if row.get("path") in DIAGNOSTIC_PATHS else "UNKNOWN_SOURCE",
             "started_at_ms": self._diagnostic_time(row.get("started_at_ms")),
             "finished_at_ms": self._diagnostic_time(row.get("finished_at_ms"))}
            for row in (timings[:128] if isinstance(timings, list) else []) if isinstance(row, Mapping)]
        batches = value.get("account_get_batches")
        summary["account_get_batches"] = [
            {name: self._diagnostic_time(row.get(name)) for name in
             ("queued_at_ms", "earliest_get_started_ms", "finished_at_ms")}
            for row in (batches[:8] if isinstance(batches, list) else []) if isinstance(row, Mapping)]
        self._risk_observations.append(summary)
        if (summary["demo_verified"] is not True or summary["refusal_codes"]
                or summary["request_faults"]):
            self._last_failed_risk = summary

    def _retry_diagnostics(self):
        safe = []
        try:
            client = (self._runtime or {}).get("client")
            events = client.retry_events() if client is not None else []
            kinds = {"SSLEOFError", "TemporaryDNS", "TimeoutError", "ConnectionResetError",
                     "ConnectionAbortedError", "ConnectionRefusedError", "BrokenPipeError", "EOFError"}
            for event in events[-64:] if isinstance(events, list) else []:
                if (isinstance(event, Mapping) and event.get("path") in DIAGNOSTIC_PATHS
                        and event.get("code") == "DEMO_NETWORK_RETRY"
                        and type(event.get("attempt")) is int and event["attempt"] == 2
                        and event.get("type") in kinds):
                    safe.append({name: event[name] for name in ("path", "code", "attempt", "type")})
        except Exception:
            pass
        return {"network_retries": 1, "budget": "verified-provider-same-path",
                "binding_network_retries": 0, "route_read_network_retries": 0,
                "events": safe}

    def recover_owners(self, runtime):
        from src.demo_successor_gate import SuccessorGateRefused, assert_activated_generation
        from demo_auto_run import assert_service_recovery_ready
        root = self._runtime_root(runtime)
        try:
            assert_service_recovery_ready(root / "state" / "demo-auto")
            assert_activated_generation(root)
        except SuccessorGateRefused as error:
            raise TestRefused(error.code) from None
        except RuntimeError:
            raise TestRefused("DEMO_SERVICE_RECOVERY_REFUSED") from None
        return runtime["router"].recover_demo_owners()

    def reconcile(self, runtime):
        result = runtime["controller"].tick()
        if isinstance(result, dict):
            self._last = dict(result)
        return result

    def verify_terminal(self, runtime, signal_id):
        """只读核验本路由实际终态（不依赖口头 status）。"""
        path = Path(self._root) / "state" / "demo-auto" / "router.sqlite3"
        try:
            con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            try:
                row = con.execute("SELECT status,data FROM routes WHERE account=? AND signal=?",
                                  ("DEMO-AUTO-SOL-V1", signal_id)).fetchone()
            finally:
                con.close()
        except Exception:  # noqa: BLE001
            return False
        if row is None:
            return False
        status, raw = row
        if status not in {"CLOSED", "CANCELED"}:
            return False
        try:
            data = json.loads(raw)
        except ValueError:
            return False
        return (data.get("signal_id") == signal_id
                and data.get("status") == status
                and data.get("inst_id") == "SOL-USDT-SWAP")

    @staticmethod
    def _runtime_root(runtime):
        return Path(runtime["root"]) if "root" in runtime else ROOT

    def safety_halt(self, runtime, code):
        return runtime["controller"].safety_halt(code)

    def write_report(self, new_root, report, outcome):
        base = (Path(new_root).resolve() / "logs" / "technical-tests").resolve()
        run_id = report.get("run_id") or "unknown"
        if not run_id or not all(c.isalnum() or c in "_-" for c in str(run_id)):
            run_id = "unknown"
        run_dir = (base / run_id).resolve()
        if not run_dir.is_relative_to(base):
            raise TestRefused("REPORT_PATH_ESCAPE")
        run_dir.mkdir(parents=True, exist_ok=True)
        safe = _sanitize({**report, "risk_observations": list(self._risk_observations),
                          "last_failed_risk": self._last_failed_risk,
                          "initial_plan_quote": self._initial_plan_quote,
                          "entry_quote_observations": list(self._entry_quote_observations),
                          "get_retry_diagnostics": self._retry_diagnostics()})
        path = run_dir / "report.json"
        path.write_text(json.dumps(safe, ensure_ascii=False, indent=2), encoding="utf-8")
        lines = [f"purpose={safe.get('purpose')}", f"scenario={safe.get('scenario')}",
                 f"fixed_commit={safe.get('fixed_commit')}",
                 f"result={safe.get('result')}",
                 f"run_id={safe.get('run_id')}"]
        (run_dir / "report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        return {"result": outcome, "report_path": str(path)}


class ActivatedProductionDeps(ProductionDeps):
    """Executable technical tests use an activated generation, never migration."""

    @staticmethod
    def _activated(new_root):
        from src.demo_successor_gate import SuccessorGateRefused, assert_activated_generation
        try:
            return assert_activated_generation(new_root)
        except SuccessorGateRefused as error:
            raise TestRefused(error.code) from None

    def validate_roots(self, predecessor_root, new_root):
        new = Path(new_root).absolute()
        if new != ROOT:
            raise TestRefused("NEW_ROOT_MISMATCH")
        prepared = self._activated(new)
        if Path(predecessor_root).absolute() != prepared.source_root:
            raise TestRefused("SUCCESSOR_PREDECESSOR_MISMATCH")
        self._fixed_commit = self._verify_archive(new, prepared.target_commit, require_entry=True)
        return self._fixed_commit

    def check_predecessor(self, predecessor_root):
        # Full frozen source inventory/HALT/STOP checks belong to the common gate.
        if Path(predecessor_root).absolute() != self._activated(ROOT).source_root:
            raise TestRefused("SUCCESSOR_PREDECESSOR_MISMATCH")

    @contextlib.contextmanager
    def locks(self, predecessor_root, new_root):
        # Both lock files must already exist. Never initialize a source lock.
        roots = (Path(predecessor_root).absolute(), Path(new_root).absolute())
        if roots[0] == roots[1]:
            raise TestRefused("SAME_RUNTIME_ROOT")
        from src.demo_successor_gate import SuccessorGateRefused, owned_process_locks
        from src.demo_runtime_get import DemoRuntimeGetRefused
        from src.demo_transport import DemoTransportError
        try:
            with owned_process_locks(roots):
                self._activated(roots[1])
                with runtime_demo_get() as http_get:
                    self._http_get = http_get
                    try:
                        yield
                    finally:
                        self._http_get = None
        except SuccessorGateRefused as error:
            raise TestRefused(error.code) from None
        except (DemoRuntimeGetRefused, DemoTransportError) as error:
            raise TestRefused(error.code) from None

    def preserve_state(self, predecessor_root, new_root):
        # Preparation already atomically projected the journals and archived
        # history. A technical run only rechecks; it never migrates or clears.
        self.validate_roots(predecessor_root, new_root)
        sys.path.insert(0, str(CODE_ROOT / "scripts"))
        from demo_auto_run import assert_service_recovery_ready
        try:
            assert_service_recovery_ready(Path(new_root) / "state/demo-auto")
        except RuntimeError:
            raise TestRefused("DEMO_SERVICE_RECOVERY_REFUSED") from None


def _sanitize(payload):
    """去敏：不输出凭据/HTTP 头/账户哈希/授权字段。"""
    banned = ("key", "secret", "passphrase", "authorization", "header", "account_hash",
              "account_identity", "uid")
    if isinstance(payload, dict):
        out = {}
        for key, value in payload.items():
            if any(word in str(key).lower() for word in banned):
                continue
            out[key] = _sanitize(value)
        return out
    if isinstance(payload, list):
        return [_sanitize(item) for item in payload]
    return payload


def explain() -> int:
    print("=== DEMO 技术测试入口（默认只解释，零凭据/零请求）===")
    print("执行必须同时提供：--run-test --enable-demo-test --scenario smoke|swing")
    print("并给出 --predecessor-root（后继准备记录绑定的原事故 runtime）；新 generation 必须已受控激活。")
    print("默认 smoke 观察 30 秒、swing 观察 120 秒；离线测试可用 --allow-short-observation 缩短。")
    print("本入口只用于独立 DEMO；不读取实盘凭据、不改全局开关、不放宽风控。")
    print("smoke：最小 0.01 张、maker 入场、成交后立即 OCO、观察后 planned_close。")
    print("swing：同一链路的技术测试，不冒充自然波段策略或盈利样本。")
    print("报告脱敏；本地原始证据保留在独立 run 目录。")
    return 0


def main(argv=None, deps_factory=None) -> int:
    args = _parse(argv)
    if not args.run_test:
        return explain()
    if not args.enable_demo_test or not args.scenario or not args.predecessor_root:
        print("DEMO_TECH_REFUSED: 执行需要 --run-test --enable-demo-test --scenario --predecessor-root 同时给出")
        return 2
    default_observe = SCENARIOS[args.scenario]["observe_s"]
    if args.observe_seconds is None:
        args.observe_seconds = default_observe
    elif not args.allow_short_observation and args.observe_seconds < default_observe:
        print("DEMO_TECH_REFUSED: observe seconds below real-run minimum without --allow-short-observation")
        return 2
    if not 1 <= args.observe_seconds <= 600:
        print("DEMO_TECH_REFUSED: observe seconds out of bounds")
        return 2
    if not 1 <= args.entry_wait_seconds <= 600:
        print("DEMO_TECH_REFUSED: entry wait out of bounds")
        return 2
    try:
        ProductionDeps.validate_run_id(args.run_id)
    except TestRefused as exc:
        print(f"DEMO_TECH_REFUSED: {exc.code}")
        return 2
    factory = deps_factory or ActivatedProductionDeps
    try:
        deps = factory()
        if hasattr(deps, "run_id_hint"):
            deps.run_id_hint = args.run_id
        result = execute_test(deps, args)
    except TestRefused as exc:
        print(f"DEMO_TECH_REFUSED: {exc.code}")
        return 2
    return 0 if result.get("result") == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
