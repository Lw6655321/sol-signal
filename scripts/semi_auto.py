# -*- coding: utf-8 -*-
"""Half-automatic entry with strict live inputs and deterministic offline demos.

Offline mode never reads an account, credentials, or trading environment. Live
mode needs verified accounting and exact-card confirmation. No route identities
are deleted, and cleanup never expands into an account-wide flatten operation.
"""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.execution import ExecutionRejected, build_intent
from src.order_router import OrderRouter
from src.util import LOG_DIR, load_config, read_csv

INST = "SOL-USDT-SWAP"
TICKER_URL = "https://www.okx.com/api/v5/market/ticker?instId=" + INST
WAITING = {"PENDING", "RESERVED", "SUBMITTING", "PROTECTING"}
DEMO_NOW = 1792339200000
AUDIT_FIELDS = ("ts_utc", "mode", "signal_id", "decision", "route_status", "route_code", "run_dir")


class EntryRefused(ValueError):
    """Stable errors exclude API response bodies and exception text."""


def now_ms():
    return time.time_ns() // 1_000_000


def number(value):
    if value is None or isinstance(value, bool):
        raise EntryRefused("INVALID_NUMBER")
    try:
        value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise EntryRefused("INVALID_NUMBER") from None
    if not value.is_finite():
        raise EntryRefused("INVALID_NUMBER")
    return value


def trading_enabled():
    # Tests stub this function, never modify the real environment.
    return os.environ.get("OKX_TRADING_ENABLED") == "1"


def audit(result, directory, decision):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "semi_auto_audit.csv"
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(dict(ts_utc=datetime.now(timezone.utc).isoformat(), mode=decision,
            signal_id=result.signal_id, decision=decision, route_status=result.status,
            route_code=result.code, run_dir=str(result.run_dir)))


def fetch_market_snapshot():
    """The exchange source timestamp, never the local response receipt time."""
    from src.util import http_request
    try:
        payload = json.loads(http_request(TICKER_URL, timeout=15, retries=2).decode("utf-8"))
        row = payload["data"][0]
        last, stamp = number(row["last"]), number(row["ts"])
        if str(payload.get("code")) != "0" or row.get("instId") != INST:
            raise EntryRefused("MARKET_UNAVAILABLE")
        if last <= 0 or stamp <= 0 or stamp != stamp.to_integral_value():
            raise EntryRefused("MARKET_UNAVAILABLE")
        return {"last": str(last), "timestamp_ms": int(stamp)}
    except Exception:
        raise EntryRefused("MARKET_UNAVAILABLE") from None


def load_live_state(cfg):
    """No fallback to old signal-mode counters or invented macro clearance."""
    try:
        from src.live_state import live_state_provider
        state = dict(live_state_provider(cfg))
        required = {"equity_estimate", "day_start_equity", "realized_pnl_today", "entries_today",
            "open_positions", "pending_orders", "data_fresh", "macro_blackout", "halted",
            "now_ms", "observed_at_ms", "live_verified"}
        if not required.issubset(state) or state["live_verified"] is not True:
            raise EntryRefused("LIVE_STATE_UNVERIFIED")
        if any(type(state[k]) is not bool for k in ("data_fresh", "macro_blackout", "halted")):
            raise EntryRefused("LIVE_STATE_UNVERIFIED")
        if any(not isinstance(state[k], (list, tuple)) for k in ("open_positions", "pending_orders")):
            raise EntryRefused("LIVE_STATE_UNVERIFIED")
        stamp = number(state["observed_at_ms"])
        if stamp != stamp.to_integral_value() or not 0 <= now_ms() - int(stamp) <= 5000:
            raise EntryRefused("LIVE_STATE_STALE")
        return state
    except EntryRefused:
        raise
    except Exception:
        raise EntryRefused("LIVE_STATE_UNAVAILABLE") from None


def signal_from_csv(signal_id):
    for row in read_csv(LOG_DIR / "signals.csv"):
        if row.get("id") == signal_id:
            return {"id": row["id"], "instId": INST, "kind": row.get("kind"),
                "side": row.get("side"), "entry": row.get("entry"), "stop": row.get("stop"),
                "tp1": row.get("tp1"), "tp2": row.get("tp2"),
                "generated_at_utc": row.get("generated_at_utc"), "expires_at_utc": row.get("expires_at_utc")}
    return None


def render_card(sig, intent, equity, live):
    print("\n" + "=" * 62)
    print("真实交易确认卡" if live else "离线模拟确认卡（固定数据）")
    print(f"信号 {sig['id']} | {intent.side} | {intent.inst_id}")
    print(f"入场 {intent.entry} | 止损 {intent.stop} | 止盈 {intent.tp1}")
    print(f"数量 {intent.qty} 张 | 名义 {intent.notional} USDT | 权益 {equity}")
    print(f"止损风险 {intent.effective_risk:.4f} USDT | 扣费滑后 RR {intent.net_rr:.2f}")
    print(f"maker={intent.require_maker} | 截止 {intent.expires_at_ms} ms UTC")
    print("=" * 62)


def confirm_exact_card(intent, *, live, auto_confirm=False):
    if live:
        expected = "CONFIRM " + intent.signal_id
        return input(f"核对以上参数后输入 {expected}：").strip() == expected
    return auto_confirm or input("确认离线模拟？[y/N] ").strip().lower() in {"y", "yes"}


def apply_risk_reduction(cfg, requested):
    result = copy.deepcopy(cfg)
    if requested is not None:
        value = number(requested)
        if value <= 0 or value > number(cfg["risk"]["risk_per_trade_pct"]):
            raise EntryRefused("RISK_INCREASE_FORBIDDEN")
        result["risk"]["risk_per_trade_pct"] = float(value)
    return result


def execute_and_watch(router, intent, watch_seconds, *, monotonic=time.monotonic, sleep=time.sleep):
    """Cancel and confirm via the router; a cancellation ACK alone is insufficient."""
    if not math.isfinite(watch_seconds) or watch_seconds < 0:
        raise EntryRefused("WATCH_SECONDS_INVALID")
    try:
        result = router.execute(intent)
        deadline = monotonic() + watch_seconds
        while result.status in WAITING and monotonic() < deadline:
            sleep(min(2.0, max(0.0, deadline - monotonic())))
            result = router.execute(intent)
        return router.cancel_and_reconcile(intent) if result.status in WAITING else result
    except BaseException:
        try:
            result = router.cancel_and_reconcile(intent)
            print(f"中断清理：{result.status} / {result.code}")
        except Exception:
            print("清理未确认；请核对该信号的入场单、持仓与保护单。")
        raise


def read_live_exposure():
    from src import okx_private as private
    from src.okx_native import pending_algos
    return {"positions": private.positions(INST), "pending": private.pending_orders(INST),
        "algos": [a for kind in ("oco", "conditional", "trigger") for a in pending_algos(INST, kind)]}


def verify_live_result(router, intent, result):
    """Read-only postflight after the router has canceled or verified protection."""
    try:
        exposure = read_live_exposure()
        positions = [p for p in exposure["positions"] if number(p["pos"]) != 0]
        if exposure["pending"]:
            raise EntryRefused("POSTFLIGHT_PENDING")
        if result.status == "PROTECTED":
            expected = result.filled_qty if intent.side == "long" else -result.filled_qty
            if (len(positions) != 1 or positions[0].get("instId") != INST
                or positions[0].get("posSide") != "net" or number(positions[0]["pos"]) != expected
                or result.filled_qty <= 0 or result.protected_qty != result.filled_qty):
                raise EntryRefused("POSTFLIGHT_POSITION")
            protection_id = "r" + hashlib.sha256((intent.signal_id + ":protection").encode()).hexdigest()[:31]
            p = router.transport.get_protection(INST, protection_id)
            if not isinstance(p, Mapping):
                raise EntryRefused("POSTFLIGHT_PROTECTION")
            if (p.get("algoClOrdId") != protection_id or not p.get("algoId") or p.get("instId") != INST
                or p.get("state") != "live" or p.get("ordType") != "oco" or p.get("posSide") != "net"
                or p.get("side") != ("sell" if intent.side == "long" else "buy")
                or str(p.get("reduceOnly", "")).lower() != "true"
                or str(p.get("slOrdPx")) != "-1" or str(p.get("tpOrdPx")) != "-1"
                or number(p.get("sz")) != result.filled_qty
                or number(p.get("slTriggerPx")) != intent.stop or number(p.get("tpTriggerPx")) != intent.tp1):
                raise EntryRefused("POSTFLIGHT_PROTECTION")
            if (len(exposure["algos"]) != 1
                or str(exposure["algos"][0].get("algoId")) != str(p["algoId"])):
                raise EntryRefused("POSTFLIGHT_EXTRA_PROTECTION")
        elif result.status in {"CANCELED", "CLOSED"} or result.emergency_status == "FLAT_CONFIRMED":
            if positions or exposure["algos"]:
                raise EntryRefused("POSTFLIGHT_NOT_FLAT")
        else:
            raise EntryRefused("POSTFLIGHT_UNCONFIRMED")
    except EntryRefused:
        raise
    except Exception:
        raise EntryRefused("POSTFLIGHT_UNAVAILABLE") from None


def halt_local(code):
    directory = ROOT / "state"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "HALT").write_text(code + "\n", encoding="utf-8")


def postflight_or_cleanup(router, intent, result):
    try:
        verify_live_result(router, intent, result)
    except EntryRefused as error:
        # A bracket may disappear between router verification and this read.
        # Re-enter only the existing route so its normal protection/emergency
        # protocol handles any confirmed fill; never flatten unrelated orders.
        cleanup = None
        verified = False
        try:
            cleanup = router.cancel_and_reconcile(intent)
            verify_live_result(router, intent, cleanup)
            verified = True
        except Exception:
            pass
        halt_local(str(error))
        if verified:
            print(f"该路由清理后已独立核对：{cleanup.status}；系统保持暂停。")
        else:
            print("该路由清理结果未确认；系统保持暂停，请立即核对持仓与保护单。")
        raise


class MockTransport:
    """Exchange-shaped fixture; this class has no native/account dependencies."""
    mode = "mock"

    def __init__(self, *, delay_reads=0, cancel_race=False):
        self.orders, self.protections, self.events = {}, {}, []
        self.delay_reads, self.cancel_race, self.reads = delay_reads, cancel_race, 0
        self.intent = None

    def submit_entry(self, intent, cl_ord_id):
        self.events.append("submit")
        self.intent = intent
        self.orders[cl_ord_id] = dict(instId=intent.inst_id, clOrdId=cl_ord_id,
            side="buy" if intent.side == "long" else "sell", posSide="net",
            ordType="post_only" if intent.require_maker else "limit", state="live", sz=str(intent.qty),
            accFillSz="0", avgPx="", ordId="offline-entry")
        return {"sCode": "0", "ordId": "offline-entry"}

    def get_order(self, inst_id, cl_ord_id):
        row = self.orders.get(cl_ord_id)
        if row and row["state"] == "live":
            self.reads += 1
            if self.reads > self.delay_reads:
                row.update(state="filled", accFillSz=str(self.intent.qty), avgPx=str(self.intent.entry))
                self.events.append("fill")
        return dict(row) if row else None

    def cancel_entry(self, inst_id, cl_ord_id):
        self.events.append("cancel")
        row = self.orders.get(cl_ord_id)
        if row and row["state"] == "live":
            if self.cancel_race:
                row.update(state="filled", accFillSz=str(self.intent.qty), avgPx=str(self.intent.entry))
                self.events.append("fill_during_cancel")
            else:
                row["state"] = "canceled"
        return {"sCode": "0"}

    def set_protection(self, intent, qty, protection_id, *, reduce_only):
        assert reduce_only is True
        self.events.append("protect")
        self.protections[protection_id] = dict(instId=intent.inst_id, algoClOrdId=protection_id,
            algoId="offline-oco", state="live", ordType="oco", posSide="net",
            side="sell" if intent.side == "long" else "buy", sz=str(qty), slTriggerPx=str(intent.stop),
            tpTriggerPx=str(intent.tp1), slOrdPx="-1", tpOrdPx="-1", reduceOnly="true")
        return {"sCode": "0", "algoId": "offline-oco"}

    def get_protection(self, inst_id, protection_id):
        return self.protections.get(protection_id)

    def emergency_close(self, inst_id, side, qty, cl_ord_id, *, reduce_only):
        assert reduce_only is True
        self.events.append("close")
        self.orders[cl_ord_id] = dict(instId=inst_id, clOrdId=cl_ord_id, ordId="offline-close",
            side=side, posSide="net", state="filled", ordType="market", sz=str(qty),
            accFillSz=str(qty), avgPx="100", reduceOnly="true")
        return {"sCode": "0", "ordId": "offline-close"}


def run_offline_demo(scenario, log_root, *, cfg=None, interactive=False):
    if scenario not in {"delayed-fill", "timeout", "cancel-race", "rejected"}:
        raise EntryRefused("DEMO_SCENARIO")
    cfg = copy.deepcopy(cfg if cfg is not None else load_config())
    tick = [DEMO_NOW]
    state = dict(now_ms=tick[0], equity_estimate="100", day_start_equity="100",
        realized_pnl_today="0", entries_today=0, open_positions=[], pending_orders=[],
        data_fresh=True, macro_blackout=False, halted=False, slippage_bps="5")
    signal = dict(id="OFFLINE-" + scenario, instId=INST, kind="swing_1h4h", side="long",
        entry="100", stop="98.9", tp1="102.5", tp2="103.8",
        generated_at_ms=tick[0] - 1000, expires_at_ms=tick[0] + 180000)
    directory = Path(log_root) / ("offline-" + scenario + "-" + uuid.uuid4().hex)
    transport = MockTransport(delay_reads=1 if scenario == "delayed-fill" else 10**9,
        cancel_race=scenario == "cancel-race")
    router = OrderRouter(transport, config=cfg, state_provider=lambda: dict(state, now_ms=tick[0]),
        market_provider=lambda: {"last": "100", "timestamp_ms": tick[0]}, log_root=directory,
        clock=lambda: tick[0], account_key="offline-demo")
    intent = build_intent(signal, state, cfg)
    render_card(signal, intent, state["equity_estimate"], False)
    if interactive and not confirm_exact_card(intent, live=False):
        directory.mkdir(parents=True, exist_ok=True)
        summary = dict(scenario=scenario, steps=["REJECTED"], status="REJECTED",
            code="OPERATOR_REJECTED", filled="0", protected="0", events=[], run_dir=str(directory))
        (directory / "operator_decision.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print("已拒绝，未提交模拟入场单。")
        print(json.dumps(summary, ensure_ascii=False))
        return summary
    if scenario == "rejected":
        state["entries_today"] = int(cfg["risk"]["max_entries_per_day"])
    first = router.execute(intent)
    steps, result = [first.status], first
    if result.status == "PENDING":
        tick[0] += 2000
        result = router.execute(intent) if scenario == "delayed-fill" else router.cancel_and_reconcile(intent)
        steps.append(result.status)
    audit(result, directory, "offline_fixed_fixture")
    summary = dict(scenario=scenario, steps=steps, status=result.status, code=result.code,
        filled=str(result.filled_qty), protected=str(result.protected_qty), events=transport.events,
        run_dir=str(directory))
    print(json.dumps(summary, ensure_ascii=False))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline-demo", action="store_true")
    parser.add_argument("--interactive-demo", action="store_true", help="固定离线卡片，亲手y/N确认后才模拟提交")
    parser.add_argument("--demo", action="store_true", help="--offline-demo 的别名")
    parser.add_argument("--scenario", choices=("all", "delayed-fill", "timeout", "cancel-race", "rejected"), default="all")
    parser.add_argument("--signal-id")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--smoke", action="store_true", help="此入口已关闭冒烟旁路")
    parser.add_argument("--yes", action="store_true", help="不会绕过live逐单确认")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--watch-seconds", type=float, default=180.0)
    parser.add_argument("--risk-pct", type=float, help="仅允许降低配置风险")
    parser.add_argument("--log-root", type=Path, default=LOG_DIR)
    args = parser.parse_args(argv)
    if args.smoke or (args.live and (args.demo or args.offline_demo or args.interactive_demo)):
        print("拒绝：live演示/冒烟旁路已关闭。")
        return 2
    if not args.live:
        if not (args.demo or args.offline_demo or args.interactive_demo):
            parser.error("请选择 --offline-demo；真实模式需 --live --signal-id")
        if args.interactive_demo and args.scenario not in {"all", "delayed-fill"}:
            print("交互演示使用 delayed-fill 固定场景。")
            return 2
        scenarios = (("delayed-fill",) if args.interactive_demo else
            (("delayed-fill", "timeout", "cancel-race", "rejected") if args.scenario == "all" else (args.scenario,)))
        for scenario in scenarios:
            run_offline_demo(scenario, args.log_root, interactive=args.interactive_demo)
        return 0
    if not trading_enabled() or not args.signal_id:
        print("拒绝：live需要既有交易开关与真实signal-id。")
        return 2
    try:
        cfg = apply_risk_reduction(load_config(), args.risk_pct)
        state = load_live_state(cfg)
        signal = signal_from_csv(args.signal_id)
        if signal is None:
            raise EntryRefused("SIGNAL_NOT_FOUND")
        intent = build_intent(signal, state, cfg)
        render_card(signal, intent, state["equity_estimate"], True)
        if args.dry_run:
            return 0
        if not confirm_exact_card(intent, live=True, auto_confirm=args.yes):
            print("已取消，未提交。")
            return 0
        from src.okx_native import NativeOkxTransport
        router = OrderRouter(NativeOkxTransport(), config=cfg,
            state_provider=lambda: load_live_state(cfg), market_provider=fetch_market_snapshot,
            log_root=args.log_root, account_key="T3-live")
        result = execute_and_watch(router, intent, args.watch_seconds)
        postflight_or_cleanup(router, intent, result)
        audit(result, result.run_dir, "confirmed_exact_live_card")
        print(f"路由结果：{result.status} / {result.code}；成交{result.filled_qty}，保护{result.protected_qty}")
        return 0 if result.status in {"PROTECTED", "CANCELED", "CLOSED"} else 3
    except (EntryRefused, ExecutionRejected) as error:
        print("拒绝：" + (error.code if isinstance(error, ExecutionRejected) else str(error)))
        return 2
    except KeyboardInterrupt:
        halt_local("OPERATOR_INTERRUPTED_RECONCILE_REQUIRED")
        print("已中断；停止新增交易并核对该路由终态。")
        return 130
    except Exception:
        halt_local("ENTRY_FAILURE_RECONCILE_REQUIRED")
        print("执行失败；已暂停新增交易，请核对该路由。")
        return 3


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
