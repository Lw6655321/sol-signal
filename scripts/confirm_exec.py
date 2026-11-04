# -*- coding: utf-8 -*-
"""T3 半自动确认 UI（mock 阶段）：信号 -> 执行卡 -> 人工确认 -> mock 路由执行。

安全边界（非常重要）：
  - 本脚本 **绝不** 设置 OKX_TRADING_ENABLED，**绝不** 调用真实下单接口；
  - 只读 OKX（持仓/挂单/余额/公开行情）；
  - "确认执行"后走的是 **MockTransport**（内存模拟撮合+保护单），只写本地审计；
  - 真实下单要等 T2B 原生适配器 + T6 模拟盘验收 + 用户书面授权。

用法：
  python scripts/confirm_exec.py --demo --dry-run     # 看执行卡，不询问
  python scripts/confirm_exec.py --demo               # 执行卡 + y/N 确认（mock 执行）
  python scripts/confirm_exec.py --demo --yes         # 自动确认（仅用于自动化测试）
  python scripts/confirm_exec.py --signal-id XXX      # 查看某条真实历史信号（过期会如实拒绝）

演示模式（--demo）会用当前 SOL 最新价构造一条"形状合法"的演示信号（非真实信号），
用于验证确认界面与 mock 链路；真实信号回放会被执行层的过期/风控闸门拒绝，属预期。
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P, risk                     # noqa: E402
from src.execution import ExecutionRejected, build_intent  # noqa: E402
from src.order_router import OrderRouter                   # noqa: E402
from src.util import LOG_DIR, load_config, read_csv        # noqa: E402

INST = "SOL-USDT-SWAP"
TICKER_URL = "https://www.okx.com/api/v5/market/ticker?instId=" + INST
AUDIT = LOG_DIR / "confirm_audit.csv"
AUDIT_FIELDS = ["ts_utc", "mode", "signal_id", "side", "entry", "stop", "tp1", "qty",
                "notional", "decision", "route_status", "route_code", "run_dir"]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def fmt(value, digits: int = 2) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def audit(row: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    exists = AUDIT.exists() and AUDIT.stat().st_size > 0
    with open(AUDIT, "a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=AUDIT_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in AUDIT_FIELDS})


# --------------------------------------------------------------------------- 行情 / 账户


def fetch_last_price() -> tuple[float, int]:
    """公开行情，无需鉴权。返回 (last, 服务端时间ms近似)。"""
    from src.util import http_request
    raw = http_request(TICKER_URL, timeout=15, retries=2)
    import json
    payload = json.loads(raw.decode("utf-8"))
    if str(payload.get("code")) != "0" or not payload.get("data"):
        raise RuntimeError(f"ticker failed: {payload.get('msg')}")
    row = payload["data"][0]
    return float(row["last"]), now_ms()


def read_only_account_state() -> tuple[float, list, list]:
    """只读账户：返回 (总权益, 非零持仓, 普通挂单)。"""
    bal = P.balance("USDT")
    equity = float(bal[0].get("totalEq")) if bal else 0.0
    positions = [p for p in P.positions(INST) if abs(float(p.get("pos") or 0)) > 0]
    pending = P.pending_orders(INST)
    return equity, positions, pending


def build_state(cfg: dict, equity: float, fresh: bool) -> dict:
    st = risk.load_state(cfg)
    return {
        "now_ms": now_ms(),
        "equity_estimate": f"{equity:.4f}",
        "day_start_equity": f"{float(st.get('day_start_equity') or equity):.4f}",
        "realized_pnl_today": str(st.get("realized_pnl_today", 0)),
        "entries_today": int(st.get("entries_today", 0)),
        "open_positions": [],
        "pending_orders": [],
        "data_fresh": bool(fresh),
        "macro_blackout": False,          # 演示：不做宏观窗口判断（真实执行会由调用方提供）
        "halted": bool(st.get("halted")) or risk.HALT_FILE.exists(),
        "paused_until_ms": int(st.get("paused_until_ms", 0) or 0),
        "slippage_bps": "5",              # 演示假设 5bps 滑点
        "seen_signal_ids": [],
    }


def demo_signal(price: float) -> dict:
    """构造形状合法的演示信号（swing 1H/4H，止损 1.2%，TP1 +2.4%，TP2 +3.6%）。"""
    t = now_ms()
    entry = round(price, 2)
    stop = round(entry * (1 - 0.012), 2)
    tp1 = round(entry * (1 + 0.024), 2)
    tp2 = round(entry * (1 + 0.036), 2)
    return {
        "id": f"DEMO-T3-{t}", "instId": INST, "kind": "swing_1h4h", "side": "long",
        "entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2,
        "generated_at_ms": t - 1000, "expires_at_ms": t + 180_000,
    }


def signal_from_csv(signal_id: str) -> dict | None:
    for row in read_csv(LOG_DIR / "signals.csv"):
        if row.get("id") == signal_id:
            return {
                "id": row["id"], "instId": INST, "kind": row.get("kind"), "side": row.get("side"),
                "entry": row.get("entry"), "stop": row.get("stop"),
                "tp1": row.get("tp1"), "tp2": row.get("tp2"),
                "generated_at_utc": row.get("generated_at_utc"),
                "expires_at_utc": row.get("expires_at_utc"),
            }
    return None


# --------------------------------------------------------------------------- mock 传输


class MockTransport:
    """纯内存模拟：即时全成 + 保护单验证，不触网、不碰真实账户。"""

    mode = "mock"

    def __init__(self) -> None:
        self.orders: dict[str, dict] = {}
        self.protections: dict[str, dict] = {}
        self._seq = 0

    def _next(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}{self._seq:04d}"

    def submit_entry(self, intent, cl_ord_id: str):
        ord_id = self._next("mock-ord-")
        self.orders[cl_ord_id] = {
            "instId": intent.inst_id, "clOrdId": cl_ord_id, "side":
            "buy" if intent.side == "long" else "sell",
            "ordType": "post_only" if intent.require_maker else "limit",
            "posSide": "net", "state": "filled", "sz": str(intent.qty),
            "accFillSz": str(intent.qty), "avgPx": str(intent.entry), "ordId": ord_id,
        }
        return {"sCode": "0", "ordId": ord_id, "msg": "mock filled"}

    def get_order(self, inst_id: str, cl_ord_id: str):
        return self.orders.get(cl_ord_id)

    def cancel_entry(self, inst_id: str, cl_ord_id: str):
        return {"sCode": "0", "msg": "mock cancel"}

    def set_protection(self, intent, qty, protection_id: str, *, reduce_only: bool):
        algo_id = self._next("mock-algo-")
        self.protections[protection_id] = {
            "instId": intent.inst_id, "algoClOrdId": protection_id, "algoId": algo_id,
            "state": "live", "ordType": "oco", "side": "sell" if intent.side == "long" else "buy",
            "posSide": "net", "sz": str(qty), "slTriggerPx": str(intent.stop),
            "tpTriggerPx": str(intent.tp1), "slOrdPx": "-1", "tpOrdPx": "-1",
            "reduceOnly": "true" if reduce_only else "false",
        }
        return {"sCode": "0", "algoId": algo_id, "msg": "mock protected"}

    def get_protection(self, inst_id: str, protection_id: str):
        return self.protections.get(protection_id)

    def emergency_close(self, inst_id: str, side: str, qty, cl_ord_id: str, *, reduce_only: bool):
        ord_id = self._next("mock-close-")
        self.orders[cl_ord_id] = {
            "instId": inst_id, "clOrdId": cl_ord_id, "side": side, "ordType": "market",
            "posSide": "net", "state": "filled", "sz": str(qty), "accFillSz": str(qty),
            "avgPx": "0", "ordId": ord_id, "reduceOnly": "true",
        }
        return {"sCode": "0", "ordId": ord_id}


# --------------------------------------------------------------------------- 执行卡 / 主流程


def render_card(sig: dict, intent, equity: float) -> None:
    direction = "做多 LONG" if sig["side"] == "long" else "做空 SHORT"
    print()
    print("=" * 72)
    print("  ★ 半自动执行确认卡（mock 阶段，不会下真实单）")
    print("=" * 72)
    print(f"  信号      : {sig['id']}   ({sig.get('kind')})")
    print(f"  方向      : {direction}")
    print(f"  入场      : {fmt(intent.entry)}（限价，{'maker' if intent.require_maker else 'taker'}）")
    print(f"  止损      : {fmt(intent.stop)}  "
          f"（{abs(float(intent.stop) / float(intent.entry) - 1) * 100:.2f}%）")
    print(f"  止盈 TP1  : {fmt(intent.tp1)}   TP2: {fmt(intent.tp2)}")
    print(f"  手数      : {intent.qty} 张   名义 {fmt(intent.notional)} U")
    print(f"  单笔风险  : {fmt(intent.effective_risk)} U（预算 {fmt(intent.risk_budget)} U，"
          f"占权益 {float(intent.effective_risk) / equity * 100:.2f}%）")
    print(f"  盈亏比    : 毛 {fmt(intent.rr)} / 扣费滑后 {fmt(intent.net_rr)}")
    print(f"  有效时间  : 至 {datetime.fromtimestamp(intent.expires_at_ms / 1000, tz=timezone.utc).strftime('%H:%M:%S')} UTC")
    print("=" * 72)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="用当前价构造演示信号")
    ap.add_argument("--signal-id", help="回放 logs/signals.csv 里的某条真实历史信号")
    ap.add_argument("--yes", action="store_true", help="自动确认（仅自动化测试用）")
    ap.add_argument("--dry-run", action="store_true", help="只显示执行卡，不询问")
    args = ap.parse_args()

    cfg = load_config()

    # 0) 熔断 / 裸仓检查（只读）
    if risk.HALT_FILE.exists():
        print(f"⛔ 系统处于熔断状态（state/HALT）：{risk.HALT_FILE.read_text(encoding='utf-8').strip()}")
        print("   请先处理并 run.cmd --resume 后再试。")
        return 1
    equity, positions, pending = read_only_account_state()
    if positions or pending:
        print(f"⛔ 账户非空仓（持仓 {len(positions)} / 挂单 {len(pending)}），"
              f"半自动只允许在空仓时开新仓。")
        return 1

    # 1) 行情与数据新鲜度（公开行情，无需鉴权）
    try:
        price, tick_ts = fetch_last_price()
    except Exception as exc:                                  # noqa: BLE001
        print(f"⛔ 行情获取失败：{exc}")
        return 1
    fresh = abs(now_ms() - tick_ts) < 60_000
    print(f"最新价 {price:.2f}  USDT（{utc_now()}，数据{'新鲜' if fresh else '过期'}）")

    # 2) 信号
    if args.signal_id:
        sig = signal_from_csv(args.signal_id)
        if sig is None:
            print(f"⛔ 找不到信号 {args.signal_id}")
            return 1
    elif args.demo:
        sig = demo_signal(price)
        print("ℹ️ 演示信号（基于当前价构造，仅用于验证确认流程，不是真实交易信号）")
    else:
        print("用法：--demo 演示 / --signal-id 回放历史信号；见脚本头注释。")
        return 1

    # 3) 生成执行意图（执行层风控闸门）
    state = build_state(cfg, equity, fresh)
    try:
        intent = build_intent(sig, state, cfg)
    except ExecutionRejected as exc:
        print(f"⛔ 执行层拒绝：{exc.code} — {exc.reason}")
        audit({"ts_utc": utc_now(), "mode": "mock", "signal_id": sig.get("id", ""),
               "side": sig.get("side", ""), "decision": "rejected_by_risk",
               "route_code": exc.code})
        return 2

    render_card(sig, intent, equity)

    if args.dry_run:
        print("（dry-run：未询问确认，未执行）")
        return 0

    answer = "y" if args.yes else input("确认执行？[y/N] ").strip().lower()
    if answer not in ("y", "yes"):
        print("已拒绝，未执行任何操作。")
        audit({"ts_utc": utc_now(), "mode": "mock", "signal_id": sig["id"], "side": sig["side"],
               "entry": str(intent.entry), "stop": str(intent.stop), "tp1": str(intent.tp1),
               "qty": str(intent.qty), "notional": str(intent.notional),
               "decision": "rejected_by_operator"})
        return 0

    # 4) mock 路由执行（状态机 + 幂等 + 保护单验证，全部离线）
    transport = MockTransport()
    router = OrderRouter(
        transport=transport, config=cfg,
        state_provider=lambda: state,
        market_provider=lambda: {"last": str(price), "timestamp_ms": tick_ts},
        log_root=str(LOG_DIR), account_key="T3-mock",
    )
    result = router.execute(intent)
    print()
    print(f"✅ mock 执行结果：status={result.status}  code={result.code}  "
          f"filled={result.filled_qty}  protected={result.protected_qty}  "
          f"emergency={result.emergency_status or '-'}")
    print(f"   审计目录：{result.run_dir}")
    audit({"ts_utc": utc_now(), "mode": "mock", "signal_id": sig["id"], "side": sig["side"],
           "entry": str(intent.entry), "stop": str(intent.stop), "tp1": str(intent.tp1),
           "qty": str(intent.qty), "notional": str(intent.notional),
           "decision": "accepted_by_operator", "route_status": result.status,
           "route_code": result.code, "run_dir": str(result.run_dir)})
    print("⚠️ 以上为 mock 模拟，没有任何真实委托发送到交易所。")
    return 0 if result.status == "PROTECTED" else 3


if __name__ == "__main__":
    sys.exit(main())
