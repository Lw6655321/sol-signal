# -*- coding: utf-8 -*-
"""T4 对账循环：定期核对 OKX 实盘与本地台账，异常自动熔断；空仓时同步实时权益。

用法：
  python scripts/reconcile_loop.py --once              # 对账一次（适合开机自检）
  python scripts/reconcile_loop.py --interval 60       # 常驻，每 60 秒一次
  python scripts/reconcile_loop.py --once --no-toast   # 不弹桌面通知

安全边界：
  - 只读 OKX 接口（positions / pending / algo / balance）
  - 只写 state/ 与 logs/，绝不下单、绝不改账户设置
  - 发现异常 → 写 state/HALT（熔断），需要人工确认后用 --resume 解除

检查项：
  1. OKX 有持仓、本地台账没有 → 未知持仓（HALT）
  2. 本地台账有持仓、OKX 空仓 → 幽灵持仓（HALT）
  3. 两边都有持仓但数量差异 > 0 → 不一致（HALT）
  4. 有持仓但查不到任何止损/止盈保护单 → 裸单（HALT）
  5. 空仓且无挂单 → 把 OKX 实时权益同步进 risk_state.equity_estimate
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import notify, okx_private as P, risk          # noqa: E402
from src.util import LOG_DIR, load_config, write_json    # noqa: E402

INST = "SOL-USDT-SWAP"
RECON_LOG = LOG_DIR / "reconcile.log"
EPS = 1e-7


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _audit(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(RECON_LOG, "a", encoding="utf-8") as f:
        f.write(f"{_ts()} {msg}\n")


def _toast(title: str, message: str, enabled: bool) -> None:
    if not enabled:
        return
    try:
        notify.toast(title, message)
    except Exception as exc:                            # noqa: BLE001
        _audit(f"TOAST_FAILED {exc}")


def _protect_orders(positions: list[dict]) -> list[dict]:
    """止损/止盈可能挂在两处：仓位级 closeOrderAlgo，或策略委托。"""
    found: list[dict] = []
    for p in positions:
        for a in (p.get("closeOrderAlgo") or []):
            found.append({"src": "position_oco", "sl": a.get("slTriggerPx"),
                          "tp": a.get("tpTriggerPx")})
    for kind in ("conditional", "oco", "trigger"):
        try:
            for o in P.algo_orders(kind):
                found.append({"src": f"algo/{kind}", "sl": o.get("slTriggerPx"),
                              "tp": o.get("tpTriggerPx")})
        except P.PrivateError as exc:
            _audit(f"ALGO_QUERY_FAILED {kind}: {exc}")
    return found


def check_once(cfg: dict, toast: bool = True) -> tuple[bool, str]:
    st = risk.load_state(cfg)
    ledger_pos = st.get("open_positions") or []
    try:
        positions = [p for p in P.positions(INST) if abs(float(p.get("pos") or 0)) > EPS]
        pending = P.pending_orders(INST)
        protects = _protect_orders(positions)
        bal = P.balance("USDT")
        equity = float(bal[0].get("totalEq")) if bal else 0.0
    except P.PrivateError as exc:
        _audit(f"READ_FAILED {exc}")
        return False, f"读取 OKX 失败：{exc}"

    issues: list[str] = []
    okx_qty = sum(abs(float(p.get("pos") or 0)) for p in positions)
    ledger_qty = sum(float(p.get("qty") or 0) for p in ledger_pos)

    if positions and not ledger_pos:
        issues.append(f"OKX 有持仓 {okx_qty:g} 张，但本地台账为 0（未知持仓）")
    elif ledger_pos and not positions:
        issues.append(f"本地台账有 {len(ledger_pos)} 个持仓，但 OKX 空仓（幽灵持仓）")
    elif positions and ledger_pos and abs(okx_qty - ledger_qty) > EPS:
        issues.append(f"持仓数量不一致：OKX {okx_qty:g} 张 vs 台账 {ledger_qty:g} 张")
    if positions and not protects:
        issues.append("有持仓但查不到任何止损/止盈保护单（裸单）")

    if issues:
        msg = "；".join(issues)
        risk.halt(f"对账异常：{msg}")
        _audit(f"MISMATCH {msg}")
        _toast("⚠ 对账异常，已熔断", msg, toast)
        return False, f"❌ {msg} → 已写 HALT"

    if not positions and not pending and not protects:
        old = float(st.get("equity_estimate") or 0)
        st["equity_estimate"] = equity
        write_json(risk.STATE_PATH, st)
        if abs(equity - old) > 0.005:
            _audit(f"EQUITY_SYNC {old:.4f} -> {equity:.4f}")
        return True, (f"✅ 空仓一致；权益 {equity:.4f} u"
                      + (f"（台账原 {old:.4f} → 已同步）" if abs(equity - old) > 0.005 else "（台账一致）"))

    return True, (f"✅ 一致；OKX 持仓 {len(positions)} / 普通挂单 {len(pending)} / "
                  f"保护单 {len(protects)}；权益 {equity:.4f} u")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只对账一次")
    ap.add_argument("--interval", type=float, default=60.0, help="常驻间隔（秒）")
    ap.add_argument("--no-toast", action="store_true", help="不弹桌面通知")
    ap.add_argument("--quiet", action="store_true", help="常驻时不重复打印成功行")
    args = ap.parse_args()

    cfg = load_config()
    toast = not args.no_toast
    print(f"对账循环启动 [{_ts()}]  标的 {INST}  间隔 {args.interval}s  "
          f"（只读账户，异常自动熔断）")

    last_ok: bool | None = None
    while True:
        ok, msg = check_once(cfg, toast=toast)
        if args.once or not args.quiet or not ok or last_ok is not ok:
            print(f"[{_ts()}] {msg}")
        last_ok = ok
        if args.once:
            return 0 if ok else 1
        if not ok:
            # 熔断后仍然继续监控（人工处理后 --resume）
            pass
        time.sleep(max(5.0, args.interval))


if __name__ == "__main__":
    sys.exit(main())
