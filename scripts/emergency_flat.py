# -*- coding: utf-8 -*-
"""一键全平（真实账户）：撤销所有普通委托 + 策略委托，然后 reduceOnly 市价平掉持仓。

安全：
  - dry-run（默认）：只读，打印当前持仓/挂单/策略委托，不做任何修改；
  - --yes：仍需 `OKX_TRADING_ENABLED=1` 才会真正执行；
  - 只允许 SOL-USDT-SWAP；所有写操作写 logs/native_orders.csv 审计。

用法：
  python scripts/emergency_flat.py             # 只读预览
  python scripts/emergency_flat.py --yes       # 真执行（需 env OKX_TRADING_ENABLED=1）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P               # noqa: E402
from src import okx_native as N                # noqa: E402

INST = "SOL-USDT-SWAP"
FIELDS = ["ts_utc", "action", "instId", "clOrdId", "ok", "code", "msg", "resp"]


def _iso(ms) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return "?"


def show_state() -> dict:
    pos = [p for p in P.positions(INST) if abs(float(p.get("pos") or 0)) > 0]
    pending = P.pending_orders(INST)
    algos = []
    for kind in ("oco", "conditional", "trigger"):
        try:
            algos.extend(N.pending_algos(INST, kind))
        except Exception:                                     # noqa: BLE001
            pass
    print("=" * 70)
    print("当前账户状态（只读）")
    print("=" * 70)
    if pos:
        for p in pos:
            print(f"  持仓: {p.get('posSide')} {p.get('pos')} 张  均价 {p.get('avgPx')}  "
                  f"未实现 {p.get('upl')}  杠杆 {p.get('lever')}x")
    else:
        print("  持仓: 无")
    print(f"  普通挂单: {len(pending)} 笔")
    for o in pending:
        print(f"    {o.get('side')} {o.get('sz')} 张 @ {o.get('px')}  clOrdId={o.get('clOrdId')}")
    print(f"  策略委托: {len(algos)} 笔")
    for a in algos:
        print(f"    {a.get('ordType')} side={a.get('side')} sz={a.get('sz')} "
              f"止损={a.get('slTriggerPx')} 止盈={a.get('tpTriggerPx')} algoId={a.get('algoId')}")
    return {"positions": pos, "pending": pending, "algos": algos}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true", help="真正执行（还需 OKX_TRADING_ENABLED=1）")
    args = ap.parse_args()

    state = show_state()
    if not state["positions"] and not state["pending"] and not state["algos"]:
        print("\n✅ 已经是空仓、无挂单，无需操作。")
        return 0

    if not args.yes:
        print("\n（dry-run：未做任何修改。确实要全平请加 --yes，并先设置 OKX_TRADING_ENABLED=1）")
        return 1

    if os.environ.get("OKX_TRADING_ENABLED") != "1":
        print("\n⛔ 拒绝执行：环境变量 OKX_TRADING_ENABLED 不是 1。")
        return 2
    print("\n⚠️ 即将执行：撤销所有委托 + reduceOnly 市价平仓")
    ans = input("确认全平？输入 FLATTEN 继续：").strip()
    if ans != "FLATTEN":
        print("已取消，未做任何操作。")
        return 0

    result = N.flatten(INST)
    print("\n执行结果：")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    print("\n当前状态复核：")
    show_state()
    return 0 if result.get("flat") else 3


if __name__ == "__main__":
    sys.exit(main())
