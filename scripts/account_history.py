# -*- coding: utf-8 -*-
"""账户真实历史诊断：用 OKX 官方口径核对交易战绩与资金流水。

用途：本地台账不可信时，用交易所权威数据回答三个问题：
  1. 我真实平过哪些仓位？每个仓位官方结算的 realizedPnl 是多少？
  2. 手续费、资金费一共吃掉多少钱？
  3. 权益变动里有多少来自交易、多少来自充值/提现？

用法：python scripts/account_history.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                    # noqa: E402


def ms2iso(ms) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return "?"


def f(x, d=0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def main() -> int:
    print("=" * 84)
    print("账户真实历史诊断（OKX 官方口径）")
    print("=" * 84)

    # ---------- 1. 平仓历史 ----------
    try:
        ph = P.private("GET", "/api/v5/account/positions-history",
                       {"instType": "SWAP", "limit": "100"})
    except P.PrivateError as e:
        print(f"❌ positions-history 失败: {e}")
        return 1

    print(f"\n【1】已平仓位：{len(ph)} 个（官方结算口径）")
    print("-" * 84)
    print(f"{'开仓时间':<17}{'方向':<6}{'开→平':<18}{'张数':>7}{'realizedPnl':>12}{'fee':>9}{'funding':>9}")
    total_real = total_fee = total_fund = 0.0
    wins = losses = 0
    for p in sorted(ph, key=lambda x: int(x.get("uTime") or 0)):
        rp = f(p.get("realizedPnl"))
        fee = f(p.get("fee"))
        fund = f(p.get("fundingFee"))
        total_real += rp
        total_fee += abs(fee)
        total_fund += fund
        if rp > 0:
            wins += 1
        elif rp < 0:
            losses += 1
        print(f"{ms2iso(p.get('openTime')):<17}{str(p.get('posSide')):<6}"
              f"{f(p.get('openAvgPx')):>7.2f}→{f(p.get('closeAvgPx')):<8.2f}"
              f"{f(p.get('closeTotalPos')):>7.2f}{rp:>12.4f}{fee:>9.4f}{fund:>9.4f}")
    print("-" * 84)
    print(f"合计：{len(ph)} 个仓位  盈 {wins} / 亏 {losses}  "
          f"官方 realizedPnl 合计 {total_real:+.4f} U  fee {total_fee:.4f} U  "
          f"funding {total_fund:+.4f} U")
    print(f"净值贡献（realized + funding）: {total_real + total_fund:+.4f} U")

    # ---------- 2. 资金流水 ----------
    print("\n【2】资金流水（近 90 天，解释权益变动来源）")
    print("-" * 84)
    bills = []
    for path in ("/api/v5/account/bills", "/api/v5/account/bills-archive"):
        try:
            bills += P.private("GET", path, {"instType": "SWAP", "limit": "100"})
        except P.PrivateError as e:
            print(f"  {path} 读取失败: {e}")
    seen = {}
    for b in bills:
        seen[str(b.get("billId"))] = b
    bills = sorted(seen.values(), key=lambda x: int(x.get("ts") or 0))

    by_type = defaultdict(float)
    by_sub = defaultdict(float)
    total_chg = 0.0
    for b in bills:
        chg = f(b.get("balChg"))
        total_chg += chg
        by_type[str(b.get("type"))] += chg
        by_sub[f"{b.get('type')}/{b.get('subType')}"] += chg
    print(f"  流水笔数: {len(bills)}   合计 balChg: {total_chg:+.4f} U")
    print("  按类型汇总（balChg）:")
    for k, v in sorted(by_type.items(), key=lambda kv: -abs(kv[1])):
        print(f"    type={k:<4} {v:+12.4f} U")
    print("  按 类型/子类型 汇总（只显示 |金额|>0.01）:")
    for k, v in sorted(by_sub.items(), key=lambda kv: -abs(kv[1])):
        if abs(v) > 0.01:
            print(f"    {k:<12} {v:+12.4f} U")

    # ---------- 3. 当前权益 ----------
    bal = P.balance("USDT")
    if bal:
        b = bal[0]
        print("\n【3】当前账户")
        print(f"  总权益 {f(b.get('totalEq')):.4f} U   可用 {f(b.get('availBal') or (b.get('details') or [{}])[0].get('availBal')):.4f} U")
    return 0


if __name__ == "__main__":
    sys.exit(main())
