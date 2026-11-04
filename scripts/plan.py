# -*- coding: utf-8 -*-
"""生成当日结构化挂单方案（长/空各一套），全部数字来自实时数据。

运行：python scripts/plan.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import engine, risk                       # noqa: E402
from src.collect import collect                    # noqa: E402
from src.util import load_config                   # noqa: E402


def plan(cfg, equity, name, otype, entry, stop, tp2=None, r_mult=1.8, label=""):
    d = abs(entry - stop)
    stop_pct = d / entry * 100
    risk_u = equity * float(cfg["risk"]["risk_per_trade_pct"]) / 100
    tp1 = entry + r_mult * d if stop < entry else entry - r_mult * d
    tp2 = tp2 if tp2 is not None else (entry + (r_mult + 1.0) * d if stop < entry
                                       else entry - (r_mult + 1.0) * d)
    sm, stt = engine._fee_share(cfg, entry, d)
    net1 = abs(tp1 - entry) / d - sm
    qty = math.floor((risk_u / d) / 0.01) * 0.01
    notional = qty * entry
    max_share = float(cfg["risk"]["fee"]["max_fee_share_of_risk"])
    ok_stop = 0.30 <= stop_pct <= 3.50
    ok_fee = sm <= max_share
    ok_lev = notional / equity <= float(cfg["account"]["leverage_setting"])
    print(f"【{label or name}】{otype}")
    print(f"   进场 {entry:.2f}    止损 {stop:.2f}    止损距离 {d:.2f} = {stop_pct:.2f}%")
    print(f"   TP1 {tp1:.2f} (毛 {r_mult:.2f}R / 净 {net1:.2f}R)      TP2 {tp2:.2f} "
          f"(毛 {abs(tp2 - entry) / d:.2f}R)")
    print(f"   手数 {qty:.2f} 张 = 名义 {notional:.0f}u = 实际杠杆 {notional / equity:.2f}x")
    print(f"   风险 {risk_u:.2f}u    手续费占风险 {sm * 100:.1f}% (吃单 {stt * 100:.1f}%)")
    flags = []
    if not ok_stop:
        flags.append("止损距离越界")
    if not ok_fee:
        flags.append("手续费占比超限")
    if not ok_lev:
        flags.append("超杠杆设置")
    print(f"   检查: {'✅ 全部通过' if not flags else '❌ ' + '、'.join(flags)}")
    print()
    return {"entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2, "qty": qty,
            "notional": notional, "risk_u": risk_u, "net_rr": net1}


def main() -> int:
    cfg = load_config()
    eq = risk.compute(cfg)["equity_estimate"]
    s = collect(cfg, None)
    f, tf = s["features"], s["features"]["tf"]
    px = f["book"]["microprice"]
    t5, t15, t1h, t4h = tf["5m"], tf["15m"], tf["1H"], tf["4H"]
    lo, hi = t15["donchian20"]["low"], t15["donchian20"]["high"]
    atr = t15["atr14"]

    print("=" * 76)
    print(f"结构化方案 @ {s['generated_at_utc']}   现价 {px:.3f}   净值 {eq:.2f}u")
    print(f"15m区间 {lo:.2f}~{hi:.2f}   VWAP {t15.get('vwap_day'):.2f}   "
          f"15m EMA20 {t15['ema20']:.2f}   ATR15 {atr:.3f}")
    print(f"结构: 5m={t5['structure']['structure']} 15m={t15['structure']['structure']} "
          f"1H={t1h['structure']['structure']} 4H={t4h['structure']['structure']}")
    print("=" * 76)
    print()

    # 止损锚定「入场价 ± 1.5×ATR」，而不是锚定区间沿口。
    # 原因：锚在沿口时，现价离沿口越远止损就越宽，会撑破玩法的 max_stop_pct 导致方案作废
    # （真实踩过：现价 105.65 时算出 2.57% 的止损，超过 5m 快线 1.20% 的上限）。
    e_s = round(min(hi - 0.05, px + 2.0 * atr), 2)
    st_s = round(e_s + 1.5 * atr, 2)
    print("方案一 · 限价做空（挂在阻力方向，等反弹成交）")
    plan(cfg, eq, "short_limit", "限价委托（挂在现价上方，等反弹成交）",
         e_s, st_s, r_mult=1.8, label="限价做空")

    # 方案二：限价做多 —— 挂在支撑方向，等回踩成交
    e_l = round(max(lo + 0.10, px - 2.0 * atr), 2)
    st_l = round(e_l - 1.5 * atr, 2)
    print("方案二 · 限价做多（逆势接支撑，仅在跌到位时成交）")
    plan(cfg, eq, "long_limit", "限价委托（挂在现价下方，等回踩成交）",
         e_l, st_l, tp2=round(hi, 2), r_mult=1.8, label="限价做多")

    print("=" * 76)
    print("⚠️ 只挂一个方向，两个都挂会在同一区间对冲、白交手续费。")
    print("⚠️ 限价单必须同时挂止损；成交后到 +1R 再把止损上移到入场价（保本）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
