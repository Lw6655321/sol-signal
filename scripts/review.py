# -*- coding: utf-8 -*-
"""每日复盘 / 成交回填 / 关键价位速查。

用法：
  python scripts/review.py                                  # 只看台账 + 当前关键价位
  python scripts/review.py --signal PLANB-1 --entry 117.62 --qty 1.49 \
        --side long --exit 120.03 --closed-at 2026-09-28 --reason tp1

说明：
- 跨日回填必须带 --closed-at（只更新净值、不影响今日亏损闸门）。
- 关键价位全部来自实时数据，含多空两个方向的挂单位、止损、止盈与手数。
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import engine, risk                        # noqa: E402
from src.collect import collect                     # noqa: E402
from src.util import load_config, ms_to_iso         # noqa: E402


def show_ledger(cfg: dict) -> None:
    st = risk.compute(cfg)
    print("=" * 74)
    print("风控台账")
    print("=" * 74)
    print(f"  净值            {st['equity_estimate']:.3f} u   (起始 {cfg['account']['equity_usdt']} u)")
    print(f"  今日已实现      {st['realized_pnl_today']:+.3f} u   单日上限 -{st['daily_loss_limit_usd']:.2f} u")
    print(f"  今日开仓次数    {st['entries_today']} / {st['_max_entries']}")
    print(f"  连续亏损        {st['consecutive_losses']} 笔   "
          f"暂停={ms_to_iso(st['paused_until_ms']) if st.get('paused') else '无'}")
    print(f"  熔断            {'开启：' + str(st.get('halt_reason')) if st['halted'] else '未开启'}")
    print(f"  累计成交        {st['total_trades']} 笔   胜 {st['wins']} / 亏 {st['losses']}")
    print(f"  持仓            {len(st.get('open_positions') or [])} 个")
    print()
    print("  成交统计: " + str(risk.stats()))


def show_levels(cfg: dict) -> None:
    s = collect(cfg, None)
    f, tf = s["features"], s["features"]["tf"]
    px = f["book"]["microprice"]
    t5, t15, t1h, t4h = tf["5m"], tf["15m"], tf["1H"], tf["4H"]
    print()
    print("=" * 74)
    print(f"实时行情 @ {s['generated_at_utc']}   数据源 {s['health']['ok']} 正常")
    print("=" * 74)
    print(f"  现价 {px:.3f}   买/卖 {f['book']['best_bid']:.3f}/{f['book']['best_ask']:.3f}   "
          f"价差 {f['book']['spread_bps']:.2f}bps")
    print(f"  24h 区间 {s['facts']['low24h']:.3f} ~ {s['facts']['high24h']:.3f}   "
          f"24h {s['facts']['chg24h_pct']:+.2f}%   日内VWAP {t15.get('vwap_day'):.3f}")
    print()
    print("  周期  结构        均线    ATR%    EMA20     EMA50     EMA200")
    for n, t in (("5m", t5), ("15m", t15), ("1H", t1h), ("4H", t4h)):
        print(f"  {n:<5} {t['structure']['structure']:<11} {t['ema_stack']:<7} "
              f"{t['atr_pct']:<7.3f} {t['ema20']:<9.3f} {t['ema50']:<9.3f} {t['ema200'] or 0:<9.3f}")
    print()
    print(f"  5m  20周期区间  {t5['donchian20']['low']:.3f} ~ {t5['donchian20']['high']:.3f}")
    print(f"  15m 20周期区间  {t15['donchian20']['low']:.3f} ~ {t15['donchian20']['high']:.3f}")
    print(f"  15m 枢轴        高 {t15['structure']['last_pivot_high']:.3f} / 低 {t15['structure']['last_pivot_low']:.3f}")
    tk, liq, fr = f["taker_5m"], f["liquidation"]["w15"], f["funding"]
    print(f"  主动买占比(15m) {tk['3bars']['buy_ratio']:.3f}   "
          f"强平15m {liq['total_usd']:,.0f}u (多头占 {liq['long_share'] or 0:.2f})   "
          f"费率 {fr['fundingRate_pct']:.5f}%")

    # 两个方向的结构化挂单方案
    equity = risk.compute(cfg)["equity_estimate"]
    p = engine._params("scalp_5m", cfg)
    risk_u = equity * float(cfg["risk"]["risk_per_trade_pct"]) / 100
    print()
    print("=" * 74)
    print(f"结构化挂单参考（净值 {equity:.2f}u，单笔风险 {risk_u:.2f}u = "
          f"{cfg['risk']['risk_per_trade_pct']}%）")
    print("=" * 74)
    plans = []
    # 空：跌破 15m 区间下沿
    trig = t15["donchian20"]["low"] - 0.22
    stop = trig + 1.30
    plans.append(("做空·跌破进场", "触发委托(Stop)", trig, stop, trig - 2.30, trig - 2.60))
    # 多：回踩支撑簇
    sup = min(t15["donchian20"]["low"], t1h["ema200"], t4h["ema50"])
    buy = sup + 0.10
    bstop = sup - 0.55
    plans.append(("做多·回踩进场", "限价委托", buy, bstop,
                  buy + 1.8 * (buy - bstop), t15["donchian20"]["high"]))
    for name, otype, entry, stop_, tp1, tp2 in plans:
        d = abs(entry - stop_)
        qty = math.floor((risk_u / d) / 0.01) * 0.01
        notional = qty * entry
        sm, stt = engine._fee_share(cfg, entry, d)
        net_rr = abs(tp1 - entry) / d - sm
        print(f"  【{name}】{otype}")
        print(f"     挂单价 {entry:.2f}   止损 {stop_:.2f}   止损距离 {d:.2f} ({d / entry * 100:.2f}%)")
        print(f"     TP1 {tp1:.2f} (毛{abs(tp1 - entry) / d:.2f}R 净{net_rr:.2f}R)   "
              f"TP2 {tp2:.2f} (毛{abs(tp2 - entry) / d:.2f}R)")
        print(f"     手数 {qty:.2f} 张 = 名义 {notional:.0f}u = 实际杠杆 {notional / equity:.2f}x   "
              f"手续费占风险 {sm * 100:.1f}%")
        print()


def main() -> int:
    ap = argparse.ArgumentParser(description="每日复盘 / 成交回填")
    ap.add_argument("--signal", help="信号ID/自定义编号（回填时必填）")
    ap.add_argument("--entry", type=float)
    ap.add_argument("--qty", type=float)
    ap.add_argument("--side", choices=["long", "short"])
    ap.add_argument("--exit", dest="exit_px", type=float)
    ap.add_argument("--closed-at", help="平仓时间(UTC)，如 2026-09-28")
    ap.add_argument("--reason", default="manual")
    ap.add_argument("--notes", default="")
    args = ap.parse_args()

    cfg = load_config()
    if args.signal and args.exit_px is not None:
        import datetime
        closed_ms = None
        if args.closed_at:
            dt = datetime.datetime.fromisoformat(args.closed_at.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            closed_ms = int(dt.timestamp() * 1000)
        print("=" * 74)
        print("回填成交")
        print("=" * 74)
        res = risk.record_trade(cfg, args.signal, args.exit_px, args.reason, args.notes,
                                args.entry, args.qty, args.side, closed_at_ms=closed_ms)
        print(f"  完成。净值 {res['equity_estimate']:.3f}u   今日已实现 {res['realized_pnl_today']:+.3f}u")
        print()

    show_ledger(cfg)
    show_levels(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
