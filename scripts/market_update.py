# -*- coding: utf-8 -*-
"""每日更新：持仓状态 + 多周期涨跌 + 资金流/宏观同期变化。

回答"为什么跌"这类问题时，本脚本只负责摆出**同期发生的可验证事实**（跨市场涨跌、
资金流、费率、持仓、强平、宏观事件），不做因果断言——同期相关不等于因果。

运行：python scripts/market_update.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import risk                                  # noqa: E402
from src.collect import collect                       # noqa: E402
from src.sources_ext import cnbc_macro                # noqa: E402
from src.util import (http_json, load_config, ms_to_iso, now_ms,  # noqa: E402
                      okx, safe_call)

POS_ID = "L-1168"
POS_ENTRY, POS_QTY, POS_STOP, POS_TP = 116.80, 2.00, 115.77, 118.65


def pct(a, b):
    return (a / b - 1) * 100 if b else None


def main() -> int:
    cfg = load_config()
    print("=" * 78)
    print(f"更新 @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 78)

    # ---------- 1. 持仓
    st = risk.compute(cfg)
    pos = [p for p in (st.get("open_positions") or []) if p["signal_id"] == POS_ID]
    print("\n【持仓状态】")
    if pos:
        p = pos[0]
        print(f"  仍在持仓: {p['side']} {p['qty']} 张 @ {p['entry']}  止损 {p['stop']}")
    else:
        print("  持仓已不在台账中（已平仓或已被你手动了结）")

    # 入场后是否触及止盈/止损（用4H K线覆盖足够长的历史，并按台账里的真实入场时间）
    c4 = okx("/api/v5/market/candles", {"instId": "SOL-USDT-SWAP", "bar": "4H", "limit": "200"})
    b4 = sorted([{"ts": int(v[0]), "o": float(v[1]), "h": float(v[2]), "l": float(v[3]),
                  "c": float(v[4])} for v in c4], key=lambda x: x["ts"])
    entry_ts = None
    for p in (st.get("open_positions") or []):
        if p.get("signal_id") == POS_ID:
            try:
                entry_ts = int(datetime.fromisoformat(p["opened_at_utc"]).timestamp() * 1000)
            except Exception:  # noqa: BLE001
                pass
    after = [r for r in b4 if entry_ts and r["ts"] >= entry_ts]
    if after:
        f_tp = next((r for r in after if r["h"] >= POS_TP), None)
        f_sl = next((r for r in after if r["l"] <= POS_STOP), None)
        print(f"  入场后(4H) 最高 {max(r['h'] for r in after):.3f}  最低 {min(r['l'] for r in after):.3f}")
        print(f"  首次触及止盈 {POS_TP}: {ms_to_iso(f_tp['ts'])[:16] if f_tp else '未触及'}")
        print(f"  首次触及止损 {POS_STOP}: {ms_to_iso(f_sl['ts'])[:16] if f_sl else '未触及'}")
        if f_tp and f_sl:
            print("  先后判定: " + ("止盈在先 → 盈利单" if f_tp["ts"] < f_sl["ts"] else "止损在先 → 亏损单"))

    # ---------- 2. 多周期涨跌（SOL 用真实K线算）
    print("\n【SOL 多周期涨跌（基于真实4H K线）】")
    c4 = okx("/api/v5/market/candles", {"instId": "SOL-USDT-SWAP", "bar": "4H", "limit": "200"})
    b4 = sorted([{"ts": int(v[0]), "o": float(v[1]), "h": float(v[2]), "l": float(v[3]),
                  "c": float(v[4])} for v in c4], key=lambda x: x["ts"])
    last = b4[-1]["c"]
    print(f"  现价 {last:.3f}")
    for label, bars in (("4小时", 1), ("12小时", 3), ("1天", 6), ("2天", 12),
                        ("3天", 18), ("7天", 42), ("14天", 84)):
        if len(b4) > bars:
            print(f"  近 {label:<5}: {pct(last, b4[-1 - bars]['c']):+7.2f}%   "
                  f"（{ms_to_iso(b4[-1 - bars]['ts'])} 起）")

    print("\n【最近48小时最大的几根下跌（4H）】")
    recent = b4[-12:]
    moves = sorted([(pct(r["c"], r["o"]), r) for r in recent], key=lambda x: x[0])[:5]
    for m, r in moves:
        print(f"  {ms_to_iso(r['ts'])}  {m:+6.2f}%   {r['o']:.2f} -> {r['c']:.2f}   "
              f"区间 {r['l']:.2f}~{r['h']:.2f}")

    # ---------- 3. 跨市场对比
    print("\n【跨市场对比（CoinGecko）】")
    try:
        mk = http_json("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
                       "&ids=bitcoin,ethereum,solana&price_change_percentage=1h,24h,7d,30d")
        print(f"  {'币':<8}{'现价':>12}{'1h':>9}{'24h':>9}{'7d':>9}{'30d':>9}")
        for m in mk:
            print(f"  {m['symbol'].upper():<8}{m['current_price']:>12,.2f}"
                  f"{m.get('price_change_percentage_1h_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_24h_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_7d_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_30d_in_currency') or 0:>8.2f}%")
    except Exception as e:  # noqa: BLE001
        print(f"  失败: {e}")

    # ---------- 4. 宏观
    print("\n【宏观同期变化（CNBC 实时）】")
    env = safe_call("cnbc_macro", lambda: cnbc_macro(cfg["macro"]["cnbc_symbols"]))
    if env["ok"]:
        for k, v in env["data"].items():
            print(f"  {k:<8} {str(v['name'])[:22]:<24} {v['last']:>12,.3f}   "
                  f"当日 {v['change_pct']:+.2f}%")
    print("  对照：09-28 快照里 US10Y 5.226%(+0.87%) / DXY 101.20(+0.23%) / "
          "黄金 4188(-3.06%) / VIX 15.90(+6.93%)")
    print("  → 美债收益率与美元同步走强、黄金大跌：全球无风险利率上行的典型组合")

    # ---------- 5. 资金流与衍生品
    s = collect(cfg, None)
    f = s["features"]
    print("\n【资金流 / 链上 / 衍生品】")
    oc = f.get("onchain") or {}
    sc = oc.get("stablecoins") or {}
    d7 = sc.get("chg7d_usd", 0) / 1e9
    print(f"  稳定币总量 : {sc.get('total_usd', 0) / 1e9:.1f}B   1日 {sc.get('chg1d_usd', 0) / 1e9:+.2f}B   "
          f"7日 {d7:+.2f}B  → {'场外流动性净流入' if d7 > 0 else '场外流动性净流出'}")
    tvl = oc.get("solana_tvl") or {}
    print(f"  Solana TVL : {tvl.get('tvl_usd', 0) / 1e9:.2f}B  1日 {tvl.get('chg1d_pct'):.2f}%  "
          f"7日 {tvl.get('chg7d_pct'):.2f}%")
    cg = (f.get("sentiment") or {}).get("coingecko") or {}
    print(f"  全局市值24h: {cg.get('mcap_change24h_pct'):.2f}%   BTC占比 {cg.get('btc_dominance'):.2f}%")
    fr = f.get("funding") or {}
    print(f"  资金费率   : {fr.get('fundingRate_pct')}%   历史分位 {fr.get('hist_pct_rank')}%   "
          f"近3期累计 {fr.get('last3_cum_pct')}%   近9期累计 {fr.get('last9_cum_pct')}%")
    oi = f.get("oi") or {}
    print(f"  持仓量     : {(s.get('facts') or {}).get('oi_usd', 0) / 1e6:.1f}M USD   "
          f"15m {((oi.get('5m') or {}).get('3bars_pct'))}%   1h {((oi.get('5m') or {}).get('12bars_pct'))}%   "
          f"4h {((oi.get('5m') or {}).get('48bars_pct'))}%")
    liq = f.get("liquidation") or {}
    for w in ("w15", "w60"):
        L = liq.get(w) or {}
        print(f"  强平 {w:<4}   : {L.get('total_usd', 0):,.0f}u   多头占 {L.get('long_share')}   "
              f"笔数 {L.get('count')}")
    tk = f.get("taker_5m") or {}
    print(f"  主动买占比 : 15m {(tk.get('3bars') or {}).get('buy_ratio')}   "
          f"1h {(tk.get('12bars') or {}).get('buy_ratio')}   "
          f"4h {(tk.get('48bars') or {}).get('buy_ratio')}")

    # ---------- 6. 同期宏观事件
    print("\n【近4天已发生的高影响事件 + 未来事件】")
    lo = ms_to_iso(now_ms() - 4 * 86400_000).replace("+00:00", ".000Z")
    hi = ms_to_iso(now_ms() + 3 * 86400_000).replace("+00:00", ".000Z")
    try:
        j = http_json("https://economic-calendar.tradingview.com/events?from=" + lo +
                      "&to=" + hi + "&countries=US,EU,GB,JP,CN", retries=2,
                      headers={"Origin": "https://www.tradingview.com",
                               "Referer": "https://www.tradingview.com/"})
        evs = [e for e in (j.get("result") or []) if str(e.get("importance")) == "1"
               and e.get("country") == "US"]
        evs.sort(key=lambda x: x["date"])
        nw = now_ms()
        for e in evs:
            ts = int(datetime.fromisoformat(e["date"].replace("Z", "+00:00")).timestamp() * 1000)
            tag = "已发生" if ts < nw else "待公布"
            print(f"  [{tag}] {e['date'][:16].replace('T', ' ')}Z  {e.get('title')}   "
                  f"实际={e.get('actual')} 预期={e.get('forecast')} 前值={e.get('previous')}")
        if not evs:
            print("  近4天无美国高影响事件")
    except Exception as e:  # noqa: BLE001
        print(f"  日历获取失败: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
