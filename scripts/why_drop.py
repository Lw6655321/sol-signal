# -*- coding: utf-8 -*-
"""回溯分析：这两三天到底跌了多少、同期各类资产与资金流发生了什么。

原则：只摆可验证的事实序列（价格路径、资金费率、持仓量、宏观多日序列、链上），
不编叙事。新闻面另行核实。

运行：python scripts/why_drop.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.sources_okx import candles, funding_history, open_interest, open_interest_volume  # noqa: E402
from src.sources_ext import defillama_solana_tvl, stablecoin_liquidity  # noqa: E402
from src.util import http_json, load_config, ms_to_iso, now_ms, safe_call  # noqa: E402

INST = "SOL-USDT-SWAP"
PAIRS = [("SOL", "SOL-USDT-SWAP", "solana"), ("BTC", "BTC-USDT-SWAP", "bitcoin"),
         ("ETH", "ETH-USDT-SWAP", "ethereum")]


def pct(a, b):
    return (a / b - 1) * 100 if b else None


def daily_path(bar: str = "4H", limit: int = 200, days: int = 10):
    """把 4H 聚合成日线，返回最近 N 天。"""
    env = candles(INST, bar, limit)
    if not env["ok"]:
        return []
    rows = env["data"]
    buckets: dict[str, dict] = {}
    for r in rows:
        d = datetime.fromtimestamp(r["ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        b = buckets.setdefault(d, {"o": r["o"], "h": r["h"], "l": r["l"], "c": r["c"],
                                   "vol": 0.0})
        b["h"] = max(b["h"], r["h"])
        b["l"] = min(b["l"], r["l"])
        b["c"] = r["c"]
        b["vol"] += r.get("volQuote") or 0
    keys = sorted(buckets)[-days:]
    return [{"date": k, **buckets[k]} for k in keys]


def yahoo(symbol: str, rng: str = "1mo") -> dict | None:
    try:
        j = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                      f"?range={rng}&interval=1d", retries=2, timeout=15,
                      headers={"User-Agent": "Mozilla/5.0"})
        res = j["chart"]["result"][0]
        ts = res["timestamp"]
        cl = res["indicators"]["quote"][0]["close"]
        rows = [(int(t), float(c)) for t, c in zip(ts, cl) if c is not None]
        if not rows:
            return None
        return {"symbol": symbol, "rows": rows,
                "last": rows[-1][1], "chg_1d": pct(rows[-1][1], rows[-2][1]) if len(rows) > 1 else None,
                "chg_5d": pct(rows[-1][1], rows[-6][1]) if len(rows) > 5 else None,
                "chg_20d": pct(rows[-1][1], rows[0][1]) if len(rows) > 1 else None}
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    cfg = load_config()
    print("=" * 80)
    print(f"回溯分析 @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
    print("=" * 80)

    # ---------- 1. SOL 日线路径
    print("\n【SOL 最近10天日线（4H聚合）】")
    print(f"  {'日期':<12}{'开':>9}{'高':>9}{'低':>9}{'收':>9}{'日涨跌':>10}{'成交额(亿u)':>13}")
    path = daily_path()
    prev = None
    for d in path:
        ch = pct(d["c"], prev) if prev else None
        print(f"  {d['date']:<12}{d['o']:>9.2f}{d['h']:>9.2f}{d['l']:>9.2f}{d['c']:>9.2f}"
              f"{(f'{ch:+.2f}%' if ch is not None else '-'):>10}{d['vol'] / 1e8:>13.1f}")
        prev = d["c"]

    # ---------- 2. 三大币种多周期
    print("\n【三大币种多周期涨跌（CoinGecko）】")
    try:
        mk = http_json("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
                       "&ids=bitcoin,ethereum,solana"
                       "&price_change_percentage=1h,24h,7d,14d,30d", retries=2)
        print(f"  {'币':<6}{'现价':>12}{'24h':>9}{'7d':>9}{'14d':>9}{'30d':>9}")
        for m in mk:
            print(f"  {m['symbol'].upper():<6}{m['current_price']:>12,.2f}"
                  f"{m.get('price_change_percentage_24h_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_7d_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_14d_in_currency') or 0:>8.2f}%"
                  f"{m.get('price_change_percentage_30d_in_currency') or 0:>8.2f}%")
    except Exception as e:  # noqa: BLE001
        print(f"  失败: {e}")

    # ---------- 3. 宏观多日序列
    print("\n【宏观多日序列（Yahoo Finance，1个月日线）】")
    for sym, label in (("^TNX", "美债10年收益率"), ("DX-Y.NYB", "美元指数"),
                       ("^GSPC", "标普500"), ("^VIX", "VIX"), ("GC=F", "黄金"),
                       ("BTC-USD", "BTC")):
        d = yahoo(sym)
        if d:
            print(f"  {label:<14}{d['symbol']:<10} 最新 {d['last']:>10,.2f}   "
                  f"1日 {d['chg_1d']:+.2f}%   5日 {d['chg_5d']:+.2f}%   "
                  f"20日 {d['chg_20d']:+.2f}%")
        else:
            print(f"  {label:<14}{sym:<10} 获取失败/无数据")

    # ---------- 4. 资金费率与持仓量历史
    print("\n【SOL 资金费率历史（最近30期 = 10天）】")
    fh = funding_history(INST, 100)
    if fh["ok"]:
        rows = fh["data"]["rows"][-30:]
        pos = sum(1 for r in rows if r["fundingRate"] > 0)
        cum = sum(r["fundingRate"] for r in rows) * 100
        print(f"  正值期数 {pos}/{len(rows)}   累计 {cum:+.5f}%   "
              f"最高 {max(r['fundingRate'] for r in rows) * 100:+.5f}%   "
              f"最低 {min(r['fundingRate'] for r in rows) * 100:+.5f}%")
        print("  逐期（时间 费率%）:")
        for r in rows[-12:]:
            print(f"    {ms_to_iso(r['fundingTime'])[:16]}  {r['fundingRate'] * 100:+.5f}%")

    print("\n【SOL 持仓量历史（1H，最近10天）】")
    oiv = open_interest_volume("SOL", "1H")
    if oiv["ok"]:
        rows = oiv["data"][-240:]
        if rows:
            first, last = rows[0]["value"][0], rows[-1]["value"][0]
            print(f"  10天前 {first / 1e6:,.1f}M  ->  现在 {last / 1e6:,.1f}M   "
                  f"变化 {pct(last, first):+.2f}%")
            chg = [(r["ts"], r["value"][0]) for r in rows]
            peak = max(chg, key=lambda x: x[1])
            print(f"  峰值 {peak[1] / 1e6:,.1f}M @ {ms_to_iso(peak[0])[:16]}   "
                  f"自峰值回落 {pct(last, peak[1]):+.2f}%")
    oi = open_interest(INST)
    if oi["ok"]:
        print(f"  当前持仓 {oi['data']['oi_contracts']:,.0f} 张 = "
              f"{oi['data']['oi_usd'] / 1e6:,.1f}M USD")

    # ---------- 5. 链上与流动性
    print("\n【链上 / 场外流动性】")
    tvl = safe_call("tvl", defillama_solana_tvl)
    if tvl["ok"]:
        d = tvl["data"]
        print(f"  Solana TVL {d['tvl_usd'] / 1e9:.2f}B   1日 {d['chg1d_pct']:+.2f}%   "
              f"7日 {d['chg7d_pct']:+.2f}%   30日 {d['chg30d_pct']:+.2f}%")
    sc = safe_call("stablecoin", stablecoin_liquidity)
    if sc["ok"]:
        d = sc["data"]
        print(f"  稳定币总量 {d['total_usd'] / 1e9:.1f}B   1日 {d['chg1d_usd'] / 1e9:+.2f}B   "
              f"7日 {d['chg7d_usd'] / 1e9:+.2f}B")
    try:
        g = http_json("https://api.coingecko.com/api/v3/global")["data"]
        print(f"  全局市值 {g['total_market_cap']['usd'] / 1e12:.3f}T   "
              f"24h {g.get('market_cap_change_percentage_24h_usd'):+.2f}%   "
              f"BTC占比 {g['market_cap_percentage']['btc']:.2f}%   "
              f"ETH占比 {g['market_cap_percentage']['eth']:.2f}%")
    except Exception as e:  # noqa: BLE001
        print(f"  全局数据失败: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
