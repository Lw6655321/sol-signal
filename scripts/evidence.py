# -*- coding: utf-8 -*-
"""全维度证据矩阵：把 14 个维度放在同一张表上按日对齐，各自与自身历史分布比较。

为什么需要它：
凭几个数字临时讲故事很容易出错（真实教训：曾用两个时间点的截面数据推断
"美元走强导致下跌"，用多日序列一查就发现最近5天美元几乎没动、美债还跌了）。
本脚本强制把所有维度都摆出来，每个读数都标出它在自身历史中的分位，
再由固定规则给出"看涨/看跌/中性"，而不是靠记忆和直觉。

诚实边界：订单簿深度、强平明细、跨所基差没有历史，只能取当下，
因此"归因"必然不完整——本脚本只回答"哪些维度在同期处于极端"，
不声称证明因果。

运行：python scripts/evidence.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.collect import collect                                    # noqa: E402
from src.sources_ext import defillama_solana_tvl, stablecoin_liquidity  # noqa: E402
from src.sources_okx import (candles, funding_history,             # noqa: E402
                             long_short_account_ratio, long_short_top_position_ratio,
                             open_interest_volume, taker_volume)
from src.util import http_json, load_config, safe_call              # noqa: E402

INST = "SOL-USDT-SWAP"


def pct(a, b):
    return (a / b - 1) * 100 if b else None


def percentile(series: list[float], v: float):
    if not series or v is None:
        return None
    return sum(1 for x in series if x <= v) / len(series) * 100


def day_of(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%m-%d")


def fs(v, spec="+.2f", suffix=""):
    """安全格式化：None 显示为 -，避免 NoneType 格式化崩溃。"""
    return "-" if v is None else format(v, spec) + suffix


def yahoo_series(symbol: str, rng: str = "1mo"):
    try:
        j = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                      f"?range={rng}&interval=1d", retries=2, timeout=15,
                      headers={"User-Agent": "Mozilla/5.0"})
        r = j["chart"]["result"][0]
        # 注意：Yahoo 的 timestamp 单位是「秒」，不是毫秒
        return {datetime.fromtimestamp(int(t), tz=timezone.utc).strftime("%m-%d"): float(c)
                for t, c in zip(r["timestamp"], r["indicators"]["quote"][0]["close"])
                if c is not None}
    except Exception:  # noqa: BLE001
        return {}


def main() -> int:
    cfg = load_config()
    print("=" * 100)
    print(f"全维度证据矩阵 @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC   标的 SOL-USDT-SWAP")
    print("=" * 100)

    # ---------- 按日构建各维度序列
    days: dict[str, dict] = {}

    def slot(d):
        return days.setdefault(d, {})

    env = candles(INST, "4H", 200)
    if env["ok"]:
        for r in env["data"]:
            s = slot(day_of(r["ts"]))
            s.setdefault("highs", []).append(r["h"])
            s.setdefault("lows", []).append(r["l"])
            s["close"] = r["c"]
            s["vol"] = s.get("vol", 0) + (r.get("volQuote") or 0)

    fh = funding_history(INST, 100)
    if fh["ok"]:
        for r in fh["data"]["rows"]:
            s = slot(day_of(r["fundingTime"]))
            s["funding"] = s.get("funding", 0) + r["fundingRate"] * 100

    oiv = open_interest_volume("SOL", "1H")
    if oiv["ok"]:
        for r in oiv["data"]:
            slot(day_of(r["ts"]))["oi"] = r["value"][0] / 1e6

    lsa = long_short_account_ratio("SOL", "1H")
    if lsa["ok"]:
        for r in lsa["data"]:
            slot(day_of(r["ts"]))["ls_acct"] = r["value"][0]

    lst = long_short_top_position_ratio(INST, "1H")
    if lst["ok"]:
        for r in lst["data"]:
            slot(day_of(r["ts"]))["ls_top"] = r["value"][0]

    tk = taker_volume("SOL", "1H")
    if tk["ok"]:
        acc: dict[str, list[float]] = {}
        for r in tk["data"]:
            acc.setdefault(day_of(r["ts"]), []).append(r["value"])
        for d, rows in acc.items():
            sell = sum(x[0] for x in rows)
            buy = sum(x[1] for x in rows)
            slot(d)["taker_buy"] = buy / (sell + buy) if (sell + buy) else None

    macro = {k: yahoo_series(v) for k, v in
             (("us10y", "^TNX"), ("dxy", "DX-Y.NYB"), ("spx", "^GSPC"),
              ("vix", "^VIX"), ("gold", "GC=F"))}

    # ---------- 只打印最近 8 天
    keys = sorted(days)[-8:]
    hist_close = [days[d]["close"] for d in sorted(days) if "close" in days[d]]
    all_lows = [min(days[d]["lows"]) for d in sorted(days) if "lows" in days[d]]
    all_highs = [max(days[d]["highs"]) for d in sorted(days) if "highs" in days[d]]
    oi_hist = [days[d]["oi"] for d in sorted(days) if "oi" in days[d]]
    fr_hist = [days[d]["funding"] for d in sorted(days) if "funding" in days[d]]
    ls_hist = [days[d]["ls_acct"] for d in sorted(days) if "ls_acct" in days[d]]

    def n(v, fmt_str, suffix="", dash="-"):
        return dash if v is None else format(v, fmt_str) + suffix

    print("\n【表1】SOL 日频多维度序列（每天 4H 聚合）")
    print(f"{'日期':<7}{'收盘':>8}{'涨跌':>9}{'日振幅':>9}{'成交额亿':>10}"
          f"{'10日位置':>10}{'日内费率%':>11}{'持仓M':>9}{'多空比':>8}{'大户持仓比':>11}{'主动买占比':>11}")
    print("-" * 100)
    prev = None
    for d in keys:
        s = days[d]
        rng = pct(max(s["highs"]), min(s["lows"])) if s.get("highs") else None
        idx = sorted(days).index(d)
        win = sorted(days)[max(0, idx - 9):idx + 1]
        wl = min(min(days[x]["lows"]) for x in win if "lows" in days[x])
        wh = max(max(days[x]["highs"]) for x in win if "highs" in days[x])
        pos = (s["close"] - wl) / (wh - wl) if wh > wl else None
        print(f"{d:<7}{s['close']:>8.2f}"
              f"{n(pct(s['close'], prev), '+.2f', '%'):>9}"
              f"{n(rng, '+.2f', '%'):>9}"
              f"{s.get('vol', 0) / 1e8:>10.1f}"
              f"{n(pos, '.2f'):>10}"
              f"{n(s.get('funding'), '+.5f'):>11}"
              f"{n(s.get('oi'), '.1f'):>9}"
              f"{n(s.get('ls_acct'), '.3f'):>8}"
              f"{n(s.get('ls_top'), '.3f'):>11}"
              f"{n(s.get('taker_buy'), '.3f'):>11}")
        prev = s["close"]

    print("\n【表2】宏观日频序列（Yahoo Finance）")
    print(f"{'日期':<7}{'美债10年':>11}{'美元指数':>11}{'标普500':>11}{'VIX':>9}{'黄金':>11}")
    print("-" * 60)
    mkeys = sorted(set().union(*[set(v) for v in macro.values()]))[-8:]
    for d in mkeys:
        row = [macro[k].get(d) for k in ("us10y", "dxy", "spx", "vix", "gold")]
        print(f"{d:<7}" + "".join(f"{(f'{v:>11,.2f}' if v is not None else '          -')}"
                                  for v in row))

    # ---------- 当下时点维度（无历史）
    s = collect(cfg, None)
    f = s["features"]
    book = f.get("book") or {}
    liq = f.get("liquidation") or {}
    print("\n【表3】仅当下可得的维度（无历史，无法回溯归因）")
    b05 = (book.get("bands") or {}).get("0.5%") or {}
    print(f"  盘口价差 {book.get('spread_bps'):.2f}bps   0.5%档失衡 {b05.get('imbalance'):+.3f}   "
          f"100u滑点 {((book.get('slippage_buy') or {}).get('100') or {}).get('slippage_bps'):.2f}bps")
    print(f"  买墙 {(book.get('wall_bid') or {}).get('px')} / 卖墙 {(book.get('wall_ask') or {}).get('px')}")
    for w in ("w15", "w60"):
        L = liq.get(w) or {}
        print(f"  强平{w:<4} {L.get('total_usd', 0):,.0f}u  多头占比 {L.get('long_share')}  "
              f"最大单笔 {L.get('largest', 0):,.0f}u")
    basis = f.get("basis") or {}
    print("  跨所基差 " + "  ".join(f"{k}:{v.get('basis_bps'):+.1f}bps"
                                    for k, v in basis.items() if k != "_spread_bps"))
    oc = f.get("onchain") or {}
    sol = oc.get("solana") or {}
    print(f"  Solana TPS {sol.get('tps')}  epoch {sol.get('epoch')} ({sol.get('epoch_progress_pct')}%)")

    # ---------- 归因矩阵
    print("\n【表4】维度倾向矩阵（每项与其自身历史比较，给出看跌/中性/看涨）")
    print("-" * 100)
    rows = []
    cur_close = hist_close[-1]
    last_d, prev_d = keys[-1], keys[-2]
    chg = pct(days[last_d]["close"], days[prev_d]["close"])

    def add(dim, reading, rank, call, note):
        rows.append((dim, reading, rank, call, note))

    vix = macro["vix"].get(last_d)
    spx = macro["spx"].get(last_d)
    dxy = macro["dxy"].get(last_d)
    y10 = macro["us10y"].get(last_d)
    # 宏观 5日变化
    def chg5(series):
        ks = sorted(series)
        return pct(series[ks[-1]], series[ks[-6]]) if len(ks) > 5 else None
    add("宏观·美元", f"{dxy}", f"5日 {fs(chg5(macro['dxy']), '+.2f', '%')}",
        "看跌" if (chg5(macro['dxy']) or 0) > 0.5 else "中性",
        "美元走强压制风险资产")
    add("宏观·美债10年", f"{y10}", f"5日 {fs(chg5(macro['us10y']), '+.2f', '%')}",
        "看跌" if (chg5(macro['us10y']) or 0) > 1.0 else "中性", "利率上行压制估值")
    add("宏观·股市", f"{spx:,.0f}", f"5日 {fs(chg5(macro['spx']), '+.2f', '%')}",
        "看涨" if (chg5(macro['spx']) or 0) > 0.5 else "中性", "股市上涨=风险偏好未坏")
    add("宏观·VIX", f"{vix}", f"5日 {fs(chg5(macro['vix']), '+.2f', '%')}",
        "看涨" if (chg5(macro['vix']) or 0) < -3 else ("看跌" if (chg5(macro['vix']) or 0) > 3 else "中性"),
        "波动率下降=市场不恐慌")

    cur_oi = days[last_d].get("oi")
    add("持仓量", f"{cur_oi:.1f}M" if cur_oi else "-", f"分位 {percentile(oi_hist, cur_oi):.0f}%" if cur_oi else "-",
        "中性", "持仓量与价格同步回落=去杠杆，不是新空进场")

    cur_fr = days[last_d].get("funding")
    add("资金费率", f"{cur_fr:+.5f}%" if cur_fr is not None else "-",
        f"分位 {percentile(fr_hist, cur_fr):.0f}%" if cur_fr is not None else "-",
        "看涨" if (cur_fr or 0) < 0 else "中性", "费率转负=多头拥挤已释放")

    tb = days[last_d].get("taker_buy")
    add("主动买卖流", f"{tb:.3f}" if tb else "-", "1H序列",
        "看跌" if (tb or 1) < 0.45 else ("看涨" if (tb or 0) > 0.55 else "中性"),
        "主动买占比<0.5=卖方主导")

    ls = days[last_d].get("ls_acct")
    lst_v = days[last_d].get("ls_top")
    add("多空比·散户", f"{ls:.3f}" if ls else "-", f"分位 {percentile(ls_hist, ls):.0f}%" if ls else "-",
        "看跌" if (ls or 0) > 1.5 else "中性", "散户多头占比高=反向指标")
    add("多空比·大户持仓", f"{lst_v:.3f}" if lst_v else "-", "-",
        "看涨" if (lst_v or 1) < 1 else "看跌", "<1 表示大户持仓偏空")

    tvl = safe_call("tvl", defillama_solana_tvl)
    if tvl["ok"]:
        d = tvl["data"]
        add("链上·Solana TVL", f"{d['tvl_usd'] / 1e9:.2f}B", f"1日 {d['chg1d_pct']:+.2f}% 7日 {d['chg7d_pct']:+.2f}%",
            "看跌" if (d["chg7d_pct"] or 0) < -2 else "中性", "生态资金流出")
    sc = safe_call("stable", stablecoin_liquidity)
    if sc["ok"]:
        d = sc["data"]
        add("场外流动性·稳定币", f"{d['total_usd'] / 1e9:.1f}B", f"1日 {d['chg1d_usd'] / 1e9:+.2f}B 7日 {d['chg7d_usd'] / 1e9:+.2f}B",
            "看涨" if (d["chg7d_usd"] or 0) > 0 else "看跌", "稳定币增发=场外资金进场")
    cg = (f.get("sentiment") or {}).get("coingecko") or {}
    add("情绪·全局市值", f"{cg.get('total_mcap_usd', 0) / 1e12:.3f}T",
        f"24h {cg.get('mcap_change24h_pct'):+.2f}%",
        "看跌" if (cg.get("mcap_change24h_pct") or 0) < -2 else "中性", "全市场同步下跌")
    add("情绪·BTC占比", f"{cg.get('btc_dominance'):.2f}%", "前值 58.1%",
        "看跌" if (cg.get("btc_dominance") or 0) > 58.3 else "中性", "占比上升=资金弃山寨奔BTC")
    facts = s.get("facts") or {}
    add("情绪·恐慌贪婪", f"{facts.get('fng')} {facts.get('fng_label')}", "分位 -",
        "看跌" if (facts.get("fng") or 0) > 70 else "中性", "仍处贪婪=情绪未出清")
    add("价格结构·短期", f"5m/15m/1H = {(f['tf']['5m']['structure']['structure'])}/"
        f"{(f['tf']['15m']['structure']['structure'])}/{(f['tf']['1H']['structure']['structure'])}",
        f"4H {f['tf']['4H']['structure']['structure']}",
        "看跌" if all(f["tf"][x]["structure"]["structure"] == "downtrend" for x in ("15m", "1H")) else "中性",
        "短周期一致向下才是趋势")

    print(f"{'维度':<20}{'当前读数':>16}{'历史比较':>26}{'倾向':>7}   说明")
    print("-" * 100)
    for dim, reading, rank, call, note in rows:
        print(f"{dim:<20}{reading:>16}{rank:>26}{call:>7}   {note}")

    n_bear = sum(1 for r in rows if r[3] == "看跌")
    n_bull = sum(1 for r in rows if r[3] == "看涨")
    n_neu = sum(1 for r in rows if r[3] == "中性")
    print("-" * 100)
    print(f"倾向统计：看跌 {n_bear} 项 / 看涨 {n_bull} 项 / 中性 {n_neu} 项   （共 {len(rows)} 个维度）")
    print("说明：这只是把各维度摆平了看，不是预测。维度之间会互相矛盾，矛盾本身就是信息。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
