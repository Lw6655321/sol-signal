# -*- coding: utf-8 -*-
"""特征工程：把原始数据转成可判断的因子。

全部为纯 Python 实现（实时路径不依赖 pandas），且只使用已发生的、可观测的数据。
没有任何"预测未来"的计算：所有输出都是对当前状态与历史的描述。
"""
from __future__ import annotations

import math
from datetime import datetime, timezone


# --------------------------------------------------------------------------- 基础指标


def sma(vals: list[float], n: int) -> float | None:
    if len(vals) < n or n <= 0:
        return None
    return sum(vals[-n:]) / n


def ema_series(vals: list[float], n: int) -> list[float]:
    if not vals or n <= 0:
        return []
    k = 2.0 / (n + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def ema(vals: list[float], n: int) -> float | None:
    s = ema_series(vals, n)
    return s[-1] if s else None


def rsi(closes: list[float], n: int = 14) -> float | None:
    if len(closes) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - 100 / (1 + rs)


def atr(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> float | None:
    if len(closes) < n + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i - 1]),
                       abs(lows[i] - closes[i - 1])))
    a = sum(trs[:n]) / n
    for i in range(n, len(trs)):
        a = (a * (n - 1) + trs[i]) / n
    return a


def macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> dict | None:
    if len(closes) < slow + signal:
        return None
    ef, es = ema_series(closes, fast), ema_series(closes, slow)
    line = [f - s for f, s in zip(ef, es)]
    sig = ema_series(line, signal)
    return {"macd": line[-1], "signal": sig[-1], "hist": line[-1] - sig[-1],
            "hist_prev": line[-2] - sig[-2] if len(sig) > 1 else None}


def stdev(vals: list[float]) -> float | None:
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))


def percentile_rank(series: list[float], value: float) -> float | None:
    """value 在 series 中的分位（0-100）。用于把指标放到真实历史分布里比较。"""
    if not series:
        return None
    below = sum(1 for x in series if x <= value)
    return below / len(series) * 100.0


def zscore(series: list[float], value: float) -> float | None:
    sd = stdev(series)
    if not sd:
        return None
    return (value - sum(series) / len(series)) / sd


# --------------------------------------------------------------------------- 结构


def pivots(highs: list[float], lows: list[float], k: int = 2) -> dict:
    """分型高低点（左右各 k 根确认），返回最近若干枢轴。"""
    ph, pl = [], []
    for i in range(k, len(highs) - k):
        if all(highs[i] > highs[i - j] for j in range(1, k + 1)) and \
           all(highs[i] >= highs[i + j] for j in range(1, k + 1)):
            ph.append({"i": i, "px": highs[i]})
        if all(lows[i] < lows[i - j] for j in range(1, k + 1)) and \
           all(lows[i] <= lows[i + j] for j in range(1, k + 1)):
            pl.append({"i": i, "px": lows[i]})
    return {"highs": ph[-6:], "lows": pl[-6:]}


def structure_state(highs: list[float], lows: list[float], k: int = 2) -> dict:
    p = pivots(highs, lows, k)
    ph, pl = p["highs"], p["lows"]
    out = {"last_pivot_high": ph[-1]["px"] if ph else None,
           "last_pivot_low": pl[-1]["px"] if pl else None,
           "structure": "unknown"}
    if len(ph) >= 2 and len(pl) >= 2:
        hh = ph[-1]["px"] > ph[-2]["px"]
        hl = pl[-1]["px"] > pl[-2]["px"]
        if hh and hl:
            out["structure"] = "uptrend"
        elif (not hh) and (not hl):
            out["structure"] = "downtrend"
        else:
            out["structure"] = "range"
    return out


def donchian(highs: list[float], lows: list[float], n: int = 20) -> dict | None:
    if len(highs) < n:
        return None
    hi, lo = max(highs[-n:]), min(lows[-n:])
    return {"high": hi, "low": lo, "width_pct": (hi / lo - 1) * 100 if lo else None}


def session_vwap(candles: list[dict], since_utc_day: bool = True) -> float | None:
    """以 UTC 日切分的成交量加权均价：sum(成交额)/sum(成交量)。"""
    if not candles:
        return None
    if since_utc_day:
        last_day = datetime.fromtimestamp(candles[-1]["ts"] / 1000, tz=timezone.utc).date()
        rows = [c for c in candles
                if datetime.fromtimestamp(c["ts"] / 1000, tz=timezone.utc).date() == last_day]
    else:
        rows = candles
    qty = sum(c.get("volBase") or c.get("vol") or 0 for c in rows)
    quote = sum(c.get("volQuote") or 0 for c in rows)
    if not qty or not quote:
        return None
    return quote / qty


# --------------------------------------------------------------------------- K线特征


def candle_features(candles: list[dict], closed_only: bool = False,
                    now_ms: int | None = None) -> dict:
    """单周期完整状态描述。

    未收盘K线的成交量必须按已过去的时间比例折算：
    否则在每根K线开头量比恒为极小值（例如 5m 刚开盘量比只有 0.16），
    使"放量突破"在多数时间里永远无法成立；而在K线末尾又会突然成立。
    """
    rows = [c for c in candles if (c["confirm"] or not closed_only)]
    if len(rows) < 30:
        return {"ok": False, "reason": f"K线不足({len(rows)})"}
    highs = [c["h"] for c in rows]
    lows = [c["l"] for c in rows]
    closes = [c["c"] for c in rows]
    vols = [c.get("volQuote") or c.get("vol") or 0 for c in rows]
    last = rows[-1]

    a = atr(highs, lows, closes, 14)
    vol_avg20 = sma(vols[:-1], 20)
    d = donchian(highs, lows, 20)
    st = structure_state(highs, lows, 2)
    m = macd(closes)
    ema20, ema50, ema200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    chg_1 = (closes[-1] / closes[-2] - 1) * 100 if len(closes) > 1 else None
    chg_4 = (closes[-1] / closes[-5] - 1) * 100 if len(closes) > 4 else None
    chg_20 = (closes[-1] / closes[-21] - 1) * 100 if len(closes) > 20 else None

    body = abs(last["c"] - last["o"])
    rng = max(last["h"] - last["l"], 1e-9)
    upper_wick = last["h"] - max(last["c"], last["o"])
    lower_wick = min(last["c"], last["o"]) - last["l"]

    # ---- 成交量比（未收盘按时间进度折算）
    now = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    ts_all = [c["ts"] for c in rows]
    diffs = sorted({b - a2 for a2, b in zip(ts_all, ts_all[1:]) if b > a2})
    bar_ms = diffs[len(diffs) // 2] if diffs else 0
    progress = None
    if bar_ms:
        progress = max(0.05, min(1.0, (now - ts_all[-1]) / bar_ms))
    raw_ratio = (vols[-1] / vol_avg20) if vol_avg20 else None
    if last["confirm"] or not progress or not vol_avg20:
        vol_ratio = raw_ratio
    else:
        vol_ratio = vols[-1] / (vol_avg20 * progress)
    closed = [c for c in rows if c["confirm"]]
    vol_ratio_closed = None
    if len(closed) >= 21:
        cv = [c.get("volQuote") or c.get("vol") or 0 for c in closed]
        cavg = sma(cv[:-1], 20)
        vol_ratio_closed = (cv[-1] / cavg) if cavg else None

    return {
        "ok": True,
        "bar_ts": last["ts"],
        "bar_utc": datetime.fromtimestamp(last["ts"] / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
        "bar_confirmed": last["confirm"],
        "bar_progress": progress,
        "close": last["c"], "open": last["o"], "high": last["h"], "low": last["l"],
        "chg1_pct": chg_1, "chg4_pct": chg_4, "chg20_pct": chg_20,
        "ema20": ema20, "ema50": ema50, "ema200": ema200,
        "ema_stack": ("bull" if (ema20 and ema50 and ema200 and ema20 > ema50 > ema200)
                      else "bear" if (ema20 and ema50 and ema200 and ema20 < ema50 < ema200)
                      else "mixed"),
        "px_vs_ema20_pct": (last["c"] / ema20 - 1) * 100 if ema20 else None,
        "px_vs_ema50_pct": (last["c"] / ema50 - 1) * 100 if ema50 else None,
        "rsi14": rsi(closes, 14),
        "macd": m,
        "atr14": a,
        "atr_pct": (a / last["c"] * 100) if (a and last["c"]) else None,
        "vol_quote": last.get("volQuote"),
        "vol_ratio20": vol_ratio,
        "vol_ratio20_raw": raw_ratio,
        "vol_ratio20_closed": vol_ratio_closed,
        "donchian20": d,
        "donchian_pos": ((last["c"] - d["low"]) / (d["high"] - d["low"])
                         if d and d["high"] > d["low"] else None),
        "structure": st,
        "vwap_day": session_vwap(rows, True),
        "px_vs_vwap_pct": None,
        "body_pct_of_range": body / rng,
        "upper_wick_pct": upper_wick / rng,
        "lower_wick_pct": lower_wick / rng,
        "bars": len(rows),
    }


# --------------------------------------------------------------------------- 订单簿


def _depth_usd(levels: list[list[float]], mid: float, band_pct: float) -> float:
    lo, hi = mid * (1 - band_pct / 100), mid * (1 + band_pct / 100)
    return sum(px * sz for px, sz in levels if lo <= px <= hi)


def estimate_slippage(levels: list[list[float]], notional_usd: float, mid: float) -> dict | None:
    """按真实盘口逐档吃单，估算指定名义金额的成交均价与滑点。"""
    need, cost, filled = notional_usd, 0.0, 0.0
    for px, sz in levels:
        avail = px * sz
        take = min(avail, need)
        cost += take
        filled += take / px
        need -= take
        if need <= 1e-9:
            break
    if filled <= 0:
        return None
    avg = cost / filled
    return {"avg_px": avg, "fill_pct": (notional_usd - need) / notional_usd * 100,
            "slippage_bps": (avg / mid - 1) * 10000}


def orderbook_features(book: dict, notionals: list[float] | None = None) -> dict:
    bids, asks = book["bids"], book["asks"]
    if not bids or not asks:
        return {"ok": False, "reason": "盘口为空"}
    bb, ba = bids[0][0], asks[0][0]
    mid = (bb + ba) / 2
    micro = (bb * asks[0][1] + ba * bids[0][1]) / (bids[0][1] + asks[0][1])
    bands = {}
    for band in (0.1, 0.25, 0.5, 1.0, 2.0):
        bd = _depth_usd(bids, mid, band)
        ad = _depth_usd(asks, mid, band)
        bands[f"{band}%"] = {"bid_usd": bd, "ask_usd": ad,
                             "imbalance": (bd - ad) / (bd + ad) if (bd + ad) else None}

    def _wall(levels, band=0.5):
        lo, hi = mid * (1 - band / 100), mid * (1 + band / 100)
        inb = [(px, sz) for px, sz in levels if lo <= px <= hi]
        if not inb:
            return None
        px, sz = max(inb, key=lambda x: x[1])
        return {"px": px, "sz": sz, "usd": px * sz,
                "dist_pct": abs(px / mid - 1) * 100}

    ntl = notionals or [50, 100, 200, 500, 1000]
    return {
        "ok": True, "levels_bid": len(bids), "levels_ask": len(asks),
        "best_bid": bb, "best_ask": ba, "mid": mid, "microprice": micro,
        "micro_vs_mid_bps": (micro / mid - 1) * 10000,
        "spread": ba - bb, "spread_bps": (ba - bb) / mid * 10000,
        "top_bid_usd": bb * bids[0][1], "top_ask_usd": ba * asks[0][1],
        "bands": bands,
        "wall_bid": _wall(bids), "wall_ask": _wall(asks),
        "slippage_buy": {str(n): estimate_slippage(asks, n, mid) for n in ntl},
        "slippage_sell": {str(n): estimate_slippage(bids, n, mid) for n in ntl},
    }


# --------------------------------------------------------------------------- 资金流


def taker_flow(rows: list[dict], windows: tuple[int, ...] = (3, 12, 48)) -> dict:
    """主动买/卖量（Rubik，单位 USDT，value = [sell_usd, buy_usd]）。

    注意：rows 的 ts 是区间结束时刻；本函数只做统计，不做时间对齐。
    windows 以"行数"为单位，5m 数据下 3/12/48 行 = 15m/1h/4h。
    """
    out = {}
    for w in windows:
        seg = rows[-w:]
        if not seg:
            continue
        sell = sum(r["value"][0] for r in seg)
        buy = sum(r["value"][1] for r in seg)
        tot = sell + buy
        out[f"{w}bars"] = {
            "buy_usd": buy, "sell_usd": sell,
            "buy_ratio": buy / tot if tot else None,
            "net_usd": buy - sell,
        }
    if rows:
        last = rows[-1]["value"]
        tot = last[0] + last[1]
        out["last_bar"] = {"sell_usd": last[0], "buy_usd": last[1],
                           "buy_ratio": last[1] / tot if tot else None,
                           "net_usd": last[1] - last[0]}
    return out


def oi_series_features(rows: list[dict], windows: tuple[int, ...] = (3, 12, 48)) -> dict:
    """持仓量历史（Rubik value = [oi_usd, vol_usd]，ts 为区间结束时刻）。

    windows 以"行数"为单位：5m 数据下 3/12/48 行 = 15m/1h/4h。
    """
    out = {}
    if not rows:
        return out
    latest = rows[-1]["value"][0]
    for w in windows:
        if len(rows) > w:
            prev = rows[-1 - w]["value"][0]
            out[f"{w}bars_pct"] = (latest / prev - 1) * 100 if prev else None
    out["latest_oi_usd"] = latest
    out["n"] = len(rows)
    return out


def liquidation_pressure(details: list[dict], now_srv_ms: int, window_min: int = 15) -> dict:
    """最近窗口内的强平压力（多空分别统计）。这是真实的爆仓订单数据，不是估算。"""
    cut = now_srv_ms - window_min * 60_000
    seg = [d for d in details if d["ts"] >= cut]
    long_liq = sum(d["px"] * d["sz"] for d in seg if (d.get("posSide") == "long" or d["side"] == "sell"))
    short_liq = sum(d["px"] * d["sz"] for d in seg if (d.get("posSide") == "short" or d["side"] == "buy"))
    tot = long_liq + short_liq
    return {
        "window_min": window_min, "count": len(seg),
        "long_liq_usd": long_liq, "short_liq_usd": short_liq, "total_usd": tot,
        "long_share": long_liq / tot if tot else None,
        "last_ts": seg[-1]["ts"] if seg else None,
        "largest": max((d["px"] * d["sz"] for d in seg), default=0),
    }


def basis_features(components: list[dict], okx_mid: float) -> dict:
    """跨所基差：OKX 价格相对其他交易所溢价/折价。"""
    out = {}
    for c in components:
        if c["exch"] == "OKX":
            continue
        out[c["exch"]] = {"px": c["px"], "basis_bps": (okx_mid / c["px"] - 1) * 10000}
    if out:
        out["_spread_bps"] = (max(v["basis_bps"] for k, v in out.items() if k != "_spread_bps")
                              - min(v["basis_bps"] for k, v in out.items() if k != "_spread_bps"))
    return out


def trade_tape_features(trades: list[dict], p95_sz: float | None = None) -> dict:
    """最近成交明细：主动买卖、大单占比、CVD。"""
    if not trades:
        return {"ok": False}
    buy = sum(t["sz"] for t in trades if t["side"] == "buy")
    sell = sum(t["sz"] for t in trades if t["side"] == "sell")
    tot = buy + sell
    big = [t for t in trades if t["sz"] >= (p95_sz or 0)] if p95_sz else []
    return {
        "ok": True, "n": len(trades),
        "buy_vol": buy, "sell_vol": sell,
        "buy_ratio": buy / tot if tot else None,
        "cvd": buy - sell,
        "big_buy_vol": sum(t["sz"] for t in big if t["side"] == "buy"),
        "big_sell_vol": sum(t["sz"] for t in big if t["side"] == "sell"),
    }
