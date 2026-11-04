# -*- coding: utf-8 -*-
"""快照采集与因子装配。

一次 collect() 产出：
- sources : 每个数据源的原始信封（含 source_utc / fetched_at_ms / ok / error）
- features: 计算好的因子（趋势、波动、盘口、主动流、持仓、强平、基差、宏观、链上）
- facts   : 供仪表盘直接展示的关键实时事实
- freshness/health : 哪些数据过期或缺失

"所有数据必须符合真实交易时间" 的落实：
1. 每个源都带数据源自身的时间戳，并按 config 的 stale_seconds 判定是否过期；
2. 过期/缺失的源会导致信号引擎直接拒发信号（宁可不做，也不用旧数据做）。
"""
from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from . import features as F
from . import sources_ext as E
from . import sources_okx as O
from .util import (STATE_DIR, is_stale, load_config, load_thresholds, log, ms_to_iso,
                   now_ms, safe_call, server_now_ms, sync_server_time, write_json)

# 需要落盘给仪表盘的较重数据集（不落原始档位与K线，落摘要）
HEAVY = {"order_book", "candles_5m", "candles_15m", "candles_1H", "candles_4H", "trades"}


def _calendar_max_age(calendar_max_age_s: int | None) -> int | None:
    """The execution-only override may tighten collection, never relax its 30m limit."""
    if calendar_max_age_s is not None and (
            type(calendar_max_age_s) is not int or not 0 < calendar_max_age_s <= 1800):
        raise ValueError("calendar_max_age_s must be an integer from 1 to 1800")
    return calendar_max_age_s


def build_tasks(cfg: dict, *, calendar_max_age_s: int | None = None) -> list[tuple]:
    calendar_max_age_s = _calendar_max_age(calendar_max_age_s)
    s = cfg["symbols"][0]
    inst, ccy, idx = s["instId"], s["ccy"], s["index"]
    uly = f"{ccy}-USDT"
    p = cfg["data"]["poll_seconds"]
    depth = int(cfg["data"]["depth_levels"])
    calendar_poll = p["calendar"]
    calendar_fetch = E.economic_calendar
    if calendar_max_age_s is not None:
        calendar_poll = min(calendar_poll, calendar_max_age_s)
        calendar_fetch = lambda: E.economic_calendar(cache_ttl_s=calendar_max_age_s)
    return [
        ("ticker",              p["ticker"],        lambda: O.ticker(inst)),
        ("order_book",          p["orderbook"],     lambda: O.order_book(inst, depth)),
        ("trades",              p["ticker"],        lambda: O.trades(inst, 100)),
        ("mark_price",          p["ticker"],        lambda: O.mark_price(inst)),
        ("index_ticker",        p["ticker"],        lambda: O.index_ticker(idx)),
        ("price_limit",         60,                 lambda: O.price_limit(inst)),
        ("instruments",         86400,              lambda: O.instruments(inst)),
        ("candles_5m",          p["candles"],       lambda: O.candles(inst, "5m", 300)),
        ("candles_15m",         p["candles"],       lambda: O.candles(inst, "15m", 300)),
        ("candles_1H",          p["candles"],       lambda: O.candles(inst, "1H", 300)),
        ("candles_4H",          p["candles"],       lambda: O.candles(inst, "4H", 200)),
        ("funding_rate",        p["funding"],       lambda: O.funding_rate(inst)),
        ("funding_history",     3600,               lambda: O.funding_history(inst, 30)),
        ("open_interest",       p["open_interest"], lambda: O.open_interest(inst)),
        ("liquidations",        60,                 lambda: O.liquidation_orders(uly)),
        ("index_components",    60,                 lambda: O.index_components(idx)),
        ("taker_volume_5m",     p["rubik"],         lambda: O.taker_volume(ccy, "5m")),
        ("taker_volume_1H",     p["rubik"],         lambda: O.taker_volume(ccy, "1H")),
        ("oi_volume_5m",        p["rubik"],         lambda: O.open_interest_volume(ccy, "5m")),
        ("oi_volume_1H",        p["rubik"],         lambda: O.open_interest_volume(ccy, "1H")),
        ("ls_account_ratio_5m", p["rubik"],         lambda: O.long_short_account_ratio(ccy, "5m")),
        ("ls_top_pos_ratio_5m", p["rubik"],         lambda: O.long_short_top_position_ratio(inst, "5m")),
        ("margin_loan_ratio_5m", p["rubik"],        lambda: O.margin_loan_ratio(ccy, "5m")),
        ("fear_greed",          p["sentiment"],     E.fear_greed),
        ("coingecko_global",    p["sentiment"],     E.coingecko_global),
        ("defillama_solana_tvl", p["onchain"],      E.defillama_solana_tvl),
        ("stablecoin_liquidity", p["onchain"],      E.stablecoin_liquidity),
        ("solana_rpc_health",   p["onchain"],       E.solana_rpc_health),
        ("cnbc_macro",          p["macro"],         lambda: E.cnbc_macro(cfg["macro"]["cnbc_symbols"])),
        ("economic_calendar",   calendar_poll,      calendar_fetch),
        ("kraken_spot",         60,                 E.kraken_spot),
        ("coinbase_spot",       60,                 E.coinbase_spot),
    ]


def collect(cfg: dict | None = None, prev: dict | None = None, force: bool = False,
            *, calendar_max_age_s: int | None = None) -> dict:
    """按各源自己的轮询周期增量刷新；未到期的源复用上一轮结果（并保留其真实抓取时刻）。

    并行抓取：32 个源串行要 30 秒以上，会让冷启动时的盘口数据"开局就是旧的"。
    改为 8 线程并发，冷启动缩短到数秒，信号用到的盘口/报价始终是新的。
    """
    calendar_max_age_s = _calendar_max_age(calendar_max_age_s)
    cfg = cfg or load_config()
    prev_src = (prev or {}).get("sources", {})
    t = now_ms()
    sources, reused = {}, 0
    to_run = []
    tasks = (build_tasks(cfg) if calendar_max_age_s is None else
             build_tasks(cfg, calendar_max_age_s=calendar_max_age_s))
    for key, poll_s, fn in tasks:
        old = prev_src.get(key)
        reusable = (not force) and old and old.get("ok")
        if reusable:
            if calendar_max_age_s is not None and key == "economic_calendar":
                # A failed upstream may repackage an older disk cache. Execution
                # polls from the original source time, so recovery is tried next scan.
                stamp = old.get("source_ts")
                reusable = (type(stamp) is int and 0 < stamp <= t and
                            (t - stamp) < poll_s * 1000)
            else:
                reusable = (t - old["fetched_at_ms"]) < poll_s * 1000
        if reusable:
            sources[key] = old
            reused += 1
        else:
            to_run.append((key, fn))
    inst = cfg["symbols"][0]["instId"]
    if to_run:
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="fetch") as ex:
            futs = {ex.submit(safe_call, key, fn, inst): key for key, fn in to_run}
            for fut in as_completed(futs):
                try:
                    sources[futs[fut]] = fut.result()
                except Exception as e:  # noqa: BLE001
                    sources[futs[fut]] = {"dataset": futs[fut], "ok": False, "data": None,
                                          "source_ts": None, "fetched_at_ms": now_ms(),
                                          "error": f"{type(e).__name__}: {e}"}

    sync = (prev or {}).get("server_sync") or sync_server_time()
    snap = {
        "generated_at_ms": t,
        "generated_at_utc": ms_to_iso(server_now_ms()),
        "server_sync": sync,
        "inst": cfg["symbols"][0]["instId"],
        "symbol": cfg["symbols"][0],
        "reused_sources": reused,
        "sources": sources,
    }
    snap["features"] = build_features(snap, cfg)
    snap["facts"] = build_facts(snap, cfg)
    snap["freshness"], snap["health"] = check_freshness(sources, cfg)
    return snap


# --------------------------------------------------------------------------- 因子


def _src(sources: dict, key: str):
    env = sources.get(key)
    if not env or not env.get("ok"):
        return None
    return env["data"]


def build_features(snap: dict, cfg: dict) -> dict:
    src = snap["sources"]
    th = load_thresholds()
    out: dict = {}

    # ---- 各周期K线状态
    tf_map = {"5m": "candles_5m", "15m": "candles_15m", "1H": "candles_1H", "4H": "candles_4H"}
    tf = {}
    for name, key in tf_map.items():
        rows = _src(src, key)
        if not rows:
            tf[name] = {"ok": False, "reason": "无数据"}
            continue
        # 高周期只用已收盘K线，避免"重绘"；低周期保留未收盘并标注
        f = F.candle_features(rows, closed_only=(name in ("1H", "4H")),
                              now_ms=server_now_ms())
        if f.get("ok"):
            f["vwap_ok"] = f.get("vwap_day") is not None
            if f.get("vwap_day"):
                f["px_vs_vwap_pct"] = (f["close"] / f["vwap_day"] - 1) * 100
        tf[name] = f
    out["tf"] = tf

    # ---- 盘口与深度
    book = _src(src, "order_book")
    if book:
        bf = F.orderbook_features(book, notionals=[50, 100, 200, 300, 500])
        out["book"] = bf
        if bf.get("ok") and th.get("spread_bps"):
            bf["spread_percentile"] = F.percentile_rank(th["spread_bps"].get("samples") or [], bf["spread_bps"])
    else:
        out["book"] = {"ok": False, "reason": "无盘口数据"}

    # ---- 主动买卖流（Rubik，ts=区间结束）
    for key, tag, wins in (("taker_volume_5m", "5m", (3, 12, 48)),
                           ("taker_volume_1H", "1H", (1, 4, 24))):
        rows = _src(src, key)
        if rows:
            fl = F.taker_flow(rows, wins)
            fl["latest_bucket_end_utc"] = ms_to_iso(rows[-1]["ts"])
            if tag == "5m":
                br = fl.get("3bars", {}).get("buy_ratio")   # 15m
                if br is not None and th.get("taker_buy_ratio_15m"):
                    fl["buy_ratio_15m_pct_rank"] = F.percentile_rank(
                        th["taker_buy_ratio_15m"].get("samples") or [], br)
            out[f"taker_{tag}"] = fl

    # ---- 持仓量历史（Rubik，ts=区间结束）
    oi = {}
    for key, tag, wins in (("oi_volume_5m", "5m", (3, 12, 48)),
                           ("oi_volume_1H", "1H", (4, 24))):
        rows = _src(src, key)
        if rows:
            s = F.oi_series_features(rows, wins)
            s["latest_bucket_end_utc"] = ms_to_iso(rows[-1]["ts"])
            oi[tag] = s
    cur_oi = _src(src, "open_interest")
    if cur_oi:
        oi["current"] = cur_oi
    out["oi"] = oi

    # ---- 强平压力
    liq = _src(src, "liquidations")
    if liq is not None:
        out["liquidation"] = {
            "w15": F.liquidation_pressure(liq, server_now_ms(), 15),
            "w60": F.liquidation_pressure(liq, server_now_ms(), 60),
        }

    # ---- 跨所基差
    comps = _src(src, "index_components")
    tick = _src(src, "ticker")
    if comps and tick:
        out["basis"] = F.basis_features(comps["components"], tick["last"])

    # ---- 逐笔成交
    trades = _src(src, "trades")
    if trades:
        szs = sorted(t["sz"] for t in trades)
        p95 = szs[int(len(szs) * 0.95)] if szs else None
        out["tape"] = F.trade_tape_features(trades, p95)
        out["tape"]["p95_sz"] = p95

    # ---- 资金费率（并放入历史分布）
    fr = _src(src, "funding_rate")
    fh = _src(src, "funding_history")
    if fr:
        fd = dict(fr)
        fd["next_funding_utc"] = ms_to_iso(fr["nextFundingTime"])
        fd["settlement_in_min"] = round((fr["nextFundingTime"] - server_now_ms()) / 60000, 1)
        if fh:
            fd.update({k: fh[k] for k in ("last3_cum_pct", "last9_cum_pct") if k in fh})
            rates = [r["fundingRate"] * 100 for r in fh["rows"]]
            fd["hist_pct_rank"] = F.percentile_rank(rates, fr["fundingRate_pct"])
            fd["hist_bars"] = len(rates)
            if th.get("funding_rate_pct"):
                fd["calibrated_rank"] = F.percentile_rank(
                    th["funding_rate_pct"].get("samples") or [], fr["fundingRate_pct"])
        out["funding"] = fd

    # ---- 多空比 / 杠杆情绪
    ls = {}
    for key, tag in (("ls_account_ratio_5m", "account"),
                     ("ls_top_pos_ratio_5m", "top_trader_position"),
                     ("margin_loan_ratio_5m", "margin_loan")):
        rows = _src(src, key)
        if rows:
            vals = [r["value"][0] for r in rows]
            ls[tag] = {"latest": vals[-1], "pct_rank": F.percentile_rank(vals, vals[-1]),
                       "chg_1h": (vals[-1] / vals[-13] - 1) * 100 if len(vals) > 13 else None,
                       "n": len(vals), "bucket_end_utc": ms_to_iso(rows[-1]["ts"])}
    out["positioning"] = ls

    # ---- 情绪 / 宏观 / 链上
    fng = _src(src, "fear_greed")
    cg = _src(src, "coingecko_global")
    out["sentiment"] = {"fng": fng, "coingecko": cg}

    macro = _src(src, "cnbc_macro")
    cal = _src(src, "economic_calendar")
    out["macro"] = _macro_state(macro, cal, cfg)

    oc = {}
    if (v := _src(src, "solana_rpc_health")):
        oc["solana"] = v
    if (v := _src(src, "defillama_solana_tvl")):
        oc["solana_tvl"] = v
    if (v := _src(src, "stablecoin_liquidity")):
        oc["stablecoins"] = v
    out["onchain"] = oc

    # ---- 跨所现价
    ks, cs = _src(src, "kraken_spot"), _src(src, "coinbase_spot")
    if tick and (ks or cs):
        px = tick["last"]
        out["xvenue"] = {
            "okx": px,
            "kraken": (ks or {}).get("last"),
            "coinbase": (cs or {}).get("last"),
            "kraken_bps": (px / ks["last"] - 1) * 10000 if ks else None,
            "coinbase_bps": (px / cs["last"] - 1) * 10000 if cs else None,
        }
    return out


def _macro_state(macro: dict | None, cal: list | None, cfg: dict) -> dict:
    now = server_now_ms()
    mc = cfg["macro"]
    state: dict = {"quotes": macro or {}}
    if macro:
        def _chg(sym):
            v = macro.get(sym)
            return v["change_pct"] if v and v["change_pct"] is not None else None
        vix = (macro.get(".VIX") or {}).get("last")
        score = 0
        if (_chg(".SPX") or 0) < -0.5:
            score += 1
        if (vix or 0) > 20:
            score += 1
        if (_chg(".DXY") or 0) > 0.3:
            score += 1
        if (_chg("US10Y") or 0) > 0.5:
            score += 1
        state["risk_regime"] = ("risk_off" if score >= 3 else "risk_on" if score <= 1 else "neutral")
        state["risk_score"] = score
        state["vix"] = vix
    if cal is not None:
        b_before = int(mc["blackout_minutes_before"]) * 60000
        b_after = int(mc["blackout_minutes_after"]) * 60000
        blocking = [e for e in cal
                    if e["impact"] in mc["block_impact"] and e["country"] in mc["block_currencies"]
                    and (e["ts"] - b_before) <= now <= (e["ts"] + b_after)]
        upcoming = [e for e in cal
                    if e["ts"] > now and e["impact"] in mc["block_impact"]
                    and e["country"] in mc["block_currencies"]][:4]
        state["blackout"] = bool(blocking)
        state["blackout_events"] = [{"title": e["title"], "country": e["country"],
                                     "utc": ms_to_iso(e["ts"]), "impact": e["impact"]}
                                    for e in blocking]
        state["upcoming_high_impact"] = [{"title": e["title"], "country": e["country"],
                                          "utc": ms_to_iso(e["ts"]), "impact": e["impact"],
                                          "in_min": round((e["ts"] - now) / 60000, 0)}
                                         for e in upcoming]
    return state


# --------------------------------------------------------------------------- 事实


def build_facts(snap: dict, cfg: dict) -> dict:
    src, f = snap["sources"], snap["features"]
    tick = _src(src, "ticker")
    oi = _src(src, "open_interest")
    fr = _src(src, "funding_rate")
    book = f.get("book") or {}
    facts = {
        "instId": snap["inst"],
        "price": tick["last"] if tick else None,
        "bid": tick["bid"] if tick else None,
        "ask": tick["ask"] if tick else None,
        "spread_bps": book.get("spread_bps"),
        "chg24h_pct": tick["chg24h_pct"] if tick else None,
        "high24h": tick["high24h"] if tick else None,
        "low24h": tick["low24h"] if tick else None,
        "vol24h_base": tick["vol24h_base"] if tick else None,
        "oi_usd": oi["oi_usd"] if oi else None,
        "funding_pct": fr["fundingRate_pct"] if fr else None,
        "next_funding_utc": ms_to_iso(fr["nextFundingTime"]) if fr else None,
        "slippage_buy_100u_bps": ((book.get("slippage_buy") or {}).get("100") or {}).get("slippage_bps"),
        "depth_imbalance_05": ((book.get("bands") or {}).get("0.5%") or {}).get("imbalance"),
    }
    liq = f.get("liquidation", {}).get("w15")
    if liq:
        facts["liq_15m_usd"] = liq["total_usd"]
        facts["liq_15m_long_share"] = liq["long_share"]
    mac = f.get("macro", {})
    facts["risk_regime"] = mac.get("risk_regime")
    facts["macro_blackout"] = mac.get("blackout")
    facts["vix"] = mac.get("vix")
    fng = (f.get("sentiment") or {}).get("fng")
    facts["fng"] = fng["value"] if fng else None
    facts["fng_label"] = fng["classification"] if fng else None
    t15 = (f.get("tf") or {}).get("15m") or {}
    facts["atr_pct_15m"] = t15.get("atr_pct")
    facts["rsi14_15m"] = t15.get("rsi14")
    facts["structure_15m"] = (t15.get("structure") or {}).get("structure")
    facts["structure_1H"] = ((f.get("tf") or {}).get("1H") or {}).get("structure", {}).get("structure")
    facts["structure_4H"] = ((f.get("tf") or {}).get("4H") or {}).get("structure", {}).get("structure")
    return facts


# --------------------------------------------------------------------------- 健康度


def check_freshness(sources: dict, cfg: dict) -> tuple[dict, dict]:
    """区分"关键源"与"辅助源"：
    - 关键源缺失/过期 → usable=False，直接拒发信号。
    - 辅助源缺失/过期 → degraded=True，只告警并提高触发分数线，不让系统整体瘫痪。
    """
    stale_cfg = cfg["data"]["stale_seconds"]
    critical = set(cfg["data"].get("critical_sources") or [])
    stale, missing, ages = [], [], {}
    c_stale, c_missing, a_stale, a_missing = [], [], [], []
    for key, env in sources.items():
        is_crit = key in critical
        if not env.get("ok"):
            missing.append({"source": key, "error": env.get("error"), "critical": is_crit})
            (c_missing if is_crit else a_missing).append(key)
            continue
        max_age = stale_cfg.get(key, 900)
        is_old, age = is_stale(env.get("source_ts"), max_age)
        ages[key] = None if age == float("inf") else round(age, 1)
        if is_old:
            stale.append({"source": key, "age_s": ages[key], "max_age_s": max_age,
                          "critical": is_crit})
            (c_stale if is_crit else a_stale).append(key)
    health = {
        "ok": len([1 for e in sources.values() if e.get("ok")]),
        "failed": len(missing),
        "stale": len(stale),
        "usable": not (c_missing or c_stale),
        "degraded": bool(a_missing or a_stale),
        "critical_missing": c_missing, "critical_stale": c_stale,
        "advisory_missing": a_missing, "advisory_stale": a_stale,
        "ages_s": ages,
    }
    return {"stale": stale, "missing": missing}, health


def trimmed(snap: dict) -> dict:
    """落盘给仪表盘的轻量快照（去掉原始档位与K线）。"""
    out = copy.deepcopy({k: v for k, v in snap.items() if k != "sources"})
    out["sources"] = {}
    for key, env in snap["sources"].items():
        out["sources"][key] = {
            "ok": env.get("ok"), "source_ts": env.get("source_ts"),
            "source_utc": env.get("source_utc"), "fetched_at_ms": env.get("fetched_at_ms"),
            "error": env.get("error"),
            "n": len(env["data"]) if isinstance(env.get("data"), list) else (
                1 if env.get("data") is not None else 0),
        }
    return out
