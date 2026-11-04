# -*- coding: utf-8 -*-
"""外部免费数据源：市场情绪、链上、宏观、跨所价格。
全部为实测可直连的免费源（Binance/Bybit/FRED/Coinglass 在本机被封锁，已排除）。
"""
from __future__ import annotations

from datetime import datetime, timezone

from .util import (DataError, _env_ok, cached_payload, http_json, http_request, iso_ms,
                   iso_to_ms, log, ms_to_iso, now_ms)

SOLANA_RPCS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
]


# --------------------------------------------------------------------------- 情绪


def fear_greed() -> dict:
    j = http_json("https://api.alternative.me/fng/?limit=1")
    x = j["data"][0]
    data = {"value": int(x["value"]), "classification": x["value_classification"],
            "until_update_s": int(x.get("time_until_update") or 0)}
    return _env_ok("fear_greed", data, source_ts=int(x["timestamp"]) * 1000)


def coingecko_global() -> dict:
    j = http_json("https://api.coingecko.com/api/v3/global")["data"]
    data = {
        "total_mcap_usd": float(j["total_market_cap"]["usd"]),
        "mcap_change24h_pct": float(j.get("market_cap_change_percentage_24h_usd") or 0),
        "btc_dominance": float(j["market_cap_percentage"]["btc"]),
        "eth_dominance": float(j["market_cap_percentage"]["eth"]),
        "active_cryptos": int(j.get("active_cryptocurrencies") or 0),
    }
    return _env_ok("coingecko_global", data, source_ts=now_ms())


def coingecko_price(ids: str = "solana,ethereum") -> dict:
    url = ("https://api.coingecko.com/api/v3/simple/price?ids=" + ids +
           "&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true")
    j = http_json(url)
    data = {k: {"usd": float(v["usd"]),
                "vol24h_usd": float(v.get("usd_24h_vol") or 0),
                "chg24h_pct": float(v.get("usd_24h_change") or 0)} for k, v in j.items()}
    return _env_ok("coingecko_price", data, source_ts=now_ms())


# --------------------------------------------------------------------------- 链上


def defillama_solana_tvl() -> dict:
    j = http_json("https://api.llama.fi/v2/historicalChainTvl/Solana")
    rows = [{"date": int(x["date"]) * 1000, "tvl": float(x["tvl"])} for x in j]
    rows.sort(key=lambda x: x["date"])
    last = rows[-1]
    def _at(days: float):
        target = last["date"] - days * 86400_000
        cand = [r for r in rows if r["date"] <= target]
        return cand[-1]["tvl"] if cand else None
    d1, d7, d30 = _at(1), _at(7), _at(30)
    data = {
        "tvl_usd": last["tvl"],
        "chg1d_pct": (last["tvl"] / d1 - 1) * 100 if d1 else None,
        "chg7d_pct": (last["tvl"] / d7 - 1) * 100 if d7 else None,
        "chg30d_pct": (last["tvl"] / d30 - 1) * 100 if d30 else None,
    }
    return _env_ok("defillama_solana_tvl", data, source_ts=last["date"])


def stablecoin_liquidity() -> dict:
    """稳定币总市值变化 = 场外资金进出（链上流动性）。"""
    j = http_json("https://stablecoins.llama.fi/stablecoins?includePrices=false")
    tot = prev_d = prev_w = 0.0
    for a in j["peggedAssets"]:
        if a.get("pegType") != "peggedUSD":
            continue
        c = a.get("circulating", {})
        cur = float(c.get("peggedUSD") or 0)
        pd = float((a.get("circulatingPrevDay") or {}).get("peggedUSD") or cur)
        pw = float((a.get("circulatingPrevWeek") or {}).get("peggedUSD") or cur)
        tot += cur
        prev_d += pd
        prev_w += pw
    data = {
        "total_usd": tot,
        "chg1d_usd": tot - prev_d,
        "chg7d_usd": tot - prev_w,
        "chg1d_pct": (tot / prev_d - 1) * 100 if prev_d else None,
        "chg7d_pct": (tot / prev_w - 1) * 100 if prev_w else None,
    }
    return _env_ok("stablecoin_liquidity", data, source_ts=now_ms())


def solana_rpc_health() -> dict:
    """Solana 网络健康度：epoch/slot/TPS/交易数。多个 RPC 互为备份。"""
    last_err = None
    for base in SOLANA_RPCS:
        try:
            info = http_json(base, method="POST",
                             body={"jsonrpc": "2.0", "id": 1, "method": "getEpochInfo"})
            perf = http_json(base, method="POST",
                             body={"jsonrpc": "2.0", "id": 2, "method": "getRecentPerformanceSamples",
                                   "params": [3]})
            samples = perf.get("result") or []
            tps = None
            if samples:
                tps = round(sum(s["numTransactions"] for s in samples) /
                            max(1, sum(s["samplePeriodSecs"] for s in samples)), 1)
            r = info["result"]
            data = {
                "rpc": base, "epoch": r["epoch"], "absoluteSlot": r["absoluteSlot"],
                "blockHeight": r["blockHeight"], "slotIndex": r["slotIndex"],
                "slotsInEpoch": r["slotsInEpoch"],
                "epoch_progress_pct": round(r["slotIndex"] / r["slotsInEpoch"] * 100, 2),
                "transactionCount": r["transactionCount"], "tps": tps,
            }
            return _env_ok("solana_rpc_health", data, source_ts=now_ms())
        except Exception as e:  # noqa: BLE001
            last_err = f"{base}: {e}"
    raise DataError(f"Solana RPC 全部失败: {last_err}")


# --------------------------------------------------------------------------- 宏观


def cnbc_macro(symbols: dict) -> dict:
    """CNBC 实时报价：美债收益率、股指、VIX、美元指数、黄金。"""
    sym = "|".join(symbols.keys()).replace("@", "%40").replace(".", ".")
    url = ("https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols="
           + sym + "&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json&events=1")
    j = http_json(url)
    out = {}
    newest = 0
    for q in j["FormattedQuoteResult"]["FormattedQuote"]:
        def _f(v):
            if v is None:
                return None
            try:
                return float(str(v).replace("%", "").replace(",", "").replace("+", ""))
            except ValueError:
                return None
        try:
            ts = int(datetime.fromisoformat(q["last_time"]).timestamp() * 1000)
        except Exception:  # noqa: BLE001
            ts = now_ms()
        newest = max(newest, ts)
        out[q["symbol"]] = {
            "name": symbols.get(q["symbol"], q.get("name")),
            "last": _f(q.get("last")), "change": _f(q.get("change")),
            "change_pct": _f(q.get("change_pct")), "ts": ts,
            "asof": q.get("last_time"),
        }
    return _env_ok("cnbc_macro", out, source_ts=newest or now_ms())


_TV_IMPACT = {"1": "High", "0": "Medium", "-1": "Low"}
_CTRY_CCY = {"US": "USD", "EU": "EUR", "GB": "GBP", "JP": "JPY", "CN": "CNY",
             "CA": "CAD", "AU": "AUD", "CH": "CHF", "NZ": "NZD"}


def economic_calendar(*, cache_ttl_s: int = 2 * 3600) -> dict:
    """财经日历（宏观事件窗口）。三级链路：TradingView 主源 → ForexFactory 备源 → 磁盘缓存。

    为什么要链路：ForexFactory 对本机 IP 会硬限流(HTTP 429)，实测已连续失败；
    TradingView 的经济日历接口（其官网挂件使用的公开 JSON）稳定可用，字段为
    importance: 1=高 0=中 -1=低，国家代码为 US/EU/GB/JP/CN，这里统一归一化成
    货币代码(USD/EUR/...)并映射成 High/Medium/Low，与 config.macro 的封锁配置对齐。
    """
    if type(cache_ttl_s) is not int or cache_ttl_s <= 0:
        raise ValueError("cache_ttl_s must be a positive integer")
    now = now_ms()
    lo = iso_ms(now - 12 * 3600_000)
    hi = iso_ms(now + 7 * 86400_000)
    tv_url = ("https://economic-calendar.tradingview.com/events?from=" + lo + "&to=" + hi +
              "&countries=US,EU,GB,JP,CN")

    def _tv():
        j = http_json(tv_url, retries=2, timeout=20,
                      headers={"Origin": "https://www.tradingview.com",
                               "Referer": "https://www.tradingview.com/"})
        out = []
        for e in (j.get("result") or []):
            if not e.get("date"):
                continue
            ctry = (e.get("country") or "").strip()
            out.append({
                "title": (e.get("title") or "").strip(),
                "country": _CTRY_CCY.get(ctry, ctry),
                "impact": _TV_IMPACT.get(str(e.get("importance")), "Low"),
                "ts": iso_to_ms(e["date"]),
                "forecast": e.get("forecast"), "previous": e.get("previous"),
                "actual": e.get("actual"), "period": e.get("period") or "",
                "source": "tradingview",
            })
        out.sort(key=lambda x: x["ts"])
        if not out:
            raise DataError("TradingView 日历返回空")
        return out

    def _ff():
        raw = http_request("https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                           retries=2, timeout=20)
        j = __import__("json").loads(raw.decode("utf-8"))
        out = []
        for e in j:
            if not e.get("date"):
                continue
            out.append({
                "title": (e.get("title") or "").strip(),
                "country": (e.get("country") or "").strip(),
                "impact": (e.get("impact") or "").strip(),
                "ts": iso_to_ms(e["date"]),
                "forecast": e.get("forecast") or "", "previous": e.get("previous") or "",
                "actual": None, "period": "", "source": "forexfactory",
            })
        out.sort(key=lambda x: x["ts"])
        if not out:
            raise DataError("ForexFactory 日历返回空")
        return out

    def _chain():
        errs = []
        for name, fn in (("tradingview", _tv), ("forexfactory", _ff)):
            try:
                ev = fn()
                log(f"财经日历来源: {name}（{len(ev)} 条）")
                return ev
            except Exception as e:  # noqa: BLE001
                errs.append(f"{name}: {e}")
        raise DataError("财经日历全部来源失败 :: " + " | ".join(errs))

    events, fetch_ms, cached = cached_payload("economic_calendar.json", cache_ttl_s, _chain)
    env = _env_ok("economic_calendar", events, source_ts=fetch_ms)
    env["cached"] = cached
    env["provider"] = events[0].get("source") if events else None
    return env


# --------------------------------------------------------------------------- 跨所价格


def kraken_spot(pair: str = "SOLUSD") -> dict:
    j = http_json(f"https://api.kraken.com/0/public/Ticker?pair={pair}")
    if j.get("error"):
        raise DataError(f"Kraken: {j['error']}")
    r = list(j["result"].values())[0]
    data = {"last": float(r["c"][0]), "bid": float(r["b"][0]), "ask": float(r["a"][0]),
            "vol24h": float(r["v"][1])}
    return _env_ok("kraken_spot", data, source_ts=now_ms())


def coinbase_spot(pair: str = "SOL-USD") -> dict:
    j = http_json(f"https://api.coinbase.com/v2/prices/{pair}/spot")
    data = {"last": float(j["data"]["amount"])}
    return _env_ok("coinbase_spot", data, source_ts=now_ms())
