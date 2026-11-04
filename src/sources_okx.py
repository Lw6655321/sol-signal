# -*- coding: utf-8 -*-
"""OKX 公开数据采集层。

契约：每个函数返回统一信封
    {"dataset", "instId", "source_ts", "fetched_at_ms", "ok", "error", "data"}
- source_ts 一律来自数据源自身返回的时间戳；取不到时为 None。
- 失败时 ok=False、data=None、error=原因。绝不返回上一次的缓存值冒充实时数据。
"""
from __future__ import annotations

from .util import DataError, _env_ok, now_ms, okx

# --------------------------------------------------------------------------- 行情


def server_time() -> dict:
    d = okx("/api/v5/public/time")
    ts = int(d[0]["ts"])
    return _env_ok("server_time", {"server_ms": ts, "server_utc_ms": ts}, source_ts=ts)


def ticker(inst_id: str) -> dict:
    d = okx("/api/v5/market/ticker", {"instId": inst_id})
    r = d[0]
    data = {
        "last": float(r["last"]),
        "bid": float(r["bidPx"]),
        "ask": float(r["askPx"]),
        "bidSz": float(r["bidSz"]),
        "askSz": float(r["askSz"]),
        "open24h": float(r["open24h"]),
        "high24h": float(r["high24h"]),
        "low24h": float(r["low24h"]),
        "vol24h_contracts": float(r["vol24h"]),
        "vol24h_base": float(r.get("volCcy24h") or 0),
        "chg24h_pct": (float(r["last"]) / float(r["open24h"]) - 1) * 100 if float(r["open24h"]) else None,
    }
    return _env_ok("ticker", data, source_ts=int(r["ts"]), instId=inst_id)


def candles(inst_id: str, bar: str, limit: int = 300) -> dict:
    """OKX 返回倒序，这里统一转成时间升序。最后一根通常未收盘(confirm=0)。"""
    d = okx("/api/v5/market/candles", {"instId": inst_id, "bar": bar, "limit": str(limit)})
    rows = []
    for v in d:
        rows.append({
            "ts": int(v[0]),
            "o": float(v[1]), "h": float(v[2]), "l": float(v[3]), "c": float(v[4]),
            "vol": float(v[5]), "volBase": float(v[6]), "volQuote": float(v[7]),
            "confirm": str(v[8]) == "1",
        })
    rows.sort(key=lambda x: x["ts"])
    return _env_ok(f"candles_{bar}", rows, source_ts=rows[-1]["ts"] if rows else None, instId=inst_id)


def mark_price(inst_id: str) -> dict:
    r = okx("/api/v5/public/mark-price", {"instType": "SWAP", "instId": inst_id})[0]
    return _env_ok("mark_price", {"markPx": float(r["markPx"])}, source_ts=int(r["ts"]), instId=inst_id)


def index_ticker(index_id: str) -> dict:
    r = okx("/api/v5/market/index-tickers", {"instId": index_id})[0]
    data = {
        "idxPx": float(r["idxPx"]), "high24h": float(r["high24h"]),
        "low24h": float(r["low24h"]), "open24h": float(r["open24h"]),
        "chg24h_pct": (float(r["idxPx"]) / float(r["open24h"]) - 1) * 100 if float(r["open24h"]) else None,
    }
    return _env_ok("index_ticker", data, source_ts=int(r["ts"]), instId=index_id)


def price_limit(inst_id: str) -> dict:
    r = okx("/api/v5/public/price-limit", {"instId": inst_id})[0]
    return _env_ok("price_limit", {"buyLmt": float(r["buyLmt"]), "sellLmt": float(r["sellLmt"])},
                   source_ts=int(r["ts"]), instId=inst_id)


def instruments(inst_id: str) -> dict:
    r = okx("/api/v5/public/instruments", {"instType": "SWAP", "instId": inst_id})[0]
    keys = ["ctVal", "ctValCcy", "lotSz", "minSz", "tickSz", "lever", "settleCcy", "ctType",
            "maxLmtSz", "maxMktSz", "listTime"]
    # 合约规格属于静态配置，source_ts 记为本次抓取时间（数据本身无时间戳）
    return _env_ok("instruments", {k: r.get(k) for k in keys},
                   source_ts=now_ms(), instId=inst_id)


# --------------------------------------------------------------------------- 订单簿


def order_book(inst_id: str, depth: int = 400) -> dict:
    r = okx("/api/v5/market/books", {"instId": inst_id, "sz": str(depth)})[0]
    bids = [[float(x[0]), float(x[1])] for x in r["bids"]]
    asks = [[float(x[0]), float(x[1])] for x in r["asks"]]
    data = {"bids": bids, "asks": asks, "levels_bid": len(bids), "levels_ask": len(asks)}
    return _env_ok("order_book", data, source_ts=int(r["ts"]), instId=inst_id)


# --------------------------------------------------------------------------- 合约指标


def funding_rate(inst_id: str) -> dict:
    r = okx("/api/v5/public/funding-rate", {"instId": inst_id})[0]
    data = {
        "fundingRate": float(r["fundingRate"]),
        "fundingRate_pct": float(r["fundingRate"]) * 100,
        "nextFundingRate": float(r["nextFundingRate"]) if r.get("nextFundingRate") else None,
        "fundingTime": int(r["fundingTime"]),
        "nextFundingTime": int(r["nextFundingTime"]),
        "prevFundingTime": int(r.get("prevFundingTime") or 0),
        "settFundingRate": float(r["settFundingRate"]) if r.get("settFundingRate") else None,
        "premium": float(r["premium"]) if r.get("premium") else None,
        "interestRate": float(r["interestRate"]) if r.get("interestRate") else None,
        "maxFundingRate": float(r["maxFundingRate"]) if r.get("maxFundingRate") else None,
        "minFundingRate": float(r["minFundingRate"]) if r.get("minFundingRate") else None,
        "settState": r.get("settState"),
    }
    return _env_ok("funding_rate", data, source_ts=int(r["ts"]), instId=inst_id)


def funding_history(inst_id: str, limit: int = 30) -> dict:
    d = okx("/api/v5/public/funding-rate-history", {"instId": inst_id, "limit": str(limit)})
    rows = [{"fundingTime": int(x["fundingTime"]), "fundingRate": float(x["fundingRate"]),
             "realizedRate": float(x.get("realizedRate") or x["fundingRate"])} for x in d]
    rows.sort(key=lambda x: x["fundingTime"])
    rates = [x["fundingRate"] for x in rows]
    data = {
        "rows": rows,
        "last3_cum_pct": sum(rates[-3:]) * 100 if len(rates) >= 3 else None,
        "last9_cum_pct": sum(rates[-9:]) * 100 if len(rates) >= 9 else None,
        "count": len(rows),
    }
    return _env_ok("funding_history", data,
                   source_ts=rows[-1]["fundingTime"] if rows else None, instId=inst_id)


def open_interest(inst_id: str) -> dict:
    r = okx("/api/v5/public/open-interest", {"instType": "SWAP", "instId": inst_id})[0]
    data = {"oi_contracts": float(r["oi"]), "oi_base": float(r["oiCcy"]), "oi_usd": float(r["oiUsd"])}
    return _env_ok("open_interest", data, source_ts=int(r["ts"]), instId=inst_id)


def trades(inst_id: str, limit: int = 100) -> dict:
    d = okx("/api/v5/market/trades", {"instId": inst_id, "limit": str(min(limit, 500))})
    rows = [{"ts": int(x["ts"]), "px": float(x["px"]), "sz": float(x["sz"]), "side": x["side"]}
            for x in d]
    rows.sort(key=lambda x: x["ts"])
    return _env_ok("trades", rows, source_ts=rows[-1]["ts"] if rows else None, instId=inst_id)


def index_components(index_id: str) -> dict:
    """OKX 指数成分 → 无需直连 Binance/Bybit 即可算跨所基差。"""
    r = okx("/api/v5/market/index-components", {"index": index_id})
    comps = [{"exch": c["exch"], "symbol": c["symbol"], "px": float(c["symPx"]),
              "wgt": float(c["wgt"])} for c in r["components"]]
    return _env_ok("index_components", {"components": comps, "index_px": float(r["last"])},
                   source_ts=int(r["ts"]), instId=index_id)


def liquidation_orders(uly: str, state: str = "filled", limit: int = 100) -> dict:
    """注意：该端点必须传 uly（或 instFamily），传 instId 会报 50015。"""
    d = okx("/api/v5/public/liquidation-orders",
            {"instType": "SWAP", "uly": uly, "state": state, "limit": str(limit)})
    details = []
    for block in d:
        for x in block.get("details", []):
            details.append({
                "ts": int(x["ts"]), "px": float(x["bkPx"]), "sz": float(x["sz"]),
                "side": x["side"], "posSide": x.get("posSide"),
                "bkLoss": float(x.get("bkLoss") or 0),
            })
    details.sort(key=lambda x: x["ts"])
    # 新鲜度用「本次抓取时刻」而不是「最新一笔强平的时刻」：
    # 强平是稀疏事件，"最近 N 分钟没有强平"本身就是有效信息，不是数据故障。
    # 若用最新记录的时刻做 source_ts，市场一进入平静期就会判为"过期"并锁死全部信号
    #（真实踩过：无强平 66 分钟 → liquidations 过期 → 闸门全锁）。
    # 窗口内的强平统计由 features.liquidation_pressure 按 ts 现算，不受影响。
    return _env_ok("liquidations", details, source_ts=now_ms(), instId=uly)


# --------------------------------------------------------------------------- Rubik 统计


def _rubik_simple(path: str, params: dict, dataset: str, inst_id: str | None = None) -> dict:
    """Rubik 统计接口。

    实测确认的两条重要事实（用 SOL/BTC、5m/1H 对齐检验得出，勿凭直觉修改）：
    1. 返回体是纯数组的数组，形如 [["<ts>", "v1", "v2"], ...]。
    2. **ts 是统计区间的"结束时刻"，而 OKX K线的 ts 是区间"开始时刻"，两者相差一个周期。**
       对齐规则：与 K 线比较时用 ts - bar_ms。
       依据：taker 净卖出与滞后一根K线的收益相关系数为 -0.45 ~ -0.72；lag=0 时仅 ±0.2 以内。
    """
    d = okx(path, params)
    rows = []
    for x in d:
        vals = x["value"] if isinstance(x, dict) else x
        rows.append({"ts": int(vals[0]), "value": [float(v) for v in vals[1:]]})
    rows.sort(key=lambda x: x["ts"])
    return _env_ok(dataset, rows, source_ts=rows[-1]["ts"] if rows else None, instId=inst_id)


# 各 Rubik 数据集的字段语义（实测确认），单位均为 USDT 计价
RUBIK_FIELDS = {
    "taker_volume": ["sell_usd", "buy_usd"],
    "oi_volume": ["oi_usd", "vol_usd"],
    "ls_account_ratio": ["long_short_account_ratio"],
    "ls_top_account_ratio": ["long_short_account_ratio_top_trader"],
    "ls_top_position_ratio": ["long_short_position_ratio_top_trader"],
    "margin_loan_ratio": ["loan_ratio"],
}
RUBIK_TS_IS_BUCKET_END = True

BAR_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
          "30m": 1_800_000, "1H": 3_600_000, "2H": 7_200_000, "4H": 14_400_000,
          "6H": 21_600_000, "12H": 43_200_000, "1D": 86_400_000}


def rubik_ts_to_candle_ts(ts: int, bar: str) -> int:
    """把 Rubik 的"区间结束时刻"换算成同区间的 K 线"开始时刻"。"""
    return ts - BAR_MS[bar]


def long_short_account_ratio(ccy: str, period: str = "5m") -> dict:
    """全账户多空持仓人数比。"""
    return _rubik_simple("/api/v5/rubik/stat/contracts/long-short-account-ratio",
                         {"ccy": ccy, "period": period}, f"ls_account_ratio_{period}")


def long_short_top_account_ratio(inst_id: str, period: str = "5m") -> dict:
    """精英交易员(大户)多空人数比。"""
    return _rubik_simple("/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader",
                         {"instId": inst_id, "period": period},
                         f"ls_top_account_ratio_{period}", inst_id)


def long_short_top_position_ratio(inst_id: str, period: str = "5m") -> dict:
    """精英交易员(大户)多空持仓量比。"""
    return _rubik_simple("/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader",
                         {"instId": inst_id, "period": period},
                         f"ls_top_position_ratio_{period}", inst_id)


def taker_volume(ccy: str, period: str = "5m") -> dict:
    """主动买入/卖出量：[ts, sellVol, buyVol]。"""
    return _rubik_simple("/api/v5/rubik/stat/taker-volume",
                         {"ccy": ccy, "instType": "CONTRACTS", "period": period},
                         f"taker_volume_{period}")


def open_interest_volume(ccy: str, period: str = "5m") -> dict:
    """持仓量与成交量历史：[ts, oi, vol]。"""
    return _rubik_simple("/api/v5/rubik/stat/contracts/open-interest-volume",
                         {"ccy": ccy, "period": period}, f"oi_volume_{period}")


def margin_loan_ratio(ccy: str, period: str = "5m") -> dict:
    """杠杆借币多空比，反映杠杆情绪。"""
    return _rubik_simple("/api/v5/rubik/stat/margin/loan-ratio",
                         {"ccy": ccy, "period": period}, f"margin_loan_ratio_{period}")
