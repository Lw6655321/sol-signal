# -*- coding: utf-8 -*-
"""数据源全量体检：一次性拉取所有数据，打印真实时间戳与新鲜度，并落盘一份快照。

用途：
1. 证明"所有数据都是真实、实时、带交易所时间戳的"，而不是模型记忆或估算。
2. 每次改动数据层后跑一次，确认没有源失效。
运行：  python scripts/probe.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import sources_ext as E          # noqa: E402
from src import sources_okx as O          # noqa: E402
from src.util import (DATA_DIR, is_stale, load_config, log,  # noqa: E402
                      safe_call, server_now_ms, sync_server_time, write_json)

INST = "SOL-USDT-SWAP"
IDX = "SOL-USDT"
ULY = "SOL-USDT"


def main() -> int:
    cfg = load_config()
    sync = sync_server_time()
    print("=" * 78)
    print("SOL 信号系统 · 数据源全量体检")
    print(f"OKX服务器时间 : {sync['server_utc']}   本机/服务器漂移 {sync['offset_ms']} ms "
          f"({'正常' if sync['drift_ok'] else '异常!'})")
    print("=" * 78)

    jobs = [
        ("OKX 行情 · Ticker",          "ticker",                lambda: O.ticker(INST), 20),
        ("OKX 行情 · 15m K线",          "candles_15m",           lambda: O.candles(INST, "15m", 300), 1200),
        ("OKX 行情 · 1H K线",           "candles_1H",            lambda: O.candles(INST, "1H", 300), 4200),
        ("OKX 行情 · 4H K线",           "candles_4H",            lambda: O.candles(INST, "4H", 200), 18000),
        ("OKX 行情 · 标记价",           "mark_price",            lambda: O.mark_price(INST), 20),
        ("OKX 行情 · 指数价",           "index_ticker",          lambda: O.index_ticker(IDX), 20),
        ("OKX 行情 · 涨跌停价",         "price_limit",           lambda: O.price_limit(INST), 20),
        ("OKX 合约 · 规格",             "instruments",           lambda: O.instruments(INST), 86400),
        ("OKX 盘口 · 400档深度",        "order_book",            lambda: O.order_book(INST, 400), 20),
        ("OKX 盘口 · 逐笔成交",         "trades",                lambda: O.trades(INST, 100), 20),
        ("OKX 衍生品 · 当期资金费率",   "funding_rate",          lambda: O.funding_rate(INST), 300),
        ("OKX 衍生品 · 费率历史",       "funding_history",       lambda: O.funding_history(INST, 30), 36000),
        ("OKX 衍生品 · 持仓量",         "open_interest",         lambda: O.open_interest(INST), 60),
        ("OKX 衍生品 · 强平订单",       "liquidations",          lambda: O.liquidation_orders(ULY), 1800),
        ("OKX 跨所 · 指数成分",         "index_components",      lambda: O.index_components(IDX), 60),
        ("OKX 统计 · 全账户多空比",     "ls_account_ratio_5m",   lambda: O.long_short_account_ratio("SOL", "5m"), 900),
        ("OKX 统计 · 大户多空人数比",   "ls_top_account_ratio_5m", lambda: O.long_short_top_account_ratio(INST, "5m"), 900),
        ("OKX 统计 · 大户多空持仓比",   "ls_top_position_ratio_5m", lambda: O.long_short_top_position_ratio(INST, "5m"), 900),
        ("OKX 统计 · 主动买卖量",       "taker_volume_5m",       lambda: O.taker_volume("SOL", "5m"), 900),
        ("OKX 统计 · 持仓量历史",       "oi_volume_5m",          lambda: O.open_interest_volume("SOL", "5m"), 900),
        ("OKX 统计 · 杠杆借币比",       "margin_loan_ratio_5m",  lambda: O.margin_loan_ratio("SOL", "5m"), 900),
        ("外部 · 恐慌贪婪指数",         "fear_greed",            E.fear_greed, 93600),
        ("外部 · CoinGecko 全局",       "coingecko_global",      E.coingecko_global, 3600),
        ("外部 · CoinGecko 现价",       "coingecko_price",       E.coingecko_price, 600),
        ("外部 · Solana 链 TVL",        "defillama_solana_tvl",  E.defillama_solana_tvl, 180000),
        ("外部 · 稳定币总量",           "stablecoin_liquidity",  E.stablecoin_liquidity, 86400),
        ("外部 · Solana RPC 健康度",    "solana_rpc_health",     E.solana_rpc_health, 120),
        ("宏观 · CNBC 多品种",          "cnbc_macro",            lambda: E.cnbc_macro(cfg["macro"]["cnbc_symbols"]), 7200),
        ("宏观 · 财经日历",             "economic_calendar",     E.economic_calendar, 10 ** 9),
        ("跨所 · Kraken 现货",          "kraken_spot",           E.kraken_spot, 120),
        ("跨所 · Coinbase 现货",        "coinbase_spot",         E.coinbase_spot, 120),
    ]

    snapshot, results = {}, []
    for label, key, fn, max_age in jobs:
        env = safe_call(key, fn, INST)
        snapshot[key] = env
        stale, age = is_stale(env["source_ts"], max_age)
        n = len(env["data"]) if isinstance(env["data"], list) else (
            1 if env["data"] is not None else 0)
        results.append((label, key, env, stale, age, n))

    ok_cnt = sum(1 for r in results if r[2]["ok"])
    print(f"{'数据源':<26}{'状态':<6}{'源时间(UTC)':<22}{'年龄':<11}{'条目':<7}摘要")
    print("-" * 118)
    for label, key, env, stale, age, n in results:
        if not env["ok"]:
            print(f"{label:<26}{'失败':<6}{'-':<22}{'-':<11}{'-':<7}{env['error'][:60]}")
            continue
        age_s = "∞" if age == float("inf") else f"{age:.0f}s"
        flag = "过期!" if stale else "新鲜"
        d = env["data"]
        if key.startswith("candles"):
            last = d[-1]
            summ = (f"最新K线 {last['ts'] and __import__('datetime').datetime.utcfromtimestamp(last['ts']/1000).strftime('%m-%d %H:%M')}"
                    f" 收{last['c']} 量{last.get('volQuote') or 0:,.0f} 已收盘={last['confirm']}")
        elif key == "ticker":
            summ = f"最新 {d['last']} 买/卖 {d['bid']}/{d['ask']} 24h量 {d['vol24h_base']:,.0f} SOL"
        elif key == "order_book":
            summ = f"买/卖档 {d['levels_bid']}/{d['levels_ask']}  最优 {d['bids'][0][0]}/{d['asks'][0][0]}"
        elif key == "funding_rate":
            summ = f"当期 {d['fundingRate_pct']:.5f}%  下次 {d['nextFundingTime']}"
        elif key == "funding_history":
            summ = f"{d['count']}期  近3期累计 {d['last3_cum_pct']:.5f}%"
        elif key == "open_interest":
            summ = f"持仓 {d['oi_contracts']:,.0f} 张 = {d['oi_usd']/1e6:.1f}M USD"
        elif key == "liquidations":
            summ = f"最近 {len(d)} 笔强平  最大 {max((x['px']*x['sz'] for x in d), default=0):,.0f} USD"
        elif key == "index_components":
            summ = " | ".join(f"{c['exch']} {c['px']}" for c in d["components"])
        elif key.startswith("ls_") or key == "margin_loan_ratio_5m":
            summ = f"最新 {d[-1]['value']} ({(len(d))} 期)"
        elif key in ("taker_volume_5m", "oi_volume_5m"):
            summ = f"最新 {[round(x,2) for x in d[-1]['value']]} ({len(d)} 期)"
        elif key == "fear_greed":
            summ = f"{d['value']} {d['classification']}"
        elif key == "coingecko_global":
            summ = f"总市值 {d['total_mcap_usd']/1e12:.3f}T  24h {d['mcap_change24h_pct']:+.2f}%  BTC占比 {d['btc_dominance']:.1f}%"
        elif key == "coingecko_price":
            summ = " | ".join(f"{k} ${v['usd']} ({v['chg24h_pct']:+.2f}%)" for k, v in d.items())
        elif key == "defillama_solana_tvl":
            summ = f"TVL {d['tvl_usd']/1e9:.2f}B  7d {d['chg7d_pct']:+.2f}%"
        elif key == "stablecoin_liquidity":
            summ = f"稳定币 {d['total_usd']/1e9:.1f}B  7d {d['chg7d_usd']/1e9:+.2f}B ({d['chg7d_pct']:+.2f}%)"
        elif key == "solana_rpc_health":
            summ = f"epoch {d['epoch']} ({d['epoch_progress_pct']}%)  TPS {d['tps']}"
        elif key == "cnbc_macro":
            summ = " | ".join(f"{k} {v['last']} ({v['change_pct']:+.2f}%)" for k, v in d.items())
        elif key == "economic_calendar":
            now = server_now_ms()
            nxt = [e for e in d if e["ts"] > now][:1]
            summ = (f"{len(d)} 条事件；下一个: {nxt[0]['ts'] and __import__('datetime').datetime.utcfromtimestamp(nxt[0]['ts']/1000).strftime('%m-%d %H:%M')} "
                    f"{nxt[0]['country']}/{nxt[0]['impact']}/{nxt[0]['title'][:26]}" if nxt else f"{len(d)} 条事件，无未来事件")
        elif key in ("kraken_spot", "coinbase_spot"):
            summ = f"${d['last']}"
        else:
            summ = str(d)[:70]
        print(f"{label:<26}{flag:<6}{str(env['source_utc']):<22}{age_s:<11}{n:<7}{summ}")
    print("-" * 118)
    print(f"成功 {ok_cnt}/{len(results)} 个数据源")
    if ok_cnt < len(results):
        print("失败列表：" + ", ".join(f"{r[1]}({r[2]['error'][:40]})" for r in results if not r[2]["ok"]))

    out = DATA_DIR / "probe_latest.json"
    write_json(out, {"generated_at_ms": server_now_ms(), "sync": sync, "sources": snapshot})
    log(f"体检快照已写入 {out}")
    return 0 if ok_cnt == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
