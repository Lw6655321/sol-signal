# -*- coding: utf-8 -*-
"""补充标定 5m 周期阈值（原 calibrate.py 只覆盖 15m/1H/4H）。

为什么要单独一个脚本而不是改 calibrate.py：
- calibrate.py 已经跑通且负责长周期，不动它可避免把已验证的东西改坏；
- 本脚本只做"新增 5m 指标 + 合并进 thresholds.json"，是纯增量的安全操作。

产出（合并进 config/thresholds.json，保留原有键与 _meta）：
  atr_pct_5m / vol_ratio20_5m / rsi14_5m / donchian_pos_5m / price_chg_5m_pct

数据来源：OKX /api/v5/market/candles bar=5m（实测历史深度约 5 天，用 after 分页）。
所有分位数都来自真实成交K线，样本不足(<30)一律写 null。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.features import atr, donchian, rsi, sma  # noqa: E402
from src.util import (CONFIG_DIR, DATA_DIR, log, okx, read_json,  # noqa: E402
                      sync_server_time, write_json)

INST = "SOL-USDT-SWAP"
PCTS = [2, 5, 10, 25, 50, 75, 90, 95, 98]


def fetch_5m(max_pages: int = 12) -> list[dict]:
    """按 after 分页抓 5m K线，返回时间升序。实测深度约 5 天即会返回空页。"""
    rows: dict[int, dict] = {}
    after = None
    for i in range(max_pages):
        params = {"instId": INST, "bar": "5m", "limit": "300"}
        if after:
            params["after"] = str(after)
        d = okx("/api/v5/market/candles", params, timeout=20)
        if not d:
            log(f"[标定5m] 第{i + 1}页返回空，停止分页")
            break
        new = 0
        for v in d:
            ts = int(v[0])
            if ts not in rows:
                new += 1
            rows[ts] = {"ts": ts, "o": float(v[1]), "h": float(v[2]), "l": float(v[3]),
                        "c": float(v[4]), "vol": float(v[5]), "volBase": float(v[6]),
                        "volQuote": float(v[7]), "confirm": str(v[8]) == "1"}
        seq = sorted(rows)
        log(f"[标定5m] 第{i + 1}页 {len(d)} 根，新增 {new}，覆盖 {seq[0]} ~ {seq[-1]}")
        after = seq[0]
        if new == 0:
            break
    return [rows[k] for k in sorted(rows)]


def pct(sorted_vals: list[float], q: float):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * q / 100.0
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def describe(name: str, series: list[float], note: str = "") -> dict:
    vals = sorted(v for v in series if v is not None)
    n = len(vals)
    out = {"n": n, "note": note, "insufficient": n < 30}
    for q in PCTS:
        out[f"p{q:02d}"] = None if n < 30 else round(pct(vals, q), 8)
    out["min"] = round(vals[0], 8) if vals else None
    out["max"] = round(vals[-1], 8) if vals else None
    return out


def main() -> int:
    sync = sync_server_time()
    print(f"OKX 服务器时间 {sync['server_utc']}   漂移 {sync['offset_ms']}ms")
    rows = fetch_5m()
    if len(rows) < 60:
        print(f"[中止] 5m K线只有 {len(rows)} 根，样本不足，不写入")
        return 1

    closes = [r["c"] for r in rows]
    highs = [r["h"] for r in rows]
    lows = [r["l"] for r in rows]
    vols = [r["volQuote"] for r in rows]

    atr_pct, volr, rsis, dpos, chg = [], [], [], [], []
    for i in range(30, len(rows)):
        h, l, c = highs[:i + 1], lows[:i + 1], closes[:i + 1]
        a = atr(h, l, c, 14)
        if a and c[-1]:
            atr_pct.append(a / c[-1] * 100)
        avg = sma(vols[i - 20:i], 20)
        if avg:
            volr.append(vols[i] / avg)
        r = rsi(c, 14)
        if r is not None:
            rsis.append(r)
        d = donchian(h, l, 20)
        if d and d["high"] > d["low"]:
            dpos.append((c[-1] - d["low"]) / (d["high"] - d["low"]))
        chg.append((closes[i] / closes[i - 1] - 1) * 100)

    metrics = {
        "atr_pct_5m": describe("atr_pct_5m", atr_pct,
                               "5m ATR14/收盘价×100；用于日内止损距离与手续费占比评估"),
        "vol_ratio20_5m": describe("vol_ratio20_5m", volr,
                                   "当根USDT成交额/前20根均值；已收盘K线"),
        "rsi14_5m": describe("rsi14_5m", rsis, "5m RSI14"),
        "donchian_pos_5m": describe("donchian_pos_5m", dpos, "5m 20周期区间位置 0~1"),
        "price_chg_5m_pct": describe("price_chg_5m_pct", chg, "5m 单根涨跌幅%"),
    }

    th = read_json(CONFIG_DIR / "thresholds.json", {}) or {}
    meta = th.get("_meta", {})
    added = {k: v for k, v in metrics.items() if not v["insufficient"]}
    th.update(added)
    meta["five_min_supplement"] = {
        "source": "scripts/calibrate_5m.py",
        "instId": INST, "bar": "5m",
        "bars": len(rows),
        "from_ts": rows[0]["ts"], "to_ts": rows[-1]["ts"],
        "server_offset_ms": sync["offset_ms"],
        "added_metrics": sorted(added),
        "insufficient_metrics": sorted(k for k, v in metrics.items() if v["insufficient"]),
        "note": "5m 历史深度受 OKX 接口限制（实测约 5 天），样本量小于 15m/1H，属弱标定。",
    }
    th["_meta"] = meta
    write_json(CONFIG_DIR / "thresholds.json", th)
    write_json(DATA_DIR / "calibration_5m.json", {"metrics": metrics, "bars": len(rows)})

    print()
    print(f"5m K线 {len(rows)} 根，覆盖 {rows[0]['ts']} ~ {rows[-1]['ts']}")
    print(f"{'指标':<20}{'n':>6}{'p10':>12}{'p50':>12}{'p90':>12}")
    print("-" * 64)
    for k, v in metrics.items():
        flag = " 样本不足->null" if v["insufficient"] else ""
        print(f"{k:<20}{v['n']:>6}{str(v['p10']):>12}{str(v['p50']):>12}{str(v['p90']):>12}{flag}")
    print()
    print("已合并进 config/thresholds.json（未覆盖任何原有键）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
