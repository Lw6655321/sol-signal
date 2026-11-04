# -*- coding: utf-8 -*-
"""阈值标定脚本：用 OKX 真实历史数据算出各因子的历史分位数，写入 config/thresholds.json。

用途
----
SOL 永续合约实时多因子信号引擎**不允许拍脑袋定阈值**。本脚本抓取真实历史数据，
对每个因子计算历史分布的分位数（p10/p50/p90 等），供信号引擎判定
"当前值是否异常"。本脚本只做**历史分布的统计描述**，不含任何预测逻辑。

运行方式（Windows，必须用内置 Python，系统 python 是微软商店占位符）
------------------------------------------------------------------
    $env:PYTHONIOENCODING='utf-8'
    python -X utf8 -B scripts/calibrate.py

产出
----
    config/thresholds.json          —— 阈值表（含 _meta）
    data/calibration_samples.json   —— 标定中间样本（各指标分位数与样本数）

实测确认的 OKX 接口事实（勿凭直觉改）
------------------------------------
1. /api/v5/market/candles：单次 limit 最大 300，返回**倒序**，分页参数 after=<ts>
   返回 ts **小于**该值的数据。
2. /api/v5/public/funding-rate-history：支持 after 分页，单次最大 100，8 小时一期。
3. Rubik 统计接口：
   - period 只支持 [5m, 1H, 1D]（部分接口额外支持 15m/4H）。
   - taker-volume 默认且最多只返回 **72 行**；open-interest-volume /
     long-short-account-ratio / margin/loan-ratio 返回 **576 行**(5m) / **720 行**(1H)；
     long-short-*-top-trader 最多 **100 行**。limit 参数**不生效**。
   - Rubik 的 ts 是统计区间的**结束时刻**，K线 ts 是区间**开始时刻**，相差一个周期，
     必须用 sources_okx.rubik_ts_to_candle_ts(ts, bar) 换算后再合并。

样本不足的处理
--------------
任何分位数指标的样本数 n < 30 时，该阈值一律写 null，并在 stdout 明确打印"样本不足"。
绝不编造数字。

幂等性
------
脚本可重复运行：每次全量重新抓取并覆盖写出两个 JSON（tmp + os.replace 原子替换）。
"""
from __future__ import annotations

import bisect
import math
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# --------------------------------------------------------------------------- 路径 / 模块

SCRIPT_PATH = Path(__file__).resolve()
ROOT = SCRIPT_PATH.parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data"
CONFIG_DIR = ROOT / "config"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
THRESHOLDS_PATH = CONFIG_DIR / "thresholds.json"
SAMPLES_PATH = DATA_DIR / "calibration_samples.json"

from src import features as F                     # noqa: E402
from src import sources_okx as S                  # noqa: E402
from src import util as U                         # noqa: E402

INST_ID = "SOL-USDT-SWAP"
CCY = "SOL"

# 目标样本量（抓不到就如实记录实际覆盖范围，绝不伪造）
TARGET_15M = 2880      # 30 天
TARGET_1H = 720        # 30 天
TARGET_4H = 180        # 30 天
TARGET_FUNDING = 90    # 30 天，8 小时一期
MIN_N = 30             # 低于此样本数的阈值写 null

BAR_MS = S.BAR_MS
ALIGN_TOLERANCE_MS = 200_000   # Rubik 对齐容差：最近邻样本与目标时刻的最大偏差


# --------------------------------------------------------------------------- 抓取工具


class FetchReport:
    """记录每个数据源的实际覆盖范围与失败原因。"""

    def __init__(self) -> None:
        self.sources: dict[str, dict] = {}
        self.failed: list[dict] = []

    def ok(self, name: str, rows: list, ts_key=lambda r: r) -> None:
        ts = sorted(int(ts_key(r)) for r in rows)
        self.sources[name] = {
            "status": "ok",
            "n": len(rows),
            "first_ts": ts[0] if ts else None,
            "last_ts": ts[-1] if ts else None,
            "first_utc": U.ms_to_iso(ts[0]) if ts else None,
            "last_utc": U.ms_to_iso(ts[-1]) if ts else None,
            "span_hours": round((ts[-1] - ts[0]) / 3_600_000, 2) if len(ts) > 1 else 0.0,
        }

    def empty(self, name: str, note: str = "接口返回空") -> None:
        self.sources[name] = {"status": "empty", "n": 0, "note": note,
                              "first_ts": None, "last_ts": None,
                              "first_utc": None, "last_utc": None, "span_hours": 0.0}
        self.failed.append({"source": name, "error": note})

    def fail(self, name: str, err: Exception) -> None:
        msg = f"{type(err).__name__}: {err}"
        self.sources[name] = {"status": "failed", "n": 0, "note": msg,
                              "first_ts": None, "last_ts": None,
                              "first_utc": None, "last_utc": None, "span_hours": 0.0}
        self.failed.append({"source": name, "error": msg})
        U.log(f"[标定-数据源失败] {name}: {msg}", "WARN")


REPORT = FetchReport()


def paginate(fetch_page, key, target_n: int, max_pages: int = 25, pause: float = 0.2,
             trace: str = "") -> tuple[list, list[dict]]:
    """通用倒序分页：fetch_page(cursor) -> 行列表（按 ts 降序或升序均可）。

    cursor 为 None 表示首页；否则传入"上一页最老的时间戳"。
    返回 (按 key 升序去重的行列表, 每页轨迹)。
    OKX 对每类历史都有一个"滚动窗口"限制，超出窗口后接口返回空；遇到空页即停止。
    """
    seen: dict[int, object] = {}
    pages: list[dict] = []
    cursor = None
    for pno in range(1, max_pages + 1):
        rows = fetch_page(cursor)
        if not rows:
            pages.append({"page": pno, "cursor": cursor, "rows": 0, "note": "接口返回空，历史窗口已到尽头"})
            break
        keys = [int(key(r)) for r in rows]
        oldest, newest = min(keys), max(keys)
        new = 0
        for r in rows:
            k = int(key(r))
            if k not in seen:
                seen[k] = r
                new += 1
        pages.append({"page": pno, "cursor": cursor, "rows": len(rows), "new": new,
                      "oldest": oldest, "newest": newest,
                      "oldest_utc": U.ms_to_iso(oldest), "newest_utc": U.ms_to_iso(newest)})
        if trace:
            U.log(f"[标定-分页] {trace} 第{pno}页 {len(rows)} 行 "
                  f"{U.ms_to_iso(oldest)} → {U.ms_to_iso(newest)}（新增 {new}）", "INFO")
        if len(seen) >= target_n:
            break
        if cursor is not None and oldest >= cursor:
            break                      # 游标没有前进，防止死循环
        if new == 0:
            break
        cursor = oldest
        time.sleep(pause)
    return [seen[k] for k in sorted(seen)], pages


def fetch_candles(bar: str, target_n: int) -> tuple[list[dict], list[dict]]:
    """分页抓 K 线，返回 (按 ts 升序的 candle 列表, 分页轨迹)。

    实测：OKX /market/candles 历史深度随 bar 递减（15m≈15天、1H≈30天、
    4H≈60天、5m≈5天），超出后接口返回空页；这里如实记录实际覆盖范围。
    """
    def page(cursor):
        if cursor is None:
            rows = S.candles(INST_ID, bar, limit=300)["data"]
        else:
            rows = _candles_after(bar, cursor)["data"]
        return list(reversed(rows))          # 交给 paginate 的"降序页"形式
    return paginate(page, lambda r: r["ts"], target_n, trace=f"candles_{bar}")


def _candles_after(bar: str, after_ts: int) -> dict:
    """sources_okx.candles 不支持 after 分页，这里复用 util.okx 做同样解析。"""
    d = U.okx("/api/v5/market/candles",
              {"instId": INST_ID, "bar": bar, "limit": "300", "after": str(after_ts)},
              timeout=20, retries=3)
    rows = []
    for v in d:
        rows.append({
            "ts": int(v[0]),
            "o": float(v[1]), "h": float(v[2]), "l": float(v[3]), "c": float(v[4]),
            "vol": float(v[5]), "volBase": float(v[6]), "volQuote": float(v[7]),
            "confirm": str(v[8]) == "1",
        })
    rows.sort(key=lambda x: x["ts"])
    return {"data": rows}


def fetch_funding(target_n: int) -> tuple[list[dict], list[dict]]:
    """分页抓资金费率历史，返回 (按 fundingTime 升序的行, 分页轨迹)。"""
    def page(cursor):
        params = {"instId": INST_ID, "limit": "100"}
        if cursor is not None:
            params["after"] = str(cursor)
        d = U.okx("/api/v5/public/funding-rate-history", params, timeout=20, retries=3)
        rows = [{"fundingTime": int(x["fundingTime"]),
                 "fundingRate": float(x["fundingRate"])} for x in d]
        rows.sort(key=lambda r: r["fundingTime"], reverse=True)   # 降序页
        return rows
    return paginate(page, lambda r: r["fundingTime"], target_n, trace="funding_history")


def gaps(ts_list: list[int], step_ms: int) -> list[str]:
    """检查时间戳序列是否等间隔连续，返回缺口描述（最多 5 条）。"""
    out = []
    for a, b in zip(ts_list, ts_list[1:]):
        if b - a != step_ms:
            out.append(f"{U.ms_to_iso(a)} -> {U.ms_to_iso(b)} (间隔 {(b - a) / 60000:.0f} 分钟)")
            if len(out) >= 5:
                break
    return out


# --------------------------------------------------------------------------- 分位数


def pct(sorted_vals: list[float], q: float) -> float | None:
    """线性插值分位数（numpy 默认口径）。q 取 0-100。"""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])
    pos = (n - 1) * (q / 100.0)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac)


def dist(vals: list[float], extra_q: tuple[float, ...] = ()) -> dict | None:
    """把一串观测值转成分位数描述。样本 < MIN_N 返回 None（样本不足）。"""
    clean = sorted(float(v) for v in vals if v is not None and math.isfinite(float(v)))
    n = len(clean)
    out = {
        "n": n,
        "p05": pct(clean, 5), "p10": pct(clean, 10), "p25": pct(clean, 25),
        "p50": pct(clean, 50), "p75": pct(clean, 75), "p90": pct(clean, 90),
        "p95": pct(clean, 95),
        "min": clean[0] if clean else None,
        "max": clean[-1] if clean else None,
        "insufficient": n < MIN_N,
    }
    for q in extra_q:
        out[f"p{int(q):02d}"] = pct(clean, q)
    if n < MIN_N:
        return None
    return out


# --------------------------------------------------------------------------- 时间对齐


def fold_to_grid(rows: list[dict], src_step_ms: int, grid_ms: int) -> list[dict]:
    """把细粒度序列折叠到 grid_ms 网格。

    Rubik ts 是区间**结束**时刻；先换算成同区间 K 线**开始**时刻（ts - src_step），
    再归入 [grid_start, grid_start+grid_ms) 的桶，取桶内最后一个样本。
    返回 [{"ts": 桶开始时刻, "value": [...]}] 升序。
    """
    buckets: dict[int, dict] = {}
    for r in rows:
        start = int(r["ts"]) - src_step_ms
        g = (start // grid_ms) * grid_ms
        if g not in buckets or start > buckets[g]["_start"]:
            buckets[g] = {"ts": g, "_start": start, "value": list(r["value"])}
    return [{"ts": buckets[g]["ts"], "value": buckets[g]["value"]} for g in sorted(buckets)]


def changes_aligned(series: list[dict], lookback_bars: int,
                    bar_ms: int) -> list[float]:
    """对已按 bar_ms 网格对齐的序列，计算相隔 lookback_bars 个周期的变化率(%)。

    以"目标时刻 = 当前桶开始时刻 - lookback*bar_ms"为准，在基准序列里取最近的样本；
    偏差超过容差就丢弃该观测（**不做任何插值，绝不编造**）。基准样本必须早于目标时刻。
    """
    if not series:
        return []
    by_ts = {int(r["ts"]): float(r["value"][0]) for r in series}
    ts_sorted = sorted(by_ts)
    out = []
    for ts in ts_sorted:
        target = ts - lookback_bars * bar_ms
        i = bisect.bisect_right(ts_sorted, target) - 1     # 最后一个 <= target
        if i < 0:
            continue
        ref = ts_sorted[i]
        if abs(target - ref) > ALIGN_TOLERANCE_MS:
            continue
        base, cur = by_ts[ref], by_ts[ts]
        if not base:
            continue
        out.append((cur / base - 1) * 100)
    return out


def ratio_buy_series(rows: list[dict]) -> list[float]:
    """taker 序列 -> 主动买入占比 buy/(sell+buy)。value = [sell_usd, buy_usd]。"""
    out = []
    for r in rows:
        sell, buy = float(r["value"][0]), float(r["value"][1])
        tot = sell + buy
        if tot > 0:
            out.append(buy / tot)
    return out


# --------------------------------------------------------------------------- 指标计算


def candle_metric_series(candles: list[dict]) -> dict[str, list[float]]:
    """从K线序列滚动计算各因子的历史序列（每个下标一个观测值）。

    全部只用"当下及过去"的数据，无未来信息泄露。
    """
    closes = [c["c"] for c in candles]
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    vols = [c.get("volQuote") or c.get("vol") or 0.0 for c in candles]
    n = len(candles)
    atr_pct, vol_ratio20, donch, rsi14, chg1 = [], [], [], [], []
    for i in range(n):
        if i >= 15:
            a = F.atr(highs[:i + 1], lows[:i + 1], closes[:i + 1], 14)
            if a and closes[i]:
                atr_pct.append(a / closes[i] * 100)
            r = F.rsi(closes[:i + 1], 14)
            if r is not None:
                rsi14.append(r)
        if i >= 21:
            avg20 = sum(vols[i - 20:i]) / 20.0
            if avg20 > 0:
                vol_ratio20.append(vols[i] / avg20)
        if i >= 19:
            d = F.donchian(highs[:i + 1], lows[:i + 1], 20)
            if d and d["high"] > d["low"]:
                donch.append((closes[i] - d["low"]) / (d["high"] - d["low"]))
        if i >= 1 and closes[i - 1]:
            chg1.append((closes[i] / closes[i - 1] - 1) * 100)
    return {"atr_pct": atr_pct, "vol_ratio20": vol_ratio20, "donchian_pos": donch,
            "rsi14": rsi14, "chg1_pct": chg1}


# --------------------------------------------------------------------------- 主流程


def main() -> int:
    started = datetime.now(timezone.utc)
    print("=" * 78)
    print("SOL 永续合约 — 阈值标定（真实历史分布分位数）")
    print("=" * 78)

    # ---- 1. 对表
    drift = None
    try:
        drift = U.sync_server_time()
        print(f"[1/4] 服务器对表：本机 vs OKX 漂移 {drift['offset_ms']:+d} ms "
              f"({drift['server_utc']})  {'正常' if drift['drift_ok'] else '⚠ 超过5秒'}")
    except Exception as e:                                   # noqa: BLE001
        REPORT.fail("server_time", e)
        print(f"[1/4] 服务器对表失败（继续执行）：{e}")

    # ---- 2. 抓数据
    print("-" * 78)
    print("[2/4] 抓取真实历史数据（分页，尽量覆盖 30 天）…")

    candles_by_bar: dict[str, list[dict]] = {}
    page_trace: dict[str, list[dict]] = {}
    for bar, target in (("15m", TARGET_15M), ("1H", TARGET_1H), ("4H", TARGET_4H)):
        try:
            rows, pages = fetch_candles(bar, target)
            if not rows:
                REPORT.empty(f"candles_{bar}", "分页抓取返回空")
                continue
            candles_by_bar[bar] = rows
            page_trace[f"candles_{bar}"] = pages
            REPORT.ok(f"candles_{bar}", rows, lambda r: r["ts"])
            info = REPORT.sources[f"candles_{bar}"]
            info["target_n"] = target
            info["covered_target"] = len(rows) >= target
            gs = gaps([r["ts"] for r in rows], BAR_MS[bar])
            info["gaps"] = gs
            print(f"      K线 {bar:>3s}: {len(rows):5d} 根（目标 {target}）  "
                  f"{info['first_utc']} → {info['last_utc']}  "
                  f"({info['span_hours'] / 24:.1f} 天)  缺口={len(gs)}"
                  f"{'' if len(rows) >= target else '  ⚠ 未达目标：OKX 该周期历史窗口已到尽头'}")
            for g in gs:
                U.log(f"[标定] {bar} K线存在时间缺口：{g}", "WARN")
        except Exception as e:                               # noqa: BLE001
            REPORT.fail(f"candles_{bar}", e)
            print(f"      K线 {bar}: 失败 {e}")

    funding = []
    try:
        funding, fpages = fetch_funding(TARGET_FUNDING)
        page_trace["funding_history"] = fpages
        if funding:
            REPORT.ok("funding_history", funding, lambda r: r["fundingTime"])
            info = REPORT.sources["funding_history"]
            info["target_n"] = TARGET_FUNDING
            info["covered_target"] = len(funding) >= TARGET_FUNDING
            print(f"      资金费率: {len(funding):5d} 期（目标 {TARGET_FUNDING}）  "
                  f"{info['first_utc']} → {info['last_utc']}  "
                  f"({info['span_hours'] / 24:.1f} 天)")
        else:
            REPORT.empty("funding_history")
    except Exception as e:                                   # noqa: BLE001
        REPORT.fail("funding_history", e)
        print(f"      资金费率: 失败 {e}")

    # Rubik 系列
    rubik_specs = [
        ("taker_volume_5m", lambda: S.taker_volume(CCY, "5m")),
        ("taker_volume_1H", lambda: S.taker_volume(CCY, "1H")),
        ("oi_volume_5m", lambda: S.open_interest_volume(CCY, "5m")),
        ("oi_volume_1H", lambda: S.open_interest_volume(CCY, "1H")),
        ("ls_account_ratio_5m", lambda: S.long_short_account_ratio(CCY, "5m")),
        ("ls_account_ratio_1H", lambda: S.long_short_account_ratio(CCY, "1H")),
        ("ls_top_position_ratio_5m", lambda: S.long_short_top_position_ratio(INST_ID, "5m")),
        ("ls_top_position_ratio_1H", lambda: S.long_short_top_position_ratio(INST_ID, "1H")),
        ("margin_loan_ratio_5m", lambda: S.margin_loan_ratio(CCY, "5m")),
        ("margin_loan_ratio_1H", lambda: S.margin_loan_ratio(CCY, "1H")),
    ]
    rubik: dict[str, list[dict]] = {}
    for name, fn in rubik_specs:
        try:
            env = fn()
            rows = env["data"]
            if not rows:
                REPORT.empty(name)
                print(f"      {name}: 空")
                continue
            rubik[name] = rows
            REPORT.ok(name, rows, lambda r: r["ts"])
            info = REPORT.sources[name]
            print(f"      {name:26s}: {len(rows):4d} 行  "
                  f"{info['first_utc']} → {info['last_utc']}  "
                  f"({info['span_hours']:.1f} 小时)")
        except Exception as e:                               # noqa: BLE001
            REPORT.fail(name, e)
            print(f"      {name}: 失败 {e}")
        time.sleep(0.15)

    # ---- 3. 计算分位数
    print("-" * 78)
    print("[3/4] 计算历史分位数…")

    thresholds: dict[str, dict | None] = {}
    samples: dict[str, dict] = {}
    insufficient: list[str] = []

    def record(key: str, dist_obj: dict | None, note: str, source_rows: int = 0,
               source: str = "") -> None:
        """登记一个指标：阈值 + 中间样本。dist_obj 为 None 表示样本不足。"""
        thresholds[key] = dist_obj
        samples[key] = {
            "note": note,
            "source": source,
            "source_rows": source_rows,
            "n": (dist_obj or {}).get("n", 0),
            "insufficient": bool(dist_obj is None or dist_obj.get("n", 0) < MIN_N),
            "percentiles": ({k: v for k, v in dist_obj.items() if k.startswith("p")}
                            if dist_obj else None),
            "min": (dist_obj or {}).get("min"),
            "max": (dist_obj or {}).get("max"),
        }
        if thresholds[key] is None:
            insufficient.append(key)

    # --- K线类因子
    feat15 = candle_metric_series(candles_by_bar.get("15m", []))
    feat1h = candle_metric_series(candles_by_bar.get("1H", []))

    if candles_by_bar.get("15m"):
        n = len(candles_by_bar["15m"])
        record("atr_pct_15m", dist(feat15["atr_pct"]),
               "15m K线 14周期ATR/收盘价*100（滚动逐根计算）", n, "candles_15m")
        record("vol_ratio20_15m", dist(feat15["vol_ratio20"]),
               "15m 当根USDT成交额 / 前20根均值（滚动逐根计算）", n, "candles_15m")
        record("rsi14_15m", dist(feat15["rsi14"]), "15m K线 RSI(14)（滚动逐根计算）",
               n, "candles_15m")
        record("donchian_pos_15m", dist(feat15["donchian_pos"]),
               "15m 收盘价在20根唐奇安通道中的位置 0-1（滚动逐根计算）", n, "candles_15m")
        record("price_chg_15m_pct", dist(feat15["chg1_pct"]),
               "15m 单根涨跌幅%（滚动逐根计算）", n, "candles_15m")
    else:
        for k in ("atr_pct_15m", "vol_ratio20_15m", "rsi14_15m", "donchian_pos_15m",
                  "price_chg_15m_pct"):
            record(k, None, "15m K线缺失", 0, "candles_15m")

    if candles_by_bar.get("1H"):
        n = len(candles_by_bar["1H"])
        record("atr_pct_1H", dist(feat1h["atr_pct"]),
               "1H K线 14周期ATR/收盘价*100（滚动逐根计算）", n, "candles_1H")
        record("rsi14_1H", dist(feat1h["rsi14"]), "1H K线 RSI(14)（滚动逐根计算）",
               n, "candles_1H")
        record("price_chg_1h_pct", dist(feat1h["chg1_pct"]),
               "1H 单根涨跌幅%（滚动逐根计算）", n, "candles_1H")
    else:
        for k in ("atr_pct_1H", "rsi14_1H", "price_chg_1h_pct"):
            record(k, None, "1H K线缺失", 0, "candles_1H")

    # --- 资金费率
    if funding:
        rates_pct = [r["fundingRate"] * 100 for r in funding]
        cum3 = [sum(rates_pct[i - 2:i + 1]) for i in range(2, len(rates_pct))]
        record("funding_rate_pct", dist(rates_pct),
               "资金费率%（8小时一期，逐期观测）", len(rates_pct), "funding_history")
        record("funding_last3_cum_pct", dist(cum3),
               "最近3期资金费率累计%（滚动 3 期求和）", len(cum3), "funding_history")
    else:
        record("funding_rate_pct", None, "资金费率历史缺失", 0, "funding_history")
        record("funding_last3_cum_pct", None, "资金费率历史缺失", 0, "funding_history")

    # --- 持仓量变化率（Rubik 时间对齐的核心：先把"区间结束时刻"换算成桶开始时刻，再按网格折叠）
    oi5 = rubik.get("oi_volume_5m", [])
    oi1h = rubik.get("oi_volume_1H", [])
    # 5m 原始序列严格等间隔（实测 574/574 步均为 5 分钟），折叠成 15m 网格以对齐 K 线
    oi15 = fold_to_grid(oi5, BAR_MS["5m"], BAR_MS["15m"])
    # 1H -> 4H 网格
    oi4h = fold_to_grid(oi1h, BAR_MS["1H"], BAR_MS["4H"])

    # 15m 变化：15m 网格上前 1 个周期
    chg_oi_15m = changes_aligned(oi15, 1, BAR_MS["15m"]) if oi15 else []
    # 1h 变化：15m 网格上前 4 个周期（=1 小时）
    chg_oi_1h = changes_aligned(oi15, 4, BAR_MS["15m"]) if oi15 else []
    chg_oi_4h = changes_aligned(oi4h, 1, BAR_MS["4H"]) if oi4h else []

    record("oi_chg_pct_15m", dist(chg_oi_15m),
           ("Rubik 5m OI：ts 换算为区间开始时刻后折成15m网格，对齐取前1个周期的变化率%"
            if oi15 else "Rubik 5m 持仓量数据缺失"), len(oi5), "oi_volume_5m")
    record("oi_chg_pct_1h", dist(chg_oi_1h),
           ("Rubik 5m OI：折成15m网格，对齐取前4个周期(1h)的变化率%"
            if oi15 else "Rubik 5m 持仓量数据缺失"), len(oi5), "oi_volume_5m")
    record("oi_chg_pct_4h", dist(chg_oi_4h),
           ("Rubik 1H OI：ts 换算为区间开始时刻后折成4H网格，对齐取前1个周期的变化率%"
            if oi4h else "Rubik 1H 持仓量数据缺失"), len(oi1h), "oi_volume_1H")

    # --- 主动买入占比 buy/(sell+buy)。OKX v5 market/candles **不返回** taker 买卖拆分，
    #     所以唯一真实来源是 Rubik taker-volume（接口硬上限：5m/1H 各约 72 行）。
    tv5 = rubik.get("taker_volume_5m", [])
    tv1h = rubik.get("taker_volume_1H", [])
    r5_15m = _taker_ratio_folded(tv5, 3)          # 3 个 5m 桶求和 = 15m
    r1h = ratio_buy_series(tv1h)
    record("taker_buy_ratio_15m", dist(r5_15m, extra_q=(2.0, 98.0)),
           ("Rubik taker-volume 5m 按3桶聚合为15m 的主动买入占比（含 p02/p98 极端失衡阈值）。"
            "注：窗口按 5m 步长滑动，因此相邻样本高度重叠（不是独立同分布样本）；"
            "接口最多只返回约72行 → 聚合后约 70 个窗口，覆盖仅约 6 小时"),
           len(tv5), "taker_volume_5m")
    record("taker_buy_ratio_1h", dist(r1h),
           ("Rubik taker-volume 1H 的主动买入占比；接口最多只返回约72行，覆盖约 3 天，"
            "n 仅略高于 30 的下限，解释力有限"), len(tv1h), "taker_volume_1H")

    # ---- 4. 写出
    print("-" * 78)
    print("[4/4] 写出阈值文件…")

    meta_sources = {k: v for k, v in sorted(REPORT.sources.items())}
    thresholds_doc: dict = dict(thresholds)
    # 目标 vs 实际覆盖（如实记录，抓不到 30 天就明说）
    targets = {"candles_15m": TARGET_15M, "candles_1H": TARGET_1H, "candles_4H": TARGET_4H,
               "funding_history": TARGET_FUNDING}
    coverage_summary = {}
    for name, info in meta_sources.items():
        cov = {"status": info["status"], "n": info["n"],
               "first_utc": info["first_utc"], "last_utc": info["last_utc"],
               "span_hours": info["span_hours"]}
        if name in targets:
            cov["target_n"] = targets[name]
            cov["target_met"] = info["n"] >= targets[name]
        coverage_summary[name] = cov

    thresholds_doc["_meta"] = {
        "generated_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generator": "scripts/calibrate.py",
        "instId": INST_ID,
        "ccy": CCY,
        "server_offset_ms": drift["offset_ms"] if drift else None,
        "server_utc_at_start": drift["server_utc"] if drift else None,
        "min_samples_required": MIN_N,
        "note": ("所有分位数均来自 OKX 公开接口的真实历史数据；样本数 n 小于 "
                 f"{MIN_N} 的指标一律写 null。本文件只做历史分布的统计描述，不含预测。"),
        "rubik_alignment": ("Rubik 的 ts 为统计区间结束时刻，K线 ts 为区间开始时刻，"
                            "已用 sources_okx.rubik_ts_to_candle_ts(ts, bar) 换算为桶开始时刻后"
                            f"再按网格折叠合并，最近邻容差 {ALIGN_TOLERANCE_MS} ms，不做插值。"),
        "requested_targets": {
            "candles_15m": TARGET_15M, "candles_1H": TARGET_1H, "candles_4H": TARGET_4H,
            "funding_history": TARGET_FUNDING,
            "rubik": "5m 与 1H 各指标",
        },
        "api_limits_observed": {
            "market_candles": ("单次 limit 最大 300；历史深度受限（实测 15m≈15天/1440根、"
                               "1H≈30天、4H≈60天、5m≈5天），超出窗口返回空页。"),
            "funding_rate_history": "after 分页有效，单次最大 100，实测可取到约 295 期(98天)。",
            "funding_history_this_run": len(funding),
            "rubik": ("period 仅支持 [5m,1H,1D]（top-trader 系列还支持 15m/4H）；"
                      "taker-volume 最多约 72 行；open-interest-volume / long-short-account-ratio"
                      " / margin-loan-ratio 最多 576 行(5m) 或 720 行(1H)；"
                      "long-short-*-top-trader 最多 100 行；limit 参数不生效。"),
        },
        "coverage": coverage_summary,
        "source_detail": meta_sources,
        "pagination_trace": page_trace,
        "failed_sources": REPORT.failed,
        "insufficient_metrics": insufficient,
        "insufficient_note": (
            f"样本数 n < {MIN_N} 的指标阈值写 null（见 insufficient_metrics）。"
            "taker_buy_ratio_15m 的窗口按 5m 步长滑动、相邻样本重叠，"
            "有效独立样本远少于名义 n，解释时需谨慎。"),
    }
    U.write_json(THRESHOLDS_PATH, thresholds_doc)

    samples_doc = {
        "_meta": thresholds_doc["_meta"],
        "metrics": samples,
    }
    U.write_json(SAMPLES_PATH, samples_doc)

    # ---- stdout 汇总表
    print("=" * 78)
    print("指标 | 样本数 | p10 | p50 | p90")
    print("-" * 78)

    def fmt(v):
        if v is None:
            return "  null"
        a = abs(v)
        if a == 0:
            return "0.0000"
        if a < 1e-4:
            return f"{v:.3e}"
        if a < 1:
            return f"{v:.4f}"
        if a < 1000:
            return f"{v:.4f}"
        return f"{v:.1f}"

    order = [
        "atr_pct_15m", "atr_pct_1H", "vol_ratio20_15m", "funding_rate_pct",
        "funding_last3_cum_pct", "oi_chg_pct_15m", "oi_chg_pct_1h", "oi_chg_pct_4h",
        "taker_buy_ratio_15m", "taker_buy_ratio_1h",
        "rsi14_15m", "rsi14_1H", "donchian_pos_15m",
        "price_chg_15m_pct", "price_chg_1h_pct",
    ]
    for key in order:
        d = thresholds.get(key)
        if d is None:
            print(f"{key:26s} | {'样本不足':>7s} | {'null':>9s} | {'null':>9s} | {'null':>9s}")
        else:
            print(f"{key:26s} | {d['n']:>7d} | {fmt(d['p10']):>9s} | "
                  f"{fmt(d['p50']):>9s} | {fmt(d['p90']):>9s}")

    tb = thresholds.get("taker_buy_ratio_15m")
    if tb and "p02" in tb and "p98" in tb:
        print("-" * 78)
        print(f"极端失衡补充阈值  taker_buy_ratio_15m : "
              f"p02={fmt(tb['p02'])}  p50={fmt(tb['p50'])}  p98={fmt(tb['p98'])}  (n={tb['n']})")

    print("-" * 78)
    print("实际覆盖区间（目标 vs 实际）：")
    for name, info in meta_sources.items():
        if info["status"] != "ok":
            print(f"  ✘ {name:26s} {info['status']}  {info.get('note')}")
            continue
        tgt = f" / 目标 {info['target_n']}" if info.get("target_n") else ""
        flag = "" if info.get("covered_target", True) else "  ⚠ 未达目标（接口历史窗口限制）"
        print(f"  ✔ {name:26s} n={info['n']:<5d}{tgt}  "
              f"{info['first_utc']} → {info['last_utc']}  ({info['span_hours']:.1f}h){flag}")

    if REPORT.failed:
        print("失败的数据源：")
        for f_ in REPORT.failed:
            print(f"  - {f_['source']}: {f_['error']}")
    else:
        print("失败的数据源：无")

    if insufficient:
        print("样本不足（阈值已写 null，绝未编造）：")
        for k in insufficient:
            n_ = (samples.get(k) or {}).get("n", 0)
            print(f"  - {k}（n={n_}，要求 n≥{MIN_N}）")
    else:
        print("样本不足的指标：无")

    print("-" * 78)
    print(f"已写出：{THRESHOLDS_PATH}")
    print(f"已写出：{SAMPLES_PATH}")
    print("=" * 78)
    return 0


def _taker_ratio_folded(rows: list[dict], k: int) -> list[float]:
    """Rubik taker (5m) 每 k 桶求和后再算主动买入占比。rows 已按 ts 升序。"""
    out = []
    for i in range(k - 1, len(rows)):
        seg = rows[i - k + 1:i + 1]
        sell = sum(float(r["value"][0]) for r in seg)
        buy = sum(float(r["value"][1]) for r in seg)
        if sell + buy > 0:
            out.append(buy / (sell + buy))
    return out


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:                                        # noqa: BLE001
        traceback.print_exc()
        raise SystemExit(1)
