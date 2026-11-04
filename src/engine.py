# -*- coding: utf-8 -*-
"""信号引擎：条件触发 + 多因子加权 + 仓位/止损计算。

设计原则（直接对应用户要求）：
1. **不预测**：任何一条规则都只判断"当前已发生的可观测状态"，信号里的价位全部来自
   已发生的结构（前高/前低/VWAP/ATR倍数/突破位），不含任何未来价格预测。
2. **多因子共振**：K线结构 + 成交量 + 主动买卖流 + 订单簿深度 + 资金费率 + 持仓量
   + 多空比 + 强平 + 跨所基差 + 链上 + 情绪 + 宏观，全部参与打分。
3. **宁可不做**：数据缺失/过期、宏观封锁窗口、盘口滑点过大、手续费占比过高时直接不发信号。
4. **前提硬门槛**：每个玩法的"成立前提"不满足则整条作废，不论总分多高。
5. **周期可参数化**：快线(5m/15m/1H)与波段(1H/4H)共用同一套逻辑，只是周期不同；
   周期越短，止损越窄，手续费占风险的比例越高——这条必须用真实的标定分位数来卡。
"""
from __future__ import annotations

import math

from .util import load_config, load_thresholds, log, ms_to_iso, server_now_ms

# --------------------------------------------------------------------------- 数据驱动的阈值
# 全部来自 scripts/calibrate.py 与 scripts/calibrate_5m.py 用真实历史算出的分位数。
# 例：5m ATR 中位数仅 0.235%，而吃单来回手续费 0.10% = 吃掉 42.6% 的单笔风险，
# 所以"进场必须挂单(maker)"是数据推出来的硬结论，不是保守偏好。
_TH_CACHE: dict = {}


def _q(name: str, q: float, default=None):
    """从 config/thresholds.json 取分位数（缺档时线性插值）。"""
    if name not in _TH_CACHE:
        _TH_CACHE[name] = load_thresholds().get(name) or {}
    th = _TH_CACHE[name]
    pts = []
    for k, v in th.items():
        if k.startswith("p") and k[1:].isdigit() and isinstance(v, (int, float)):
            pts.append((float(k[1:]), float(v)))
    pts.sort()
    if not pts:
        return default
    if q <= pts[0][0]:
        return pts[0][1]
    if q >= pts[-1][0]:
        return pts[-1][1]
    for (q1, v1), (q2, v2) in zip(pts, pts[1:]):
        if q1 <= q <= q2:
            return v1 + (v2 - v1) * (q - q1) / (q2 - q1)
    return default


def _q_any(names: list[str], q: float, default=None):
    """按顺序尝试多个指标名，取第一个有数据的（用于未标定周期的优雅降级）。"""
    for n in names:
        v = _q(n, q)
        if v is not None:
            return v
    return default


DEFAULT_PARAMS = {
    "scalp_5m": {"min_score": 70, "expiry_min": 15, "risk_scale": 1.0, "min_rr": 1.8,
                 "min_stop_pct": 0.30, "max_stop_pct": 1.20, "stop_atr": 1.0,
                 "near_ema_atr": 0.40, "vol_pullback_pct": 70, "taker_pct": 70,
                 "vol_breakout_pct": 88, "oi_chg_pct": 65, "liq_scale_min_pct": 0.05,
                 "liq_dominance_min": 0.70, "oi_drop_pct": 20, "oi_drop_floor": -0.25},
    "swing_1h4h": {"min_score": 62, "expiry_min": 180, "risk_scale": 1.0, "min_rr": 2.0,
                   "min_stop_pct": 0.80, "max_stop_pct": 3.50, "stop_atr": 1.0,
                   "near_ema_atr": 0.60, "vol_pullback_pct": 70, "taker_pct": 65,
                   "vol_breakout_pct": 85, "oi_chg_pct": 60, "liq_scale_min_pct": 0.05,
                   "liq_dominance_min": 0.70, "oi_drop_pct": 20, "oi_drop_floor": -0.25},
}

MAX_SLIPPAGE_BPS = 6.0
MAX_SPREAD_BPS = 3.0


# --------------------------------------------------------------------------- 工具


def _n(x, d=None):
    return x if isinstance(x, (int, float)) and not isinstance(x, bool) else d


def _cond(name, ok, weight, detail, required=False):
    """required=True 表示这是该玩法的"成立前提"：不满足则整条候选作废，
    不论总分多高。防止"加权总分达标、但玩法定义的行情根本没发生"。
    """
    return {"name": name, "ok": bool(ok), "weight": weight, "detail": detail,
            "required": bool(required)}


def _score(conds: list[dict]) -> float:
    tot = sum(c["weight"] for c in conds)
    if tot <= 0:
        return 0.0
    return sum(c["weight"] for c in conds if c["ok"]) / tot * 100.0


def _params(name: str, cfg: dict) -> dict:
    """优先级：config.json 的 playbooks 段 > 代码内保守默认值。"""
    p = dict(DEFAULT_PARAMS.get(name, {}))
    p.update((cfg.get("playbooks") or {}).get(name) or {})
    return p


def _enabled(cfg: dict) -> dict:
    return {k: v for k, v in (cfg.get("playbooks") or {}).items() if v.get("enabled")}


def _fee_share(cfg: dict, px: float, stop_dist: float) -> tuple[float, float]:
    """返回 (挂单进场的手续费占风险比例, 吃单进场的手续费占风险比例)。"""
    f = cfg["risk"]["fee"]
    maker = px * float(f["maker_pct"]) * 2 / 100      # 开+平都挂单
    taker = px * (float(f["taker_pct"]) + float(f["maker_pct"])) / 100
    if stop_dist <= 0:
        return 9.9, 9.9
    return maker / stop_dist, taker / stop_dist


# --------------------------------------------------------------------------- 硬闸门


def global_gates(snap: dict, cfg: dict, risk_state: dict | None) -> dict:
    """与周期无关的硬闸门。通过后才逐玩法评估。"""
    reasons, warn = [], []
    f, health = snap["features"], snap["health"]
    fresh = snap.get("freshness") or {}

    if health.get("critical_missing"):
        reasons.append(f"关键数据缺失：{', '.join(health['critical_missing'])}")
    if health.get("critical_stale"):
        det = ", ".join(f"{s['source']}({s['age_s']}s)" for s in fresh.get("stale", [])
                        if s.get("critical"))
        reasons.append(f"关键数据过期：{det}")
    penalty, degraded = 0, []
    if health.get("advisory_missing"):
        degraded += [f"{k}(缺失)" for k in health["advisory_missing"]]
    if health.get("advisory_stale"):
        degraded += [f"{k}(过期)" for k in health["advisory_stale"]]
    if degraded:
        penalty = min(15.0, 5.0 * len(degraded))
        warn.append(f"辅助数据不全：{', '.join(degraded)} → 触发分数线提高 {penalty:.0f} 分作为补偿")

    sync = snap.get("server_sync") or {}
    if not sync.get("drift_ok", True):
        reasons.append(f"本机与交易所时间漂移 {sync.get('offset_ms')}ms 超限")

    tick = (snap["sources"].get("ticker") or {}).get("data")
    if tick is None:
        reasons.append("无最新报价，无法计算入场价")

    book = f.get("book") or {}
    if not book.get("ok"):
        reasons.append("盘口不可用，无法评估滑点")
    else:
        sp = _n(book.get("spread_bps"))
        if sp is not None and sp > MAX_SPREAD_BPS:
            reasons.append(f"买卖价差 {sp:.2f}bps 超过上限 {MAX_SPREAD_BPS}bps（流动性差）")
        for ntl in ("50", "100", "200"):
            sl = _n(((book.get("slippage_buy") or {}).get(ntl) or {}).get("slippage_bps"))
            if sl is not None and abs(sl) > MAX_SLIPPAGE_BPS:
                reasons.append(f"{ntl}u 下单滑点 {abs(sl):.2f}bps 超上限（盘口太薄）")
                break
        imb = _n(((book.get("bands") or {}).get("0.5%") or {}).get("imbalance"))
        if imb is not None and abs(imb) > 0.75:
            warn.append(f"盘口严重失衡({imb:+.2f})，行情可能剧烈波动")

    macro = f.get("macro") or {}
    if macro.get("blackout"):
        evs = "；".join(f"{e['country']}{e['title']}@{e['utc']}" for e in macro.get("blackout_events", []))
        reasons.append(f"处于高影响宏观数据封锁窗口：{evs}")

    t15 = (f.get("tf") or {}).get("15m") or {}
    atr_pct = _n(t15.get("atr_pct"))
    if atr_pct is None:
        reasons.append("缺少15m波动率(ATR)，无法计算止损距离")
    else:
        lo = _n((cfg.get("risk") or {}).get("min_atr_pct")) or _q("atr_pct_15m", 5) or 0.15
        hi = _n((cfg.get("risk") or {}).get("max_atr_pct")) or max(
            1.2, (_q("atr_pct_15m", 95) or 0.75) * 1.6)
        if atr_pct < lo:
            reasons.append(f"15m ATR={atr_pct:.3f}% 低于标定下限{lo:.3f}%（历史p05，市场过于死寂）")
        if atr_pct > hi:
            reasons.append(f"15m ATR={atr_pct:.3f}% 高于上限{hi:.3f}%（波动异常，止损易被扫）")

    if risk_state:
        if risk_state.get("halted"):
            reasons.append(f"风控熔断中：{risk_state.get('halt_reason')}")
        if risk_state.get("daily_loss_hit"):
            reasons.append(f"已触及单日亏损上限，今日停止交易（已亏{risk_state.get('realized_pnl_today')}u）")
        if risk_state.get("entries_left", 1) <= 0:
            reasons.append("今日可开仓次数已用完")
        if risk_state.get("paused_until_ms") and server_now_ms() < risk_state["paused_until_ms"]:
            reasons.append(f"连续亏损暂停中，解禁时间 {ms_to_iso(risk_state['paused_until_ms'])}")
        if len(risk_state.get("open_positions") or []) >= 1:
            reasons.append("已持有1个仓位（本系统上限1个）")

    return {"pass": len(reasons) == 0, "reasons": reasons, "warnings": warn,
            "score_penalty": penalty}


# --------------------------------------------------------------------------- 玩法1：趋势回踩


def _pullback(snap, cfg, side, name):
    """顺势回踩接续。入场看 primary，过滤看 filter，方向偏好看 bias。"""
    f = snap["features"]
    tfs = f.get("tf") or {}
    p = _params(name, cfg)
    tp_name, tf_name, tb_name = p.get("primary"), p.get("filter"), p.get("bias")
    tp, tf_, tb = tfs.get(tp_name) or {}, tfs.get(tf_name) or {}, tfs.get(tb_name) or {}
    px = _n((f.get("book") or {}).get("microprice")) or _n(tp.get("close"))
    if px is None or not tp.get("ok") or not tf_.get("ok") or not tb.get("ok"):
        return None
    atr = _n(tp.get("atr14"))
    if not atr:
        return None
    long_side = side == "long"

    ema20, ema50 = _n(tp.get("ema20")), _n(tp.get("ema50"))
    near = min(abs(px - ema20) if ema20 else 1e9, abs(px - ema50) if ema50 else 1e9)
    near_ok = near <= p["near_ema_atr"] * atr

    # 前提：过滤周期趋势方向一致
    f_stack, f_close, f_ema200 = tf_.get("ema_stack"), _n(tf_.get("close")), _n(tf_.get("ema200"))
    f_struct = (tf_.get("structure") or {}).get("structure")
    up = (f_stack == "bull") or (f_close and f_ema200 and f_close > f_ema200 and f_struct == "uptrend")
    dn = (f_stack == "bear") or (f_close and f_ema200 and f_close < f_ema200 and f_struct == "downtrend")
    htf_ok = up if long_side else dn

    # 偏大周期不反向
    b_close, b_ema50 = _n(tb.get("close")), _n(tb.get("ema50"))
    bias_ok = (b_ema50 is None or b_close is None) or (b_close > b_ema50 if long_side else b_close < b_ema50)

    volr = _n(tp.get("vol_ratio20"))
    vol_thr = _q_any([f"vol_ratio20_{tp_name}", "vol_ratio20_15m"], p["vol_pullback_pct"])
    vol_ok = (volr is not None) and (vol_thr is not None) and (volr <= vol_thr)

    lw = _n(tp.get("lower_wick_pct")) or 0
    uw = _n(tp.get("upper_wick_pct")) or 0
    o, c = _n(tp.get("open")), _n(tp.get("close"))
    if long_side:
        reject = (lw >= 0.3) or bool(uw < 0.3 and c and o and c > o)
    else:
        reject = (uw >= 0.3) or bool(lw < 0.3 and c and o and c < o)

    tk = f.get("taker_5m") or {}
    br15 = _n((tk.get("3bars") or {}).get("buy_ratio"))
    br_last = _n((tk.get("last_bar") or {}).get("buy_ratio"))
    tr = _q("taker_buy_ratio_15m", p["taker_pct"])
    thr = (tr if long_side else 1 - tr) if tr is not None else None
    flow_ok = None
    if br15 is not None and thr is not None:
        first = (br15 >= thr) if long_side else (br15 <= thr)
        last = ((br_last or 0) >= thr - 0.03) if long_side else ((br_last or 1) <= (1 - tr) + 0.03)
        flow_ok = bool(first and last)

    frank = _n((f.get("funding") or {}).get("hist_pct_rank"))
    crowd_ok = (frank is None) or ((frank <= 80) if long_side else (frank >= 20))

    liq = (f.get("liquidation") or {}).get("w15") or {}
    lshare = _n(liq.get("long_share"))
    liq_ok = True
    if lshare is not None and (liq.get("total_usd") or 0) > 0:
        liq_ok = (lshare <= 0.70) if long_side else (lshare >= 0.30)

    oi15 = _n(((f.get("oi") or {}).get("5m") or {}).get("3bars_pct"))
    oi_ok = (oi15 is None) or (oi15 > -0.8)
    struct_ok = (tp.get("structure") or {}).get("structure") != ("downtrend" if long_side else "uptrend")

    # 交易成本前提：按预估止损距离算手续费占风险比例
    est_stop = max(p["stop_atr"] * atr, px * p["min_stop_pct"] / 100)
    share_maker, share_taker = _fee_share(cfg, px, est_stop)
    max_share = float(cfg["risk"]["fee"]["max_fee_share_of_risk"])
    cost_ok = share_maker <= max_share

    conds = [
        _cond(f"{tf_name}趋势方向一致", htf_ok, 3.0,
              f"{tf_name}均线={f_stack} 结构={f_struct} 价={f_close} EMA200={f_ema200}", required=True),
        _cond(f"{tb_name}不反向", bias_ok, 1.5, f"{tb_name}收={b_close} EMA50={b_ema50}"),
        _cond(f"{tp_name}回踩至均线附近", near_ok, 2.5,
              f"距最近均线 {near:.4f} = {near / atr:.2f}×ATR({tp_name}) 阈值 {p['near_ema_atr']}"),
        _cond(f"{tp_name}回踩缩量", vol_ok, 2.0, f"量比={volr} 阈值≤{vol_thr}"),
        _cond(f"{tp_name}出现拒绝或收复", bool(reject), 2.0,
              f"下影={lw:.2f} 上影={uw:.2f}"),
        _cond("主动买卖流同向", bool(flow_ok), 2.5,
              f"15m主动买占比={br15} 阈值{'≥' if long_side else '≤'}{thr} 最新5m={br_last}"),
        _cond("资金费率不拥挤", crowd_ok, 1.5, f"费率分位={frank}"),
        _cond("强平方向未破坏趋势", liq_ok, 1.0, f"15m多头强平占比={lshare}"),
        _cond("持仓量未崩塌", oi_ok, 1.0, f"15m持仓变化={oi15}%"),
        _cond(f"{tp_name}结构未被破坏", struct_ok, 1.0,
              f"{tp_name}结构={(tp.get('structure') or {}).get('structure')}"),
        _cond("交易成本可接受", cost_ok, 2.0,
              f"预估止损{est_stop:.4f} → 挂单手续费占风险{share_maker * 100:.1f}% "
              f"(吃单{share_taker * 100:.1f}%) 上限{max_share * 100:.0f}%", required=True),
    ]

    sw_low = _n((tp.get("structure") or {}).get("last_pivot_low"))
    sw_high = _n((tp.get("structure") or {}).get("last_pivot_high"))
    if long_side:
        stop = (sw_low - 0.30 * atr) if sw_low else (px - p["stop_atr"] * atr)
        stop = min(stop, px - 0.20 * atr)
    else:
        stop = (sw_high + 0.30 * atr) if sw_high else (px + p["stop_atr"] * atr)
        stop = max(stop, px + 0.20 * atr)
    return {"kind": name, "side": side, "entry": px, "stop": stop,
            "target_ref": (sw_high if long_side else sw_low), "conds": conds, "params": p,
            "notes": [f"{tp_name} ATR={atr:.4f} ({atr / px * 100:.3f}%)"]}


# --------------------------------------------------------------------------- 玩法2：区间突破


def _breakout_play(snap, cfg, side, name):
    f = snap["features"]
    tfs = f.get("tf") or {}
    p = _params(name, cfg)
    tp_name, tf_name, tb_name = p.get("primary"), p.get("filter"), p.get("bias")
    tp, tf_, tb = tfs.get(tp_name) or {}, tfs.get(tf_name) or {}, tfs.get(tb_name) or {}
    px = _n((f.get("book") or {}).get("microprice")) or _n(tp.get("close"))
    if px is None or not tp.get("ok") or not tf_.get("ok"):
        return None
    atr = _n(tp.get("atr14"))
    don = tp.get("donchian20") or {}
    if not atr or not don:
        return None
    long_side = side == "long"

    dc_hi, dc_lo = _n(don.get("high")), _n(don.get("low"))
    broke = (px > dc_hi) if long_side else (px < dc_lo)

    volr = _n(tp.get("vol_ratio20"))
    vol_thr = _q_any([f"vol_ratio20_{tp_name}", "vol_ratio20_15m"], p["vol_breakout_pct"])
    vol_ok = (volr is not None) and (vol_thr is not None) and (volr >= vol_thr)

    tk = f.get("taker_5m") or {}
    br15 = _n((tk.get("3bars") or {}).get("buy_ratio"))
    tr = _q("taker_buy_ratio_15m", p["taker_pct"] + 15)
    thr = (tr if long_side else 1 - tr) if tr is not None else None
    flow_ok = None if (br15 is None or thr is None) else (
        (br15 >= thr) if long_side else (br15 <= thr))

    oi15 = _n(((f.get("oi") or {}).get("5m") or {}).get("3bars_pct"))
    oi_thr = _q("oi_chg_pct_15m", p["oi_chg_pct"])
    oi_ok = (oi15 is None) or (oi_thr is None) or (oi15 >= oi_thr)
    oi_note = "持仓增加=新资金进场" if (oi15 or 0) >= 0 else "持仓下降=可能是空头回补/平仓，突破质量差"

    book = f.get("book") or {}
    wall = book.get("wall_ask") if long_side else book.get("wall_bid")
    wall_dist = abs((_n((wall or {}).get("px")) or px) / px - 1) * 100 if wall else None
    wall_ok = (wall_dist is None) or wall_dist > 0.35

    frank = _n((f.get("funding") or {}).get("hist_pct_rank"))
    crowd_ok = (frank is None) or ((frank <= 90) if long_side else (frank >= 10))

    # 前提：偏大周期不能与突破方向相反
    tb_struct = (tb.get("structure") or {}).get("structure")
    tb_close, tb_ema50 = _n(tb.get("close")), _n(tb.get("ema50"))
    bias_ok = True
    if long_side and tb_struct == "downtrend":
        bias_ok = False
    if (not long_side) and tb_struct == "uptrend":
        bias_ok = False
    if tb_close is not None and tb_ema50 is not None:
        if long_side and tb_close < tb_ema50 * 0.995:
            bias_ok = False
        if (not long_side) and tb_close > tb_ema50 * 1.005:
            bias_ok = False

    est_stop = max(0.55 * atr, px * p["min_stop_pct"] / 100)
    share_maker, share_taker = _fee_share(cfg, px, est_stop)
    max_share = float(cfg["risk"]["fee"]["max_fee_share_of_risk"])
    cost_ok = share_maker <= max_share

    conds = [
        _cond(f"价格突破{tp_name} 20周期区间", broke, 3.0,
              f"现价={px} 上沿={dc_hi} 下沿={dc_lo}", required=True),
        _cond("成交量放大", vol_ok, 3.0, f"量比={volr} 阈值≥{vol_thr}"),
        _cond("主动买卖流同向", bool(flow_ok), 3.0,
              f"15m主动买占比={br15} 阈值{'≥' if long_side else '≤'}{thr}"),
        _cond("持仓量配合", oi_ok, 2.0, f"15m持仓变化={oi15}% 阈值≥{oi_thr} ({oi_note})"),
        _cond("突破前方无大额挂单墙", wall_ok, 1.5, f"最近墙距离={wall_dist}%"),
        _cond(f"{tb_name}趋势不反向", bias_ok, 2.0,
              f"{tb_name}结构={tb_struct} 收={tb_close} EMA50={tb_ema50}", required=True),
        _cond("资金费率不极端", crowd_ok, 1.0, f"费率分位={frank}"),
        _cond("交易成本可接受", cost_ok, 2.0,
              f"预估止损{est_stop:.4f} → 挂单手续费占风险{share_maker * 100:.1f}% "
              f"(吃单{share_taker * 100:.1f}%) 上限{max_share * 100:.0f}%", required=True),
    ]

    stop = (dc_hi - 0.55 * atr) if long_side else (dc_lo + 0.55 * atr)
    stop = min(stop, px - 0.25 * atr) if long_side else max(stop, px + 0.25 * atr)
    return {"kind": name, "side": side, "entry": px, "stop": stop,
            "target_ref": (px + 2.0 * atr) if long_side else (px - 2.0 * atr),
            "conds": conds, "params": p, "notes": [f"{tp_name} 突破 | 区间宽度 {don.get('width_pct')}%"]}


# --------------------------------------------------------------------------- 玩法3：强平衰竭反抽


def _exhaustion(snap, cfg, side, name):
    """一侧被大规模强平后的反向机会。强平数据最细只到15m，故固定用15m。"""
    f = snap["features"]
    tfs = f.get("tf") or {}
    p = _params(name, cfg)
    tp, t15 = tfs.get("5m") or {}, tfs.get("15m") or {}
    px = _n((f.get("book") or {}).get("microprice")) or _n(tp.get("close"))
    atr = _n(t15.get("atr14"))
    if px is None or not atr:
        return None
    long_side = side == "long"

    liq15 = (f.get("liquidation") or {}).get("w15") or {}
    tot = _n(liq15.get("total_usd"), 0) or 0
    lshare = _n(liq15.get("long_share"))
    dominance = (lshare if long_side else (1 - lshare)) if lshare is not None else None
    dom_ok = (dominance is not None) and dominance >= p["liq_dominance_min"]

    tick = (snap["sources"].get("ticker") or {}).get("data") or {}
    vol24 = _n(tick.get("vol24h_base"))
    liq_ratio = (tot / (vol24 * px)) * 100 if (vol24 and px) else None
    scale_ok = (liq_ratio is not None) and liq_ratio >= p["liq_scale_min_pct"]

    oi15 = _n(((f.get("oi") or {}).get("5m") or {}).get("3bars_pct"))
    oi_drop = min(p["oi_drop_floor"], _q("oi_chg_pct_15m", p["oi_drop_pct"]) or p["oi_drop_floor"])
    delever = (oi15 is not None) and oi15 <= oi_drop

    wick = _n(t15.get("lower_wick_pct")) if long_side else _n(t15.get("upper_wick_pct"))
    vwap = _n(t15.get("vwap_day"))
    back_ok = (vwap is None) or ((px > vwap) if long_side else (px < vwap))

    frank = _n((f.get("funding") or {}).get("hist_pct_rank"))
    extreme_ok = (frank is not None) and ((frank <= 15) if long_side else (frank >= 85))

    tk = f.get("taker_5m") or {}
    br = _n((tk.get("last_bar") or {}).get("buy_ratio"))
    tr = _q("taker_buy_ratio_15m", 85)
    thr = (tr if long_side else 1 - tr) if tr is not None else None
    absorb_ok = (br is not None and thr is not None) and (
        (br >= thr) if long_side else (br <= thr))

    imb = _n(((f.get("book") or {}).get("bands") or {}).get("0.5%", {}).get("imbalance"))
    book_ok = (imb is None) or ((imb > -0.2) if long_side else (imb < 0.2))

    est_stop = max(p["stop_atr"] * atr, px * p["min_stop_pct"] / 100)
    share_maker, share_taker = _fee_share(cfg, px, est_stop)
    max_share = float(cfg["risk"]["fee"]["max_fee_share_of_risk"])
    cost_ok = share_maker <= max_share

    conds = [
        _cond("一侧被集中强平", dom_ok, 3.0,
              f"15m多头强平占比={lshare} 总额={tot:,.0f}u", required=True),
        _cond("强平规模相对成交额显著", scale_ok, 2.0,
              f"强平/24h成交额={liq_ratio if liq_ratio is None else round(liq_ratio, 3)}% "
              f"阈值≥{p['liq_scale_min_pct']}%"),
        _cond("持仓量快速下降(去杠杆)", delever, 2.5,
              f"15m持仓变化={oi15}% 阈值≤{oi_drop:.3f}%"),
        _cond("出现拒绝影线", (wick or 0) >= 0.45, 2.5, f"影线占比={wick}"),
        _cond("价格收复日内VWAP", back_ok, 1.5, f"VWAP={vwap} 现价={px}"),
        _cond("资金费率处于极端", extreme_ok, 2.0, f"费率分位={frank}"),
        _cond("最新主动流反向吸收", absorb_ok, 2.5,
              f"最新5m主动买占比={br} 阈值{'≥' if long_side else '≤'}{thr}"),
        _cond("盘口未继续单边倾斜", book_ok, 1.5, f"0.5%档失衡={imb}"),
        _cond("交易成本可接受", cost_ok, 1.5,
              f"预估止损{est_stop:.4f} → 挂单手续费占风险{share_maker * 100:.1f}% "
              f"上限{max_share * 100:.0f}%", required=True),
    ]

    sw_low = _n((t15.get("structure") or {}).get("last_pivot_low"))
    sw_high = _n((t15.get("structure") or {}).get("last_pivot_high"))
    if long_side:
        stop = min((sw_low - 0.4 * atr) if sw_low else px - p["stop_atr"] * atr, px - 0.3 * atr)
        tgt = sw_high if (sw_high and sw_high > px) else px + 2.2 * atr
    else:
        stop = max((sw_high + 0.4 * atr) if sw_high else px + p["stop_atr"] * atr, px + 0.3 * atr)
        tgt = sw_low if (sw_low and sw_low < px) else px - 2.2 * atr
    return {"kind": name, "side": side, "entry": px, "stop": stop, "target_ref": tgt,
            "conds": conds, "params": p, "notes": ["强平驱动，属于快线玩法"]}


# --------------------------------------------------------------------------- 仓位


def position_size(entry: float, stop: float, snap: dict, cfg: dict, p: dict) -> dict:
    """按"止损距离反推仓位"，而不是先定仓位再想止损。含手续费占比硬门槛。"""
    r, fee = cfg["risk"], cfg["risk"]["fee"]
    spec = snap.get("symbol") or cfg["symbols"][0]
    lot, min_sz = float(spec["lotSz"]), float(spec["minSz"])
    equity = float(snap.get("equity_estimate") or cfg["account"]["equity_usdt"])
    lev_cap = float(cfg["account"]["leverage_setting"])
    risk_scale = float(p.get("risk_scale", 1.0))

    stop_dist = abs(entry - stop)
    if stop_dist <= 0:
        return {"ok": False, "reason": "止损距离为0"}
    stop_pct = stop_dist / entry * 100
    risk_usd = equity * float(r["risk_per_trade_pct"]) / 100 * risk_scale
    min_stop = float(p.get("min_stop_pct", r.get("min_stop_pct", 0.45)))
    max_stop = float(p.get("max_stop_pct", r.get("max_stop_pct", 3.5)))

    if stop_pct > max_stop:
        return {"ok": False, "reason": f"止损距离 {stop_pct:.2f}% 超过该玩法上限 {max_stop}%（点位太远，不值得做）"}
    if stop_pct < min_stop:
        return {"ok": False, "reason": f"止损距离 {stop_pct:.2f}% 低于该玩法下限 {min_stop}%（会被正常噪音扫掉）"}

    share_maker, share_taker = _fee_share(cfg, entry, stop_dist)
    max_share = float(fee["max_fee_share_of_risk"])
    if share_maker > max_share:
        return {"ok": False,
                "reason": f"止损 {stop_pct:.2f}% 太窄：即使全部挂单，手续费也占单笔风险的 "
                          f"{share_maker * 100:.1f}%（上限{max_share * 100:.0f}%）。周期越短越容易被手续费吃掉。"}

    qty = math.floor((risk_usd / stop_dist) / lot) * lot
    qty = round(qty, 4)
    if qty < min_sz:
        return {"ok": False, "reason": f"计算手数 {qty} 低于最小下单量 {min_sz}"}

    notional = qty * entry
    fees_maker = notional * float(fee["maker_pct"]) * 2 / 100
    fees_taker = notional * (float(fee["taker_pct"]) + float(fee["maker_pct"])) / 100
    book = snap["features"].get("book") or {}
    nkey = "50" if notional < 80 else ("100" if notional < 150 else ("200" if notional < 300 else "500"))
    side_key = "slippage_buy" if stop < entry else "slippage_sell"
    slip_entry = (book.get(side_key) or {}).get(nkey) or {}
    slip_bps = abs(_n(slip_entry.get("slippage_bps"), 0) or 0)
    slip_cost = notional * slip_bps / 10000
    effective_risk = qty * stop_dist + fees_maker + slip_cost
    leverage = notional / equity

    if leverage > lev_cap:
        need = float(r["risk_per_trade_pct"]) / stop_pct
        return {"ok": False,
                "reason": f"名义 {notional:.0f}u 需要 {leverage:.2f}x，超过交易所杠杆设置 {lev_cap}x。"
                          f"该止损距离下要放 2% 风险，杠杆设置至少要 {math.ceil(need)}x。"}
    if effective_risk > risk_usd * 1.30:
        return {"ok": False,
                "reason": f"含费滑的真实风险 {effective_risk:.3f}u 超过计划 {risk_usd:.2f}u 的130%"}

    return {"ok": True, "qty": qty, "notional": notional, "leverage": leverage,
            "stop_dist": stop_dist, "stop_pct": stop_pct, "risk_usd": risk_usd,
            "fees_maker": fees_maker, "fees_taker": fees_taker,
            "fee_share_maker": share_maker, "fee_share_taker": share_taker,
            "slip_cost": slip_cost, "slip_bps": slip_bps,
            "effective_risk": effective_risk,
            "require_maker": share_taker > max_share,
            "leverage_setting_needed": math.ceil(float(r["risk_per_trade_pct"]) / stop_pct)}


# --------------------------------------------------------------------------- 主入口


def evaluate(snap: dict, cfg: dict | None = None, risk_state: dict | None = None,
             *, signal_filter=None) -> dict:
    """Evaluate current candidates, then apply optional eligibility before quota.

    The trusted caller's predicate can only remove fully evaluated signals. It
    must return a bool and must not persist consumption before this call returns.
    A failed predicate rejects the whole evaluation, never bypasses filtering.
    Existing scanner/paper callers retain the default output and quota behavior.
    """
    cfg = cfg or load_config()
    gates = global_gates(snap, cfg, risk_state)
    out = {"generated_at_ms": server_now_ms(), "gates": gates, "signals": [],
           "candidates": [], "rejected": [], "market": _market_note(snap, cfg)}
    if not gates["pass"]:
        return out

    books = _enabled(cfg)
    raw = []
    for side in ("long", "short"):
        for name in books:
            for fn in (_pullback, _breakout_play):
                try:
                    c = fn(snap, cfg, side, name)
                except Exception as e:  # noqa: BLE001
                    log(f"[引擎异常] {fn.__name__}/{side}/{name}: {e}", "WARN")
                    c = None
                if c:
                    raw.append(c)
        try:
            c = _exhaustion(snap, cfg, side, "scalp_5m")
        except Exception as e:  # noqa: BLE001
            log(f"[引擎异常] exhaustion/{side}: {e}", "WARN")
            c = None
        if c:
            raw.append(c)

    min_rr_cfg = float(cfg["risk"]["min_rr"])
    min_net_rr = float(cfg["risk"].get("min_net_rr", 1.55))
    max_share = float(cfg["risk"]["fee"]["max_fee_share_of_risk"])
    penalty = float(gates.get("score_penalty") or 0)

    for c in raw:
        score = _score(c["conds"])
        p = c["params"]
        need = p["min_score"] + penalty
        entry, stop = c["entry"], c["stop"]
        a = abs(entry - stop)
        long_side = c["side"] == "long"
        r_mult = max(float(p.get("min_rr") or min_rr_cfg), min_rr_cfg, 1.8)
        rr_target = entry + r_mult * a if long_side else entry - r_mult * a
        struct_target = c.get("target_ref")
        if struct_target and ((struct_target > entry) if long_side else (struct_target < entry)):
            tp1 = min(rr_target, struct_target) if long_side else max(rr_target, struct_target)
        else:
            tp1 = rr_target
        rr = (abs(tp1 - entry) / a) if a else None
        # 手续费折算成 R：净赔率才是真实赔率
        share_maker, share_taker = _fee_share(cfg, entry, a or 1)
        cost_r = min(share_maker, share_taker)
        net_rr = (rr - cost_r) if rr is not None else None
        required_ok = all(x["ok"] for x in c["conds"] if x.get("required"))
        passed_score = score >= need
        passed_rr = (net_rr is not None) and (net_rr >= min_net_rr)
        cand = {
            "kind": c["kind"], "side": c["side"], "score": round(score, 1),
            "min_score": round(need, 1), "base_min_score": p["min_score"],
            "rr": round(rr, 2) if rr else None,
            "net_rr": round(net_rr, 2) if net_rr is not None else None,
            "cost_r": round(cost_r, 3),
            "tp1": tp1, "r_mult": r_mult, "entry": entry, "stop": stop,
            "target_ref": struct_target, "conds": c["conds"], "notes": c.get("notes", []),
            "required_ok": required_ok,
            "missing_required": [x["name"] for x in c["conds"] if x.get("required") and not x["ok"]],
            "passed_score": passed_score, "passed_rr": passed_rr,
            "passed": bool(required_ok and passed_score and passed_rr),
        }
        out["candidates"].append(cand)

    out["candidates"].sort(key=lambda x: (-x["score"], -(x["net_rr"] or 0)))

    for cand in out["candidates"]:
        if not cand["passed"]:
            continue
        name = cand["kind"]
        p = _params(name, cfg)
        label = (cfg["playbooks"].get(name) or {}).get("label", name)
        entry, stop = cand["entry"], cand["stop"]
        size = position_size(entry, stop, snap, cfg, p)
        if not size.get("ok"):
            cand["rejected"] = size["reason"]
            out["rejected"].append({"kind": name, "side": cand["side"], "reason": size["reason"]})
            continue
        a = abs(entry - stop)
        long_side = cand["side"] == "long"
        tp1, r_mult = cand["tp1"], cand["r_mult"]
        tp2 = entry + (r_mult + 1.6) * a if long_side else entry - (r_mult + 1.6) * a
        expiry = server_now_ms() + int(p["expiry_min"]) * 60000
        sid = f"{name.split('_')[0][:3].upper()}-{cand['side'][:4].upper()}-{server_now_ms()}"

        maker_note = ("必须用限价挂单(maker)进场，不能用市价：吃单手续费会占单笔风险的 "
                      f"{size['fee_share_taker'] * 100:.1f}%，挂单只有 {size['fee_share_maker'] * 100:.1f}%。")
        out["signals"].append({
            "id": sid,
            "generated_at_utc": ms_to_iso(server_now_ms()),
            "instId": snap["inst"],
            "playbook": label, "kind": name, "side": cand["side"],
            "grade": "A" if cand["score"] >= cand["min_score"] + 12 else "B",
            "score": cand["score"],
            "entry": entry, "entry_type": "限价挂单(maker)" if size["require_maker"] else "限价优先，可接受市价",
            "entry_zone": [round(entry * 0.9995, 3), round(entry * 1.0005, 3)],
            "stop": round(stop, 3), "stop_pct": round(size["stop_pct"], 3),
            "tp1": round(tp1, 3), "tp2": round(tp2, 3),
            "rr1": cand["rr"], "net_rr1": cand["net_rr"],
            "cost_r": cand["cost_r"],
            "qty": size["qty"], "notional_usd": round(size["notional"], 2),
            "leverage_used": round(size["leverage"], 2),
            "leverage_setting_needed": size["leverage_setting_needed"],
            "planned_risk_usd": round(size["risk_usd"], 3),
            "effective_risk_usd": round(size["effective_risk"], 3),
            "fees_maker_usd": round(size["fees_maker"], 4),
            "fees_taker_usd": round(size["fees_taker"], 4),
            "fee_share_of_risk": round(size["fee_share_maker"], 4),
            "slip_est_usd": round(size["slip_cost"], 4),
            "expires_at_utc": ms_to_iso(expiry), "expiry_min": p["expiry_min"],
            "max_hold_hours": p.get("max_hold_hours"),
            "evidence": [{"条件": x["name"], "满足": x["ok"], "实测值": x["detail"],
                          "前提": x.get("required", False)} for x in cand["conds"]],
            "invalidation": _invalidation(cand, snap, cfg),
            "notes": cand["notes"] + [maker_note,
                                      "本信号不含任何价格预测：仅描述当前可观测状态，条件消失即作废。",
                                      "必须同时挂出止损单；全仓模式下请勿同时持有其他仓位。"],
        })
    if signal_filter is not None:
        eligible = []
        try:
            for signal in out["signals"]:
                decision = signal_filter(signal)
                if type(decision) is not bool:
                    raise ValueError("signal predicate must return bool")
                if decision:
                    eligible.append(signal)
        except Exception:
            raise ValueError("SIGNAL_FILTER_FAILED") from None
        out["signals"] = eligible
    max_e = int((cfg["risk"] or {}).get("max_entries_per_day", 3))
    left = max_e - int((risk_state or {}).get("entries_today", 0))
    out["signals"] = out["signals"][:max(0, left)]
    return out


def _invalidation(cand: dict, snap: dict, cfg: dict) -> list[str]:
    f = snap["features"]
    name = cand["kind"]
    p = _params(name, cfg)
    tp = (f.get("tf") or {}).get(p.get("primary") or "15m") or {}
    inv = [f"价格触及止损 {round(cand['stop'], 3)}（立即离场，不允许上移止损）",
           "数据源出现缺失或过期（立即停止依据本信号操作）",
           "进入高影响宏观数据封锁窗口",
           f"信号有效期 {p.get('expiry_min')} 分钟，超时作废、需重新评估"]
    if "breakout" in name:
        don = tp.get("donchian20") or {}
        edge = don.get("high") if cand["side"] == "long" else don.get("low")
        if edge:
            inv.append(f"价格重新回到区间内（{edge:.3f}）= 假突破，撤销信号")
    else:
        inv.append(f"{p.get('primary')} 周期结构被反向破坏 = 玩法前提消失")
    if p.get("no_overnight"):
        inv.append("跨日不留仓（no_overnight=true），收盘前未达目标即平掉")
    return inv


def _market_note(snap: dict, cfg: dict) -> dict:
    f, facts = snap["features"], snap["facts"]
    tfs = f.get("tf") or {}
    t5, t15 = tfs.get("5m") or {}, tfs.get("15m") or {}
    return {
        "price": facts.get("price"),
        "chg24h_pct": facts.get("chg24h_pct"),
        "atr_pct_5m": t5.get("atr_pct"),
        "atr_pct_15m": facts.get("atr_pct_15m"),
        "structure_5m": (t5.get("structure") or {}).get("structure"),
        "structure_15m": facts.get("structure_15m"),
        "structure_1H": facts.get("structure_1H"),
        "structure_4H": facts.get("structure_4H"),
        "funding_pct": facts.get("funding_pct"),
        "oi_usd": facts.get("oi_usd"),
        "fng": facts.get("fng"), "fng_label": facts.get("fng_label"),
        "risk_regime": facts.get("risk_regime"),
        "blackout": facts.get("macro_blackout"),
        "spread_bps": facts.get("spread_bps"),
        "health": snap.get("health"),
        "playbooks": list(_enabled(cfg)),
        "fee_share_note": "手续费占单笔风险比例 = 价格×费率 ÷ 止损距离；周期越短占比越高。",
    }
