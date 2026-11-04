# -*- coding: utf-8 -*-
"""风控台账。

因为系统是"信号模式"（你手动下单），成交结果需要你回填；没有回填时系统
只统计"发出过多少信号"，无法计算真实盈亏——这是诚实的局限，不要假装知道。

风控规则（已在 config.json 确认）：
- 单笔风险 ≤ 账户 2%（按止损距离反推仓位，引擎已强制）
- 单日亏损 ≥ 5% 立即停手
- 当日最多 2 次开仓
- 连亏 3 笔暂停 24 小时
- 同时最多 1 个仓位；禁止马丁、加仓死扛、上移止损
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from .util import (LOG_DIR, STATE_DIR, append_csv, load_config, log, ms_to_iso,
                   now_ms, read_csv, read_json, server_now_ms, write_json)

STATE_PATH = STATE_DIR / "risk_state.json"
HALT_FILE = STATE_DIR / "HALT"
TRADES_CSV = LOG_DIR / "trades.csv"
SIGNALS_CSV = LOG_DIR / "signals.csv"

SIGNAL_FIELDS = ["id", "generated_at_utc", "playbook", "kind", "side", "grade", "score",
                 "entry", "stop", "stop_pct", "tp1", "tp2", "rr1", "qty", "notional_usd",
                 "leverage_used", "planned_risk_usd", "effective_risk_usd", "expires_at_utc"]
TRADE_FIELDS = ["signal_id", "opened_at_utc", "closed_at_utc", "side", "entry", "exit",
                "qty", "notional", "fees_usd", "pnl_usd", "pnl_pct_of_equity", "r_multiple",
                "exit_reason", "notes"]


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def load_state(cfg: dict | None = None) -> dict:
    cfg = cfg or load_config()
    equity = float(cfg["account"]["equity_usdt"])
    max_entries = int(cfg["risk"]["max_entries_per_day"])
    st = read_json(STATE_PATH, None) or {}
    if st.get("date") != _today():
        carried_equity = float(st.get("equity_estimate", equity))
        st = {
            "date": _today(),
            "day_start_equity": carried_equity,
            "equity_estimate": carried_equity,
            "realized_pnl_today": 0.0,
            "entries_today": 0,
            # 持仓必须跨日保留：否则日切后"已持仓→不再发信号"这道闸门会静默失效，
            # 系统会在你仍然持仓时继续给新信号（这是一个真实的踩过的坑）。
            "open_positions": st.get("open_positions", []),
            "consecutive_losses": int(st.get("consecutive_losses", 0)),
            "paused_until_ms": int(st.get("paused_until_ms", 0) or 0),
            "history": st.get("history", [])[-200:],
            "total_trades": int(st.get("total_trades", 0)),
            "wins": int(st.get("wins", 0)),
            "losses": int(st.get("losses", 0)),
        }
    st["_max_entries"] = max_entries
    return st


def compute(cfg: dict, st: dict | None = None) -> dict:
    """算出引擎需要的风控闸门状态。"""
    st = load_state(cfg) if st is None else st
    equity = float(st["equity_estimate"])
    day_start = float(st["day_start_equity"])
    loss_limit = day_start * float(cfg["risk"]["daily_loss_limit_pct"]) / 100
    week_limit = day_start * float(cfg["risk"]["weekly_loss_limit_pct"]) / 100
    pnl_today = float(st["realized_pnl_today"])
    entries_left = int(cfg["risk"]["max_entries_per_day"]) - int(st["entries_today"])
    paused = int(st.get("paused_until_ms") or 0)
    halted = HALT_FILE.exists()
    halt_reason = HALT_FILE.read_text(encoding="utf-8").strip() if halted else None
    return {
        **st,
        "equity_estimate": equity,
        "daily_loss_limit_usd": round(loss_limit, 3),
        "daily_loss_hit": pnl_today <= -loss_limit,
        "entries_left": max(0, entries_left),
        "paused_until_ms": paused,
        "paused": bool(paused and server_now_ms() < paused),
        "halted": halted,
        "halt_reason": halt_reason or (f"已触及单日亏损上限 -{loss_limit:.2f}u" if pnl_today <= -loss_limit else None),
        "open_positions": st.get("open_positions", []),
        "win_rate": (st["wins"] / st["total_trades"] * 100) if st.get("total_trades") else None,
        "weekly_loss_limit_usd": round(week_limit, 3),
    }


def register_signal(cfg: dict, sig: dict, as_position: bool | None = None) -> dict:
    """记录一条已发出的信号。

    信号模式下**默认不把信号当成持仓**——因为"系统发出了信号"不等于"你真的下单了"。
    早期版本默认按已成交处理，后果是：你每忽略一条信号，台账就凭空多出一个持仓，
    "已持仓 → 不再发信号"的闸门会把整个系统锁死（真实踩过两次）。

    正确流程：信号只写进 logs/signals.csv 备查；你真的成交之后用 --open 登记，
    那时才占用仓位与当日开仓次数。若你希望信号自动视为已成交，把 config.json 里的
    account.signal_creates_position 改成 true。
    """
    st = load_state(cfg)
    if as_position is None:
        as_position = bool((cfg.get("account") or {}).get("signal_creates_position", False))
    if as_position:
        st["entries_today"] = int(st.get("entries_today", 0)) + 1
        st.setdefault("open_positions", []).append({
            "signal_id": sig["id"], "opened_at_utc": sig["generated_at_utc"], "side": sig["side"],
            "entry": sig["entry"], "qty": sig["qty"], "stop": sig["stop"],
        })
        write_json(STATE_PATH, st)
        log(f"信号 {sig['id']} 已登记为持仓（{sig['playbook']} {sig['side']} 入场{sig['entry']} "
            f"止损{sig['stop']} 手数{sig['qty']} 风险{sig['effective_risk_usd']}u）")
    else:
        log(f"发出信号 {sig['id']}：{sig['playbook']} {sig['side']} 入场{sig['entry']} "
            f"止损{sig['stop']} 手数{sig['qty']} 风险{sig['effective_risk_usd']}u "
            f"（仅记录，未占用仓位；成交后请用 --open 登记）")
    append_csv(SIGNALS_CSV, {k: sig.get(k) for k in SIGNAL_FIELDS}, SIGNAL_FIELDS)
    return st


def register_manual_position(cfg: dict, side: str, entry: float, qty: float, stop: float,
                             note: str = "", signal_id: str | None = None) -> tuple[str, dict]:
    """登记一笔手动开仓（例如按 scripts/plan.py 给的结构化方案下的单）。

    目的：让风控知道"已持仓"，从而触发"最多1个仓位"闸门，并计入当日开仓次数；
    同时把 entry/qty/stop 存下来，平仓后可用 --record 直接回填并算出 R 倍数。
    """
    st = load_state(cfg)
    sid = signal_id or f"MANUAL-{int(now_ms())}"
    st["entries_today"] = int(st.get("entries_today", 0)) + 1
    st.setdefault("open_positions", []).append({
        "signal_id": sid, "opened_at_utc": ms_to_iso(server_now_ms()), "side": side,
        "entry": float(entry), "qty": float(qty), "stop": float(stop), "note": note,
    })
    write_json(STATE_PATH, st)
    risk_u = abs(float(entry) - float(stop)) * float(qty)
    log(f"登记手动持仓 {sid}: {side} 入场{entry} 止损{stop} 手数{qty} "
        f"单笔风险 {risk_u:.3f}u（占净值 {risk_u / float(st['equity_estimate']) * 100:.2f}%）")
    return sid, compute(cfg, st)


def record_trade(cfg: dict, signal_id: str, exit_px: float, exit_reason: str = "manual",
                 notes: str = "", entry_px: float | None = None, qty: float | None = None,
                 side: str | None = None, closed_at_ms: int | None = None) -> dict:
    """回填成交结果。

    支持跨日回填：如果这笔是**昨天**平的仓，它必须计入净值（影响今天的手数计算），
    但**不能**计入"今日已实现盈亏"——否则会错误放宽今天的单日亏损闸门。
    """
    st = load_state(cfg)
    pos = None
    rest = []
    for p in st.get("open_positions", []):
        if p["signal_id"] == signal_id and pos is None:
            pos = p
        else:
            rest.append(p)
    entry = float(entry_px if entry_px is not None else (pos or {}).get("entry") or 0)
    q = float(qty if qty is not None else (pos or {}).get("qty") or 0)
    sd = side or (pos or {}).get("side") or "long"
    if not entry or not q:
        raise ValueError("缺少开仓价或手数，请在命令行传入 --entry 与 --qty")
    # 幂等保护：同一条成交重复回填会重复计入净值与胜率（真实踩过的坑，尤其是命令被中断后重跑）
    for h in (st.get("history") or []):
        if h.get("signal_id") == signal_id:
            raise ValueError(
                f"{signal_id} 已经回填过（盈亏 {h.get('pnl')}u @ {h.get('closed_day')}）。"
                f"若确实需要重复记录，请改用不同的编号。")
    direction = 1 if sd == "long" else -1
    gross = (float(exit_px) - entry) * q * direction
    fees = (entry + float(exit_px)) * q * float(cfg["risk"]["fee"]["taker_pct"]) / 100
    pnl = gross - fees
    r_mult = None
    stop = float((pos or {}).get("stop") or 0)
    if stop:
        risk_per_unit = abs(entry - stop)
        if risk_per_unit:
            r_mult = (pnl / (risk_per_unit * q)) if q else None

    closed_ms = int(closed_at_ms) if closed_at_ms else now_ms()
    closed_day = datetime.fromtimestamp(closed_ms / 1000, tz=timezone.utc).date().isoformat()
    same_day = closed_day == st.get("date")

    if same_day:
        st["realized_pnl_today"] = float(st.get("realized_pnl_today", 0)) + pnl
    # 净值按增量滚动，跨日同样正确（影响之后所有手数计算）
    st["equity_estimate"] = float(
        st.get("equity_estimate", cfg["account"]["equity_usdt"])) + pnl
    st["open_positions"] = rest
    st["total_trades"] = int(st.get("total_trades", 0)) + 1
    if pnl >= 0:
        st["wins"] = int(st.get("wins", 0)) + 1
        st["consecutive_losses"] = 0
    else:
        st["losses"] = int(st.get("losses", 0)) + 1
        st["consecutive_losses"] = int(st.get("consecutive_losses", 0)) + 1
    pause_n = int(cfg["risk"]["consecutive_loss_pause_count"])
    if st["consecutive_losses"] >= pause_n:
        st["paused_until_ms"] = now_ms() + int(cfg["risk"]["consecutive_loss_pause_hours"]) * 3600_000
        log(f"[风控] 连续亏损 {st['consecutive_losses']} 笔，暂停至 {ms_to_iso(st['paused_until_ms'])}", "WARN")
    st.setdefault("history", []).append(
        {"signal_id": signal_id, "exit": exit_px, "pnl": round(pnl, 4), "closed_day": closed_day})
    write_json(STATE_PATH, st)
    append_csv(TRADES_CSV, {
        "signal_id": signal_id, "opened_at_utc": (pos or {}).get("opened_at_utc"),
        "closed_at_utc": ms_to_iso(closed_ms), "side": sd, "entry": entry, "exit": exit_px,
        "qty": q, "notional": round(entry * q, 3), "fees_usd": round(fees, 4),
        "pnl_usd": round(pnl, 4),
        "pnl_pct_of_equity": round(pnl / float(cfg["account"]["equity_usdt"]) * 100, 3),
        "r_multiple": round(r_mult, 3) if r_mult is not None else None,
        "exit_reason": exit_reason, "notes": notes,
    }, TRADE_FIELDS)
    log(f"回填成交 {signal_id}: 盈亏 {pnl:+.3f}u  R={r_mult if r_mult is None else round(r_mult,2)}")
    return compute(cfg, st)


def halt(reason: str) -> None:
    HALT_FILE.write_text(reason, encoding="utf-8")
    log(f"[风控] 熔断已开启：{reason}", "WARN")


def resume() -> None:
    if HALT_FILE.exists():
        HALT_FILE.unlink()
    log("[风控] 熔断已解除")


def stats() -> dict:
    rows = read_csv(TRADES_CSV)
    if not rows:
        return {"trades": 0, "note": "还没有回填任何成交；信号模式下的盈亏必须由你回填才能统计"}
    pnls = [float(r["pnl_usd"]) for r in rows if r.get("pnl_usd")]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    return {
        "trades": len(pnls),
        "win_rate_pct": round(len(wins) / len(pnls) * 100, 1) if pnls else None,
        "total_pnl_usd": round(sum(pnls), 3),
        "avg_win": round(gp / len(wins), 3) if wins else None,
        "avg_loss": round(sum(losses) / len(losses), 3) if losses else None,
        "profit_factor": round(gp / gl, 2) if gl else None,
        "expectancy_r": round(sum(float(r.get("r_multiple") or 0) for r in rows) / len(rows), 3),
    }
