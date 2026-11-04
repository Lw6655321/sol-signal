# -*- coding: utf-8 -*-
"""T5 纸面回放：把引擎真实发出的信号，用「信号之后」的真实 K 线回放，模拟成交/止损/止盈，
计算扣费后的净收益。用于判断"扣掉手续费和滑点后到底是不是正期望"。

安全边界：只读公共行情（OKX /market/candles）与本地文件；不连账户、不下单、不改风控配置。

用法：
  python scripts/paper_runner.py --once            # 只打印，不写文件
  python scripts/paper_runner.py --once --write    # 写 state/paper_results.json + logs/paper/results.csv
  python scripts/paper_runner.py --once --write --force   # 重算已处理过的信号

限制（写在结果里，别当实盘）：
  - 按 K 线路径撮合（o→h→l→c 或 o→l→h→c），不模拟排队/盘口深度/资金费；
  - 信号发出时刻在 K 线内部，用该根 K 线开盘价作为"下单时"报价的近似；
  - 信号按 config 的 playbook 最大持有时间 + 2 小时截止，剩余仓位按最后收盘价平仓；
  - 风控权益用「当前实时权益」近似（历史时刻的真实权益无法还原）。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                          # noqa: E402
from src import risk                                      # noqa: E402
from src.execution import ExecutionRejected, build_intent  # noqa: E402
from src.paper_broker import PaperBroker                  # noqa: E402
from src.util import LOG_DIR, STATE_DIR, load_config, okx, read_csv  # noqa: E402

INST = "SOL-USDT-SWAP"
RESULT_CSV = LOG_DIR / "paper" / "results.csv"
RESULT_STATE = STATE_DIR / "paper_results.json"
BAR = "15m"
MAX_CANDLES_PER_CALL = 300


def iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> int:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def fetch_candles(start_ms: int, end_ms: int, bar: str = BAR) -> list[dict]:
    """拉取 (start, end] 区间的 K 线，返回时间升序。OKX 返回倒序，用 after 分页。"""
    rows: dict[int, dict] = {}
    after = end_ms + 1
    for _ in range(20):
        data = okx("/api/v5/market/candles",
                   {"instId": INST, "bar": bar, "limit": str(MAX_CANDLES_PER_CALL),
                    "after": str(after)}, timeout=20)
        if not data:
            break
        batch = []
        for v in data:
            ts = int(v[0])
            batch.append({"ts": ts, "o": float(v[1]), "h": float(v[2]),
                          "l": float(v[3]), "c": float(v[4])})
        for c in batch:
            rows[c["ts"]] = c
        oldest = min(c["ts"] for c in batch)
        if oldest <= start_ms or len(data) < MAX_CANDLES_PER_CALL:
            break
        after = oldest
    return [rows[k] for k in sorted(rows) if start_ms <= k <= end_ms]


def candle_points(candle: dict) -> list[float]:
    """把一根 K 线展开成价格路径点（近似，避免同一根内同时触发止损/止盈的偏差）。"""
    o, h, l, c = candle["o"], candle["h"], candle["l"], candle["c"]
    if c >= o:
        return [o, l, h, c]
    return [o, h, l, c]


def replay_signal(sig: dict, cfg: dict, equity: float, now_ms: int,
                  bar: str, run_root: Path) -> dict:
    """回放一条信号；返回结果字典。"""
    sid = sig["id"]
    gen_ms = parse_iso(sig["generated_at_utc"])
    exp_ms = parse_iso(sig["expires_at_utc"])
    playbook = cfg.get("playbooks", {}).get(sig.get("kind") or "", {})
    max_hold_h = float(playbook.get("max_hold_hours", 8))
    end_ms = min(now_ms, gen_ms + int((max_hold_h + 2) * 3600_000))

    state = {
        "now_ms": gen_ms + 1000,
        "equity_estimate": f"{equity:.4f}",
        "day_start_equity": f"{equity:.4f}",
        "realized_pnl_today": "0",
        "entries_today": 0,
        "open_positions": [],
        "pending_orders": [],
        "data_fresh": True,
        "macro_blackout": False,
        "halted": False,
        "paused_until_ms": 0,
        "slippage_bps": "5",
        "seen_signal_ids": [],
    }
    try:
        intent = build_intent(sig, state, cfg)
    except ExecutionRejected as exc:
        return {"signal_id": sid, "status": "rejected_by_risk", "code": exc.code,
                "generated_at": sig["generated_at_utc"], "kind": sig.get("kind"),
                "side": sig.get("side"), "pnl": 0.0, "entry": "", "exit": "",
                "qty": "", "fees": "", "reason": exc.reason, "run_dir": ""}

    candles = fetch_candles(gen_ms, end_ms, bar)
    if not candles:
        return {"signal_id": sid, "status": "no_data", "code": "NO_CANDLES",
                "generated_at": sig["generated_at_utc"], "kind": sig.get("kind"),
                "side": sig.get("side"), "pnl": 0.0, "entry": "", "exit": "",
                "qty": "", "fees": "", "reason": "区间内没有 K 线", "run_dir": ""}

    run_dir = run_root / f"{sid}-{int(now_ms)}"
    clock = {"now": int(candles[0]["ts"])}
    broker = PaperBroker(log_dir=run_dir, slippage_bps=intent.slippage_bps,
                         clock=lambda: clock["now"])
    first = candles[0]
    broker.on_price(price=first["o"], timestamp_ms=int(first["ts"]))
    order = broker.place_limit(intent)

    for candle in candles:
        pts = candle_points(candle)
        for i, px in enumerate(pts):
            if candle is first and i == 0:
                continue                      # 已经喂过开盘价
            clock["now"] = int(candle["ts"]) + i
            broker.on_price(price=px, timestamp_ms=clock["now"])

    # 到最大持有时间仍未平仓：按最后收盘价平掉（taker + 滑点由 broker.close 处理）
    last = candles[-1]
    if order.open_qty > 0:
        clock["now"] = int(last["ts"]) + 10
        broker.close(order.order_id, price=last["c"], reason="max_hold", timestamp_ms=clock["now"])

    fills = [r for r in (order.filled_qty,) if True]
    entry_px = str(order.avg_entry_price)
    exit_px = ""
    trades_csv = run_dir / "paper_trades.csv"
    if trades_csv.exists():
        with open(trades_csv, encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.DictReader(fh))
        if rows:
            exit_px = rows[-1].get("exit_price", "")
    filled = order.filled_qty
    if filled <= 0:
        status = "no_fill"          # 未成交（例如 maker 单在模拟接单时已可成交，被 broker 拒绝）
    elif order.open_qty == 0:
        status = "closed"
    else:
        status = "open"
    return {
        "signal_id": sid, "status": status,
        "code": "OK", "generated_at": sig["generated_at_utc"], "kind": sig.get("kind"),
        "side": sig.get("side"), "entry": entry_px if filled > 0 else "",
        "exit": exit_px, "qty": str(filled), "fees": str(order.entry_fee + order.exit_fee),
        "pnl": float(order.realized_pnl), "reason": order.reason or status,
        "run_dir": str(run_dir),
    }


def load_done(state_path: Path = RESULT_STATE) -> dict:
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data.get("processed", {}) if isinstance(data, dict) else {}


def save_done(processed: dict, state_path: Path = RESULT_STATE,
              result_csv: Path = RESULT_CSV) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(
        {"updated_at": iso(int(datetime.now(tz=timezone.utc).timestamp() * 1000)),
         "processed": processed}, ensure_ascii=False, indent=1), encoding="utf-8")
    rows = sorted(processed.values(), key=lambda r: r.get("generated_at", ""))
    result_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = ["signal_id", "generated_at", "kind", "side", "status", "code", "entry",
              "exit", "qty", "fees", "pnl", "reason", "run_dir"]
    with open(result_csv, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--force", action="store_true", help="重算已处理信号")
    ap.add_argument("--bar", default=BAR)
    ap.add_argument("--signals", choices=["live", "research"], default="live",
                    help="live=logs/signals.csv；research=logs/research/research_signals.csv")
    args = ap.parse_args()

    if args.signals == "research":
        sig_path = LOG_DIR / "research" / "research_signals.csv"
        result_state = STATE_DIR / "paper_results_research.json"
        result_csv = LOG_DIR / "paper" / "results_research.csv"
    else:
        sig_path = LOG_DIR / "signals.csv"
        result_state = RESULT_STATE
        result_csv = RESULT_CSV

    cfg = load_config()
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    bal = P.balance("USDT")
    equity = float(bal[0].get("totalEq")) if bal else 0.0
    print("=" * 84)
    print(f"T5 纸面回放（{args.bar} K 线，source={args.signals}）  权益基准 {equity:.4f} U   "
          f"{iso(now)}  {'[WRITE]' if args.write else '[DRY-RUN]'}")
    print("=" * 84)

    signals = [r for r in read_csv(sig_path)
               if r.get("generated_at_utc") and parse_iso(r["generated_at_utc"]) >= parse_iso("2026-09-28T00:00:00Z")]
    done = load_done(result_state)
    todo = [s for s in signals if args.force or s["id"] not in done]
    print(f"信号总数 {len(signals)}，已处理 {len(done)}，本次待回放 {len(todo)}")

    results = []
    for sig in todo:
        gen = parse_iso(sig["generated_at_utc"])
        pb = cfg.get("playbooks", {}).get(sig.get("kind") or "", {})
        hold_end = gen + int((float(pb.get("max_hold_hours", 8)) + 2) * 3600_000)
        if hold_end > now:
            print(f"  - {sig['id']} 尚未到最大持有截止，跳过")
            continue
        res = replay_signal(sig, cfg, equity, now, args.bar, LOG_DIR / "paper" / "runs")
        results.append(res)
        done[sig["id"]] = res
        flag = "✅" if res["status"] == "closed" else "⛔"
        print(f"  {flag} {res['signal_id']:<24} {res.get('kind') or '':<14} {res.get('side'):<5} "
              f"status={res['status']:<16} code={res.get('code',''):<18} "
              f"pnl={res.get('pnl', 0):+.3f}U  reason={res.get('reason','')[:22]}")

    if results:
        closed = [r for r in results if r["status"] == "closed"]
        total = sum(r["pnl"] for r in closed)
        wins = [r for r in closed if r["pnl"] > 0]
        print("-" * 84)
        print(f"本次回放 {len(results)} 条：成交并平仓 {len(closed)} 条，"
              f"胜 {len(wins)} / 负 {len(closed) - len(wins)}，净收益合计 {total:+.3f} U")
        if closed:
            print(f"平均每笔 {total / len(closed):+.3f} U；手续费合计 "
                  f"{sum(float(r['fees'] or 0) for r in closed):.3f} U")
    if args.write and results:
        save_done(done, result_state, result_csv)
        print(f"\n已写入 {result_state} 与 {result_csv}")
    elif results:
        print("\n（dry-run：未写文件，加 --write 保存结果）")
    print("\n注意：这是 K 线回放，不是实盘；滑点/排队/资金费未完整建模，不能等同于真实成交。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
