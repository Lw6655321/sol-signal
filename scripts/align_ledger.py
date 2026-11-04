# -*- coding: utf-8 -*-
"""T0 台账对齐：用 OKX 官方平仓历史重建本地 SOL 台账 + 把风控权益改成真实权益。

背景：本地 logs/trades.csv 只记录了 7 笔（+13.42u），与 OKX 官方平仓历史严重脱节；
state/risk_state.json 的 equity_estimate 还停留在 113.42，而账户实际权益 117.72。

本脚本（只读账户 + 只写本地文件）：
  1. 拉取 SOL-USDT-SWAP 的官方平仓历史（positions-history，页大小 100，自动翻页）
  2. 重建 logs/trades.csv（字段与 src/risk.py 完全兼容）
  3. 重建 state/risk_state.json：权益 = OKX 实时权益；统计从重建历史重算
  4. 写前自动备份原文件

用法：
  python scripts/align_ledger.py            # 只出报告（dry-run）
  python scripts/align_ledger.py --write    # 真正写回
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                        # noqa: E402
from src import risk as R                              # noqa: E402
from src.util import (LOG_DIR, STATE_DIR, load_config,  # noqa: E402
                      read_csv, write_json)

INST = "SOL-USDT-SWAP"
TRADE_FIELDS = R.TRADE_FIELDS
# 系统上线日：trades.csv 只装系统上线以来的真实平仓记录（更早的历史只做参考文件）
CUT_DATE = "2026-09-28"


def _f(x, d=0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _iso(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _day(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date().isoformat()


def fetch_sol_positions() -> list[dict]:
    """拉全量 SOL 平仓历史。

    注意：OKX 的 positions-history 里 posId 会在不同仓位间复用（全仓模式），
    不能用 posId 去重；每行是一条独立的平仓记录，用 (posId, uTime) 去重即可。
    """
    found: dict[tuple, dict] = {}
    after = None
    for _ in range(40):
        params = {"instType": "SWAP", "instId": INST, "limit": "100"}
        if after:
            params["after"] = str(after)
        page = P.private("GET", "/api/v5/account/positions-history", params)
        if not page:
            break
        for r in page:
            found[(str(r.get("posId")), str(r.get("uTime")))] = r
        oldest = min(int(r.get("uTime") or 0) for r in page)
        if len(page) < 100:
            break
        if after is not None and oldest >= int(after):
            break
        after = oldest - 1
    return sorted(found.values(), key=lambda r: int(r.get("uTime") or 0))


def to_trade(r: dict, equity: float) -> dict:
    pos_id = str(r.get("posId"))
    direction = str(r.get("direction") or r.get("posSide") or "")
    side = "long" if direction.lower().startswith("long") else "short"
    entry = _f(r.get("openAvgPx"))
    exit_px = _f(r.get("closeAvgPx"))
    qty = _f(r.get("closeTotalPos"))
    pnl = round(_f(r.get("realizedPnl")), 4)            # 已含手续费/资金费
    fee = round(abs(_f(r.get("fee"))), 6)
    funding = round(_f(r.get("fundingFee")), 6)
    return {
        "signal_id": f"OKX-SOL-{pos_id[-5:]}-{str(r.get('uTime'))[-6:]}",
        "opened_at_utc": _iso(r.get("cTime")),
        "closed_at_utc": _iso(r.get("uTime")),
        "side": side,
        "entry": entry,
        "exit": exit_px,
        "qty": qty,
        "notional": round(entry * qty, 3),
        "fees_usd": fee,
        "pnl_usd": pnl,
        "pnl_pct_of_equity": round(pnl / equity * 100 if equity else 0, 3),
        "r_multiple": None,
        "exit_reason": "okx_rebuilt",
        "notes": (f"官方平仓历史重建 posId={pos_id} lever={r.get('lever')} "
                  f"funding={funding} mgn={r.get('mgnMode')} type={r.get('type')}"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    cfg = load_config()
    print("=" * 84)
    print("T0 台账对齐：" + ("[WRITE 真写]" if args.write else "[DRY-RUN 只报告]"))
    print("=" * 84)

    bal = P.balance("USDT")
    equity = _f(bal[0].get("totalEq")) if bal else 0.0
    print(f"OKX 实时权益: {equity:.4f} USDT")

    st = R.load_state(cfg)
    print(f"本地 risk_state: equity_estimate={_f(st.get('equity_estimate')):.4f} "
          f"day_start={_f(st.get('day_start_equity')):.4f} date={st.get('date')}")

    old_rows = read_csv(R.TRADES_CSV)
    old_pnl = sum(_f(r.get("pnl_usd")) for r in old_rows)
    print(f"本地 trades.csv: {len(old_rows)} 笔  盈亏合计 {old_pnl:+.4f}")

    print("\n拉取 OKX SOL 全量平仓历史 ...")
    pos = fetch_sol_positions()
    all_trades = [to_trade(r, equity) for r in pos]
    trades = [t for t in all_trades if t["closed_at_utc"][:10] >= CUT_DATE]
    total_pnl = sum(_f(t["pnl_usd"]) for t in all_trades)
    all_fees = sum(_f(t["fees_usd"]) for t in all_trades)
    sys_pnl = sum(_f(t["pnl_usd"]) for t in trades)
    wins = [t for t in trades if _f(t["pnl_usd"]) > 0]
    losses = [t for t in trades if _f(t["pnl_usd"]) < 0]
    print(f"全量官方平仓: {len(all_trades)} 笔  "
          f"{all_trades[0]['closed_at_utc'][:10] if all_trades else '-'} ~ "
          f"{all_trades[-1]['closed_at_utc'][:10] if all_trades else '-'}")
    print(f"  历史累计实现盈亏: {total_pnl:+.4f}   手续费: {all_fees:.4f}")
    print(f"系统上线以来（>={CUT_DATE}）: {len(trades)} 笔  "
          f"实现盈亏 {sys_pnl:+.4f}  "
          f"胜 {len(wins)} / 负 {len(losses)}  "
          f"胜率 {len(wins) / max(1, len(trades)) * 100:.1f}%  →  写入 trades.csv")

    # 对比
    print("\n最近 5 笔（官方口径）:")
    for t in trades[-5:]:
        print(f"  {t['closed_at_utc'][:16]}  {t['side']:<5} {t['entry']:>8.2f}→{t['exit']:>8.2f}  "
              f"qty {t['qty']:>6.2f}  pnl {t['pnl_usd']:>+9.3f}u")

    # 连续亏损
    consec = 0
    for t in reversed(trades):
        if _f(t["pnl_usd"]) < 0:
            consec += 1
        else:
            break
    print(f"\n当前连续亏损: {consec} 笔")

    if not args.write:
        print("\n（dry-run：未写任何文件。核对无误后加 --write）")
        return 0

    # ---------- 备份 ----------
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bdir = LOG_DIR / "backup"
    bdir.mkdir(parents=True, exist_ok=True)
    if R.TRADES_CSV.exists():
        shutil.copy2(R.TRADES_CSV, bdir / f"trades.before-rebuild.{stamp}.csv")
    sbak = STATE_DIR / "backup"
    sbak.mkdir(parents=True, exist_ok=True)
    if R.STATE_PATH.exists():
        shutil.copy2(R.STATE_PATH, sbak / f"risk_state.before-rebuild.{stamp}.json")
    print(f"\n已备份: {bdir} / {sbak}")

    # ---------- 写 trades.csv（系统上线以来）+ 全量参考文件 ----------
    with open(R.TRADES_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_FIELDS)
        w.writeheader()
        for t in trades:
            w.writerow({k: t.get(k) for k in TRADE_FIELDS})
    print(f"已写入 {R.TRADES_CSV}（{len(trades)} 笔，>={CUT_DATE}）")

    hist_path = LOG_DIR / "official_sol_history.csv"
    with open(hist_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["closed_at_utc", "opened_at_utc", "posId", "side", "entry", "exit",
                    "qty", "realized_pnl", "fee", "funding_fee", "leverage", "mgn_mode"])
        for r in pos:
            w.writerow([_iso(r.get("uTime")), _iso(r.get("cTime")), r.get("posId"),
                        r.get("direction") or r.get("posSide"), r.get("openAvgPx"),
                        r.get("closeAvgPx"), r.get("closeTotalPos"), r.get("realizedPnl"),
                        r.get("fee"), r.get("fundingFee"), r.get("lever"), r.get("mgnMode")])
    print(f"已写入 {hist_path}（全量 {len(pos)} 笔，仅供参考）")

    # ---------- 写 risk_state ----------
    today = datetime.now(timezone.utc).date().isoformat()
    new_state = {
        "date": today,
        "day_start_equity": equity,
        "equity_estimate": equity,
        "realized_pnl_today": 0.0,
        "entries_today": 0,
        "open_positions": [],
        "consecutive_losses": consec,
        "paused_until_ms": 0,
        "history": [{"signal_id": t["signal_id"], "exit": t["exit"],
                     "pnl": _f(t["pnl_usd"]), "closed_day": t["closed_at_utc"][:10]}
                    for t in trades[-50:]],
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "_rebuilt_from": "OKX positions-history",
        "_rebuilt_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_max_entries": int(cfg["risk"]["max_entries_per_day"]),
    }
    write_json(R.STATE_PATH, new_state)
    print(f"已写入 {R.STATE_PATH}（权益 {equity:.4f}，{len(trades)} 笔统计）")

    # ---------- 复核 ----------
    print("\n复核：")
    s = R.stats()
    print(f"  risk.stats(): {s}")
    st2 = R.load_state(cfg)
    print(f"  load_state(): equity_estimate={_f(st2.get('equity_estimate')):.4f} "
          f"entries_left={int(cfg['risk']['max_entries_per_day']) - int(st2.get('entries_today', 0))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
