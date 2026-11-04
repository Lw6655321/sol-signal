# -*- coding: utf-8 -*-
"""从 OKX 成交历史重建真实交易台账。

用途：本地 trades.csv / risk_state.json 与真实账户脱节时，以交易所成交为准重建。
流程：
  1. 分窗口拉取 OKX fills-history（真实成交明细，含手续费）
  2. 按「持仓从 0 到 0」切分成回合（round trip），计算每个回合的真实盈亏
  3. 与本地 records 对比，输出报告；--write 时才写回文件（先自动备份）

用法：
  python scripts/rebuild_ledger.py             # 只出报告，不改文件
  python scripts/rebuild_ledger.py --write     # 写 logs/trades_rebuilt.csv
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

from src import okx_private as P                    # noqa: E402
from src.util import LOG_DIR, load_config           # noqa: E402

INST = "SOL-USDT-SWAP"
DAY_MS = 86_400_000


def ts2day(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def ts2iso(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_all_fills(begin_ms: int, end_ms: int) -> list[dict]:
    """按 7 天窗口拉全量成交，按 tradeId 去重。"""
    found: dict[str, dict] = {}
    t = begin_ms
    while t < end_ms:
        e = min(t + 7 * DAY_MS, end_ms)
        try:
            rows = P.fills_history(t, e, 100)
        except P.PrivateError as exc:
            print(f"  ⚠️ 窗口 {ts2day(t)}~{ts2day(e)} 拉取失败: {exc}")
            rows = []
        for r in rows:
            key = str(r.get("tradeId") or f"{r.get('ordId')}_{r.get('fillTime')}")
            found[key] = r
        t = e
    return sorted(found.values(), key=lambda r: int(r.get("fillTime") or 0))


def build_round_trips(fills: list[dict]) -> tuple[list[dict], float]:
    """把成交序列切成回合（持仓 0 -> 0），返回 (trips, 毛实现盈亏)。"""
    trips: list[dict] = []
    pos = 0.0
    avg = 0.0
    realized = 0.0
    trip: dict | None = None

    def new_trip(t: int, px: float) -> dict:
        return {"start_ms": t, "end_ms": t, "fills": 0, "fees": 0.0, "pnl": 0.0,
                "entry_px": px, "exit_px": px, "direction": ""}

    for f in fills:
        side = str(f.get("side") or "").lower()
        sz = float(f.get("fillSz") or 0)
        px = float(f.get("fillPx") or 0)
        fee = float(f.get("fee") or 0)          # OKX 负数=扣费
        t = int(f.get("fillTime") or 0)
        if sz <= 0 or px <= 0 or side not in ("buy", "sell"):
            continue
        if trip is None:
            trip = new_trip(t, px)
        trip["fills"] += 1
        trip["fees"] += abs(fee)

        q = sz if side == "buy" else -sz
        remaining = sz
        while remaining > 1e-12:
            if pos == 0:
                pos = remaining if q > 0 else -remaining
                avg = px
                if not trip["direction"]:
                    trip["direction"] = "long" if pos > 0 else "short"
                remaining = 0
            elif (pos > 0) == (q > 0):
                signed = remaining if q > 0 else -remaining
                total = abs(pos) + abs(signed)
                avg = (avg * abs(pos) + px * abs(signed)) / total
                pos += signed
                remaining = 0
            else:
                close_qty = min(abs(pos), remaining)
                pnl = (px - avg) * close_qty * (1 if pos > 0 else -1)
                realized += pnl
                trip["pnl"] += pnl
                pos += (-close_qty if pos > 0 else close_qty)
                remaining -= close_qty
                if abs(pos) < 1e-12:
                    pos = 0.0
                    avg = 0.0
                    trip["end_ms"] = t
                    trip["exit_px"] = px
                    trips.append(trip)
                    if remaining > 1e-12:
                        # 同一笔成交里反手开新仓
                        trip = new_trip(t, px)
                        trip["fills"] += 1
                        trip["fees"] = abs(fee)
                        pos = remaining if q > 0 else -remaining
                        trip["direction"] = "long" if pos > 0 else "short"
                        avg = px
                        remaining = 0
                    else:
                        trip = None

    if trip is not None:
        trip["end_ms"] = int(fills[-1].get("fillTime") or trip["start_ms"]) if fills else trip["start_ms"]
        trip["exit_px"] = float(fills[-1].get("fillPx") or 0) if fills else 0
        trip["open"] = True
        trips.append(trip)

    return trips, realized


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="写 logs/trades_rebuilt.csv（默认只出报告）")
    args = ap.parse_args()

    cfg = load_config()
    start_equity = float(cfg["account"]["equity_usdt"])
    print("=" * 78)
    print("OKX 成交历史 → 本地台账重建")
    print("=" * 78)

    st = P.status()
    print(f"凭据: {'已配置' if st['configured'] else '未配置'}   模拟盘: {'是' if st['simulated'] else '否'}   "
          f"白名单: {', '.join(st['allowed_inst'])}")

    bal = P.balance("USDT")
    equity = float(bal[0].get("totalEq")) if bal else 0.0
    delta = equity - start_equity
    print(f"当前总权益: {equity:.4f} USDT   初始本金: {start_equity:.2f}   "
          f"权益变动: {delta:+.4f}")

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    begin_ms = now_ms - 90 * DAY_MS
    print(f"\n拉取成交: {ts2day(begin_ms)} ~ {ts2day(now_ms)} ...")
    fills = fetch_all_fills(begin_ms, now_ms)
    print(f"共 {len(fills)} 笔成交")
    if not fills:
        print("没有成交记录，退出。")
        return 0

    trips, realized = build_round_trips(fills)
    fees = sum(abs(float(f.get("fee") or 0)) for f in fills)
    print(f"回合数: {len(trips)}   手续费合计: {fees:.4f} USDT")
    print(f"毛实现盈亏: {realized:+.4f}   扣费后: {realized - fees:+.4f}   "
          f"与权益变动核对差: {delta - (realized - fees):+.4f}")

    print("\n" + "-" * 78)
    print(f"{'#':>3} {'方向':<6} {'开→平':<25} {'均价→均出':<20} {'盈亏u':>9}")
    for i, t in enumerate(trips, 1):
        print(f"{i:>3} {t.get('direction', ''):<6} "
              f"{ts2day(t['start_ms'])}→{ts2day(int(t['end_ms'])):<14} "
              f"{t.get('entry_px', 0):.2f}→{t.get('exit_px', 0):.2f}      "
              f"{t['pnl']:+9.4f}")
    print("-" * 78)

    trades_csv = LOG_DIR / "trades.csv"
    local = []
    if trades_csv.exists():
        with open(trades_csv, encoding="utf-8-sig", newline="") as f:
            local = list(csv.DictReader(f))
    local_pnl = sum(float(x.get("pnl_usd") or 0) for x in local)
    print(f"本地 trades.csv 记录: {len(local)} 笔   合计盈亏 {local_pnl:+.4f}")
    for x in local:
        print(f"  {x.get('signal_id',''):<10} {x.get('side',''):<5} "
              f"entry={x.get('entry','')} exit={x.get('exit','')} pnl={x.get('pnl_usd','')}")

    if args.write:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if trades_csv.exists():
            shutil.copy2(trades_csv, trades_csv.with_name(f"trades.backup.{stamp}.csv"))
        out = LOG_DIR / "trades_rebuilt.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["trade_no", "opened_at_utc", "closed_at_utc", "direction",
                        "entry_avg", "exit_avg", "fills", "fees_usd", "pnl_usd", "source"])
            for i, t in enumerate(trips, 1):
                w.writerow([i, ts2iso(t["start_ms"]), ts2iso(int(t["end_ms"])),
                            t.get("direction", ""), f"{t.get('entry_px', 0):.6f}",
                            f"{t.get('exit_px', 0):.6f}", t["fills"],
                            f"{t['fees']:.6f}", f"{t['pnl']:.6f}", "okx_fills_rebuilt"])
        print(f"\n✅ 已写 {out}")
        print("ℹ️ risk_state.json 未自动覆盖，先核对后再更新")
    else:
        print("\n（报告模式，未写任何文件。核对无误后加 --write）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
