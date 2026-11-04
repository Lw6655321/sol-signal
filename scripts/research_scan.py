# -*- coding: utf-8 -*-
"""研究模式扫描：记录所有候选（含"差一点达标"的近失候选），并生成研究信号供纸面回放。

目的：引擎 3 周只发 6 条信号，样本太少无法评估策略。研究模式**不改 live config、
不发真实信号、不写 logs/signals.csv**，只做两件事：
  1. 把每次扫描的全部候选（评分/条件/净RR/是否近失）记录到 logs/research/candidates.csv
     → 用来回答"阈值定得准不准""低分候选是不是也有正期望"；
  2. 把"前提全满足 + 净RR 达标、只差评分"的近失候选转成研究信号（RSCH- 前缀）写入
     logs/research/research_signals.csv
     → 交给 scripts/paper_runner.py --signals research 回放，扩大纸面样本。

用法：
  python scripts/research_scan.py --once            # 只打印
  python scripts/research_scan.py --once --write    # 写入 logs/research/
  python scripts/research_scan.py --once --write --margin 10   # 近失边界（默认 15 分）

安全：只读公共行情 + 本地文件；不碰配置、账户、交易开关。
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import engine, risk                                   # noqa: E402
from src.collect import collect                                # noqa: E402
from src.util import (LOG_DIR, STATE_DIR, load_config, log,    # noqa: E402
                      ms_to_iso, server_now_ms, sync_server_time, write_json)

RESEARCH_DIR = LOG_DIR / "research"
CAND_CSV = RESEARCH_DIR / "candidates.csv"
SIG_CSV = RESEARCH_DIR / "research_signals.csv"
LAST_SCAN = RESEARCH_DIR / "last_scan.json"

CAND_FIELDS = ["scanned_at_utc", "kind", "side", "score", "threshold", "base_threshold",
               "deficit", "required_ok", "missing_required", "rr", "net_rr", "cost_r",
               "entry", "stop", "tp1", "passed_score", "passed_rr", "passed", "gates_pass"]
SIG_FIELDS = ["id", "generated_at_utc", "playbook", "kind", "side", "grade", "score",
              "entry", "stop", "tp1", "tp2", "expires_at_utc", "research_margin",
              "research_note"]


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    if not rows:
        return
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with open(path, "a", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def _append_signals(rows: list[dict]) -> None:
    if not rows:
        return
    # 去重：同一（kind, side, 分数档）一天只记一次，避免每日扫描堆积重复样本
    existing = set()
    if SIG_CSV.exists():
        with open(SIG_CSV, encoding="utf-8-sig", newline="") as fh:
            for r in csv.DictReader(fh):
                existing.add((r.get("kind"), r.get("side"), r.get("generated_at_utc", "")[:10],
                              str(r.get("score"))))
    fresh = [r for r in rows
             if (r["kind"], r["side"], r["generated_at_utc"][:10], str(r["score"])) not in existing]
    _append_csv(SIG_CSV, fresh, SIG_FIELDS)
    return fresh


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--margin", type=float, default=15.0,
                    help="近失边界：评分差距 <= margin 的候选生成研究信号")
    ap.add_argument("--max-research-signals", type=int, default=5)
    args = ap.parse_args()

    cfg = load_config()
    print("=" * 88)
    print(f"研究模式扫描（不改 live config / 不发真实信号）  {_now_iso()}")
    print("=" * 88)

    try:
        sync_server_time()
    except Exception as exc:                                  # noqa: BLE001
        log(f"[研究扫描] 对时失败（继续）：{exc}", "WARN")

    risk_st = risk.compute(cfg)
    snap = collect(cfg, None)
    snap["equity_estimate"] = risk_st["equity_estimate"]
    ev = engine.evaluate(snap, cfg, risk_st)
    gates = ev.get("gates") or {}
    cands = ev.get("candidates") or []
    print(f"数据闸门: {'通过' if gates.get('pass') else '未通过 ' + ' | '.join(gates.get('reasons') or [])}")
    print(f"候选数: {len(cands)}   正式信号: {len(ev.get('signals') or [])}")

    now_iso = _now_iso()
    cand_rows = []
    near_miss = []
    for c in cands:
        deficit = round(float(c["min_score"]) - float(c["score"]), 1)
        row = {
            "scanned_at_utc": now_iso, "kind": c["kind"], "side": c["side"],
            "score": c["score"], "threshold": c["min_score"],
            "base_threshold": c.get("base_min_score"), "deficit": deficit,
            "required_ok": c["required_ok"],
            "missing_required": ";".join(c.get("missing_required") or []),
            "rr": c.get("rr"), "net_rr": c.get("net_rr"), "cost_r": c.get("cost_r"),
            "entry": c.get("entry"), "stop": c.get("stop"), "tp1": c.get("tp1"),
            "passed_score": c.get("passed_score"), "passed_rr": c.get("passed_rr"),
            "passed": c.get("passed"), "gates_pass": bool(gates.get("pass")),
        }
        cand_rows.append(row)
        if (gates.get("pass") and c["required_ok"] and c.get("passed_rr")
                and not c.get("passed_score") and 0 < deficit <= args.margin):
            near_miss.append((c, deficit))
        tag = "触发" if c.get("passed") else ("近失" if 0 < deficit <= args.margin else "未达")
        print(f"  [{tag}] {c['kind']:<14} {c['side']:<5} {c['score']:5.1f}/{c['min_score']:<5} "
              f"净RR {c.get('net_rr')}  缺: {','.join((c.get('missing_required') or [])[:2]) or '-'}")

    near_miss.sort(key=lambda x: -x[0]["score"])
    near_miss = near_miss[:args.max_research_signals]
    sig_rows = []
    for c, deficit in near_miss:
        p = cfg.get("playbooks", {}).get(c["kind"]) or {}
        entry, stop, tp1 = float(c["entry"]), float(c["stop"]), float(c["tp1"])
        long_side = c["side"] == "long"
        r_mult = float(c.get("r_mult") or 1.8)
        a = abs(entry - stop)
        tp2 = entry + (r_mult + 1.6) * a if long_side else entry - (r_mult + 1.6) * a
        ts = server_now_ms()
        expiry_min = int(p.get("expiry_min", 60))
        sig_rows.append({
            "id": f"RSCH-{c['kind'].split('_')[0][:3].upper()}-{c['side'][:4].upper()}-{ts}",
            "generated_at_utc": ms_to_iso(ts),
            "playbook": p.get("label", c["kind"]), "kind": c["kind"], "side": c["side"],
            "grade": "R", "score": c["score"], "entry": round(entry, 3),
            "stop": round(stop, 3), "tp1": round(tp1, 3), "tp2": round(tp2, 3),
            "expires_at_utc": ms_to_iso(ts + expiry_min * 60_000),
            "research_margin": deficit,
            "research_note": "近失候选（前提满足/净RR达标，仅评分差 %.1f 分）；仅用于纸面研究，不是实盘信号" % deficit,
        })

    print(f"\n近失候选（评分差 ≤ {args.margin}）：{len(sig_rows)} 条"
          + (f" → {', '.join(s['id'] for s in sig_rows)}" if sig_rows else ""))

    if args.write:
        _append_csv(CAND_CSV, cand_rows, CAND_FIELDS)
        fresh = _append_signals(sig_rows)
        write_json(LAST_SCAN, {
            "scanned_at_utc": now_iso, "gates_pass": bool(gates.get("pass")),
            "gates": gates, "candidates": len(cands),
            "near_miss": len(sig_rows), "new_research_signals": len(fresh),
            "equity_estimate": risk_st.get("equity_estimate"),
        })
        print(f"\n已写入 {CAND_CSV}（{len(cand_rows)} 行）、{SIG_CSV}（新增 {len(fresh)} 条）、{LAST_SCAN}")
    else:
        print("\n（dry-run：未写文件，加 --write 保存）")
    print("\n注意：研究信号只用纸面回放，不得当作实盘信号；live 信号仍以 logs/signals.csv 为准。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
