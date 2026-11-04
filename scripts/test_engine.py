# -*- coding: utf-8 -*-
"""引擎回归测试：验证"信号生成"与"风控否决"两条路径都能正确工作。

为什么需要它：实盘里大部分时间没有信号，触发路径可能长期不被执行。
这里用注入的合成候选把整条路径跑通，并对止损距离/手续费门槛做边界验证。

运行：python scripts/test_engine.py      退出码 0 = 全部通过
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import engine                      # noqa: E402
from src.collect import collect             # noqa: E402
from src.util import load_config            # noqa: E402

PASS, FAIL = [], []


def check(name: str, cond: bool, detail: str = ""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'✔' if cond else '✘'} {name}" + (f"   {detail}" if detail else ""))


def main() -> int:
    cfg = load_config()
    print("采集实时快照（用于测试）...")
    snap = collect(cfg, None)
    px = snap["features"]["book"]["microprice"]
    atr5 = snap["features"]["tf"]["5m"]["atr14"]
    print(f"现价 {px:.3f}  5m ATR {atr5:.4f}  净值 {cfg['account']['equity_usdt']}u  "
          f"杠杆设置 {cfg['account']['leverage_setting']}x\n")

    # ---------- 1. 仓位计算：正常止损
    print("[1] 仓位计算（正常止损）")
    p = engine._params("scalp_5m", cfg)
    stop = px - max(atr5, px * p["min_stop_pct"] / 100)
    r = engine.position_size(px, stop, snap, cfg, p)
    check("正常止损可算出仓位", r.get("ok"), r.get("reason", ""))
    if r.get("ok"):
        check("手数为0.01的整数倍", abs(round(r["qty"] / 0.01) * 0.01 - r["qty"]) < 1e-9, f"qty={r['qty']}")
        check("真实风险≈计划风险2u", abs(r["effective_risk"] - 2.0) < 1.0,
              f"含费滑={r['effective_risk']:.3f}u")
        check("实际杠杆≤杠杆设置8x", r["leverage"] <= 8.0, f"{r['leverage']:.2f}x")
        check("挂单手续费占比≤22%", r["fee_share_maker"] <= 0.22,
              f"{r['fee_share_maker'] * 100:.1f}%")

    # ---------- 2. 仓位计算：止损过窄 -> 手续费吃掉风险，必须否决
    print("\n[2] 止损过窄（0.15%）必须被手续费门槛否决")
    r2 = engine.position_size(px, px * (1 - 0.0015), snap, cfg, p)
    check("0.15%止损被否决", not r2.get("ok"), r2.get("reason", "")[:60])

    # ---------- 3. 止损过宽 -> 必须否决
    print("\n[3] 止损超过玩法上限（1.20%）必须被否决")
    r3 = engine.position_size(px, px * (1 - 0.02), snap, cfg, p)
    check("2%止损被否决", not r3.get("ok"), r3.get("reason", "")[:60])

    # ---------- 4. 杠杆不足时给出明确提示
    print("\n[4] 杠杆设置不足时应提示需要多少倍")
    cfg_low = load_config()
    cfg_low["account"]["leverage_setting"] = 2
    r4 = engine.position_size(px, stop, snap, cfg_low, p)
    check("2x杠杆设置下被否决", not r4.get("ok"), r4.get("reason", "")[:70])
    check("提示里给出了所需倍数", "杠杆设置至少" in (r4.get("reason") or ""))

    # ---------- 5. 完整信号构建路径（注入合成候选）
    print("\n[5] 信号构建路径（注入合成候选，前提全满足）")
    real_pullback = engine._pullback

    def fake_pullback(snap_, cfg_, side, name):
        if side != "long":
            return None
        p_ = engine._params(name, cfg_)
        tfp = (snap_["features"]["tf"] or {}).get(p_["primary"]) or {}
        a = tfp.get("atr14") or 0
        entry = snap_["features"]["book"]["microprice"]
        st = entry - max(a, entry * p_["min_stop_pct"] / 100)
        conds = [engine._cond("过滤周期趋势方向一致", True, 3.0, "合成测试", required=True),
                 engine._cond("交易成本可接受", True, 2.0, "合成测试", required=True),
                 engine._cond("主动买卖流同向", True, 2.5, "合成测试")]
        return {"kind": name, "side": side, "entry": entry, "stop": st,
                "target_ref": entry + 6 * a, "conds": conds, "params": p_, "notes": ["合成测试"]}

    engine._pullback = fake_pullback
    try:
        ev = engine.evaluate(snap, cfg, None)
    finally:
        engine._pullback = real_pullback

    sigs = ev.get("signals") or []
    check("生成了信号", len(sigs) >= 1, f"共 {len(sigs)} 条")
    if sigs:
        s = sigs[0]
        need = ["id", "playbook", "kind", "side", "score", "entry", "stop", "tp1", "tp2",
                "rr1", "net_rr1", "cost_r", "qty", "notional_usd", "leverage_used",
                "leverage_setting_needed", "planned_risk_usd", "effective_risk_usd",
                "fee_share_of_risk", "expires_at_utc", "invalidation", "evidence", "entry_type"]
        missing = [k for k in need if k not in s]
        check("信号字段完整", not missing, f"缺少 {missing}" if missing else f"{len(need)} 个字段齐全")
        check("止损在入场下方（做多）", s["stop"] < s["entry"], f"{s['stop']} < {s['entry']}")
        check("TP1在入场上方（做多）", s["tp1"] > s["entry"], f"{s['tp1']} > {s['entry']}")
        check("净赔率 < 毛赔率（已扣手续费）", s["net_rr1"] < s["rr1"],
              f"净{s['net_rr1']} vs 毛{s['rr1']}")
        check("数据源字段都带前提标记", all("前提" in e for e in s["evidence"]))
        check("失效条件非空", len(s["invalidation"]) >= 4, f"{len(s['invalidation'])} 条")
        print("\n  合成信号样例：")
        for k in ("id", "playbook", "side", "entry", "stop", "tp1", "tp2", "rr1", "net_rr1",
                  "cost_r", "qty", "notional_usd", "leverage_used", "leverage_setting_needed",
                  "effective_risk_usd", "fee_share_of_risk", "entry_type", "expires_at_utc"):
            print(f"    {k:26s} {s[k]}")

    print("\n" + "=" * 66)
    print(f"通过 {len(PASS)} 项，失败 {len(FAIL)} 项")
    if FAIL:
        print("失败项：" + ", ".join(FAIL))
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
