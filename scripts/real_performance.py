# -*- coding: utf-8 -*-
"""真实业绩与资金报告：把 OKX 全量数据拉全，算清「到底赚没赚、充了多少钱」。

数据来源（全部只读）：
  1) /api/v5/account/positions-history   已平仓位的官方结算（分页拉全）
  2) /api/v5/account/bills / bills-archive / bills-history-archive   资金流水（分页拉全）
  3) /api/v5/asset/deposit-history / withdrawal-history   充值提现记录（有权限才可读）

输出：logs/real_performance_report.md（同时打印到终端）
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                    # noqa: E402
from src.util import LOG_DIR, DataError             # noqa: E402

DAY_MS = 86_400_000


def f(x, d=0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def ms2iso(ms) -> str:
    try:
        v = int(ms)
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return "?"


def paginate_positions() -> list[dict]:
    out: dict[str, dict] = {}
    after = None
    for _ in range(60):
        params = {"instType": "SWAP", "limit": "100"}
        if after:
            params["after"] = str(after)
        rows = P.private("GET", "/api/v5/account/positions-history", params)
        if not rows:
            break
        for r in rows:
            out[str(r.get("posId"))] = r
        oldest = min(int(r.get("uTime") or 0) for r in rows)
        if len(rows) < 100:
            break
        if after is not None and oldest >= int(after):
            break
        after = oldest - 1
    return list(out.values())


def paginate_bills() -> list[dict]:
    out: dict[str, dict] = {}
    for path in ("/api/v5/account/bills",
                 "/api/v5/account/bills-archive",
                 "/api/v5/account/bills-history-archive"):
        after = None
        ok = False
        for _ in range(80):
            params: dict = {"limit": "100"}
            if path == "/api/v5/account/bills":
                params["instType"] = "SWAP"
            if after:
                params["after"] = str(after)
            try:
                rows = P.private("GET", path, params)
            except (P.PrivateError, DataError) as exc:
                if after is None:
                    print(f"  （{path} 不可用/无权限，跳过：{str(exc)[:80]}）")
                break
            ok = True
            if not rows:
                break
            for r in rows:
                out[str(r.get("billId"))] = r
            oldest = min(int(r.get("billId") or 0) for r in rows)
            if len(rows) < 100:
                break
            if after is not None and oldest >= int(after):
                break
            after = oldest - 1
        if ok:
            print(f"  （{path} 读取完成）")
    return list(out.values())


def main() -> int:
    lines: list[str] = []

    def say(s: str = "") -> None:
        print(s)
        lines.append(s)

    say("=" * 88)
    say("真实业绩与资金报告（OKX 官方数据）")
    say(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    say("=" * 88)

    bal = P.balance("USDT")
    equity = f(bal[0].get("totalEq")) if bal else 0.0
    say(f"\n当前总权益: {equity:.4f} USDT")

    # ---------------- 1. 已平仓位 ----------------
    pos = paginate_positions()
    say(f"\n【1】已平仓位全量: {len(pos)} 个")

    by_inst: dict[str, list[dict]] = defaultdict(list)
    for p in pos:
        by_inst[str(p.get("instId"))].append(p)

    say("-" * 88)
    say(f"{'品种':<20}{'仓位':>5}{'盈亏合计u':>12}{'手续费u':>11}{'资金费u':>10}")
    tot_pnl = tot_fee = tot_fund = 0.0
    for inst, rows in sorted(by_inst.items(), key=lambda kv: -abs(sum(f(x.get("realizedPnl")) for x in kv[1]))):
        pnl = sum(f(x.get("realizedPnl")) for x in rows)
        fee = sum(abs(f(x.get("fee"))) for x in rows)
        fund = sum(f(x.get("fundingFee")) for x in rows)
        tot_pnl += pnl
        tot_fee += fee
        tot_fund += fund
        say(f"{inst:<20}{len(rows):>5}{pnl:>12.2f}{fee:>11.2f}{fund:>10.2f}")
    say("-" * 88)
    say(f"{'合计':<20}{len(pos):>5}{tot_pnl:>12.2f}{tot_fee:>11.2f}{tot_fund:>10.2f}")

    wins = [p for p in pos if f(p.get("realizedPnl")) > 0]
    losses = [p for p in pos if f(p.get("realizedPnl")) < 0]
    gp = sum(f(p.get("realizedPnl")) for p in wins)
    gl = sum(f(p.get("realizedPnl")) for p in losses)
    say(f"\n盈利 {len(wins)} 笔  亏损 {len(losses)} 笔  胜率 "
        f"{len(wins) / max(1, len(pos)) * 100:.1f}%")
    say(f"总盈利 {gp:+.2f}  总亏损 {gl:+.2f}  盈亏比(总额) {abs(gp / gl) if gl else 0:.2f}")
    if pos:
        best = max(pos, key=lambda p: f(p.get("realizedPnl")))
        worst = min(pos, key=lambda p: f(p.get("realizedPnl")))
        say(f"最大单笔盈利 {f(best.get('realizedPnl')):+.2f} ({best.get('instId')})  "
            f"最大单笔亏损 {f(worst.get('realizedPnl')):+.2f} ({worst.get('instId')})")
        times = [int(p.get("uTime") or 0) for p in pos if p.get("uTime")]
        if times:
            say(f"最早平仓 {ms2iso(min(times))}   最近平仓 {ms2iso(max(times))}")

    # ---------------- 2. 资金流水 ----------------
    try:
        bills = paginate_bills()
    except Exception as exc:                            # noqa: BLE001
        bills = []
        say(f"资金流水读取失败: {exc}")
    say(f"\n【2】资金流水全量: {len(bills)} 笔")
    by_type = defaultdict(lambda: [0, 0.0])
    for b in bills:
        t = str(b.get("type"))
        by_type[t][0] += 1
        by_type[t][1] += f(b.get("balChg"))
    say("-" * 88)
    say(f"{'流水类型':<14}{'笔数':>6}{'balChg合计(u)':>16}")
    for t, (n, s) in sorted(by_type.items(), key=lambda kv: -abs(kv[1][1])):
        say(f"type={t:<9}{n:>6}{s:>16.2f}")

    # 交易类 / 转移类拆分（OKX: 1=划转 2=交易 3=交割 4=ADL 5=强平 6=保证金划转 7=资金费）
    trade_types = {"2", "3", "4", "5", "7"}
    transfer_types = {"1", "6"}
    trade_net = sum(f(b.get("balChg")) for b in bills if str(b.get("type")) in trade_types)
    transfer_net = sum(f(b.get("balChg")) for b in bills if str(b.get("type")) in transfer_types)
    other_net = sum(f(b.get("balChg")) for b in bills
                    if str(b.get("type")) not in trade_types | transfer_types)
    say(f"\n交易类流水合计（含手续费/资金费/强平）: {trade_net:+.2f} u")
    say(f"划转类流水合计（进出交易账户）: {transfer_net:+.2f} u")
    say(f"其他类流水合计: {other_net:+.2f} u")
    derived = equity - trade_net - other_net
    say(f"→ 反推「净转入交易账户」≈ {derived:+.2f} u（= 当前权益 − 交易类流水）")

    # ---------------- 3. 充值/提现记录 ----------------
    say("\n【3】充值 / 提现记录")
    dep_total = defaultdict(float)
    wd_total = defaultdict(float)
    for path, label, bucket in (
        ("/api/v5/asset/deposit-history", "充值", dep_total),
        ("/api/v5/asset/withdrawal-history", "提现", wd_total),
    ):
        try:
            rows = P.private("GET", path, {"limit": "100"})
            for r in rows:
                bucket[str(r.get("ccy"))] += f(r.get("amt"))
            say(f"  {label}记录: {len(rows)} 条")
        except (P.PrivateError, DataError) as e:
            say(f"  {label}记录读取失败（可能需要资产权限）: {str(e)[:120]}")
    if dep_total:
        say(f"  充值合计: " + ", ".join(f"{k} {v:.4f}" for k, v in dep_total.items()))
    if wd_total:
        say(f"  提现合计: " + ", ".join(f"{k} {v:.4f}" for k, v in wd_total.items()))

    # ---------------- 4. 结论 ----------------
    say("\n" + "=" * 88)
    say("【4】结论")
    say("=" * 88)
    say(f"· 官方口径已平仓位盈亏: {tot_pnl:+.2f} u（不含资金费；资金费 {tot_fund:+.2f} u）")
    say(f"· 手续费合计: {tot_fee:.2f} u（占亏损的 "
        f"{tot_fee / abs(tot_pnl) * 100 if tot_pnl else 0:.0f}%）" if tot_pnl else f"· 手续费合计: {tot_fee:.2f} u")
    say(f"· 反推净投入: {derived:+.2f} u；当前权益 {equity:.2f} u")
    say("· 注意：positions-history/bills 的翻页上限可能导致早期数据缺失；")
    say("  币本位品种的 pnl/fee 单位可能是币而非 U，若品种列表里出现 -USD-SWAP，数字需按币价折算。")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out = LOG_DIR / "real_performance_report.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n已写入 {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
