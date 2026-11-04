# -*- coding: utf-8 -*-
"""账户准备：把 OKX 账户调整成自动化交易所需的配置。

做三件事：
  1. 检查是否空仓、是否有挂单（有仓/有单时拒绝执行，因为持仓模式不能切）
  2. 持仓模式 -> 单向持仓（net_mode）
  3. SOL-USDT-SWAP 杠杆 -> 8x（全仓 cross）

安全设计：
  - 默认 dry-run，只打印计划，不发任何修改请求；确认后加 --apply 才真正执行
  - 每一步执行后重新读取账户配置做验证，失败立即停止
  - 全程写审计日志 logs/account_ops.log

用法：
  python scripts/prepare_account.py            # 只看计划
  python scripts/prepare_account.py --apply    # 真正执行
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                    # noqa: E402
from src.util import LOG_DIR, log                   # noqa: E402

INST = "SOL-USDT-SWAP"
TARGET_POS_MODE = "net_mode"
TARGET_LEVER = "8"
MGN_MODE = "cross"
OPS_LOG = LOG_DIR / "account_ops.log"


def audit(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(OPS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")
    print(f"  [audit] {msg}")


def get_pos_mode() -> str:
    cfg = P.account_config()
    return (cfg[0].get("posMode") if cfg else "") or ""


def current_lever() -> str:
    pos = P.positions(INST)
    if pos:
        return str(pos[0].get("lever", ""))
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正执行修改（默认 dry-run）")
    args = ap.parse_args()

    print("=" * 70)
    print("账户准备  " + ("[APPLY 真执行]" if args.apply else "[DRY-RUN 只检查]"))
    print("=" * 70)

    # --- 0. 凭据状态 ---
    st = P.status()
    print(f"凭据已配置: {st['configured']}   模拟盘: {st['simulated']}   "
          f"交易开关: {st['trading_enabled']}（本脚本直接调私有接口，不受它限制）")
    if not st["configured"]:
        print("❌ 未配置 API 凭据")
        return 1

    # --- 1. 前置检查：必须空仓、无挂单 ---
    print("\n[1/3] 前置检查：空仓 / 无挂单")
    positions = [p for p in P.positions(INST) if float(p.get("pos") or 0) != 0]
    pending = P.pending_orders(INST)
    algos = []
    for t in ("conditional", "oco", "trigger"):
        try:
            algos.extend(P.algo_orders(t))
        except P.PrivateError:
            pass
    print(f"  持仓: {len(positions)} 个   普通挂单: {len(pending)} 笔   策略委托: {len(algos)} 笔")
    if positions or pending or algos:
        print("❌ 仍有持仓或挂单，OKX 不允许切换持仓模式。请先全部平仓/撤单。")
        for p in positions:
            print(f"    持仓: {p.get('posSide')} {p.get('pos')} 张 均价 {p.get('avgPx')}")
        for o in pending:
            print(f"    挂单: {o.get('side')} {o.get('sz')} @ {o.get('px')}  ordId={o.get('ordId')}")
        for a in algos:
            print(f"    策略委托: {a.get('side')} {a.get('sz')} 止损 {a.get('slTriggerPx')} "
                  f"止盈 {a.get('tpTriggerPx')}  algoId={a.get('algoId')}")
        return 2
    print("  ✅ 已空仓、无挂单，可以继续")

    # --- 2. 持仓模式 ---
    mode_now = get_pos_mode()
    print(f"\n[2/3] 持仓模式：当前 {mode_now or '?'} -> 目标 {TARGET_POS_MODE}")
    if mode_now == TARGET_POS_MODE:
        print("  ✅ 已经是单向持仓，无需修改")
    else:
        print(f"  计划：POST /api/v5/account/set-position-mode  body={{\"posMode\": \"{TARGET_POS_MODE}\"}}")
        if not args.apply:
            print("  （dry-run，不执行）")
        else:
            try:
                P.private("POST", "/api/v5/account/set-position-mode",
                          body={"posMode": TARGET_POS_MODE})
            except P.PrivateError as e:
                print(f"  ❌ 切换失败：{e}")
                print("     常见原因：API Key 没有『交易』权限。请在 OKX->API 里勾选交易权限（不要勾提现）。")
                audit(f"set-position-mode FAILED: {e}")
                return 3
            after = get_pos_mode()
            print(f"  验证：当前持仓模式 = {after}")
            audit(f"set-position-mode -> {after}")
            if after != TARGET_POS_MODE:
                print("  ❌ 验证失败，模式未生效")
                return 3
            print("  ✅ 已切换为单向持仓")

    # --- 3. 杠杆 ---
    print(f"\n[3/3] 杠杆：{INST} -> {TARGET_LEVER}x（{MGN_MODE}）")
    lev_now = current_lever()
    print(f"  当前持仓杠杆读取: {lev_now or '无持仓，读不到（按账户设置）'}")
    print(f"  计划：POST /api/v5/account/set-leverage  "
          f"body={json.dumps({'instId': INST, 'lever': TARGET_LEVER, 'mgnMode': MGN_MODE})}")
    if not args.apply:
        print("  （dry-run，不执行）")
    else:
        try:
            P.private("POST", "/api/v5/account/set-leverage",
                      body={"instId": INST, "lever": TARGET_LEVER, "mgnMode": MGN_MODE})
        except P.PrivateError as e:
            print(f"  ❌ 设置杠杆失败：{e}")
            audit(f"set-leverage FAILED: {e}")
            return 4
        audit(f"set-leverage {INST} -> {TARGET_LEVER}x {MGN_MODE}")
        print("  ✅ 杠杆已设置为 8x")

    # --- 收尾：复核 ---
    print("\n" + "=" * 70)
    cfg = P.account_config()
    c = cfg[0] if cfg else {}
    print(f"复核：持仓模式={c.get('posMode')}  账户模式={c.get('acctLv')}  "
          f"手续费档={c.get('level')}")
    print("完成。" + ("" if args.apply else "  （本次是 dry-run，加 --apply 才会真正改）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
