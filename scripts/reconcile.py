# -*- coding: utf-8 -*-
"""第一阶段：只读对账。

作用：用 OKX 私有接口读取账户真实状态，和你本地台账比对，找出差异。
不需要交易权限，只需要「读取」权限。

运行：
  1) 先设置环境变量（设置后必须重开命令行窗口）：
       setx OKX_API_KEY "..."
       setx OKX_API_SECRET "..."
       setx OKX_API_PASSPHRASE "..."
  2) python scripts/reconcile.py

它只读，不下单，不改任何东西。
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import okx_private as P                      # noqa: E402
from src import risk                                  # noqa: E402
from src.util import load_config, log                 # noqa: E402

INST = "SOL-USDT-SWAP"


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def show_status() -> bool:
    st = P.status()
    print("=" * 74)
    print("API 配置状态")
    print("=" * 74)
    print(f"  凭据已配置 : {'是' if st['configured'] else '否'}")
    print(f"  模拟盘模式 : {'是' if st['simulated'] else '否（实盘）'}")
    print(f"  交易已开启 : {'是' if st['trading_enabled'] else '否（当前只能读，安全）'}")
    print(f"  白名单交易对: {', '.join(st['allowed_inst'])}")
    if not st["configured"]:
        print()
        print("  ⚠️ 还没配置凭据。请设置这三个环境变量后重开命令行窗口：")
        print("     setx OKX_API_KEY \"你的key\"")
        print("     setx OKX_API_SECRET \"你的secret\"")
        print("     setx OKX_API_PASSPHRASE \"你的passphrase\"")
        print()
        print("  只想先跑模拟盘就再加一个： setx OKX_SIMULATED 1")
        return False
    return True


def main() -> int:
    if not show_status():
        return 1

    print()
    print("=" * 74)
    print("① 账户配置")
    print("=" * 74)
    try:
        cfg = P.account_config()
        if cfg:
            c = cfg[0]
            pos_mode = {"net_mode": "单向持仓", "long_short_mode": "双向持仓"}.get(
                c.get("posMode"), c.get("posMode"))
            acct_lv = c.get("acctLv", "")
            print(f"  持仓模式 : {pos_mode}")
            print(f"  账户模式 : {acct_lv}")
            print(f"  手续费档 : Lv{c.get('level', '?')}  "
                  f"taker {_f(c.get('taker')) * 100:.4f}%  maker {_f(c.get('maker')) * 100:.4f}%")
            if pos_mode != "单向持仓":
                print("  ⚠️ 警告：系统按「单向持仓」设计，你的账户是双向持仓，下单参数会不匹配！")
            if _f(c.get("taker")) > 0.0006:
                print("  ⚠️ 你的 taker 费率高于 config.json 里的 0.05%，请更新配置，否则手续费估算会偏低")
    except P.PrivateError as e:
        print(f"  ❌ 失败：{e}")
        return 2

    print()
    print("=" * 74)
    print("② 账户余额")
    print("=" * 74)
    try:
        bal = P.balance("USDT")
        if bal:
            b = bal[0]
            details = b.get("details") or [{}]
            d0 = details[0] if details else {}
            print(f"  总权益   : {_f(b.get('totalEq')):.4f} USDT")
            print(f"  可用保证金: {_f(b.get('availBal') or d0.get('availBal')):.4f} USDT")
            print(f"  已用保证金: {_f(d0.get('frozenBal')):.4f} USDT")
            print(f"  未实现盈亏: {_f(b.get('upl')):+.4f} USDT")
            print(f"  账户更新时间: {b.get('uTime', '')}")
    except P.PrivateError as e:
        print(f"  ❌ 失败：{e}")

    print()
    print("=" * 74)
    print("③ 当前持仓（OKX 实盘 vs 本地台账）")
    print("=" * 74)
    try:
        pos = [p for p in P.positions(INST) if _f(p.get("pos")) != 0]
        local = risk.compute(load_config())
        if not pos:
            print("  OKX 实际持仓：无")
        for p in pos:
            print(f"  OKX: {p.get('posSide')} {p.get('pos')} 张  开仓均价 {p.get('avgPx')}  "
                  f"未实现 {_f(p.get('upl')):+.3f} u  杠杆 {p.get('lever')}x  "
                  f"{'全仓' if p.get('mgnMode') == 'cross' else '逐仓'}")
        lp = local.get("open_positions") or []
        print(f"  本地台账持仓：{len(lp)} 个" + (
            "（" + ", ".join(f"{x['signal_id']} {x['side']} {x['qty']}张" for x in lp) + "）"
            if lp else ""))
        if bool(pos) != bool(lp):
            print("  ⚠️ **不一致**：OKX 和本地台账的持仓状态不匹配，需要人工确认")
        else:
            print("  ✅ 持仓状态一致")
    except P.PrivateError as e:
        print(f"  ❌ 失败：{e}")

    print()
    print("=" * 74)
    print("④ 挂单与止损单（验证止损到底挂了没）")
    print("=" * 74)
    try:
        pend = P.pending_orders(INST)
        print(f"  普通挂单：{len(pend)} 笔")
        for o in pend:
            print(f"    {o.get('side')} {o.get('sz')} 张 @ {o.get('px')}  状态 {o.get('state')}")

        # 止损有三种载体，必须全查，否则会误报"裸单"（真实踩过的坑）：
        #   ① 仓位级 OCO —— 挂在持仓的 closeOrderAlgo 字段里
        #   ② 策略委托 ordType=conditional / oco / trigger
        found = []
        for p in P.positions(INST):
            if float(p.get("pos") or 0) == 0:
                continue
            for a in (p.get("closeOrderAlgo") or []):
                found.append({
                    "载体": "仓位级OCO", "side": "close",
                    "sz": f"{p.get('pos')}x{a.get('closeFraction', '?')}",
                    "止损": a.get("slTriggerPx"), "止盈": a.get("tpTriggerPx"),
                    "状态": "live",
                })
        for t in ("conditional", "oco", "trigger"):
            try:
                for o in P.algo_orders(t):
                    found.append({
                        "载体": f"策略委托/{t}", "side": o.get("side"), "sz": o.get("sz"),
                        "止损": o.get("slTriggerPx"), "止盈": o.get("tpTriggerPx"),
                        "状态": o.get("state"),
                    })
            except P.PrivateError:
                pass

        print(f"  止损/止盈单：{len(found)} 笔")
        for x in found:
            print(f"    [{x['载体']}] 方向{x['side']} {x['sz']}  "
                  f"止损 {x['止损']}  止盈 {x['止盈']}  状态 {x['状态']}")
        if not found:
            print("  ⚠️ **确实没有任何止损/止盈** —— 如果你有持仓，这就是裸单！")
        else:
            print("  ✅ 持仓已有止损保护")
    except P.PrivateError as e:
        print(f"  ❌ 失败：{e}")

    print()
    print("=" * 74)
    print("⑤ 最近成交（真实成交价 + 手续费）")
    print("=" * 74)
    try:
        fl = P.fills(INST, 10)
        if not fl:
            print("  最近没有成交")
        for x in fl:
            print(f"  {x.get('fillTime')}  {x.get('side')} {x.get('fillSz')} 张 @ {x.get('fillPx')}  "
                  f"手续费 {_f(x.get('fee')):+.5f}  {x.get('feeCcy')}  订单 {x.get('ordId')}")
    except P.PrivateError as e:
        print(f"  ❌ 失败：{e}")

    print()
    print("=" * 74)
    print("第一阶段验收标准：①~⑤ 全部正常读取，且 ③ 持仓状态与 OKX 一致。")
    print("通过后再进入第二阶段（纸面执行，不发真单）。")
    print("=" * 74)
    return 0


if __name__ == "__main__":
    sys.exit(main())
