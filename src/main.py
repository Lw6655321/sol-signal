# -*- coding: utf-8 -*-
"""主程序。

用法：
  python -m src.main --once                    单次扫描（推荐先用这个看当前状态）
  python -m src.main                           持续运行（默认每 15 秒刷新快、每 60 秒刷新慢数据）
  python -m src.main --interval 10 --no-toast  自定义间隔、关闭弹窗
  python -m src.main --record SIGNAL_ID --exit 120.55 --reason tp1   回填成交结果
  python -m src.main --stats                   查看已回填成交的统计
  python -m src.main --halt "手滑亏太多，停两天"    熔断（写入 state/HALT）
  python -m src.main --resume                  解除熔断
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timezone

from . import engine, notify, risk
from .collect import collect, trimmed
from .dashboard import serve
from .util import (STATE_DIR, load_config, log, ms_to_iso, now_ms, read_json,
                   server_now_ms, sync_server_time, write_json)

LATEST = STATE_DIR / "latest.json"
SIGNALS_JSON = STATE_DIR / "signals.json"
EMIT_STATE = STATE_DIR / "last_emit.json"


# --------------------------------------------------------------------------- 输出


def _r(x, n=2, dash="?"):
    if x is None:
        return dash
    try:
        return f"{float(x):.{n}f}"
    except (TypeError, ValueError):
        return str(x)


def print_report(snap: dict, ev: dict, risk_st: dict) -> None:
    f, facts = snap["features"], snap["facts"]
    tf = f.get("tf") or {}
    def st(name):
        return ((tf.get(name) or {}).get("structure") or {}).get("structure", "?")
    h = snap["health"]
    print("-" * 96)
    print(f"[{snap['generated_at_utc']}] {snap['inst']}  {facts.get('price')}  "
          f"24h {_r(facts.get('chg24h_pct'))}%  ATR15 {_r(facts.get('atr_pct_15m'), 3)}%  "
          f"结构 15m/1H/4H = {st('15m')}/{st('1H')}/{st('4H')}")
    print(f"  数据源: {h['ok']} 正常 / {h['failed']} 失败 / {h['stale']} 过期"
          f"{'  ✅可用于决策' if h['usable'] else '  ❌ 关键数据不可用，不发信号'}"
          + (f"  ⚠️ 辅助源降级: {','.join(h['advisory_missing'] + h['advisory_stale'])}"
             if h.get("degraded") else "")
          + f"   复用缓存 {snap.get('reused_sources', 0)} 项")
    tk = f.get("taker_5m") or {}
    t3 = tk.get("3bars") or {}
    oi = f.get("oi") or {}
    oi5 = (oi.get("5m") or {})
    liq = (f.get("liquidation") or {}).get("w15") or {}
    book = f.get("book") or {}
    if t3:
        print(f"  主动流: 15m 买占比 {_r(t3.get('buy_ratio'), 3)} 净额 {_r(t3.get('net_usd'), 0)}u"
              f"   持仓: 现 {_r((facts.get('oi_usd') or 0) / 1e6, 1)}M USD  "
              f"15m变化 {_r(oi5.get('3bars_pct'), 3)}%  1h变化 {_r(oi5.get('12bars_pct'), 3)}%")
    if liq.get("total_usd") is not None:
        print(f"  强平: 15m {_r(liq['total_usd'], 0)}u  多头占比 {_r(liq.get('long_share'))}"
              f"   资金费率 {_r(facts.get('funding_pct'), 5)}%  下次结算 {facts.get('next_funding_utc')}")
    if book.get("ok"):
        print(f"  盘口: 价差 {_r(book['spread_bps'])}bps  0.5%档失衡 "
              f"{_r(book['bands']['0.5%']['imbalance'], 3)}  100u滑点 "
              f"{_r(facts.get('slippage_buy_100u_bps'))}bps")
    mac = f.get("macro") or {}
    print(f"  宏观: 风险状态 {mac.get('risk_regime')} (分{mac.get('risk_score')}) VIX {_r(mac.get('vix'))}"
          f"  封锁窗口 {'是' if mac.get('blackout') else '否'}"
          + (f"  下一个高影响: {mac['upcoming_high_impact'][0]['utc']} {mac['upcoming_high_impact'][0]['title'][:24]}"
             if mac.get("upcoming_high_impact") else ""))
    s = f.get("sentiment") or {}
    oc = f.get("onchain") or {}
    cg = s.get("coingecko") or {}
    print(f"  情绪: FNG {facts.get('fng')} {facts.get('fng_label')}   全局市值24h "
          f"{_r(cg.get('mcap_change24h_pct'))}%   BTC占比 {_r(cg.get('btc_dominance'), 1)}%"
          f"   链上: TPS {_r((oc.get('solana') or {}).get('tps'), 0)}  "
          f"SolanaTVL7d {_r((oc.get('solana_tvl') or {}).get('chg7d_pct'))}%  "
          f"稳定币7d {_r((oc.get('stablecoins') or {}).get('chg7d_usd', 0) / 1e9)}B")
    g = ev["gates"]
    print(f"  风控: 净值 {risk_st['equity_estimate']:.2f}u  今日已实现 {risk_st['realized_pnl_today']:+.2f}u "
          f"(上限 -{risk_st['daily_loss_limit_usd']:.2f}u)  开仓次数 {risk_st.get('entries_today',0)}/"
          f"{risk_st.get('_max_entries')}  连亏 {risk_st.get('consecutive_losses')}"
          + (f"  暂停至 {ms_to_iso(risk_st['paused_until_ms'])}" if risk_st.get("paused") else ""))
    if not g["pass"]:
        print("  闸门: ❌ 未通过 -> " + " | ".join(g["reasons"]))
    elif g.get("warnings"):
        print("  闸门: ✅ 通过（提示：" + "；".join(g["warnings"]) + "）")
    else:
        print("  闸门: ✅ 通过")

    if ev["candidates"]:
        print("  候选评分:")
        for c in ev["candidates"][:6]:
            miss = [x["name"] for x in c["conds"] if not x["ok"]]
            if c.get("passed"):
                tag = "触发"
            elif not c.get("required_ok", True):
                tag = "前提缺失"
            else:
                tag = "未达"
            extra = ""
            if not c.get("required_ok", True):
                extra = f"  ⚠前提: {', '.join(c.get('missing_required') or [])}"
            print(f"    [{tag}] {c['kind']:<14} {c['side']:<5} {c['score']:5.1f}分 (需{c['min_score']}) "
                  f"净RR {c['net_rr']} (毛{c['rr']} 费{c.get('cost_r', 0) * 100:.1f}%R)  "
                  f"缺: {', '.join(miss[:3]) if miss else '无'}{extra}")
    if ev.get("rejected"):
        for r in ev["rejected"]:
            print(f"    [否决] {r['kind']} {r['side']}: {r['reason']}")

    for sig in ev["signals"]:
        side = "做多" if sig["side"] == "long" else "做空"
        print("  " + "=" * 92)
        print(f"  ✅ 信号 {sig['id']}  {sig['instId']} {side}  {sig['playbook']}  "
              f"[{sig['grade']}级 / {sig['score']}分]")
        print(f"     入场 {sig['entry']:.3f}（{sig['entry_type']}）  止损 {sig['stop']} ({sig['stop_pct']}%)  "
              f"TP1 {sig['tp1']} (净RR {sig['net_rr1']})  TP2 {sig['tp2']}")
        print(f"     手数 {sig['qty']} 张 ≈ {sig['notional_usd']}u  实际杠杆 {sig['leverage_used']}x  "
              f"计划风险 {sig['planned_risk_usd']}u  含费滑真实风险 {sig['effective_risk_usd']}u")
        print(f"     手续费占风险 {sig['fee_share_of_risk'] * 100:.1f}%  "
              f"（挂单 {sig['fees_maker_usd']}u / 吃单 {sig['fees_taker_usd']}u）  "
              f"建议杠杆设置 {sig['leverage_setting_needed']}x"
              f"（当前设置 {load_config()['account']['leverage_setting']}x）")
        print(f"     有效期至 {sig['expires_at_utc']} UTC（超时作废，需重新评估）  "
              f"最长持有 {sig.get('max_hold_hours')} 小时")
        print(f"     失效条件: " + "；".join(sig["invalidation"][:4]))
        print("  " + "=" * 92)


# --------------------------------------------------------------------------- 主循环


def _dedup(cfg: dict, signals: list[dict]) -> list[dict]:
    st = read_json(EMIT_STATE, {}) or {}
    out = []
    for s in signals:
        key = f"{s['kind']}|{s['side']}"
        last = st.get(key)
        cool = max(30, int(s.get("expiry_min", 30))) * 60_000
        if last and (server_now_ms() - last) < cool:
            log(f"抑制重复信号 {key}（冷却中）")
            continue
        st[key] = server_now_ms()
        out.append(s)
    write_json(EMIT_STATE, st)
    return out


def scan(cfg: dict, prev: dict | None = None, toast: bool = True, persist: bool = True) -> dict:
    snap = collect(cfg, prev)
    risk_st = risk.compute(cfg)
    snap["equity_estimate"] = risk_st["equity_estimate"]
    ev = engine.evaluate(snap, cfg, risk_st)

    new_sigs = _dedup(cfg, ev["signals"])
    # 注意：不要清空 ev["rejected"]，否则"评分达标但被仓位规则否决"的原因会被吞掉，
    # 用户只会看到 [触发] 却拿不到信号，无从判断原因。
    for sig in new_sigs:
        risk.register_signal(cfg, sig)
        if toast:
            notify.toast(f"SOL 信号 {sig['grade']}级 {sig['score']}分", notify.signal_text(sig))
        print("\a", end="")
    # 记录被否决的信号（评分达标但仓位算不出来）
    hist = read_json(SIGNALS_JSON, []) or []
    hist.extend(new_sigs)
    write_json(SIGNALS_JSON, hist[-200:])

    payload = {
        "updated_ms": now_ms(),
        "updated_utc": snap["generated_at_utc"],
        "snapshot": trimmed(snap),
        "eval": ev,
        "risk": risk_st,
        "stats": risk.stats(),
        "recent_signals": hist[-20:],
    }
    if persist:
        write_json(LATEST, payload)
    print_report(snap, ev, risk_st)
    return payload


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="SOL 永续合约实时多因子信号系统")
    ap.add_argument("--once", action="store_true", help="只扫描一次然后退出")
    ap.add_argument("--interval", type=float, default=15.0, help="快速数据刷新间隔（秒）")
    ap.add_argument("--no-toast", action="store_true", help="关闭桌面弹窗")
    ap.add_argument("--no-dashboard", action="store_true", help="不启动网页仪表盘")
    ap.add_argument("--port", type=int, default=None, help="仪表盘端口（默认取配置）")
    ap.add_argument("--record", metavar="SIGNAL_ID", help="回填某个信号的成交结果")
    ap.add_argument("--exit", dest="exit_px", type=float, help="平仓价")
    ap.add_argument("--entry", dest="entry_px", type=float, help="开仓价（可选，默认取信号值）")
    ap.add_argument("--qty", type=float, help="实际成交手数（可选）")
    ap.add_argument("--side", choices=["long", "short"], help="方向（可选）")
    ap.add_argument("--reason", default="manual", help="平仓原因")
    ap.add_argument("--notes", default="", help="备注")
    ap.add_argument("--closed-at", help="平仓时间(UTC)，如 2026-09-29 或 2026-09-29T04:30Z。"
                                       "跨日回填必须带上，否则昨天的盈亏会被算进今天")
    ap.add_argument("--open", dest="open_side", choices=["long", "short"],
                    help="登记一笔手动开仓（按结构化方案自己下单时用）")
    ap.add_argument("--open-entry", type=float, help="手动开仓的入场价")
    ap.add_argument("--open-qty", type=float, help="手动开仓的手数（张）")
    ap.add_argument("--open-stop", type=float, help="手动开仓的止损价")
    ap.add_argument("--open-id", help="给这笔手动持仓起个编号（便于之后 --record 回填）")
    ap.add_argument("--open-note", default="", help="备注")
    ap.add_argument("--stats", action="store_true", help="查看成交统计")
    ap.add_argument("--halt", metavar="REASON", help="开启熔断停止交易")
    ap.add_argument("--resume", action="store_true", help="解除熔断")
    args = ap.parse_args(argv)

    cfg = load_config()

    if args.halt:
        risk.halt(args.halt)
        return 0
    if args.resume:
        risk.resume()
        return 0
    if args.stats:
        print(json.dumps(risk.stats(), ensure_ascii=False, indent=2))
        return 0
    if args.open_side:
        if None in (args.open_entry, args.open_qty, args.open_stop):
            print("--open 需同时提供 --open-entry --open-qty --open-stop", file=sys.stderr)
            return 2
        sid, st = risk.register_manual_position(cfg, args.open_side, args.open_entry,
                                                args.open_qty, args.open_stop,
                                                args.open_note, args.open_id)
        print(json.dumps({"signal_id": sid, "equity_estimate": st["equity_estimate"],
                          "entries_today": st["entries_today"],
                          "open_positions": st["open_positions"]},
                         ensure_ascii=False, indent=2, default=str))
        return 0

    if args.record:
        if args.exit_px is None:
            print("必须提供 --exit 平仓价", file=sys.stderr)
            return 2
        closed_ms = None
        if args.closed_at:
            try:
                dt = datetime.fromisoformat(args.closed_at.strip().replace("Z", "+00:00"))
            except ValueError:
                print(f"无法解析 --closed-at: {args.closed_at}", file=sys.stderr)
                return 2
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            closed_ms = int(dt.timestamp() * 1000)
        st = risk.record_trade(cfg, args.record, args.exit_px, args.reason, args.notes,
                               args.entry_px, args.qty, args.side, closed_at_ms=closed_ms)
        print(json.dumps(st, ensure_ascii=False, indent=2, default=str))
        return 0

    sync = sync_server_time()
    log(f"启动：服务器时间 {sync['server_utc']}，本机漂移 {sync['offset_ms']}ms "
        f"({'正常' if sync['drift_ok'] else '异常'})")
    if not args.no_dashboard and cfg["notify"]["dashboard"]:
        serve(args.port or int(cfg["notify"]["dashboard_port"]))

    if args.once:
        scan(cfg, None, toast=not args.no_toast)
        return 0

    prev = None
    log(f"进入循环：每 {args.interval}s 刷新一次（按各数据源自己的周期增量拉取）")
    while True:
        try:
            prev = collect(cfg, prev)
            risk_st = risk.compute(cfg)
            prev["equity_estimate"] = risk_st["equity_estimate"]
            ev = engine.evaluate(prev, cfg, risk_st)
            new_sigs = _dedup(cfg, ev["signals"])
            for sig in new_sigs:
                risk.register_signal(cfg, sig)
                if not args.no_toast:
                    notify.toast(f"SOL 信号 {sig['grade']}级 {sig['score']}分", notify.signal_text(sig))
                print("\a", end="")
            hist = read_json(SIGNALS_JSON, []) or []
            hist.extend(new_sigs)
            write_json(SIGNALS_JSON, hist[-200:])
            write_json(LATEST, {
                "updated_ms": now_ms(), "updated_utc": prev["generated_at_utc"],
                "snapshot": trimmed(prev), "eval": ev, "risk": risk_st,
                "stats": risk.stats(), "recent_signals": hist[-20:],
            })
            print_report(prev, ev, risk_st)
        except KeyboardInterrupt:
            log("收到中断，退出")
            return 0
        except Exception:  # noqa: BLE001
            log("[主循环异常]\n" + traceback.format_exc(), "ERROR")
        time.sleep(max(2.0, args.interval))


if __name__ == "__main__":
    sys.exit(main())
