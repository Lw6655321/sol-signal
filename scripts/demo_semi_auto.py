#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""独立 DEMO 半自动入口（阶段 1：客户端 / 只读 preflight / 纯 mock 卡片流程）。

阶段边界（Codex《DEMO_EXECUTION_DESIGN.md》）：
- 默认 `--explain`：不读取凭据、**零网络请求**。
- `--preflight`：需要完整三项 DEMO 凭据；只读 GET；输出脱敏 preflight 摘要。
- `--mock-run`：纯内存 mock 卡片 + 逐单确认/异常退出演练；**不联网、不下单**。
- `--demo-run`：**未放行**（`DEMO_ENTRY_NOT_RELEASED`）；需单独路由接线复核 + 用户逐单卡片确认。
- 不使用旧 `--live`；不设置 `OKX_TRADING_ENABLED` / `OKX_SIMULATED`；不解除 `ADAPTER_UNVERIFIED`。
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.demo_transport import (  # noqa: E402
    DemoCredentialError,
    DemoReadOnlyClient,
    DemoTransportError,
    MockDemoTransport,
    demo_preflight,
    load_demo_credentials,
)

LOG_ROOT = ROOT / "logs" / "demo-stage1"
DEMO_SIGNAL = {
    "id": "DEMO-S1",
    "side": "short",
    "entry": "100.00",
    "stop": "101.60",
    "tp1": "97.60",
    "qty": "0.01",
}
MOCK_SCENARIOS = ("protected", "timeout", "cancel-race", "protection-fail", "exception")


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _audit_dir(log_root=None) -> Path:
    root = Path(log_root).resolve() if log_root else LOG_ROOT.resolve()
    try:
        root.relative_to((ROOT / "logs").resolve())
    except ValueError as exc:
        raise SystemExit("AUDIT_ROOT_NOT_ALLOWED") from exc
    run = root / f"run-{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run.mkdir(parents=True, exist_ok=False)
    return run


def _write(run: Path, name: str, payload: dict) -> Path:
    path = run / name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_preflight(client_factory=None, log_root=None) -> int:
    try:
        credentials = load_demo_credentials()
    except DemoCredentialError as exc:
        print(f"DEMO_PREFLIGHT_REFUSED {exc.code}")
        return 2
    client = client_factory(credentials) if client_factory else DemoReadOnlyClient(credentials)
    try:
        summary = demo_preflight(client)
    except DemoTransportError as exc:
        print(f"DEMO_PREFLIGHT_REFUSED {exc.code}")
        return 2
    run = _audit_dir(log_root)
    _write(run, "preflight.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"report: {run / 'preflight.json'}")
    return 0


def run_mock(scenario: str, confirm: str, log_root=None) -> int:
    if scenario not in MOCK_SCENARIOS:
        print(f"MOCK_REFUSED UNKNOWN_SCENARIO:{scenario}")
        return 2
    transport = MockDemoTransport(scenario=scenario)
    card = transport.render_card(DEMO_SIGNAL)
    print("=== 离线模拟确认卡（DEMO-MOCK，不联网/不下单）===")
    for key, value in card.items():
        print(f"  {key}: {value}")
    expected = f"CONFIRM {DEMO_SIGNAL['id']}"
    confirmed = str(confirm or "").strip() == expected
    if not confirmed:
        print(f"输入未匹配 {expected}：已拒绝，未提交任何模拟委托。")
    result = transport.execute(card, confirmed)
    run = _audit_dir(log_root)
    payload = {
        "purpose": "demo_mock_stage1",
        "scenario": scenario,
        "confirmed": confirmed,
        "result": result,
        "events": transport.events,
        "orders_sent": False,
        "trading_state_changed": False,
        "network_calls": 0,
        "reported_at_utc": _utc_now(),
    }
    _write(run, "mock-run.json", payload)
    print(f"mock result: status={result['status']} code={result['code']} "
          f"steps={result['steps']} halt={result['halt']}")
    return 0 if result["status"] in {"PROTECTED", "CANCELED", "REJECTED", "CLOSED"} else 2


def explain() -> int:
    print("=== 独立 DEMO 半自动入口（阶段 1，未放行真实模拟开仓）===")
    print("- 默认/--explain：不读取凭据、不联网。")
    print("- --preflight：需 OKX_DEMO_* 三项，只读 GET，输出脱敏摘要。")
    print("- --mock-run：纯内存 mock 卡片 + 逐单确认/异常退出演练。")
    print("- --demo-run：DEMO_ENTRY_NOT_RELEASED（需路由接线复核 + 用户逐单确认）。")
    print("边界：只用 DEMO 凭据；固定模拟头 1；无实盘回退；不设置交易开关；不解除 ADAPTER_UNVERIFIED。")
    print("该入口阶段 1 不发送任何订单，也不改动原项目执行代码。")
    return 0


def main(argv=None, client_factory=None, log_root=None) -> int:
    parser = argparse.ArgumentParser(description="独立 DEMO 入口（阶段 1：只读/纯 mock）")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--explain", action="store_true", help="仅说明（默认，零请求）")
    group.add_argument("--preflight", action="store_true", help="只读 GET preflight")
    group.add_argument("--mock-run", action="store_true", help="纯内存 mock 卡片流程")
    group.add_argument("--demo-run", action="store_true", help="未放行：预留到接线验收后")
    parser.add_argument("--scenario", default="protected",
                        choices=list(MOCK_SCENARIOS), help="mock 场景")
    parser.add_argument("--confirm", default="", help="逐单确认串：CONFIRM <signal_id>")
    parser.add_argument("--log-root", default=None, help="审计目录（须位于 <repo>/logs 下）")
    args = parser.parse_args(argv)

    if args.demo_run:
        print("DEMO_ENTRY_NOT_RELEASED: 真实模拟开仓未放行；需"
              " 1) 独立路由接线验收 2) 用户逐单具体卡片确认。")
        return 3
    if args.preflight:
        return run_preflight(client_factory=client_factory, log_root=args.log_root or log_root)
    if args.mock_run:
        return run_mock(args.scenario, args.confirm, log_root=args.log_root or log_root)
    return explain()


if __name__ == "__main__":
    sys.exit(main())
