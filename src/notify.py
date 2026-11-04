# -*- coding: utf-8 -*-
"""提醒：Windows 桌面气泡通知 + 蜂鸣音。信号模式下这是你唯一的"实时"入口之一。"""
from __future__ import annotations

import subprocess
import sys

from .util import log

CREATE_NO_WINDOW = 0x08000000
DETACHED = 0x00000008


def _ps_quote(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def toast(title: str, message: str, beep: bool = True) -> bool:
    """弹出 Windows 气泡通知（不阻塞主循环）。失败只记日志，绝不影响交易逻辑。"""
    if sys.platform != "win32":
        return False
    script = (
        "Add-Type -AssemblyName System.Windows.Forms;"
        "Add-Type -AssemblyName System.Drawing;"
        "$n = New-Object System.Windows.Forms.NotifyIcon;"
        "$n.Icon = [System.Drawing.SystemIcons]::Warning;"
        "$n.Visible = $true;"
        "$n.BalloonTipTitle = TITLE;"
        "$n.BalloonTipText = BODY;"
        "$n.ShowBalloonTip(20000);"
        + ("[console]::beep(880,250);[console]::beep(1180,250);" if beep else "")
        + "Start-Sleep -Seconds 21;"
        "$n.Dispose();"
    )
    script = script.replace("TITLE", _ps_quote(title)).replace("BODY", _ps_quote(message))
    try:
        subprocess.Popen(["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", script],
                         creationflags=CREATE_NO_WINDOW | DETACHED,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception as e:  # noqa: BLE001
        log(f"[通知失败] {e}", "WARN")
        return False


def signal_text(sig: dict) -> str:
    side = "做多" if sig["side"] == "long" else "做空"
    return (f"{sig['instId']} {side} {sig['playbook']} [{sig['grade']}级 {sig['score']}分]\n"
            f"入场 {sig['entry']}  止损 {sig['stop']} ({sig['stop_pct']}%)  "
            f"TP1 {sig['tp1']}  TP2 {sig['tp2']}\n"
            f"手数 {sig['qty']} 张 = {sig['notional_usd']}u ({sig['leverage_used']}x)  "
            f"真实风险 {sig['effective_risk_usd']}u\n"
            f"有效期至 {sig['expires_at_utc']}（UTC）")
