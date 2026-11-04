"""Read-only risk evidence diagnosis. No trading switch or order operations.

Default invocation prints the plan. --read-only explicitly reads existing OKX
account evidence and writes an allowlisted local report, never a trading state.
The report cannot release a HALT or enable an unverified adapter.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.live_state import live_state_provider, read_only_evidence
from src.util import load_config


def collect_report(config, *, provider=live_state_provider):
    report = {
        "purpose": "read_only_risk_diagnosis",
        "reported_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "orders_sent": 0,
        "trading_state_changed": False,
        "release_ready": False,
        "blocking_reasons": [],
    }
    try:
        state = provider(config)
        if not isinstance(state, Mapping):
            raise ValueError("state must be a mapping")
        reasons = state.get("refusal_reasons", [])
        if not isinstance(reasons, (list, tuple)) or any(
                not isinstance(code, str) or not code.isascii()
                or not code.replace("_", "").isalnum() for code in reasons):
            raise ValueError("invalid refusal codes")
        report["blocking_reasons"] = list(reasons)
        if state.get("live_verified") is not True:
            report["blocking_reasons"].append("LIVE_STATE_UNVERIFIED")
        # A data adapter passing by itself is never a live release decision.
        weekly = all(key in state and state[key] is not None
                     for key in ("week_start_equity", "realized_pnl_week"))
        if not weekly:
            report["blocking_reasons"].append("WEEKLY_EVIDENCE_MISSING")
        report["account"] = {
            "equity_estimate": state.get("equity_estimate"),
            "position_count": len(state.get("open_positions", [])),
            "pending_count": len(state.get("pending_orders", [])),
            "protection_count": len(state.get("protection_orders", [])),
        }
        report["risk"] = {key: state.get(key) for key in (
            "day_start_equity", "realized_pnl_today", "entries_today",
            "week_start_equity", "realized_pnl_week", "macro_blackout", "halted")}
        report["evidence_status"] = {
            "live_verified": state.get("live_verified") is True,
            "observed_at_ms": state.get("observed_at_ms"),
            "data_fresh": state.get("data_fresh") is True,
            "weekly_fields_present": weekly,
        }
        evidence = state.get("evidence", {})
        report["collection_errors"] = {}
        for field in ("fills_errors", "funding_errors"):
            codes = evidence.get(field, []) if isinstance(evidence, Mapping) else []
            if not isinstance(codes, (list, tuple)) or any(
                    not isinstance(code, str) or not code.isascii()
                    or not code.replace("_", "").isalnum() for code in codes):
                codes = ["INVALID_DIAGNOSTIC_CODES"]
            report["collection_errors"][field] = list(codes)
        # The diagnostic deliberately has no way to mark a release approved.
        report["blocking_reasons"].append("RELEASE_REVIEW_REQUIRED")
    except Exception:
        # Raw exception text can contain API data. Keep only a stable code.
        report["blocking_reasons"] = ["EVIDENCE_UNAVAILABLE", "RELEASE_REVIEW_REQUIRED"]
    report["blocking_reasons"] = list(dict.fromkeys(report["blocking_reasons"]))
    return report


def save_report(report, root):
    directory = Path(root) / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-")
                              + uuid.uuid4().hex)
    directory.mkdir(parents=True, exist_ok=False)
    path = directory / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-only", action="store_true", help="collect read-only OKX evidence")
    parser.add_argument("--context-root", type=Path,
                        help="read existing state/journal from this project; never write there")
    parser.add_argument("--log-root", type=Path, default=ROOT / "logs" / "live-risk-audit")
    args = parser.parse_args(argv)
    if not args.read_only:
        print("Read-only integration diagnostic. No account access in this mode.")
        print("Use --read-only to collect evidence; this tool never sends orders or enables trading.")
        return 0
    provider = live_state_provider
    if args.context_root is not None:
        context_root = args.context_root.resolve(strict=True)
        if not context_root.is_dir():
            parser.error("context-root must be a project directory")
        args.log_root = args.log_root.resolve()
        if args.log_root == context_root or args.log_root.is_relative_to(context_root):
            parser.error("log-root must be outside the read-only context-root")
        provider = lambda cfg: live_state_provider(
            cfg, evidence=read_only_evidence(cfg, context_root=context_root))
    report = collect_report(load_config(), provider=provider)
    path = save_report(report, args.log_root)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("Report:", path)
    return 2  # A diagnostic is not a release approval, even if all data is present.


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
