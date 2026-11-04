"""Offline controller acceptance; fakes cannot read keys or send network IO."""
from __future__ import annotations

import copy
import json
import socket
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.demo_auto import DEMO_ACCOUNT_KEY, DemoAutoController, DemoAutoError


ROOT = Path(__file__).resolve().parents[1]
NOW = 1800000000000


class FakeRisk:
    def __init__(self):
        self.calls = []
        self.state = {"equity_estimate": "117.72", "day_start_equity": "117.72",
                      "realized_pnl_today": "0", "entries_today": 0, "open_positions": [],
                      "pending_orders": [], "data_fresh": True, "macro_blackout": False,
                      "halted": False, "paused_until_ms": 0, "slippage_bps": "0",
                      "week_start_equity": "117.72", "realized_pnl_week": "0",
                      "demo_verified": True, "observed_at_ms": NOW, "refusal_reasons": []}

    def snapshot(self, market_snapshot):
        self.calls.append(copy.deepcopy(market_snapshot))
        return copy.deepcopy(self.state)


class FakeRouter:
    mode = "demo"
    account_key = DEMO_ACCOUNT_KEY

    def __init__(self):
        self.calls = []
        self.initial = "PENDING"
        self.current = "PENDING"
        self.exit = "PROTECTED"
        self.raise_execute = False
        self.result_code = "OK"

    def result(self, intent, status):
        filled = Decimal("1") if status in {"PROTECTED", "CLOSED"} else Decimal("0")
        protected = filled if status == "PROTECTED" else Decimal("0")
        return SimpleNamespace(status=status, code=self.result_code,
                               signal_id=intent.signal_id, filled_qty=filled, protected_qty=protected)

    def execute(self, intent):
        self.calls.append(("execute", intent))
        if self.raise_execute:
            raise RuntimeError("DO_NOT_LOG_SECRET")
        return self.result(intent, self.initial)

    def reconcile_existing(self, intent):
        self.calls.append(("reconcile_existing", intent))
        return self.result(intent, self.current)

    def cancel_and_reconcile(self, intent):
        self.calls.append(("cancel_and_reconcile", intent))
        return self.result(intent, "CANCELED")

    def reconcile_exit(self, intent):
        self.calls.append(("reconcile_exit", intent))
        return self.result(intent, self.exit)

    def emergency_reduce(self, intent, code):
        self.calls.append(("emergency_reduce", intent, code))
        return self.result(intent, "HALTED")


class DemoAutoTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.clock_ms = NOW
        self.config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
        self.router = FakeRouter()
        self.risk = FakeRisk()
        self.network = patch.object(socket, "create_connection", side_effect=AssertionError("NETWORK_FORBIDDEN"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.controller = self.make_controller()

    def make_controller(self):
        return DemoAutoController(self.config, self.router, self.risk,
                                  lambda: {"last": "100", "timestamp_ms": self.clock_ms},
                                  self.root, clock=lambda: self.clock_ms)

    def scan(self, suffix="1"):
        return {"generated_at_ms": self.clock_ms, "gates": {"pass": True},
                "data_fresh": True, "macro_blackout": False,
                "new_signals": [{"id": "SCA-LONG-" + suffix, "instId": "SOL-USDT-SWAP",
                                  "kind": "scalp_5m", "side": "long", "entry": "100",
                                  "stop": "99", "tp1": "103", "tp2": "104",
                                  "generated_at_ms": self.clock_ms - 1000,
                                  "expires_at_ms": self.clock_ms + 60000}]}

    def test_current_formal_signal_uses_provider_equity_and_does_not_mutate_config(self):
        before = copy.deepcopy(self.config)
        result = self.controller.process_scan(self.scan())
        self.assertEqual(result["results"][0]["status"], "PENDING")
        intent = self.router.calls[0][1]
        self.assertEqual(intent.risk_budget, Decimal("2.3544"))
        self.assertLessEqual(intent.notional, Decimal("400"))
        self.assertEqual(self.risk.calls[0]["data_fresh"], True)
        self.assertEqual(self.config, before)

    def test_modes_and_account_alias_are_fixed_demo(self):
        for field, value in (("mode", "mock"), ("mode", "native-live"), ("account_key", "LIVE")):
            router = FakeRouter()
            setattr(router, field, value)
            with self.assertRaises(DemoAutoError):
                DemoAutoController(self.config, router, self.risk, lambda: {}, self.root)

    def test_no_signal_is_idle_and_never_loads_account(self):
        scan = self.scan()
        scan["new_signals"] = []
        self.assertEqual(self.controller.process_scan(scan)["code"], "NO_SIGNAL")
        self.assertEqual(self.controller.tick()["status"], "IDLE")
        self.assertEqual(self.risk.calls, [])
        self.assertEqual(self.router.calls, [])

    def test_old_signal_list_is_not_a_new_signal_source(self):
        scan = self.scan()
        scan["signals"] = scan.pop("new_signals")
        self.assertEqual(self.controller.process_scan(scan)["code"], "NEW_SIGNALS_REQUIRED")
        self.assertEqual(self.router.calls, [])

    def test_research_signal_never_claims_or_submits(self):
        scan = self.scan()
        scan["new_signals"][0]["id"] = "RSCH-SOL-1"
        self.assertEqual(self.controller.process_scan(scan)["results"][0]["code"], "RESEARCH_SIGNAL")
        self.assertEqual(self.risk.calls, [])
        self.assertEqual(self.controller.status()["status"], "IDLE")

    def test_scan_real_booleans_and_timestamps_are_required(self):
        for field, value, code in (("data_fresh", "true", "STALE_DATA"),
                                   ("macro_blackout", 0, "MACRO_BLACKOUT"),
                                   ("generated_at_ms", NOW + 1, "SCAN_STALE"),
                                   ("generated_at_ms", NOW - 1000000, "SCAN_STALE")):
            scan = self.scan()
            scan[field] = value
            self.assertEqual(self.controller.process_scan(scan)["code"], code)
        scan = self.scan()
        scan["gates"]["pass"] = 1
        self.assertEqual(self.controller.process_scan(scan)["code"], "SCAN_GATES_REJECTED")
        self.assertEqual(self.router.calls, [])

    def test_expired_or_not_current_signal_is_rejected_without_retry(self):
        scan = self.scan()
        scan["new_signals"][0]["expires_at_ms"] = NOW - 1
        self.assertEqual(self.controller.process_scan(scan)["results"][0]["code"], "EXPIRED")
        self.assertEqual(self.make_controller().process_scan(self.scan())["results"][0]["code"], "DUPLICATE")
        other = self.scan("2")
        other["new_signals"][0]["generated_at_ms"] = NOW - 30000
        self.assertEqual(self.controller.process_scan(other)["results"][0]["code"], "SIGNAL_NOT_FROM_CURRENT_SCAN")
        self.assertEqual(self.router.calls, [])

    def test_unverified_stale_or_incomplete_risk_state_cannot_submit(self):
        cases = [("demo_verified", False, "DEMO_STATE_UNVERIFIED"),
                 ("demo_verified", "true", "DEMO_STATE_UNVERIFIED"),
                 ("observed_at_ms", NOW - 5001, "DEMO_STATE_STALE"),
                 ("observed_at_ms", NOW + 1, "DEMO_STATE_STALE"),
                 ("refusal_reasons", ["BASELINE_MISSING"], "DEMO_RISK_NOT_READY"),
                 ("data_fresh", 1, "DEMO_RISK_NOT_READY"),
                 ("macro_blackout", 0, "DEMO_RISK_NOT_READY")]
        original = copy.deepcopy(self.risk.state)
        for number, (field, value, code) in enumerate(cases):
            self.risk.state = {**original, field: value}
            self.assertEqual(self.controller.process_scan(self.scan(str(number)))["results"][0]["code"], code)
        self.risk.state = original
        del self.risk.state["week_start_equity"]
        self.assertEqual(self.controller.process_scan(self.scan("more"))["results"][0]["code"], "DEMO_RISK_INCOMPLETE")
        self.assertEqual(self.router.calls, [])

    def test_second_signal_cannot_use_occupied_account_slot(self):
        scan = self.scan()
        scan["new_signals"] += self.scan("2")["new_signals"]
        results = self.controller.process_scan(scan)["results"]
        self.assertEqual(results[1]["code"], "POSITION_SLOT_BUSY")
        self.assertEqual(len(self.router.calls), 1)

    def test_reboot_retains_exact_decimal_intent_and_does_not_resubmit(self):
        self.controller.process_scan(self.scan())
        first = self.router.calls[0][1]
        resumed = self.make_controller()
        self.assertNotEqual(resumed.run_dir, self.controller.run_dir)
        self.assertEqual(resumed.tick()["status"], "PENDING")
        self.assertEqual([c[0] for c in self.router.calls], ["execute", "reconcile_existing"])
        restored = self.router.calls[1][1]
        self.assertEqual(restored, first)
        self.assertIsInstance(restored.qty, Decimal)
        self.assertEqual(resumed.process_scan(self.scan())["results"][0]["code"], "DUPLICATE")

    def test_expired_pending_is_canceled_and_reconciled_once(self):
        self.controller.process_scan(self.scan())
        self.clock_ms += 60001
        self.assertEqual(self.make_controller().tick()["status"], "CANCELED")
        self.assertEqual(self.router.calls[-1][0], "cancel_and_reconcile")
        self.assertEqual(self.controller.tick()["status"], "IDLE")

    def test_protected_position_is_not_canceled_on_signal_expiry(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.clock_ms += 60001
        self.assertEqual(self.controller.tick()["status"], "PROTECTED")
        self.assertEqual(self.router.calls[-1][0], "reconcile_exit")
        self.assertNotIn("cancel_and_reconcile", [c[0] for c in self.router.calls])

    def test_only_independent_exit_check_releases_filled_slot(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.assertEqual(self.controller.tick()["status"], "PROTECTED")
        self.assertEqual(self.controller.process_scan(self.scan("2"))["results"][0]["code"], "POSITION_SLOT_BUSY")
        self.router.exit = "CLOSED"
        self.assertEqual(self.controller.tick()["status"], "CLOSED")
        self.assertEqual(self.controller.status()["status"], "IDLE")

    def test_daily_count_is_provider_authority_after_exit(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.router.exit = "CLOSED"
        self.controller.tick()
        self.risk.state["entries_today"] = self.config["risk"]["max_entries_per_day"]
        self.assertEqual(self.controller.process_scan(self.scan("2"))["results"][0]["code"], "DAILY_ENTRIES")
        self.assertEqual(len([c for c in self.router.calls if c[0] == "execute"]), 1)

    def test_busy_signal_is_consumed_instead_of_queued_after_exit(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.assertEqual(self.controller.process_scan(self.scan("2"))["results"][0]["code"], "POSITION_SLOT_BUSY")
        self.router.exit = "CLOSED"
        self.controller.tick()
        self.assertEqual(self.make_controller().process_scan(self.scan("2"))["results"][0]["code"], "DUPLICATE")
        self.assertEqual(len([c for c in self.router.calls if c[0] == "execute"]), 1)

    def test_uncertain_submission_stays_halted_across_restarts(self):
        self.router.initial = "UNKNOWN"
        self.controller.process_scan(self.scan())
        before = len(self.router.calls)
        resumed = self.make_controller()
        self.assertEqual(resumed.tick()["status"], "HALTED")
        self.assertEqual(resumed.process_scan(self.scan("2"))["status"], "HALTED")
        self.assertEqual(len(self.router.calls), before)

    def test_exception_is_sanitized_and_delegates_emergency_then_halts(self):
        self.router.raise_execute = True
        self.assertEqual(self.controller.process_scan(self.scan())["results"][0]["status"], "HALTED")
        self.assertEqual(self.router.calls[-1][0], "emergency_reduce")
        self.assertNotIn("DO_NOT_LOG_SECRET", self.controller.audit_path.read_text(encoding="utf-8"))
        self.assertEqual(self.make_controller().status()["status"], "HALTED")

    def test_missing_router_journal_cannot_repeat_entry(self):
        self.controller.process_scan(self.scan())
        self.router.current = "REJECTED"
        self.router.result_code = "RECONCILE_NOT_FOUND"
        self.assertEqual(self.make_controller().tick()["status"], "HALTED")
        self.assertEqual(len([c for c in self.router.calls if c[0] == "execute"]), 1)

    def test_unprotected_fill_result_requests_emergency(self):
        original = self.router.result
        def invalid(intent, status):
            result = original(intent, "PROTECTED")
            result.protected_qty = Decimal("0")
            return result
        self.router.result = invalid
        self.assertEqual(self.controller.process_scan(self.scan())["results"][0]["status"], "HALTED")
        self.assertEqual(self.router.calls[-1][0], "emergency_reduce")

    def test_closed_status_from_initial_submission_is_not_exit_proof(self):
        self.router.initial = "CLOSED"
        self.assertEqual(self.controller.process_scan(self.scan())["results"][0]["status"], "HALTED")
        self.assertEqual(self.controller.status()["status"], "HALTED")

    def test_service_failure_cancels_or_reduces_only_persisted_owned_intent(self):
        self.controller.process_scan(self.scan())
        original = self.router.calls[0][1]
        result = self.controller.safety_halt("SERVICE_IO_FAILURE")
        self.assertEqual(result["status"], "HALTED")
        self.assertEqual(self.router.calls[-1], ("emergency_reduce", original, "SERVICE_IO_FAILURE"))
        self.assertEqual(self.make_controller().status()["status"], "HALTED")

    def test_service_failure_without_active_intent_sends_no_account_operation(self):
        self.assertEqual(self.controller.safety_halt("SERVICE_IO_FAILURE")["status"], "HALTED")
        self.assertEqual(self.router.calls, [])
        self.assertEqual(self.make_controller().process_scan(self.scan())["status"], "HALTED")

    def test_service_failure_does_not_reconstruct_a_damaged_plan(self):
        self.controller.process_scan(self.scan())
        with self.controller._db() as db:
            db.execute("UPDATE controller_signals SET intent='{}'")
        before = len(self.router.calls)
        self.assertEqual(self.controller.safety_halt("SERVICE_IO_FAILURE")["status"], "HALTED")
        self.assertEqual(len(self.router.calls), before)

    def test_maximum_holding_duration_uses_existing_policy_and_planned_exit(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        hours = self.config["playbooks"]["scalp_5m"]["max_hold_hours"]
        self.clock_ms += int(hours * 3_600_000)
        def close(intent, *, code):
            self.router.calls.append(("planned_close", intent, code))
            result = self.router.result(intent, "CLOSED")
            result.code = code
            return result
        with patch.object(self.router, "planned_close", side_effect=close, create=True):
            result = self.controller.tick()
        self.assertEqual(result["code"], "MAX_HOLD_REACHED")
        self.assertEqual(result["status"], "CLOSED")
        self.assertEqual(self.router.calls[-1][0], "planned_close")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])
        self.assertEqual(self.make_controller().status()["status"], "IDLE")

    def test_no_overnight_utc_boundary_exits_owned_position(self):
        self.clock_ms = (NOW // 86_400_000 + 1) * 86_400_000 - 10_000
        self.risk.state["observed_at_ms"] = self.clock_ms
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.clock_ms += 10_001
        def close(intent, *, code):
            self.router.calls.append(("planned_close", intent, code))
            result = self.router.result(intent, "CLOSED")
            result.code = code
            return result
        with patch.object(self.router, "planned_close", side_effect=close, create=True):
            result = self.controller.tick()
        self.assertEqual(result["code"], "NO_OVERNIGHT_EXIT")
        self.assertEqual(result["status"], "CLOSED")
        self.assertEqual(self.router.calls[-1][0], "planned_close")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])
        self.assertNotIn("cancel_and_reconcile", [call[0] for call in self.router.calls])

    def test_already_flat_exit_does_not_send_a_deadline_reduction(self):
        self.router.initial = "PROTECTED"
        self.controller.process_scan(self.scan())
        self.clock_ms += 100 * 3_600_000
        self.router.exit = "CLOSED"
        self.assertEqual(self.controller.tick()["status"], "CLOSED")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])


if __name__ == "__main__":
    unittest.main()
