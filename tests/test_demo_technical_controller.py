"""Explicit technical plans keep the real controller/risk/route boundaries."""
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
from test_demo_auto import FakeRisk, NOW


ROOT = Path(__file__).resolve().parents[1]


class TechnicalRouter:
    mode = "demo"
    account_key = DEMO_ACCOUNT_KEY

    def __init__(self):
        self.calls = []
        self.initial = "PROTECTED"
        self.close_status = "CLOSING"
        self.close_code = "DEMO_TECHNICAL_TEST_EXIT"
        self.close_raises = False

    def result(self, intent, status, code="OK"):
        filled = intent.qty if status not in {"PENDING", "CANCELED", "REJECTED"} else Decimal("0")
        return SimpleNamespace(status=status, code=code, signal_id=intent.signal_id,
                               filled_qty=filled,
                               protected_qty=filled if status == "PROTECTED" else Decimal("0"))

    def execute(self, intent):
        self.calls.append(("execute", intent))
        return self.result(intent, self.initial)

    def planned_close(self, intent, *, code):
        self.calls.append(("planned_close", intent, code))
        if self.close_raises:
            raise RuntimeError("DO_NOT_LOG_PRIVATE_RESPONSE")
        return self.result(intent, self.close_status, self.close_code)

    def emergency_reduce(self, intent, code):
        self.calls.append(("emergency_reduce", intent, code))
        return self.result(intent, "HALTED", code)

    def resume_emergency(self, intent):
        self.calls.append(("resume_emergency", intent))
        result = self.result(intent, "HALTED")
        result.emergency_status = "CLOSE_UNCONFIRMED"
        return result

    def reconcile_existing(self, intent):
        self.calls.append(("reconcile_existing", intent))
        return self.result(intent, self.initial)

    def reconcile_exit(self, intent):
        self.calls.append(("reconcile_exit", intent))
        return self.result(intent, "PROTECTED")

    def cancel_and_reconcile(self, intent):
        self.calls.append(("cancel_and_reconcile", intent))
        return self.result(intent, "CANCELED")


class DemoTechnicalControllerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
        self.router = TechnicalRouter()
        self.risk = FakeRisk()
        self.now = NOW
        self.addCleanup(patch.stopall)
        patch.object(socket.socket, "connect", side_effect=AssertionError("NO_NETWORK")).start()
        self.controller = self.make_controller()

    def make_controller(self, enabled=True):
        return DemoAutoController(self.config, self.router, self.risk,
                                  lambda: {"last": "100", "timestamp_ms": self.now},
                                  self.root, clock=lambda: self.now, enable_demo_tests=enabled)

    def plan(self, scenario="smoke", suffix="ONE"):
        return {"purpose": "DEMO_TECHNICAL_TEST", "scenario": scenario,
                "authorization_id": "user-approved-demo-technical-tests-20261019",
                "signal": {"id": "DEMO-TEST-" + scenario.upper() + "-" + suffix,
                           "instId": "SOL-USDT-SWAP",
                           "kind": "scalp_5m" if scenario == "smoke" else "swing_1h4h",
                           "side": "long", "entry": "100", "stop": "99", "tp1": "103",
                           "tp2": "104", "qty": "0.01", "generated_at_ms": self.now,
                           "expires_at_ms": self.now + 60000}}

    def market(self):
        return {"generated_at_ms": self.now, "data_fresh": True, "macro_blackout": False}

    def test_default_disabled_and_strict_boolean_enable(self):
        ordinary = DemoAutoController(self.config, self.router, self.risk,
                                      lambda: {}, self.root, clock=lambda: self.now)
        self.assertEqual(ordinary.process_demo_test(self.plan(), self.market())["code"], "DEMO_TESTS_DISABLED")
        self.assertEqual(ordinary.close_demo_test(self.plan()["signal"]["id"])["code"], "DEMO_TESTS_DISABLED")
        for invalid in (1, 0, "true", None):
            with self.subTest(invalid=invalid), self.assertRaises(DemoAutoError):
                self.make_controller(invalid)
        self.assertEqual(self.router.calls, [])

    def test_smoke_and_swing_use_existing_maker_policy_with_exact_minimum(self):
        before = copy.deepcopy(self.config)
        for scenario in ("smoke", "swing"):
            with self.subTest(scenario=scenario):
                result = self.controller.process_demo_test(self.plan(scenario), self.market())
                self.assertEqual(result["status"], "PROTECTED")
                intent = self.router.calls[-1][1]
                self.assertEqual(intent.qty, Decimal("0.01"))
                self.assertTrue(intent.require_maker)
                self.assertEqual(intent.kind, self.plan(scenario)["signal"]["kind"])
                self.assertLessEqual(intent.notional, Decimal("400"))
                self.router.close_status = "CLOSED"
                self.assertEqual(self.controller.close_demo_test(intent.signal_id)["status"], "CLOSED")
        self.assertEqual(self.config, before)
        with self.controller._db() as db:
            rows = db.execute("SELECT purpose,scenario,authorization_id FROM technical_tests ORDER BY scenario").fetchall()
        self.assertEqual([row[1] for row in rows], ["smoke", "swing"])
        self.assertTrue(all(row[0] == "DEMO_TECHNICAL_TEST" for row in rows))
        self.assertEqual(len(self.risk.calls), 2)

    def test_plan_purpose_authorization_and_scenario_are_not_inferred(self):
        for field, value in (("purpose", "STRATEGY"), ("authorization_id", "arbitrary"),
                             ("scenario", "other"), ("scenario", [])):
            plan = self.plan()
            plan[field] = value
            self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "REJECTED")
        plan = self.plan()
        plan["allow_unverified_risk"] = True
        self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "REJECTED")
        self.assertEqual(self.router.calls, [])

    def test_scenario_prefix_kind_and_instrument_cannot_be_relabelled(self):
        for field, value in (("id", "SCA-LONG-ONE"), ("id", "DEMO-TEST-SWING-ONE"),
                             ("kind", "swing_1h4h"), ("instId", "ETH-USDT-SWAP"),
                             ("id", "DEMO-TEST-SMOKE-")):
            plan = self.plan()
            plan["signal"][field] = value
            self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "REJECTED")
        self.assertEqual(self.risk.calls, [])
        self.assertEqual(self.router.calls, [])

    def test_only_instrument_minimum_is_accepted_even_when_normal_risk_allows_more(self):
        for qty in ("0.02", "0.009", "0", True, None, "NaN"):
            plan = self.plan()
            plan["signal"]["qty"] = qty
            self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "REJECTED")
        self.assertEqual(self.router.calls, [])
        plan = self.plan()
        plan["signal"]["qty"] = Decimal("0.0100")
        self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "PROTECTED")

    def test_unknown_signal_evidence_fields_are_not_a_risk_bypass(self):
        plan = self.plan()
        plan["signal"]["demo_verified"] = True
        self.assertEqual(self.controller.process_demo_test(plan, self.market())["code"], "DEMO_TEST_SIGNAL_INVALID")
        self.assertEqual(self.router.calls, [])

    def test_actual_market_freshness_and_blackout_are_mandatory_without_scan_gates(self):
        for field, value in (("data_fresh", False), ("data_fresh", 1),
                             ("macro_blackout", True), ("macro_blackout", None),
                             ("generated_at_ms", self.now - 60001),
                             ("generated_at_ms", self.now + 1)):
            market = self.market()
            market[field] = value
            self.assertEqual(self.controller.process_demo_test(self.plan(), market)["status"], "REJECTED")
        self.assertEqual(self.router.calls, [])
        # No gates.pass is invented or needed for an explicitly technical plan.
        self.assertEqual(self.controller.process_demo_test(self.plan(), self.market())["status"], "PROTECTED")

    def test_verified_risk_daily_entries_and_pause_remain_authoritative(self):
        original = copy.deepcopy(self.risk.state)
        cases = (("demo_verified", False, "DEMO_STATE_UNVERIFIED"),
                 ("refusal_reasons", ["LEDGER_FILL_MISSING"], "DEMO_RISK_NOT_READY"),
                 ("entries_today", 3, "DAILY_ENTRIES"),
                 ("paused_until_ms", self.now + 50000, "PAUSED"))
        for number, (field, value, code) in enumerate(cases):
            self.risk.state = {**original, field: value}
            result = self.controller.process_demo_test(self.plan(suffix=str(number)), self.market())
            self.assertEqual(result["code"], code)
        self.assertEqual(self.router.calls, [])

    def test_ordinary_scan_rejects_technical_ids_without_consuming_registration(self):
        for prefix in ("DEMO-TEST-SMOKE-ONE", "demo-test-smoke-one"):
            signal = self.plan()["signal"]
            signal["id"] = prefix
            scan = {**self.market(), "gates": {"pass": True}, "new_signals": [signal]}
            result = self.controller.process_scan(scan)
            self.assertEqual(result["results"][0]["code"], "DEMO_TEST_REQUIRES_SEPARATE_ENTRY")
        self.assertEqual(self.router.calls, [])
        with self.controller._db() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM technical_tests").fetchone()[0], 0)
        self.assertEqual(self.controller.process_demo_test(self.plan(), self.market())["status"], "PROTECTED")

    def test_registration_and_exact_plan_are_idempotent_across_restart(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        resumed = self.make_controller()
        self.assertEqual(resumed.process_demo_test(plan, self.market())["code"], "DUPLICATE")
        conflict = copy.deepcopy(plan)
        conflict["signal"]["entry"] = "100.01"
        self.assertEqual(resumed.process_demo_test(conflict, self.market())["code"], "DEMO_TEST_IDEMPOTENCY_CONFLICT")
        self.assertEqual([call[0] for call in self.router.calls], ["execute"])

    def test_registration_crash_before_controller_claim_can_resume_same_plan(self):
        plan = self.plan()
        with patch.object(self.controller, "_process_signal", side_effect=RuntimeError("crash")):
            self.assertEqual(self.controller.process_demo_test(plan, self.market())["status"], "HALTED")
        # Failure latches a HALT; a restart does not erase it or submit a trade.
        self.assertEqual(self.make_controller().process_demo_test(plan, self.market())["status"], "HALTED")
        self.assertEqual(self.router.calls, [])

    def test_existing_ordinary_trade_cannot_be_registered_or_closed_as_test(self):
        signal = self.plan()["signal"]
        signal["id"] = "SCA-LONG-REAL"
        self.controller.process_scan({**self.market(), "gates": {"pass": True}, "new_signals": [signal]})
        before = len(self.router.calls)
        self.assertEqual(self.controller.close_demo_test(signal["id"])["code"], "DEMO_TEST_NOT_REGISTERED")
        self.assertEqual(len(self.router.calls), before)

    def test_wrong_test_id_cannot_close_another_active_test(self):
        first = self.plan()
        self.controller.process_demo_test(first, self.market())
        second = self.plan(suffix="TWO")
        self.assertEqual(self.controller.process_demo_test(second, self.market())["code"], "POSITION_SLOT_BUSY")
        before = len(self.router.calls)
        self.assertEqual(self.controller.close_demo_test(second["signal"]["id"])["code"], "DEMO_TEST_NOT_ACTIVE")
        self.assertEqual(len(self.router.calls), before)

    def test_close_checkpoint_is_saved_before_any_route_close_call(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        original = self.router.planned_close

        def checked(intent, *, code):
            with self.controller._db() as db:
                status = db.execute("SELECT status FROM controller_signals WHERE signal=?", (intent.signal_id,)).fetchone()[0]
            self.assertEqual(status, "CLOSING")
            return original(intent, code=code)

        self.router.planned_close = checked
        result = self.controller.close_demo_test(plan["signal"]["id"])
        self.assertEqual(result["status"], "CLOSING")
        self.assertEqual(self.controller.status()["status"], "CLOSING")

    def test_closing_reboot_only_uses_existing_planned_exit_even_after_expiry_and_disable(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.controller.close_demo_test(plan["signal"]["id"])
        self.now += 100000
        self.assertEqual(self.make_controller(enabled=False).tick()["status"], "CLOSING")
        self.assertEqual([call[0] for call in self.router.calls], ["execute", "planned_close", "planned_close"])
        self.router.close_status = "CLOSED"
        self.assertEqual(self.make_controller(enabled=False).tick()["status"], "CLOSED")
        self.assertEqual(self.controller.status()["status"], "IDLE")
        self.assertEqual(self.controller.close_demo_test(plan["signal"]["id"])["code"], "DEMO_TEST_NOT_ACTIVE")

    def test_no_fill_planned_close_can_be_canceled_but_not_counted_as_filled(self):
        self.router.initial = "PENDING"
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.router.close_status = "CANCELED"
        self.assertEqual(self.controller.close_demo_test(plan["signal"]["id"])["status"], "CANCELED")
        self.assertEqual(self.controller.status()["status"], "IDLE")

    def test_close_result_cannot_regress_to_entry_or_protection_flow(self):
        for number, invalid in enumerate(("PENDING", "PROTECTED", "SUBMITTING")):
            root = self.root / str(number)
            controller = DemoAutoController(self.config, self.router, self.risk,
                                            lambda: {"last": "100", "timestamp_ms": self.now},
                                            root, clock=lambda: self.now, enable_demo_tests=True)
            plan = self.plan(suffix=str(number))
            controller.process_demo_test(plan, self.market())
            self.router.close_status = invalid
            self.assertEqual(controller.close_demo_test(plan["signal"]["id"])["status"], "CLOSING")
            self.assertEqual(self.router.calls[-1][0], "planned_close")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])

    def test_unknown_exit_keeps_sticky_halt_and_no_new_entry_after_restart(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.router.close_status = "UNKNOWN"
        self.assertEqual(self.controller.close_demo_test(plan["signal"]["id"])["status"], "HALTED")
        resumed = self.make_controller()
        self.assertEqual(resumed.process_demo_test(self.plan(suffix="TWO"), self.market())["status"], "HALTED")
        self.assertEqual(resumed.tick()["status"], "HALTED")
        self.assertEqual([call[0] for call in self.router.calls].count("execute"), 1)
        self.assertEqual(self.router.calls[-1][0], "resume_emergency")

    def test_throwing_close_is_sanitized_and_never_uses_alternate_emergency(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        intent = self.router.calls[0][1]
        self.router.close_raises = True
        result = self.controller.close_demo_test(plan["signal"]["id"])
        self.assertEqual(result["status"], "CLOSING")
        self.assertEqual(result["code"], "PLANNED_EXIT_UNCERTAIN")
        self.assertEqual(self.router.calls[-1], ("planned_close", intent, "DEMO_TECHNICAL_TEST_EXIT"))
        self.assertNotIn("DO_NOT_LOG_PRIVATE_RESPONSE", self.controller.audit_path.read_text(encoding="utf-8"))
        resumed = self.make_controller()
        self.assertEqual(resumed.tick()["status"], "CLOSING")
        self.router.close_raises = False
        self.router.close_status = "CLOSED"
        self.assertEqual(resumed.tick()["status"], "CLOSED")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])

    def test_service_safety_guard_keeps_existing_planned_exit_and_sends_no_second_close(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.controller.close_demo_test(plan["signal"]["id"])
        before = len(self.router.calls)
        result = self.controller.safety_halt("REPORT_WRITE_FAILED")
        self.assertEqual((result["status"], result["code"]), ("CLOSING", "REPORT_WRITE_FAILED"))
        self.assertEqual(len(self.router.calls), before)
        self.assertEqual(self.make_controller().tick()["status"], "CLOSING")
        self.assertEqual(self.router.calls[-1][0], "planned_close")

    def test_local_audit_failure_after_planned_close_does_not_send_emergency(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        original = self.controller._audit
        calls = 0

        def failing_result_audit(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("local audit failed after route returned")
            return original(*args)

        with patch.object(self.controller, "_audit", side_effect=failing_result_audit):
            result = self.controller.close_demo_test(plan["signal"]["id"])
        self.assertEqual(result["status"], "CLOSING")
        self.assertEqual([call[0] for call in self.router.calls], ["execute", "planned_close"])
        self.assertEqual(self.make_controller().tick()["status"], "CLOSING")

    def test_unwritable_closing_journal_halts_without_an_alternate_post(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.controller.close_demo_test(plan["signal"]["id"])
        self.router.close_raises = True
        with patch.object(self.controller, "_save", side_effect=OSError("disk failure")):
            self.assertEqual(self.controller.tick()["status"], "HALTED")
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])

    def test_corrupted_registration_cannot_authorize_a_close(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        with self.controller._db() as db:
            db.execute("UPDATE technical_tests SET scenario='swing'")
        before = len(self.router.calls)
        self.assertEqual(self.controller.close_demo_test(plan["signal"]["id"])["code"], "DEMO_TEST_REGISTRATION_INVALID")
        self.assertEqual(len(self.router.calls), before)

    def test_corrupted_closing_registration_remains_unresolved_instead_of_reentering(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.controller.close_demo_test(plan["signal"]["id"])
        with self.controller._db() as db:
            db.execute("DELETE FROM technical_tests")
        self.assertEqual(self.make_controller().tick()["status"], "CLOSING")
        self.assertEqual([call[0] for call in self.router.calls].count("execute"), 1)
        self.assertNotIn("emergency_reduce", [call[0] for call in self.router.calls])

    def test_initial_route_cannot_claim_closing_is_independently_verified_exit(self):
        self.router.initial = "CLOSING"
        self.assertEqual(self.controller.process_demo_test(self.plan(), self.market())["status"], "HALTED")
        self.assertEqual(self.router.calls[-1][0], "emergency_reduce")

    def test_next_technical_test_cannot_ignore_actual_count_or_ledger_after_previous_exit(self):
        plan = self.plan()
        self.controller.process_demo_test(plan, self.market())
        self.router.close_status = "CLOSED"
        self.controller.close_demo_test(plan["signal"]["id"])
        self.risk.state["entries_today"] = 3
        result = self.controller.process_demo_test(self.plan("swing"), self.market())
        self.assertEqual(result["code"], "DAILY_ENTRIES")
        self.assertEqual(self.risk.state["entries_today"], 3)
        self.risk.state["entries_today"] = 1
        self.risk.state["refusal_reasons"] = ["LEDGER_POSITION_HISTORY_MISSING"]
        result = self.controller.process_demo_test(self.plan("swing", "SECOND"), self.market())
        self.assertEqual(result["code"], "DEMO_RISK_NOT_READY")
        self.assertEqual([call[0] for call in self.router.calls].count("execute"), 1)


if __name__ == "__main__":
    unittest.main()
