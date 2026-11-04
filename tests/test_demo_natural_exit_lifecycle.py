"""Natural holding deadlines over real controller/router and fake HTTP only."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.demo_auto import DemoAutoController
from src.demo_route_transport import DemoRouteTransport
from src.demo_transport import CREDENTIAL_NAMES
from src.execution import build_intent
from src.order_router import OrderRouter
import test_demo_planned_close as planned_fixtures
import test_demo_router_integration as exchange_fixtures
from test_demo_service_halt_recovery import ENTRY as SERVICE_ENTRY
from test_execution import NOW, inputs


class NaturalExitLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.serial = 0
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target in ("socket.socket.connect", "socket.create_connection",
                       "src.demo_transport.load_demo_credentials",
                       "src.demo_route_transport.load_demo_credentials"):
            self.stack.enter_context(patch(target, side_effect=AssertionError("NO_NETWORK_OR_CREDENTIAL_READ")))

    def fixture(self, kind="scalp_5m", *, started=NOW, technical=False):
        self.serial += 1
        self.stack.enter_context(patch.object(planned_fixtures, "NOW", started))
        self.stack.enter_context(patch.object(exchange_fixtures, "NOW", started))
        signal, state, config = inputs()
        signal.update(id=f"{'SCA' if kind == 'scalp_5m' else 'SWI'}-LONG-NATURAL-{self.serial}",
                      kind=kind, generated_at_ms=started - 1000, expires_at_ms=started + 60_000,
                      qty="0.01" if technical else "0.02")
        state.update(now_ms=started, demo_verified=True, observed_at_ms=started, account_hash="a" * 64,
                     week_start_equity="117.72", realized_pnl_week="0", refusal_reasons=[])
        case = SimpleNamespace(root=self.root / str(self.serial), config=config, state=state, signal=signal,
                               now=started, exchange=planned_fixtures.PlannedExchange(), risk_calls=[])
        self.reopen(case, technical=technical)
        if technical:
            scenario = "smoke" if kind == "scalp_5m" else "swing"
            signal["id"] = f"DEMO-TEST-{scenario.upper()}-NATURAL-BOUNDARY-{self.serial}"
            plan = {"purpose": "DEMO_TECHNICAL_TEST", "scenario": scenario,
                    "authorization_id": "user-approved-demo-technical-tests-20261019", "signal": signal}
            result = case.controller.process_demo_test(plan, self.payload(case))
        else:
            result = case.controller.process_scan(self.payload(case))["results"][0]
        self.assertEqual(result["status"], "PROTECTED", result)
        case.intent = build_intent(signal, state, config)
        self.assertEqual(case.controller.tick()["status"], "PROTECTED")
        return case

    def reopen(self, case, *, technical=False):
        credentials = dict(zip(CREDENTIAL_NAMES, ("fixture-key", "fixture-secret", "fixture-pass")))
        transport = DemoRouteTransport(credentials, enable_writes=True,
                    http_get=case.exchange.get, http_post=case.exchange.post, log_root=case.root / "logs")
        def state_provider(_market=None):
            case.risk_calls.append(True)
            return deepcopy(case.state)
        router = OrderRouter(transport, config=case.config, state_provider=state_provider,
                    market_provider=lambda: {"last": "100", "bid": "99.9", "ask": "100.1", "timestamp_ms": case.now},
                    log_root=case.root / "logs", journal_path=case.root / "state" / "demo-auto" / "router.sqlite3",
                    account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64, clock=lambda: case.now)
        controller = DemoAutoController(case.config, router, state_provider,
                    lambda: {"last": "100", "timestamp_ms": case.now}, case.root,
                    clock=lambda: case.now, enable_demo_tests=technical)
        case.router, case.controller = router, controller

    @staticmethod
    def payload(case, *, suffix=None):
        signal = deepcopy(case.signal)
        if suffix is not None:
            signal.update(id="SCA-LONG-NEXT-" + suffix, generated_at_ms=case.now,
                          expires_at_ms=case.now + 60_000)
        return {"generated_at_ms": case.now, "data_fresh": True, "macro_blackout": False,
                "gates": {"pass": True}, "new_signals": [signal]}

    @staticmethod
    def closes(case):
        return [body for _, body in case.exchange.posts if isinstance(body, dict) and body.get("ordType") == "market"]

    @staticmethod
    def route(case):
        with sqlite3.connect(case.router.journal_path) as db:
            return json.loads(db.execute("SELECT data FROM routes WHERE signal=?", (case.intent.signal_id,)).fetchone()[0])

    @staticmethod
    def registration(case):
        with case.controller._db() as db:
            row = db.execute("SELECT record FROM natural_exits WHERE signal=?", (case.intent.signal_id,)).fetchone()
        if row is None:
            return None
        record = json.loads(row[0])
        return tuple(record[key] for key in ("reason", "intent_sha256", "requested_at_ms"))

    @staticmethod
    def replace_registration(case, changes=None, *, raw=None):
        with case.controller._db() as db:
            if raw is None:
                record = json.loads(db.execute("SELECT record FROM natural_exits WHERE signal=?",
                                              (case.intent.signal_id,)).fetchone()[0])
                record.update(changes)
                raw = json.dumps(record, sort_keys=True, separators=(",", ":"))
            db.execute("UPDATE natural_exits SET record=? WHERE signal=?", (raw, case.intent.signal_id))

    @staticmethod
    def deadline(case):
        return case.intent.created_at_ms + int(case.config["playbooks"][case.intent.kind]["max_hold_hours"] * 3_600_000)

    def assert_clean_terminal(self, case, reason):
        result = case.controller.tick()
        self.assertEqual((result["status"], result["code"]), ("CLOSED", reason))
        self.assertEqual(case.controller.status()["status"], "IDLE")
        self.assertFalse(case.controller.halt_path.exists())
        self.assertFalse(case.router.halt_path.exists())
        self.assertEqual(case.exchange.positions, [])
        self.assertEqual(len(self.closes(case)), 1)
        self.assertIs(self.closes(case)[0]["reduceOnly"], True)
        self.assertEqual(case.exchange.order_events[-2:], ["close", "cancel_protection"])
        row = self.route(case)
        self.assertEqual(row["status"], "CLOSED")
        self.assertEqual(row["planned_close_code"], reason)
        self.assertEqual(row["close_order_ids"], ["plannedorder"])
        self.assertEqual(row["position_id"], "position1")
        self.assertEqual(Decimal(row["confirmed_closed_qty"]), case.intent.qty)
        self.assertTrue(row["close_attempted"])
        self.assertTrue(row["cancel_protection_attempted"])
        self.assertEqual(self.registration(case), (reason, case.controller._intent_fingerprint(case.intent), case.now))
        writes = len(case.exchange.posts)
        self.assertEqual(case.controller.tick()["status"], "IDLE")
        self.assertEqual(len(case.exchange.posts), writes)

    def test_scalp_exact_eight_hour_deadline_exits_with_protection_without_halting(self):
        case = self.fixture()
        before = deepcopy(case.config)
        case.now = self.deadline(case) - 1
        self.assertEqual(case.controller.tick()["status"], "PROTECTED")
        self.assertEqual(self.closes(case), [])
        case.now += 1
        self.assert_clean_terminal(case, "MAX_HOLD_REACHED")
        self.assertEqual(case.config, before)

    def test_swing_keeps_overnight_position_until_original_seventy_two_hours(self):
        case = self.fixture("swing_1h4h")
        case.now += 24 * 3_600_000
        self.assertEqual(case.controller.tick()["status"], "PROTECTED")
        self.assertEqual(self.closes(case), [])
        case.now = self.deadline(case)
        self.assert_clean_terminal(case, "MAX_HOLD_REACHED")

    def test_scalp_utc_date_boundary_uses_original_no_overnight_deadline(self):
        started = (NOW // 86_400_000 + 1) * 86_400_000 - 10_000
        case = self.fixture(started=started)
        case.now = started + 9999
        self.assertEqual(case.controller.tick()["status"], "PROTECTED")
        case.now += 1
        self.assert_clean_terminal(case, "NO_OVERNIGHT_EXIT")

    def test_already_flat_natural_oco_exit_wins_before_due_planned_close(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.exchange.positions, case.exchange.algos = [], {}
        self.assertEqual(case.controller.tick()["status"], "CLOSED")
        self.assertEqual(self.closes(case), [])
        self.assertIsNone(self.registration(case))

    def test_registry_and_closing_checkpoint_are_atomic_before_exit_io(self):
        case = self.fixture()
        case.now = self.deadline(case)
        original = case.router.planned_close
        def checked(intent, *, code):
            self.assertEqual(case.controller.status()["status"], "CLOSING")
            self.assertEqual(self.registration(case), (code, case.controller._intent_fingerprint(intent), case.now))
            return original(intent, code=code)
        with patch.object(case.router, "planned_close", side_effect=checked):
            self.assert_clean_terminal(case, "MAX_HOLD_REACHED")

    def test_unknown_close_ack_and_receipt_restart_do_not_repost_or_cancel_protection(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.exchange.close_unknown = case.exchange.hide_close_receipt = True
        self.assertEqual(case.controller.tick()["status"], "CLOSING")
        saved = self.registration(case)
        close_id = self.route(case)["close_id"]
        self.assertEqual(len(self.closes(case)), 1)
        self.assertNotIn("cancel_protection", case.exchange.order_events)
        self.assertEqual(next(iter(case.exchange.algos.values()))["state"], "live")
        self.assertEqual(case.controller.process_scan(self.payload(case, suffix="BUSY"))["results"][0]["code"], "POSITION_SLOT_BUSY")
        self.reopen(case)
        case.router.recover_demo_owners()
        for _ in range(2):
            self.assertEqual(case.controller.tick()["status"], "CLOSING")
        self.assertEqual(self.registration(case), saved)
        self.assertEqual(self.route(case)["close_id"], close_id)
        self.assertEqual(len(self.closes(case)), 1)
        case.exchange.hide_close_receipt = False
        self.assertEqual(case.controller.tick()["status"], "CLOSED")
        self.assertEqual(len(self.closes(case)), 1)
        self.assertFalse(case.controller.halt_path.exists())

    def test_partial_exit_and_service_failure_never_create_an_alternate_close(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.exchange.close_partial = True
        self.assertEqual(case.controller.tick()["status"], "CLOSING")
        self.assertEqual(case.controller.safety_halt("REPORT_FAILED")["status"], "CLOSING")
        self.reopen(case)
        for _ in range(2):
            self.assertEqual(case.controller.tick()["status"], "CLOSING")
        self.assertEqual(len(self.closes(case)), 1)
        self.assertNotEqual(case.exchange.positions, [])
        self.assertNotIn("cancel_protection", case.exchange.order_events)

    def test_crash_after_local_checkpoint_before_first_exit_post_can_resume_once(self):
        case = self.fixture()
        case.now = self.deadline(case)
        before = len(case.exchange.posts)
        with patch.object(case.router, "planned_close", side_effect=SystemExit("simulated process loss")):
            with self.assertRaises(SystemExit):
                case.controller.tick()
        self.assertEqual(case.controller.status()["status"], "CLOSING")
        self.assertIsNotNone(self.registration(case))
        self.assertEqual(len(case.exchange.posts), before)
        self.assertEqual(self.route(case)["status"], "PROTECTED")
        self.reopen(case)
        self.assertEqual(case.controller.tick()["status"], "CLOSED")
        self.assertEqual(len(self.closes(case)), 1)
        self.assertEqual(case.controller.tick()["status"], "IDLE")

    def test_closed_does_not_bypass_new_ledger_or_daily_entry_admission(self):
        case = self.fixture()
        case.now = self.deadline(case)
        self.assert_clean_terminal(case, "MAX_HOLD_REACHED")
        writes = len(case.exchange.posts)
        case.state.update(observed_at_ms=case.now, entries_today=3)
        result = case.controller.process_scan(self.payload(case, suffix="DAILY"))
        self.assertEqual(result["results"][0]["code"], "DAILY_ENTRIES")
        case.state.update(entries_today=1, demo_verified=False)
        result = case.controller.process_scan(self.payload(case, suffix="LEDGER"))
        self.assertEqual(result["results"][0]["code"], "DEMO_STATE_UNVERIFIED")
        self.assertEqual(len(case.exchange.posts), writes)

    def test_corrupt_protection_still_triggers_original_emergency_and_sticky_halt(self):
        case = self.fixture()
        case.now = self.deadline(case)
        next(iter(case.exchange.algos.values()))["slTriggerPx"] = "98"
        case.exchange.allow_unprotected_close = True
        self.assertEqual(case.controller.tick()["status"], "HALTED")
        self.assertTrue(case.controller.halt_path.exists())
        self.assertTrue(case.router.halt_path.exists())
        self.assertIsNone(self.registration(case))

    def test_external_position_is_not_closed_as_a_normal_deadline(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.exchange.positions.append(dict(instId="ETH-USDT-SWAP", pos="1", posSide="net", mgnMode="cross"))
        self.assertEqual(case.controller.tick()["status"], "HALTED")
        self.assertEqual(self.closes(case), [])
        self.assertIsNone(self.registration(case))

    def test_arbitrary_closing_row_without_registration_grants_no_exit_io(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.controller._save(case.intent.signal_id, "CLOSING", "MAX_HOLD_REACHED")
        before = len(case.exchange.posts)
        with patch.object(case.router, "planned_close", side_effect=AssertionError("NO_EXIT_AUTHORITY")), \
                patch.object(case.router, "emergency_reduce", side_effect=AssertionError("NO_ALTERNATE_EXIT")):
            result = case.controller.tick()
        self.assertEqual((result["status"], result["code"]), ("HALTED", "NATURAL_EXIT_NOT_REGISTERED"))
        self.assertEqual(len(case.exchange.posts), before)

    def test_bad_reason_fingerprint_time_or_technical_binding_cannot_resume_natural_exit(self):
        for change in ("reason", "fingerprint", "not_due", "future", "technical"):
            with self.subTest(change=change):
                case = self.fixture()
                case.now = self.deadline(case)
                case.exchange.close_unknown = case.exchange.hide_close_receipt = True
                self.assertEqual(case.controller.tick()["status"], "CLOSING")
                if change == "reason":
                    self.replace_registration(case, {"reason": "ARBITRARY_EXIT"})
                elif change == "fingerprint":
                    self.replace_registration(case, {"intent_sha256": "0" * 64})
                elif change == "not_due":
                    self.replace_registration(case, {"requested_at_ms": case.intent.created_at_ms + 1})
                elif change == "future":
                    self.replace_registration(case, {"requested_at_ms": case.now + 1})
                else:
                    with case.controller._db() as db:
                        db.execute("INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)",
                                   (case.intent.signal_id, "DEMO_TECHNICAL_TEST", "smoke", "forged", "0" * 64, "{}", case.now))
                writes = len(case.exchange.posts)
                self.reopen(case)
                with patch.object(case.router, "planned_close", side_effect=AssertionError("NO_EXIT_AUTHORITY")), \
                        patch.object(case.router, "emergency_reduce", side_effect=AssertionError("NO_ALTERNATE_EXIT")):
                    with SERVICE_ENTRY.service_safety_guard(case.controller):
                        self.assertEqual(case.controller.tick()["status"], "HALTED")
                    self.assertEqual(case.controller.safety_halt("LATER_CLEANUP")["status"], "HALTED")
                    self.assertEqual(case.controller.tick()["status"], "HALTED")
                self.assertEqual(len(case.exchange.posts), writes)
                self.assertEqual(len(self.closes(case)), 1)

    def test_registration_schema_purpose_strict_types_and_exact_fields_are_required(self):
        cases = (("schema", True), ("schema", 1.0), ("schema", 2),
                 ("purpose", "DEMO_TECHNICAL_TEST"), ("signal_id", "OTHER-SIGNAL"),
                 ("requested_at_ms", True), ("requested_at_ms", float(NOW + 8 * 3_600_000 - 1000)),
                 ("requested_at_ms", float("nan")), ("requested_at_ms", float("inf")),
                 ("extra", "unexpected"))
        for key, value in cases:
            with self.subTest(key=key, value=value):
                case = self.fixture()
                case.now = self.deadline(case)
                case.exchange.close_unknown = case.exchange.hide_close_receipt = True
                self.assertEqual(case.controller.tick()["status"], "CLOSING")
                self.replace_registration(case, {key: value})
                before = len(case.exchange.posts)
                self.reopen(case)
                with patch.object(case.router, "planned_close", side_effect=AssertionError("NO_EXIT_AUTHORITY")), \
                        patch.object(case.router, "emergency_reduce", side_effect=AssertionError("NO_ALTERNATE_EXIT")):
                    self.assertEqual(case.controller.tick()["status"], "HALTED")
                self.assertEqual(len(case.exchange.posts), before)

    def test_missing_duplicate_or_extra_registration_state_never_authorizes_exit_io(self):
        for corruption in ("missing_field", "duplicate_field", "duplicate_rows", "extra_column", "orphan"):
            with self.subTest(corruption=corruption):
                case = self.fixture()
                case.now = self.deadline(case)
                case.exchange.close_unknown = case.exchange.hide_close_receipt = True
                self.assertEqual(case.controller.tick()["status"], "CLOSING")
                with case.controller._db() as db:
                    raw = db.execute("SELECT record FROM natural_exits").fetchone()[0]
                    if corruption == "missing_field":
                        record = json.loads(raw)
                        del record["requested_at_ms"]
                        db.execute("UPDATE natural_exits SET record=?", (json.dumps(record),))
                    elif corruption == "duplicate_field":
                        db.execute("UPDATE natural_exits SET record=?", (raw[:-1] + ',"schema":1}',))
                    elif corruption == "duplicate_rows":
                        db.execute("DROP TABLE natural_exits")
                        db.execute("CREATE TABLE natural_exits (signal TEXT,record TEXT NOT NULL)")
                        db.executemany("INSERT INTO natural_exits VALUES(?,?)", [(case.intent.signal_id, raw)] * 2)
                    elif corruption == "extra_column":
                        db.execute("ALTER TABLE natural_exits ADD COLUMN extra TEXT")
                    else:
                        db.execute("INSERT INTO natural_exits VALUES(?,?)", ("ORPHAN-SIGNAL", raw))
                before = len(case.exchange.posts)
                self.reopen(case)
                with patch.object(case.router, "planned_close", side_effect=AssertionError("NO_EXIT_AUTHORITY")), \
                        patch.object(case.router, "emergency_reduce", side_effect=AssertionError("NO_ALTERNATE_EXIT")):
                    self.assertEqual(case.controller.tick()["status"], "HALTED")
                self.assertEqual(len(case.exchange.posts), before)

    def test_registration_transaction_interruption_retains_protected_row_and_no_post(self):
        case = self.fixture()
        case.now = self.deadline(case)
        with case.controller._db() as db:
            db.execute("CREATE TRIGGER interrupt_natural_exit BEFORE UPDATE OF status ON controller_signals "
                       "WHEN NEW.status='CLOSING' BEGIN SELECT RAISE(ABORT,'simulated transaction interruption'); END")
        before = len(case.exchange.posts)
        with patch.object(case.router, "planned_close", side_effect=AssertionError("NO_EXIT_AUTHORITY")), \
                patch.object(case.router, "emergency_reduce", side_effect=AssertionError("NO_ALTERNATE_EXIT")):
            with SERVICE_ENTRY.service_safety_guard(case.controller):
                result = case.controller.tick()
            self.assertEqual(case.controller.safety_halt("LATER_CLEANUP")["status"], "HALTED")
            self.assertEqual(case.controller.tick()["status"], "HALTED")
        self.assertEqual((result["status"], result["code"]), ("HALTED", "NATURAL_EXIT_REGISTRATION_FAILED"))
        self.assertIsNone(self.registration(case))
        with case.controller._db() as db:
            self.assertEqual(db.execute("SELECT status FROM controller_signals WHERE signal=?",
                                        (case.intent.signal_id,)).fetchone()[0], "PROTECTED")
        self.assertEqual(len(case.exchange.posts), before)
        self.assertEqual(next(iter(case.exchange.algos.values()))["state"], "live")
        self.reopen(case)
        self.assertEqual(case.controller.safety_halt("RESTART_CLEANUP")["status"], "HALTED")
        self.assertEqual(case.controller.tick()["status"], "HALTED")
        self.assertEqual(len(case.exchange.posts), before)

    def test_research_or_technical_id_cannot_be_registered_as_a_natural_exit(self):
        from dataclasses import replace
        for signal_id in ("RSCH-MISLABELLED", "DEMO-TEST-SMOKE-MISLABELLED"):
            with self.subTest(signal_id=signal_id):
                case = self.fixture()
                case.now = self.deadline(case)
                before = len(case.exchange.posts)
                with self.assertRaisesRegex(ValueError, "NATURAL_EXIT_SCOPE_INVALID"):
                    case.controller._begin_natural_exit(replace(case.intent, signal_id=signal_id), "MAX_HOLD_REACHED")
                self.assertIsNone(self.registration(case))
                self.assertEqual(len(case.exchange.posts), before)

    def test_natural_signal_cannot_be_closed_through_explicit_technical_api(self):
        case = self.fixture()
        self.reopen(case, technical=True)
        before = len(case.exchange.posts)
        self.assertEqual(case.controller.close_demo_test(case.intent.signal_id)["code"], "DEMO_TEST_NOT_REGISTERED")
        self.assertEqual(len(case.exchange.posts), before)

    def test_registered_technical_exit_keeps_existing_separate_authorization(self):
        case = self.fixture(technical=True)
        result = case.controller.close_demo_test(case.intent.signal_id)
        self.assertEqual((result["status"], result["code"]), ("CLOSED", "DEMO_TECHNICAL_TEST_EXIT"))
        self.assertIsNone(self.registration(case))
        self.assertEqual(len(self.closes(case)), 1)

    def test_prior_halt_is_never_cleared_or_used_to_authorize_deadline_writes(self):
        case = self.fixture()
        case.now = self.deadline(case)
        case.controller._halt("PRIOR_REVIEW_REQUIRED")
        before = len(case.exchange.posts)
        self.assertEqual(case.controller.tick()["status"], "HALTED")
        self.assertEqual(len(case.exchange.posts), before)
        self.assertIsNone(self.registration(case))
        self.assertTrue(case.controller.halt_path.exists())


if __name__ == "__main__":
    unittest.main()
