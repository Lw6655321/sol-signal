"""Offline service startup/finally recovery using real controller and router."""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
spec = importlib.util.spec_from_file_location("service_halt_recovery_entry", ROOT / "scripts" / "demo_auto_run.py")
ENTRY = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ENTRY)

from src.demo_auto import DemoAutoController
from src.demo_route_transport import DemoRouteTransport
from src.demo_transport import CREDENTIAL_NAMES
from src.execution import build_intent
from src.order_router import OrderRouter
from test_demo_planned_close import PlannedExchange
from test_execution import NOW, inputs


class ObservedExchange(PlannedExchange):
    def __init__(self):
        super().__init__()
        self.gets = []

    def get(self, url, headers, timeout):
        self.gets.append(url)
        return super().get(url, headers, timeout)


class ServiceHaltRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.serial = 0
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIAL_READ")).start()
        patch("src.demo_route_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIAL_READ")).start()

    def fixture(self, *, start="PROTECTED"):
        self.serial += 1
        root = self.root / str(self.serial)
        state_root = root / "state" / "demo-auto"
        signal, state, config = inputs()
        signal.update(id=f"DEMO-SERVICE-TEST-{self.serial}", qty="0.01")
        intent = build_intent(signal, state, config)
        state.update(demo_verified=True, observed_at_ms=NOW, account_hash="a" * 64,
                     week_start_equity="117.72", realized_pnl_week="0")
        exchange = ObservedExchange()
        exchange.pending_entry = start == "PENDING"
        case = SimpleNamespace(root=root, state_root=state_root, signal=signal, intent=intent,
                               state=state, config=config, exchange=exchange)
        self.reopen(case)
        if start != "IDLE":
            payload = {"generated_at_ms": NOW, "gates": {"pass": True}, "new_signals": [signal],
                       "data_fresh": True, "macro_blackout": False}
            self.assertEqual(case.controller.process_scan(payload)["results"][0]["status"], start)
            if start == "PROTECTED":
                self.assertEqual(case.controller.tick()["status"], "PROTECTED")
        self.clear_io(case)
        return case

    def reopen(self, case):
        credentials = dict(zip(CREDENTIAL_NAMES, ("fixture-key", "fixture-secret", "fixture-pass")))
        transport = DemoRouteTransport(credentials, enable_writes=True,
                    http_get=case.exchange.get, http_post=case.exchange.post, log_root=case.root / "logs")
        router = OrderRouter(transport, config=case.config,
                    state_provider=lambda: deepcopy(case.state),
                    market_provider=lambda: {"last": "100", "timestamp_ms": NOW},
                    account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
                    log_root=case.root / "logs", journal_path=case.state_root / "router.sqlite3", clock=lambda: NOW)
        controller = DemoAutoController(case.config, router, lambda market: deepcopy(case.state),
                    lambda: {"last": "100", "timestamp_ms": NOW}, case.root, clock=lambda: NOW)
        case.transport, case.router, case.controller = transport, router, controller

    @staticmethod
    def clear_io(case):
        case.exchange.posts.clear()
        case.exchange.gets.clear()

    def assert_no_io(self, case):
        self.assertEqual(case.exchange.gets, [])
        self.assertEqual(case.exchange.posts, [])

    def old_submitting(self, case):
        case.controller._save(case.intent.signal_id, "SUBMITTING", "SUBMITTING", case.intent)
        with sqlite3.connect(case.router.journal_path) as db:
            raw = db.execute("SELECT data FROM routes WHERE signal=?", (case.intent.signal_id,)).fetchone()[0]
            row = json.loads(raw)
            row.update(status="SUBMITTING", filled_qty="0", protected_qty="0", entry_state="",
                       protection_requested_qty="", protection_order_id="", cancel_requested=False,
                       submit_attempted=True)
            db.execute("UPDATE routes SET status='SUBMITTING',owner='old-owner',data=? WHERE signal=?",
                       (json.dumps(row), case.intent.signal_id))

    @staticmethod
    def owner(case):
        with sqlite3.connect(f"file:{case.router.journal_path.as_posix()}?mode=ro", uri=True) as db:
            return db.execute("SELECT owner FROM routes WHERE signal=?", (case.intent.signal_id,)).fetchone()[0]

    @contextmanager
    def armed(self, case):
        ENTRY.assert_service_recovery_ready(case.state_root)
        with ENTRY.service_safety_guard(case.controller, defer_cleanup=True) as safety:
            ENTRY.assert_service_recovery_ready(case.state_root)
            case.router.recover_demo_owners()
            ENTRY.assert_service_recovery_ready(case.state_root)
            if case.router._local_halted:
                raise RuntimeError("DEMO_SERVICE_RECOVERY_HALTED")
            safety.arm()
            yield safety

    def test_absent_journals_are_fresh_and_are_not_created_by_readonly_gate(self):
        state = self.root / "not-created"
        ENTRY.assert_service_recovery_ready(state)
        self.assertFalse(state.exists())

    def test_initialized_idle_journals_are_accepted_without_account_io(self):
        case = self.fixture(start="IDLE")
        ENTRY.assert_service_recovery_ready(case.state_root)
        self.assert_no_io(case)

    def test_router_halt_file_blocks_fresh_submitting_recovery_before_any_io(self):
        case = self.fixture()
        self.old_submitting(case)
        case.router.halt_path.write_text("ENTRY_REFUSAL_PERSIST_FAILED\n", encoding="utf-8")
        self.reopen(case)
        self.assertEqual(case.router._entry_not_sent_fingerprints, {})
        self.assertEqual(case.controller.status()["status"], "SUBMITTING")
        with self.assertRaisesRegex(RuntimeError, "RECOVERY_HALTED"):
            ENTRY.assert_service_recovery_ready(case.state_root)
        self.assertEqual(self.owner(case), "old-owner")
        self.assertTrue(case.router.halt_path.exists())
        self.assert_no_io(case)

    def test_router_controls_without_halt_file_also_block_before_credentials(self):
        case = self.fixture()
        self.old_submitting(case)
        with sqlite3.connect(case.router.journal_path) as db:
            db.execute("INSERT INTO controls VALUES(?,?)", (case.router.account_key, "ENTRY_REFUSAL_PERSIST_FAILED"))
        self.reopen(case)
        with patch.object(ENTRY, "ROOT", case.root), patch("src.util.load_config", return_value=case.config), \
                patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("MUST_NOT_READ")) as credentials:
            with self.assertRaisesRegex(RuntimeError, "RECOVERY_HALTED"):
                ENTRY._run_locked_service(enabled=True, continuous=True)
        credentials.assert_not_called()
        self.assertEqual(self.owner(case), "old-owner")
        self.assert_no_io(case)

    def test_controller_meta_halt_without_file_is_a_startup_barrier(self):
        case = self.fixture()
        self.old_submitting(case)
        with sqlite3.connect(case.controller.journal_path) as db:
            db.execute("INSERT INTO controller_meta VALUES('halt','PREVIOUS_HALT')")
        self.reopen(case)
        with self.assertRaisesRegex(RuntimeError, "RECOVERY_HALTED"):
            ENTRY.assert_service_recovery_ready(case.state_root)
        self.assertEqual(self.owner(case), "old-owner")
        self.assert_no_io(case)

    def test_halted_or_unknown_rows_block_even_without_control_markers(self):
        for target, status in (("controller", "HALTED"), ("controller", "UNKNOWN"),
                               ("router", "HALTED"), ("router", "UNKNOWN")):
            with self.subTest(target=target, status=status):
                case = self.fixture()
                self.old_submitting(case)
                if target == "controller":
                    case.controller._save(case.intent.signal_id, status, "PRIOR_FAILURE")
                else:
                    with sqlite3.connect(case.router.journal_path) as db:
                        raw = db.execute("SELECT data FROM routes").fetchone()[0]
                        row = {**json.loads(raw), "status": status}
                        db.execute("UPDATE routes SET status=?,data=?", (status, json.dumps(row)))
                self.reopen(case)
                with self.assertRaisesRegex(RuntimeError, "RECOVERY_HALTED"):
                    ENTRY.assert_service_recovery_ready(case.state_root)
                self.assertEqual(self.owner(case), "old-owner")
                self.assert_no_io(case)

    def test_corrupt_router_journal_is_rejected_before_any_recovery_or_credentials(self):
        case = self.fixture()
        self.old_submitting(case)
        case.router.journal_path.write_bytes(b"not a sqlite journal")
        with patch.object(ENTRY, "ROOT", case.root), patch("src.util.load_config", return_value=case.config), \
                patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("MUST_NOT_READ")) as credentials:
            with self.assertRaisesRegex(RuntimeError, "JOURNAL_UNREADABLE"):
                ENTRY._run_locked_service(enabled=True, continuous=True)
        credentials.assert_not_called()
        self.assert_no_io(case)

    def test_existing_missing_schema_and_missing_sibling_are_not_fresh_runs(self):
        for missing_schema in (True, False):
            with self.subTest(missing_schema=missing_schema):
                case = self.fixture(start="IDLE")
                case.router.journal_path.unlink()
                if missing_schema:
                    sqlite3.connect(case.router.journal_path).close()
                expected = "JOURNAL_UNREADABLE" if missing_schema else "JOURNAL_INCOMPLETE"
                with self.assertRaisesRegex(RuntimeError, expected):
                    ENTRY.assert_service_recovery_ready(case.state_root)
                self.assertEqual(case.router.journal_path.exists(), missing_schema)
                self.assert_no_io(case)

    def test_unknown_status_or_incomplete_active_intent_is_not_safe_initialization(self):
        for change in ("unknown-status", "missing-intent"):
            with self.subTest(change=change):
                case = self.fixture()
                self.old_submitting(case)
                with sqlite3.connect(case.controller.journal_path) as db:
                    if change == "unknown-status":
                        db.execute("UPDATE controller_signals SET status='UNRECOGNIZED'")
                    else:
                        db.execute("UPDATE controller_signals SET intent=NULL")
                with self.assertRaisesRegex(RuntimeError, "JOURNAL_INVALID"):
                    ENTRY.assert_service_recovery_ready(case.state_root)
                self.assertEqual(self.owner(case), "old-owner")
                self.assert_no_io(case)

    def test_preexisting_controller_halt_cannot_arm_finally_cleanup(self):
        case = self.fixture()
        self.old_submitting(case)
        case.controller._halt("PRIOR_HALT")
        self.reopen(case)
        with self.assertRaisesRegex(RuntimeError, "RECOVERY_HALTED"):
            with ENTRY.service_safety_guard(case.controller):
                self.fail("old HALT must not enter a writable guard")
        self.assertEqual(self.owner(case), "old-owner")
        self.assert_no_io(case)
        self.assertTrue(case.controller.halt_path.exists())

    def test_unarmed_guard_does_not_cleanup_when_owner_validation_fails(self):
        case = self.fixture()
        self.old_submitting(case)
        with sqlite3.connect(case.router.journal_path) as db:
            row = json.loads(db.execute("SELECT data FROM routes").fetchone()[0])
            row["account_hash"] = "b" * 64
            db.execute("UPDATE routes SET data=?", (json.dumps(row),))
        with patch.object(case.controller, "safety_halt", wraps=case.controller.safety_halt) as cleanup:
            with self.assertRaises(Exception):
                with self.armed(case):
                    self.fail("foreign owner must not arm recovery")
        cleanup.assert_not_called()
        self.assertEqual(self.owner(case), "old-owner")
        self.assert_no_io(case)

    def test_armed_owned_protected_exit_still_reduces_once_and_keeps_halt(self):
        case = self.fixture()
        stop = case.state_root / "STOP_NEW_ENTRIES"
        stop.write_text("OPERATOR_STOP\n", encoding="utf-8")
        with self.assertRaises(OSError):
            with self.armed(case):
                raise OSError("offline service output failure")
        closes = [body for _, body in case.exchange.posts if isinstance(body, dict) and body.get("ordType") == "market"]
        self.assertEqual(len(closes), 1)
        self.assertIs(closes[0]["reduceOnly"], True)
        self.assertEqual(case.exchange.positions, [])
        self.assertEqual(case.controller.status()["status"], "HALTED")
        self.assertTrue(case.router.halt_path.exists())
        self.assertEqual(stop.read_text(encoding="utf-8"), "OPERATOR_STOP\n")

    def test_new_halted_status_after_arming_does_not_disable_real_owned_cleanup(self):
        case = self.fixture()
        with self.assertRaises(OSError):
            with self.armed(case):
                case.controller._halt("CURRENT_PROCESS_FAILURE")
                raise OSError("current process failure after arming")
        closes = [body for _, body in case.exchange.posts if isinstance(body, dict) and body.get("ordType") == "market"]
        self.assertEqual(len(closes), 1)
        self.assertEqual(case.exchange.positions, [])
        self.assertEqual(case.controller.status()["status"], "HALTED")

    def test_owned_pending_cancel_race_still_reduces_only_confirmed_fill(self):
        case = self.fixture(start="PENDING")
        case.exchange.fill_entry_on_cancel = "0.01"
        case.exchange.allow_unprotected_close = True
        with self.assertRaises(OSError):
            with self.armed(case):
                raise OSError("pending service output failure")
        cancels = [url for url, _ in case.exchange.posts if url.endswith("/trade/cancel-order")]
        closes = [body for _, body in case.exchange.posts if isinstance(body, dict) and body.get("ordType") == "market"]
        self.assertEqual(len(cancels), 1)
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]["sz"], "0.01")
        self.assertIs(closes[0]["reduceOnly"], True)
        self.assertEqual(case.exchange.positions, [])

    def test_planned_closing_exit_keeps_existing_checkpoint_without_emergency_post(self):
        case = self.fixture()
        case.exchange.hide_close_receipt = True
        self.assertEqual(case.router.planned_close(case.intent).status, "CLOSING")
        case.controller._save(case.intent.signal_id, "CLOSING", "DEMO_TECHNICAL_TEST_EXIT", case.intent)
        self.clear_io(case)
        with patch.object(case.router, "emergency_reduce", wraps=case.router.emergency_reduce) as emergency:
            with self.assertRaises(OSError):
                with self.armed(case):
                    raise OSError("planned close output failure")
        emergency.assert_not_called()
        self.assert_no_io(case)
        self.assertEqual(case.controller.status()["status"], "CLOSING")

    def test_idle_normal_exit_needs_no_cleanup_or_halt(self):
        case = self.fixture(start="IDLE")
        with patch.object(case.controller, "safety_halt", wraps=case.controller.safety_halt) as cleanup:
            with self.armed(case):
                pass
        cleanup.assert_not_called()
        self.assert_no_io(case)
        self.assertFalse(case.controller.halt_path.exists())
