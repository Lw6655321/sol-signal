"""Independent execution boundary regressions; no credentials or network IO."""
import importlib.util
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch
from urllib.parse import parse_qs, urlsplit

from src.demo_auto import DemoAutoController
from src.demo_route_transport import DemoRouteTransport
from src.demo_transport import CREDENTIAL_NAMES
from src.execution import build_intent
from src.order_router import OrderRouter, client_order_id
from test_demo_router_integration import Exchange
from test_demo_auto import FakeRisk
from test_execution import NOW, inputs


PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("independent_demo_auto_entry", PROJECT / "scripts" / "demo_auto_run.py")
ENTRY = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ENTRY)


class IndependentDemoExecutionReview(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NETWORK_FORBIDDEN")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("CREDENTIALS_FORBIDDEN")).start()
        self.root = Path(self.temp.name)
        signal, self.state, self.config = inputs()
        self.intent = build_intent(signal, self.state, self.config)
        self.state.update(demo_verified=True, observed_at_ms=NOW, account_hash="a" * 64)
        self.exchange = Exchange()
        self.transport = self.make_transport()
        self.router = self.make_router("a" * 64)

    def make_transport(self):
        credentials = dict(zip(CREDENTIAL_NAMES, ("fixture-key", "fixture-secret", "fixture-pass")))
        return DemoRouteTransport(credentials, enable_writes=True,
                   http_get=self.exchange.get, http_post=self.exchange.post, log_root=self.root)

    def make_router(self, identity):
        return OrderRouter(self.transport, config=self.config,
                           state_provider=lambda: deepcopy(self.state),
                           market_provider=lambda: {"last": "100", "timestamp_ms": NOW},
                           account_key="DEMO-AUTO-SOL-V1", account_identity=identity,
                           log_root=self.root, journal_path=self.root / "router.sqlite3", clock=lambda: NOW)

    def test_closed_route_emergency_cannot_reduce_the_next_position(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        self.exchange.positions, self.exchange.algos = [], {}
        self.assertEqual(self.router.reconcile_exit(self.intent).status, "CLOSED")
        next_intent = replace(self.intent, signal_id="NEXT-SIGNAL")
        self.assertEqual(self.router.execute(next_intent).status, "PROTECTED")
        before = len(self.exchange.posts)
        self.router.emergency_reduce(self.intent, "LATE_FAILURE")
        self.assertEqual(len(self.exchange.posts), before, "a CLOSED route must never reduce a later position")
        self.assertTrue(self.exchange.positions, "the second position must remain untouched")

    def test_existing_route_account_hash_must_match_before_any_emergency(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        other_binding = self.make_router("b" * 64)
        before = len(self.exchange.posts)
        result = other_binding.emergency_reduce(self.intent, "SERVICE_FAILURE")
        self.assertIn(result.status, {"UNKNOWN", "HALTED"})
        self.assertEqual(len(self.exchange.posts), before, "foreign account binding cannot touch this route")

    def test_existing_route_account_hash_must_match_before_exit_release(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        other_binding = self.make_router("b" * 64)
        self.exchange.positions, self.exchange.algos = [], {}
        result = other_binding.reconcile_exit(self.intent)
        self.assertNotEqual(result.status, "CLOSED", "a different bound account cannot clear the old slot")

    def test_crashed_owner_recovery_cannot_leave_observed_fill_unprotected(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        # Simulate the durable checkpoint of a hard-killed process: the
        # exchange accepted/filled entry, but local owner never released and
        # protection never reached the exchange. No process is running here.
        self.exchange.algos = {}
        with sqlite3.connect(self.router.journal_path) as db:
            raw = db.execute("SELECT data FROM routes WHERE signal=?", (self.intent.signal_id,)).fetchone()[0]
            row = json.loads(raw)
            row.update(status="SUBMITTING", filled_qty="0", protected_qty="0", entry_state="",
                       protection_requested_qty="", protection_order_id="")
            db.execute("UPDATE routes SET owner='crashed-owner',status=?,data=? WHERE signal=?",
                       (row["status"], json.dumps(row), self.intent.signal_id))
        self.transport = self.make_transport()
        restarted = self.make_router("a" * 64)
        with ENTRY.process_lock(self.root / "process.lock"):
            restarted.recover_demo_owners()
            result = restarted.reconcile_existing(self.intent)
        self.assertTrue(self.exchange.algos or not self.exchange.positions,
                        "a locked stale owner cannot be left with an unprotected exchange fill")
        self.assertNotEqual(result.code, "IN_PROGRESS", "operator recovery must be explicit and available before unattended release")
        entry_posts = [body for _, body in self.exchange.posts if body.get("ordType") in {"limit", "post_only"}]
        self.assertEqual(len(entry_posts), 1, "recovery must never post a second entry")

    def test_recovery_checks_entire_owner_set_before_releasing_any_owner(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        with sqlite3.connect(self.router.journal_path) as db:
            raw = db.execute("SELECT data FROM routes WHERE signal=?", (self.intent.signal_id,)).fetchone()[0]
            db.execute("UPDATE routes SET owner='old-owner'")
            foreign = json.loads(raw)
            foreign.update(signal_id="FOREIGN-ROUTE", client_order_id=client_order_id("FOREIGN-ROUTE"),
                           account_hash="b" * 64)
            db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
                       (self.router.account_key, foreign["signal_id"], "fixture-fingerprint",
                        foreign["client_order_id"], foreign["status"], "foreign-owner", json.dumps(foreign)))
        writes = len(self.exchange.posts)
        with ENTRY.process_lock(self.root / "process.lock"):
            with self.assertRaises(Exception):
                self.router.recover_demo_owners()
        with sqlite3.connect(self.router.journal_path) as db:
            owner = db.execute("SELECT owner FROM routes WHERE signal=?", (self.intent.signal_id,)).fetchone()[0]
        self.assertEqual(owner, "old-owner", "a bad sibling route must roll back the whole recovery")
        self.assertEqual(len(self.exchange.posts), writes)

    def test_owner_recovery_never_clears_unknown_state_or_safety_halt(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        with sqlite3.connect(self.router.journal_path) as db:
            raw = db.execute("SELECT data FROM routes WHERE signal=?", (self.intent.signal_id,)).fetchone()[0]
            row = json.loads(raw)
            row.update(status="UNKNOWN", code="CLOSE_UNCONFIRMED")
            db.execute("UPDATE routes SET owner='old-owner',status=?,data=? WHERE signal=?",
                       (row["status"], json.dumps(row), self.intent.signal_id))
            db.execute("INSERT INTO controls VALUES(?,?)", (self.router.account_key, "CLOSE_UNCONFIRMED"))
        writes = len(self.exchange.posts)
        with ENTRY.process_lock(self.root / "process.lock"):
            self.assertEqual(self.router.recover_demo_owners(), 1)
            self.assertEqual(self.router.reconcile_existing(self.intent).status, "UNKNOWN")
        with sqlite3.connect(self.router.journal_path) as db:
            row = db.execute("SELECT owner,status FROM routes WHERE signal=?", (self.intent.signal_id,)).fetchone()
            halt = db.execute("SELECT code FROM controls WHERE account=?", (self.router.account_key,)).fetchone()
        self.assertEqual(row, ("", "UNKNOWN"))
        self.assertEqual(halt[0], "CLOSE_UNCONFIRMED")
        self.assertEqual(len(self.exchange.posts), writes)

    def test_halted_close_ack_loss_is_later_confirmed_without_repeating_market_post(self):
        hidden = {"close": True}
        original_get, original_post = self.exchange.get, self.exchange.post

        def get(url, headers, timeout):
            query = parse_qs(urlsplit(url).query)
            if urlsplit(url).path.endswith("/trade/order"):
                row = self.exchange.orders.get(query["clOrdId"][0])
                if row and row["ordType"] == "market" and hidden["close"]:
                    raise TimeoutError("fixture temporarily cannot observe close")
            return original_get(url, headers, timeout)

        def post(url, headers, body, timeout):
            data = json.loads(body)
            outcome = original_post(url, headers, body, timeout)
            if data.get("ordType") == "market":
                raise TimeoutError("fixture close filled but ACK was lost")
            return outcome

        self.exchange.get, self.exchange.post = get, post
        self.transport = self.make_transport()
        self.router = self.make_router("a" * 64)
        risk = FakeRisk()
        risk.state["account_hash"] = "a" * 64
        controller = DemoAutoController(self.config, self.router, risk,
                     lambda: {"last": "100", "timestamp_ms": NOW}, self.root, clock=lambda: NOW)
        signal, _, _ = inputs()
        payload = {"generated_at_ms": NOW, "gates": {"pass": True}, "new_signals": [signal],
                   "data_fresh": True, "macro_blackout": False}
        self.assertEqual(controller.process_scan(payload)["results"][0]["status"], "PROTECTED")
        controller.safety_halt("SERVICE_IO_FAILURE")
        with sqlite3.connect(self.router.journal_path) as db:
            raw = db.execute("SELECT data FROM routes WHERE signal=?", (signal["id"],)).fetchone()[0]
        self.assertEqual(json.loads(raw)["emergency_status"], "CLOSE_UNCONFIRMED")
        writes = len(self.exchange.posts)
        hidden["close"] = False
        result = controller.tick()
        self.assertEqual(result["status"], "HALTED", "new entries remain disabled after recovery")
        self.assertEqual(result.get("emergency_status"), "FLAT_CONFIRMED")
        self.assertEqual(len(self.exchange.posts), writes, "unknown ACK recovery is read-only, not a second POST")
        self.assertEqual(len([body for _, body in self.exchange.posts if body.get("ordType") == "market"]), 1)

    def test_lifecycle_halt_and_service_exit_cannot_flatten_replacement_sol_position(self):
        risk = FakeRisk()
        risk.state["account_hash"] = "a" * 64
        controller = DemoAutoController(self.config, self.router, risk,
                     lambda: {"last": "100", "timestamp_ms": NOW}, self.root, clock=lambda: NOW)
        signal, _, _ = inputs()
        payload = {"generated_at_ms": NOW, "gates": {"pass": True}, "new_signals": [signal],
                   "data_fresh": True, "macro_blackout": False}
        self.assertEqual(controller.process_scan(payload)["results"][0]["status"], "PROTECTED")
        self.assertEqual(controller.tick()["status"], "PROTECTED")  # bind observed posId/cTime
        self.exchange.positions[0].update(posId="external-replacement-position")
        writes = len(self.exchange.posts)
        self.assertEqual(controller.tick()["status"], "HALTED")
        controller.safety_halt("SERVICE_STOPPED")
        controller.tick()
        self.assertEqual(len(self.exchange.posts), writes, "HALT recovery cannot touch a different SOL lifecycle")
        self.assertEqual(self.exchange.positions[0]["posId"], "external-replacement-position")

    def test_emergency_fill_arithmetic_does_not_prove_current_account_flat(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        original_post = self.exchange.post

        def post(url, headers, body, timeout):
            result = original_post(url, headers, body, timeout)
            if json.loads(body).get("ordType") == "market":
                self.exchange.positions = [{"instId": "BTC-USDT-SWAP", "pos": "1",
                    "posSide": "net", "mgnMode": "cross", "posId": "external-btc", "cTime": str(NOW)}]
            return result

        self.transport._http_post = post
        result = self.router.emergency_reduce(self.intent, "SERVICE_FAILURE")
        self.assertEqual(result.status, "HALTED")
        self.assertNotEqual(result.emergency_status, "FLAT_CONFIRMED",
                            "entry and close quantities cannot replace a current full-account read")

    def test_no_position_with_residual_algo_is_not_flat_clean_confirmation(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        self.exchange.positions = []
        before = len(self.exchange.posts)
        result = self.router.emergency_reduce(self.intent, "SERVICE_FAILURE")
        self.assertNotEqual(result.emergency_status, "FLAT_CONFIRMED",
                            "an orphaned protection order must remain an explicit unresolved state")
        self.assertEqual(len(self.exchange.posts), before)

    def test_resume_never_reuses_a_stale_flat_confirmation_for_new_exposure(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        result = self.router.emergency_reduce(self.intent, "SERVICE_FAILURE")
        self.assertEqual(result.emergency_status, "FLAT_CONFIRMED")
        self.exchange.positions = [{"instId": "BTC-USDT-SWAP", "pos": "1",
            "posSide": "net", "mgnMode": "cross", "posId": "external-btc", "cTime": str(NOW)}]
        before = len(self.exchange.posts)
        receipt = self.router.resume_emergency(self.intent)
        self.assertNotEqual(receipt.emergency_status, "FLAT_CONFIRMED")
        self.assertEqual(len(self.exchange.posts), before, "HALT re-observation is strictly read-only")

    def _service_fixture(self):
        calls = []
        controller = SimpleNamespace(status=lambda: {"status": "PENDING", "code": "PENDING"},
                                     safety_halt=lambda code: calls.append(code))
        client = SimpleNamespace(get=lambda *args, **kwargs: [{"uid": "123", "perm": "read_only"}])
        return controller, client, calls

    def _run_stubbed_service(self, *, enabled, controller, client):
        with ExitStack() as stack:
            stack.enter_context(patch.object(ENTRY, "ROOT", self.root))
            stack.enter_context(patch("src.demo_transport.load_demo_credentials", return_value={}))
            stack.enter_context(patch("src.demo_transport.DemoReadOnlyClient", return_value=client))
            stack.enter_context(patch("src.demo_route_transport.DemoRouteTransport", return_value=SimpleNamespace()))
            provider = SimpleNamespace(snapshot=lambda *args, **kwargs: {
                "demo_verified": False, "refusal_reasons": ["FIXTURE_UNVERIFIED"]})
            stack.enter_context(patch("src.demo_risk.DemoRiskProvider", return_value=provider))
            stack.enter_context(patch("src.collect.collect", return_value={}))
            router_factory = stack.enter_context(patch("src.order_router.OrderRouter",
                                return_value=SimpleNamespace(recover_demo_owners=lambda: None)))
            controller_factory = stack.enter_context(patch("src.demo_auto.DemoAutoController", return_value=controller))
            stack.enter_context(patch("src.util.load_config", return_value=self.config))
            stack.enter_context(redirect_stdout(io.StringIO()))
            result = ENTRY._run_locked_service(enabled=enabled, continuous=False)
            if not enabled:
                router_factory.assert_not_called()
                controller_factory.assert_not_called()
            return result

    def test_diagnostic_mode_never_changes_active_controller_halt(self):
        controller, client, calls = self._service_fixture()
        self.assertEqual(self._run_stubbed_service(enabled=False, controller=controller, client=client), 0)
        self.assertEqual(calls, [], "read-only diagnostics must not poison persistent execution state")

    def test_corrupt_optional_scan_state_is_inside_enabled_safety_guard(self):
        controller, client, calls = self._service_fixture()
        client.get = lambda *args, **kwargs: [{"uid": "123", "perm": "read_only,trade"}]
        state_dir = self.root / "state" / "demo-auto"
        state_dir.mkdir(parents=True)
        (state_dir / "scan-seen.json").write_text("{invalid", encoding="utf-8")
        # This test exercises cleanup after the already-activated startup gate.
        # Real activation acceptance/rejection is verified in its own suite.
        with patch("src.demo_successor_gate.assert_activated_generation") as activation, \
                self.assertRaises(Exception):
            self._run_stubbed_service(enabled=True, controller=controller, client=client)
        self.assertEqual(activation.call_args_list, [call(self.root), call(self.root)])
        self.assertTrue(calls, "startup must cancel/reconcile owned pending routes before propagating corrupt optional JSON")


if __name__ == "__main__":
    unittest.main()
