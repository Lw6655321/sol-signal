"""Real router/DEMO transport admission contracts over offline HTTP fixtures."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from src.demo_route_transport import DemoEntryNotSent, DemoRouteError, DemoRouteTransport
from src.demo_auto import DemoAutoController
from src.demo_transport import CREDENTIAL_NAMES
from src.execution import build_intent
from src.order_router import OrderRouter, RouterError
from test_demo_router_integration import Exchange
from test_execution import NOW, inputs


class AdmissionClock:
    def __init__(self):
        self.wall = NOW
        self.mono = 1_000_000_000

    def advance(self, milliseconds, *, wall=True, monotonic=True):
        if wall:
            self.wall += milliseconds
        if monotonic:
            self.mono += milliseconds * 1_000_000


class PassiveExchange(Exchange):
    """Entries rest unfilled, so a normal case performs exactly one POST."""
    def __init__(self):
        super().__init__()
        self.spoof_not_sent_in_http = False
        self.get_calls = 0

    def get(self, url, headers, timeout):
        self.get_calls += 1
        return super().get(url, headers, timeout)

    def post(self, url, headers, body, timeout):
        assert headers["x-simulated-trading"] == "1"
        assert url.startswith("https://openapi.okx.com/api/v5/")
        data = json.loads(body)
        if url.endswith("/trade/order") and data["ordType"] == "post_only":
            self.posts.append((url, data))
            if self.spoof_not_sent_in_http:
                raise DemoEntryNotSent("MARKET_STALE")
            row = dict(data, ordId="test-order-" + str(len(self.orders) + 1),
                       state="live", accFillSz="0", avgPx="", cTime=str(NOW), uTime=str(NOW))
            self.orders[data["clOrdId"]] = row
            return 200, {"code": "0", "data": [{"sCode": "0", "ordId": row["ordId"]}]}
        if url.endswith("/trade/cancel-order"):
            self.posts.append((url, data))
            row = self.orders.get(data["clOrdId"])
            if row is not None:
                row["state"] = "canceled"
            return 200, {"code": "0", "data": [{"sCode": "0", "ordId": "test-cancel"}]}
        return super().post(url, headers, body, timeout)


class DemoEntryAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.serial = 0
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_route_transport.load_demo_credentials",
              side_effect=AssertionError("NO_CREDENTIAL_READ")).start()
        patch("src.demo_transport.load_demo_credentials",
              side_effect=AssertionError("NO_CREDENTIAL_READ")).start()
        self.clock = AdmissionClock()
        patch("src.order_router.time.monotonic_ns", side_effect=lambda: self.clock.mono).start()

    def fixture(self, *, side="long"):
        self.serial += 1
        root = self.root / str(self.serial)
        signal, state, config = inputs()
        signal.update(id=f"DEMO-TEST-SMOKE-ADMISSION-{self.serial}", qty="0.01", require_maker=True)
        if side == "short":
            signal.update(side="short", stop="101", tp1="97", tp2="96")
        intent = build_intent(signal, state, config)
        state.update(demo_verified=True, observed_at_ms=NOW, account_hash="a" * 64)
        book = {"last": "100", "timestamp_ms": NOW, "bid": "99.99", "ask": "100.01"}
        exchange = PassiveExchange()
        credentials = dict(zip(CREDENTIAL_NAMES, ("fixture-key", "fixture-secret", "fixture-pass")))
        transport = DemoRouteTransport(credentials, enable_writes=True,
                        http_get=exchange.get, http_post=exchange.post, log_root=root)
        router = OrderRouter(transport, config=config,
                        state_provider=lambda: deepcopy(state), market_provider=lambda: deepcopy(book),
                        account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
                        log_root=root, journal_path=root / "router.sqlite3", clock=lambda: self.clock.wall)
        return SimpleNamespace(root=root, signal=signal, state=state, config=config, intent=intent,
                               book=book, exchange=exchange, transport=transport, router=router)

    def route(self, case):
        with sqlite3.connect(f"file:{case.router.journal_path.as_posix()}?mode=ro", uri=True) as db:
            row = db.execute("SELECT status,data,owner FROM routes WHERE signal=?", (case.intent.signal_id,)).fetchone()
        if row is None:
            return None
        return {"status": row[0], "data": json.loads(row[1]), "owner": row[2]}

    def assert_no_write(self, case):
        self.assertEqual(case.exchange.posts, [])
        self.assertEqual(case.exchange.get_calls, 0)

    def assert_local_rejection(self, case, result, code, *, persisted=True):
        self.assertEqual((result.status, result.code), ("REJECTED", code))
        self.assert_no_write(case)
        self.assertFalse(case.router.halt_path.exists())
        self.assertFalse(case.router._local_halted)
        row = self.route(case)
        if persisted:
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "REJECTED")
            self.assertEqual(row["owner"], "")
        else:
            self.assertIsNone(row)

    def test_long_and_short_crossing_opposite_quote_are_refused_without_http(self):
        for side, book in (("long", {"bid": "99.99", "ask": "100"}),
                           ("long", {"bid": "99.98", "ask": "99.99"}),
                           ("short", {"bid": "100", "ask": "100.01"}),
                           ("short", {"bid": "100.01", "ask": "100.02"})):
            with self.subTest(side=side, book=book):
                case = self.fixture(side=side)
                original = case.intent.to_dict()
                case.book.update(book)
                result = case.router.execute(case.intent)
                self.assert_local_rejection(case, result, "MAKER_QUOTE_MOVED", persisted=False)
                self.assertEqual(case.intent.to_dict(), original)

    def test_quote_moves_only_at_second_preflight_releases_reserved_slot(self):
        case = self.fixture()
        moved = {**case.book, "bid": "99.99", "ask": "100"}
        provider = Mock(side_effect=[deepcopy(case.book), moved])
        case.router.market_provider = provider
        result = case.router.execute(case.intent)
        self.assertEqual(provider.call_count, 2)
        self.assert_local_rejection(case, result, "MAKER_QUOTE_MOVED")
        self.assertFalse(self.route(case)["data"]["submit_attempted"])
        case.router.market_provider = lambda: deepcopy(case.book)
        next_signal = {**case.signal, "id": case.signal["id"] + "-NEXT"}
        next_intent = build_intent(next_signal, case.state, case.config)
        self.assertEqual(case.router.execute(next_intent).status, "PENDING")
        self.assertEqual(len(case.exchange.posts), 1)

    def test_real_quote_timestamp_over_five_seconds_is_refused_despite_config_ttl(self):
        case = self.fixture()
        self.assertGreater(case.config["data"]["stale_seconds"]["ticker"], 5)
        case.book["timestamp_ms"] = NOW - 5001
        self.assert_local_rejection(case, case.router.execute(case.intent), "MARKET_STALE", persisted=False)

    def test_quote_requires_integer_source_timestamp_and_rejects_future(self):
        for stamp in (str(NOW), float(NOW), NOW + 1):
            with self.subTest(stamp_type=type(stamp).__name__):
                case = self.fixture()
                case.book["timestamp_ms"] = stamp
                self.assert_local_rejection(case, case.router.execute(case.intent), "MARKET_STALE", persisted=False)

    def test_invalid_or_missing_book_is_not_replaced_by_last_price(self):
        for changes in ({"bid": None}, {"ask": "NaN"}, {"bid": "100.02", "ask": "100.01"}):
            with self.subTest(changes=changes):
                case = self.fixture()
                case.book.update(changes)
                self.assert_local_rejection(case, case.router.execute(case.intent), "MAKER_QUOTE_INVALID", persisted=False)

    def _delay(self, case, where, milliseconds):
        fired = False
        def once():
            nonlocal fired
            if not fired:
                fired = True
                self.clock.advance(milliseconds)
        if where == "save":
            original = case.router._save
            def save(row):
                if row.get("submit_attempted") and row.get("status") == "SUBMITTING":
                    once()
                return original(row)
            return patch.object(case.router, "_save", side_effect=save)
        if where == "router_audit":
            original = case.router._audit
            def audit(row, event):
                if event == "entry_request":
                    once()
                return original(row, event)
            return patch.object(case.router, "_audit", side_effect=audit)
        if where == "transport_audit":
            original = case.transport._audit
            def audit(action, identity, ok, code, numeric=None):
                if action == "entry" and code == "REQUEST":
                    once()
                return original(action, identity, ok, code, numeric)
            return patch.object(case.transport, "_audit", side_effect=audit)
        if where == "signature":
            original = case.transport._reader._headers
            def headers(method, request_path, body=""):
                if method == "POST":
                    once()
                return original(method, request_path, body)
            return patch.object(case.transport._reader, "_headers", side_effect=headers)
        raise AssertionError("unknown delay point")

    def test_save_audit_and_signature_delays_cannot_submit_expired_account_evidence(self):
        for where in ("save", "router_audit", "transport_audit", "signature"):
            with self.subTest(where=where):
                self.clock = AdmissionClock()
                case = self.fixture()
                with self._delay(case, where, 5001):
                    result = case.router.execute(case.intent)
                self.assert_local_rejection(case, result, "DEMO_STATE_STALE")
                row = self.route(case)["data"]
                self.assertTrue(row["submit_attempted"], "keep the durable attempt checkpoint")
                self.assertIs(row["entry_not_sent"], True)
                self.assertEqual(row["entry_not_sent_code"], "DEMO_STATE_STALE")
                self.assertEqual(case.transport._attempts, {})

    def test_final_transport_gate_also_rejects_only_quote_expiry(self):
        case = self.fixture()
        case.book["timestamp_ms"] = NOW - 4000
        with self._delay(case, "signature", 1001):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "MARKET_STALE")

    def test_final_not_sent_refusal_is_cached_but_releases_slot_for_new_plan(self):
        case = self.fixture()
        with self._delay(case, "signature", 5001):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "DEMO_STATE_STALE")
        # A later fresh quote never authorizes rewriting/replaying the old plan.
        case.book["timestamp_ms"] = self.clock.wall
        case.state.update(observed_at_ms=self.clock.wall, now_ms=self.clock.wall)
        self.assertEqual(case.router.execute(case.intent).status, "REJECTED")
        self.assert_no_write(case)
        next_signal = {**case.signal, "id": case.signal["id"] + "-AFTER-REFUSAL"}
        next_intent = build_intent(next_signal, case.state, case.config)
        self.assertEqual(case.router.execute(next_intent).status, "PENDING")
        self.assertEqual(len(case.exchange.posts), 1)
        self.assertEqual(self.route(case)["status"], "REJECTED")

    def test_exact_five_second_source_boundary_is_allowed(self):
        for source in ("account", "quote", "both"):
            with self.subTest(source=source):
                self.clock = AdmissionClock()
                case = self.fixture()
                if source in {"account", "both"}:
                    case.state["observed_at_ms"] = NOW - 5000
                if source in {"quote", "both"}:
                    case.book["timestamp_ms"] = NOW - 5000
                self.assertEqual(case.router.execute(case.intent).status, "PENDING")
                self.assertEqual(len(case.exchange.posts), 1)

    def test_one_millisecond_past_final_boundary_is_rejected(self):
        case = self.fixture()
        case.state["observed_at_ms"] = NOW - 5000
        with self._delay(case, "signature", 1):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "DEMO_STATE_STALE")

    def test_final_gate_at_exact_five_seconds_elapsed_allows_one_post(self):
        case = self.fixture()
        with self._delay(case, "signature", 5000):
            result = case.router.execute(case.intent)
        self.assertEqual(result.status, "PENDING")
        self.assertEqual(len(case.exchange.posts), 1)

    def test_wall_clock_rollback_cannot_refresh_evidence(self):
        case = self.fixture()
        original = case.transport._reader._headers
        def headers(method, request_path, body=""):
            if method == "POST":
                self.clock.wall -= 1
                self.clock.mono += 1_000_000
            return original(method, request_path, body)
        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "DEMO_ENTRY_CLOCK_INVALID")

    def test_monotonic_deadline_expires_even_if_wall_clock_does_not_advance(self):
        case = self.fixture()
        original = case.transport._reader._headers
        def headers(method, request_path, body=""):
            if method == "POST":
                self.clock.advance(5001, wall=False)
            return original(method, request_path, body)
        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "DEMO_STATE_STALE")

    def test_monotonic_rollback_is_refused(self):
        case = self.fixture()
        original = case.transport._reader._headers
        def headers(method, request_path, body=""):
            if method == "POST":
                self.clock.mono -= 1
            return original(method, request_path, body)
        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "DEMO_ENTRY_CLOCK_INVALID")

    def test_expired_intent_during_local_work_is_not_sent(self):
        case = self.fixture()
        case.intent = build_intent({**case.signal, "expires_at_ms": NOW + 1000}, case.state, case.config)
        with self._delay(case, "signature", 1000):
            result = case.router.execute(case.intent)
        self.assert_local_rejection(case, result, "EXPIRED")

    def test_admission_is_bound_to_the_exact_immutable_intent(self):
        case = self.fixture()
        admission = case.router._preflight(case.intent)
        with self.assertRaises(RouterError) as caught:
            case.router._check_demo_entry_admission(replace(case.intent), admission)
        self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")
        self.assert_no_write(case)

    def test_fake_not_sent_exception_after_http_entry_remains_unknown_and_is_not_retried(self):
        case = self.fixture()
        case.exchange.spoof_not_sent_in_http = True
        result = case.router.execute(case.intent)
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.code, "SUBMISSION_UNKNOWN")
        entries = [body for _, body in case.exchange.posts if body.get("ordType") == "post_only"]
        self.assertEqual(len(entries), 1)
        row = self.route(case)["data"]
        self.assertTrue(row["submit_attempted"])
        self.assertFalse(row.get("entry_not_sent", False))
        self.assertNotIn(case.intent.signal_id, case.router._entry_not_sent_fingerprints)
        self.assertTrue(case.router.halt_path.exists())
        count = len(case.exchange.posts)
        self.assertEqual(case.router.execute(case.intent).status, "UNKNOWN")
        self.assertEqual(len(case.exchange.posts), count)

    def test_transport_cannot_reclassify_http_callback_failure_as_zero_send(self):
        case = self.fixture()
        case.exchange.spoof_not_sent_in_http = True
        with self.assertRaises(DemoRouteError) as caught:
            case.transport.submit_entry(case.intent, "OfflineAdmissionEntry", before_send=lambda: None)
        self.assertNotIsInstance(caught.exception, DemoEntryNotSent)
        self.assertEqual(caught.exception.code, "DEMO_WRITE_UNKNOWN")
        with self.assertRaises(DemoRouteError) as repeated:
            case.transport.submit_entry(case.intent, "OfflineAdmissionEntry", before_send=lambda: None)
        self.assertEqual(repeated.exception.code, "DEMO_WRITE_UNKNOWN")
        self.assertEqual(len(case.exchange.posts), 1)

    def test_refusal_persistence_failure_halts_locally_without_cancel_or_close(self):
        case = self.fixture()
        original_save = case.router._save
        def fail_refusal(row):
            if row.get("entry_not_sent"):
                raise OSError("offline refusal persistence fault")
            return original_save(row)
        with self._delay(case, "signature", 5001), patch.object(case.router, "_save", side_effect=fail_refusal):
            result = case.router.execute(case.intent)
        self.assertEqual((result.status, result.code), ("HALTED", "ENTRY_REFUSAL_PERSIST_FAILED"))
        self.assert_no_write(case)
        self.assertTrue(case.router._local_halted)
        self.assertTrue(case.router.halt_path.exists())
        with sqlite3.connect(f"file:{case.router.journal_path.as_posix()}?mode=ro", uri=True) as db:
            self.assertEqual(db.execute("SELECT code FROM controls").fetchone()[0], "ENTRY_REFUSAL_PERSIST_FAILED")
        self.assertEqual(self.route(case)["owner"], "")

    def test_normal_long_and_short_send_original_price_quantity_and_maker_once(self):
        for side in ("long", "short"):
            with self.subTest(side=side):
                case = self.fixture(side=side)
                original = case.intent.to_dict()
                self.assertEqual(case.router.execute(case.intent).status, "PENDING")
                self.assertEqual(case.intent.to_dict(), original)
                self.assertEqual(len(case.exchange.posts), 1)
                _, body = case.exchange.posts[0]
                self.assertEqual(body["px"], str(case.intent.entry))
                self.assertEqual(body["sz"], str(case.intent.qty))
                self.assertEqual(body["ordType"], "post_only")
                self.assertEqual(body["side"], "buy" if side == "long" else "sell")
                self.assertEqual(self.route(case)["status"], "PENDING")

    def _controller(self, case):
        case.state.update(week_start_equity="117.72", realized_pnl_week="0")
        return DemoAutoController(case.config, case.router,
                    lambda market: deepcopy(case.state), lambda: deepcopy(case.book),
                    case.root, clock=lambda: self.clock.wall, enable_demo_tests=True)

    @staticmethod
    def _plan(case):
        return {"purpose": "DEMO_TECHNICAL_TEST", "scenario": "smoke",
                "authorization_id": "user-approved-demo-technical-tests-20261019",
                "signal": deepcopy(case.signal)}

    @contextmanager
    def _failed_refusal_persistence(self, case, controller=None):
        original_router_save = case.router._save
        def fail_router(row):
            if row.get("entry_not_sent"):
                raise OSError("offline refusal persistence failure")
            return original_router_save(row)
        with self._delay(case, "signature", 5001), patch.object(case.router, "_save", side_effect=fail_router):
            if controller is None:
                yield
            else:
                original_controller_save = controller._save
                def fail_controller(signal_id, status, code, intent=None):
                    if status == "HALTED":
                        raise OSError("offline controller status persistence failure")
                    return original_controller_save(signal_id, status, code, intent)
                with patch.object(controller, "_save", side_effect=fail_controller):
                    yield

    def _assert_controller_halted_with_old_submitting(self, controller, signal_id):
        self.assertEqual(controller.status()["status"], "HALTED")
        self.assertTrue(controller.halt_path.exists())
        with sqlite3.connect(f"file:{controller.journal_path.as_posix()}?mode=ro", uri=True) as db:
            self.assertIsNotNone(db.execute("SELECT value FROM controller_meta WHERE key='halt'").fetchone())
            # The cleanup must tolerate this exact older durable state.
            self.assertEqual(db.execute("SELECT status FROM controller_signals WHERE signal=?",
                                        (signal_id,)).fetchone()[0], "SUBMITTING")

    def test_real_controller_cleanup_restores_intent_and_keeps_not_sent_fault_local(self):
        case = self.fixture()
        controller = self._controller(case)
        market = {"generated_at_ms": NOW, "data_fresh": True, "macro_blackout": False}
        with self._failed_refusal_persistence(case, controller), \
                patch.object(case.router, "emergency_reduce", wraps=case.router.emergency_reduce) as reduce:
            result = controller.process_demo_test(self._plan(case), market)
            self.assertEqual(result["status"], "HALTED")
            self.assert_no_write(case)
            self._assert_controller_halted_with_old_submitting(controller, case.intent.signal_id)
            cleaned = controller.safety_halt("ENTRY_HALTED")
            self.assertEqual(cleaned["status"], "HALTED")
            self.assertGreaterEqual(reduce.call_count, 2)
            first_intent = reduce.call_args_list[0].args[0]
            restored_intent = reduce.call_args_list[-1].args[0]
            self.assertIsNot(first_intent, restored_intent)
            self.assertEqual(first_intent.to_dict(), restored_intent.to_dict())
            self.assert_no_write(case)
            self._assert_controller_halted_with_old_submitting(controller, case.intent.signal_id)
            self.assertTrue(case.router.halt_path.exists())

    def test_cli_fail_cleanup_uses_real_controller_without_any_account_io(self):
        from test_demo_technical_test import D, FakeDeps
        case = self.fixture()
        controller = self._controller(case)
        plan = self._plan(case)
        market = {"generated_at_ms": NOW, "data_fresh": True, "macro_blackout": False}
        class CleanupDeps(FakeDeps):
            def build_runtime(self, new_root):
                return {"controller": controller, "router": case.router, "root": str(case.root)}
            def market_fields(self, snapshot):
                return {"evidence": market}
            def resume_or_create_plan(self, new_root, scenario, side, fields, report):
                return deepcopy(plan)
            def start_test(self, runtime, supplied_plan, evidence):
                return controller.process_demo_test(supplied_plan, evidence)
            def safety_halt(self, runtime, code):
                self.halted.append(code)
                return controller.safety_halt(code)
        deps = CleanupDeps(case.root)
        options = SimpleNamespace(scenario="smoke", side="long", run_id="entry-not-sent-cleanup",
                    predecessor_root=case.root / "unused-predecessor", new_root=case.root,
                    entry_wait_seconds=1, observe_seconds=1)
        with self._failed_refusal_persistence(case, controller):
            result = D.execute_test(deps, options)
            self.assertEqual(result["result"], "FAIL")
            report = json.loads(Path(result["report_path"]).read_text(encoding="utf-8"))
            self.assertEqual(report["refusal"]["code"], "ENTRY_HALTED")
            self.assertEqual(deps.halted, ["ENTRY_HALTED"])
            self.assert_no_write(case)
            self._assert_controller_halted_with_old_submitting(controller, case.intent.signal_id)
            self.assertTrue(case.router.halt_path.exists())

    def test_not_sent_cache_accepts_equal_restored_value_without_object_identity(self):
        case = self.fixture()
        with self._failed_refusal_persistence(case):
            self.assertEqual(case.router.execute(case.intent).status, "HALTED")
            restored = replace(case.intent)
            self.assertIsNot(restored, case.intent)
            self.assertEqual(case.router.emergency_reduce(restored, "ENTRY_HALTED").status, "HALTED")
            self.assert_no_write(case)

    def test_same_signal_different_intent_cannot_match_not_sent_emergency_cache(self):
        case = self.fixture()
        with self._failed_refusal_persistence(case):
            self.assertEqual(case.router.execute(case.intent).status, "HALTED")
        cached = case.router._entry_not_sent_fingerprints[case.intent.signal_id]
        different = replace(case.intent, stop=case.intent.stop - Decimal("0.01"))
        self.assertNotEqual(case.router._intent_fingerprint(different), cached)
        # The public route still refuses its changed immutable fingerprint.
        public = case.router.emergency_reduce(different, "ENTRY_HALTED")
        self.assertEqual(public.code, "DEMO_SAFETY_UNKNOWN")
        self.assert_no_write(case)
        # Exercise the cache boundary itself: a different fingerprint cannot
        # take its local-only shortcut. Existing emergency safety logic runs.
        row = self.route(case)["data"]
        self.assertFalse(row.get("cancel_requested"))
        result = case.router._emergency(different, row, "DIFFERENT_INTENT")
        self.assertEqual(result.code, "DIFFERENT_INTENT")
        self.assertGreater(case.exchange.get_calls, 0)
        self.assertEqual(len(case.exchange.posts), 1)
        self.assertTrue(case.exchange.posts[0][0].endswith("/trade/cancel-order"))
        self.assertEqual(case.router._entry_not_sent_fingerprints[case.intent.signal_id], cached)


if __name__ == "__main__":
    unittest.main(verbosity=2)
