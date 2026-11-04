"""Natural DEMO entry gates over the real router/transport with fake HTTP only."""
from __future__ import annotations

import sys
import unittest
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

import test_demo_entry_admission as fixtures
from src.execution import build_intent
from src.order_router import NativeTransport, OrderRouter, RouterError


class NaturalDemoEntryAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.DemoEntryAdmissionTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.clock = self.helper.clock
        self.context_calls = []
        self.deadline = fixtures.NOW + 60_000
        self.context_code = "MACRO_BLACKOUT"

    def context_check(self, effective_now_ms):
        self.context_calls.append(effective_now_ms)
        return {"valid_until_ms": self.deadline, "code": self.context_code}

    def fixture(self, *, side="long", check=None):
        case = self.helper.fixture(side=side)
        case.signal["id"] = "SCA-" + side.upper() + "-NATURAL-" + str(self.helper.serial)
        case.intent = build_intent(case.signal, case.state, case.config)
        case.router = OrderRouter(case.transport, config=case.config,
                    state_provider=lambda: deepcopy(case.state), market_provider=lambda: deepcopy(case.book),
                    account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
                    log_root=case.root, journal_path=case.root / "router.sqlite3",
                    clock=lambda: self.clock.wall, require_demo_entry_admission=True,
                    final_entry_check=self.context_check if check is None else check)
        return case

    def assert_local_refusal(self, case, result, code, *, persisted=True):
        self.helper.assert_local_rejection(case, result, code, persisted=persisted)

    def test_natural_long_and_short_use_original_maker_price_and_single_post(self):
        for side in ("long", "short"):
            with self.subTest(side=side):
                case = self.fixture(side=side)
                original = case.intent.to_dict()
                self.assertEqual(case.router.execute(case.intent).status, "PENDING")
                self.assertEqual(len(case.exchange.posts), 1)
                body = case.exchange.posts[0][1]
                self.assertEqual(body["px"], str(case.intent.entry))
                self.assertEqual(body["sz"], str(case.intent.qty))
                self.assertEqual(body["ordType"], "post_only")
                self.assertEqual(case.intent.to_dict(), original)
                self.assertEqual(case.router.execute(case.intent).status, "PENDING")
                self.assertEqual(len(case.exchange.posts), 1)
                self.assertGreaterEqual(len(self.context_calls), 3)

    def test_second_preflight_crossed_maker_quote_releases_reserved_slot(self):
        case = self.fixture()
        case.router.market_provider = Mock(side_effect=[deepcopy(case.book),
                        {**case.book, "ask": str(case.intent.entry)}])
        self.assert_local_refusal(case, case.router.execute(case.intent), "MAKER_QUOTE_MOVED")
        self.assertFalse(self.helper.route(case)["data"]["submit_attempted"])

    def test_no_book_or_fabricated_last_book_is_not_accepted(self):
        for changes in ({"bid": None}, {"ask": None}, {"bid": "NaN"},
                        {"bid": "101", "ask": "100"}):
            with self.subTest(changes=changes):
                case = self.fixture()
                case.book.update(changes)
                self.assert_local_refusal(case, case.router.execute(case.intent),
                                          "MAKER_QUOTE_INVALID", persisted=False)

    def test_strict_quote_and_account_require_original_integer_times(self):
        for source in ("account", "quote"):
            for stamp in (str(fixtures.NOW), float(fixtures.NOW), True):
                with self.subTest(source=source, stamp_type=type(stamp).__name__):
                    case = self.fixture()
                    if source == "account":
                        case.state["observed_at_ms"] = stamp
                        code = "DEMO_STATE_UNVERIFIED"
                    else:
                        case.book["timestamp_ms"] = stamp
                        code = "MARKET_UNAVAILABLE" if type(stamp) is bool else "MARKET_STALE"
                    self.assert_local_refusal(case, case.router.execute(case.intent), code, persisted=False)

    def test_save_audit_and_signature_delay_all_block_natural_entry(self):
        for where in ("save", "router_audit", "transport_audit", "signature"):
            with self.subTest(where=where):
                self.clock.wall, self.clock.mono = fixtures.NOW, 1_000_000_000
                case = self.fixture()
                with self.helper._delay(case, where, 5001):
                    result = case.router.execute(case.intent)
                self.assert_local_refusal(case, result, "DEMO_STATE_STALE")
                row = self.helper.route(case)["data"]
                self.assertTrue(row["submit_attempted"])
                self.assertTrue(row["entry_not_sent"])
                self.assertEqual(case.transport._attempts, {})

    def test_exact_five_seconds_allowed_one_more_millisecond_refused(self):
        for delay in (5000, 5001):
            with self.subTest(delay=delay):
                self.clock.wall, self.clock.mono = fixtures.NOW, 1_000_000_000
                case = self.fixture()
                with self.helper._delay(case, "signature", delay):
                    result = case.router.execute(case.intent)
                if delay == 5000:
                    self.assertEqual(result.status, "PENDING")
                    self.assertEqual(len(case.exchange.posts), 1)
                else:
                    self.assert_local_refusal(case, result, "DEMO_STATE_STALE")

    def test_only_quote_expires_even_with_fresh_account(self):
        case = self.fixture()
        case.book["timestamp_ms"] -= 4000
        with self.helper._delay(case, "signature", 1001):
            result = case.router.execute(case.intent)
        self.assert_local_refusal(case, result, "MARKET_STALE")

    def test_wall_rollback_and_monotonic_expiry_are_independently_enforced(self):
        for wall_delta, mono_delta, code in ((-1, 1, "DEMO_ENTRY_CLOCK_INVALID"),
                    (0, 5001, "DEMO_STATE_STALE"), (0, -1, "DEMO_ENTRY_CLOCK_INVALID")):
            with self.subTest(wall_delta=wall_delta, mono_delta=mono_delta):
                self.clock.wall, self.clock.mono = fixtures.NOW, 1_000_000_000
                case = self.fixture()
                original = case.transport._reader._headers
                def headers(method, request_path, body=""):
                    if method == "POST":
                        self.clock.wall += wall_delta
                        self.clock.mono += mono_delta * 1_000_000
                    return original(method, request_path, body)
                with patch.object(case.transport._reader, "_headers", side_effect=headers):
                    result = case.router.execute(case.intent)
                self.assert_local_refusal(case, result, code)

    def test_final_macro_and_calendar_boundaries_keep_specific_zero_send_code(self):
        for code in ("MACRO_BLACKOUT", "DEMO_CALENDAR_STALE", "DATA_STALE", "MARKET_STALE"):
            with self.subTest(code=code):
                self.clock.wall, self.clock.mono = fixtures.NOW, 1_000_000_000
                self.deadline, self.context_code = fixtures.NOW + 1000, code
                case = self.fixture()
                with self.helper._delay(case, "signature", 1000):
                    result = case.router.execute(case.intent)
                self.assert_local_refusal(case, result, code)
                self.assertEqual(self.helper.route(case)["data"]["entry_not_sent_code"], code)
                self.assertEqual(case.transport._attempts, {})

    def test_final_callback_exceptions_are_zero_send_and_never_replayed(self):
        calls = 0
        def guard(stamp):
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise RouterError("MACRO_BLACKOUT")
            return self.context_check(stamp)
        case = self.fixture(check=guard)
        self.assert_local_refusal(case, case.router.execute(case.intent), "MACRO_BLACKOUT")
        self.clock.advance(1)
        self.assertEqual(case.router.execute(case.intent).code, "MACRO_BLACKOUT")
        self.helper.assert_no_write(case)
        self.assertEqual(calls, 3)

    def test_callback_processing_time_cannot_cross_its_own_deadline(self):
        calls = 0
        self.deadline = fixtures.NOW + 1000
        def slow(stamp):
            nonlocal calls
            calls += 1
            result = self.context_check(stamp)
            if calls == 3:
                self.clock.advance(1000)
            return result
        case = self.fixture(check=slow)
        self.assert_local_refusal(case, case.router.execute(case.intent), "MACRO_BLACKOUT")

    def test_callback_cannot_extend_the_original_trusted_deadline(self):
        calls = 0
        self.deadline = fixtures.NOW + 1000
        def extending(stamp):
            nonlocal calls
            calls += 1
            if calls == 3:
                return {"valid_until_ms": fixtures.NOW + 60_000, "code": "MACRO_BLACKOUT"}
            return self.context_check(stamp)
        case = self.fixture(check=extending)
        with self.helper._delay(case, "signature", 1000):
            result = case.router.execute(case.intent)
        self.assert_local_refusal(case, result, "MACRO_BLACKOUT")

    def test_monotonic_macro_boundary_is_used_when_wall_stops(self):
        case = self.fixture()
        self.deadline = fixtures.NOW + 1000
        original = case.transport._reader._headers
        def headers(method, request_path, body=""):
            if method == "POST":
                self.clock.advance(1000, wall=False)
            return original(method, request_path, body)
        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            result = case.router.execute(case.intent)
        self.assert_local_refusal(case, result, "MACRO_BLACKOUT")
        self.assertEqual(self.context_calls[-1], fixtures.NOW + 1000)

    def test_fractional_monotonic_time_rounds_up_at_inclusive_macro_boundary(self):
        case = self.fixture()
        self.deadline = fixtures.NOW + 1
        original = case.transport._reader._headers
        def headers(method, request_path, body=""):
            if method == "POST":
                self.clock.mono += 1
            return original(method, request_path, body)
        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            result = case.router.execute(case.intent)
        self.assert_local_refusal(case, result, "MACRO_BLACKOUT")

    def test_callback_delay_also_rechecks_quote_account_and_signal_expiry(self):
        for expected in ("DEMO_STATE_STALE", "MARKET_STALE", "EXPIRED"):
            with self.subTest(expected=expected):
                self.clock.wall, self.clock.mono = fixtures.NOW, 1_000_000_000
                calls = 0
                def slow(stamp):
                    nonlocal calls
                    calls += 1
                    result = self.context_check(stamp)
                    if calls == 3:
                        self.clock.advance(5001 if expected == "DEMO_STATE_STALE" else 1000)
                    return result
                case = self.fixture(check=slow)
                if expected == "MARKET_STALE":
                    case.book["timestamp_ms"] -= 4001
                if expected == "EXPIRED":
                    case.intent = build_intent({**case.signal, "expires_at_ms": fixtures.NOW + 1000},
                                                case.state, case.config)
                self.assert_local_refusal(case, case.router.execute(case.intent), expected)

    def test_malformed_or_unknown_context_window_is_refused_without_http(self):
        for value in (None, True, {}, {"valid_until_ms": "123", "code": "MACRO_BLACKOUT"},
                      {"valid_until_ms": True, "code": "MACRO_BLACKOUT"},
                      {"valid_until_ms": fixtures.NOW + 1000, "code": []},
                      {"valid_until_ms": fixtures.NOW + 1000, "code": "UNTRUSTED"}):
            with self.subTest(value=value):
                case = self.fixture(check=lambda stamp: value)
                self.assert_local_refusal(case, case.router.execute(case.intent),
                                          "DEMO_ENTRY_ADMISSION_FAILED", persisted=False)

    def test_flag_cannot_enable_other_transport_or_missing_guard_identity(self):
        case = self.fixture()
        options = dict(config=case.config, state_provider=lambda: case.state,
                    market_provider=lambda: case.book, log_root=case.root,
                    account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
                    require_demo_entry_admission=True, final_entry_check=self.context_check)
        for changes in ({"transport": NativeTransport()}, {"account_identity": None},
                        {"account_key": "LIVE-SOL"}, {"final_entry_check": None},
                        {"require_demo_entry_admission": 1},
                        {"require_demo_entry_admission": False}):
            with self.subTest(changes=changes):
                args = {**options, "transport": case.transport, **changes}
                with self.assertRaises(ValueError):
                    OrderRouter(**args)
        self.helper.assert_no_write(case)

    def test_actual_unknown_post_keeps_original_unknown_semantics_no_resubmit(self):
        case = self.fixture()
        case.exchange.spoof_not_sent_in_http = True
        result = case.router.execute(case.intent)
        self.assertEqual((result.status, result.code), ("UNKNOWN", "SUBMISSION_UNKNOWN"))
        entries = [body for _, body in case.exchange.posts if body.get("ordType") == "post_only"]
        self.assertEqual(len(entries), 1)
        self.assertFalse(self.helper.route(case)["data"].get("entry_not_sent", False))
        count = len(case.exchange.posts)
        self.assertEqual(case.router.execute(case.intent).status, "UNKNOWN")
        self.assertEqual(len(case.exchange.posts), count)

    def test_final_refusal_persistence_failure_remains_local_halt(self):
        case = self.fixture()
        original = case.router._save
        def save(row):
            if row.get("entry_not_sent"):
                raise OSError("offline local refusal write fault")
            return original(row)
        with self.helper._delay(case, "signature", 5001), patch.object(case.router, "_save", side_effect=save):
            result = case.router.execute(case.intent)
        self.assertEqual((result.status, result.code), ("HALTED", "ENTRY_REFUSAL_PERSIST_FAILED"))
        self.assertTrue(case.router.halt_path.exists())
        self.assertEqual(case.router.emergency_reduce(replace(case.intent), "ENTRY_HALTED").status, "HALTED")
        self.helper.assert_no_write(case)


if __name__ == "__main__":
    unittest.main(verbosity=2)
