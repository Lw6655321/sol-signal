"""Offline service evidence and production wiring; no real keys or network."""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, call, patch

from src.order_router import RouterError


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("service_entry_evidence", ROOT / "scripts" / "demo_auto_run.py")
ENTRY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENTRY)
NOW = 1_800_000_000_000


class ServiceEntryEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIAL_READ")).start()
        patch("src.demo_route_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIAL_READ")).start()
        self.config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
        self.now = NOW
        self.quote = Mock(side_effect=lambda: {"last": "100", "bid": "99.99", "ask": "100.01",
                                              "timestamp_ms": self.now})
        self.evidence = ENTRY.ServiceEntryEvidence(self.config, self.quote, clock=lambda: self.now)
        self.scan = self.scan_at(NOW)
        self.evidence.update_scan(self.scan)

    def scan_at(self, now, events=None):
        sources = {name: {"ok": True, "source_ts": now, "fetched_at_ms": now}
                   for name in self.config["data"]["critical_sources"]}
        sources["economic_calendar"] = {"ok": True, "source_ts": now - 10_000,
            "fetched_at_ms": now - 5_000, "data": events if events is not None else []}
        return {"generated_at_ms": now, "health": {"usable": True}, "sources": sources,
                "features": {"macro": {"blackout": False}}}

    def event(self, start):
        return {"ts": start + self.config["macro"]["blackout_minutes_before"] * 60_000,
                "impact": "High", "country": "USD", "title": "offline fixture"}

    def assert_code(self, code, now=None):
        with self.assertRaises(RouterError) as raised:
            self.evidence.assert_entry_current(self.now if now is None else now)
        self.assertEqual(raised.exception.code, code)

    def test_quote_retains_real_bid_ask_and_original_timestamp(self):
        client = Mock()
        client.get.return_value = [{"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99",
                                    "askPx": "100.01", "ts": str(NOW - 5000)}]
        result = ENTRY.service_quote(client, clock=lambda: NOW)
        self.assertEqual(result, {"last": "100", "bid": "99.99", "ask": "100.01", "timestamp_ms": NOW - 5000})
        client.get.assert_called_once_with("/api/v5/market/ticker", {"instId": "SOL-USDT-SWAP"})

    def test_quote_rejects_bad_source_identity_timestamps_and_prices(self):
        row = {"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99", "askPx": "100.01", "ts": NOW}
        invalid = [("instId", "BTC-USDT-SWAP"), ("ts", True), ("ts", float(NOW)),
                   ("ts", "１８０００００００００００"), ("ts", NOW + 1), ("ts", NOW - 5001),
                   ("bidPx", "100.02"), ("bidPx", "NaN"), ("askPx", "Infinity"),
                   ("last", True), ("last", "0")]
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                client = Mock()
                client.get.return_value = [{**row, field: value}]
                with self.assertRaisesRegex(RuntimeError, "DEMO_MARKET_UNAVAILABLE"):
                    ENTRY.service_quote(client, clock=lambda: NOW)
        for rows in ([], [row, row], [None], None):
            with self.subTest(rows=rows):
                with self.assertRaisesRegex(RuntimeError, "DEMO_MARKET_UNAVAILABLE"):
                    ENTRY.service_quote(Mock(get=Mock(return_value=rows)), clock=lambda: NOW)

    def test_quote_wall_regression_during_read_cannot_make_evidence_fresh(self):
        client = Mock(get=Mock(return_value=[{"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99",
                                             "askPx": "100.01", "ts": NOW - 100}]))
        times = iter((NOW, NOW - 1))
        with self.assertRaisesRegex(RuntimeError, "DEMO_MARKET_UNAVAILABLE"):
            ENTRY.service_quote(client, clock=lambda: next(times))

    def test_quote_waits_for_small_clock_difference_without_rewriting_source_or_retrying(self):
        stamp = NOW + 436
        row = {"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99", "askPx": "100.01", "ts": str(stamp)}
        client = Mock(get=Mock(return_value=[row]))
        wall = iter((NOW, NOW, stamp))
        with patch.object(ENTRY.time, "sleep") as wait, \
                patch.object(ENTRY.time, "monotonic_ns", side_effect=(10_000_000_000, 10_436_000_000)):
            quote = ENTRY.service_quote(client, clock=lambda: next(wall))
        wait.assert_called_once_with(0.436)
        client.get.assert_called_once_with("/api/v5/market/ticker", {"instId": "SOL-USDT-SWAP"})
        self.assertEqual(quote["timestamp_ms"], stamp)
        self.assertEqual(row["ts"], str(stamp))

    def test_quote_clock_wait_does_not_admit_future_stale_rollback_or_unbounded_delay(self):
        row = {"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99", "askPx": "100.01", "ts": NOW + 400}
        cases = ((NOW + 399, 400_000_000), (NOW - 1, 400_000_000),
                 (NOW + 5401, 400_000_000), (NOW + 400, 1_000_000_001))
        for completed, duration in cases:
            client = Mock(get=Mock(return_value=[row]))
            wall = iter((NOW, NOW, completed))
            with self.subTest(completed=completed, duration=duration), patch.object(ENTRY.time, "sleep") as wait, \
                    patch.object(ENTRY.time, "monotonic_ns", side_effect=(10_000_000_000, 10_000_000_000 + duration)), \
                    self.assertRaisesRegex(RuntimeError, "DEMO_MARKET_UNAVAILABLE"):
                ENTRY.service_quote(client, clock=lambda: next(wall))
            wait.assert_called_once_with(0.4)
            client.get.assert_called_once()
        client = Mock(get=Mock(return_value=[{**row, "ts": NOW + 1001}]))
        with patch.object(ENTRY.time, "sleep") as wait, self.assertRaisesRegex(RuntimeError, "DEMO_MARKET_UNAVAILABLE"):
            ENTRY.service_quote(client, clock=lambda: NOW)
        wait.assert_not_called()
        client.get.assert_called_once()

    def test_quote_clock_wait_preserves_macro_boundary_and_stop_admission(self):
        row = {"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99.99", "askPx": "100.01", "ts": NOW + 400}
        client = Mock(get=Mock(return_value=[row]))
        self.evidence.quote = lambda: ENTRY.service_quote(client, clock=lambda: self.now)
        self.evidence.update_scan(self.scan_at(NOW, [self.event(NOW + 200)]))
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as folder:
            state = Path(folder)

            def wait(seconds):
                self.now += round(seconds * 1000)
                (state / "STOP_NEW_ENTRIES").touch()

            with patch.object(ENTRY.time, "sleep", side_effect=wait), \
                    patch.object(ENTRY.time, "monotonic_ns", side_effect=(10_000_000_000, 10_400_000_000)):
                context = self.evidence.fresh_context()
            self.assertEqual(context["observed_at_ms"], NOW + 400)
            self.assertIs(context["macro_blackout"], True)
            self.assertIs(context["data_fresh"], False)
            with self.assertRaises(RouterError) as caught:
                ENTRY.assert_service_entry_current(state, self.evidence, self.now)
            self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")

    def test_dynamic_macro_crossing_ignores_scan_false_boolean(self):
        start = NOW + 1000
        self.scan = self.scan_at(NOW, [self.event(start)])
        self.evidence.update_scan(self.scan)
        self.assertEqual(self.evidence.assert_entry_current(NOW),
                         {"valid_until_ms": start, "code": "MACRO_BLACKOUT"})
        self.now = start
        self.assert_code("MACRO_BLACKOUT")
        context = self.evidence.fresh_context()
        self.assertIs(context["macro_blackout"], True)
        self.assertIs(context["data_fresh"], False)

    def test_macro_window_end_remains_inclusive(self):
        event = {"ts": NOW - 30 * 60_000, "impact": "High", "country": "USD"}
        self.evidence.update_scan(self.scan_at(NOW, [event]))
        self.assert_code("MACRO_BLACKOUT")
        self.now += 1
        self.assertIs(self.evidence.fresh_context()["macro_blackout"], False)

    def test_calendar_source_age_never_uses_new_cache_envelope(self):
        calendar = self.scan["sources"]["economic_calendar"]
        calendar.update(source_ts=NOW - ENTRY.DEMO_CALENDAR_MAX_AGE_MS, fetched_at_ms=NOW)
        self.evidence.update_scan(self.scan)
        self.assertEqual(self.evidence.assert_entry_current(NOW),
                         {"valid_until_ms": NOW + 1, "code": "DEMO_CALENDAR_STALE"})
        self.now += 1
        self.assert_code("DEMO_CALENDAR_STALE")
        self.assertIsNone(self.evidence.fresh_context()["macro_blackout"])

    def test_bad_calendar_never_becomes_empty_clear_calendar(self):
        cases = [("ok", 1), ("source_ts", True), ("source_ts", NOW + 1),
                 ("source_ts", str(NOW)), ("source_ts", None), ("fetched_at_ms", NOW + 1),
                 ("fetched_at_ms", NOW - 20_000), ("data", None), ("data", {}),
                 ("data", [{"ts": True, "impact": "High", "country": "USD"}]),
                 ("data", [{"ts": NOW, "impact": "High"}])]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                scan = deepcopy(self.scan)
                scan["sources"]["economic_calendar"][field] = value
                self.evidence.update_scan(scan)
                self.assert_code("DEMO_CALENDAR_STALE")

    def test_original_critical_source_expiry_wins_over_refreshed_quote(self):
        self.scan["sources"]["order_book"]["source_ts"] = NOW - 45_000
        self.evidence.update_scan(self.scan)
        self.assertEqual(self.evidence.assert_entry_current(NOW),
                         {"valid_until_ms": NOW + 1, "code": "MARKET_STALE"})
        self.now += 1
        self.assertIs(self.evidence.fresh_context()["data_fresh"], False)
        self.assert_code("MARKET_STALE")

    def test_missing_unusable_or_future_critical_scan_is_not_refreshed_by_quote(self):
        cases = [lambda scan: scan.pop("generated_at_ms"),
                 lambda scan: scan.update(generated_at_ms=NOW + 1),
                 lambda scan: scan["health"].update(usable=1),
                 lambda scan: scan["sources"].pop("order_book"),
                 lambda scan: scan["sources"]["order_book"].update(source_ts=True),
                 lambda scan: scan["sources"]["order_book"].update(ok=False)]
        for mutation in cases:
            scan = deepcopy(self.scan)
            mutation(scan)
            self.evidence.update_scan(scan)
            self.assert_code("MARKET_STALE")
            self.assertIs(self.evidence.fresh_context()["data_fresh"], False)

    def test_original_scan_generation_not_replaced_by_current_context_time(self):
        config = deepcopy(self.config)
        config["data"]["critical_sources"] = []
        evidence = ENTRY.ServiceEntryEvidence(config, self.quote, clock=lambda: self.now)
        evidence.update_scan(self.scan)
        self.now += 60_000
        self.assertEqual(evidence.assert_entry_current(self.now)["valid_until_ms"], self.now + 1)
        self.now += 1
        self.assertIs(evidence.fresh_context()["data_fresh"], False)

    def test_evidence_is_deep_copy_and_boundary_lookup_has_no_quote_or_network(self):
        start = NOW + 1000
        self.scan["sources"]["economic_calendar"]["data"] = [self.event(start)]
        self.evidence.update_scan(self.scan)
        self.scan["sources"]["economic_calendar"]["data"].clear()
        self.scan["sources"]["economic_calendar"]["source_ts"] = NOW
        self.config["macro"]["block_currencies"].clear()
        self.assertEqual(self.evidence.assert_entry_current(NOW)["valid_until_ms"], start)
        self.quote.assert_not_called()
        self.assert_code("MACRO_BLACKOUT", start)

    def test_fresh_context_copies_quote_source_time_and_rechecks_each_snapshot(self):
        provider = Mock()
        wrapper = ENTRY.ServiceRiskProvider(provider, self.evidence)
        self.quote.side_effect = lambda: {"timestamp_ms": self.now - 4000}
        wrapper.snapshot({"observed_at_ms": self.now, "data_fresh": True, "macro_blackout": False})
        self.assertEqual(provider.snapshot.call_args.args[0]["observed_at_ms"], self.now - 4000)
        self.now += 46_000
        wrapper.snapshot({"observed_at_ms": self.now, "data_fresh": True, "macro_blackout": False})
        self.assertIs(provider.snapshot.call_args.args[0]["data_fresh"], False)
        self.assertEqual(self.quote.call_count, 2)

    def test_service_wires_strict_router_provider_and_same_provider_retry_budget(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            state = root / "state" / "demo-auto"
            state.mkdir(parents=True)
            (state / "STOP_NEW_ENTRIES").write_text("offline fixture\n", encoding="utf-8")
            binding_client = Mock()
            evidence_client = Mock(get=Mock(return_value=[{"instId": "SOL-USDT-SWAP", "last": "100",
                "bidPx": "99.99", "askPx": "100.01", "ts": NOW}]))
            provider = Mock()
            provider.snapshot.return_value = {"demo_verified": False, "entries_today": 0,
                "observed_at_ms": NOW, "refusal_reasons": ["OFFLINE_FIXTURE"]}
            controller = Mock()
            controller.status.return_value = {"status": "IDLE"}
            controller.tick.return_value = {"status": "IDLE"}
            with patch.object(ENTRY, "ROOT", root), patch.object(ENTRY, "_now", return_value=NOW), \
                    patch.object(ENTRY, "assert_service_recovery_ready"), \
                    patch("src.demo_successor_gate.assert_activated_generation") as activation, \
                    patch.object(ENTRY, "bind_demo_account", return_value="a" * 64), \
                    patch("src.demo_transport.load_demo_credentials", return_value={"offline": "fixture"}), \
                    patch("src.demo_transport.DemoReadOnlyClient", side_effect=[binding_client, evidence_client]) as clients, \
                    patch("src.demo_route_transport.DemoRouteTransport") as transport, \
                    patch("src.demo_verified_risk.VerifiedDemoRiskProvider", return_value=provider) as providers, \
                    patch("src.order_router.OrderRouter") as routers, \
                    patch("src.demo_auto.DemoAutoController", return_value=controller) as controllers, \
                    patch("src.collect.collect", return_value=self.scan), patch("src.collect.trimmed", return_value={}), \
                    patch("src.util.load_config", return_value=self.config), \
                    patch("src.engine.evaluate", return_value={"signals": [], "gates": {"pass": True}}), \
                    patch("builtins.print"):
                routers.return_value._local_halted = False
                self.assertEqual(ENTRY._run_locked_service(enabled=True, continuous=True), 0)
                hook = routers.call_args.kwargs["final_entry_check"]
                with self.assertRaises(RouterError) as caught:
                    hook(NOW)
                self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")
                (state / "STOP_NEW_ENTRIES").unlink()
                evidence = controllers.call_args.args[2].evidence
                self.assertEqual(hook(NOW), evidence.assert_entry_current(NOW))
                self.assertGreater(hook(NOW)["valid_until_ms"], NOW)
            self.assertEqual(activation.call_args_list, [call(root), call(root)])
            self.assertEqual(clients.call_args_list[0].kwargs, {"http_get": None, "network_retries": 0})
            retry = clients.call_args_list[1].kwargs
            self.assertEqual(retry["network_retries"], 1)
            retry["retry_budget"]("/api/v5/account/balance")
            provider._reserve_get.assert_called_once_with("/api/v5/account/balance")
            self.assertIs(providers.call_args.args[0], evidence_client)
            self.assertIs(routers.call_args.kwargs["require_demo_entry_admission"], True)
            wrapped = controllers.call_args.args[2]
            self.assertIsInstance(wrapped, ENTRY.ServiceRiskProvider)
            self.assertIs(routers.call_args.kwargs["state_provider"].__self__, wrapped)
            transport.assert_called_once_with({"offline": "fixture"}, enable_writes=True,
                http_get=None, log_root=root / "logs" / "demo-auto")
            controller.process_scan.assert_not_called()
        with self.assertRaises(RouterError) as caught:
            hook(NOW)
        self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")


class ServiceEvidenceRouterIntegrationTests(unittest.TestCase):
    def setUp(self):
        import test_demo_entry_admission as fixtures

        self.fixtures = fixtures
        self.helper = fixtures.DemoEntryAdmissionTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.clock = self.helper.clock

    def fixture(self, *, boundary="macro", side="long"):
        from src.execution import build_intent
        from src.order_router import OrderRouter

        case = self.helper.fixture(side=side)
        case.signal["id"] = "SCA-" + side.upper() + "-SERVICE-" + str(self.helper.serial)
        case.intent = build_intent(case.signal, case.state, case.config)
        sources = {name: {"ok": True, "source_ts": self.fixtures.NOW,
                         "fetched_at_ms": self.fixtures.NOW}
                   for name in case.config["data"]["critical_sources"]}
        calendar = {"ok": True, "source_ts": self.fixtures.NOW - 10_000,
                    "fetched_at_ms": self.fixtures.NOW, "data": []}
        if boundary == "macro":
            calendar["data"] = [{"ts": self.fixtures.NOW + 1000 + 30 * 60_000,
                                  "country": "USD", "impact": "High"}]
            deadline = self.fixtures.NOW + 1000
        elif boundary == "calendar":
            calendar["source_ts"] = self.fixtures.NOW - ENTRY.DEMO_CALENDAR_MAX_AGE_MS + 1000
            deadline = self.fixtures.NOW + 1001
        elif boundary == "critical":
            sources["order_book"]["source_ts"] = self.fixtures.NOW - 45_000 + 1000
            deadline = self.fixtures.NOW + 1001
        else:
            deadline = self.fixtures.NOW + 45_001
        sources["economic_calendar"] = calendar
        scan = {"generated_at_ms": self.fixtures.NOW, "health": {"usable": True},
                "features": {"macro": {"blackout": False}}, "sources": sources}
        quote_client = Mock()
        quote_client.get.side_effect = lambda *_: [{"instId": "SOL-USDT-SWAP",
            "last": case.book["last"], "bidPx": case.book["bid"], "askPx": case.book["ask"],
            "ts": case.book["timestamp_ms"]}]
        quote = lambda: ENTRY.service_quote(quote_client, clock=lambda: self.clock.wall)
        evidence = ENTRY.ServiceEntryEvidence(case.config, quote, clock=lambda: self.clock.wall)
        evidence.update_scan(scan)
        raw_provider = Mock()
        raw_provider.snapshot.side_effect = lambda context: {**deepcopy(case.state), **context}
        risk_provider = ENTRY.ServiceRiskProvider(raw_provider, evidence)
        case.router = OrderRouter(case.transport, config=case.config, state_provider=risk_provider.snapshot,
            market_provider=quote, account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
            log_root=case.root, journal_path=case.root / "router.sqlite3", clock=lambda: self.clock.wall,
            require_demo_entry_admission=True, final_entry_check=evidence.assert_entry_current)
        case.evidence, case.deadline = evidence, deadline
        return case

    def test_service_macro_calendar_and_critical_boundaries_block_signature_delay(self):
        for boundary, code in (("macro", "MACRO_BLACKOUT"), ("calendar", "DEMO_CALENDAR_STALE"),
                               ("critical", "MARKET_STALE")):
            with self.subTest(boundary=boundary):
                self.clock.wall, self.clock.mono = self.fixtures.NOW, 1_000_000_000
                case = self.fixture(boundary=boundary)
                with self.helper._delay(case, "signature", case.deadline - self.fixtures.NOW):
                    result = case.router.execute(case.intent)
                self.helper.assert_local_rejection(case, result, code)
                self.assertIs(self.helper.route(case)["data"]["entry_not_sent"], True)
                self.assertEqual(case.router.execute(case.intent).status, "REJECTED")
                self.assertEqual(case.exchange.posts, [])

    def test_pure_calendar_hook_processing_delay_cannot_cross_its_own_deadline(self):
        case = self.fixture()
        original = case.evidence.assert_entry_current
        calls = 0

        def delayed(effective):
            nonlocal calls
            result = original(effective)
            calls += 1
            if calls == 3:
                self.clock.advance(1000)
            return result

        case.router.final_entry_check = delayed
        self.helper.assert_local_rejection(case, case.router.execute(case.intent), "MACRO_BLACKOUT")
        self.assertEqual(calls, 3)

    def test_monotonic_macro_crossing_is_detected_when_wall_clock_stalls(self):
        case = self.fixture()
        original = case.transport._reader._headers

        def headers(method, path, body=""):
            if method == "POST":
                self.clock.advance(1000, wall=False)
            return original(method, path, body)

        with patch.object(case.transport._reader, "_headers", side_effect=headers):
            self.helper.assert_local_rejection(case, case.router.execute(case.intent), "MACRO_BLACKOUT")

    def test_service_quote_and_final_guard_preserve_safe_natural_maker_plan(self):
        for side in ("long", "short"):
            with self.subTest(side=side):
                case = self.fixture(boundary="none", side=side)
                original = case.intent.to_dict()
                self.assertEqual(case.router.execute(case.intent).status, "PENDING")
                self.assertEqual(len(case.exchange.posts), 1)
                self.assertEqual(case.exchange.posts[0][1]["px"], str(case.intent.entry))
                self.assertEqual(case.exchange.posts[0][1]["ordType"], "post_only")
                self.assertEqual(case.intent.to_dict(), original)


if __name__ == "__main__":
    unittest.main()
