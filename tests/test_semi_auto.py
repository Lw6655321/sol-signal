"""Offline entry-point regressions. All sockets, credentials and account IO fail."""
from __future__ import annotations

import copy
import io
import json
import socket
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import semi_auto as app
from src import okx_private
from src.execution import build_intent
from src.order_router import OrderRouter, RouteResult


class SemiAutoTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.guards = ExitStack()
        self.addCleanup(self.guards.close)
        for name in ("private", "creds", "balance", "positions", "pending_orders", "algo_orders",
                     "place_order", "place_oco"):
            if hasattr(okx_private, name):
                self.guards.enter_context(patch.object(okx_private, name,
                    side_effect=AssertionError("forbidden account or credential IO")))
        self.guards.enter_context(patch.object(socket, "socket", side_effect=AssertionError("network forbidden")))
        self.guards.enter_context(patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")))
        self.guards.enter_context(patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")))
        self.cfg = app.load_config()
        self.state = dict(now_ms=app.DEMO_NOW, equity_estimate="100", day_start_equity="100",
            realized_pnl_today="0", entries_today=0, open_positions=[], pending_orders=[],
            data_fresh=True, macro_blackout=False, halted=False, slippage_bps="5",
            observed_at_ms=app.DEMO_NOW, live_verified=True)
        self.signal = dict(id="ENTRY-TEST", instId=app.INST, kind="swing_1h4h", side="long",
            entry="100", stop="98.9", tp1="102.5", tp2="103.8",
            generated_at_ms=app.DEMO_NOW - 1000, expires_at_ms=app.DEMO_NOW + 180000)
        self.intent = build_intent(self.signal, self.state, self.cfg)
        self.output = io.StringIO()
        self.guards.enter_context(redirect_stdout(self.output))

    def result(self, status="CANCELED", *, filled="0", protected="0", emergency=""):
        return RouteResult(status, "OK", self.intent.signal_id, "wire001", Decimal(filled),
            Decimal(protected), self.directory, emergency)

    def provider(self, state):
        return patch.dict(sys.modules, {"src.live_state": SimpleNamespace(live_state_provider=lambda cfg: state)})

    def test_offline_all_scenarios_never_read_switch_or_account(self):
        with patch.object(app, "trading_enabled", side_effect=AssertionError("environment forbidden")), \
             patch.object(app, "load_live_state", side_effect=AssertionError("live state forbidden")), \
             patch.object(app, "fetch_market_snapshot", side_effect=AssertionError("ticker forbidden")):
            self.assertEqual(app.main(["--offline-demo", "--log-root", str(self.directory)]), 0)
        summaries = [json.loads(line) for line in self.output.getvalue().splitlines() if line.startswith("{")]
        self.assertEqual([s["status"] for s in summaries], ["PROTECTED", "CANCELED", "PROTECTED", "REJECTED"])
        self.assertEqual(summaries[0]["steps"], ["PENDING", "PROTECTED"])
        self.assertEqual(summaries[1]["events"], ["submit", "cancel"])
        self.assertEqual(summaries[2]["events"], ["submit", "cancel", "fill_during_cancel", "protect"])
        self.assertEqual(summaries[3]["code"], "DAILY_ENTRIES")
        self.assertEqual(summaries[3]["events"], [])
        self.assertEqual(len({s["run_dir"] for s in summaries}), 4)

    def test_live_demo_and_smoke_are_rejected_before_switch_lookup(self):
        with patch.object(app, "trading_enabled", side_effect=AssertionError("must not inspect switch")):
            for flags in (["--live", "--demo"], ["--live", "--offline-demo"], ["--smoke", "--yes"]):
                self.assertEqual(app.main(flags), 2)

    def test_interactive_demo_rejection_never_submits(self):
        with patch.object(app, "trading_enabled", side_effect=AssertionError("environment forbidden")), \
             patch.object(app.MockTransport, "submit_entry", side_effect=AssertionError("rejection must not submit")), \
             patch("builtins.input", return_value="n") as prompt:
            self.assertEqual(app.main(["--interactive-demo", "--log-root", str(self.directory)]), 0)
        self.assertEqual(prompt.call_count, 1)
        self.assertIn("OPERATOR_REJECTED", self.output.getvalue())
        decisions = list(self.directory.glob("*/operator_decision.json"))
        self.assertEqual(len(decisions), 1)
        self.assertEqual(json.loads(decisions[0].read_text(encoding="utf-8"))["events"], [])

    def test_interactive_demo_card_precedes_confirmation_and_submission(self):
        original_submit = app.MockTransport.submit_entry
        events = []
        def submit(transport, *args):
            events.append("submit")
            return original_submit(transport, *args)
        with patch.object(app, "trading_enabled", side_effect=AssertionError("environment forbidden")), \
             patch.object(app, "render_card", side_effect=lambda *a: events.append("card")), \
             patch("builtins.input", side_effect=lambda *a: events.append("confirm") or "y"), \
             patch.object(app.MockTransport, "submit_entry", submit):
            self.assertEqual(app.main(["--interactive-demo", "--log-root", str(self.directory)]), 0)
        self.assertEqual(events, ["card", "confirm", "submit"])
        self.assertIn('"steps": ["PENDING", "PROTECTED"]', self.output.getvalue())

    def test_yes_cannot_bypass_exact_live_confirmation(self):
        with patch("builtins.input", return_value="I UNDERSTAND") as prompt:
            self.assertFalse(app.confirm_exact_card(self.intent, live=True, auto_confirm=True))
        self.assertIn(self.intent.signal_id, prompt.call_args.args[0])
        with patch("builtins.input", return_value="CONFIRM " + self.intent.signal_id):
            self.assertTrue(app.confirm_exact_card(self.intent, live=True, auto_confirm=True))

    def test_live_card_precedes_prompt_and_rejection_never_constructs_transport(self):
        events = []
        with patch.object(app, "trading_enabled", return_value=True), \
             patch.object(app, "load_live_state", return_value=self.state), \
             patch.object(app, "signal_from_csv", return_value=self.signal), \
             patch.object(app, "render_card", side_effect=lambda *a: events.append("card")), \
             patch("builtins.input", side_effect=lambda *a: events.append("prompt") or "n"), \
             patch.object(app, "OrderRouter", side_effect=AssertionError("submission forbidden")):
            self.assertEqual(app.main(["--live", "--signal-id", self.signal["id"], "--yes"]), 0)
        self.assertEqual(events, ["card", "prompt"])

    def test_timeout_calls_router_cancel_and_reconcile(self):
        router = SimpleNamespace(execute=Mock(return_value=self.result("PENDING")),
            cancel_and_reconcile=Mock(return_value=self.result()),
            transport=SimpleNamespace(cancel_entry=Mock(side_effect=AssertionError("raw cancellation forbidden"))))
        self.assertEqual(app.execute_and_watch(router, self.intent, 0).status, "CANCELED")
        router.cancel_and_reconcile.assert_called_once_with(self.intent)
        router.transport.cancel_entry.assert_not_called()

    def test_timeout_race_keeps_newly_protected_fill_result(self):
        protected = self.result("PROTECTED", filled="1", protected="1")
        router = SimpleNamespace(execute=Mock(return_value=self.result("PENDING")),
            cancel_and_reconcile=Mock(return_value=protected))
        self.assertIs(app.execute_and_watch(router, self.intent, 0), protected)

    def test_interrupt_cleans_only_the_existing_route(self):
        router = SimpleNamespace(execute=Mock(side_effect=KeyboardInterrupt()),
            cancel_and_reconcile=Mock(return_value=self.result()))
        with self.assertRaises(KeyboardInterrupt):
            app.execute_and_watch(router, self.intent, 1)
        router.cancel_and_reconcile.assert_called_once_with(self.intent)

    def test_invalid_watch_seconds_cannot_submit(self):
        router = Mock()
        for value in (-1, float("nan"), float("inf")):
            with self.assertRaisesRegex(app.EntryRefused, "WATCH_SECONDS_INVALID"):
                app.execute_and_watch(router, self.intent, value)
        router.execute.assert_not_called()

    def test_verified_fresh_live_state_passes_without_fabricating_flags(self):
        with self.provider(self.state), patch.object(app, "now_ms", return_value=app.DEMO_NOW):
            self.assertEqual(app.load_live_state(self.cfg), self.state)

    def test_live_state_missing_fields_wrong_boolean_stale_or_future_reject(self):
        variants = []
        for key in ("live_verified", "macro_blackout", "entries_today", "realized_pnl_today"):
            state = dict(self.state)
            del state[key]
            variants.append(state)
        variants += [dict(self.state, live_verified=False), dict(self.state, macro_blackout="false"),
            dict(self.state, observed_at_ms=app.DEMO_NOW - 5001),
            dict(self.state, observed_at_ms=app.DEMO_NOW + 1)]
        for state in variants:
            with self.subTest(state=state), self.provider(state), patch.object(app, "now_ms", return_value=app.DEMO_NOW):
                with self.assertRaises(app.EntryRefused):
                    app.load_live_state(self.cfg)

    def test_unavailable_live_provider_has_no_risk_state_fallback(self):
        with patch.dict(sys.modules, {"src.live_state": None}):
            with self.assertRaisesRegex(app.EntryRefused, "LIVE_STATE_UNAVAILABLE"):
                app.load_live_state(self.cfg)

    def test_live_provider_exception_is_not_printed(self):
        provider = SimpleNamespace(live_state_provider=Mock(side_effect=RuntimeError("SECRET-MARKER")))
        with patch.dict(sys.modules, {"src.live_state": provider}):
            with self.assertRaisesRegex(app.EntryRefused, "LIVE_STATE_UNAVAILABLE") as raised:
                app.load_live_state(self.cfg)
        self.assertNotIn("SECRET-MARKER", str(raised.exception))

    def test_market_uses_exchange_timestamp(self):
        payload = {"code": "0", "data": [{"instId": app.INST, "last": "100", "ts": "12345"}]}
        with patch("src.util.http_request", return_value=json.dumps(payload).encode()), \
             patch.object(app, "now_ms", side_effect=AssertionError("receipt time forbidden")):
            self.assertEqual(app.fetch_market_snapshot(), {"last": "100", "timestamp_ms": 12345})

    def test_market_without_source_timestamp_rejects(self):
        payload = {"code": "0", "data": [{"instId": app.INST, "last": "100"}]}
        with patch("src.util.http_request", return_value=json.dumps(payload).encode()):
            with self.assertRaisesRegex(app.EntryRefused, "MARKET_UNAVAILABLE"):
                app.fetch_market_snapshot()

    def test_risk_can_only_be_reduced_without_mutating_config(self):
        before = copy.deepcopy(self.cfg)
        self.assertEqual(app.apply_risk_reduction(self.cfg, 1)["risk"]["risk_per_trade_pct"], 1)
        self.assertEqual(self.cfg, before)
        for risk in (0, -1, 2.01, 5, float("nan")):
            with self.assertRaises(app.EntryRefused):
                app.apply_risk_reduction(self.cfg, risk)

    def test_postflight_canceled_rejects_residual_entry_or_position(self):
        for exposure in (dict(positions=[], pending=[{"clOrdId": "leftover"}], algos=[]),
                         dict(positions=[{"pos": "0.01"}], pending=[], algos=[])):
            with patch.object(app, "read_live_exposure", return_value=exposure):
                with self.assertRaises(app.EntryRefused):
                    app.verify_live_result(Mock(), self.intent, self.result())

    def test_postflight_canceled_confirms_flat(self):
        with patch.object(app, "read_live_exposure", return_value=dict(positions=[], pending=[], algos=[])):
            app.verify_live_result(Mock(), self.intent, self.result())

    def test_postflight_protected_requires_exact_position_and_bracket(self):
        transport = app.MockTransport()
        router = OrderRouter(transport, config=self.cfg, state_provider=lambda: self.state,
            market_provider=lambda: dict(last="100", timestamp_ms=app.DEMO_NOW),
            clock=lambda: app.DEMO_NOW, log_root=self.directory)
        result = router.execute(self.intent)
        self.assertEqual(result.status, "PROTECTED")
        exposure = dict(positions=[{"instId": app.INST, "posSide": "net", "pos": str(result.filled_qty)}], pending=[],
            algos=list(transport.protections.values()))
        with patch.object(app, "read_live_exposure", return_value=exposure):
            app.verify_live_result(router, self.intent, result)
            next(iter(transport.protections.values()))["reduceOnly"] = "false"
            with self.assertRaisesRegex(app.EntryRefused, "POSTFLIGHT_PROTECTION"):
                app.verify_live_result(router, self.intent, result)

    def test_postflight_failure_reconciles_only_existing_route_and_stays_halted(self):
        router = SimpleNamespace(cancel_and_reconcile=Mock(return_value=self.result()))
        with patch.object(app, "verify_live_result", side_effect=[app.EntryRefused("POSTFLIGHT_PROTECTION"), None]), \
             patch.object(app, "halt_local") as halt:
            with self.assertRaisesRegex(app.EntryRefused, "POSTFLIGHT_PROTECTION"):
                app.postflight_or_cleanup(router, self.intent, self.result("PROTECTED", filled="1", protected="1"))
        router.cancel_and_reconcile.assert_called_once_with(self.intent)
        halt.assert_called_once_with("POSTFLIGHT_PROTECTION")

    def test_postflight_cleanup_unknown_never_reports_confirmed_safety(self):
        router = SimpleNamespace(cancel_and_reconcile=Mock(return_value=self.result("UNKNOWN")))
        with patch.object(app, "verify_live_result", side_effect=app.EntryRefused("POSTFLIGHT_UNCONFIRMED")), \
             patch.object(app, "halt_local") as halt:
            with self.assertRaises(app.EntryRefused):
                app.postflight_or_cleanup(router, self.intent, self.result("UNKNOWN"))
        halt.assert_called_once()
        self.assertIn("未确认", self.output.getvalue())
        self.assertNotIn("已独立核对", self.output.getvalue())

    def test_postflight_protected_rejects_an_extra_algo(self):
        transport = app.MockTransport()
        router = OrderRouter(transport, config=self.cfg, state_provider=lambda: self.state,
            market_provider=lambda: dict(last="100", timestamp_ms=app.DEMO_NOW),
            clock=lambda: app.DEMO_NOW, log_root=self.directory)
        result = router.execute(self.intent)
        exposure = dict(positions=[{"instId": app.INST, "posSide": "net", "pos": str(result.filled_qty)}],
            pending=[], algos=list(transport.protections.values()) + [{"algoId": "unrelated"}])
        with patch.object(app, "read_live_exposure", return_value=exposure):
            with self.assertRaisesRegex(app.EntryRefused, "POSTFLIGHT_EXTRA_PROTECTION"):
                app.verify_live_result(router, self.intent, result)

    def test_confirmed_live_ui_refreshes_provider_before_each_new_submission_check(self):
        # UI labels are live; the injected transport remains strictly mode=mock.
        transport = app.MockTransport()
        native_module = SimpleNamespace(NativeOkxTransport=lambda: transport)
        with patch.object(app, "trading_enabled", return_value=True), \
             patch.object(app, "load_live_state", side_effect=lambda cfg: dict(self.state)) as state_call, \
             patch.object(app, "fetch_market_snapshot", return_value=dict(last="100", timestamp_ms=app.DEMO_NOW)) as market_call, \
             patch.object(app, "signal_from_csv", return_value=self.signal), \
             patch.object(app, "verify_live_result") as postflight, \
             patch("builtins.input", return_value="CONFIRM " + self.signal["id"]), \
             patch.dict(sys.modules, {"src.okx_native": native_module}), \
             patch("src.order_router.time.time_ns", return_value=app.DEMO_NOW * 1000000):
            self.assertEqual(app.main(["--live", "--signal-id", self.signal["id"], "--log-root", str(self.directory)]), 0)
        self.assertGreaterEqual(state_call.call_count, 3)
        self.assertGreaterEqual(market_call.call_count, 2)
        postflight.assert_called_once()


if __name__ == "__main__":
    unittest.main()
