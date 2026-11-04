# -*- coding: utf-8 -*-
"""Offline T2 acceptance tests; no live transport, credentials, or shared ledgers."""
from __future__ import annotations

import copy
import csv
import socket
import tempfile
import unittest
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import patch

from src import okx_private
from src.execution import build_intent
from src.order_router import NativeTransport, OrderRouter, RouterError, client_order_id
from test_execution import NOW, inputs


D = Decimal


class FakeTransport:
    """An exchange-shaped in-memory fixture, independent of the router internals."""

    mode = "mock"

    def __init__(self):
        self.calls = []
        self.intent = None
        self.entry_id = None
        self.entry_snapshots = []
        self.entry_overrides = {}
        self.submit_error = None
        self.submit_ack = {"sCode": "0", "ordId": "entry001"}
        self.recover_submission = True
        self.protection_error = None
        self.protection_created_before_error = False
        self.protection_ack = {"sCode": "0", "algoId": "protect001"}
        self.protection_overrides = {}
        self.protection = None
        self.cancel_ack = {"sCode": "0", "ordId": "entry001"}
        self.close_error = None
        self.close_ack = {"sCode": "0", "ordId": "close001"}
        self.close_state = "filled"
        self.close_fill = None
        self.close_orders = {}
        self.close_overrides = {}
        self.read_errors = 0

    def submit_entry(self, intent, cl_ord_id):
        self.calls.append(("submit", intent, cl_ord_id))
        self.intent, self.entry_id = intent, cl_ord_id
        if self.submit_error:
            raise self.submit_error
        return copy.deepcopy(self.submit_ack)

    def get_order(self, inst_id, cl_ord_id):
        self.calls.append(("get_order", inst_id, cl_ord_id))
        if self.read_errors:
            self.read_errors -= 1
            raise TimeoutError("read temporarily unavailable")
        if cl_ord_id in self.close_orders:
            side, qty, ord_id = self.close_orders[cl_ord_id]
            snapshot = {"instId": inst_id, "clOrdId": cl_ord_id, "ordId": ord_id,
                        "state": self.close_state, "accFillSz": str(qty if self.close_fill is None else self.close_fill),
                        "sz": str(qty), "avgPx": "100", "side": side,
                        "posSide": "net", "ordType": "market", "reduceOnly": "true"}
            snapshot.update(self.close_overrides)
            return snapshot
        if self.intent is None or cl_ord_id != self.entry_id:
            return None
        if self.submit_error and not self.recover_submission:
            return None
        if self.entry_snapshots:
            state, filled = self.entry_snapshots[0]
            if len(self.entry_snapshots) > 1:
                self.entry_snapshots.pop(0)
        else:
            state, filled = "filled", self.intent.qty
        snapshot = {"instId": inst_id, "clOrdId": cl_ord_id, "ordId": "entry001",
                    "state": state, "accFillSz": str(filled), "sz": str(self.intent.qty),
                    "avgPx": str(self.intent.entry) if D(str(filled)) else "",
                    "side": "buy" if self.intent.side == "long" else "sell", "posSide": "net",
                    "ordType": "post_only" if self.intent.require_maker else "limit"}
        snapshot.update(self.entry_overrides)
        return snapshot

    def cancel_entry(self, inst_id, cl_ord_id):
        self.calls.append(("cancel", inst_id, cl_ord_id))
        return copy.deepcopy(self.cancel_ack)

    def set_protection(self, intent, qty, protection_id, *, reduce_only=True):
        self.calls.append(("protect", qty, protection_id, reduce_only))
        if self.protection_error and not self.protection_created_before_error:
            raise self.protection_error
        if self.protection_ack.get("sCode") == "0":
            self.protection = {"instId": intent.inst_id, "algoClOrdId": protection_id,
                               "algoId": "protect001", "state": "live", "ordType": "oco",
                               "side": "sell" if intent.side == "long" else "buy", "posSide": "net",
                               "sz": str(qty), "slTriggerPx": str(intent.stop), "slOrdPx": "-1",
                               "tpTriggerPx": str(intent.tp1), "tpOrdPx": "-1", "reduceOnly": "true"}
            self.protection.update(self.protection_overrides)
        if self.protection_error:
            raise self.protection_error
        return copy.deepcopy(self.protection_ack)

    def get_protection(self, inst_id, protection_id):
        self.calls.append(("get_protection", inst_id, protection_id))
        return copy.deepcopy(self.protection)

    def emergency_close(self, inst_id, side, qty, cl_ord_id, *, reduce_only=True):
        self.calls.append(("close", inst_id, side, qty, cl_ord_id, reduce_only))
        if self.close_error:
            raise self.close_error
        response = copy.deepcopy(self.close_ack)
        if response.get("ordId") == "close001":
            response["ordId"] = f"close{len(self.close_orders) + 1:03d}"
        self.close_orders[cl_ord_id] = (side, qty, response.get("ordId", ""))
        return response

    def named(self, operation):
        return [call for call in self.calls if call[0] == operation]


class RouterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.signal, self.state, self.config = inputs()
        self.intent = build_intent(self.signal, self.state, self.config)
        self.market = {"last": "100", "timestamp_ms": NOW}
        self.transport = FakeTransport()
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(socket, "socket", side_effect=AssertionError("network forbidden in T2 tests")))
        for name in ("creds", "private", "place_order", "place_oco", "place_algo_stop",
                     "cancel_order", "cancel_algo", "amend_algo"):
            stack.enter_context(patch.object(okx_private, name,
                                             side_effect=AssertionError("native private API forbidden")))

    def router(self, **kwargs):
        options = {"config": self.config,
                   "state_provider": lambda: copy.deepcopy(self.state),
                   "market_provider": lambda: copy.deepcopy(self.market),
                   "clock": lambda: NOW, "log_root": self.root,
                   "max_read_attempts": 3}
        options.update(kwargs)
        transport = options.pop("transport", self.transport)
        return OrderRouter(transport, **options)

    def execute(self, intent=None, **kwargs):
        return self.router(**kwargs).execute(self.intent if intent is None else intent)

    def assert_rejected_without_submission(self, code, intent=None):
        result = self.execute(intent)
        self.assertEqual((result.status, result.code), ("REJECTED", code))
        self.assertFalse(self.transport.named("submit"))
        self.assertFalse(self.transport.named("protect"))
        self.assertFalse(self.transport.named("close"))
        return result

    def test_client_order_id_is_stable_valid_wire_id(self):
        for signal_id in ("S123", "a" * 32):
            self.assertEqual(client_order_id(signal_id), signal_id)
        for signal_id in (self.intent.signal_id, "S" * 33, "信号甲", "S_1"):
            wire_id = client_order_id(signal_id)
            self.assertRegex(wire_id, r"^[A-Za-z0-9]{1,32}$")
            self.assertEqual(len(wire_id), 32)
            self.assertEqual(wire_id, client_order_id(signal_id))
        self.assertNotEqual(client_order_id("A-B"), client_order_id("AB"))

    def test_acknowledgement_without_fill_never_creates_protection(self):
        self.transport.entry_snapshots = [("live", D("0"))]
        result = self.execute()
        self.assertEqual((result.status, result.code), ("PENDING", "PENDING"))
        self.assertEqual(result.filled_qty, 0)
        self.assertEqual(result.protected_qty, 0)
        self.assertFalse(self.transport.named("protect"))
        self.assertFalse(self.transport.named("close"))

    def test_filled_entry_is_confirmed_then_protected_and_verified(self):
        result = self.execute()
        self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))
        self.assertEqual(result.signal_id, self.intent.signal_id)
        self.assertEqual(result.client_order_id, client_order_id(self.intent.signal_id))
        self.assertEqual(result.filled_qty, self.intent.qty)
        self.assertEqual(result.protected_qty, self.intent.qty)
        names = [call[0] for call in self.transport.calls]
        self.assertLess(names.index("submit"), names.index("get_order"))
        self.assertLess(names.index("get_order"), names.index("protect"))
        self.assertLess(names.index("protect"), names.index("get_protection"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertTrue(all(call[3] for call in self.transport.named("protect")))
        self.assertFalse(self.transport.named("close"))

    def test_short_uses_buy_protection(self):
        signal = {**self.signal, "side": "short", "stop": "101", "tp1": "97", "tp2": "96"}
        intent = build_intent(signal, self.state, self.config)
        result = self.execute(intent)
        self.assertEqual(result.status, "PROTECTED")
        self.assertEqual(self.transport.protection["side"], "buy")

    def test_protection_trigger_feed_defaults_and_explicit_last_remain_protected(self):
        for side in ("long", "short"):
            signal = self.signal if side == "long" else {**self.signal, "side": "short", "stop": "101", "tp1": "97", "tp2": "96"}
            intent = build_intent(signal, self.state, self.config)
            for index, changes in enumerate(({}, {"slTriggerPxType": ""}, {"tpTriggerPxType": ""},
                    {"slTriggerPxType": "last"}, {"tpTriggerPxType": "last"},
                    {"slTriggerPxType": "", "tpTriggerPxType": "last"},
                    {"slTriggerPxType": "last", "tpTriggerPxType": ""})):
                with self.subTest(side=side, changes=changes):
                    transport = FakeTransport()
                    transport.protection_overrides = changes
                    result = self.router(transport=transport, log_root=self.root/(side+str(index))).execute(intent)
                    self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))
                    self.assertEqual(result.protected_qty, intent.qty)
                    self.assertFalse(transport.named("close"))

    def test_wrong_protection_trigger_feed_emergency_closes_and_preserves_halt(self):
        case = 0
        for side in ("long", "short"):
            signal = self.signal if side == "long" else {**self.signal, "side": "short", "stop": "101", "tp1": "97", "tp2": "96"}
            intent = build_intent(signal, self.state, self.config)
            for field in ("slTriggerPxType", "tpTriggerPxType"):
                for value in ("mark", "index", "INVALID", None, False, [], {}):
                    with self.subTest(side=side, field=field, value=value):
                        case += 1
                        transport = FakeTransport()
                        transport.protection_overrides = {field: value}
                        router = self.router(transport=transport, log_root=self.root/str(case))
                        result = router.execute(intent)
                        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
                        self.assertEqual(result.protected_qty, 0)
                        self.assertEqual(result.emergency_status, "FLAT_CONFIRMED")
                        closes = transport.named("close")
                        self.assertEqual(len(closes), 1)
                        self.assertEqual((closes[0][2], closes[0][3], closes[0][5]),
                                         ("sell" if side == "long" else "buy", intent.qty, True))
                        again = router.execute(replace(intent, signal_id="AFTERHALT001"))
                        self.assertEqual((again.status, again.code), ("REJECTED", "HALTED"))
                        self.assertEqual(len(transport.named("submit")), 1)

    def test_partial_fill_is_protected_before_cancel_and_final_fill_is_covered(self):
        first_qty = self.intent.qty / 2
        self.transport.entry_snapshots = [("partially_filled", first_qty),
                                           ("canceled", self.intent.qty)]
        result = self.execute()
        self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))
        protections = self.transport.named("protect")
        self.assertEqual(protections[0][1], first_qty)
        self.assertEqual(protections[-1][1], self.intent.qty)
        self.assertEqual(len({call[2] for call in protections}), 1)
        names = [call[0] for call in self.transport.calls]
        self.assertLess(names.index("protect"), names.index("cancel"))
        self.assertEqual(result.protected_qty, self.intent.qty)

    def test_duplicate_signal_never_posts_twice_in_one_run_or_next_run(self):
        router = self.router()
        first = router.execute(self.intent)
        second = router.execute(self.intent)
        restarted = self.router().execute(self.intent)
        self.assertEqual(first.status, "PROTECTED")
        self.assertEqual(second.status, "PROTECTED")
        self.assertEqual(restarted.status, "PROTECTED")
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertEqual(len(self.transport.named("protect")), 1)

    def test_same_signal_cannot_change_approved_payload(self):
        self.execute()
        result = self.execute(replace(self.intent, stop=D("99.01")))
        self.assertEqual((result.status, result.code), ("REJECTED", "IDEMPOTENCY_CONFLICT"))
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_other_signal_cannot_reuse_reserved_position_slot_after_restart(self):
        self.transport.entry_snapshots = [("live", D("0"))]
        self.execute()
        result = self.execute(replace(self.intent, signal_id="ANOTHER001"))
        self.assertEqual((result.status, result.code), ("REJECTED", "POSITION_LIMIT"))
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_submit_timeout_recovers_by_lookup_without_second_post(self):
        self.transport.submit_error = TimeoutError("ambiguous submission")
        result = self.execute()
        self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertTrue(self.transport.named("get_order"))

    def test_rejected_entry_is_not_retried_or_protected(self):
        self.transport.submit_ack = {"sCode": "51008", "sMsg": "insufficient balance"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("REJECTED", "ENTRY_REJECTED"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertFalse(self.transport.named("protect"))
        self.assertFalse(self.transport.named("close"))

    def test_wrong_entry_snapshot_cannot_be_used_to_verify_a_fill(self):
        self.transport.entry_overrides = {"clOrdId": "UnrelatedOrder"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("UNKNOWN", "ORDER_MISMATCH"))
        self.assertFalse(self.transport.named("protect"))
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_entry_requires_net_mode_and_approved_post_only_type(self):
        for override in ({"posSide": "long"}, {"ordType": "limit"}):
            with self.subTest(override=override):
                self.transport = FakeTransport()
                self.transport.entry_overrides = override
                result = self.execute(log_root=self.root / str(len(list(self.root.iterdir()))))
                self.assertEqual(result.status, "UNKNOWN")
                self.assertFalse(self.transport.named("protect"))
                self.assertEqual(len(self.transport.named("submit")), 1)

    def test_concurrent_same_signal_has_only_one_entry_submission(self):
        first_router, second_router = self.router(), self.router()
        submitted, release = Event(), Event()
        submit = self.transport.submit_entry

        def blocking_submit(intent, cl_ord_id):
            submitted.set()
            if not release.wait(5):
                raise AssertionError("test synchronization timed out")
            return submit(intent, cl_ord_id)

        with patch.object(self.transport, "submit_entry", side_effect=blocking_submit):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(first_router.execute, self.intent)
                try:
                    self.assertTrue(submitted.wait(5))
                    concurrent = second_router.execute(self.intent)
                    self.assertEqual((concurrent.status, concurrent.code), ("REJECTED", "IN_PROGRESS"))
                finally:
                    release.set()
                self.assertEqual(pending.result(timeout=5).status, "PROTECTED")
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_unknown_submit_is_not_retried_and_blocks_next_run(self):
        self.transport.submit_error = TimeoutError("ambiguous submission")
        self.transport.recover_submission = False
        result = self.execute()
        self.assertEqual((result.status, result.code), ("UNKNOWN", "SUBMISSION_UNKNOWN"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertEqual(len(self.transport.named("cancel")), 1)
        second = self.execute(replace(self.intent, signal_id="NEXT001"))
        self.assertEqual((second.status, second.code), ("REJECTED", "HALTED"))
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_transient_read_error_retries_only_read(self):
        self.transport.read_errors = 1
        result = self.execute()
        self.assertEqual(result.status, "PROTECTED")
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertEqual(len(self.transport.named("get_order")), 2)

    def test_protection_timeout_can_recover_by_query_without_second_post(self):
        self.transport.protection_error = TimeoutError("protection outcome unknown")
        self.transport.protection_created_before_error = True
        result = self.execute()
        self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))
        self.assertEqual(len(self.transport.named("protect")), 1)
        self.assertFalse(self.transport.named("close"))

    def test_read_retries_are_bounded(self):
        self.transport.read_errors = 99
        result = self.execute()
        self.assertIn(result.status, ("UNKNOWN", "HALTED"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertLessEqual(len(self.transport.named("get_order")), 6)
        self.assertEqual(len(self.transport.named("cancel")), 1)
        self.assertFalse(self.transport.named("protect"))

    def test_protection_rejection_emergency_closes_reduce_only_and_halts(self):
        self.transport.protection_ack = {"sCode": "51000", "sMsg": "rejected"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        close = self.transport.named("close")
        self.assertEqual(len(close), 1)
        self.assertEqual(close[0][2], "sell")
        self.assertEqual(close[0][3], self.intent.qty)
        self.assertTrue(close[0][5])
        self.assertEqual(result.emergency_status, "FLAT_CONFIRMED")
        self.assertTrue(any(call[2] == close[0][4] for call in self.transport.named("get_order")))
        second = self.execute(replace(self.intent, signal_id="AFTERHALT001"))
        self.assertEqual((second.status, second.code), ("REJECTED", "HALTED"))
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_partial_fill_protection_failure_cancels_and_checks_final_fill_before_close(self):
        self.transport.entry_snapshots = [("partially_filled", D("0.50")),
                                           ("canceled", D("0.75"))]
        self.transport.protection_ack = {"sCode": "51000"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        self.assertEqual(self.transport.named("close")[0][3], D("0.75"))
        names = [call[0] for call in self.transport.calls]
        self.assertLess(names.index("cancel"), names.index("close"))
        between = names[names.index("cancel") + 1:names.index("close")]
        self.assertIn("get_order", between)

    def test_incorrect_protection_quantity_does_not_count_as_protected(self):
        self.transport.protection_overrides = {"sz": "0.01"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        self.assertTrue(self.transport.named("close"))

    def test_wrong_side_or_widened_stop_is_rejected_as_invalid_protection(self):
        for override in ({"side": "buy"}, {"slTriggerPx": "98"}, {"tpTriggerPx": "102"},
                         {"instId": "BTC-USDT-SWAP"}, {"state": "canceled"},
                         {"reduceOnly": "false"}, {"reduceOnly": None}):
            with self.subTest(override=override):
                self.transport = FakeTransport()
                self.transport.protection_overrides = override
                result = self.execute(log_root=self.root / str(len(list(self.root.iterdir()))))
                self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
                self.assertTrue(self.transport.named("close"))

    def test_cancel_race_additional_fill_gets_a_separate_reduce_only_close(self):
        self.transport.entry_snapshots = [("partially_filled", D("0.50")),
                                           ("partially_filled", D("0.50")),
                                           ("canceled", D("0.75")),
                                           ("canceled", D("0.75"))]
        self.transport.protection_ack = {"sCode": "51000"}
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        closes = self.transport.named("close")
        self.assertEqual(sum((call[3] for call in closes), D("0")), D("0.75"))
        self.assertEqual(len(closes), 2)
        self.assertEqual(len({call[4] for call in closes}), 2)
        self.assertTrue(all(call[5] for call in closes))
        self.assertEqual(result.emergency_status, "FLAT_CONFIRMED")

    def test_audit_io_failure_after_fill_still_attempts_emergency_close(self):
        for event in ("entry_observed", "protection_request"):
            with self.subTest(event=event):
                self.transport = FakeTransport()
                router = self.router(log_root=self.root / event)
                real_audit = router._audit

                def failed_audit(row, actual_event):
                    if actual_event == event:
                        raise OSError("simulated ledger failure")
                    return real_audit(row, actual_event)

                with patch.object(router, "_audit", side_effect=failed_audit):
                    result = router.execute(self.intent)
                self.assertEqual(result.status, "HALTED")
                self.assertTrue(self.transport.named("close"))
                self.assertTrue(all(call[5] for call in self.transport.named("close")))

    def test_emergency_close_failure_remains_halted_and_does_not_retry_post(self):
        self.transport.protection_ack = {"sCode": "51000"}
        self.transport.close_error = TimeoutError("close outcome unknown")
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        self.assertEqual(len(self.transport.named("close")), 1)
        self.assertEqual(result.protected_qty, 0)
        self.assertEqual(result.emergency_status, "CLOSE_UNCONFIRMED")
        replay = self.execute()
        self.assertEqual(replay.status, "HALTED")
        self.assertEqual(len(self.transport.named("close")), 1)

    def test_close_acknowledgement_is_not_treated_as_confirmed_flat(self):
        self.transport.protection_ack = {"sCode": "51000"}
        self.transport.close_state = "live"
        self.transport.close_fill = D("0")
        result = self.execute()
        self.assertEqual((result.status, result.code), ("HALTED", "PROTECTION_FAILED"))
        close_id = self.transport.named("close")[0][4]
        self.assertTrue(any(call[2] == close_id for call in self.transport.named("get_order")))
        self.assertEqual(result.emergency_status, "CLOSE_UNCONFIRMED")

    def test_emergency_close_verifies_quantity_identity_and_reduce_only_mode(self):
        for override in ({"sz": "0.01"}, {"ordId": "UnrelatedClose"}, {"posSide": "long"},
                         {"ordType": "limit"}, {"reduceOnly": "false"}):
            with self.subTest(override=override):
                self.transport = FakeTransport()
                self.transport.protection_ack = {"sCode": "51000"}
                self.transport.close_overrides = override
                result = self.execute(log_root=self.root / str(len(list(self.root.iterdir()))))
                self.assertEqual(result.status, "HALTED")
                self.assertEqual(result.emergency_status, "CLOSE_UNCONFIRMED")

    def test_hard_400_notional_ceiling_cannot_be_forged(self):
        forged = replace(self.intent, qty=D("4.01"), notional=D("401"))
        self.assert_rejected_without_submission("NOTIONAL_LIMIT", forged)

    def test_configured_lower_notional_ceiling_is_applied_at_submission(self):
        self.config["max_notional"] = "100"
        self.assert_rejected_without_submission("NOTIONAL_LIMIT")

    def test_live_equity_times_leverage_is_applied_at_submission(self):
        self.config["account"]["leverage_setting"] = "1"
        self.assert_rejected_without_submission("NOTIONAL_LIMIT")

    def test_market_deviation_exactly_one_point_five_percent_is_accepted(self):
        signal = {**self.signal, "entry": "101.50", "stop": "100.50", "tp1": "104.50", "tp2": "105.50"}
        intent = build_intent(signal, self.state, self.config)
        result = self.execute(intent)
        self.assertEqual((result.status, result.code), ("PROTECTED", "OK"))

    def test_market_deviation_over_boundary_is_rejected_before_submission(self):
        signal = {**self.signal, "entry": "101.51", "stop": "100.51", "tp1": "104.51", "tp2": "105.51"}
        intent = build_intent(signal, self.state, self.config)
        self.assert_rejected_without_submission("PRICE_DEVIATION", intent)

    def test_stale_market_is_rejected_before_submission(self):
        self.market["timestamp_ms"] = NOW - 61000
        self.assert_rejected_without_submission("MARKET_STALE")

    def test_changed_risk_state_is_rechecked_before_submission(self):
        self.state["equity_estimate"] = "100"
        self.assert_rejected_without_submission("RISK_CHANGED")

    def test_daily_entry_limit_is_rechecked_before_submission(self):
        self.state["entries_today"] = 3
        self.assert_rejected_without_submission("DAILY_ENTRIES")

    def test_second_preflight_observes_changed_market_and_releases_slot(self):
        markets = iter([dict(self.market), {**self.market, "last": "110"}])
        router = self.router(market_provider=lambda: next(markets))
        result = router.execute(self.intent)
        self.assertEqual((result.status, result.code), ("REJECTED", "PRICE_DEVIATION"))
        self.assertFalse(self.transport.named("submit"))
        next_result = self.execute(replace(self.intent, signal_id="MARKETRECOVER001"))
        self.assertEqual(next_result.status, "PROTECTED")

    def test_every_router_run_has_its_own_audit_directory(self):
        first = self.execute()
        second = self.execute()
        self.assertNotEqual(Path(first.run_dir), Path(second.run_dir))
        for result in (first, second):
            audit_path = Path(result.run_dir) / "orders.csv"
            self.assertTrue(audit_path.is_file())
            with audit_path.open(encoding="utf-8-sig", newline="") as handle:
                records = list(csv.DictReader(handle))
            self.assertTrue(records)

    def test_audit_never_serializes_raw_exceptions_or_headers(self):
        secret_marker = "DO_NOT_PERSIST_CREDENTIAL_8841"
        self.transport.submit_error = TimeoutError("Authorization: " + secret_marker)
        self.transport.recover_submission = False
        self.execute()
        for file in self.root.rglob("*"):
            if file.is_file():
                self.assertNotIn(secret_marker.encode(), file.read_bytes(), str(file))
                self.assertNotIn(b"Authorization:", file.read_bytes(), str(file))

    def test_default_native_path_stays_disabled_and_cannot_call_private_api(self):
        with patch.dict("os.environ", {}, clear=True):
            result = self.router(transport=None).execute(self.intent)
        self.assertEqual((result.status, result.code), ("REJECTED", "TRADING_DISABLED"))
        self.assertFalse(self.transport.calls)

    def test_native_adapter_remains_locked_with_only_a_mock_switch_provider(self):
        # Replace this module's OS reference, never the actual process environment.
        fake_os = SimpleNamespace(environ={"OKX_TRADING_ENABLED": "1"})
        with patch("src.order_router.os", fake_os):
            for method in ("submit_entry", "get_order", "cancel_entry", "set_protection",
                           "get_protection", "emergency_close"):
                with self.subTest(method=method), self.assertRaises(RouterError) as caught:
                    getattr(NativeTransport(), method)()
                self.assertEqual(caught.exception.code, "NATIVE_UNAVAILABLE")
            result = self.router(transport=None).execute(self.intent)
        self.assertEqual((result.status, result.code), ("REJECTED", "NATIVE_UNAVAILABLE"))
        self.assertFalse(self.transport.calls)


if __name__ == "__main__":
    unittest.main()
