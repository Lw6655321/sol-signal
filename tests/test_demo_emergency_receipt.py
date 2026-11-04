"""Typed emergency closure proof over bounded fake HTTP; never runtime/account IO."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal, localcontext
import hashlib
import json
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from src import demo_route_transport as T
from src.order_router import _derived_id
import test_demo_halted_exit_receipts as C
from test_execution import NOW


class EmergencyClosedReceiptTests(unittest.TestCase):
    def setUp(self):
        self.f = C.ClosedHaltedReceipts()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.stack.enter_context(patch.object(T.time, "time_ns", return_value=NOW*1_000_000))
        self.f.child.update(clOrdId=_derived_id(self.f.intent.signal_id, "emergency:0:"+str(self.f.intent.qty)),
                            algoId="", algoClOrdId="")
        self.f.fills[1]["clOrdId"] = self.f.child["clOrdId"]
        self.f.history.update(pnl=self.f.fills[1]["fillPnl"], fee="-0.02", fundingFee="0",
                              realizedPnl=str(Decimal(self.f.fills[1]["fillPnl"])-Decimal("0.02")))
        self.ids = ["301"]

    def verify(self, **changes):
        args = dict(intent=self.f.intent, order_id="101", client_id=self.f.client,
                    filled_qty=self.f.intent.qty, flat_observed_at_ms=self.f.flat,
                    close_order_ids=self.ids, expected_account_hash=self.f.account)
        args.update(changes)
        return self.f.transport.verify_emergency_closed_lifecycle(**args)

    def refused(self, code=None, **changes):
        with self.assertRaises(T.DemoRouteError) as caught:
            self.verify(**changes)
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.f.posts, [])

    def test_typed_receipt_is_nonadmitting_and_preserves_all_evidence(self):
        before = deepcopy((self.f.intent.to_dict(), self.ids, self.f.entry, self.f.child, self.f.fills, self.f.history))
        proof = self.verify()
        self.assertEqual(proof["purpose"], "HALTED_EMERGENCY_CLOSURE_RECEIPT_ONLY")
        for key in ("emergency_closed_lifecycle_verified", "position_binding_verified", "closed_lifecycle_verified", "diagnostic_only"):
            self.assertIs(proof[key], True)
        self.assertIs(proof["can_resume"], False)
        self.assertIs(proof["can_activate"], False)
        self.assertNotIn("exit_reconciled", proof)
        self.assertEqual(proof["intent_sha256"], hashlib.sha256(json.dumps(self.f.intent.to_dict(), sort_keys=True,
            separators=(",", ":")).encode()).hexdigest())
        self.assertEqual(proof["order"], self.f.entry)
        self.assertEqual(proof["closes"], [self.f.child])
        self.assertEqual(proof["fills"], self.f.fills)
        self.assertEqual(proof["position_history"], self.f.history)
        self.assertEqual(proof["account_hash"], self.f.account)
        self.assertEqual(proof["exposure_algo_types"], list(T.ALGO_TYPES))
        self.assertEqual(before, (self.f.intent.to_dict(), self.ids, self.f.entry, self.f.child, self.f.fills, self.f.history))
        self.assertEqual(self.f.posts, [])
        self.assertEqual(self.f.parent_reads, 0)
        for name in ("exposure_raw_before", "exposure_raw_after"):
            self.assertEqual(set(proof[name]), {"positions", "pending", *("algo:"+kind for kind in T.ALGO_TYPES)})
        self.assertEqual(len([path for path, _ in self.f.gets if path.endswith("orders-algo-pending")]), 16)
        self.assertEqual(len(self.f.gets), 30)

    def test_no_write_switch_required_and_no_post_attempted(self):
        self.assertFalse(self.f.transport.enable_writes)
        self.verify()
        self.assertEqual(self.f.posts, [])

    def test_exact_short_emergency_direction_and_role(self):
        intent = self.f.intent
        self.f.intent = replace(intent, side="short", stop=Decimal("101"), tp1=Decimal("97"))
        self.f.entry["side"], self.f.child["side"] = "sell", "buy"
        self.f.fills[0].update(side="sell", subType="4")
        self.f.fills[1].update(side="buy", subType="6", fillPnl=str(-Decimal(self.f.fills[1]["fillPnl"])))
        self.f.history.update(direction="short", pnl=self.f.fills[1]["fillPnl"],
            realizedPnl=str(Decimal(self.f.fills[1]["fillPnl"])-Decimal("0.02")))
        self.assertTrue(self.verify()["emergency_closed_lifecycle_verified"])

    def test_syntactically_valid_unowned_entry_client_is_rejected_before_get(self):
        self.refused("DEMO_EMERGENCY_ENTRY_ID_MISMATCH", client_id="other")
        self.assertEqual(self.f.gets, [])

    def test_invalid_retained_close_sets_reject_before_get(self):
        for value in ([], ["301", "301"], ["301", "302"], ["101"], [301], ("301",), ["x"], ["1", "2", "3", "4", "5"]):
            with self.subTest(value=value):
                self.refused("DEMO_EMERGENCY_CLOSE_IDS_INVALID", close_order_ids=value)
        self.assertEqual(self.f.gets, [])

    def test_partial_entry_is_explicitly_outside_narrow_capability(self):
        self.refused("DEMO_LIFECYCLE_QUANTITY", filled_qty=self.f.intent.qty-self.f.intent.lot_size)
        self.assertEqual(self.f.gets, [])

    def test_missing_account_binding_rejects_before_get(self):
        self.refused("DEMO_EMERGENCY_ACCOUNT_UNVERIFIED", expected_account_hash="PENDING")
        self.assertEqual(self.f.gets, [])

    def test_wrong_account_and_position_mode_refuse(self):
        self.refused("DEMO_EMERGENCY_ACCOUNT_UNVERIFIED", expected_account_hash="a"*64)

    def test_hedged_mode_never_proves_net_emergency_lifecycle(self):
        original = self.f.transport._reader._http_get
        def get(url, headers, timeout):
            if urlsplit(url).path.endswith("/account/config"):
                return 200, {"code": "0", "data": [{"uid": "7", "posMode": "long_short_mode"}]}
            return original(url, headers, timeout)
        with patch.object(self.f.transport._reader, "_http_get", get):
            self.refused("DEMO_EMERGENCY_ACCOUNT_UNVERIFIED")

    def test_failed_concurrent_wave_drains_every_worker_before_returning(self):
        original = self.f.transport._reader._http_get
        active, maximum = 0, 0
        lock = threading.Lock()
        def get(url, headers, timeout):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                if urlsplit(url).path.endswith("/positions-history"):
                    return 200, {"code": "1", "data": []}
                if urlsplit(url).path.endswith("/account/positions"):
                    time.sleep(0.02)
                return original(url, headers, timeout)
            finally:
                with lock:
                    active -= 1
        with patch.object(self.f.transport._reader, "_http_get", get):
            self.refused("DEMO_READ_UNKNOWN")
        self.assertGreaterEqual(maximum, 2)
        self.assertEqual(active, 0)

    def test_tail_window_can_complete_without_changing_any_receipt(self):
        count = 0
        def changed(path, params):
            nonlocal count
            if path.endswith("/public/time"):
                count += 1
                self.f.server = NOW if count == 1 else NOW+1
        self.f.on_get = changed
        with patch.object(T.time, "time_ns", side_effect=[NOW*1_000_000, (NOW+1)*1_000_000]):
            proof = self.verify()
        self.assertEqual(proof["history_through_ms"], NOW+1)
        self.assertEqual(proof["observed_at_ms"], NOW)
        self.assertEqual(proof["fills"], self.f.fills)
        self.assertEqual(len(self.f.gets), 32)

    def test_exact_close_receipt_predicates(self):
        mutations = {"clOrdId": "unrelated", "ordId": "302", "instId": "BTC-USDT-SWAP",
            "side": "buy", "posSide": "long", "tdMode": "isolated", "state": "live",
            "ordType": "limit", "reduceOnly": "false", "algoId": "201", "algoClOrdId": "unrelated",
            "sz": str(self.f.intent.qty+self.f.intent.lot_size), "accFillSz": "0"}
        original = deepcopy(self.f.child)
        for field, value in mutations.items():
            with self.subTest(field=field):
                self.f.child = {**original, field: value}
                self.refused("DEMO_INVALID_NUMBER" if field == "accFillSz" else "DEMO_EMERGENCY_CLOSE_MISMATCH")
        self.f.child = original

    def test_entry_receipt_rejects_wrong_fill_price_size_or_algo(self):
        original = deepcopy(self.f.entry)
        for field, value in {"ordId": "102", "state": "canceled", "px": "101", "reduceOnly": "true",
                             "algoId": "201", "algoClOrdId": "other", "sz": "100"}.items():
            with self.subTest(field=field):
                self.f.entry = {**original, field: value}
                self.refused("DEMO_EMERGENCY_ENTRY_MISMATCH")
        self.f.entry = original

    def test_exchange_normalized_size_spelling_does_not_change_retained_role(self):
        self.f.child["sz"] = str(self.f.intent.qty)+"0"
        self.assertTrue(self.verify()["emergency_closed_lifecycle_verified"])

    def test_retained_quantity_spelling_determines_role_without_trial_guesses(self):
        self.refused("DEMO_EMERGENCY_CLOSE_MISMATCH", filled_qty=Decimal(str(self.f.intent.qty)+"0"))

    def test_partial_request_cannot_impersonate_real_full_emergency_role(self):
        quantity = self.f.intent.qty-self.f.intent.lot_size
        self.f.child.update(sz=str(quantity), accFillSz=str(quantity),
            clOrdId=_derived_id(self.f.intent.signal_id, "emergency:0:"+str(quantity)))
        self.refused("DEMO_EMERGENCY_CLOSE_MISMATCH")

    def test_duplicate_trade_and_bill_ids_refuse(self):
        for field in ("tradeId", "billId"):
            original = self.f.fills[1][field]
            self.f.fills[1][field] = self.f.fills[0][field]
            self.refused("DEMO_LIFECYCLE_DUPLICATE_FILL")
            self.f.fills[1][field] = original

    def test_unfiltered_external_activity_and_wrong_fee_currency_refuse(self):
        original = deepcopy(self.f.fills[1])
        for field, value in {"ordId": "999", "clOrdId": "other", "instType": "FUTURES",
            "feeCcy": "BTC", "subType": "6", "algoId": "201", "side": "buy"}.items():
            with self.subTest(field=field):
                self.f.fills[1] = {**original, field: value}
                self.refused("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")
        self.f.fills[1] = original

    def test_missing_or_incomplete_fills_never_prove_closed(self):
        self.f.fills.pop()
        self.refused("DEMO_LIFECYCLE_FILL_UNPROVEN")

    def test_nonzero_entry_pnl_is_external_activity(self):
        self.f.fills[0]["fillPnl"] = "1"
        self.refused("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY")

    def test_future_and_cross_boundary_fill_times_refuse(self):
        original = deepcopy(self.f.fills)
        for field, value in (("ts", str(NOW+1)), ("ts", str(self.f.closed-1))):
            self.f.fills = deepcopy(original)
            self.f.fills[1][field] = value
            self.refused("DEMO_LIFECYCLE_TIME_INVALID")
        self.f.fills = deepcopy(original)
        self.f.fills[0]["fillTime"] = str(self.f.flat-1)
        self.f.fill_override = self.f.fills
        self.refused("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW")

    def test_fill_quantity_and_weighted_prices_are_exact(self):
        original = deepcopy(self.f.fills[1])
        for field, value, code in (("fillSz", "0.001", "DEMO_LIFECYCLE_QUANTITY"),
                ("fillPx", "104", "DEMO_EMERGENCY_PRICE_MISMATCH")):
            self.f.fills[1] = {**original, field: value}
            self.refused(code)
        self.f.fills[1] = original

    def test_unique_history_position_and_complete_scope(self):
        original = deepcopy(self.f.history)
        for field, value in {"posSide": "long", "mgnMode": "isolated", "direction": "short", "ccy": "BTC",
            "openMaxPos": "100", "closeTotalPos": "0", "liqPenalty": "1", "settledPnl": "1",
            "posId": "999", "cTime": str(self.f.opened+2), "uTime": str(self.f.closed+1)}.items():
            with self.subTest(field=field):
                self.f.history = {**original, field: value}
                self.refused("DEMO_INVALID_NUMBER" if field == "closeTotalPos" else "DEMO_EMERGENCY_HISTORY_MISMATCH")
        self.f.history = original

    def test_duplicate_history_and_unknown_recent_position_refuse(self):
        self.f.history_extra = [deepcopy(self.f.history)]
        self.refused("DEMO_EMERGENCY_HISTORY_MISMATCH")
        self.f.history_extra[0]["posId"] = "402"
        self.refused("DEMO_EMERGENCY_HISTORY_MISMATCH")

    def test_swap_empty_settled_pnl_preserves_raw_empty(self):
        self.f.history["settledPnl"] = ""
        self.assertEqual(self.verify()["position_history"]["settledPnl"], "")

    def test_history_and_fee_accounting_join(self):
        original = deepcopy(self.f.history)
        for field in ("pnl", "fee", "realizedPnl", "fundingFee"):
            with self.subTest(field=field):
                self.f.history = {**original, field: "999"}
                self.refused("DEMO_EMERGENCY_ACCOUNTING_MISMATCH")
        self.f.history = original

    def test_missing_fee_cannot_be_treated_as_zero(self):
        del self.f.fills[1]["fee"]
        self.refused("DEMO_INVALID_NUMBER")

    def test_orders_must_fit_real_execution_and_record_intervals(self):
        original = deepcopy(self.f.child)
        for field, value in {"cTime": str(self.f.opened-1), "uTime": str(self.f.closed+1),
                             "fillTime": str(self.f.closed-1)}.items():
            with self.subTest(field=field):
                self.f.child = {**original, field: value}
                self.refused("DEMO_EMERGENCY_ORDER_TIME_MISMATCH")
        self.f.child = original

    def test_current_position_or_pending_order_prevents_receipt(self):
        self.f.positions = [dict(instId=T.INST, posSide="net", pos="0.01")]
        self.refused("DEMO_EMERGENCY_EXPOSURE_NOT_FLAT")
        self.f.positions = []
        self.f.pending = [dict(instId=T.INST, ordId="999", state="live", sz="1", accFillSz="0")]
        self.refused("DEMO_EMERGENCY_EXPOSURE_NOT_FLAT")

    def test_all_eight_algo_families_are_mandatory_and_failure_is_unknown(self):
        original = self.f.transport._reader._http_get
        for kind in T.ALGO_TYPES:
            def get(url, headers, timeout, kind=kind):
                if urlsplit(url).path.endswith("orders-algo-pending") and parse_qs(urlsplit(url).query)["ordType"] == [kind]:
                    return 200, {"code": "1", "data": []}
                return original(url, headers, timeout)
            with self.subTest(kind=kind), patch.object(self.f.transport._reader, "_http_get", get):
                self.refused("DEMO_READ_UNKNOWN")

    def test_second_exposure_cannot_become_nonflat(self):
        def changed(path, params):
            if path.endswith("/account/positions") and self.f.position_reads == 1:
                self.f.positions = [dict(instId="BTC-USDT-SWAP", posSide="net", pos="1")]
        self.f.on_get = changed
        self.refused("DEMO_EMERGENCY_EXPOSURE_NOT_FLAT")

    def test_receipts_and_account_config_cannot_change_between_waves(self):
        count = 0
        def changed(path, params):
            nonlocal count
            if path.endswith("/trade/order") and "ordId" in params:
                count += 1
                if count == 2:
                    self.f.child["avgPx"] = "104"
        self.f.on_get = changed
        self.refused("DEMO_EMERGENCY_RECEIPTS_CHANGED")

    def test_full_fill_page_is_incomplete(self):
        self.f.fill_override = [deepcopy(self.f.fills[0]) for _ in range(100)]
        self.refused("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")

    def test_full_history_page_is_incomplete(self):
        self.f.history_extra = [deepcopy(self.f.history) for _ in range(99)]
        self.refused("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")

    def test_full_exposure_page_is_unknown(self):
        self.f.positions = [dict(instId=T.INST, posSide="net", pos="0") for _ in range(100)]
        self.refused("DEMO_EXPOSURE_UNKNOWN")

    def test_server_clock_in_future_or_reversed_refuses(self):
        self.f.server = NOW+1
        self.refused("DEMO_LIFECYCLE_STALE")
        self.f.server = NOW
        count = 0
        def changed(path, params):
            nonlocal count
            if path.endswith("/public/time"):
                count += 1
                self.f.server = NOW if count == 1 else NOW-1
        self.f.on_get = changed
        self.refused("DEMO_LIFECYCLE_STALE")

    def test_five_second_wall_boundary_and_backward_wall(self):
        for elapsed, accepted in ((5000, True), (5001, False), (-1, False)):
            with self.subTest(elapsed=elapsed), patch.object(T.time, "time_ns", side_effect=[NOW*1_000_000, (NOW+elapsed)*1_000_000]):
                if accepted:
                    self.assertTrue(self.verify()["emergency_closed_lifecycle_verified"])
                else:
                    self.refused("DEMO_LIFECYCLE_STALE")

    def test_small_server_clock_difference_waits_without_timestamp_relabel_or_budget_extension(self):
        self.f.server = NOW + 700
        with patch.object(T.time, "time_ns", side_effect=[NOW*1_000_000, NOW*1_000_000, (NOW+700)*1_000_000]), \
                patch.object(T.time, "monotonic_ns", side_effect=[10_000_000_000, 10_000_000_000, 10_700_000_000]), \
                patch.object(T.time, "monotonic", return_value=10), patch.object(T.time, "sleep") as wait:
            result = self.verify()
        wait.assert_called_once_with(0.7)
        self.assertEqual(result["history_through_ms"], NOW + 700)
        self.assertEqual(result["observed_at_ms"], NOW + 700)
        self.assertEqual(result["collection_completed_at_ms"], NOW + 700)
        self.assertEqual(result["collection_elapsed_monotonic_ns"], 700_000_000)
        self.assertEqual(self.f.posts, [])
        for final_wall, final_mono in ((NOW, 10_700_000_000), (NOW+5001, 10_700_000_000),
                                       (NOW+700, 15_000_000_001), (NOW+700, 9_999_999_999)):
            with self.subTest(wall=final_wall, mono=final_mono), \
                    patch.object(T.time, "time_ns", side_effect=[NOW*1_000_000, NOW*1_000_000, final_wall*1_000_000]), \
                    patch.object(T.time, "monotonic_ns", side_effect=[10_000_000_000, 10_000_000_000, final_mono]), \
                    patch.object(T.time, "monotonic", return_value=10), patch.object(T.time, "sleep"):
                self.refused("DEMO_LIFECYCLE_STALE")
        self.f.server = NOW + 1001
        with patch.object(T.time, "sleep") as wait:
            self.refused("DEMO_LIFECYCLE_STALE")
        wait.assert_not_called()

    def test_five_second_monotonic_boundary_and_backward_monotonic(self):
        for elapsed, accepted in ((5_000_000_000, True), (5_000_000_001, False), (-1, False)):
            began = time.monotonic_ns()
            with self.subTest(elapsed=elapsed), patch.object(T.time, "monotonic_ns", side_effect=[began, began+elapsed]):
                if accepted:
                    self.assertTrue(self.verify()["emergency_closed_lifecycle_verified"])
                else:
                    self.refused("DEMO_LIFECYCLE_STALE")

    def test_tail_history_change_refuses_and_no_gap_exists_in_fills_windows(self):
        count = 0
        def changed(path, params):
            nonlocal count
            if path.endswith("/public/time"):
                count += 1
                self.f.server = NOW if count == 1 else NOW+1
            if path.endswith("/positions-history") and self.f.server == NOW+1:
                self.f.history["fee"] = "999"
        self.f.on_get = changed
        with patch.object(T.time, "time_ns", side_effect=[NOW*1_000_000, (NOW+1)*1_000_000]):
            self.refused("DEMO_EMERGENCY_HISTORY_CHANGED")
        windows = [params for path, params in self.f.gets if path.endswith("fills-history")]
        self.assertEqual([(row["begin"], row["end"]) for row in windows],
                         [([str(self.f.flat)], [str(NOW)]), ([str(NOW+1)], [str(NOW+1)])])

    def test_decimal_context_is_local_and_does_not_round_join(self):
        with localcontext() as context:
            context.prec = 2
            self.assertTrue(self.verify()["emergency_closed_lifecycle_verified"])
            self.assertEqual(context.prec, 2)


if __name__ == "__main__":
    unittest.main()
