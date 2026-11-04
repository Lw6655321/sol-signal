"""Offline contract tests for cancelling only a planned DEMO route's OCO."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from src import demo_route_transport as T
from src.demo_transport import CREDENTIAL_NAMES


FAKE = dict(zip(CREDENTIAL_NAMES, ("test-demo-key", "test-demo-secret", "test-demo-passphrase")))


class CancelWire:
    def __init__(self):
        self.gets = []
        self.posts = []
        self.row = dict(instId=T.INST, algoClOrdId="protection1", algoId="algo1",
                        ordType="oco", state="live", posSide="net", tdMode="cross",
                        side="sell", reduceOnly="true", sz="0.01",
                        slTriggerPx="99", slOrdPx="-1", tpTriggerPx="102", tpOrdPx="-1")
        self.read_reply = None
        self.read_error = None
        self.orders = {}
        self.on_read = None
        self.write_reply = (200, dict(code="0", data=[dict(algoId="algo1", sCode="0")]))

    def get(self, url, headers, timeout):
        self.gets.append((url, deepcopy(headers), timeout))
        if self.on_read:
            self.on_read(url)
        if self.read_error:
            raise self.read_error
        if self.read_reply is not None:
            return deepcopy(self.read_reply)
        if urlsplit(url).path == "/api/v5/trade/order":
            order_id = parse_qs(urlsplit(url).query).get("ordId", [""])[0]
            order = self.orders.get(order_id)
            return 200, dict(code="0", data=[] if order is None else [deepcopy(order)])
        return 200, dict(code="0", data=[] if self.row is None else [deepcopy(self.row)])

    def post(self, url, headers, body, timeout):
        self.posts.append((url, deepcopy(headers), body, timeout))
        if isinstance(self.write_reply, Exception):
            raise self.write_reply
        return deepcopy(self.write_reply)


class PlannedCancelTransportTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch.object(T, "load_demo_credentials",
                                             side_effect=AssertionError("NO_CREDENTIAL_READ")))
        self.temp = self.stack.enter_context(tempfile.TemporaryDirectory(ignore_cleanup_errors=True))
        self.wire = CancelWire()
        self.transport = self.make()

    def make(self, **overrides):
        options = dict(credentials=FAKE, enable_writes=True, http_get=self.wire.get,
                       http_post=self.wire.post, log_root=self.temp)
        options.update(overrides)
        return T.DemoRouteTransport(**options)

    def cancel(self, **changes):
        args = dict(inst_id=T.INST, protection_id="protection1", algo_id="algo1")
        args.update(changes)
        return self.transport.cancel_protection(**args)

    def rejected(self, code, callback=None):
        with self.assertRaises(T.DemoRouteError) as caught:
            (callback or self.cancel)()
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_cancel_has_single_element_array_and_exact_signature(self):
        result = self.cancel()
        self.assertEqual(result["status"], "CANCEL_ACKNOWLEDGED")
        self.assertEqual(result["state"], "cancel_requested")
        self.assertTrue(result["cancel_post_sent"])
        url, headers, body, timeout = self.wire.posts[0]
        path = "/api/v5/trade/cancel-algos"
        self.assertEqual(url, T.BASE + path)
        self.assertEqual(body, b'[{"algoId":"algo1","instId":"SOL-USDT-SWAP"}]')
        self.assertEqual(json.loads(body), [dict(instId=T.INST, algoId="algo1")])
        expected = base64.b64encode(hmac.new(FAKE[CREDENTIAL_NAMES[1]].encode(),
                    (headers["OK-ACCESS-TIMESTAMP"] + "POST" + path).encode() + body,
                    hashlib.sha256).digest()).decode()
        self.assertEqual(headers["OK-ACCESS-SIGN"], expected)
        self.assertEqual(headers["x-simulated-trading"], "1")
        self.assertLessEqual(timeout, 8)
        get_url, get_headers, _ = self.wire.gets[0]
        self.assertEqual(urlsplit(get_url).path, "/api/v5/trade/order-algo")
        self.assertEqual(parse_qs(urlsplit(get_url).query), {"algoClOrdId": ["protection1"]})
        self.assertEqual(get_headers["x-simulated-trading"], "1")

    def test_disabled_or_changed_demo_identity_refuses_before_get(self):
        self.transport = self.make(enable_writes=False)
        self.rejected("DEMO_WRITES_DISABLED")
        self.transport = self.make()
        self.transport.mode = "live"
        self.rejected("DEMO_ISOLATION_INVALID")
        self.assertEqual(self.wire.gets, [])
        self.assertEqual(self.wire.posts, [])

    def test_changed_simulated_header_refuses_before_any_account_request(self):
        original = self.transport._reader._headers
        def wrong_header(*args, **kwargs):
            headers = original(*args, **kwargs)
            headers["x-simulated-trading"] = "0"
            return headers
        with patch.object(self.transport._reader, "_headers", side_effect=wrong_header):
            self.rejected("DEMO_ISOLATION_INVALID")
        self.assertEqual(self.wire.gets, [])
        self.assertEqual(self.wire.posts, [])

    def test_global_live_flags_do_not_change_fixed_demo_identity(self):
        with patch.dict("os.environ", {"OKX_TRADING_ENABLED": "1", "OKX_SIMULATED": "0"}):
            self.cancel()
        self.assertEqual(self.wire.posts[0][1]["x-simulated-trading"], "1")
        self.assertTrue(all(urlsplit(row[0]).netloc == "openapi.okx.com"
                            for row in self.wire.posts + self.wire.gets))

    def test_bad_instrument_and_invalid_ids_fail_before_get(self):
        self.rejected("DEMO_INSTRUMENT", lambda: self.cancel(inst_id="ETH-USDT-SWAP"))
        for key in ("protection_id", "algo_id"):
            for value in (None, "", "bad/id", "x" * 33, 123, "algo\n1"):
                with self.subTest(key=key, value=value):
                    self.rejected("DEMO_INVALID_ID", lambda: self.cancel(**{key: value}))
        self.assertEqual(self.wire.gets, [])
        self.assertEqual(self.wire.posts, [])

    def test_wrong_algo_id_never_cancels(self):
        self.wire.row["algoId"] = "external1"
        self.rejected("DEMO_PROTECTION_MISMATCH")
        self.assertEqual(self.wire.posts, [])

    def test_wrong_client_id_or_instrument_never_cancels(self):
        for key, value in (("algoClOrdId", "external1"), ("instId", "ETH-USDT-SWAP")):
            with self.subTest(key=key):
                row = deepcopy(self.wire.row)
                self.wire.row[key] = value
                self.rejected("DEMO_PROTECTION_MISMATCH")
                self.wire.row = row
        self.assertEqual(self.wire.posts, [])

    def test_only_reduce_only_oco_net_cross_is_eligible(self):
        original = deepcopy(self.wire.row)
        for key, values in (("reduceOnly", [False, "false", "", 1, None]),
                            ("ordType", ["trigger", "conditional", "", None]),
                            ("posSide", ["long", "short", "", None]),
                            ("tdMode", ["isolated", "cash", "", None]),
                            ("side", ["hold", "", None]),
                            ("slOrdPx", ["99", None]), ("tpOrdPx", ["102", None])):
            for value in values:
                with self.subTest(key=key, value=value):
                    self.wire.row = dict(original, **{key: value})
                    self.rejected("DEMO_PROTECTION_MISMATCH")
        self.assertEqual(self.wire.posts, [])

    def test_missing_required_fields_are_not_invented(self):
        original = deepcopy(self.wire.row)
        for key in ("algoId", "algoClOrdId", "instId", "reduceOnly", "ordType", "posSide",
                    "tdMode", "side", "slOrdPx", "tpOrdPx"):
            with self.subTest(key=key):
                self.wire.row = deepcopy(original)
                del self.wire.row[key]
                self.rejected("DEMO_PROTECTION_MISMATCH")
        self.assertEqual(self.wire.posts, [])

    def test_invalid_trigger_or_size_rejects_before_post(self):
        original = deepcopy(self.wire.row)
        for key in ("sz", "slTriggerPx", "tpTriggerPx"):
            for value in (None, "", "NaN", "Infinity", "0", "-1", False):
                with self.subTest(key=key, value=value):
                    self.wire.row = dict(original, **{key: value})
                    self.rejected("DEMO_INVALID_NUMBER")
        self.assertEqual(self.wire.posts, [])

    def test_terminal_protection_is_get_only_and_not_flat_confirmation(self):
        for state in ("canceled", "effective", "order_failed"):
            with self.subTest(state=state):
                self.wire.row["state"] = state
                result = self.cancel()
                self.assertEqual((result["status"], result["state"]), ("TERMINAL", state))
                self.assertFalse(result["cancel_post_sent"])
                self.assertNotIn("flat", result)
        self.assertEqual(len(self.wire.gets), 3)
        self.assertEqual(self.wire.posts, [])

    def test_terminal_external_or_false_reduce_only_still_refuses(self):
        self.wire.row["state"] = "canceled"
        self.wire.row["reduceOnly"] = False
        self.rejected("DEMO_PROTECTION_MISMATCH")
        self.wire.row["reduceOnly"] = True
        self.wire.row["algoId"] = "external1"
        self.rejected("DEMO_PROTECTION_MISMATCH")
        self.assertEqual(self.wire.posts, [])

    def test_unknown_state_is_neither_live_nor_terminal(self):
        for state in ("", None, "partially_effective", "paused", "cancel_requested"):
            with self.subTest(state=state):
                self.wire.row["state"] = state
                self.rejected("DEMO_PROTECTION_STATE_UNKNOWN")
        self.assertEqual(self.wire.posts, [])

    def test_explicit_empty_or_51603_is_absent_not_position_closed(self):
        self.wire.row = None
        result = self.cancel()
        self.assertEqual((result["status"], result["state"]), ("ABSENT", "not_found"))
        self.assertFalse(result["cancel_post_sent"])
        self.assertNotIn("flat", result)
        self.wire.read_reply = (200, dict(code="51603", data=[]))
        self.assertEqual(self.cancel()["status"], "ABSENT")
        self.assertEqual(self.wire.posts, [])

    def test_generic_read_errors_never_become_absent(self):
        for response in ((503, {}), (200, dict(code="51000", data=[])),
                         (200, dict(code="0", data=None))):
            with self.subTest(response=response):
                self.wire.read_reply = response
                self.rejected("DEMO_READ_UNKNOWN")
        self.wire.read_reply = None
        self.wire.read_error = TimeoutError("untrusted-private-server-detail")
        self.rejected("DEMO_READ_UNKNOWN")
        self.assertEqual(self.wire.posts, [])

    def test_multiple_protections_or_row_error_never_authorize_cancellation(self):
        self.wire.read_reply = (200, dict(code="0", data=[self.wire.row] * 2))
        self.rejected("DEMO_PROTECTION_MISMATCH")
        self.wire.read_reply = (200, dict(code="0", data=[dict(self.wire.row, sCode="51000")]))
        self.rejected("DEMO_BAD_RESPONSE")
        self.assertEqual(self.wire.posts, [])

    def test_repeated_cancel_reuses_ack_but_still_gets_identity(self):
        first = self.cancel()
        second = self.cancel()
        self.assertEqual(first["status"], second["status"])
        self.assertTrue(first["cancel_post_sent"])
        self.assertFalse(second["cancel_post_sent"])
        self.assertEqual(len(self.wire.gets), 2)
        self.assertEqual(len(self.wire.posts), 1)
        self.wire.row["algoId"] = "external1"
        self.rejected("DEMO_PROTECTION_MISMATCH")
        self.assertEqual(len(self.wire.posts), 1)

    def test_changed_algo_with_same_client_id_cannot_send_second_cancel(self):
        self.cancel()
        self.wire.row["algoId"] = "algo2"
        self.rejected("DEMO_IDEMPOTENCY_CONFLICT", lambda: self.cancel(algo_id="algo2"))
        self.assertEqual(len(self.wire.posts), 1)

    def test_concurrent_duplicate_calls_still_send_exactly_one_post(self):
        all_read = threading.Barrier(4)
        original_get = self.transport.get_protection
        def synchronized_get(*args):
            row = original_get(*args)
            all_read.wait(timeout=2)
            return row
        with patch.object(self.transport, "get_protection", side_effect=synchronized_get):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: self.cancel(), range(4)))
        self.assertEqual(len(self.wire.posts), 1)
        self.assertEqual(sum(result["cancel_post_sent"] for result in results), 1)
        self.assertTrue(all(result["status"] == "CANCEL_ACKNOWLEDGED" for result in results))

    def test_unknown_write_never_reposts_even_if_read_still_live(self):
        self.wire.write_reply = TimeoutError("untrusted-private-server-detail")
        self.rejected("DEMO_WRITE_UNKNOWN")
        self.rejected("DEMO_WRITE_UNKNOWN")
        self.assertEqual(len(self.wire.posts), 1)
        self.assertEqual(len(self.wire.gets), 2)

    def test_unknown_write_can_be_resolved_only_by_terminal_get(self):
        self.wire.write_reply = TimeoutError("untrusted-private-server-detail")
        self.rejected("DEMO_WRITE_UNKNOWN")
        self.wire.row["state"] = "canceled"
        result = self.cancel()
        self.assertEqual(result["status"], "TERMINAL")
        self.assertFalse(result["cancel_post_sent"])
        self.assertEqual(len(self.wire.posts), 1)

    def test_bad_ack_algo_id_is_unknown_and_never_cached_as_success(self):
        for algo_id in (None, "", "external1"):
            with self.subTest(algo_id=algo_id):
                self.transport = self.make()
                self.wire.write_reply = (200, dict(code="0", data=[dict(algoId=algo_id, sCode="0")]))
                self.rejected("DEMO_PROTECTION_MISMATCH")
                self.rejected("DEMO_WRITE_UNKNOWN")
        self.assertEqual(len(self.wire.posts), 3)

    def test_http_top_and_row_codes_are_checked_and_never_retried(self):
        cases = (((403, {}), "DEMO_HTTP_ERROR"),
                 ((200, dict(code="51000", data=[])), "DEMO_API_REJECTED"),
                 ((200, dict(code="0", data=[dict(algoId="algo1", sCode="51001")])), "DEMO_ROW_REJECTED"),
                 ((200, dict(code="0", data=[dict(algoId="algo1")])), "DEMO_ROW_REJECTED"),
                 ((200, dict(code="0", data=[])), "DEMO_BAD_RESPONSE"),
                 ((200, dict(code="0", data=[dict(algoId="algo1", sCode="0")] * 2)), "DEMO_BAD_RESPONSE"))
        for response, code in cases:
            with self.subTest(code=code):
                self.transport = self.make()
                self.wire.write_reply = response
                self.rejected(code)
                self.rejected("DEMO_WRITE_UNKNOWN")
        self.assertEqual(len(self.wire.posts), len(cases))

    def test_new_array_scope_does_not_allow_batches_or_other_paths(self):
        for body in (dict(instId=T.INST, algoId="algo1"), [],
                     [dict(instId=T.INST, algoId="algo1")] * 2,
                     [dict(instId=T.INST, algoId="algo1", extra="value")],
                     [dict(instId="ETH-USDT-SWAP", algoId="algo1")]):
            with self.subTest(body=body):
                self.rejected("DEMO_CANCEL_SCOPE_INVALID", lambda: self.transport._post(
                    "cancel_protection", "protection1", "/api/v5/trade/cancel-algos", body))
        self.rejected("DEMO_CANCEL_SCOPE_INVALID", lambda: self.transport._post(
            "generic", "protection1", "/api/v5/trade/cancel-algos", [dict(instId=T.INST, algoId="algo1")]))
        self.rejected("DEMO_BAD_REQUEST", lambda: self.transport._post(
            "entry", "entry1", "/api/v5/trade/order", [dict(instId=T.INST)]))
        self.rejected("DEMO_PATH_NOT_ALLOWED", lambda: self.transport._post(
            "generic", "protection1", "/api/v5/account/set-leverage", {}))
        self.assertEqual(self.wire.posts, [])

    def test_cancel_audit_uses_only_safe_fixed_metadata(self):
        self.wire.write_reply = TimeoutError("untrusted-private-server-detail test-demo-secret")
        self.rejected("DEMO_WRITE_UNKNOWN")
        audit = self.transport.audit_path.read_text(encoding="utf-8")
        self.assertIn("cancel_protection", audit)
        self.assertIn("DEMO_WRITE_UNKNOWN", audit)
        for value in [*FAKE.values(), "untrusted-private-server-detail", "OK-ACCESS", "https://"]:
            self.assertNotIn(value, audit)

    def exit_fixture(self):
        self.wire.row.update(state="effective", ordIdList=["1001"], ordId="1001")
        self.wire.orders["1001"] = dict(instId=T.INST, ordId="1001", algoId="algo1",
                algoClOrdId="protection1", clOrdId="", posSide="net", tdMode="cross", side="sell",
                reduceOnly="true", ordType="market", sz="0.01", accFillSz="0.01",
                state="filled", fee="-0.0001", feeCcy="USDT", fillTime="1792400400000")

    def exit_orders(self, **changes):
        args = dict(inst_id=T.INST, protection_id="protection1", algo_id="algo1")
        args.update(changes)
        return self.transport.get_protection_exit_orders(**args)

    def test_effective_parent_child_receipts_are_raw_get_only_by_actual_ord_id(self):
        self.exit_fixture()
        raw = deepcopy(self.wire.orders["1001"])
        result = self.exit_orders()
        self.assertEqual(result, [raw])
        self.assertEqual(result[0]["clOrdId"], "")
        self.assertEqual(self.wire.posts, [])
        self.assertEqual(len(self.wire.gets), 3)  # parent, real child, stable parent
        url, headers, _ = self.wire.gets[1]
        self.assertEqual(urlsplit(url).path, "/api/v5/trade/order")
        self.assertEqual(parse_qs(urlsplit(url).query), {"instId": [T.INST], "ordId": ["1001"]})
        self.assertEqual(headers["x-simulated-trading"], "1")
        result[0]["accFillSz"] = "99"
        self.assertEqual(self.wire.orders["1001"], raw)

    def test_exit_receipt_parent_must_be_the_same_effective_owned_oco(self):
        self.exit_fixture()
        original = deepcopy(self.wire.row)
        cases = (("instId", "ETH-USDT-SWAP", "DEMO_PROTECTION_MISMATCH"),
                 ("algoClOrdId", "external1", "DEMO_PROTECTION_MISMATCH"),
                 ("algoId", "external1", "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("state", "live", "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("state", "canceled", "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("ordType", "conditional", "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("reduceOnly", False, "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("posSide", "long", "DEMO_PROTECTION_EXIT_UNKNOWN"),
                 ("tdMode", "isolated", "DEMO_PROTECTION_EXIT_UNKNOWN"))
        for key, value, code in cases:
            with self.subTest(key=key, value=value):
                self.wire.row = dict(original, **{key: value})
                self.rejected(code, self.exit_orders)
        self.assertTrue(all(urlsplit(item[0]).path == "/api/v5/trade/order-algo" for item in self.wire.gets))
        self.assertEqual(self.wire.posts, [])

    def test_no_algo_or_missing_parent_shape_is_unknown_never_a_fill(self):
        self.exit_fixture()
        original = deepcopy(self.wire.row)
        for field in ("reduceOnly", "state", "side", "slOrdPx", "tpOrdPx"):
            self.wire.row = deepcopy(original)
            del self.wire.row[field]
            self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.wire.row = None
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_ord_id_list_must_be_complete_strict_numeric_deduplicated_and_bounded(self):
        self.exit_fixture()
        original = deepcopy(self.wire.row)
        invalid = (None, [], "1001", [1001], ["1001", "1001"], ["1001,1002"],
                   ["1001/other"], ["١٠٠١"], ["1" * 33], [str(i) for i in range(5)])
        for ids in invalid:
            with self.subTest(ids=ids):
                self.wire.row = dict(original, ordIdList=ids)
                self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.wire.row = deepcopy(original)
        del self.wire.row["ordIdList"]
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.wire.row = dict(original, ordId="9999")
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertTrue(all(urlsplit(item[0]).path == "/api/v5/trade/order-algo" for item in self.wire.gets))
        self.assertEqual(self.wire.posts, [])

    def test_child_must_match_actual_parent_id_instrument_side_and_reduce_only_shape(self):
        self.exit_fixture()
        original = deepcopy(self.wire.orders["1001"])
        for field, value in (("instId", "ETH-USDT-SWAP"), ("ordId", "1002"),
                             ("algoId", "external1"), ("algoClOrdId", "external1"),
                             ("side", "buy"), ("posSide", "short"), ("tdMode", "isolated"),
                             ("reduceOnly", False), ("ordType", "post_only"), ("state", "unknown")):
            with self.subTest(field=field):
                self.wire.orders["1001"] = dict(original, **{field: value})
                self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_child_missing_correlation_or_fill_fields_never_invents_a_receipt(self):
        self.exit_fixture()
        original = deepcopy(self.wire.orders["1001"])
        for field in ("algoId", "ordId", "instId", "side", "posSide", "tdMode", "reduceOnly", "state", "ordType"):
            with self.subTest(field=field):
                self.wire.orders["1001"] = deepcopy(original)
                del self.wire.orders["1001"][field]
                self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        for field in ("sz", "accFillSz"):
            self.wire.orders["1001"] = deepcopy(original)
            del self.wire.orders["1001"][field]
            self.rejected("DEMO_INVALID_NUMBER", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_effective_parent_with_no_actual_child_remains_unknown(self):
        self.exit_fixture()
        self.wire.orders.clear()
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_child_canceled_partial_receipt_preserves_actual_cumulative_fill(self):
        self.exit_fixture()
        self.wire.orders["1001"].update(state="canceled", accFillSz="0.004")
        result = self.exit_orders()
        self.assertEqual(result[0]["state"], "canceled")
        self.assertEqual(result[0]["accFillSz"], "0.004")
        self.wire.orders["1001"].update(state="partially_filled")
        self.assertEqual(self.exit_orders()[0]["accFillSz"], "0.004")
        self.assertEqual(self.wire.posts, [])

    def test_inconsistent_child_fill_state_or_quantity_is_unknown(self):
        self.exit_fixture()
        original = deepcopy(self.wire.orders["1001"])
        for state, size, filled in (("filled", "0.01", "0.004"), ("live", "0.01", "0.001"),
                                    ("partially_filled", "0.01", "0"),
                                    ("partially_filled", "0.01", "0.01"),
                                    ("canceled", "0.01", "0.02"), ("filled", "0.02", "0.02")):
            with self.subTest(state=state, size=size, filled=filled):
                self.wire.orders["1001"] = dict(original, state=state, sz=size, accFillSz=filled)
                self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_split_receipts_sum_real_fills_without_exceeding_parent_size(self):
        self.exit_fixture()
        self.wire.row.update(ordIdList=["1001", "1002"], ordId="1002")
        self.wire.orders["1001"].update(sz="0.004", accFillSz="0.004")
        self.wire.orders["1002"] = dict(self.wire.orders["1001"], ordId="1002", sz="0.006", accFillSz="0.006")
        self.assertEqual(len(self.exit_orders()), 2)
        self.wire.orders["1002"].update(sz="0.007", accFillSz="0.007")
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])

    def test_child_network_failure_is_unknown_and_does_not_send_any_mutation(self):
        self.exit_fixture()
        def fail_child(url):
            if urlsplit(url).path == "/api/v5/trade/order":
                self.wire.read_error = TimeoutError("untrusted-private-server-detail")
        self.wire.on_read = fail_child
        self.rejected("DEMO_READ_UNKNOWN", self.exit_orders)
        self.assertEqual(len(self.wire.gets), 2)
        self.assertEqual(self.wire.posts, [])

    def test_parent_changed_list_during_receipt_collection_is_unknown(self):
        self.exit_fixture()
        def change_parent(url):
            if urlsplit(url).path == "/api/v5/trade/order-algo" and len(self.wire.gets) == 3:
                self.wire.row.update(ordIdList=["1001", "1002"], ordId="1002")
        self.wire.on_read = change_parent
        self.rejected("DEMO_PROTECTION_EXIT_UNKNOWN", self.exit_orders)
        self.assertEqual(self.wire.posts, [])


if __name__ == "__main__":
    unittest.main()
