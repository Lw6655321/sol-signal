"""Offline regression tests. No real credentials, environments or accounts."""
from __future__ import annotations

import json
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src import okx_native as N
from src import okx_private as P
from src import util as U
from src.order_router import OrderRouter


INST = "SOL-USDT-SWAP"


def forbidden(*args, **kwargs):
    raise AssertionError("Offline tests forbid network and credential access")


def intent():
    return SimpleNamespace(inst_id=INST, side="long", stop=Decimal("100"),
                           tp1=Decimal("110"), entry=Decimal("105"),
                           qty=Decimal("1"), require_maker=True)


def protection(qty="1", **overrides):
    row = dict(instId=INST, algoClOrdId="protect1", algoId="algo1", state="live",
               ordType="oco", side="sell", posSide="net", reduceOnly="true",
               sz=qty, slTriggerPx="100", tpTriggerPx="110", slOrdPx="-1", tpOrdPx="-1")
    row.update(overrides)
    return row


def order(client="entry1", oid="entryOrder1", state="canceled", **overrides):
    row = dict(instId=INST, clOrdId=client, ordId=oid, state=state, sz="1", accFillSz="0",
               side="buy", posSide="net", ordType="limit", reduceOnly="false")
    row.update(overrides)
    return row


class OfflineCase(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket", "socket.create_connection", "socket.getaddrinfo",
                       "urllib.request.urlopen"):
            self.enterContext(patch(target, side_effect=forbidden))
        for name in ("private", "creds", "configured", "_env_or_reg"):
            self.enterContext(patch.object(P, name, side_effect=forbidden))
        self.enterContext(patch.object(N, "http_request", side_effect=forbidden))
        self.audit = self.enterContext(patch.object(N, "_audit"))
        self.enterContext(patch.object(N, "_require_trading"))
        self.enterContext(patch.object(N.time, "sleep"))

    def fake_credentials(self):
        # Placeholders only in memory. Real environment and registry are untouched.
        self.enterContext(patch.object(P, "creds", return_value=dict(
            key="offline-placeholder", secret="offline-placeholder",
            passphrase="offline-placeholder", simulated=True)))
        self.enterContext(patch.object(P, "_sign", return_value="offline-signature"))

    def http_payload(self, payload):
        self.fake_credentials()
        return self.enterContext(patch.object(N, "http_request",
                                             return_value=json.dumps(payload).encode()))


class RequestTests(OfflineCase):
    def test_direct_entry_keeps_400_usdt_hard_limit(self):
        with patch.object(N, "_request", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "^NOTIONAL_LIMIT$"):
                N.place_limit_order(INST, "buy", "100", "4.01", "entry1")
        self.http_payload(dict(code="0", data=[dict(sCode="0", ordId="order1")]))
        self.assertEqual(N.place_limit_order(INST, "buy", "100", "4", "entry1")["ordId"], "order1")

    def test_post_uses_one_attempt_and_checks_success_row(self):
        send = self.http_payload(dict(code="0", data=[dict(sCode="0", ordId="order1")]))
        result = N.place_limit_order(INST, "buy", "100", "0.01", "entry1")
        self.assertEqual(result["ordId"], "order1")
        self.assertEqual(send.call_count, 1)
        self.assertEqual(send.call_args.kwargs["retries"], 1)
        self.assertEqual(send.call_args.kwargs["method"], "POST")

    def test_real_http_function_does_not_resend_post_on_timeout(self):
        self.fake_credentials()
        with patch.object(N, "http_request", U.http_request), \
             patch("urllib.request.urlopen", side_effect=TimeoutError("sensitive-marker")) as send:
            with self.assertRaisesRegex(N.NativeError, "^REQUEST_UNKNOWN$"):
                N.market_close(INST, "sell", "0.01", "close1")
        self.assertEqual(send.call_count, 1)
        self.assertNotIn("sensitive-marker", str(self.audit.call_args_list))

    def test_get_has_bounded_read_retry_setting(self):
        send = self.http_payload(dict(code="0", data=[order()]))
        self.assertEqual(N.get_order(INST, "entry1")["ordId"], "entryOrder1")
        self.assertEqual(send.call_args.kwargs["retries"], 2)

    def test_all_scode_rows_are_checked_not_only_first(self):
        self.http_payload(dict(code="0", data=[dict(sCode="0", algoId="algo1"),
                                               dict(sCode="51603", sMsg="secret-marker")]))
        with self.assertRaisesRegex(N.NativeError, "^ROW_REJECTED$"):
            N.cancel_algo(INST, "algo1")
        self.assertNotIn("secret-marker", str(self.audit.call_args_list))
        self.assertNotIn("resp", str(self.audit.call_args_list))

    def test_post_missing_scode_is_rejected(self):
        self.http_payload(dict(code="0", data=[dict(ordId="order1")]))
        with self.assertRaisesRegex(N.NativeError, "^ROW_REJECTED$"):
            N.cancel_order(INST, "entry1")

    def test_top_api_error_is_sanitized(self):
        self.http_payload(dict(code="51000", msg="secret-marker", data=[]))
        with self.assertRaisesRegex(N.NativeError, "^API_REJECTED$"):
            N.get_order(INST, "entry1")
        self.assertNotIn("secret-marker", str(self.audit.call_args_list))

    def test_only_explicit_absence_returns_none(self):
        self.http_payload(dict(code="51603", msg="Order does not exist", data=[]))
        self.assertIsNone(N.get_order(INST, "entry1"))
        self.assertIsNone(N.get_algo_order(INST, "protect1"))

    def test_network_unknown_is_not_order_absence(self):
        self.fake_credentials()
        with patch.object(N, "http_request", side_effect=TimeoutError("secret-marker")):
            with self.assertRaisesRegex(N.NativeError, "^REQUEST_UNKNOWN$"):
                N.get_algo_order(INST, "protect1")
        self.assertNotIn("secret-marker", str(self.audit.call_args_list))

    def test_no_writes_when_gate_rejects(self):
        with patch.object(N, "_require_trading", side_effect=N.NativeError("TRADING_DISABLED")), \
             patch.object(N, "_request", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "TRADING_DISABLED"):
                N.place_limit_order(INST, "buy", "100", "1", "entry1")


class ProtectionTests(OfflineCase):
    def test_explicit_false_remains_false_and_router_rejects(self):
        raw = protection(reduceOnly="false")
        normalized = N._norm_algo(raw, "protect1")
        self.assertEqual(normalized["reduceOnly"], "false")
        row = dict(protection_id="protect1", protection_order_id="algo1")
        router = object.__new__(OrderRouter)
        self.assertFalse(router._protection_valid(normalized, intent(), row, Decimal("1")))

    def test_missing_protection_facts_are_never_invented(self):
        for field in ("reduceOnly", "posSide", "ordType", "slOrdPx", "tpOrdPx", "algoClOrdId"):
            with self.subTest(field=field):
                raw = protection()
                raw.pop(field)
                normalized = N._norm_algo(raw, "protect1")
                self.assertNotIn(field, normalized)
                self.assertFalse(N._valid_protection(normalized, intent(), "protect1", qty=Decimal("1")))

    def test_same_id_second_quantity_amends_existing_algo(self):
        stored = None
        operations = []
        def request(method, path, params=None, body=None):
            nonlocal stored
            operations.append((method, path, body))
            if method == "GET" and path == "/api/v5/trade/order-algo":
                return [] if stored is None else [dict(stored)]
            if method == "POST" and path == "/api/v5/trade/order-algo":
                self.assertIsNone(stored)
                stored = protection(qty=body["sz"])
                return [dict(sCode="0", algoId="algo1")]
            if method == "POST" and path == "/api/v5/trade/amend-algos":
                self.assertEqual(body["algoId"], "algo1")
                self.assertIs(body["cxlOnFail"], False)
                stored["sz"] = body["newSz"]
                return [dict(sCode="0", algoId="algo1")]
            raise AssertionError("Unexpected offline request")
        with patch.object(N, "_request", side_effect=request):
            transport = N.NativeOkxTransport()
            transport.set_protection(intent(), Decimal("0.5"), "protect1", reduce_only=True)
            transport.set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)
        posts = [path for method, path, body in operations if method == "POST"]
        self.assertEqual(posts, ["/api/v5/trade/order-algo", "/api/v5/trade/amend-algos"])
        self.assertEqual(stored["sz"], "1")

    def test_existing_correct_protection_is_read_only(self):
        with patch.object(N, "get_algo_order", return_value=protection()), \
             patch.object(N, "place_oco_close", side_effect=forbidden), \
             patch.object(N, "amend_oco_size", side_effect=forbidden):
            result = N.NativeOkxTransport().set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)
        self.assertEqual(result["algoId"], "algo1")

    def test_query_unknown_prevents_new_protection_post(self):
        with patch.object(N, "get_algo_order", side_effect=N.NativeError("REQUEST_UNKNOWN")), \
             patch.object(N, "place_oco_close", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "REQUEST_UNKNOWN"):
                N.NativeOkxTransport().set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)

    def test_unknown_create_is_not_repeated_under_same_id(self):
        with patch.object(N, "get_algo_order", return_value=None), \
             patch.object(N, "place_oco_close", side_effect=N.NativeError("REQUEST_UNKNOWN")) as create:
            transport = N.NativeOkxTransport()
            with self.assertRaisesRegex(N.NativeError, "PROTECTION_UNCONFIRMED"):
                transport.set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)
            with self.assertRaisesRegex(N.NativeError, "PROTECTION_UNKNOWN"):
                transport.set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)
        self.assertEqual(create.call_count, 1)

    def test_amend_ack_does_not_replace_quantity_verification(self):
        with patch.object(N, "get_algo_order", return_value=protection(qty="0.5")), \
             patch.object(N, "amend_oco_size", return_value=dict(sCode="0", algoId="algo1")), \
             patch.object(N, "cancel_algo", side_effect=forbidden), \
             patch.object(N, "place_oco_close", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "PROTECTION_UNCONFIRMED"):
                N.NativeOkxTransport().set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)

    def test_false_existing_protection_is_not_amended_or_recreated(self):
        with patch.object(N, "get_algo_order", return_value=protection(reduceOnly=False)), \
             patch.object(N, "amend_oco_size", side_effect=forbidden), \
             patch.object(N, "place_oco_close", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "PROTECTION_MISMATCH"):
                N.NativeOkxTransport().set_protection(intent(), Decimal("1"), "protect1", reduce_only=True)

    def test_reduce_only_false_rejected_before_any_query(self):
        with patch.object(N, "get_algo_order", side_effect=forbidden):
            with self.assertRaisesRegex(N.NativeError, "REDUCE_ONLY_REQUIRED"):
                N.NativeOkxTransport().set_protection(intent(), Decimal("1"), "protect1", reduce_only=False)


class FlattenTests(OfflineCase):
    def test_entry_cancel_unknown_keeps_existing_protection(self):
        with patch.object(N, "pending_orders", return_value=[order()]), \
             patch.object(N, "cancel_order", side_effect=N.NativeError("REQUEST_UNKNOWN")), \
             patch.object(N, "get_order", return_value=order(state="live")), \
             patch.object(N, "pending_algos", side_effect=forbidden), \
             patch.object(N, "market_close", side_effect=forbidden), \
             patch.object(N, "get_position", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ORDER_UNCONFIRMED")
        self.assertEqual(result["cancelled_orders"], 0)

    def test_cancel_rejection_does_not_count_without_terminal_confirmation(self):
        with patch.object(N, "pending_orders", return_value=[order()]), \
             patch.object(N, "cancel_order", side_effect=N.NativeError("ROW_REJECTED")), \
             patch.object(N, "get_order", return_value=None), \
             patch.object(N, "pending_algos", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["cancelled_orders"], 0)

    def test_cancel_fill_race_closes_cumulative_position_before_removing_protection(self):
        events = []
        state = dict(pending=True, position=True, algo=True)
        def pending(inst):
            events.append("pending")
            return [order()] if state["pending"] else []
        def cancel(inst, client, *, ord_id):
            events.append("cancel_entry")
            state["pending"] = False
            return dict(sCode="0", ordId=ord_id)
        def read_order(inst, client="", *, ord_id=""):
            events.append("close_verified" if client.startswith("FLAT") else "entry_verified")
            if client.startswith("FLAT"):
                state["position"] = False
                return order(client, "closeOrder1", "filled", side="sell", ordType="market",
                             reduceOnly="true", accFillSz="1")
            return order(state="filled", accFillSz="1")
        def position(inst):
            events.append("position")
            return dict(instId=INST, posSide="net", pos="1") if state["position"] else None
        def close(inst, side, qty, client):
            events.append("close_sent")
            self.assertTrue(state["algo"])
            return dict(sCode="0", ordId="closeOrder1")
        def algos(inst, kind):
            events.append("algos")
            return [dict(instId=INST, algoId="algo1", reduceOnly="true")] if kind == "oco" and state["algo"] else []
        def cancel_algo(inst, algo_id):
            events.append("cancel_protection")
            self.assertFalse(state["position"])
            state["algo"] = False
            return dict(sCode="0", algoId=algo_id)
        with patch.object(N, "pending_orders", side_effect=pending), \
             patch.object(N, "cancel_order", side_effect=cancel), \
             patch.object(N, "get_order", side_effect=read_order), \
             patch.object(N, "get_position", side_effect=position), \
             patch.object(N, "market_close", side_effect=close), \
             patch.object(N, "pending_algos", side_effect=algos), \
             patch.object(N, "get_algo_order", return_value=dict(instId=INST, algoId="algo1", state="canceled")), \
             patch.object(N, "cancel_algo", side_effect=cancel_algo):
            result = N.flatten()
        self.assertTrue(result["flat"])
        self.assertLess(events.index("entry_verified"), events.index("close_sent"))
        self.assertLess(events.index("close_verified"), events.index("cancel_protection"))

    def test_close_ack_without_fill_never_removes_protection(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "get_position", return_value=dict(instId=INST, pos="1", posSide="net")), \
             patch.object(N, "market_close", return_value=dict(sCode="0", ordId="closeOrder1")), \
             patch.object(N, "get_order", return_value=None), \
             patch.object(N, "pending_algos", return_value=[]), \
             patch.object(N, "cancel_algo", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ORDER_UNCONFIRMED")

    def test_new_pending_after_cancellation_prevents_flat_claim(self):
        with patch.object(N, "pending_orders", side_effect=[[], [order()]]), \
             patch.object(N, "pending_algos", return_value=[]), \
             patch.object(N, "get_position", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ENTRY_REMAINS")

    def test_unknown_position_cannot_claim_flat(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "get_position", side_effect=N.NativeError("REQUEST_UNKNOWN")), \
             patch.object(N, "pending_algos", return_value=[]):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "REQUEST_UNKNOWN")

    def test_every_algo_kind_must_be_checked_before_success(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "get_position", return_value=None), \
             patch.object(N, "pending_algos", return_value=[]) as algos:
            result = N.flatten()
        self.assertTrue(result["flat"])
        self.assertEqual([call.args[1] for call in algos.call_args_list], list(N.ALGO_TYPES) * 4)

    def test_algo_query_unknown_cannot_claim_flat(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "get_position", return_value=None), \
             patch.object(N, "pending_algos", side_effect=N.NativeError("REQUEST_UNKNOWN")):
            result = N.flatten()
        self.assertFalse(result["flat"])

    def test_algo_still_present_after_cancel_cannot_claim_flat(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "get_position", return_value=None), \
             patch.object(N, "pending_algos", side_effect=lambda inst, kind: [
                 dict(instId=INST, algoId="algo1", reduceOnly="true")] if kind == "oco" else []), \
             patch.object(N, "get_algo_order", return_value=dict(instId=INST, algoId="algo1", state="canceled")), \
             patch.object(N, "cancel_algo", return_value=dict(sCode="0", algoId="algo1")):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ALGO_REMAINS")

    def test_algo_with_unknown_role_stops_before_closing_or_removing_protection(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "_all_algos", return_value=[dict(instId=INST, algoId="algo1")]), \
             patch.object(N, "cancel_algo", side_effect=forbidden), \
             patch.object(N, "market_close", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ALGO_ROLE_UNKNOWN")

    def test_opening_algo_canceled_before_close_and_protection_canceled_after(self):
        events = []
        state = dict(opening=True, protected=True, position=True)
        def algos(inst):
            rows = []
            if state["opening"]:
                rows.append(dict(instId=INST, algoId="openAlgo", reduceOnly="false"))
            if state["protected"]:
                rows.append(dict(instId=INST, algoId="protectAlgo", reduceOnly="true"))
            return rows
        def cancel(inst, algo_id):
            events.append(algo_id)
            if algo_id == "openAlgo":
                state["opening"] = False
            else:
                self.assertFalse(state["position"])
                state["protected"] = False
            return dict(sCode="0", algoId=algo_id)
        def algo_detail(inst, client="", *, algo_id=""):
            return dict(instId=INST, algoId=algo_id, state="canceled")
        def position(inst):
            return dict(instId=INST, posSide="net", pos="1") if state["position"] else None
        def close(inst, side, qty, client):
            events.append("close")
            self.assertFalse(state["opening"])
            self.assertTrue(state["protected"])
            return dict(sCode="0", ordId="closeOrder1")
        def closed(inst, client="", *, ord_id=""):
            state["position"] = False
            return order(client, "closeOrder1", "filled", side="sell", ordType="market",
                         reduceOnly="true", accFillSz="1")
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "_all_algos", side_effect=algos), \
             patch.object(N, "cancel_algo", side_effect=cancel), \
             patch.object(N, "get_algo_order", side_effect=algo_detail), \
             patch.object(N, "get_position", side_effect=position), \
             patch.object(N, "market_close", side_effect=close), \
             patch.object(N, "get_order", side_effect=closed):
            result = N.flatten()
        self.assertTrue(result["flat"])
        self.assertEqual(events, ["openAlgo", "close", "protectAlgo"])

    def test_opening_algo_already_effective_is_unknown_and_keeps_protection(self):
        with patch.object(N, "pending_orders", return_value=[]), \
             patch.object(N, "_all_algos", return_value=[
                 dict(instId=INST, algoId="openAlgo", reduceOnly="false")]), \
             patch.object(N, "cancel_algo", return_value=dict(sCode="0", algoId="openAlgo")), \
             patch.object(N, "get_algo_order", return_value=dict(instId=INST, algoId="openAlgo", state="effective")), \
             patch.object(N, "market_close", side_effect=forbidden):
            result = N.flatten()
        self.assertFalse(result["flat"])
        self.assertEqual(result["code"], "ALGO_CANCEL_UNCONFIRMED")


if __name__ == "__main__":
    unittest.main()
