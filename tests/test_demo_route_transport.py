"""Offline wire-contract tests: fake credentials/HTTP, no account operations."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import tempfile
import unittest
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from src import demo_route_transport as T
from src.demo_transport import CREDENTIAL_NAMES, DemoTransportError
from src.execution import build_intent
from test_execution import inputs


D = Decimal
FAKE = dict(zip(CREDENTIAL_NAMES, ("fake-demo-key", "fake-demo-secret", "fake-demo-passphrase")))


class Wire:
    def __init__(self):
        self.gets = []
        self.posts = []
        self.rows = {}
        self.responses = []
        self.on_post = None

    def get(self, url, headers, timeout):
        parsed = urlsplit(url)
        params = parse_qs(parsed.query)
        self.gets.append((url, deepcopy(headers), timeout))
        value = self.rows.get(parsed.path, [])
        if callable(value):
            value = value(params)
        if isinstance(value, Exception):
            raise value
        return 200, dict(code="0", data=deepcopy(value))

    def post(self, url, headers, body, timeout):
        self.posts.append((url, deepcopy(headers), body, timeout))
        if self.on_post:
            self.on_post(url, json.loads(body))
        result = self.responses.pop(0) if self.responses else (200, dict(code="0", data=[dict(sCode="0", ordId="order1")]))
        if isinstance(result, Exception):
            raise result
        return deepcopy(result)


class DemoRouteTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch.object(T, "load_demo_credentials", side_effect=AssertionError("NO_REAL_CREDENTIALS")))
        self.temp = self.stack.enter_context(tempfile.TemporaryDirectory(ignore_cleanup_errors=True))
        self.wire = Wire()
        signal, state, config = inputs()
        self.intent = build_intent(signal, state, config)
        self.transport = self.make()

    def make(self, **overrides):
        options = dict(credentials=FAKE, enable_writes=True, http_get=self.wire.get,
                       http_post=self.wire.post, log_root=self.temp)
        options.update(overrides)
        return T.DemoRouteTransport(**options)

    def rejected(self, code, callback):
        with self.assertRaises(T.DemoRouteError) as caught:
            callback()
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def protection(self, qty=None, **changes):
        result = dict(instId=T.INST, algoClOrdId="protect1", algoId="algo1", state="live",
                      ordType="oco", side="sell", posSide="net", tdMode="cross",
                      sz=str(self.intent.qty if qty is None else qty), reduceOnly="true",
                      slTriggerPx=str(self.intent.stop), slOrdPx="-1",
                      tpTriggerPx=str(self.intent.tp1), tpOrdPx="-1")
        result.update(changes)
        return result

    def lifecycle(self):
        self.server_ms = self.intent.created_at_ms+1000
        self.flat_ms = self.server_ms-900
        self.opened_ms = self.server_ms-500
        self.life_fills = [dict(instId=T.INST, instType="SWAP", posSide="net", side="buy",
                               ordId="entry001", clOrdId="client001", tradeId="trade001", billId="bill001",
                               fillTime=str(self.opened_ms), fillSz=str(self.intent.qty),
                               fillPnl="0", fee="-0.01", feeCcy="USDT", subType="3")]
        self.life_position = dict(instId=T.INST, pos=str(self.intent.qty), posSide="net",
                                  mgnMode="cross", posId="position001", cTime=str(self.opened_ms), lever="8")
        self.life_order = dict(instId=T.INST, ordId="entry001", clOrdId="client001", side="buy",
                              posSide="net", tdMode="cross", sz=str(self.intent.qty),
                              accFillSz=str(self.intent.qty), state="filled",
                              ordType="post_only" if self.intent.require_maker else "limit")
        self.wire.rows["/api/v5/public/time"] = [{"ts": str(self.server_ms)}]
        self.wire.rows["/api/v5/trade/order"] = lambda params: [deepcopy(self.life_order)]
        self.wire.rows["/api/v5/account/positions"] = lambda params: [deepcopy(self.life_position)]
        self.wire.rows["/api/v5/trade/fills-history"] = lambda params: [deepcopy(row) for row in self.life_fills
                    if int(params["begin"][0]) <= int(row["fillTime"]) <= int(params["end"][0])]

    def verify_lifecycle(self, **overrides):
        options = dict(intent=self.intent, order_id="entry001", client_id="client001",
                       filled_qty=self.intent.qty, flat_observed_at_ms=self.flat_ms)
        options.update(overrides)
        return self.transport.verify_position_lifecycle(**options)

    def test_writes_disabled_by_default_and_boolean_is_strict(self):
        transport = self.make(enable_writes=False)
        self.rejected("DEMO_WRITES_DISABLED", lambda: transport.submit_entry(self.intent, "entry1"))
        for value in (1, "1", None):
            self.rejected("DEMO_ENABLE_BOOLEAN_REQUIRED", lambda: self.make(enable_writes=value))
        self.assertEqual(self.wire.posts, [])
        with self.assertRaises(AttributeError):
            transport.enable_writes = True

    def test_default_credential_loader_is_only_called_at_construction(self):
        with patch.object(T, "load_demo_credentials", return_value=FAKE) as loader:
            transport = self.make(credentials=None)
            loader.assert_called_once_with()
            transport.assert_demo_enabled()
            loader.assert_called_once_with()

    def test_live_key_names_and_extra_credential_fields_are_rejected(self):
        for credentials in ({"OKX_API_KEY": "live", "OKX_API_SECRET": "live", "OKX_API_PASSPHRASE": "live"},
                            {**FAKE, "OKX_API_KEY": "live"}, {**FAKE, CREDENTIAL_NAMES[0]: ""}):
            self.rejected("DEMO_CREDENTIALS_INCOMPLETE", lambda: self.make(credentials=credentials))

    def test_signs_exact_post_body_with_iso_timestamp_and_fixed_demo_host(self):
        self.transport.submit_entry(self.intent, "entry1")
        url, headers, body, timeout = self.wire.posts[0]
        self.assertEqual(url, "https://openapi.okx.com/api/v5/trade/order")
        self.assertEqual(headers["x-simulated-trading"], "1")
        self.assertRegex(headers["OK-ACCESS-TIMESTAMP"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
        signature = base64.b64encode(hmac.new(FAKE[CREDENTIAL_NAMES[1]].encode(),
            (headers["OK-ACCESS-TIMESTAMP"] + "POST/api/v5/trade/order").encode() + body,
            hashlib.sha256).digest()).decode()
        self.assertEqual(headers["OK-ACCESS-SIGN"], signature)
        self.assertLessEqual(timeout, 8)
        payload = json.loads(body)
        self.assertEqual((payload["tdMode"], payload["posSide"]), ("cross", "net"))
        self.assertEqual(payload["ordType"], "post_only" if self.intent.require_maker else "limit")

    def test_global_live_flags_cannot_disable_demo_isolation(self):
        with patch.dict("os.environ", {"OKX_TRADING_ENABLED": "1", "OKX_SIMULATED": "0"}):
            self.transport.submit_entry(self.intent, "entry1")
        self.assertEqual(self.wire.posts[0][1]["x-simulated-trading"], "1")

    def test_bad_instrument_id_number_size_and_notional_fail_before_http(self):
        cases = [(replace(self.intent, inst_id="ETH-USDT-SWAP"), "DEMO_INSTRUMENT"),
                 (replace(self.intent, entry=D("NaN")), "DEMO_INVALID_NUMBER"),
                 (replace(self.intent, contract_value=D("0.01")), "DEMO_CONTRACT_VALUE"),
                 (replace(self.intent, qty=D("5"), notional=D("500")), "DEMO_NOTIONAL_LIMIT"),
                 (replace(self.intent, notional=D("1")), "DEMO_NOTIONAL_LIMIT"),
                 (replace(self.intent, qty=D("0.001")), "DEMO_INVALID_INCREMENT"),
                 (replace(self.intent, stop=D("101")), "DEMO_INVALID_PRICES")]
        for intent, code in cases:
            with self.subTest(code=code):
                self.rejected(code, lambda: self.transport.submit_entry(intent, "entry1"))
        self.rejected("DEMO_INVALID_ID", lambda: self.transport.submit_entry(self.intent, "http://secret"))
        self.assertEqual(self.wire.posts, [])

    def test_timeout_is_one_post_and_never_retried(self):
        self.wire.responses = [TimeoutError("fake-demo-secret https://private.invalid")]
        self.rejected("DEMO_WRITE_UNKNOWN", lambda: self.transport.submit_entry(self.intent, "entry1"))
        self.rejected("DEMO_WRITE_UNKNOWN", lambda: self.transport.submit_entry(self.intent, "entry1"))
        self.assertEqual(len(self.wire.posts), 1)

    def test_successful_write_replay_uses_cached_ack_and_conflict_rejects(self):
        first = self.transport.submit_entry(self.intent, "entry1")
        self.assertEqual(first, self.transport.submit_entry(self.intent, "entry1"))
        smaller = replace(self.intent, qty=D("1"), notional=D("100"))
        self.rejected("DEMO_IDEMPOTENCY_CONFLICT", lambda: self.transport.submit_entry(smaller, "entry1"))
        self.assertEqual(len(self.wire.posts), 1)

    def test_top_and_row_codes_and_malformed_results_fail_closed(self):
        cases = [((403, {}), "DEMO_HTTP_ERROR"),
                 ((200, dict(code="50101", msg="secret", data=[])), "DEMO_API_REJECTED"),
                 ((200, dict(code="0", data=[dict(sCode="51000", sMsg="secret")])), "DEMO_ROW_REJECTED"),
                 ((200, dict(code="0", data=[dict(ordId="one")])), "DEMO_ROW_REJECTED"),
                 ((200, dict(code="0", data=[])), "DEMO_BAD_RESPONSE"),
                 ((200, dict(code="0", data=[dict(sCode="0"), dict(sCode="0")])), "DEMO_BAD_RESPONSE"),
                 ((200, dict(code="0", data=None)), "DEMO_BAD_RESPONSE")]
        for index, (response, code) in enumerate(cases):
            with self.subTest(code=code):
                self.wire.responses = [response]
                self.rejected(code, lambda: self.transport.submit_entry(self.intent, f"entry{index}"))

    def test_get_reuses_demo_client_and_does_not_invent_fields(self):
        raw = dict(instId=T.INST, clOrdId="entry1", ordId="one", state="live",
                   posSide="net", tdMode="cross", ordType="post_only")
        self.wire.rows["/api/v5/trade/order"] = [raw]
        self.assertEqual(self.transport.get_order(T.INST, "entry1"), raw)
        self.assertNotIn("accFillSz", self.transport.get_order(T.INST, "entry1"))
        self.assertEqual(self.wire.gets[0][1]["x-simulated-trading"], "1")

    def test_get_order_requires_raw_net_cross_fields(self):
        raw = dict(instId=T.INST, clOrdId="entry1", ordId="one", state="live",
                   posSide="net", tdMode="cross", ordType="post_only")
        for field in ("posSide", "tdMode", "ordType"):
            with self.subTest(field=field):
                row = dict(raw)
                del row[field]
                self.wire.rows["/api/v5/trade/order"] = [row]
                self.rejected("DEMO_ORDER_MISMATCH", lambda: self.transport.get_order(T.INST, "entry1"))

    def test_order_not_found_is_none_but_unknown_errors_never_empty(self):
        self.wire.rows["/api/v5/trade/order"] = DemoTransportError("DEMO_API_ERROR", 51603)
        self.assertIsNone(self.transport.get_order(T.INST, "entry1"))
        self.wire.rows["/api/v5/trade/order"] = TimeoutError("secret")
        self.rejected("DEMO_READ_UNKNOWN", lambda: self.transport.get_order(T.INST, "entry1"))

    def test_aggregate_failure_preserves_row_number_without_message_or_write_replay(self):
        self.wire.responses = [(200, {"code": "1", "msg": "PRIVATE_SENTINEL",
            "data": [{"sCode": "51046", "sMsg": "PRIVATE_SENTINEL"}]})]
        error = self.rejected("DEMO_ROW_REJECTED",
            lambda: self.transport.submit_entry(self.intent, "aggregate001"))
        self.assertEqual(error.numeric, 51046)
        self.assertNotIn("PRIVATE_SENTINEL", str(error))
        self.rejected("DEMO_WRITE_UNKNOWN",
            lambda: self.transport.submit_entry(self.intent, "aggregate001"))
        self.assertEqual(len(self.wire.posts), 1)
        self.assertNotIn("PRIVATE_SENTINEL", self.transport.audit_path.read_text(encoding="utf-8"))
        for index, data in enumerate(([], [{"sCode": "0"}], [{"sCode": "SECRET"}],
                                      [{"sCode": "51046"}, {"sCode": "51047"}])):
            self.wire.responses = [(200, {"code": "1", "data": data})]
            error = self.rejected("DEMO_API_REJECTED",
                lambda: self.transport.submit_entry(self.intent, "aggregate" + str(index + 2)))
            self.assertEqual(error.numeric, 1)

    def test_order_correlation_and_multiple_rows_rejected(self):
        for rows in ([dict(instId="ETH-USDT-SWAP", clOrdId="entry1", ordId="one")],
                     [dict(instId=T.INST, clOrdId="other", ordId="one")],
                     [dict(instId=T.INST, clOrdId="entry1", ordId="one")] * 2):
            self.wire.rows["/api/v5/trade/order"] = rows
            self.rejected("DEMO_ORDER_MISMATCH", lambda: self.transport.get_order(T.INST, "entry1"))

    def test_oco_creation_is_reduce_only_and_independently_verified(self):
        self.wire.responses = [(200, dict(code="0", data=[dict(sCode="0", algoId="algo1")]))]
        self.wire.on_post = lambda url, body: self.wire.rows.update({"/api/v5/trade/order-algo": [self.protection()]})
        result = self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True)
        payload = json.loads(self.wire.posts[0][2])
        self.assertTrue(payload["reduceOnly"])
        self.assertEqual((payload["ordType"], payload["slOrdPx"], payload["tpOrdPx"]), ("oco", "-1", "-1"))
        self.assertEqual(payload["slTriggerPxType"], "last")
        self.assertEqual(payload["tpTriggerPxType"], "last")
        self.assertEqual(result, self.protection())
        self.assertNotIn("sCode", result)

    def test_new_oco_explicitly_binds_last_feed_without_changing_either_side_prices(self):
        original = self.intent
        for side in ("long", "short"):
            with self.subTest(side=side):
                self.intent = original if side == "long" else replace(original, side="short", stop=D("101"), tp1=D("97"), tp2=D("96"))
                before = self.intent.to_dict()
                self.wire = Wire()
                self.transport = self.make()
                expected_side = "sell" if side == "long" else "buy"
                observed = self.protection(side=expected_side, slTriggerPxType="last", tpTriggerPxType="last")
                self.wire.responses = [(200, dict(code="0", data=[dict(sCode="0", algoId="algo1")]))]
                self.wire.on_post = lambda url, body: self.wire.rows.update({"/api/v5/trade/order-algo": [observed]})
                self.assertEqual(self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True), observed)
                self.assertEqual(len(self.wire.posts), 1)
                body = json.loads(self.wire.posts[0][2])
                self.assertEqual((body["slTriggerPxType"], body["tpTriggerPxType"]), ("last", "last"))
                self.assertEqual((body["slTriggerPx"], body["tpTriggerPx"]), (str(self.intent.stop), str(self.intent.tp1)))
                self.assertEqual((body["side"], body["sz"], body["reduceOnly"]), (expected_side, str(self.intent.qty), True))
                self.assertEqual(self.intent.to_dict(), before)

    def test_existing_oco_trigger_feed_accepts_only_last_or_default_for_both_sides(self):
        original = self.intent
        for side in ("long", "short"):
            self.intent = original if side == "long" else replace(original, side="short", stop=D("101"), tp1=D("97"), tp2=D("96"))
            for changes in ({}, {"slTriggerPxType": ""}, {"tpTriggerPxType": ""},
                            {"slTriggerPxType": "last"}, {"tpTriggerPxType": "last"},
                            {"slTriggerPxType": "", "tpTriggerPxType": "last"},
                            {"slTriggerPxType": "last", "tpTriggerPxType": ""}):
                with self.subTest(side=side, changes=changes):
                    observed = self.protection(side="sell" if side == "long" else "buy", **changes)
                    self.wire.rows["/api/v5/trade/order-algo"] = [observed]
                    self.assertEqual(self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True), observed)
        self.assertEqual(self.wire.posts, [])

    def test_wrong_existing_oco_trigger_feed_is_never_reused_or_amended(self):
        original = self.intent
        for side in ("long", "short"):
            self.intent = original if side == "long" else replace(original, side="short", stop=D("101"), tp1=D("97"), tp2=D("96"))
            for field in ("slTriggerPxType", "tpTriggerPxType"):
                for value in ("mark", "index", "INVALID", "LAST", " last", None, False, 0, [], {}):
                    with self.subTest(side=side, field=field, value=value):
                        observed = self.protection(side="sell" if side == "long" else "buy", **{field: value})
                        self.wire.rows["/api/v5/trade/order-algo"] = [observed]
                        self.rejected("DEMO_PROTECTION_MISMATCH", lambda: self.transport.set_protection(
                            self.intent, self.intent.qty, "protect1", reduce_only=True))
        self.assertEqual(self.wire.posts, [])

    def test_wrong_trigger_feed_after_oco_post_never_confirms_protection_or_replays(self):
        original = self.intent
        for side in ("long", "short"):
            self.intent = original if side == "long" else replace(original, side="short", stop=D("101"), tp1=D("97"), tp2=D("96"))
            for field in ("slTriggerPxType", "tpTriggerPxType"):
                for value in ("mark", "index", "INVALID"):
                    with self.subTest(side=side, field=field, value=value):
                        self.wire = Wire()
                        self.transport = self.make()
                        observed = self.protection(side="sell" if side == "long" else "buy", **{field: value})
                        self.wire.responses = [(200, {"code": "0", "data": [{"sCode": "0", "algoId": "algo1"}]})]
                        self.wire.on_post = lambda url, body: self.wire.rows.update({"/api/v5/trade/order-algo": [observed]})
                        self.rejected("DEMO_PROTECTION_UNCONFIRMED", lambda: self.transport.set_protection(
                            self.intent, self.intent.qty, "protect1", reduce_only=True))
                        self.rejected("DEMO_PROTECTION_MISMATCH", lambda: self.transport.set_protection(
                            self.intent, self.intent.qty, "protect1", reduce_only=True))
                        self.assertEqual(len(self.wire.posts), 1)

    def test_missing_protection_verification_fields_are_never_filled(self):
        for field in ("reduceOnly", "posSide", "tdMode", "ordType", "slOrdPx", "tpOrdPx", "slTriggerPx", "tpTriggerPx"):
            with self.subTest(field=field):
                raw = self.protection()
                del raw[field]
                self.wire.rows["/api/v5/trade/order-algo"] = [raw]
                self.rejected("DEMO_PROTECTION_MISMATCH", lambda: self.transport.set_protection(
                    self.intent, self.intent.qty, "protect1", reduce_only=True))
        self.assertEqual(self.wire.posts, [])

    def test_in_place_amend_keeps_algo_id_and_stable_request_id(self):
        old = self.protection(D("0.01"))
        self.wire.rows["/api/v5/trade/order-algo"] = [old]
        self.wire.responses = [(200, dict(code="0", data=[dict(sCode="0", algoId="algo1")]))]
        self.wire.on_post = lambda url, body: self.wire.rows.update({"/api/v5/trade/order-algo": [self.protection()]})
        self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True)
        url, _, data, _ = self.wire.posts[0]
        payload = json.loads(data)
        self.assertTrue(url.endswith("/trade/amend-algos"))
        self.assertEqual(payload["algoId"], "algo1")
        self.assertFalse(payload["cxlOnFail"])
        expected = "a" + hashlib.sha256(("protect1:" + str(self.intent.qty.normalize())).encode()).hexdigest()[:31]
        self.assertEqual(payload["reqId"], expected)
        self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True)
        self.assertEqual(len(self.wire.posts), 1)

    def test_protection_timeout_queries_once_without_second_post(self):
        self.wire.responses = [TimeoutError("secret")]
        self.rejected("DEMO_PROTECTION_UNCONFIRMED", lambda: self.transport.set_protection(
            self.intent, self.intent.qty, "protect1", reduce_only=True))
        self.rejected("DEMO_PROTECTION_UNKNOWN", lambda: self.transport.set_protection(
            self.intent, self.intent.qty, "protect1", reduce_only=True))
        self.assertEqual(len(self.wire.posts), 1)
        self.assertEqual(len(self.wire.gets), 3)

    def test_timeout_succeeded_on_exchange_is_proven_by_raw_get(self):
        self.wire.responses = [TimeoutError("secret")]
        self.wire.on_post = lambda url, body: self.wire.rows.update({"/api/v5/trade/order-algo": [self.protection()]})
        self.assertEqual(self.transport.set_protection(self.intent, self.intent.qty, "protect1", reduce_only=True),
                         self.protection())
        self.assertEqual(len(self.wire.posts), 1)

    def test_do_not_shrink_or_change_protection_prices(self):
        self.wire.rows["/api/v5/trade/order-algo"] = [self.protection()]
        self.rejected("DEMO_PROTECTION_SHRINK", lambda: self.transport.set_protection(
            self.intent, D("0.01"), "protect1", reduce_only=True))
        self.wire.rows["/api/v5/trade/order-algo"] = [self.protection(slTriggerPx="98")]
        self.rejected("DEMO_PROTECTION_MISMATCH", lambda: self.transport.set_protection(
            self.intent, self.intent.qty, "protect1", reduce_only=True))
        self.assertEqual(self.wire.posts, [])

    def test_cancel_and_emergency_reduce_only_are_single_attempts(self):
        self.transport.cancel_entry(T.INST, "entry1")
        self.transport.cancel_entry(T.INST, "entry1")
        self.transport.emergency_close(T.INST, "sell", D("0.1"), "close1", reduce_only=True)
        self.transport.emergency_close(T.INST, "sell", D("0.1"), "close1", reduce_only=True)
        self.assertEqual(len(self.wire.posts), 2)
        body = json.loads(self.wire.posts[-1][2])
        self.assertEqual((body["ordType"], body["posSide"], body["tdMode"]), ("market", "net", "cross"))
        self.assertTrue(body["reduceOnly"])
        self.rejected("DEMO_REDUCE_ONLY_REQUIRED", lambda: self.transport.emergency_close(
            T.INST, "sell", D("0.1"), "close2", reduce_only=False))

    def test_audit_has_allowlisted_metadata_and_independent_run_directory(self):
        self.wire.responses = [RuntimeError("fake-demo-secret https://private.invalid")]
        self.rejected("DEMO_WRITE_UNKNOWN", lambda: self.transport.submit_entry(self.intent, "entry1"))
        text = self.transport.audit_path.read_text(encoding="utf-8")
        for marker in list(FAKE.values()) + ["private.invalid", "OK-ACCESS", "sMsg"]:
            self.assertNotIn(marker, text)
        self.assertEqual(text.splitlines()[0], ",".join(T.AUDIT_FIELDS))
        self.assertNotEqual(self.make().run_dir, self.transport.run_dir)

    def test_post_network_reader_is_bounded_and_disables_redirects(self):
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self, count):
                self_count.append(count)
                return b"x" * count
        self_count = []
        with patch.object(T.urllib.request, "build_opener") as opener:
            opener.return_value.open.return_value = Response()
            self.rejected("DEMO_RESPONSE_TOO_LARGE", lambda: T._real_http_post(
                T.BASE + "/api/v5/trade/order", {"x-simulated-trading": "1"}, b"{}", 5))
            self.assertEqual(self_count, [T.MAX_BODY + 1])
            self.assertIs(opener.call_args.args[0], T._NoRedirect)

    def test_redirect_exception_never_exposes_url_or_retries(self):
        self.wire.responses = [DemoTransportError("DEMO_REDIRECT_FORBIDDEN")]
        self.rejected("DEMO_WRITE_UNKNOWN", lambda: self.transport.submit_entry(self.intent, "entry1"))
        self.assertEqual(len(self.wire.posts), 1)

    def test_exposure_reads_all_products_and_all_supported_algo_families(self):
        self.wire.rows["/api/v5/account/positions"] = [dict(instId="ETH-USD-261225", pos="-2", posSide="net"),
                                                        dict(instId=T.INST, pos="0", posSide="net")]
        result = self.transport.read_exposure()
        self.assertEqual(result["positions"][0]["instId"], "ETH-USD-261225")
        self.assertEqual(len(result["positions"]), 1)
        position_call = next(call[0] for call in self.wire.gets if urlsplit(call[0]).path.endswith("account/positions"))
        self.assertEqual(urlsplit(position_call).query, "")
        algorithm_calls = [call for call in self.wire.gets if urlsplit(call[0]).path.endswith("orders-algo-pending")]
        kinds = [parse_qs(urlsplit(call[0]).query)["ordType"][0] for call in algorithm_calls]
        self.assertEqual(set(kinds), set(T.ALGO_TYPES))
        for call in algorithm_calls:
            query = parse_qs(urlsplit(call[0]).query)
            self.assertNotIn("instId", query)
            self.assertNotIn("instType", query)

    def test_foreign_instrument_algorithm_is_reported_and_never_canceled(self):
        foreign = dict(instId="ETH-USDT-SWAP", algoId="foreign1", ordType="trigger")
        self.wire.rows["/api/v5/trade/orders-algo-pending"] = lambda params: [foreign] if params["ordType"] == ["trigger"] else []
        self.assertEqual(self.transport.read_exposure()["algos"], [foreign])
        self.assertEqual(self.wire.posts, [])

    def test_unknown_exposure_fields_or_unsupported_algo_query_refuse(self):
        self.wire.rows["/api/v5/account/positions"] = [dict(instId=T.INST, posSide="net")]
        self.rejected("DEMO_INVALID_NUMBER", self.transport.read_exposure)
        self.wire.rows["/api/v5/account/positions"] = []
        self.wire.rows["/api/v5/trade/orders-algo-pending"] = DemoTransportError("DEMO_API_ERROR", 51000)
        self.rejected("DEMO_READ_UNKNOWN", self.transport.read_exposure)

    def test_full_pending_page_is_not_false_empty_or_complete(self):
        self.wire.rows["/api/v5/trade/orders-pending"] = [dict(instId=T.INST, ordId="one", state="live", sz="1", accFillSz="0")] * 100
        self.rejected("DEMO_EXPOSURE_UNKNOWN", self.transport.read_exposure)

    def test_readonly_transport_can_observe_without_enable_writes(self):
        self.assertEqual(self.make(enable_writes=False).read_exposure(), dict(positions=[], pending=[], algos=[]))
        self.assertEqual(self.wire.posts, [])

    def test_timeout_validation_and_post_path_lock(self):
        for timeout in (0, 9, True, "8"):
            self.rejected("DEMO_TIMEOUT", lambda: self.make(timeout=timeout))
        self.rejected("DEMO_PATH_NOT_ALLOWED", lambda: T._real_http_post("https://evil.invalid/api/v5/trade/order", {}, b"{}", 5))

    def test_lifecycle_proves_raw_position_identity_with_unfiltered_entry_fills_only(self):
        self.lifecycle()
        proof = self.verify_lifecycle()
        self.assertTrue(proof["position_binding_verified"])
        self.assertEqual(proof["position_id"], self.life_position["posId"])
        self.assertEqual(proof["position_opened_at_ms"], int(self.life_position["cTime"]))
        self.assertEqual(proof["flat_observed_at_ms"], self.flat_ms)
        self.assertEqual(proof["fills"], self.life_fills)
        queries = [parse_qs(urlsplit(call[0]).query) for call in self.wire.gets
                   if urlsplit(call[0]).path.endswith("fills-history")]
        self.assertEqual(len(queries), 1)
        self.assertEqual(queries[0]["begin"], [str(self.flat_ms)])
        self.assertEqual(queries[0]["limit"], ["100"])
        self.assertNotIn("ordId", queries[0])
        self.assertNotIn("clOrdId", queries[0])
        self.assertEqual(self.wire.posts, [])
        self.assertLessEqual(len(self.wire.gets), 25)
        self.assertTrue(all(1 <= call[2] <= T.LIFECYCLE_EVIDENCE_SECONDS for call in self.wire.gets))

    def test_lifecycle_requires_persisted_valid_flat_observation_not_intent_time_guess(self):
        self.lifecycle()
        for value in (None, 0, -1, True, str(self.flat_ms)):
            with self.subTest(value=value):
                self.rejected("DEMO_FLAT_EVIDENCE_MISSING", lambda: self.verify_lifecycle(flat_observed_at_ms=value))
        self.assertEqual(self.wire.gets, [])
        self.rejected("DEMO_FLAT_EVIDENCE_INVALID", lambda: self.verify_lifecycle(flat_observed_at_ms=self.server_ms+1))

    def test_lifecycle_external_replacement_fill_cannot_be_hidden_by_current_matching_size(self):
        self.lifecycle()
        external = dict(self.life_fills[0], ordId="manual", clOrdId="manual", tradeId="manualtrade", billId="manualbill")
        self.life_fills.append(external)
        self.rejected("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])

    def test_lifecycle_any_exit_or_nonzero_realized_fill_is_unproven_ownership(self):
        for field, value in (("side", "sell"), ("subType", "5"), ("fillPnl", "1"),
                             ("posSide", "long"), ("feeCcy", "BTC"), ("clOrdId", "manual")):
            with self.subTest(field=field):
                self.lifecycle()
                self.life_fills[0][field] = value
                self.rejected("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY", self.verify_lifecycle)

    def test_lifecycle_capped_or_missing_history_is_never_a_complete_binding(self):
        self.lifecycle()
        self.wire.rows["/api/v5/trade/fills-history"] = [deepcopy(self.life_fills[0]) for _ in range(100)]
        self.rejected("DEMO_LIFECYCLE_HISTORY_INCOMPLETE", self.verify_lifecycle)
        self.wire.rows["/api/v5/trade/fills-history"] = []
        self.rejected("DEMO_LIFECYCLE_FILL_UNPROVEN", self.verify_lifecycle)
        self.wire.rows["/api/v5/trade/fills-history"] = None
        self.rejected("DEMO_READ_UNKNOWN", self.verify_lifecycle)

    def test_lifecycle_partial_fills_sum_to_exact_observed_position(self):
        self.lifecycle()
        original = D(self.life_fills[0]["fillSz"])
        self.life_fills[0]["fillSz"] = str(D("0.01"))
        self.life_fills.append(dict(self.life_fills[0], tradeId="trade002", billId="bill002",
                                   fillTime=str(self.opened_ms+1), fillSz=str(original-D("0.01"))))
        self.assertTrue(self.verify_lifecycle()["position_binding_verified"])
        self.life_fills[-1]["fillSz"] = "0.01"
        self.rejected("DEMO_LIFECYCLE_FILL_UNPROVEN", self.verify_lifecycle)

    def test_lifecycle_no_position_id_or_wrong_opening_timestamp_cannot_be_invented(self):
        for field, value in (("posId", None), ("cTime", str(self.intent.created_at_ms)),
                             ("pos", "-1"), ("mgnMode", "isolated"), ("posSide", "long")):
            with self.subTest(field=field):
                self.lifecycle()
                self.life_position[field] = value
                with self.assertRaises(T.DemoRouteError):
                    self.verify_lifecycle()

    def test_lifecycle_same_quantity_other_position_id_between_reads_refuses_binding(self):
        self.lifecycle()
        reads = 0
        def positions(params):
            nonlocal reads
            reads += 1
            return [dict(self.life_position, posId="original" if reads == 1 else "manualreplacement")]
        self.wire.rows["/api/v5/account/positions"] = positions
        self.rejected("DEMO_LIFECYCLE_POSITION_CHANGED", self.verify_lifecycle)

    def test_lifecycle_foreign_position_or_pending_order_is_never_adopted(self):
        self.lifecycle()
        self.wire.rows["/api/v5/account/positions"] = [self.life_position,
                      dict(instId="ETH-USDT-SWAP", pos="1", posSide="net")]
        self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)
        self.lifecycle()
        self.wire.rows["/api/v5/trade/orders-pending"] = [dict(instId=T.INST, ordId="foreign", clOrdId="manual",
                  state="live", accFillSz="0", sz="1", side="buy", posSide="net", tdMode="cross", ordType="limit")]
        self.rejected("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY", self.verify_lifecycle)

    def test_lifecycle_allows_only_matching_deterministic_protection_parent_for_caller_check(self):
        self.lifecycle()
        protection_id = "r"+hashlib.sha256((self.intent.signal_id+":protection").encode()).hexdigest()[:31]
        algo = self.protection(algoClOrdId=protection_id)
        self.wire.rows["/api/v5/trade/orders-algo-pending"] = lambda p: [deepcopy(algo)] if p["ordType"] == ["oco"] else []
        proof = self.verify_lifecycle()
        self.assertEqual(proof["exposure"]["algos"][0]["algoId"], "algo1")
        algo["algoClOrdId"] = "foreign"
        self.rejected("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY", self.verify_lifecycle)

    def test_lifecycle_tail_read_catches_external_fill_after_initial_server_cutoff(self):
        self.lifecycle()
        times = iter((self.server_ms, self.server_ms+100))
        self.wire.rows["/api/v5/public/time"] = lambda p: [{"ts": str(next(times))}]
        self.life_fills.append(dict(self.life_fills[0], tradeId="newtrade", billId="newbill", ordId="manual",
                                   clOrdId="manual", fillTime=str(self.server_ms+50)))
        self.rejected("DEMO_LIFECYCLE_EXTERNAL_ACTIVITY", self.verify_lifecycle)
        fill_calls = [call for call in self.wire.gets if urlsplit(call[0]).path.endswith("fills-history")]
        self.assertEqual(len(fill_calls), 2)
        self.assertLessEqual(len(self.wire.gets), 25)

    def test_lifecycle_duplicate_fills_and_ignored_time_filter_are_refused(self):
        self.lifecycle()
        self.life_fills.append(deepcopy(self.life_fills[0]))
        self.rejected("DEMO_LIFECYCLE_DUPLICATE_FILL", self.verify_lifecycle)
        self.lifecycle()
        self.wire.rows["/api/v5/trade/fills-history"] = [dict(self.life_fills[0], fillTime=str(self.flat_ms-1))]
        self.rejected("DEMO_LIFECYCLE_HISTORY_OUTSIDE_WINDOW", self.verify_lifecycle)

    def test_lifecycle_actual_order_mismatch_or_cumulative_fill_change_refuses(self):
        for field, value in (("ordId", "foreign"), ("accFillSz", "0"), ("side", "sell"), ("sz", "999")):
            with self.subTest(field=field):
                self.lifecycle()
                self.life_order[field] = value
                self.rejected("DEMO_LIFECYCLE_ORDER_MISMATCH", self.verify_lifecycle)

    def test_lifecycle_five_second_evidence_budget_never_relaxes_freshness(self):
        self.lifecycle()
        times = iter((self.server_ms, self.server_ms+5001))
        self.wire.rows["/api/v5/public/time"] = lambda p: [{"ts": str(next(times))}]
        self.rejected("DEMO_LIFECYCLE_STALE", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])

    def test_lifecycle_short_position_uses_sell_entry_and_negative_net_quantity(self):
        self.lifecycle()
        intent = replace(self.intent, side="short", stop=D("101"), tp1=D("97"), tp2=D("96"))
        self.life_position["pos"] = str(-intent.qty)
        self.life_order["side"] = "sell"
        self.life_fills[0].update(side="sell", subType="4")
        self.assertTrue(self.verify_lifecycle(intent=intent)["position_binding_verified"])

    def test_lifecycle_conflicting_trade_or_bill_duplicate_cannot_double_prove_quantity(self):
        for reused in ("tradeId", "billId"):
            with self.subTest(reused=reused):
                self.lifecycle()
                original_size = D(self.life_fills[0]["fillSz"])
                self.life_fills[0]["fillSz"] = "0.01"
                other = dict(self.life_fills[0], tradeId="differenttrade", billId="differentbill",
                             fillSz=str(original_size-D("0.01")))
                other[reused] = self.life_fills[0][reused]
                self.life_fills.append(other)
                self.rejected("DEMO_LIFECYCLE_DUPLICATE_FILL", self.verify_lifecycle)

    def test_lifecycle_real_elapsed_deadline_stops_read_wave_without_any_write(self):
        self.lifecycle()
        clock = [0.0]
        def expired_positions(params):
            clock[0] = 6.0
            return [deepcopy(self.life_position)]
        self.wire.rows["/api/v5/account/positions"] = expired_positions
        with patch.object(T.time, "monotonic", side_effect=lambda: clock[0]):
            self.rejected("DEMO_LIFECYCLE_STALE", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])
        self.assertLessEqual(len(self.wire.gets), 25)


if __name__ == "__main__":
    unittest.main()
