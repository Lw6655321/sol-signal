"""Offline contracts for opt-in, bounded fixed-DEMO GET retries."""
from __future__ import annotations

import errno
import json
import socket
import ssl
import threading
import unittest
import urllib.error
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from src import demo_transport as T
from src import demo_get_pool as P
from src import demo_verified_risk as V
from src.demo_verified_risk import SAFE_GET_PATHS


CREDS = dict(zip(T.CREDENTIAL_NAMES, ("test-key", "test-secret", "test-pass")))
PATH = "/api/v5/account/positions-history"
OK = (200, {"code": "0", "data": [{"verified": True}]})
PRIVATE_MARKER = "sensitive-marker-must-never-be-logged"


class DemoGetNetworkRetryTests(unittest.TestCase):
    def setUp(self):
        self.network = mock.patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.sleep = mock.patch.object(T.time, "sleep")
        self.sleep_mock = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def client(self, wire, **kwargs):
        # The transport-only fixtures explicitly grant a fake budget. Production
        # wiring is exercised below with the real provider's shared limiter.
        if type(kwargs.get("network_retries")) is int and kwargs["network_retries"] == 1 and "retry_budget" not in kwargs:
            kwargs["retry_budget"] = lambda path: None
        return T.DemoReadOnlyClient(dict(CREDS), http_get=wire, **kwargs)

    def assert_error(self, client, code="DEMO_NETWORK_ERROR", path=PATH, params=None):
        with self.assertRaises(T.DemoTransportError) as caught:
            client.get(path, params)
        self.assertEqual(caught.exception.code, code)
        self.assertNotIn(PRIVATE_MARKER, str(caught.exception))
        return caught.exception

    def pooled_client(self, outcomes, **kwargs):
        """Real pool/client boundary, with in-memory responses and no sockets."""
        class Response:
            status, headers = 200, {}
            def __init__(self, outcome):
                self.outcome, self.closed, self.events = outcome, False, []
            def read(self, *_args, **_kwargs):
                self.events.append("read")
                if isinstance(self.outcome, Exception):
                    raise self.outcome
                self.closed = True
                return json.dumps(self.outcome).encode("utf-8")
            def close(self):
                self.events.append("close")
                self.closed = True
            def release_conn(self):
                self.events.append("release")
        class Manager:
            def __init__(self):
                self.calls, self.responses, self.errors, self.clears = [], [], [], 0
                self.outcomes = deque(outcomes)
                self.pools = {}
            def request(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                outcome = self.outcomes.popleft()
                if isinstance(outcome, Exception):
                    raise outcome
                response = outcome if isinstance(outcome, Response) else Response(outcome)
                self.responses.append(response)
                return response
            def clear(self):
                self.clears += 1
        manager = Manager()
        with mock.patch.object(P.urllib3, "PoolManager", return_value=manager), \
                mock.patch.object(P, "_tls_context", return_value=object()):
            pool = P.DemoGetPool()
        self.addCleanup(pool.close)
        def wire(*args):
            try:
                return pool(*args)
            except T.DemoTransportError as error:
                manager.errors.append(error)
                raise
        return self.client(wire, **kwargs), pool, manager, Response

    def test_pooled_tls_eof_uses_existing_budget_then_fresh_signature_once(self):
        sequence = []
        budget = mock.Mock(side_effect=lambda path: sequence.append(("reserve", path)))
        client, pool, manager, _ = self.pooled_client(
            [P.urllib3.exceptions.SSLError(ssl.SSLEOFError(PRIVATE_MARKER)), OK[1]],
            network_retries=1, retry_budget=budget)
        original_headers = client._headers
        def headers(*args):
            sequence.append(("headers", args[0]))
            return original_headers(*args)
        self.sleep_mock.side_effect = lambda delay: sequence.append(("sleep", delay))
        with mock.patch.object(T, "_now_iso", side_effect=["2026-10-20T00:00:00.000Z", "2026-10-20T00:00:00.100Z"]), \
                mock.patch.object(client, "_headers", side_effect=headers):
            self.assertEqual(client.get(PATH, {"after": PRIVATE_MARKER}), OK[1]["data"])
        self.assertEqual(sequence, [("headers", "GET"), ("sleep", 0.1), ("reserve", PATH), ("headers", "GET")])
        self.assertEqual(len(manager.calls), 2)
        first, second = manager.calls
        self.assertEqual(first[0], second[0])
        self.assertEqual(first[0][0], "GET")
        for _, options in manager.calls:
            self.assertIs(options["retries"], False)
            self.assertIs(options["redirect"], False)
            self.assertEqual(options["headers"][T.DEMO_HEADER], "1")
            self.assertEqual(options["timeout"].total, T.MAX_TIMEOUT)
            self.assertEqual(options["pool_timeout"], T.MAX_TIMEOUT)
        self.assertNotEqual(first[1]["headers"]["OK-ACCESS-TIMESTAMP"], second[1]["headers"]["OK-ACCESS-TIMESTAMP"])
        self.assertNotEqual(first[1]["headers"]["OK-ACCESS-SIGN"], second[1]["headers"]["OK-ACCESS-SIGN"])
        self.assertEqual(pool._active, 0)
        self.assertEqual(manager.responses[0].events, ["read", "release"])
        self.assertEqual(client.retry_events(), [{"path": PATH, "code": "DEMO_NETWORK_RETRY", "attempt": 2, "type": "SSLEOFError"}])
        encoded = json.dumps(client.retry_events())
        self.assertNotIn(PRIVATE_MARKER, encoded)
        for value in CREDS.values():
            self.assertNotIn(value, encoded)

    def test_pooled_second_tls_eof_preserves_final_error_and_stops(self):
        budget = mock.Mock()
        client, pool, manager, _ = self.pooled_client(
            [ssl.SSLEOFError(PRIVATE_MARKER), ssl.SSLEOFError(PRIVATE_MARKER), OK[1]],
            network_retries=1, retry_budget=budget)
        final = self.assert_error(client)
        self.assertEqual(len(manager.calls), 2)
        self.assertIs(final, manager.errors[1])
        self.assertIsNot(final, manager.errors[0])
        self.assertEqual(final.network_category, "TLS_EOF")
        self.assertIsNone(final.numeric)
        self.assertEqual(str(final), "DEMO_NETWORK_ERROR")
        self.assertTrue(final.__suppress_context__)
        budget.assert_called_once_with(PATH)
        self.sleep_mock.assert_called_once_with(0.1)
        self.assertEqual(len(client.retry_events()), 1)
        self.assertEqual(pool._active, 0)

    def test_pooled_tls_eof_default_and_explicit_zero_do_not_retry(self):
        for options in ({}, {"network_retries": 0}):
            with self.subTest(explicit_zero=bool(options)):
                budget = mock.Mock()
                client, _, manager, _ = self.pooled_client(
                    [ssl.SSLEOFError(PRIVATE_MARKER), OK[1]], retry_budget=budget, **options)
                self.assertIs(self.assert_error(client), manager.errors[0])
                self.assertEqual(len(manager.calls), 1)
                budget.assert_not_called()
                self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_pooled_tls_eof_keeps_market_and_other_endpoints_outside_retry_scope(self):
        for path in ("/api/v5/market/ticker", "/api/v5/trade/order-algo", "/api/v5/private/unknown"):
            with self.subTest(path=path):
                budget = mock.Mock()
                client, _, manager, _ = self.pooled_client(
                    [ssl.SSLEOFError(PRIVATE_MARKER), OK[1]], network_retries=1, retry_budget=budget)
                self.assert_error(client, path=path)
                self.assertEqual(len(manager.calls), 1)
                budget.assert_not_called()
                self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_other_pooled_network_categories_still_refuse_without_retry(self):
        reasons = (ssl.SSLCertVerificationError(PRIVATE_MARKER), ssl.SSLError(PRIVATE_MARKER),
                   socket.gaierror(socket.EAI_AGAIN, PRIVATE_MARKER), TimeoutError(PRIVATE_MARKER),
                   ConnectionResetError(PRIVATE_MARKER), RuntimeError(PRIVATE_MARKER))
        for reason in reasons:
            with self.subTest(reason=type(reason).__name__):
                budget = mock.Mock()
                client, _, manager, _ = self.pooled_client([reason, OK[1]], network_retries=1, retry_budget=budget)
                final = self.assert_error(client)
                self.assertNotEqual(final.network_category, "TLS_EOF")
                self.assertEqual(len(manager.calls), 1)
                budget.assert_not_called()
                self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_pooled_retry_requires_exact_error_code_numeric_and_category_types(self):
        class Text(str):
            pass
        class DerivedError(T.DemoTransportError):
            pass
        errors = []
        for category in (None, "TLS", "tls_eof", "TLS_EOF ", Text("TLS_EOF"), [], {}):
            error = T.DemoTransportError("DEMO_NETWORK_ERROR")
            error.network_category = category
            errors.append(error)
        for error in (T.DemoTransportError("DEMO_BAD_JSON"), T.DemoTransportError("DEMO_NETWORK_ERROR", 0),
                      T.DemoTransportError(Text("DEMO_NETWORK_ERROR")), DerivedError("DEMO_NETWORK_ERROR")):
            error.network_category = "TLS_EOF"
            errors.append(error)
        for error in errors:
            with self.subTest(kind=type(error).__name__, category_type=type(error.network_category).__name__):
                budget = mock.Mock()
                wire = mock.Mock(side_effect=error)
                client = self.client(wire, network_retries=1, retry_budget=budget)
                self.assertIs(self.assert_error(client, str(error.code)), error)
                wire.assert_called_once()
                budget.assert_not_called()
                self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_pooled_retry_budget_failure_refuses_before_headers_or_second_get(self):
        budget = mock.Mock(side_effect=RuntimeError(PRIVATE_MARKER))
        client, pool, manager, _ = self.pooled_client(
            [ssl.SSLEOFError(PRIVATE_MARKER), OK[1]], network_retries=1, retry_budget=budget)
        with mock.patch.object(client, "_headers", wraps=client._headers) as headers:
            self.assert_error(client, "DEMO_NETWORK_RETRY_BUDGET_FAILED")
        self.assertEqual(headers.call_count, 1)
        self.assertEqual(len(manager.calls), 1)
        budget.assert_called_once_with(PATH)
        self.assertEqual(client.retry_events(), [])
        self.assertEqual(pool._active, 0)

    def test_pooled_read_eof_releases_response_before_retry_budget(self):
        # The first read fails after a response has been acquired; connection
        # cleanup must finish before the client reserves another physical GET.
        client, pool, manager, Response = self.pooled_client([], network_retries=1)
        failed = Response(ssl.SSLEOFError(PRIVATE_MARKER))
        manager.outcomes.extend((failed, OK[1]))
        def reserve(path):
            self.assertEqual(path, PATH)
            self.assertEqual(failed.events, ["read", "close", "release"])
            self.assertEqual(pool._active, 0)
        client._retry_budget = mock.Mock(side_effect=reserve)
        self.assertEqual(client.get(PATH), OK[1]["data"])
        client._retry_budget.assert_called_once_with(PATH)
        self.assertEqual(len(manager.calls), 2)

    def test_pooled_release_failure_overrides_eof_without_retry(self):
        client, _, manager, Response = self.pooled_client([], network_retries=1)
        failed = Response(ssl.SSLEOFError(PRIVATE_MARKER))
        def failed_release():
            raise RuntimeError(PRIVATE_MARKER)
        failed.release_conn = failed_release
        manager.outcomes.extend((failed, OK[1]))
        budget = client._retry_budget = mock.Mock()
        self.assert_error(client, "DEMO_POOL_RELEASE_FAILED")
        self.assertEqual(len(manager.calls), 1)
        budget.assert_not_called()
        self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_pooled_client_post_remains_refused_without_pool_or_budget(self):
        budget = mock.Mock()
        client, _, manager, _ = self.pooled_client([ssl.SSLEOFError(PRIVATE_MARKER)], network_retries=1, retry_budget=budget)
        with self.assertRaises(T.DemoWriteRefused):
            client.post("/api/v5/trade/order", {"sensitive": PRIVATE_MARKER})
        self.assertEqual(manager.calls, [])
        budget.assert_not_called()
        self.sleep_mock.assert_not_called()

    def test_retry_policy_accepts_only_strict_zero_or_one(self):
        class IntSubclass(int):
            pass
        for invalid in (True, False, -1, 2, 0.0, 1.0, "1", None, {}, IntSubclass(1)):
            with self.subTest(value=type(invalid).__name__):
                with self.assertRaises(T.DemoTransportError) as caught:
                    self.client(mock.Mock(), network_retries=invalid)
                self.assertEqual(caught.exception.code, "DEMO_NETWORK_RETRY_POLICY_INVALID")
        for valid in (0, 1):
            self.assertEqual(self.client(mock.Mock(return_value=OK), network_retries=valid).get(PATH), OK[1]["data"])

    def test_default_remains_one_network_attempt(self):
        wire = mock.Mock(side_effect=urllib.error.URLError(TimeoutError(PRIVATE_MARKER)))
        client = self.client(wire)
        self.assert_error(client)
        self.assertEqual(wire.call_count, 1)
        self.sleep_mock.assert_not_called()
        self.assertEqual(client.retry_events(), [])

    def test_explicit_zero_remains_one_network_attempt(self):
        wire = mock.Mock(side_effect=urllib.error.URLError(ConnectionResetError(PRIVATE_MARKER)))
        client = self.client(wire, network_retries=0)
        self.assert_error(client)
        self.assertEqual(wire.call_count, 1)
        self.assertEqual(client.retry_events(), [])

    def test_retry_recovers_once_with_fresh_timestamp_and_signature(self):
        wire = mock.Mock(side_effect=[urllib.error.URLError(TimeoutError(PRIVATE_MARKER)), OK])
        client = self.client(wire, network_retries=1)
        with mock.patch.object(T, "_now_iso", side_effect=["2026-10-20T00:00:00.000Z", "2026-10-20T00:00:00.100Z"]):
            result = client.get(PATH, {"after": PRIVATE_MARKER})
        self.assertEqual(result, OK[1]["data"])
        self.assertEqual(wire.call_count, 2)
        first, second = [call.args for call in wire.call_args_list]
        self.assertEqual(first[0], second[0])
        self.assertTrue(first[0].startswith(T.DEMO_BASE + PATH + "?"))
        self.assertEqual(first[2], second[2])
        self.assertNotEqual(first[1]["OK-ACCESS-TIMESTAMP"], second[1]["OK-ACCESS-TIMESTAMP"])
        self.assertNotEqual(first[1]["OK-ACCESS-SIGN"], second[1]["OK-ACCESS-SIGN"])
        for _, headers, _ in (first, second):
            self.assertEqual(headers[T.DEMO_HEADER], "1")
        self.sleep_mock.assert_called_once_with(0.1)
        self.assertEqual(client.retry_events(), [{"path": PATH, "code": "DEMO_NETWORK_RETRY",
                                               "attempt": 2, "type": "TimeoutError"}])

    def test_second_network_failure_stops_after_two_attempts(self):
        wire = mock.Mock(side_effect=urllib.error.URLError(ConnectionResetError(PRIVATE_MARKER)))
        client = self.client(wire, network_retries=1)
        self.assert_error(client)
        self.assertEqual(wire.call_count, 2)
        self.sleep_mock.assert_called_once_with(0.1)
        self.assertEqual(len(client.retry_events()), 1)

    def test_explicit_transient_reason_types_can_retry(self):
        reasons = (TimeoutError(), ConnectionResetError(), ConnectionAbortedError(),
                   ConnectionRefusedError(), BrokenPipeError(), EOFError(), ssl.SSLEOFError(),
                   socket.gaierror(socket.EAI_AGAIN, PRIVATE_MARKER),
                   OSError(errno.ETIMEDOUT, PRIVATE_MARKER), OSError(errno.ECONNRESET, PRIVATE_MARKER),
                   OSError(errno.ECONNABORTED, PRIVATE_MARKER), OSError(errno.ECONNREFUSED, PRIVATE_MARKER),
                   OSError(errno.EPIPE, PRIVATE_MARKER))
        for reason in reasons:
            with self.subTest(reason=type(reason).__name__, errno=getattr(reason, "errno", None)):
                wire = mock.Mock(side_effect=[urllib.error.URLError(reason), OK])
                client = self.client(wire, network_retries=1)
                self.assertEqual(client.get(PATH), OK[1]["data"])
                self.assertEqual(wire.call_count, 2)
                self.assertEqual(len(client.retry_events()), 1)

    def test_http_errors_are_never_retried_even_with_transient_reason(self):
        for status in (400, 401, 403, 404, 429, 500, 503):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(T.DEMO_BASE + PATH, status,
                                               TimeoutError(PRIVATE_MARKER), {}, None)
                wire = mock.Mock(side_effect=error)
                client = self.client(wire, network_retries=1)
                self.assert_error(client)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])
        self.sleep_mock.assert_not_called()

    def test_returned_http_failures_are_never_retried(self):
        for status in (401, 403, 429, 500, 503):
            with self.subTest(status=status):
                wire = mock.Mock(return_value=(status, OK[1]))
                client = self.client(wire, network_retries=1)
                self.assertEqual(self.assert_error(client, "DEMO_HTTP_ERROR").numeric, status)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_certificate_hostname_and_unknown_tls_failures_are_never_retried(self):
        reasons = (ssl.SSLCertVerificationError(PRIVATE_MARKER), ssl.CertificateError(PRIVATE_MARKER),
                   ssl.SSLError(PRIVATE_MARKER), ssl.SSLError(errno.ETIMEDOUT, PRIVATE_MARKER))
        for reason in reasons:
            with self.subTest(reason=type(reason).__name__):
                wire = mock.Mock(side_effect=urllib.error.URLError(reason))
                client = self.client(wire, network_retries=1)
                self.assert_error(client)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_unknown_reasons_and_permanent_dns_failures_are_never_retried(self):
        for reason in (PRIVATE_MARKER, None, RuntimeError(PRIVATE_MARKER),
                       OSError(errno.EACCES, PRIVATE_MARKER),
                       socket.gaierror(socket.EAI_NONAME, PRIVATE_MARKER)):
            with self.subTest(reason=type(reason).__name__):
                wire = mock.Mock(side_effect=urllib.error.URLError(reason))
                client = self.client(wire, network_retries=1)
                self.assert_error(client)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_direct_unwrapped_timeouts_are_not_assumed_retryable(self):
        wire = mock.Mock(side_effect=TimeoutError(PRIVATE_MARKER))
        client = self.client(wire, network_retries=1)
        self.assert_error(client)
        self.assertEqual(wire.call_count, 1)
        self.assertEqual(client.retry_events(), [])

    def test_api_rejections_and_rate_limits_are_never_retried(self):
        for code in ("50011", "50113", "50114", "50100", PRIVATE_MARKER):
            with self.subTest(code=code):
                wire = mock.Mock(return_value=(200, {"code": code, "msg": PRIVATE_MARKER, "data": []}))
                client = self.client(wire, network_retries=1)
                self.assert_error(client, "DEMO_API_ERROR")
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_transport_json_and_redirect_errors_are_never_retried(self):
        for code in ("DEMO_BAD_JSON", "DEMO_RESPONSE_TOO_LARGE", "DEMO_REDIRECT_FORBIDDEN"):
            with self.subTest(code=code):
                wire = mock.Mock(side_effect=T.DemoTransportError(code))
                client = self.client(wire, network_retries=1)
                self.assert_error(client, code)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_response_shape_errors_are_never_retried(self):
        for payload in (None, [], {"code": "0"}, {"code": "0", "data": None},
                        {"code": "0", "data": {}}, {"code": "0", "data": PRIVATE_MARKER}):
            with self.subTest(payload_type=type(payload).__name__):
                wire = mock.Mock(return_value=(200, payload))
                client = self.client(wire, network_retries=1)
                self.assert_error(client, "DEMO_BAD_RESPONSE")
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_retry_allowlist_matches_existing_evidence_read_endpoints(self):
        self.assertEqual(T.RETRY_GET_PATHS, SAFE_GET_PATHS)
        self.assertIsInstance(T.RETRY_GET_PATHS, frozenset)
        for path in T.RETRY_GET_PATHS:
            with self.subTest(path=path):
                wire = mock.Mock(side_effect=[urllib.error.URLError(TimeoutError()), OK])
                self.assertEqual(self.client(wire, network_retries=1).get(path), OK[1]["data"])
                self.assertEqual(wire.call_count, 2)

    def test_other_paths_and_path_query_variants_get_no_retry(self):
        for path in ("/api/v5/trade/order-algo", "/api/v5/private/unknown",
                     PATH + "?after=1", PATH + "#fragment", PATH + "/"):
            with self.subTest(path=path):
                wire = mock.Mock(side_effect=urllib.error.URLError(TimeoutError()))
                client = self.client(wire, network_retries=1)
                self.assert_error(client, path=path)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_base_and_authority_locks_remain_in_effect(self):
        wire = mock.Mock(return_value=OK)
        with self.assertRaises(T.DemoTransportError) as caught:
            self.client(wire, base="https://other.invalid", network_retries=1)
        self.assertEqual(caught.exception.code, "DEMO_BASE_LOCKED")
        for path in ("https://other.invalid/api/v5/account/config", "//other.invalid/api/v5/account/config",
                     "/api/v5/../account/config", "/api/v5/@other.invalid/config"):
            self.assert_error(self.client(wire, network_retries=1), "DEMO_PATH_NOT_ALLOWED", path=path)
        wire.assert_not_called()

    def test_post_still_refuses_without_any_network_attempt_or_retry(self):
        wire = mock.Mock(side_effect=AssertionError("NO_HTTP_CALL_ALLOWED"))
        client = self.client(wire, network_retries=1)
        for path in ("/api/v5/trade/order", "/api/v5/trade/cancel-order", PATH):
            with self.assertRaises(T.DemoWriteRefused) as caught:
                client.post(path, {"secret": PRIVATE_MARKER})
            self.assertEqual(caught.exception.code, "DEMO_WRITE_NOT_RELEASED")
        wire.assert_not_called()
        self.sleep_mock.assert_not_called()
        self.assertEqual(client.retry_events(), [])

    def test_retry_telemetry_excludes_credentials_query_and_exception_text(self):
        wire = mock.Mock(side_effect=[urllib.error.URLError(TimeoutError(PRIVATE_MARKER)), OK])
        client = self.client(wire, network_retries=1)
        client.get(PATH, {"identity": PRIVATE_MARKER})
        events = client.retry_events()
        encoded = json.dumps(events)
        self.assertNotIn(PRIVATE_MARKER, encoded)
        for value in CREDS.values():
            self.assertNotIn(value, encoded)
        self.assertEqual(set(events[0]), {"path", "code", "attempt", "type"})

    def test_retry_telemetry_returns_detached_copies(self):
        wire = mock.Mock(side_effect=[urllib.error.URLError(TimeoutError()), OK])
        client = self.client(wire, network_retries=1)
        client.get(PATH)
        events = client.retry_events()
        events[0]["path"] = PRIVATE_MARKER
        events.append({"secret": PRIVATE_MARKER})
        self.assertEqual(len(client.retry_events()), 1)
        self.assertEqual(client.retry_events()[0]["path"], PATH)

    def test_parallel_retries_are_bounded_and_thread_safe(self):
        calls, lock = {}, threading.Lock()
        def wire(url, headers, timeout):
            with lock:
                calls[url] = calls.get(url, 0) + 1
                attempt = calls[url]
            if attempt == 1:
                raise urllib.error.URLError(ConnectionResetError(PRIVATE_MARKER))
            return OK
        client = self.client(wire, network_retries=1)
        def query(number):
            result = client.get(PATH, {"after": str(number)})
            for event in client.retry_events():
                self.assertEqual(set(event), {"path", "code", "attempt", "type"})
            return result
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(query, range(80)))
        self.assertEqual(results, [OK[1]["data"]] * 80)
        self.assertEqual(list(calls.values()), [2] * 80)
        self.assertEqual(len(client.retry_events()), 64)
        self.assertNotIn(PRIVATE_MARKER, json.dumps(client.retry_events()))
        self.assertEqual(self.sleep_mock.call_count, 80)

    def test_retry_budget_accepts_only_none_or_callable(self):
        for invalid in (False, True, 0, 1, "callable", [], {}, object()):
            with self.subTest(value=type(invalid).__name__):
                wire = mock.Mock(return_value=OK)
                with self.assertRaises(T.DemoTransportError) as caught:
                    self.client(wire, retry_budget=invalid)
                self.assertEqual(caught.exception.code, "DEMO_NETWORK_RETRY_BUDGET_INVALID")
                wire.assert_not_called()
        self.assertEqual(self.client(mock.Mock(return_value=OK), retry_budget=None).get(PATH), OK[1]["data"])
        self.assertEqual(self.client(mock.Mock(return_value=OK), retry_budget=lambda path: None).get(PATH), OK[1]["data"])

    def test_enabled_retry_without_budget_is_refused_before_any_request(self):
        for extra in ({}, {"retry_budget": None}):
            with self.subTest(explicit_none=bool(extra)):
                wire = mock.Mock(return_value=OK)
                with self.assertRaises(T.DemoTransportError) as caught:
                    T.DemoReadOnlyClient(dict(CREDS), http_get=wire, network_retries=1, **extra)
                self.assertEqual(caught.exception.code, "DEMO_NETWORK_RETRY_POLICY_INVALID")
                wire.assert_not_called()

    def test_retry_budget_runs_after_backoff_before_fresh_headers_and_second_get(self):
        sequence = []
        def wire(url, headers, timeout):
            sequence.append("get")
            if sequence.count("get") == 1:
                raise urllib.error.URLError(ssl.SSLEOFError(PRIVATE_MARKER))
            self.assertEqual(len(client.retry_events()), 1)
            return OK
        def reserve(path):
            self.assertEqual(path, PATH)
            self.assertEqual(client.retry_events(), [])
            sequence.append("reserve")
        client = self.client(wire, network_retries=1, retry_budget=reserve)
        original_headers = client._headers
        def headers(*args):
            sequence.append("headers")
            return original_headers(*args)
        self.sleep_mock.side_effect = lambda delay: sequence.append("backoff")
        with mock.patch.object(client, "_headers", side_effect=headers):
            self.assertEqual(client.get(PATH, {"after": PRIVATE_MARKER}), OK[1]["data"])
        self.assertEqual(sequence, ["headers", "get", "backoff", "reserve", "headers", "get"])

    def test_budget_failure_stops_before_second_get_and_has_no_retry_event(self):
        for error in (RuntimeError(PRIVATE_MARKER), T.DemoTransportError(PRIVATE_MARKER),
                      urllib.error.URLError(TimeoutError(PRIVATE_MARKER))):
            with self.subTest(error=type(error).__name__):
                wire = mock.Mock(side_effect=[urllib.error.URLError(ssl.SSLEOFError()), OK])
                budget = mock.Mock(side_effect=error)
                client = self.client(wire, network_retries=1, retry_budget=budget)
                with mock.patch.object(client, "_headers", wraps=client._headers) as headers:
                    self.assert_error(client, "DEMO_NETWORK_RETRY_BUDGET_FAILED")
                budget.assert_called_once_with(PATH)
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(headers.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_budget_is_not_reserved_for_ineligible_requests_or_first_success(self):
        cases = ((OK, 1, PATH, None),
                 (urllib.error.URLError(TimeoutError()), 0, PATH, "DEMO_NETWORK_ERROR"),
                 (urllib.error.URLError(TimeoutError()), 1, "/api/v5/private/unknown", "DEMO_NETWORK_ERROR"),
                 (urllib.error.URLError(PRIVATE_MARKER), 1, PATH, "DEMO_NETWORK_ERROR"),
                 (urllib.error.HTTPError(T.DEMO_BASE + PATH, 429, "throttled", {}, None), 1, PATH, "DEMO_NETWORK_ERROR"),
                 (urllib.error.URLError(ssl.SSLCertVerificationError()), 1, PATH, "DEMO_NETWORK_ERROR"),
                 ((200, {"code": "50011", "data": []}), 1, PATH, "DEMO_API_ERROR"),
                 ((200, {"code": "0", "data": None}), 1, PATH, "DEMO_BAD_RESPONSE"))
        for outcome, retries, path, error in cases:
            with self.subTest(outcome_type=type(outcome).__name__, retries=retries, path=path):
                wire = mock.Mock(side_effect=outcome) if isinstance(outcome, Exception) else mock.Mock(return_value=outcome)
                budget = mock.Mock(side_effect=AssertionError("MUST_NOT_RESERVE"))
                client = self.client(wire, network_retries=retries, retry_budget=budget)
                if error is None:
                    self.assertEqual(client.get(path), OK[1]["data"])
                else:
                    self.assert_error(client, error, path=path)
                budget.assert_not_called()
                self.assertEqual(wire.call_count, 1)
                self.assertEqual(client.retry_events(), [])

    def test_budget_is_reserved_only_once_if_second_network_attempt_fails(self):
        wire = mock.Mock(side_effect=urllib.error.URLError(ssl.SSLEOFError(PRIVATE_MARKER)))
        budget = mock.Mock()
        client = self.client(wire, network_retries=1, retry_budget=budget)
        self.assert_error(client)
        self.assertEqual(wire.call_count, 2)
        budget.assert_called_once_with(PATH)
        self.assertEqual(len(client.retry_events()), 1)

    def test_post_never_calls_retry_budget_even_when_enabled(self):
        wire = mock.Mock(side_effect=AssertionError("NO_NETWORK"))
        budget = mock.Mock(side_effect=AssertionError("NO_BUDGET_FOR_POST"))
        client = self.client(wire, network_retries=1, retry_budget=budget)
        with self.assertRaises(T.DemoWriteRefused):
            client.post("/api/v5/trade/order", {"sensitive": PRIVATE_MARKER})
        wire.assert_not_called()
        budget.assert_not_called()
        self.assertEqual(client.retry_events(), [])

    def _parallel_real_provider_budget(self, with_budget, *, pooled=False):
        """Six logical reads / twelve physical attempts; no network or state I/O."""
        class QuotaClock:
            def __init__(self):
                self.value, self.waits = 0.0, []
                self.lock = threading.Lock()
            def __call__(self):
                with self.lock:
                    return self.value
            def sleep(self, seconds):
                with self.lock:
                    self.waits.append(seconds)
                    self.value += seconds
        quota = QuotaClock()
        reservations, dispatches, counts = [], [], {}
        wire_lock, first_wave = threading.Lock(), threading.Barrier(6)
        class TracedWindow(deque):
            def append(self, timestamp):
                # Called under the real provider's rate lock.
                reservations.append(timestamp)
                super().append(timestamp)
        def wire(url, headers, timeout):
            with wire_lock:
                counts[url] = counts.get(url, 0) + 1
                first = counts[url] == 1
                dispatches.append(quota())
            if first:
                first_wave.wait(timeout=5)
                if pooled:
                    raise ssl.SSLEOFError(PRIVATE_MARKER)
                raise urllib.error.URLError(ssl.SSLEOFError(PRIVATE_MARKER))
            return OK
        def retry_budget(path):
            provider._reserve_get(path)
        budget = retry_budget if with_budget else (lambda path: None)
        if pooled:
            client, _, manager, Response = self.pooled_client([], network_retries=1, retry_budget=budget)
            def request(method, url, **options):
                self.assertEqual(method, "GET")
                self.assertIs(options["retries"], False)
                _, payload = wire(url, options["headers"], options["pool_timeout"])
                return Response(payload)
            manager.request = request
        else:
            client = self.client(wire, network_retries=1, retry_budget=budget)
        # Construction only assigns fields; _get collects timings, no snapshot
        # or filesystem/account operation is performed by this fixture.
        provider = V.VerifiedDemoRiskProvider(client, {}, "unused-budget-test-state",
                    authorization={}, router_journal_path="unused-budget-test.sqlite3",
                    clock=lambda: 0, rate_clock=quota, rate_sleep=quota.sleep)
        provider._rate_windows[PATH] = TracedWindow()
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda number: provider._get(PATH, {"after": str(number)}), range(6)))
        self.assertEqual(results, [OK[1]["data"]] * 6)
        self.assertEqual(list(counts.values()), [2] * 6)
        self.assertEqual(len(client.retry_events()), 6)
        return reservations, dispatches, quota.waits

    def test_parallel_counterexample_unshared_callback_has_unreserved_physical_attempts(self):
        reservations, dispatches, waits = self._parallel_real_provider_budget(False)
        self.assertEqual(len(reservations), 6)
        self.assertEqual(len(dispatches), 12)
        self.assertEqual(waits, [])
        self.assertGreater(len(dispatches), V.GET_LIMITS_PER_TWO_SECONDS[PATH])
        self.assertEqual(set(dispatches), {0.0})

    def test_parallel_real_provider_hook_reserves_every_physical_attempt(self):
        reservations, dispatches, waits = self._parallel_real_provider_budget(True)
        self.assertEqual(len(reservations), 12)
        self.assertEqual(len(dispatches), 12)
        self.assertTrue(waits, "the extra physical requests must consume quota and wait")
        capacity = V.GET_LIMITS_PER_TWO_SECONDS[PATH]
        for times in (reservations, sorted(dispatches)):
            for index, now in enumerate(times):
                active = [start for start in times[:index + 1] if now - start < V.RATE_WINDOW_SECONDS]
                self.assertLessEqual(len(active), capacity)

    def test_parallel_pooled_eof_retries_share_real_provider_path_budget(self):
        reservations, dispatches, waits = self._parallel_real_provider_budget(True, pooled=True)
        self.assertEqual(len(reservations), 12)
        self.assertEqual(len(dispatches), 12)
        self.assertTrue(waits)
        capacity = V.GET_LIMITS_PER_TWO_SECONDS[PATH]
        for times in (reservations, sorted(dispatches)):
            for index, now in enumerate(times):
                self.assertLessEqual(sum(now - start < V.RATE_WINDOW_SECONDS for start in times[:index + 1]), capacity)


if __name__ == "__main__":
    unittest.main(verbosity=2)
