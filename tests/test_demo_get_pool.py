"""Pure offline checks: fake pools and real urllib3 over in-memory sockets."""
from __future__ import annotations

import io
import json
import logging
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

from src import demo_get_pool as P
from src import demo_transport as T

URL = T.DEMO_BASE + "/api/v5/account/config?private=QUERY_SENTINEL"
HEADERS = {T.DEMO_HEADER: "1", "OK-ACCESS-KEY": "FAKE_KEY_SENTINEL",
           "OK-ACCESS-SIGN": "FAKE_SIGN_SENTINEL", "Accept": "application/json"}
GOOD = b'{"code":"0","data":[]}'


class FakeResponse:
    def __init__(self, body=GOOD, *, status=200, headers=None, complete=True, error=None):
        self.body, self.status, self.headers = body, status, headers or {}
        self.closed, self.complete, self.error = False, complete, error
        self.events, self.reads = [], []

    def read(self, amount, **kwargs):
        self.reads.append((amount, kwargs))
        self.events.append("read")
        if self.error:
            raise self.error
        self.closed = self.complete
        return self.body[:amount] if isinstance(self.body, (bytes, bytearray)) else self.body

    def close(self):
        self.events.append("close")
        self.closed = True

    def release_conn(self):
        self.events.append("release")


class FakeManager:
    def __init__(self, response=None, error=None):
        self.response, self.error = response or FakeResponse(), error
        self.calls, self.clears = [], 0
        self.pools = {}

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error:
            raise self.error
        return self.response

    def clear(self):
        self.clears += 1


class Driver:
    def __init__(self, bodies=(GOOD,), *, status=200, headers=None, wire=None):
        self.responses = deque(wire if wire is not None else [
            b"HTTP/1.1 " + str(status).encode() + b" Test\r\nContent-Length: " + str(len(body)).encode()
            + b"\r\n" + b"".join(name.encode() + b": " + value.encode() + b"\r\n" for name, value in (headers or {}).items())
            + b"\r\n" + body for body in bodies])
        self.connections, self.requests, self.connects = [], [], 0
        self.lock = threading.Lock()
        self.block = None
        self.sixteen = threading.Event()
        self.error = None

    def connection_type(self):
        driver = self
        class MemorySocket:
            def __init__(self):
                self.closed, self.timeout = False, None

            def makefile(self, *_):
                with driver.lock:
                    return io.BytesIO(driver.responses.popleft())

            def sendall(self, _data):
                pass

            def settimeout(self, value):
                self.timeout = value

            def gettimeout(self):
                return self.timeout

            def close(self):
                self.closed = True

        class MemoryConnection(P.urllib3.connection.HTTPSConnection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.close_count = 0
                with driver.lock:
                    driver.connections.append(self)

            @property
            def is_connected(self):
                return self.sock is not None and not self.sock.closed

            def connect(self):
                driver.connects += 1
                if driver.error:
                    raise driver.error
                self.sock = MemorySocket()
                self.is_verified = self.proxy_is_verified = True

            def request(self, method, url, *args, **kwargs):
                with driver.lock:
                    driver.requests.append((method, url, kwargs))
                    if len(driver.requests) == 16:
                        driver.sixteen.set()
                if driver.block is not None and not driver.block.wait(3):
                    raise TimeoutError("fake request gate timeout")
                return super().request(method, url, *args, **kwargs)

            def close(self):
                if self.sock is not None:
                    self.close_count += 1
                return super().close()
        return MemoryConnection


class PoolTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        for target in ("socket.socket.connect", "socket.create_connection", "subprocess.Popen", "subprocess.run",
                       "os.system", "src.demo_transport.load_demo_credentials", "src.demo_transport._read_user_env",
                       "urllib.request.getproxies", "ssl.create_default_context",
                       "src._vendor.urllib3.connection.create_urllib3_context",
                       "src._vendor.urllib3.util.ssl_.create_urllib3_context"):
            self.stack.enter_context(patch(target, side_effect=AssertionError("EXTERNAL_IO_OR_UNSAFE_TLS_FORBIDDEN")))
        if sys.platform == "win32":
            self.stack.enter_context(patch("winreg.OpenKey", side_effect=AssertionError("NO_REGISTRY")))
            self.stack.enter_context(patch("winreg.QueryValueEx", side_effect=AssertionError("NO_REGISTRY")))

    def fake_pool(self, response=None, *, error=None, proxy_url=None):
        manager = FakeManager(response, error)
        factory = self.stack.enter_context(patch.object(P.urllib3, "ProxyManager" if proxy_url else "PoolManager", return_value=manager))
        pool = P.DemoGetPool(proxy_url=proxy_url)
        self.addCleanup(self.safe_close, pool)
        return pool, manager, factory

    def memory_pool(self, driver, *, proxy_url=None):
        self.stack.enter_context(patch.object(P.urllib3.connectionpool.HTTPSConnectionPool,
                                            "ConnectionCls", driver.connection_type()))
        pool = P.DemoGetPool(proxy_url=proxy_url)
        self.addCleanup(self.safe_close, pool)
        return pool

    @staticmethod
    def safe_close(pool):
        try:
            pool.close()
        except T.DemoTransportError:
            # Dedicated failure tests assert the fixed code and persistent
            # closed state; cleanup does not retry an indeterminate close.
            pass

    def assert_code(self, code, function):
        with self.assertRaises(T.DemoTransportError) as caught:
            function()
        self.assertEqual(caught.exception.code, code)
        self.assertTrue(caught.exception.__suppress_context__)
        for secret in ("QUERY_SENTINEL", "FAKE_KEY_SENTINEL", "RAW_SECRET", URL):
            self.assertNotIn(secret, str(caught.exception))
        return caught.exception

    def test_fixed_get_options_and_finite_waits_copy_headers(self):
        pool, manager, factory = self.fake_pool()
        original = dict(HEADERS)
        self.assertEqual(pool(URL, HEADERS, 2), (200, {"code": "0", "data": []}))
        options = factory.call_args.kwargs
        self.assertEqual((options["maxsize"], options["block"], options["retries"]), (16, True, False))
        args, kwargs = manager.calls[0]
        self.assertEqual(args, ("GET", URL))
        self.assertIs(kwargs["retries"], False)
        self.assertIs(kwargs["redirect"], False)
        self.assertIs(kwargs["preload_content"], False)
        self.assertIs(kwargs["decode_content"], False)
        self.assertIs(kwargs["release_conn"], False)
        self.assertIs(kwargs["enforce_content_length"], True)
        self.assertEqual(kwargs["pool_timeout"], 2)
        self.assertEqual((kwargs["timeout"].connect_timeout, kwargs["timeout"].read_timeout), (2, 2))
        self.assertEqual(kwargs["headers"]["Accept-Encoding"], "identity")
        self.assertEqual(HEADERS, original)
        self.assertEqual(manager.response.reads, [(T.MAX_BODY + 1, {"decode_content": False, "cache_content": False})])
        self.assertEqual(manager.response.events, ["read", "release"])

    def test_bad_or_bypassing_urls_are_rejected_before_request(self):
        pool, manager, _ = self.fake_pool()
        urls = (None, "", URL + "#", " " + URL, URL + "\r\nX:1", URL + "\\extra",
                "http://openapi.okx.com/api/v5/account/config", "https://openapi.okx.com:443/api/v5/account/config",
                "https://user@openapi.okx.com/api/v5/account/config", "https://openapi.okx.com.evil/api/v5/account/config",
                T.DEMO_BASE + "/api/v5/../trade/order", T.DEMO_BASE + "/api/v5/%2e%2e/account/config",
                T.DEMO_BASE + "/api/v5/account//config", T.DEMO_BASE + "/api/v5/account/config;other",
                T.DEMO_BASE + "/api/v5x/account/config", URL + "%GG", URL + "%0d", URL + "%00")
        for url in urls:
            with self.subTest(url_type=type(url).__name__):
                with self.assertRaises(T.DemoTransportError):
                    pool.get(url, HEADERS, 1)
        self.assertEqual(manager.calls, [])

    def test_abnormal_headers_and_demo_override_are_rejected(self):
        pool, manager, _ = self.fake_pool()
        bad = (None, [], {}, {T.DEMO_HEADER: "0"}, {**HEADERS, "X-Simulated-Trading": "1"},
               {**HEADERS, "Host": "elsewhere"}, {**HEADERS, "Proxy-Authorization": "RAW_SECRET"},
               {**HEADERS, "Content-Length": "0"}, {**HEADERS, "Accept-Encoding": "gzip"},
               {**HEADERS, "OK-ACCESS-KEY": "RAW_SECRET\r\nHost:evil"},
               {**HEADERS, "Accept": ""}, {**HEADERS, "Accept": b"application/json"})
        for headers in bad:
            with self.subTest(headers_type=type(headers).__name__):
                self.assert_code("DEMO_POOL_HEADERS_INVALID", lambda: pool.get(URL, headers, 1))
        self.assertEqual(manager.calls, [])

    def test_bad_timeout_never_reaches_pool(self):
        pool, manager, _ = self.fake_pool()
        for timeout in (None, True, 0, -1, 9, 10 ** 1000, float("nan"), float("inf"), "2"):
            self.assert_code("DEMO_POOL_TIMEOUT_INVALID", lambda: pool.get(URL, HEADERS, timeout))
        self.assertEqual(manager.calls, [])

    def test_redirect_and_non_200_never_read_or_follow(self):
        for status in (301, 302, 303, 307, 308, 429, 500):
            with self.subTest(status=status):
                response = FakeResponse(status=status, headers={"Location": "https://evil/RAW_SECRET", "Retry-After": "99"})
                pool, manager, _ = self.fake_pool(response)
                code = "DEMO_REDIRECT_FORBIDDEN" if status < 400 else "DEMO_HTTP_ERROR"
                self.assert_code(code, lambda: pool.get(URL, HEADERS, 1))
                self.assertEqual(len(manager.calls), 1)
                self.assertEqual(response.events, ["close", "release"])

    def test_too_large_incomplete_encoded_or_read_failed_closes_before_release(self):
        for response, code in (
                (FakeResponse(b"x" * (T.MAX_BODY + 100), complete=False), "DEMO_RESPONSE_TOO_LARGE"),
                (FakeResponse(complete=False), "DEMO_BAD_RESPONSE"),
                (FakeResponse(body="not bytes"), "DEMO_BAD_RESPONSE"),
                (FakeResponse(headers={"Content-Encoding": "gzip"}), "DEMO_BAD_RESPONSE"),
                (FakeResponse(error=TimeoutError("RAW_SECRET")), "DEMO_NETWORK_ERROR")):
            pool, _, _ = self.fake_pool(response)
            self.assert_code(code, lambda: pool.get(URL, HEADERS, 1))
            self.assertEqual(response.events[-2:], ["close", "release"])

    def test_complete_bad_json_is_rejected_without_closing_a_reusable_connection(self):
        for body, code in ((b"RAW_SECRET", "DEMO_BAD_JSON"), (b"\xff", "DEMO_BAD_JSON"), (b"[]", "DEMO_BAD_RESPONSE")):
            response = FakeResponse(body)
            pool, _, _ = self.fake_pool(response)
            self.assert_code(code, lambda: pool.get(URL, HEADERS, 1))
            self.assertEqual(response.events, ["read", "release"])

    def test_network_failure_is_single_attempt_and_does_not_use_client_retry_budget(self):
        for error, category in ((TimeoutError("RAW_SECRET"), "TIMEOUT"),
                                (ssl.SSLCertVerificationError("RAW_SECRET"), "TLS_CERTIFICATE")):
            pool, manager, _ = self.fake_pool(error=error)
            budget = Mock(side_effect=AssertionError("NO_RETRY_BUDGET"))
            client = T.DemoReadOnlyClient(dict(zip(T.CREDENTIAL_NAMES, ("fixture-key", "fixture-secret", "fixture-pass"))),
                                        http_get=pool, network_retries=1, retry_budget=budget)
            refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: client.get("/api/v5/account/config"))
            self.assertEqual(refused.network_category, category)
            self.assertIsNone(refused.numeric)
            self.assertEqual(str(refused), "DEMO_NETWORK_ERROR")
            self.assertEqual(len(manager.calls), 1)
            budget.assert_not_called()

    def test_network_leaf_categories_use_exact_known_classes_without_payloads(self):
        cases = (
            (socket.gaierror(1, "RAW_SECRET QUERY_SENTINEL"), "DNS"),
            (ssl.SSLCertVerificationError("RAW_SECRET"), "TLS_CERTIFICATE"),
            (ssl.SSLEOFError("RAW_SECRET"), "TLS_EOF"),
            (ssl.SSLZeroReturnError("RAW_SECRET"), "TLS"),
            (ssl.SSLError("RAW_SECRET"), "TLS"),
            (P.urllib3.exceptions.NameResolutionError("RAW_SECRET", None, socket.gaierror(1, "RAW_SECRET")), "DNS"),
            (P.urllib3.exceptions.ConnectTimeoutError("RAW_SECRET"), "CONNECT_TIMEOUT"),
            (P.urllib3.exceptions.ReadTimeoutError(None, URL, "RAW_SECRET"), "READ_TIMEOUT"),
            (P.urllib3.exceptions.EmptyPoolError(None, "RAW_SECRET"), "POOL_WAIT_TIMEOUT"),
            (P.urllib3.exceptions.NewConnectionError(None, "RAW_SECRET"), "CONNECTION"),
            (ConnectionError("RAW_SECRET"), "CONNECTION"),
            (ConnectionRefusedError("RAW_SECRET"), "CONNECTION"),
            (ConnectionResetError("RAW_SECRET"), "CONNECTION"),
            (ConnectionAbortedError("RAW_SECRET"), "CONNECTION"),
            (BrokenPipeError("RAW_SECRET"), "CONNECTION"),
            (EOFError("RAW_SECRET"), "CONNECTION"),
            (TimeoutError("RAW_SECRET"), "TIMEOUT"),
            (P.http.client.RemoteDisconnected("RAW_SECRET"), "PROTOCOL"),
            (P.http.client.IncompleteRead(b"RAW_SECRET", 100), "PROTOCOL"),
            (P.http.client.BadStatusLine("RAW_SECRET"), "PROTOCOL"),
            (P.urllib3.exceptions.IncompleteRead(1, 100), "PROTOCOL"),
            (RuntimeError("TLS READ_TIMEOUT RAW_SECRET"), "UNKNOWN"),
            (OSError(1, "DNS RAW_SECRET"), "UNKNOWN"),
        )
        for error, category in cases:
            with self.subTest(category=category, error_class=type(error).__name__):
                pool, manager, _ = self.fake_pool(error=error)
                refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
                self.assertEqual(refused.network_category, category)
                self.assertIn(refused.network_category, T.NETWORK_FAILURE_CATEGORIES)
                self.assertEqual(vars(refused), dict(code="DEMO_NETWORK_ERROR", numeric=None,
                                                     network_category=category))
                self.assertEqual(len(manager.calls), 1)
                self.assertEqual(pool._active, 0)

    def test_known_network_wrappers_preserve_specific_cause_categories(self):
        tls = ssl.SSLEOFError("RAW_SECRET")
        dns = P.urllib3.exceptions.NameResolutionError("RAW_SECRET", None, socket.gaierror(1, "RAW_SECRET"))
        wrapped_tls = P.urllib3.exceptions.ProxyError("RAW_SECRET", tls)
        wrapped_tls.__cause__ = tls
        retry = P.urllib3.exceptions.MaxRetryError(None, URL, dns)
        retry.__cause__ = dns
        cases = ((wrapped_tls, "TLS_EOF"), (retry, "DNS"),
                 (P.urllib3.exceptions.SSLError(ssl.SSLCertVerificationError("RAW_SECRET")), "TLS_CERTIFICATE"),
                 (P.urllib3.exceptions.ProtocolError("RAW_SECRET", ConnectionResetError("RAW_SECRET")), "CONNECTION"),
                 (P.urllib3.exceptions.ProxyError("RAW_SECRET", RuntimeError("RAW_SECRET")), "PROXY"),
                 (P.urllib3.exceptions.SSLError("RAW_SECRET"), "TLS"),
                 (P.urllib3.exceptions.ProtocolError("RAW_SECRET"), "PROTOCOL"))
        for error, category in cases:
            with self.subTest(category=category):
                pool, manager, _ = self.fake_pool(error=error)
                refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
                self.assertEqual(refused.network_category, category)
                self.assertEqual(len(manager.calls), 1)

    def test_network_classifier_does_not_access_unknown_exception_or_metaclass_hooks(self):
        hooks = []
        class UnknownMeta(type):
            def __hash__(cls):
                hooks.append("hash"); raise AssertionError("RAW_SECRET")
            def __eq__(cls, other):
                hooks.append("equality"); raise AssertionError("RAW_SECRET")
        class UnknownError(Exception, metaclass=UnknownMeta):
            def __str__(self):
                hooks.append("str"); raise AssertionError("RAW_SECRET")
            def __repr__(self):
                hooks.append("repr"); raise AssertionError("RAW_SECRET")
            def __getattribute__(self, name):
                if name in {"reason", "original_error", "args", "__cause__", "__context__", "__class__"}:
                    hooks.append(name); raise AssertionError("RAW_SECRET")
                return super().__getattribute__(name)
        error = UnknownError("RAW_SECRET")
        error.__cause__ = ssl.SSLEOFError("RAW_SECRET")
        pool, manager, _ = self.fake_pool(error=error)
        refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
        self.assertEqual(refused.network_category, "UNKNOWN")
        self.assertEqual(hooks, [])
        self.assertEqual(len(manager.calls), 1)

    def test_network_classifier_does_not_accept_named_or_subclass_spoofs(self):
        class ReadTimeoutError(P.urllib3.exceptions.ReadTimeoutError):
            pass
        ReadTimeoutError.__module__ = P.urllib3.exceptions.__name__
        self.assertEqual(P._network_category(ReadTimeoutError(None, URL, "RAW_SECRET")), "UNKNOWN")
        self.assertEqual(P._network_category(type("SSLError", (Exception,), {})("RAW_SECRET")), "UNKNOWN")

    def test_network_cause_cycles_conflicts_and_overlong_chains_are_unknown(self):
        direct_cycle = P.urllib3.exceptions.ProxyError("RAW_SECRET", None)
        direct_cycle.original_error = direct_cycle
        cause_cycle = P.urllib3.exceptions.SSLError("RAW_SECRET")
        cause_cycle.__cause__ = cause_cycle
        first, second = (P.urllib3.exceptions.ProxyError("RAW_SECRET", None) for _ in range(2))
        first.original_error, second.original_error = second, first
        conflict = P.urllib3.exceptions.ProxyError("RAW_SECRET", ssl.SSLEOFError("RAW_SECRET"))
        conflict.__cause__ = socket.gaierror(1, "RAW_SECRET")
        deep = ssl.SSLEOFError("RAW_SECRET")
        for _ in range(P._NETWORK_CAUSE_LIMIT):
            deep = P.urllib3.exceptions.ProxyError("RAW_SECRET", deep)
        for error in (direct_cycle, cause_cycle, first, conflict, deep):
            with self.subTest(error_class=type(error).__name__):
                self.assertEqual(P._network_category(error), "UNKNOWN")
        within_bound = ssl.SSLEOFError("RAW_SECRET")
        for _ in range(P._NETWORK_CAUSE_LIMIT - 1):
            within_bound = P.urllib3.exceptions.ProxyError("RAW_SECRET", within_bound)
        self.assertEqual(P._network_category(within_bound), "TLS_EOF")

    def test_malformed_wrapper_cannot_replace_original_network_refusal(self):
        error = P.urllib3.exceptions.ProxyError.__new__(P.urllib3.exceptions.ProxyError)
        Exception.__init__(error, "RAW_SECRET")
        pool, manager, _ = self.fake_pool(error=error)
        refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
        self.assertEqual(refused.network_category, "UNKNOWN")
        self.assertEqual(len(manager.calls), 1)

    def test_read_failure_category_keeps_cleanup_and_original_release_failure_precedence(self):
        response = FakeResponse(error=P.urllib3.exceptions.ReadTimeoutError(None, URL, "RAW_SECRET"))
        pool, manager, _ = self.fake_pool(response)
        refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
        self.assertEqual(refused.network_category, "READ_TIMEOUT")
        self.assertEqual(response.events, ["read", "close", "release"])
        self.assertEqual(len(manager.calls), 1)
        response = FakeResponse(error=ssl.SSLEOFError("RAW_SECRET"))
        response.release_conn = Mock(side_effect=RuntimeError("RAW_SECRET"))
        pool, _, _ = self.fake_pool(response)
        refused = self.assert_code("DEMO_POOL_RELEASE_FAILED", lambda: pool.get(URL, HEADERS, 1))
        self.assertFalse(hasattr(refused, "network_category"))
        self.assertEqual(response.events, ["read", "close"])
        response.release_conn.assert_called_once_with()

    def test_nonnetwork_refusals_keep_status_numeric_and_no_network_category(self):
        for response, code, numeric in ((FakeResponse(status=429), "DEMO_HTTP_ERROR", 429),
                                        (FakeResponse(body=b"RAW_SECRET"), "DEMO_BAD_JSON", None),
                                        (FakeResponse(status=302), "DEMO_REDIRECT_FORBIDDEN", None)):
            with self.subTest(code=code):
                pool, manager, _ = self.fake_pool(response)
                refused = self.assert_code(code, lambda: pool.get(URL, HEADERS, 1))
                self.assertEqual(refused.numeric, numeric)
                self.assertFalse(hasattr(refused, "network_category"))
                self.assertEqual(len(manager.calls), 1)

    def test_real_urllib3_tls_wrapper_retains_specific_diagnostic_without_retries(self):
        driver = Driver()
        driver.error = ssl.SSLCertVerificationError("RAW_SECRET")
        pool = self.memory_pool(driver)
        refused = self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
        self.assertEqual(refused.network_category, "TLS_CERTIFICATE")
        self.assertEqual(driver.connects, 1)

    def test_manual_tls_contexts_have_no_keylog_and_verify_target_and_proxy(self):
        destination = Path(self.temp.name) / "keys.log"
        with patch.dict(os.environ, {"SSLKEYLOGFILE": str(destination)}):
            pool, _, factory = self.fake_pool(proxy_url="https://proxy.example:8443")
        options = factory.call_args.kwargs
        target, proxy = options["ssl_context"], options["proxy_ssl_context"]
        self.assertIsNot(target, proxy)
        for context in (target, proxy):
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertIs(context.check_hostname, True)
            self.assertIs(context.hostname_checks_common_name, False)
            self.assertIsNone(context.keylog_filename)
        self.assertIsNone(options.get("assert_hostname"))
        self.assertIsNone(options.get("proxy_assert_hostname"))
        self.assertIs(options["use_forwarding_for_https"], False)
        self.assertFalse(destination.exists())

    def test_never_reads_environment_or_discovers_default_proxy(self):
        class NoEnvironment(dict):
            def __getitem__(self, *_):
                raise AssertionError("NO_ENVIRONMENT")
            get = __contains__ = __getitem__
        with patch.object(os, "environ", NoEnvironment()), \
                patch.object(P.urllib3, "ProxyManager", side_effect=AssertionError("NO_AUTOMATIC_PROXY")):
            pool, _, _ = self.fake_pool()
            self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)

    def test_real_target_and_https_proxy_wrappers_retain_explicit_secure_contexts(self):
        pool = P.DemoGetPool(proxy_url="https://proxy.example:8443")
        self.addCleanup(pool.close)
        target_pool = pool._manager.connection_from_url(URL)
        connection = target_pool._new_conn()
        calls = []
        class TlsSocket:
            def getpeercert(self, binary_form=False):
                return b"fixture-certificate" if binary_form else {}
            def close(self):
                pass
        def wrap(**kwargs):
            calls.append(kwargs)
            self.assertIsInstance(kwargs["ssl_context"], ssl.SSLContext)
            self.assertEqual(kwargs["ssl_context"].verify_mode, ssl.CERT_REQUIRED)
            self.assertIs(kwargs["ssl_context"].check_hostname, True)
            self.assertIs(kwargs["ssl_context"].hostname_checks_common_name, False)
            self.assertIsNone(kwargs["ssl_context"].keylog_filename)
            return TlsSocket()
        with patch.object(P.urllib3.connection, "ssl_wrap_socket", side_effect=wrap):
            proxy_socket = connection._connect_tls_proxy("proxy.example", object())
            target = P.urllib3.connection._ssl_wrap_socket_and_match_hostname(proxy_socket,
                cert_reqs=ssl.CERT_REQUIRED, ssl_version=None, ssl_minimum_version=None,
                ssl_maximum_version=None, cert_file=None, key_file=None, key_password=None,
                ca_certs=None, ca_cert_dir=None, ca_cert_data=None, assert_hostname=None,
                assert_fingerprint=None, server_hostname=T.DEMO_HOST, ssl_context=connection.ssl_context)
        self.assertIs(target.is_verified, True)
        self.assertIs(connection.proxy_is_verified, True)
        self.assertEqual([call["server_hostname"] for call in calls], ["proxy.example", T.DEMO_HOST])
        self.assertIsNot(calls[0]["ssl_context"], calls[1]["ssl_context"])
        self.assertIsNone(connection.assert_hostname)
        self.assertIsNone(connection.proxy_config.assert_hostname)

    def test_real_proxy_manager_configures_connect_tunnel_for_fixed_https_target(self):
        for proxy in ("http://127.0.0.1:8080", "https://proxy.example:8443"):
            with self.subTest(proxy_scheme=proxy.split(":")[0]):
                driver = Driver()
                pool = self.memory_pool(driver, proxy_url=proxy)
                self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
                connection = driver.connections[0]
                self.assertEqual(connection._tunnel_host, T.DEMO_HOST)
                self.assertEqual(connection._tunnel_port, 443)
                self.assertIs(connection.proxy_config.use_forwarding_for_https, False)
                self.assertIsNotNone(connection.proxy_config.ssl_context)

    def test_proxy_only_accepts_explicit_http_https_connect_without_credentials(self):
        for proxy in ("socks5://127.0.0.1:1080", "http://user:pass@proxy.example:8080", "http://proxy/path",
                      "http://proxy/?x=1", "https://proxy#", "http://proxy:0", "http://proxy:99999",
                      "http://bad..proxy:80", "http://proxy:", "http://proxy\\evil:80", 1):
            self.assert_code("DEMO_POOL_PROXY_INVALID", lambda: P.DemoGetPool(proxy_url=proxy))
        for proxy in ("http://127.0.0.1:8080", "https://proxy.example:8443", "http://[::1]:8080"):
            pool, _, factory = self.fake_pool(proxy_url=proxy)
            self.assertEqual(factory.call_args.args, (proxy,))
            self.assertIs(factory.call_args.kwargs["use_forwarding_for_https"], False)

    def test_context_close_is_idempotent_and_closed_pool_rejects_new_work(self):
        pool, manager, _ = self.fake_pool()
        with pool as same:
            self.assertIs(same, pool)
        pool.close()
        self.assertEqual(manager.clears, 1)
        self.assert_code("DEMO_POOL_CLOSED", lambda: pool.get(URL, HEADERS, 1))
        self.assert_code("DEMO_POOL_CLOSED", pool.__enter__)

    def test_concurrent_close_waits_for_inflight_work_then_clears_once(self):
        pool, manager, _ = self.fake_pool()
        entered, finish = threading.Event(), threading.Event()
        def request(*args, **kwargs):
            entered.set()
            if not finish.wait(2):
                raise TimeoutError("fake request did not finish")
            return manager.response
        manager.request = request
        with ThreadPoolExecutor(max_workers=2) as executor:
            active = executor.submit(pool.get, URL, HEADERS, 1)
            self.assertTrue(entered.wait(2))
            closing = executor.submit(pool.close)
            with pool._condition:
                self.assertTrue(pool._condition.wait_for(lambda: pool._closed, timeout=2))
            self.assertEqual(manager.clears, 0)
            self.assert_code("DEMO_POOL_CLOSED", lambda: pool.get(URL, HEADERS, 1))
            finish.set()
            self.assertEqual(active.result(2)[0], 200)
            closing.result(2)
        self.assertEqual(manager.clears, 1)

    def test_response_cleanup_errors_are_sanitized_and_release_still_attempted(self):
        response = FakeResponse(status=302)
        response.close = Mock(side_effect=RuntimeError("RAW_SECRET"))
        response.release_conn = Mock()
        pool, _, _ = self.fake_pool(response)
        self.assert_code("DEMO_POOL_RELEASE_FAILED", lambda: pool.get(URL, HEADERS, 1))
        response.release_conn.assert_called_once_with()

    def test_pool_init_and_close_errors_are_fixed_codes(self):
        with patch.object(P.urllib3, "PoolManager", side_effect=RuntimeError("RAW_SECRET")):
            self.assert_code("DEMO_POOL_INIT_FAILED", P.DemoGetPool)
        pool, manager, _ = self.fake_pool()
        manager.clear = Mock(side_effect=RuntimeError("RAW_SECRET"))
        self.assert_code("DEMO_POOL_CLOSE_FAILED", pool.close)
        self.assert_code("DEMO_POOL_CLOSE_FAILED", pool.close)
        manager.clear.assert_called_once_with()

    def test_close_explicitly_disposes_retained_pool_after_bad_json_exception(self):
        driver = Driver((b"invalid JSON",))
        pool = self.memory_pool(driver)
        retained_pool = pool._manager.connection_from_url(URL)
        retained_error = None
        try:
            pool.get(URL, HEADERS, 1)
        except T.DemoTransportError as error:
            # unittest.assertRaises deliberately clears the caught traceback;
            # keep the real exception here to reproduce an external holder.
            retained_error = error
        self.assertIsNotNone(retained_error)
        self.assertEqual(retained_error.code, "DEMO_BAD_JSON")
        self.assertIsNotNone(retained_error.__traceback__)
        self.assertEqual(driver.connections[0].close_count, 0)
        pool.close()
        self.assertIsNone(retained_pool.pool)
        self.assertGreaterEqual(driver.connections[0].close_count, 1)
        self.assertIsNone(driver.connections[0].sock)
        self.assertEqual(len(pool._manager.pools), 0)
        self.assertIsNotNone(retained_error.__traceback__)

    def test_owned_pool_close_failure_still_clears_manager_and_notifies_all_callers(self):
        pool, manager, _ = self.fake_pool()
        entered, proceed = threading.Event(), threading.Event()
        def failed_close():
            entered.set()
            if not proceed.wait(2):
                raise TimeoutError("test close gate timeout")
            raise RuntimeError("RAW_SECRET")
        owned = Mock()
        owned.close.side_effect = failed_close
        manager.pools["owned-target"] = owned
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(pool.close)
            self.assertTrue(entered.wait(2))
            second = executor.submit(pool.close)
            proceed.set()
            self.assert_code("DEMO_POOL_CLOSE_FAILED", lambda: first.result(2))
            self.assert_code("DEMO_POOL_CLOSE_FAILED", lambda: second.result(2))
        self.assertEqual(manager.clears, 1)
        owned.close.assert_called_once_with()
        self.assert_code("DEMO_POOL_CLOSED", lambda: pool.get(URL, HEADERS, 1))

    def test_snapshot_failure_also_clears_manager_and_returns_fixed_close_error(self):
        pool, manager, _ = self.fake_pool()
        manager.pools = Mock()
        manager.pools.keys.side_effect = RuntimeError("RAW_SECRET")
        self.assert_code("DEMO_POOL_CLOSE_FAILED", pool.close)
        self.assertEqual(manager.clears, 1)

    def test_vendored_logger_parent_blocks_root_debug_query_and_identity_leaks(self):
        output = io.StringIO()
        root, old_level = logging.getLogger(), logging.getLogger().level
        handler = logging.StreamHandler(output)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.removeHandler, handler)
        self.addCleanup(root.setLevel, old_level)
        driver = Driver()
        pool = self.memory_pool(driver)
        self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
        logging.getLogger(P.urllib3.__name__ + ".connectionpool").debug("RAW_SECRET QUERY_SENTINEL FAKE_KEY_SENTINEL")
        self.assertEqual(output.getvalue(), "")

    def test_real_urllib3_reuses_one_memory_connection_and_consumes_framing(self):
        driver = Driver((GOOD, GOOD))
        pool = self.memory_pool(driver)
        for _ in range(2):
            self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
        self.assertEqual((driver.connects, len(driver.connections), len(driver.requests)), (1, 1, 2))
        self.assertEqual([request[0] for request in driver.requests], ["GET", "GET"])
        self.assertEqual(driver.connections[0].close_count, 0)

    def test_real_urllib3_consumes_chunked_framing_without_content_decoding(self):
        chunked = (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                   + format(len(GOOD), "x").encode() + b"\r\n" + GOOD + b"\r\n0\r\n\r\n")
        driver = Driver(wire=[chunked, chunked])
        pool = self.memory_pool(driver)
        self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
        self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
        self.assertEqual((driver.connects, len(driver.requests)), (1, 2))

    def test_real_urllib3_bad_json_does_not_close_a_connection_reused_by_another_thread(self):
        driver = Driver((b"invalid JSON", GOOD))
        pool = self.memory_pool(driver)
        consumed, proceed = threading.Event(), threading.Event()
        real_loads = json.loads
        def parse(body, *args, **kwargs):
            if body == "invalid JSON":
                consumed.set()
                if not proceed.wait(2):
                    raise AssertionError("second thread did not finish")
            return real_loads(body, *args, **kwargs)
        with patch.object(P.json, "loads", side_effect=parse), ThreadPoolExecutor(max_workers=2) as executor:
            bad = executor.submit(pool.get, URL, HEADERS, 1)
            self.assertTrue(consumed.wait(2))
            good = executor.submit(pool.get, URL, HEADERS, 1)
            self.assertEqual(good.result(2)[0], 200)
            proceed.set()
            self.assert_code("DEMO_BAD_JSON", lambda: bad.result(2))
        self.assertEqual(len(driver.connections), 1)
        self.assertEqual(driver.connections[0].close_count, 0)

    def test_exact_max_plus_one_at_content_length_eof_rejects_without_closing_reused_connection(self):
        driver = Driver((b"x" * (T.MAX_BODY + 1), GOOD))
        pool = self.memory_pool(driver)
        consumed, proceed = threading.Event(), threading.Event()
        original = P._refuse
        def refuse(code):
            if code == "DEMO_RESPONSE_TOO_LARGE":
                consumed.set()
                if not proceed.wait(2):
                    raise AssertionError("other thread did not reuse EOF connection")
            return original(code)
        with patch.object(P, "_refuse", side_effect=refuse), ThreadPoolExecutor(max_workers=2) as executor:
            oversized = executor.submit(pool.get, URL, HEADERS, 1)
            self.assertTrue(consumed.wait(2))
            good = executor.submit(pool.get, URL, HEADERS, 1)
            self.assertEqual(good.result(2)[0], 200)
            proceed.set()
            self.assert_code("DEMO_RESPONSE_TOO_LARGE", lambda: oversized.result(2))
        self.assertEqual((driver.connects, len(driver.connections)), (1, 1))
        self.assertEqual(driver.connections[0].close_count, 0)

    def test_exact_max_plus_one_chunked_data_without_consumed_trailer_discards_connection(self):
        oversized = b"x" * (T.MAX_BODY + 1)
        chunked = (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                   + format(len(oversized), "x").encode() + b"\r\n" + oversized + b"\r\n0\r\n\r\n")
        good = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(GOOD)).encode() + b"\r\n\r\n" + GOOD
        driver = Driver(wire=[chunked, good])
        pool = self.memory_pool(driver)
        self.assert_code("DEMO_RESPONSE_TOO_LARGE", lambda: pool.get(URL, HEADERS, 1))
        self.assertGreaterEqual(driver.connections[0].close_count, 1)
        self.assertEqual(pool.get(URL, HEADERS, 1)[0], 200)
        self.assertEqual(driver.connects, 2)

    def test_real_urllib3_discards_oversized_and_incomplete_connections(self):
        for wire, code in ((b"HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n" + b"x" * (T.MAX_BODY + 20),
                            "DEMO_RESPONSE_TOO_LARGE"),
                           (b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n{}", "DEMO_BAD_RESPONSE")):
            driver = Driver(wire=[wire])
            pool = self.memory_pool(driver)
            self.assert_code(code, lambda: pool.get(URL, HEADERS, 1))
            self.assertEqual(len(driver.requests), 1)
            self.assertGreaterEqual(driver.connections[0].close_count, 1)

    def test_real_urllib3_never_retries_redirect_http_failure_or_tls_failure(self):
        for status in (302, 429, 503):
            driver = Driver(status=status, headers={"Location": "https://evil/RAW_SECRET", "Retry-After": "99"})
            pool = self.memory_pool(driver)
            self.assert_code("DEMO_REDIRECT_FORBIDDEN" if status == 302 else "DEMO_HTTP_ERROR",
                             lambda: pool.get(URL, HEADERS, 1))
            self.assertEqual(len(driver.requests), 1)
        driver = Driver()
        driver.error = ssl.SSLCertVerificationError("RAW_SECRET")
        pool = self.memory_pool(driver)
        self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 1))
        self.assertEqual(driver.connects, 1)

    def test_real_urllib3_pool_limit_and_pool_wait_are_bounded(self):
        driver = Driver((GOOD,) * 16)
        driver.block = threading.Event()
        pool = self.memory_pool(driver)
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(pool.get, URL, HEADERS, 1) for _ in range(16)]
            try:
                self.assertTrue(driver.sixteen.wait(2))
                self.assert_code("DEMO_NETWORK_ERROR", lambda: pool.get(URL, HEADERS, 0.02))
                self.assertEqual(len(driver.connections), 16)
                self.assertEqual(len(driver.requests), 16)
            finally:
                driver.block.set()
            for future in futures:
                self.assertEqual(future.result(2)[0], 200)
        retained_pool = pool._manager.connection_from_url(URL)
        pool.close()
        self.assertIsNone(retained_pool.pool)
        self.assertTrue(all(connection.close_count >= 1 and connection.sock is None for connection in driver.connections))


if __name__ == "__main__":
    unittest.main()
