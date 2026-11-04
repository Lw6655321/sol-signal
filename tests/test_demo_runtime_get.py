"""Offline static-proxy selection and scoped GET transport wiring."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src import demo_runtime_get as R
from src.demo_transport import (CREDENTIAL_NAMES, DEMO_HEADER, DemoReadOnlyClient,
                                DemoTransportError, DemoWriteRefused, MAX_TIMEOUT)


class StrictEnvironment:
    """Any lookup outside the approved two fields fails, including enumeration."""
    def __init__(self, values):
        self.values, self.reads = values, []

    def get(self, name):
        if name not in {"https_proxy", "HTTPS_PROXY"}:
            raise AssertionError("ENVIRONMENT_SCOPE_EXCEEDED")
        self.reads.append(name)
        return self.values.get(name)


class RuntimeGetTests(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.getaddrinfo", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        patch("src.demo_route_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()

    def test_only_two_environment_names_are_read(self):
        environment = StrictEnvironment({"https_proxy": "http://fixture.invalid:8080"})
        with patch.object(R, "os", SimpleNamespace(environ=environment)), \
                patch.object(R, "windows_static_proxy_url") as registry:
            self.assertEqual(R.selected_proxy_url(), "http://fixture.invalid:8080")
        self.assertEqual(environment.reads, ["https_proxy", "HTTPS_PROXY"])
        registry.assert_not_called()

    def test_uppercase_and_equal_environment_fields_have_same_exact_value(self):
        value = "https://fixture.invalid:8443"
        for fields in ({"HTTPS_PROXY": value}, {"https_proxy": value, "HTTPS_PROXY": value}):
            with self.subTest(fields=tuple(fields)), \
                    patch.object(R, "os", SimpleNamespace(environ=StrictEnvironment(fields))), \
                    patch.object(R, "windows_static_proxy_url") as registry:
                self.assertEqual(R.selected_proxy_url(), value)
                registry.assert_not_called()

    def test_conflicting_fields_refuse_without_address_or_registry_read(self):
        environment = StrictEnvironment({"https_proxy": "PRIVATE_ONE", "HTTPS_PROXY": "PRIVATE_TWO"})
        with patch.object(R, "os", SimpleNamespace(environ=environment)), \
                patch.object(R, "windows_static_proxy_url") as registry, \
                self.assertRaises(R.DemoRuntimeGetRefused) as caught:
            R.selected_proxy_url()
        self.assertEqual(str(caught.exception), "DEMO_RUNTIME_PROXY_ENV_AMBIGUOUS")
        registry.assert_not_called()

    def test_absent_environment_uses_static_windows_adapter(self):
        with patch.object(R, "os", SimpleNamespace(environ=StrictEnvironment({}))), \
                patch.object(R, "windows_static_proxy_url", return_value=None) as registry:
            self.assertIsNone(R.selected_proxy_url())
        registry.assert_called_once_with()

    def test_explicit_empty_environment_refuses_pool_without_registry_fallback(self):
        with patch.object(R, "os", SimpleNamespace(environ=StrictEnvironment({"HTTPS_PROXY": ""}))), \
                patch.object(R, "windows_static_proxy_url") as registry, \
                patch("src.demo_get_pool._tls_context", side_effect=AssertionError("NO_POOL_TLS")), \
                self.assertRaises(DemoTransportError) as caught:
            with R.runtime_demo_get():
                self.fail("EMPTY_PROXY_ACCEPTED")
        self.assertEqual(caught.exception.code, "DEMO_POOL_PROXY_INVALID")
        registry.assert_not_called()

    def test_environment_proxy_userinfo_refused_before_pool_setup(self):
        with patch.object(R, "selected_proxy_url", return_value="http://PRIVATE_USER:PRIVATE_PASS@fixture.invalid"), \
                patch("src.demo_get_pool._tls_context", side_effect=AssertionError("NO_POOL_TLS")), \
                self.assertRaises(DemoTransportError) as caught:
            with R.runtime_demo_get():
                self.fail("USERINFO_ACCEPTED")
        self.assertEqual(str(caught.exception), "DEMO_POOL_PROXY_INVALID")

    def test_static_proxy_accepts_single_address_or_https_mapping(self):
        for raw, expected in (
                ("fixture.invalid:8080", "http://fixture.invalid:8080"),
                ("https://fixture.invalid:8443/", "https://fixture.invalid:8443/"),
                ("http=one.invalid:80;https=two.invalid:443", "http://two.invalid:443"),
                ("HTTPS=https://two.invalid:443;HTTP=one.invalid:80", "https://two.invalid:443"),
                ("[::1]:8080", "http://[::1]:8080")):
            with self.subTest(raw=raw):
                self.assertEqual(R.parse_windows_proxy_server(raw), expected)

    def test_static_proxy_rejects_pac_ambiguous_or_authenticated_syntax(self):
        for raw in (None, True, 1, "", "x" * 2049, "bad host:80", "主机:80", "x\n:80",
                    "http=one.invalid:80", "https=x;https=y", "https=x;", "ftp=x;https=y",
                    "socks=x;https=y", "socks://x:1080", "http://x/proxy.pac", "x:80;y:81",
                    "http://user:PRIVATE_PASSWORD@x:80", "x:0", "x:65536", "x:",
                    "http://x?", "http://x#", "http://x\\path", "http://%78:80", "https==x"):
            with self.subTest(kind=type(raw).__name__, size=len(raw) if isinstance(raw, str) else 0), \
                    self.assertRaises(R.DemoRuntimeGetRefused) as caught:
                R.parse_windows_proxy_server(raw)
            self.assertEqual(str(caught.exception), "DEMO_RUNTIME_WINDOWS_PROXY_CONFIG_INVALID")

    def registry(self, *, enabled=1, server="https=fixture.invalid:8443"):
        handle = Mock()
        handle.__enter__ = Mock(return_value=handle)
        handle.__exit__ = Mock(return_value=False)

        def query(found, name):
            self.assertIs(found, handle)
            value = {"ProxyEnable": enabled, "ProxyServer": server}[name]
            if isinstance(value, BaseException):
                raise value
            return value, 0

        registry = SimpleNamespace(HKEY_CURRENT_USER=object(), KEY_QUERY_VALUE=1,
                                   OpenKey=Mock(return_value=handle), QueryValueEx=Mock(side_effect=query))
        return registry, handle

    def test_registry_reads_only_two_public_values_with_query_only_access(self):
        registry, handle = self.registry()
        with patch.dict(sys.modules, {"winreg": registry}):
            self.assertEqual(R.windows_static_proxy_url(), "http://fixture.invalid:8443")
        registry.OpenKey.assert_called_once_with(registry.HKEY_CURRENT_USER, R.WINDOWS_PROXY_KEY, 0, 1)
        self.assertEqual([item.args[1] for item in registry.QueryValueEx.call_args_list],
                         ["ProxyEnable", "ProxyServer"])
        handle.__exit__.assert_called_once()

    def test_registry_disabled_or_missing_enable_never_reads_server(self):
        for enabled in (0, FileNotFoundError()):
            registry, _ = self.registry(enabled=enabled, server=AssertionError("NO_SERVER_READ"))
            with patch.dict(sys.modules, {"winreg": registry}):
                self.assertIsNone(R.windows_static_proxy_url())
            self.assertEqual([item.args[1] for item in registry.QueryValueEx.call_args_list], ["ProxyEnable"])

    def test_registry_missing_key_means_no_static_proxy(self):
        registry, _ = self.registry()
        registry.OpenKey.side_effect = FileNotFoundError()
        with patch.dict(sys.modules, {"winreg": registry}):
            self.assertIsNone(R.windows_static_proxy_url())
        registry.QueryValueEx.assert_not_called()

    def test_registry_invalid_enable_or_missing_server_refuses(self):
        for enabled, server in ((True, "x"), (2, "x"), ("1", "x"), (1, FileNotFoundError())):
            registry, _ = self.registry(enabled=enabled, server=server)
            with patch.dict(sys.modules, {"winreg": registry}), self.assertRaises(R.DemoRuntimeGetRefused) as caught:
                R.windows_static_proxy_url()
            self.assertEqual(str(caught.exception), "DEMO_RUNTIME_WINDOWS_PROXY_CONFIG_INVALID")

    def test_registry_read_failure_has_fixed_error_and_closes_handle(self):
        registry, handle = self.registry(enabled=PermissionError("PRIVATE_REGISTRY_ERROR"))
        with patch.dict(sys.modules, {"winreg": registry}), self.assertRaises(R.DemoRuntimeGetRefused) as caught:
            R.windows_static_proxy_url()
        self.assertEqual(str(caught.exception), "DEMO_RUNTIME_WINDOWS_PROXY_READ_FAILED")
        handle.__exit__.assert_called_once()

    def test_pool_shared_by_real_read_clients_and_route_get_post_remains_separate(self):
        from src import demo_route_transport as route
        events = []

        class FakePool:
            def __init__(self, *, proxy_url):
                self.closed = False
                events.append("construct")
                self.proxy = proxy_url

            def __enter__(self):
                events.append("enter")
                return self

            def __exit__(self, *_):
                self.closed = True
                events.append("close")

            def get(self, url, headers, timeout):
                if self.closed:
                    raise AssertionError("GET_AFTER_POOL_CLOSE")
                self_test.assertEqual(headers[DEMO_HEADER], "1")
                self_test.assertEqual(timeout, MAX_TIMEOUT)
                events.append("GET")
                return 200, {"code": "0", "data": []}

        self_test = self
        credentials = {name: "fixture" for name in CREDENTIAL_NAMES}
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory, \
                patch.object(R, "selected_proxy_url", return_value=None) as select, \
                patch.object(R, "DemoGetPool", FakePool), \
                patch.object(route, "_real_http_post", side_effect=AssertionError("NO_POST")) as post:
            context = R.runtime_demo_get()
            select.assert_not_called()
            self.assertEqual(events, [])
            with context as get:
                first = DemoReadOnlyClient(credentials, http_get=get, network_retries=0)
                second = DemoReadOnlyClient(credentials, http_get=get, network_retries=1,
                                            retry_budget=Mock(side_effect=AssertionError("NO_RETRY")))
                transport = route.DemoRouteTransport(credentials, enable_writes=True, http_get=get,
                                                     log_root=Path(directory))
                self.assertIs(first._http_get, get)
                self.assertIs(second._http_get, get)
                self.assertIs(transport._reader._http_get, get)
                self.assertIs(transport._http_post, post)
                self.assertEqual(first.get("/api/v5/account/config"), [])
                self.assertEqual(second.get("/api/v5/account/balance"), [])
                self.assertEqual(transport._reader.get("/api/v5/trade/order", {"instId": "SOL-USDT-SWAP"}), [])
                with self.assertRaises(DemoWriteRefused):
                    first.post("/api/v5/trade/order", {})
                post.assert_not_called()
            self.assertTrue(get.__self__.closed)
        self.assertEqual(events, ["construct", "enter", "GET", "GET", "GET", "close"])

    def test_pool_closes_after_exception_without_retrying_or_hiding_body_error(self):
        pool = Mock()
        pool.__enter__ = Mock(return_value=pool)
        pool.__exit__ = Mock(return_value=False)
        with patch.object(R, "selected_proxy_url", return_value=None), \
                patch.object(R, "DemoGetPool", return_value=pool) as factory, \
                self.assertRaisesRegex(OSError, "FIXTURE_BODY_FAILURE"):
            with R.runtime_demo_get() as get:
                self.assertIs(get, pool.get)
                raise OSError("FIXTURE_BODY_FAILURE")
        factory.assert_called_once_with(proxy_url=None)
        pool.__exit__.assert_called_once()

    def test_pool_setup_failure_does_not_retry_or_enter_body(self):
        with patch.object(R, "selected_proxy_url", return_value=None), \
                patch.object(R, "DemoGetPool", side_effect=DemoTransportError("DEMO_POOL_INIT_FAILED")) as factory, \
                self.assertRaises(DemoTransportError) as caught:
            with R.runtime_demo_get():
                self.fail("FAILED_POOL_ENTERED")
        self.assertEqual(caught.exception.code, "DEMO_POOL_INIT_FAILED")
        factory.assert_called_once_with(proxy_url=None)


if __name__ == "__main__":
    unittest.main()
