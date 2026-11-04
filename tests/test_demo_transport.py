# -*- coding: utf-8 -*-
"""src/demo_transport.py 的离线测试（无网络、无真实凭据/注册表、无写操作）。"""
from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import demo_transport as T  # noqa: E402

CREDS = dict(zip(T.CREDENTIAL_NAMES, ("demo-key", "demo-secret", "demo-pass")))


class CredentialTests(unittest.TestCase):
    def test_missing_any_demo_credential_is_refused(self):
        for name in T.CREDENTIAL_NAMES:
            env = dict(CREDS)
            env.pop(name)
            with self.assertRaises(T.DemoCredentialError):
                T.load_demo_credentials(env)

    def test_no_fallback_to_real_api_credentials(self):
        with self.assertRaises(T.DemoCredentialError):
            T.load_demo_credentials({"OKX_API_KEY": "real", "OKX_API_SECRET": "real",
                                     "OKX_API_PASSPHRASE": "real"})

    def test_user_registry_fallback_is_read_only_and_demo_only(self):
        requested = []

        def query(_handle, name):
            requested.append(name)
            if name not in CREDS:
                raise AssertionError("live credential access forbidden")
            return CREDS[name], 1

        registry = types.SimpleNamespace(
            HKEY_CURRENT_USER=1, KEY_READ=1, KEY_QUERY_VALUE=1,
            OpenKey=lambda *a, **k: mock.MagicMock(),
            QueryValueEx=query)
        with mock.patch.object(T.os, "environ", {}), mock.patch.dict(sys.modules, {"winreg": registry}):
            values = T.load_demo_credentials()
        self.assertEqual(values, CREDS)
        self.assertEqual(set(requested), set(T.CREDENTIAL_NAMES))


class ReadOnlyClientTests(unittest.TestCase):
    def test_auth_headers_and_fixed_demo_header(self):
        seen = {}

        def http_get(url, headers, timeout):
            seen.update({"url": url, "headers": dict(headers), "timeout": timeout})
            return 200, {"code": "0", "data": []}

        client = T.DemoReadOnlyClient(CREDS, http_get=http_get)
        client.get("/api/v5/account/config", {"x": "1"})
        names = {k.lower(): v for k, v in seen["headers"].items()}
        for header in ("ok-access-key", "ok-access-sign", "ok-access-timestamp",
                       "ok-access-passphrase"):
            self.assertIn(header, names)
        self.assertEqual(names["ok-access-key"], CREDS[T.CREDENTIAL_NAMES[0]])
        self.assertEqual(names[T.DEMO_HEADER], "1")
        self.assertTrue(seen["url"].startswith(T.DEMO_BASE + "/api/v5/account/config"))

    def test_base_is_locked(self):
        with self.assertRaises(T.DemoTransportError):
            T.DemoReadOnlyClient(CREDS, base="https://outside.invalid",
                                 http_get=lambda *a: (200, {"code": "0", "data": []}))

    def test_path_cannot_change_authority(self):
        calls = []
        client = T.DemoReadOnlyClient(
            CREDS, http_get=lambda *a: (calls.append(a) or (200, {"code": "0", "data": []})))
        with self.assertRaises(T.DemoTransportError):
            client.get("@outside.invalid/api/v5/account/config")
        self.assertEqual(calls, [])

    def test_write_is_refused_in_stage1(self):
        client = T.DemoReadOnlyClient(CREDS, http_get=lambda *a: (200, {"code": "0", "data": []}))
        with self.assertRaises(T.DemoWriteRefused):
            client.post("/api/v5/trade/order")

    def test_api_error_is_sanitized(self):
        client = T.DemoReadOnlyClient(
            CREDS, http_get=lambda *a: (200, {"code": "sensitive-marker"}))
        with self.assertRaises(T.DemoTransportError) as caught:
            client.get("/api/v5/account/config")
        self.assertNotIn("sensitive-marker", str(caught.exception))

    def test_network_error_is_sanitized(self):
        def failed(*args):
            raise RuntimeError("https://outside.invalid/sensitive-marker")
        client = T.DemoReadOnlyClient(CREDS, http_get=failed)
        with self.assertRaises(T.DemoTransportError) as caught:
            client.get("/api/v5/account/config")
        self.assertNotIn("sensitive-marker", str(caught.exception))

    def test_redirect_is_sanitized(self):
        with self.assertRaises(T.DemoTransportError) as caught:
            T._NoRedirect().redirect_request(None, None, 302, "Found", {},
                                             "https://outside.invalid/sensitive-marker")
        self.assertNotIn("sensitive-marker", str(caught.exception))

    def test_bad_response_structure_is_refused(self):
        client = T.DemoReadOnlyClient(CREDS, http_get=lambda *a: (200, {"code": "0", "data": None}))
        with self.assertRaises(T.DemoTransportError):
            client.get("/api/v5/account/config")


class PreflightTests(unittest.TestCase):
    def test_preflight_summary_is_read_only(self):
        class FakeClient:
            def get(self, path, params=None):
                return {
                    "/api/v5/account/config": [{"posMode": "net_mode", "acctLv": "2"}],
                    "/api/v5/account/balance": [{"details": [{"ccy": "USDT", "eq": "100.5"}]}],
                    "/api/v5/account/positions": [],
                    "/api/v5/trade/orders-pending": [],
                    "/api/v5/trade/orders-algo-pending": [],
                    "/api/v5/account/leverage-info": [{"lever": "8"}],
                    "/api/v5/public/instruments": [{"minSz": "0.01", "ctVal": "1"}],
                }[path]

        summary = T.demo_preflight(FakeClient())
        self.assertEqual(summary["pos_mode"], "net_mode")
        self.assertEqual(summary["equity_usdt"], 100.5)
        self.assertEqual(summary["sol_cross_lever"], "8")
        self.assertFalse(summary["release_ready"])
        self.assertFalse(summary["orders_sent"])
        self.assertFalse(summary["write_allowed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
