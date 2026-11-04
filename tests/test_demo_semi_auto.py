# -*- coding: utf-8 -*-
"""scripts/demo_semi_auto.py 的离线测试（无网络、无真实凭据、无订单）。"""
from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import demo_semi_auto as D  # noqa: E402


class StageOneTests(unittest.TestCase):
    def _tmp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "logs").mkdir(parents=True, exist_ok=True)
        return root

    def _run(self, argv, *, root=None, client_factory=None):
        root = root or self._tmp()
        out = io.StringIO()
        with mock.patch.object(D, "ROOT", root), mock.patch.object(D, "LOG_ROOT", root / "logs" / "demo-stage1"):
            with redirect_stdout(out):
                code = D.main(argv, client_factory=client_factory, log_root=root / "logs" / "demo-stage1")
        return code, out.getvalue(), root

    def test_explain_is_zero_request(self):
        def factory(_creds):
            raise AssertionError("explain must not build a client")

        code, out, _ = self._run(["--explain"], client_factory=factory)
        self.assertEqual(code, 0)
        self.assertIn("未放行", out)

    def test_default_is_explain(self):
        code, out, _ = self._run([], client_factory=lambda _c: (_ for _ in ()).throw(
            AssertionError("no client")))
        self.assertEqual(code, 0)
        self.assertIn("--preflight", out)

    def test_demo_run_is_not_released(self):
        code, out, _ = self._run(["--demo-run"])
        self.assertEqual(code, 3)
        self.assertIn("DEMO_ENTRY_NOT_RELEASED", out)

    def test_mock_protected_with_exact_confirm(self):
        code, out, root = self._run(["--mock-run", "--scenario", "protected",
                                     "--confirm", "CONFIRM DEMO-S1"])
        self.assertEqual(code, 0)
        self.assertIn("status=PROTECTED", out)
        files = list((root / "logs" / "demo-stage1").rglob("mock-run.json"))
        self.assertEqual(len(files), 1)

    def test_mock_wrong_or_missing_confirm_is_rejected(self):
        code, out, _ = self._run(["--mock-run", "--scenario", "protected", "--confirm", "yes"])
        self.assertEqual(code, 0)
        self.assertIn("OPERATOR_REJECTED", out)

    def test_mock_exception_returns_unknown_and_halt(self):
        code, out, _ = self._run(["--mock-run", "--scenario", "exception",
                                  "--confirm", "CONFIRM DEMO-S1"])
        self.assertEqual(code, 2)
        self.assertIn("UNKNOWN", out)
        self.assertIn("halt=True", out)

    def test_mock_cancel_race_is_protected(self):
        code, out, _ = self._run(["--mock-run", "--scenario", "cancel-race",
                                  "--confirm", "CONFIRM DEMO-S1"])
        self.assertEqual(code, 0)
        self.assertIn("fill_during_cancel", out)
        self.assertIn("PROTECTED", out)

    def test_preflight_missing_credentials_refuses_before_network(self):
        with mock.patch.object(D, "load_demo_credentials",
                               side_effect=D.DemoCredentialError("DEMO_CREDENTIALS_INCOMPLETE:OKX_DEMO_API_KEY")):
            def factory(_creds):
                raise AssertionError("must not build client without credentials")
            code, out, _ = self._run(["--preflight"], client_factory=factory)
        self.assertEqual(code, 2)
        self.assertIn("DEMO_CREDENTIALS_INCOMPLETE", out)

    def test_preflight_writes_summary_and_flags_release_false(self):
        summary = {"transport": "demo-readonly", "pos_mode": "net_mode",
                   "equity_usdt": 100.0, "release_ready": False, "orders_sent": False}
        with mock.patch.object(D, "load_demo_credentials", return_value={"k": "v"}), \
                mock.patch.object(D, "demo_preflight", return_value=summary):
            code, out, root = self._run(["--preflight"], client_factory=lambda c: object())
        self.assertEqual(code, 0)
        self.assertIn("release_ready", out)
        self.assertEqual(len(list((root / "logs" / "demo-stage1").rglob("preflight.json"))), 1)

    def test_preflight_transport_error_is_safe_code(self):
        with mock.patch.object(D, "load_demo_credentials", return_value={"k": "v"}), \
                mock.patch.object(D, "demo_preflight",
                                  side_effect=D.DemoTransportError("DEMO_API_ERROR", "x")):
            code, out, _ = self._run(["--preflight"], client_factory=lambda c: object())
        self.assertEqual(code, 2)
        self.assertIn("DEMO_API_ERROR", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
