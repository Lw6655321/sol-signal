# -*- coding: utf-8 -*-
"""demo_technical_test.py 的离线 Fake 测试（无网络/凭据/账户请求/订单）。"""
from __future__ import annotations

import contextlib
import builtins
import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import demo_technical_test as D  # noqa: E402


class FakeDeps:
    poll_seconds = 0
    close_wait_seconds = 1
    ledger_wait_seconds = 1

    def __init__(self, tmp: Path, behavior: str = "success", market_ok: bool = True,
                 entry_states=None):
        self.tmp = tmp
        self.behavior = behavior
        self.market_ok = market_ok
        self.calls = []
        self.run_id_hint = None
        self.plan = None
        self.evidence = None
        self.signal_id = None
        self._phase = "entry"
        self._state = {}
        self._ticks = 0
        self._prepared_state = None
        self._reconcile_override = None
        self._entry_states = list(entry_states or ["PROTECTED"])
        self._risk = {"demo_verified": True, "ledger_verified": True,
                      "baseline_trusted": True, "history_complete": True,
                      "account_snapshot_consistent": True, "refusal_reasons": [],
                      "request_faults": [], "open_positions": [], "pending_orders": [],
                      "protection_orders": [], "entries_today": 1,
                      "realized_pnl_today": "-0.01"}
        self.halted = []

    # --- 信息
    def fixed_commit(self):
        return "8381b547b8215ca7d72f80f796ef855ce8698957"

    def utc_now(self):
        return "2026-10-19T10:00:00Z"

    def beijing_now(self):
        return "2026-10-19T18:00:00+08:00"

    # --- 流程
    def validate_roots(self, predecessor_root, new_root):
        self.calls.append(("validate_roots",))

    def check_predecessor(self, predecessor_root):
        self.calls.append(("check_predecessor", str(predecessor_root)))
        if self.behavior == "predecessor_fail":
            raise D.TestRefused("PREDECESSOR_NOT_EXITED")

    def prepare_run(self, runtime, new_root, options):
        self.calls.append(("prepare_run",))
        if self.behavior == "recover":
            plan = self._recovery_plan
            status = {"status": "PROTECTED", "code": "OK",
                      "signal_id": plan["signal"]["id"],
                      "filled_qty": "0.01", "protected_qty": "0.01"}
            self._prepared_state = status
            return {"mode": "resume", "plan": plan, "status": status}
        if self.behavior == "recover_closing":
            plan = D.build_plan("smoke", D.build_signal(
                "smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000))
            status = {"status": "CLOSING", "code": "DEMO_TECHNICAL_TEST_EXIT",
                      "signal_id": plan["signal"]["id"],
                      "filled_qty": "0.01", "protected_qty": "0.01"}
            self._prepared_state = status
            return {"mode": "resume", "plan": plan, "status": status}
        if self.behavior == "recover_natural_closed":
            plan = D.build_plan("smoke", D.build_signal(
                "smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000))
            status = {"status": "PROTECTED", "code": "OK",
                      "signal_id": plan["signal"]["id"],
                      "filled_qty": "0.01", "protected_qty": "0.01"}
            self._prepared_state = status
            self._reconcile_override = {"status": "CLOSED", "code": "DEMO_NATURAL_EXIT",
                                        "signal_id": plan["signal"]["id"],
                                        "filled_qty": "0", "protected_qty": "0"}
            return {"mode": "resume", "plan": plan, "status": status}
        return {"mode": "new"}

    def reconcile(self, runtime):
        self.calls.append(("reconcile",))
        if self._reconcile_override is not None:
            self._state = dict(self._reconcile_override)
            return dict(self._reconcile_override)
        return dict(self._prepared_state or self._state)

    def recover_owners(self, runtime):
        self.calls.append(("recover_owners",))
        return 0

    def verify_terminal(self, runtime, signal_id):
        self.calls.append(("verify_terminal",))
        return self.behavior != "terminal_fail"

    @contextlib.contextmanager
    def locks(self, predecessor_root, new_root):
        self.calls.append(("lock_enter", str(predecessor_root), str(new_root)))
        try:
            yield
        finally:
            self.calls.append(("lock_exit",))

    def preserve_state(self, predecessor_root, new_root):
        self.calls.append(("preserve_state",))

    def build_runtime(self, new_root):
        self.calls.append(("build_runtime", str(new_root)))
        return {"root": str(new_root)}

    def assert_clean_start(self, runtime):
        self.calls.append(("assert_clean_start",))
        if self.behavior == "clean_fail":
            raise D.TestRefused("ACCOUNT_NOT_CLEAR")

    def collect_snapshot(self, new_root):
        self.calls.append(("collect_snapshot",))
        return {"facts": {"bid": "100.00", "ask": "100.02", "price": "100.01"}}

    def market_fields(self, snapshot):
        self.calls.append(("market_fields",))
        if not self.market_ok:
            raise D.TestRefused("MARKET_EVIDENCE_UNVERIFIED")
        return {"bid": "100.00", "ask": "100.02", "last": "100.01",
                "tick_size": "0.01", "min_sz": "0.01",
                "evidence": {"generated_at_ms": 1792400000000, "data_fresh": True,
                             "macro_blackout": False}}

    def resume_or_create_plan(self, new_root, scenario, side, fields, report):
        self.calls.append(("resume_or_create_plan", scenario, side))
        base = Path(new_root) / "logs" / "technical-tests"
        run_id = self.run_id_hint or "test-fixed"
        run_dir = base / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "plan.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        now = 1792400000000
        signal = D.build_signal(scenario, side, fields["bid"], fields["ask"],
                                fields["tick_size"], fields["min_sz"], now)
        plan = D.build_plan(scenario, signal)
        path.write_text(json.dumps(plan), encoding="utf-8")
        return plan

    def run_id(self, new_root, plan):
        return self.run_id_hint or "test-fixed"

    def start_test(self, runtime, plan, evidence):
        self.calls.append(("start_test",))
        self.plan = plan
        self.evidence = evidence
        self.signal_id = plan["signal"]["id"]
        if self.behavior == "start_raise":
            raise RuntimeError("simulated transport failure")
        if self.behavior == "natural_entry_closed":
            self._state = {"status": "CLOSED", "code": "DEMO_NATURAL_EXIT",
                           "signal_id": self.signal_id,
                           "filled_qty": "0", "protected_qty": "0"}
            return dict(self._state)
        if self.behavior == "protection_mismatch":
            self._phase = "entry"
            self._state = {"status": "PROTECTED", "code": "OK", "signal_id": self.signal_id,
                           "filled_qty": "0.01", "protected_qty": "0"}
            return dict(self._state)
        self._state = {"status": "PENDING", "code": "PENDING", "signal_id": self.signal_id,
                       "filled_qty": "0", "protected_qty": "0"}
        return dict(self._state)

    def tick(self, runtime):
        self.calls.append(("tick",))
        if not self._state and self._prepared_state:
            return dict(self._prepared_state)
        self._ticks += 1
        if self._phase == "entry":
            if self._state["status"] == "PENDING":
                if self.behavior == "unfilled":
                    return dict(self._state)
                if self._ticks >= self._entry_states.__len__() + 1:
                    self._state = {"status": self._entry_states[-1], "code": "OK",
                                   "signal_id": self.signal_id,
                                   "filled_qty": "0.01", "protected_qty": "0.01"}
            elif self._state["status"] == "PROTECTED" and self.behavior == "observe_closed":
                self._state = {"status": "CLOSED", "code": "DEMO_NATURAL_EXIT",
                               "signal_id": self.signal_id,
                               "filled_qty": "0", "protected_qty": "0"}
        elif self._phase == "close":
            self._state = {"status": "CLOSED", "code": "DEMO_TECHNICAL_TEST_EXIT",
                           "signal_id": self.signal_id, "filled_qty": "0", "protected_qty": "0"}
        return dict(self._state)

    def status(self, runtime, signal_id):
        if not self._state and self._prepared_state:
            return dict(self._prepared_state)
        return dict(self._state) if self._state else {
            "status": "IDLE", "code": "READY_FOR_SCAN", "signal_id": ""}

    def cancel_pending(self, runtime, signal_id):
        self.calls.append(("cancel_pending",))
        self._phase = "cancelled"
        self._state = {"status": "CANCELED", "code": "CANCELED", "signal_id": signal_id,
                       "filled_qty": "0", "protected_qty": "0"}
        return dict(self._state)

    def close_test(self, runtime, signal_id):
        self.calls.append(("close_test",))
        self._phase = "close"
        self._state = {"status": "CLOSING", "code": "DEMO_TECHNICAL_TEST_EXIT",
                       "signal_id": signal_id, "filled_qty": "0.01", "protected_qty": "0.01"}
        if self.behavior == "close_fail":
            self._phase = "close_fail"
        return dict(self._state)

    def risk_view(self, runtime):
        self.calls.append(("risk_view",))
        if self.behavior == "ledger_fail":
            return {"demo_verified": False, "ledger_verified": False,
                    "baseline_trusted": False, "history_complete": False,
                    "account_snapshot_consistent": False,
                    "refusal_reasons": ["LEDGER_NOT_VERIFIED"], "request_faults": [],
                    "open_positions": [], "pending_orders": [], "protection_orders": []}
        return dict(self._risk)

    def safety_halt(self, runtime, code):
        self.halted.append(code)
        self.calls.append(("safety_halt", code))

    def write_report(self, new_root, report, outcome):
        self.calls.append(("write_report", outcome))
        base = Path(new_root) / "logs" / "technical-tests"
        run_id = report.get("run_id") or "unknown"
        run_dir = base / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        safe = D._sanitize(report)
        path = run_dir / "report.json"
        path.write_text(json.dumps(safe), encoding="utf-8")
        return {"result": outcome, "report_path": str(path)}


class Helpers(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)

    def run_main(self, argv, deps):
        out = io.StringIO()
        with redirect_stdout(out):
            code = D.main(argv, deps_factory=lambda: deps)
        return code, out.getvalue()

    def base_argv(self, scenario="smoke", observe=1):
        return ["--run-test", "--enable-demo-test", "--scenario", scenario,
                "--predecessor-root", str(self.root / "old"), "--new-root", str(self.root),
                "--allow-short-observation", "--observe-seconds", str(observe),
                "--entry-wait-seconds", "1"]


class ActivatedGetScopeTests(Helpers):
    def setUp(self):
        super().setUp()
        from src import demo_successor_gate as gate
        self.gate = gate
        self.old, self.new = self.root / "old", self.root / "new"
        self.paths = []
        for root in (self.old, self.new):
            state = root / "state" / "demo-auto"
            state.mkdir(parents=True)
            path = state / "process.lock"
            path.write_bytes(b"EXISTING_LOCK_BYTES")
            self.paths.append(path)
        (self.old / "state/demo-auto/CONTROLLER_HALT").write_bytes(b"EXISTING_HALT")
        self.deps = D.ActivatedProductionDeps()
        self.events = []
        self.active = False
        self.get = mock.Mock(side_effect=self.assert_scope)
        self.addCleanup(mock.patch.stopall)
        mock.patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("socket.getaddrinfo", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()

    def assert_scope(self, *args):
        self.assertTrue(self.active)
        self.assertEqual(set(self.gate._OWNED_PROCESS_LOCKS.get()), set(self.paths))

    def assert_released(self):
        self.assertIsNone(self.deps._http_get)
        self.assertFalse(self.active)
        self.assertFalse(self.gate._OWNED_PROCESS_LOCKS.get())
        self.assertEqual((self.old / "state/demo-auto/CONTROLLER_HALT").read_bytes(), b"EXISTING_HALT")
        for path in self.paths:
            self.assertEqual(path.read_bytes(), b"EXISTING_LOCK_BYTES")
        with self.gate.owned_process_locks((self.old, self.new)):
            pass

    @contextlib.contextmanager
    def pool(self):
        self.assertEqual(set(self.gate._OWNED_PROCESS_LOCKS.get()), set(self.paths))
        self.events.append("pool-enter")
        self.active = True
        try:
            yield self.get
        finally:
            self.assert_scope()
            self.events.append("pool-close")
            self.active = False

    def gate_check(self, root):
        self.assertEqual(root, self.new)
        self.assertFalse(self.active)
        self.assertEqual(set(self.gate._OWNED_PROCESS_LOCKS.get()), set(self.paths))
        self.events.append("common-gate")

    def test_pool_begins_after_real_double_locks_and_common_gate_then_clears_hook(self):
        with mock.patch.object(self.deps, "_activated", side_effect=self.gate_check), \
                mock.patch.object(D, "runtime_demo_get", self.pool):
            with self.deps.locks(self.old, self.new):
                self.assertIs(self.deps._http_get, self.get)
                self.deps._http_get()
                self.events.append("runtime")
        self.assertEqual(self.events, ["common-gate", "pool-enter", "runtime", "pool-close"])
        self.assert_released()

    def test_failed_common_gate_never_opens_pool_and_releases_both_locks(self):
        with mock.patch.object(self.deps, "_activated", side_effect=D.TestRefused("SUCCESSOR_ACTIVATION_UNREADABLE")), \
                mock.patch.object(D, "runtime_demo_get") as pool, self.assertRaises(D.TestRefused) as caught:
            with self.deps.locks(self.old, self.new):
                self.fail("MISSING_ACTIVATION_ENTERED")
        self.assertEqual(caught.exception.code, "SUCCESSOR_ACTIVATION_UNREADABLE")
        pool.assert_not_called()
        self.assert_released()

    def test_pool_body_exception_releases_hook_pool_locks_and_preserves_halt(self):
        with mock.patch.object(self.deps, "_activated", side_effect=self.gate_check), \
                mock.patch.object(D, "runtime_demo_get", self.pool), self.assertRaisesRegex(OSError, "FIXTURE_FAILURE"):
            with self.deps.locks(self.old, self.new):
                raise OSError("FIXTURE_FAILURE")
        self.assertEqual(self.events, ["common-gate", "pool-enter", "pool-close"])
        self.assert_released()

    def test_pool_setup_failure_is_sanitized_and_releases_both_locks(self):
        from src.demo_transport import DemoTransportError
        with mock.patch.object(self.deps, "_activated", side_effect=self.gate_check), \
                mock.patch.object(D, "runtime_demo_get", side_effect=DemoTransportError("DEMO_POOL_INIT_FAILED")), \
                self.assertRaises(D.TestRefused) as caught:
            with self.deps.locks(self.old, self.new):
                self.fail("FAILED_POOL_ENTERED")
        self.assertEqual(caught.exception.code, "DEMO_POOL_INIT_FAILED")
        self.assert_released()

    def test_pool_close_failure_does_not_leave_hook_or_process_locks(self):
        from src.demo_transport import DemoTransportError

        @contextlib.contextmanager
        def failing_close():
            with self.pool() as get:
                yield get
            raise DemoTransportError("DEMO_POOL_CLOSE_FAILED")

        with mock.patch.object(self.deps, "_activated", side_effect=self.gate_check), \
                mock.patch.object(D, "runtime_demo_get", failing_close), self.assertRaises(D.TestRefused) as caught:
            with self.deps.locks(self.old, self.new):
                pass
        self.assertEqual(caught.exception.code, "DEMO_POOL_CLOSE_FAILED")
        self.assert_released()

    def test_complete_technical_flow_cleanup_and_report_stay_inside_pool_and_locks(self):
        owner = self

        class PooledFakeDeps(FakeDeps):
            locks = D.ActivatedProductionDeps.locks

            def _activated(self, root):
                return owner.gate_check(root)

            def build_runtime(self, root):
                owner.assert_scope()
                owner.assertIs(self._http_get, owner.get)
                owner.events.append("runtime")
                return super().build_runtime(root)

            def safety_halt(self, runtime, code):
                self._http_get()
                owner.events.append("cleanup")
                return super().safety_halt(runtime, code)

            def write_report(self, new_root, report, outcome):
                self._http_get()
                owner.events.append("report")
                return super().write_report(new_root, report, outcome)

        for behavior in ("success", "start_raise"):
            with self.subTest(behavior=behavior):
                self.events.clear()
                deps = PooledFakeDeps(self.new, behavior=behavior)
                deps._http_get = None
                options = SimpleNamespace(scenario="smoke", side="long", run_id=behavior,
                    new_root=self.new, predecessor_root=self.old, observe_seconds=1, entry_wait_seconds=1)
                with mock.patch.object(D, "runtime_demo_get", self.pool):
                    result = D.execute_test(deps, options)
                self.assertEqual(result["result"], "PASS" if behavior == "success" else "FAIL")
                self.assertIsNone(deps._http_get)
                self.assertEqual(self.events[0:3], ["common-gate", "pool-enter", "runtime"])
                self.assertEqual(self.events[-2:], ["report", "pool-close"])
                self.assertEqual("cleanup" in self.events, behavior == "start_raise")
                self.assert_released()


class EntryGateTests(unittest.TestCase):
    @staticmethod
    def clean_path():
        root = str(ROOT).casefold()
        return [value for value in sys.path if str(value).casefold() not in {root, root + "\\scripts", root + "/scripts"}]

    @staticmethod
    def load_isolated_entry():
        specification = importlib.util.spec_from_file_location("isolated_technical_bootstrap_test", ROOT / "scripts/demo_technical_test.py")
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        return module

    def test_fresh_explanation_bootstraps_own_root_without_local_api_imports(self):
        real_import, local_imports = builtins.__import__, []

        def imports(name, *args, **kwargs):
            if name.startswith("src"):
                local_imports.append(name)
                raise AssertionError("NO_LOCAL_API_IMPORT_FOR_EXPLANATION")
            return real_import(name, *args, **kwargs)

        with mock.patch.object(sys, "path", self.clean_path()), \
                mock.patch("builtins.__import__", imports), redirect_stdout(io.StringIO()):
            module = self.load_isolated_entry()
            self.assertEqual(module.main([]), 0)
            self.assertEqual(sys.path[0], str(ROOT))
        self.assertEqual(local_imports, [])

    def test_fresh_activated_gate_refuses_before_runtime_or_pool_with_clean_path(self):
        from src.demo_successor_gate import SuccessorGateRefused
        real_import, gate_imports = builtins.__import__, []

        def imports(name, *args, **kwargs):
            if name.startswith("src"):
                self.assertEqual(sys.path[0], str(ROOT))
                self.assertEqual(name, "src.demo_successor_gate")
                gate_imports.append(name)
            return real_import(name, *args, **kwargs)

        with mock.patch("src.demo_successor_gate.assert_activated_generation",
                        side_effect=SuccessorGateRefused("SUCCESSOR_PREPARATION_UNREADABLE")) as gate, \
                mock.patch.object(sys, "path", self.clean_path()), mock.patch("builtins.__import__", imports):
            module = self.load_isolated_entry()
            with mock.patch.object(module, "runtime_demo_get", side_effect=AssertionError("NO_POOL")) as pool, \
                    self.assertRaises(module.TestRefused) as caught:
                module.ActivatedProductionDeps._activated(ROOT)
            self.assertEqual(caught.exception.code, "SUCCESSOR_PREPARATION_UNREADABLE")
            gate.assert_called_once_with(ROOT)
            pool.assert_not_called()
        self.assertEqual(gate_imports, ["src.demo_successor_gate"])

    def test_explain_never_builds_deps(self):
        def factory():
            raise AssertionError("deps must not be built")
        with redirect_stdout(io.StringIO()):
            code = D.main([], deps_factory=factory)
        self.assertEqual(code, 0)

    def test_execution_requires_both_switches_and_scenario(self):
        for argv in (["--run-test", "--scenario", "smoke", "--predecessor-root", "x"],
                     ["--run-test", "--enable-demo-test", "--predecessor-root", "x"]):
            with redirect_stdout(io.StringIO()):
                self.assertEqual(D.main(argv, deps_factory=lambda: object()), 2)
        # 没有 --run-test 时是默认 explain（零请求），不是执行
        with redirect_stdout(io.StringIO()):
            self.assertEqual(D.main(["--enable-demo-test", "--scenario", "smoke",
                                     "--predecessor-root", "x"],
                                    deps_factory=lambda: object()), 0)

    def test_short_observation_needs_explicit_flag(self):
        with redirect_stdout(io.StringIO()):
            code = D.main(["--run-test", "--enable-demo-test", "--scenario", "smoke",
                           "--predecessor-root", "x", "--observe-seconds", "5"],
                          deps_factory=lambda: object())
        self.assertEqual(code, 2)


class SignalPlanTests(unittest.TestCase):
    def test_smoke_signal_and_plan(self):
        signal = D.build_signal("smoke", "long", "100.00", "100.02", "0.01", "0.01",
                                1792400000000)
        self.assertTrue(signal["id"].startswith("DEMO-TEST-SMOKE-"))
        self.assertEqual(signal["kind"], "scalp_5m")
        self.assertEqual(signal["qty"], "0.01")
        self.assertEqual(signal["side"], "long")
        plan = D.build_plan("smoke", signal)
        D.validate_plan(plan)
        self.assertEqual(set(plan), {"purpose", "scenario", "authorization_id", "signal"})
        self.assertEqual(plan["authorization_id"], "user-approved-demo-technical-tests-20261019")

    def test_swing_signal_and_maker_side(self):
        long_sig = D.build_signal("swing", "long", "100.00", "100.02", "0.01", "0.01",
                                  1792400000000)
        short_sig = D.build_signal("swing", "short", "100.00", "100.02", "0.01", "0.01",
                                   1792400000000)
        self.assertTrue(long_sig["id"].startswith("DEMO-TEST-SWING-"))
        self.assertEqual(long_sig["kind"], "swing_1h4h")
        self.assertEqual(long_sig["entry"], "100.00")      # long = bid
        self.assertEqual(short_sig["entry"], "100.02")     # short = ask
        self.assertLess(Decimal(long_sig["stop"]), Decimal(long_sig["entry"]))
        self.assertGreater(Decimal(short_sig["stop"]), Decimal(short_sig["entry"]))
        for sig in (long_sig, short_sig):
            D.validate_plan(D.build_plan("swing", sig))

    def test_plan_validation_rejects_wrong_contract(self):
        signal = D.build_signal("smoke", "long", "100.00", "100.02", "0.01", "0.01",
                                1792400000000)
        good = D.build_plan("smoke", signal)
        bad = [
            {**good, "purpose": "OTHER"},
            {**good, "authorization_id": "self-granted"},
            {k: v for k, v in good.items() if k != "signal"},
            {**good, "signal": {**signal, "id": "DEMO-TEST-SWING-1"}},
            {**good, "signal": {**signal, "qty": "0"}},
            {**good, "scenario": "scalp_5m"},
        ]
        for plan in bad:
            with self.assertRaises(D.TestRefused):
                D.validate_plan(plan)


class FlowTests(Helpers):
    def test_success_smoke_flow(self):
        deps = FakeDeps(self.root)
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertIsNotNone(deps.plan)
        self.assertIsNotNone(deps.evidence)
        self.assertEqual(deps.calls[0][0], "validate_roots")
        self.assertEqual(deps.calls[1][0], "check_predecessor")
        self.assertIn(("preserve_state",), deps.calls)
        self.assertIn(("start_test",), deps.calls)
        self.assertIn(("close_test",), deps.calls)
        self.assertTrue(any(call[0] == "write_report" and call[1] == "PASS"
                            for call in deps.calls))

    def test_success_swing_flow(self):
        deps = FakeDeps(self.root)
        code, _ = self.run_main(self.base_argv("swing", observe=1), deps)
        self.assertEqual(code, 0)
        self.assertTrue(deps.plan["signal"]["id"].startswith("DEMO-TEST-SWING-"))
        self.assertEqual(deps.plan["scenario"], "swing")

    def test_unfilled_entry_cancels_to_terminal_and_fails(self):
        deps = FakeDeps(self.root, behavior="unfilled")
        code, _ = self.run_main(self.base_argv(observe=1), deps)
        self.assertEqual(code, 2)
        self.assertIn(("cancel_pending",), deps.calls)
        self.assertNotIn(("close_test",), deps.calls)

    def test_protection_mismatch_halts(self):
        deps = FakeDeps(self.root, behavior="protection_mismatch")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        self.assertIn("PROTECTION_UNVERIFIED", deps.halted)

    def test_close_not_confirmed_halts(self):
        deps = FakeDeps(self.root, behavior="close_fail")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        self.assertIn("CLOSE_NOT_CONFIRMED", deps.halted)

    def test_ledger_incomplete_fails(self):
        deps = FakeDeps(self.root, behavior="ledger_fail")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)

    def test_market_evidence_failure_never_starts(self):
        deps = FakeDeps(self.root, market_ok=False)
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        self.assertNotIn(("start_test",), deps.calls)

    def test_predecessor_running_refuses(self):
        deps = FakeDeps(self.root, behavior="predecessor_fail")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        self.assertNotIn(("build_runtime", str(self.root)), deps.calls)

    def test_plan_resume_does_not_create_new_id(self):
        deps = FakeDeps(self.root)
        deps.run_id_hint = "test-resume"
        argv = self.base_argv() + ["--run-id", "test-resume"]
        code1, _ = self.run_main(argv, deps)
        first = dict(deps.plan)
        deps2 = FakeDeps(self.root)
        deps2.run_id_hint = "test-resume"
        code2, _ = self.run_main(argv, deps2)
        self.assertEqual(code1, 0)
        self.assertEqual(code2, 0)
        self.assertEqual(first["signal"]["id"], deps2.plan["signal"]["id"])

    def test_report_is_sanitized(self):
        payload = {"purpose": "x", "api_key": "k", "nested": {"secret": "s", "ok": 1},
                   "authorization_id": "a", "account_hash": "h", "headers": {"a": "b"}}
        clean = D._sanitize(payload)
        self.assertNotIn("api_key", clean)
        self.assertNotIn("secret", clean["nested"])
        self.assertNotIn("authorization_id", clean)
        self.assertNotIn("account_hash", clean)
        self.assertNotIn("headers", clean)

    def test_no_real_network_during_flow(self):
        def forbidden(*args, **kwargs):
            raise AssertionError("network forbidden")
        deps = FakeDeps(self.root)
        with mock.patch("socket.socket", side_effect=forbidden), \
                mock.patch("urllib.request.urlopen", side_effect=forbidden):
            code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)


class StatusOnly:
    def __init__(self, status):
        self._status = status

    def status(self):
        return dict(self._status)


class FakeRisk:
    def __init__(self):
        self.contexts = []

    def snapshot(self, context):
        self.contexts.append(dict(context))
        return {"demo_verified": True, "ledger_verified": True, "baseline_trusted": True,
                "history_complete": True, "account_snapshot_consistent": True,
                "refusal_reasons": [], "request_faults": [],
                "open_positions": [], "pending_orders": [], "protection_orders": [],
                "entries_today": 1, "realized_pnl_today": "0"}


class R4ProductionRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.deps = D.ProductionDeps()
        self.addCleanup(mock.patch.stopall)
        mock.patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("src.demo_transport.load_demo_credentials",
                   side_effect=AssertionError("NO_CREDENTIALS")).start()

    def local_runtime(self, root=None):
        from src.demo_auto import DemoAutoController
        from src.order_router import OrderRouter
        root = root or self.root
        state = root / "state" / "demo-auto"
        router = OrderRouter(SimpleNamespace(mode="demo"), config={},
                             state_provider=mock.Mock(side_effect=AssertionError("NO_RISK_IO")),
                             market_provider=mock.Mock(side_effect=AssertionError("NO_MARKET_IO")),
                             journal_path=state / "router.sqlite3", log_root=root / "logs",
                             account_key="DEMO-AUTO-SOL-V1")
        controller = DemoAutoController({}, router, None, lambda: {}, root,
                                        enable_demo_tests=True)
        return {"controller": controller, "router": router,
                "risk": mock.Mock(snapshot=mock.Mock(side_effect=AssertionError("NO_RISK_IO")))}

    @staticmethod
    def options(run_id=None):
        return SimpleNamespace(run_id=run_id, scenario="smoke", side="long")

    def registered_rejection(self, runtime, root=None, run_id="failed-smoke"):
        root = root or self.root
        now = int(time.time() * 1000)
        plan = D.build_plan("smoke", D.build_signal(
            "smoke", "long", "100", "100.02", "0.01", "0.01", now))
        path = root / "logs" / "technical-tests" / run_id / "plan.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(plan), encoding="utf-8")
        controller = runtime["controller"]
        with controller._db() as db:
            db.execute("INSERT INTO controller_signals VALUES(?,?,?,?,?)",
                       (plan["signal"]["id"], "REJECTED", "DEMO_STATE_UNVERIFIED", None, now))
            db.execute("INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)",
                       (plan["signal"]["id"], plan["purpose"], plan["scenario"], plan["authorization_id"],
                        controller._test_fingerprint(plan), json.dumps(plan["signal"]), now))
        return plan, path

    def test_idle_preparation_is_local_only_and_does_not_hide_risk_io_failure(self):
        runtime = self.local_runtime()
        with mock.patch.object(self.deps, "risk_view", side_effect=AssertionError("NO_PRECHECK")):
            self.assertEqual(self.deps.prepare_run(runtime, self.root, self.options()), {"mode": "new"})
        runtime["risk"].snapshot.assert_not_called()
        runtime["router"].state_provider.assert_not_called()

    def test_idle_status_cannot_hide_nonterminal_controller_or_router(self):
        for journal in ("controller", "router"):
            with self.subTest(journal=journal):
                root = self.root / journal
                runtime = self.local_runtime(root)
                if journal == "controller":
                    with runtime["controller"]._db() as db:
                        db.execute("INSERT INTO controller_signals VALUES(?,?,?,?,?)",
                                   ("OLD", "UNKNOWN", "UNCERTAIN", None, 1))
                else:
                    with runtime["router"]._db() as db:
                        db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
                                   ("DEMO-AUTO-SOL-V1", "OLD", "fp", "old", "CLOSING", "", "{}"))
                runtime["controller"] = StatusOnly({"status": "IDLE", "signal_id": ""})
                with self.assertRaises(D.TestRefused) as caught:
                    self.deps.prepare_run(runtime, root, self.options())
                self.assertEqual(caught.exception.code, "LOCAL_" + journal.upper() + "_NOT_TERMINAL")

    def test_local_halt_file_meta_and_router_controls_refuse(self):
        for mode in ("file", "meta", "controls", "memory"):
            with self.subTest(mode=mode):
                root = self.root / mode
                runtime = self.local_runtime(root)
                state = root / "state" / "demo-auto"
                if mode == "file":
                    (state / "HALT-existing").write_text("STOP", encoding="utf-8")
                elif mode == "meta":
                    with runtime["controller"]._db() as db:
                        db.execute("INSERT INTO controller_meta VALUES('halt','EXISTING')")
                elif mode == "controls":
                    with runtime["router"]._db() as db:
                        db.execute("INSERT INTO controls VALUES('DEMO-AUTO-SOL-V1','EXISTING')")
                else:
                    runtime["router"]._local_halted = True
                runtime["controller"] = StatusOnly({"status": "IDLE", "signal_id": ""})
                with self.assertRaises(D.TestRefused) as caught:
                    self.deps.prepare_run(runtime, root, self.options())
                self.assertEqual(caught.exception.code, "LOCAL_HALT_PRESENT")

    def test_unreadable_journal_never_falls_back_to_clear(self):
        runtime = self.local_runtime()
        (self.root / "state" / "demo-auto" / "router.sqlite3").write_bytes(b"not sqlite")
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.prepare_run(runtime, self.root, self.options())
        self.assertEqual(caught.exception.code, "LOCAL_JOURNAL_UNREADABLE")

    def test_rejected_same_run_id_is_not_retried_and_original_plan_is_unchanged(self):
        runtime = self.local_runtime()
        _, path = self.registered_rejection(runtime)
        before = path.read_bytes()
        with mock.patch.object(self.deps, "start_test", side_effect=AssertionError("NO_RESUBMIT")) as submit:
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.prepare_run(runtime, self.root, self.options("failed-smoke"))
            self.assertEqual(caught.exception.code, "DEMO_TEST_PLAN_EXISTS")
            submit.assert_not_called()
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.deps.prepare_run(runtime, self.root, self.options("new-smoke")), {"mode": "new"})

    def test_missing_or_changed_registered_plan_refuses(self):
        for kind in ("missing", "changed", "orphan"):
            with self.subTest(kind=kind):
                root = self.root / kind
                runtime = self.local_runtime(root)
                plan, path = self.registered_rejection(runtime, root)
                if kind == "missing":
                    path.unlink()
                elif kind == "changed":
                    plan["signal"]["tp2"] = "104"
                    path.write_text(json.dumps(plan), encoding="utf-8")
                else:
                    with runtime["controller"]._db() as db:
                        db.execute("DELETE FROM controller_signals")
                with self.assertRaises(D.TestRefused) as caught:
                    self.deps.prepare_run(runtime, root, self.options())
                self.assertEqual(caught.exception.code, "LOCAL_REGISTRATION_MISMATCH")

    def test_invalid_plan_file_is_not_silently_ignored(self):
        runtime = self.local_runtime()
        path = self.root / "logs" / "technical-tests" / "bad" / "plan.json"
        path.parent.mkdir(parents=True)
        path.write_text("{bad", encoding="utf-8")
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.prepare_run(runtime, self.root, self.options())
        self.assertEqual(caught.exception.code, "LOCAL_PLAN_INVALID")

    def test_real_closed_and_canceled_router_rows_allow_the_next_run(self):
        from src.demo_auto import DemoAutoController
        from src.demo_route_transport import DemoRouteTransport
        from src.demo_transport import CREDENTIAL_NAMES
        from src.order_router import OrderRouter
        from test_demo_planned_close import PlannedExchange
        from test_execution import inputs, NOW
        for terminal in ("CLOSED", "CANCELED"):
            with self.subTest(terminal=terminal):
                root = self.root / terminal
                _, risk_state, config = inputs()
                risk_state.update(demo_verified=True, observed_at_ms=NOW, account_hash="a" * 64,
                                  week_start_equity="117.72", realized_pnl_week="0", refusal_reasons=[])
                exchange = PlannedExchange()
                exchange.pending_entry = terminal == "CANCELED"
                transport = DemoRouteTransport({name: "fixture" for name in CREDENTIAL_NAMES},
                    enable_writes=True, http_get=exchange.get, http_post=exchange.post, log_root=root / "logs")
                quote = lambda: {"last": "100", "bid": "99.99", "ask": "100.02", "timestamp_ms": NOW}
                provider = SimpleNamespace(snapshot=lambda context: dict(risk_state))
                observed = D.ObservedRiskProvider(provider, self.deps._observe_risk)
                router = OrderRouter(transport, config=config, state_provider=lambda: observed.snapshot({}),
                    market_provider=quote, journal_path=root / "state" / "demo-auto" / "router.sqlite3",
                    log_root=root / "logs", account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64,
                    clock=lambda: NOW)
                controller = DemoAutoController(config, router, observed, quote, root,
                    clock=lambda: NOW, enable_demo_tests=True)
                plan = D.build_plan("smoke", D.build_signal(
                    "smoke", "long", "100", "100.02", "0.01", "0.01", NOW))
                path = root / "logs" / "technical-tests" / "previous" / "plan.json"
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(plan), encoding="utf-8")
                result = controller.process_demo_test(plan, {"generated_at_ms": NOW,
                    "data_fresh": True, "macro_blackout": False})
                self.assertEqual(result["status"], "PENDING" if terminal == "CANCELED" else "PROTECTED")
                self.assertEqual(controller.close_demo_test(plan["signal"]["id"])["status"], terminal)
                runtime = {"controller": controller, "router": router, "risk": observed}
                writes_before = len(exchange.posts)
                self.assertEqual(self.deps.prepare_run(runtime, root, self.options("next")), {"mode": "new"})
                self.assertEqual(len(exchange.posts), writes_before)

    def test_observer_preserves_argument_result_and_exception_identity(self):
        context, payload = object(), {"demo_verified": False}
        provider = mock.Mock(snapshot=mock.Mock(return_value=payload))
        wrapped = D.ObservedRiskProvider(provider, self.deps._observe_risk)
        self.assertIs(wrapped.snapshot(context), payload)
        self.assertIs(provider.snapshot.call_args.args[0], context)
        error = RuntimeError("DO_NOT_SERIALIZE_THIS")
        provider.snapshot.side_effect = error
        wrapped.observer = mock.Mock(side_effect=RuntimeError("observer failure"))
        with self.assertRaises(RuntimeError) as caught:
            wrapped.snapshot(context)
        self.assertIs(caught.exception, error)
        provider.snapshot.side_effect = None
        self.assertIs(wrapped.snapshot(context), payload)

    def test_observer_reports_actual_controller_refusal_without_extra_snapshot(self):
        from src.demo_auto import DemoAutoController
        from test_demo_technical_controller import TechnicalRouter
        now = int(time.time() * 1000)
        config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
        payload = {"demo_verified": False, "baseline_trusted": False, "history_complete": False,
                   "ledger_verified": False, "account_snapshot_consistent": False,
                   "refusal_reasons": ["ACCOUNT_READ_FAILED", "PRIVATE_ACCOUNT_MARKER"],
                   "request_faults": [{"path": "/api/v5/account/positions-history",
                       "code": "DEMO_NETWORK_ERROR", "numeric": None,
                       "source_fault": "DEMO_SOURCE_REQUEST_FAILED", "headers": "SECRET_HEADER"}],
                   "observed_at_ms": now, "collection_completed_ms": now,
                   "request_timings": [{"path": "/api/v5/account/positions-history",
                       "started_at_ms": now - 1, "finished_at_ms": now, "scope": {"uid": "PRIVATE_UID"}}],
                   "account_hash": "PRIVATE_HASH", "raw": "PRIVATE_RESPONSE"}
        provider = mock.Mock(snapshot=mock.Mock(return_value=payload))
        wrapped = D.ObservedRiskProvider(provider, self.deps._observe_risk)
        router = TechnicalRouter()
        controller = DemoAutoController(config, router, wrapped,
            lambda: {"last": "100", "timestamp_ms": now}, self.root,
            clock=lambda: now, enable_demo_tests=True)
        plan = D.build_plan("smoke", D.build_signal("smoke", "long", "100", "100.02", "0.01", "0.01", now))
        result = controller.process_demo_test(plan, {"generated_at_ms": now, "data_fresh": True, "macro_blackout": False})
        self.assertEqual((result["status"], result["code"]), ("REJECTED", "DEMO_STATE_UNVERIFIED"))
        self.assertEqual(provider.snapshot.call_count, 1)
        self.assertEqual(router.calls, [])
        report = {"run_id": "refused", "result": "FAIL", "orders_sent": "UNKNOWN",
                  "trading_state_changed": "UNKNOWN", "refusal": result}
        saved = self.deps.write_report(self.root, report, "FAIL")
        safe = json.loads(Path(saved["report_path"]).read_text(encoding="utf-8"))
        self.assertEqual(provider.snapshot.call_count, 1)
        self.assertEqual(safe["last_failed_risk"]["refusal_codes"], ["ACCOUNT_READ_FAILED", "UNKNOWN_CODE"])
        self.assertEqual(safe["last_failed_risk"]["request_faults"][0]["code"], "DEMO_NETWORK_ERROR")
        self.assertEqual(safe["orders_sent"], "UNKNOWN")
        serialized = json.dumps(safe)
        for marker in ("PRIVATE_ACCOUNT_MARKER", "SECRET_HEADER", "PRIVATE_UID", "PRIVATE_HASH", "PRIVATE_RESPONSE"):
            self.assertNotIn(marker, serialized)

    def test_observation_bound_retains_last_failed_facts(self):
        self.deps._observe_risk({"demo_verified": False, "refusal_reasons": ["ACCOUNT_READ_FAILED"]}, 1, 2)
        for _ in range(20):
            self.deps._observe_risk({"demo_verified": True, "refusal_reasons": [], "request_faults": []}, 3, 4)
        self.assertEqual(len(self.deps._risk_observations), 16)
        self.assertEqual(self.deps._last_failed_risk["refusal_codes"], ["ACCOUNT_READ_FAILED"])

    def test_runtime_wires_one_observed_provider_with_shared_retry_budget(self):
        state = self.root / "state" / "demo-auto"
        state.mkdir(parents=True)
        identity = "a" * 64
        (state / "auto-authorization.json").write_text(json.dumps({"environment": "DEMO",
            "account_hash": identity, "source": "user-approved-demo-activation",
            "scope": "SOL-USDT-SWAP_EXISTING_RISK", "authorization_recorded_at_ms": 1}), encoding="utf-8")
        provider = FakeRisk()
        provider._reserve_get = mock.Mock()
        get = mock.Mock()
        self.deps._http_get = get
        binding_client, risk_client = mock.Mock(), mock.Mock()
        with mock.patch("src.demo_transport.load_demo_credentials", return_value={}), \
             mock.patch("src.demo_transport.DemoReadOnlyClient", side_effect=[binding_client, risk_client]) as client, \
             mock.patch("demo_auto_run.bind_demo_account", return_value=identity) as bind, \
             mock.patch("src.demo_route_transport.DemoRouteTransport") as transport, \
             mock.patch("src.demo_verified_risk.VerifiedDemoRiskProvider", return_value=provider), \
             mock.patch("src.util.load_config", return_value={}), \
             mock.patch("src.order_router.OrderRouter") as router, \
             mock.patch("src.demo_successor_gate.assert_activated_generation"), \
             mock.patch("src.demo_auto.DemoAutoController") as controller:
            runtime = self.deps.build_runtime(self.root)
        self.assertEqual(client.call_count, 2)
        self.assertEqual(client.call_args_list[0].kwargs, {"http_get": get, "network_retries": 0})
        self.assertIs(client.call_args_list[1].kwargs["http_get"], get)
        self.assertIs(transport.call_args.kwargs["http_get"], get)
        self.assertNotIn("http_post", transport.call_args.kwargs)
        self.assertIs(router.call_args.kwargs["require_demo_entry_admission"], True)
        self.assertIs(router.call_args.kwargs["final_entry_check"].__self__, self.deps)
        self.assertEqual(router.call_args.kwargs["final_entry_check"].__name__, "_assert_entry_current")
        self.assertEqual(client.call_args_list[1].kwargs["network_retries"], 1)
        self.assertIs(bind.call_args.args[0], binding_client)
        self.assertIs(runtime["client"], risk_client)
        client.call_args_list[1].kwargs["retry_budget"]("/api/v5/trade/fills-history")
        provider._reserve_get.assert_called_once_with("/api/v5/trade/fills-history")
        self.assertIs(controller.call_args.args[2], runtime["risk"])
        self.assertIs(runtime["risk"].provider, provider)
        context = {"observed_at_ms": 1, "data_fresh": True, "macro_blackout": False}
        with mock.patch.object(self.deps, "_fresh_risk_context", return_value=context):
            router.call_args.kwargs["state_provider"]()
            self.deps.risk_view(runtime)
        self.assertEqual(provider.contexts, [context, context])

    def test_retry_event_whitelist_does_not_echo_unknown_event_fields(self):
        event = {"path": "/api/v5/account/positions-history", "code": "DEMO_NETWORK_RETRY",
                 "attempt": 2, "type": "TimeoutError", "headers": "PRIVATE_HEADER"}
        self.deps._runtime = {"client": SimpleNamespace(retry_events=lambda: [event,
            {**event, "path": "https://private.example/uid"}, {**event, "type": "PRIVATE_ERROR"}])}
        result = self.deps._retry_diagnostics()
        self.assertEqual(result, {"network_retries": 1, "budget": "verified-provider-same-path",
            "binding_network_retries": 0, "route_read_network_retries": 0, "events": [
            {key: event[key] for key in ("path", "code", "attempt", "type")}]})
        self.assertNotIn("PRIVATE", json.dumps(result))

    def _runtime_with_real_risk_client(self, io, *, provider_factory=None):
        from src.demo_transport import CREDENTIAL_NAMES, DemoReadOnlyClient
        state = self.root / "state" / "demo-auto"
        state.mkdir(parents=True, exist_ok=True)
        (state / "auto-authorization.json").write_text(json.dumps({"environment": "DEMO",
            "account_hash": "a" * 64, "source": "user-approved-demo-activation",
            "scope": "SOL-USDT-SWAP_EXISTING_RISK", "authorization_recorded_at_ms": 1}), encoding="utf-8")
        credentials = {name: "fixture" for name in CREDENTIAL_NAMES}
        client_options = []

        def client_factory(creds, **options):
            client_options.append(options)
            return DemoReadOnlyClient(creds, **{**options, "http_get": options.get("http_get") or io})

        with mock.patch("src.demo_transport.load_demo_credentials", return_value=credentials), \
             mock.patch("src.demo_transport.DemoReadOnlyClient", side_effect=client_factory), \
             mock.patch("demo_auto_run.bind_demo_account", return_value="a" * 64), \
             mock.patch("src.demo_route_transport.DemoRouteTransport"), \
             mock.patch("src.util.load_config", return_value={}), \
             mock.patch("src.order_router.OrderRouter"), \
             mock.patch("src.demo_auto.DemoAutoController"), \
             mock.patch("src.demo_successor_gate.assert_activated_generation"), \
             contextlib.ExitStack() as stack:
            if provider_factory is not None:
                stack.enter_context(mock.patch("src.demo_verified_risk.VerifiedDemoRiskProvider",
                                                side_effect=provider_factory))
            runtime = self.deps.build_runtime(self.root)
        return runtime, client_options

    def test_risk_retry_reserves_each_physical_get_in_the_real_provider_budget(self):
        import ssl
        import urllib.error
        events = []

        def io(url, headers, timeout):
            events.append("GET")
            if events.count("GET") == 1:
                raise urllib.error.URLError(ssl.SSLEOFError("fixture transient EOF"))
            return 200, {"code": "0", "data": []}

        runtime, options = self._runtime_with_real_risk_client(io)
        provider = runtime["risk"].provider
        original_reserve = provider._reserve_get
        path = "/api/v5/trade/fills-history"

        def reserve(source):
            events.append("BUDGET")
            return original_reserve(source)

        with mock.patch.object(provider, "_reserve_get", side_effect=reserve), \
             mock.patch("src.demo_transport.time.sleep"):
            self.assertEqual(provider._get(path), [])
        self.assertEqual(events, ["BUDGET", "GET", "BUDGET", "GET"])
        self.assertEqual(len(provider._rate_windows[path]), 2)
        self.assertEqual(options[0], {"http_get": None, "network_retries": 0})
        self.assertEqual(self.deps._retry_diagnostics()["events"], [{"path": path,
            "code": "DEMO_NETWORK_RETRY", "attempt": 2, "type": "SSLEOFError"}])

    def test_retry_budget_closure_refuses_before_provider_is_ready(self):
        from src.demo_transport import DemoTransportError
        import ssl
        import urllib.error
        calls = []

        def io(url, headers, timeout):
            calls.append(url)
            raise urllib.error.URLError(ssl.SSLEOFError("PRIVATE_REASON"))

        def provider_factory(client, *args, **kwargs):
            # Construction must have no IO. If any future constructor violates
            # that contract, an unready callback still prevents the second GET.
            client.get("/api/v5/trade/fills-history")
            self.fail("unready retry budget must refuse")

        with mock.patch("src.demo_transport.time.sleep"), self.assertRaises(DemoTransportError) as caught:
            self._runtime_with_real_risk_client(io, provider_factory=provider_factory)
        self.assertEqual(caught.exception.code, "DEMO_NETWORK_RETRY_BUDGET_FAILED")
        self.assertEqual(len(calls), 1)
        self.assertNotIn("PRIVATE_REASON", str(caught.exception))

    def test_recovered_get_still_refuses_stale_account_snapshot(self):
        from src.demo_auto import DemoAutoController
        from src.demo_transport import CREDENTIAL_NAMES, DemoReadOnlyClient
        from src.demo_verified_risk import VerifiedDemoRiskProvider
        from test_demo_technical_controller import TechnicalRouter
        from tests.test_demo_verified_risk import HistoricalClient, NonContendingFixtureQuotaClock
        from tests.test_demo_ledger import ACCOUNT, MONDAY, NOW, evidence
        from tests.test_demo_risk import CONFIG
        from urllib.parse import parse_qs, urlsplit
        import ssl
        import threading
        import urllib.error
        for stage, elapsed, expected in (("history-gap", 6001, "ACCOUNT_STALE"),
                                         ("history", 10001, "ACCOUNT_SNAPSHOT_UNSTABLE")):
            with self.subTest(stage=stage):
                root = self.root / stage
                root.mkdir()
                data = evidence()
                historical = HistoricalClient(NOW, data)
                journal = root / "router.sqlite3"
                with sqlite3.connect(journal) as db:
                    db.execute("CREATE TABLE routes(account TEXT,signal TEXT,status TEXT,client TEXT,data TEXT)")
                    db.execute("CREATE TABLE controls(account TEXT,code TEXT)")
                    for route in data["routes"]:
                        db.execute("INSERT INTO routes VALUES(?,?,?,?,?)", ("DEMO-AUTO-SOL-V1",
                            route["signal_id"], route["status"], route["client_order_id"], json.dumps(route)))
                now, time_calls, retried_url, recovered, calls = [NOW], [0], [None], [False], []
                lock = threading.Lock()
                holder = {}

                def io(url, headers, timeout):
                    parsed = urlsplit(url)
                    params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
                    with lock:
                        calls.append(url)
                        if (parsed.path == "/api/v5/trade/fills-history"
                                and holder["provider"]._stage == stage):
                            if retried_url[0] is None:
                                retried_url[0] = url
                                raise urllib.error.URLError(ssl.SSLEOFError("fixture transient EOF"))
                            if url == retried_url[0] and not recovered[0]:
                                recovered[0] = True
                                now[0] = NOW + elapsed
                        if parsed.path == "/api/v5/public/time":
                            time_calls[0] += 1
                            if time_calls[0] > 1:
                                now[0] = max(now[0], NOW + 1)
                            historical.now = now[0]
                    return 200, {"code": "0", "data": historical.get(parsed.path, params)}

                client = DemoReadOnlyClient({name: "fixture" for name in CREDENTIAL_NAMES},
                    http_get=io, network_retries=1,
                    retry_budget=lambda path: holder["provider"]._reserve_get(path))
                provider = VerifiedDemoRiskProvider(client, CONFIG, root / "risk",
                    authorization={"environment": "DEMO", "account_hash": ACCOUNT,
                        "authorization_recorded_at_ms": MONDAY - 4 * 3_600_000,
                        "source": "user-approved-demo-activation", "scope": "SOL-USDT-SWAP_EXISTING_RISK"},
                    router_journal_path=journal, clock=lambda: now[0],
                    rate_clock=NonContendingFixtureQuotaClock())
                holder["provider"] = provider
                observed_provider = D.ObservedRiskProvider(provider, self.deps._observe_risk)
                router = TechnicalRouter()
                controller = DemoAutoController(CONFIG, router, observed_provider,
                    lambda: {"last": "100", "timestamp_ms": now[0]}, root,
                    clock=lambda: now[0], enable_demo_tests=True)
                plan = D.build_plan("smoke", D.build_signal("smoke", "long", "100", "100.02", "0.01", "0.01", NOW))
                with mock.patch("src.demo_transport.time.sleep"):
                    result = controller.process_demo_test(plan, {"generated_at_ms": NOW,
                        "data_fresh": True, "macro_blackout": False})
                self.assertTrue(recovered[0])
                self.assertEqual(calls.count(retried_url[0]), 2)
                self.assertEqual((result["status"], result["code"]), ("REJECTED", "DEMO_STATE_UNVERIFIED"))
                self.assertEqual(router.calls, [])
                observed = self.deps._last_failed_risk
                self.assertIn(expected, observed["refusal_codes"])
                self.assertIs(observed["demo_verified"], False)
                self.assertEqual(observed["request_faults"], [])
                self.assertEqual(client.retry_events()[0]["type"], "SSLEOFError")

    def _real_incremental_market(self, mode="healthy"):
        from src import collect as C
        from src import util as U
        import demo_auto_run as run
        import threading
        start = 1_800_000_000_000
        clock = [start]
        ticker_done, calendar_done = threading.Event(), threading.Event()
        fetched = {"ticker": 0, "economic_calendar": 0, "slow_aux": 0}
        calls, results = [], []
        config = {
            "symbols": [{"instId": "SOL-USDT-SWAP", "tickSz": "0.01", "minSz": "0.01"}],
            "data": {"critical_sources": ["ticker", "economic_calendar"],
                     "stale_seconds": {"ticker": 60, "economic_calendar": 1800, "slow_aux": 60}},
            "macro": {"blackout_minutes_before": 30, "blackout_minutes_after": 30,
                      "block_impact": ["High"], "block_currencies": ["USD"]},
        }

        def env(name, source, fetched_at, data, *, ok=True):
            return {"dataset": name, "source_ts": source, "source_utc": U.ms_to_iso(source),
                    "fetched_at_ms": fetched_at, "ok": ok, "error": None if ok else "OFFLINE_FAILURE",
                    "data": data}

        def ticker():
            fetched["ticker"] += 1
            first = fetched["ticker"] == 1
            if not first and mode == "still_slow":
                clock[0] += 5500
            stamp = start + 700 if first else clock[0]
            tick = {"last": "100.01", "bid": "100", "ask": "100.02", "chg24h_pct": 0,
                    "high24h": 101, "low24h": 99, "vol24h_base": 1000}
            ticker_done.set()
            return env("ticker", stamp - 100 if first else stamp, stamp,
                       tick if mode != "bad_health" or first else None,
                       ok=mode != "bad_health" or first)

        def calendar():
            fetched["economic_calendar"] += 1
            source = start - (1_900_000 if mode == "old_calendar" else 60_000)
            fetched_at = clock[0]
            calendar_done.set()
            return env("economic_calendar", source, fetched_at, [])

        def slow_aux():
            self.assertTrue(ticker_done.wait(2))
            self.assertTrue(calendar_done.wait(2))
            fetched["slow_aux"] += 1
            clock[0] = start + 5500
            return env("slow_aux", clock[0], clock[0], {"fixture": "original"})

        def tasks(cfg, **options):
            self.assertEqual(options, {"calendar_max_age_s": 1500})
            return [("ticker", 10, ticker), ("economic_calendar", 1500, calendar),
                    ("slow_aux", 60, slow_aux)]

        def collect(cfg, previous, **options):
            calls.append(previous)
            snapshot = C.collect(cfg, previous, **options)
            results.append(snapshot)
            return snapshot

        self.deps._config = config
        self.deps._collect = collect
        self.deps._market_context = run.market_context
        (self.root / "state" / "demo-auto").mkdir(parents=True, exist_ok=True)
        with mock.patch.object(C, "build_tasks", side_effect=tasks), \
             mock.patch.object(C, "now_ms", side_effect=lambda: clock[0]), \
             mock.patch.object(C, "server_now_ms", side_effect=lambda: clock[0]), \
             mock.patch.object(U, "server_now_ms", side_effect=lambda: clock[0]), \
             mock.patch.object(C, "sync_server_time", return_value={"drift_ok": True}) as sync, \
             mock.patch.object(C, "load_thresholds", return_value={}), \
             mock.patch.object(run, "_now", side_effect=lambda: clock[0]), \
             mock.patch.object(D.time, "time", side_effect=lambda: clock[0] / 1000):
            snapshot = self.deps.collect_snapshot(self.root)
            try:
                fields, error = self.deps.market_fields(snapshot), None
            except D.TestRefused as refused:
                fields, error = None, refused.code
            sync_calls = sync.call_count
        return {"snapshot": snapshot, "cold": results[0], "calls": calls, "fetched": fetched,
                "fields": fields, "error": error, "clock": clock[0], "sync_calls": sync_calls}

    def test_slow_cold_collection_uses_real_bounded_incremental_refresh(self):
        result = self._real_incremental_market()
        self.assertEqual(result["error"], None)
        self.assertEqual(len(result["calls"]), 2)
        self.assertIsNone(result["calls"][0])
        previous = result["calls"][1]
        self.assertIsNotNone(previous)
        self.assertNotIn("ticker", previous["sources"])
        self.assertIn("ticker", result["cold"]["sources"])
        for key in ("slow_aux", "economic_calendar"):
            self.assertEqual(previous["sources"][key], result["cold"]["sources"][key])
            self.assertIsNot(previous["sources"][key], result["cold"]["sources"][key])
            self.assertIs(result["snapshot"]["sources"][key], previous["sources"][key])
        self.assertEqual(result["fetched"], {"ticker": 2, "economic_calendar": 1, "slow_aux": 1})
        self.assertEqual(result["sync_calls"], 1)
        self.assertTrue(result["snapshot"]["health"]["usable"])
        self.assertEqual(result["snapshot"]["reused_sources"], 2)
        self.assertEqual(result["cold"]["generated_at_ms"], 1_800_000_000_000)
        self.assertEqual(result["snapshot"]["generated_at_ms"], result["clock"])
        calendar = result["snapshot"]["sources"]["economic_calendar"]
        self.assertEqual(calendar["source_ts"], 1_800_000_000_000 - 60_000)
        self.assertEqual(calendar["source_utc"], result["cold"]["sources"]["economic_calendar"]["source_utc"])
        saved = json.loads((self.root / "state" / "demo-auto" / "market.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["sources"]["economic_calendar"], calendar)

    def test_incremental_refresh_does_not_promote_bad_health(self):
        result = self._real_incremental_market("bad_health")
        self.assertEqual(result["error"], "MARKET_EVIDENCE_UNVERIFIED")
        self.assertFalse(result["snapshot"]["health"]["usable"])
        self.assertEqual(result["snapshot"]["health"]["critical_missing"], ["ticker"])
        self.assertEqual(len(result["calls"]), 2)

    def test_incremental_refresh_does_not_retimestamp_old_calendar(self):
        result = self._real_incremental_market("old_calendar")
        self.assertEqual(result["error"], "MARKET_EVIDENCE_UNVERIFIED")
        original = result["cold"]["sources"]["economic_calendar"]
        current = result["snapshot"]["sources"]["economic_calendar"]
        self.assertEqual(current["source_ts"], original["source_ts"])
        self.assertEqual(current["source_utc"], original["source_utc"])
        self.assertGreater(current["fetched_at_ms"], original["fetched_at_ms"])
        self.assertEqual(result["fetched"]["economic_calendar"], 2)
        self.assertFalse(result["snapshot"]["health"]["usable"])
        self.assertIsNone(self.deps._calendar)

    def test_incremental_refresh_remains_bounded_when_it_is_still_slow(self):
        result = self._real_incremental_market("still_slow")
        self.assertEqual(result["error"], "MARKET_CONTEXT_STALE")
        self.assertEqual(len(result["calls"]), 2)
        self.assertEqual(result["fetched"]["ticker"], 2)
        self.assertGreater(result["clock"] - result["snapshot"]["generated_at_ms"], 5000)
        self.assertEqual(result["clock"], result["snapshot"]["sources"]["ticker"]["fetched_at_ms"])

    def test_missing_market_clock_triggers_only_one_incremental_collection(self):
        now = int(time.time() * 1000)
        for field, value in (("generated", None), ("generated", "invalid"),
                             ("fetched", None), ("fetched", "invalid")):
            with self.subTest(field=field, value=value):
                original = {"generated_at_ms": now,
                    "sources": {"ticker": {"fetched_at_ms": now},
                                "untouched": {"source_ts": now - 1000, "fetched_at_ms": now - 500}}}
                if field == "generated":
                    original["generated_at_ms"] = value
                else:
                    original["sources"]["ticker"]["fetched_at_ms"] = value
                refreshed = {"generated_at_ms": now, "sources": {"ticker": {"fetched_at_ms": now}}}
                self.deps._collect = mock.Mock(side_effect=[original, refreshed])
                with mock.patch.object(D.time, "time", return_value=now / 1000):
                    self.assertIs(self.deps.collect_snapshot(self.root), refreshed)
                self.assertEqual(self.deps._collect.call_count, 2)
                previous = self.deps._collect.call_args_list[1].args[1]
                self.assertNotIn("ticker", previous["sources"])
                self.assertEqual(previous["sources"]["untouched"], original["sources"]["untouched"])
                self.assertIsNot(previous["sources"]["untouched"], original["sources"]["untouched"])

    def test_future_market_clock_is_rejected_without_refresh(self):
        now = int(time.time() * 1000)
        for field, code in (("generated", "MARKET_CONTEXT_STALE"), ("fetched", "TICKER_NOT_FRESH")):
            with self.subTest(field=field):
                snapshot = {"generated_at_ms": now, "sources": {"ticker": {"fetched_at_ms": now}}}
                if field == "generated":
                    snapshot["generated_at_ms"] = now + 1
                else:
                    snapshot["sources"]["ticker"]["fetched_at_ms"] = now + 1
                self.deps._collect = mock.Mock(return_value=snapshot)
                with mock.patch.object(D.time, "time", return_value=now / 1000), \
                     self.assertRaises(D.TestRefused) as caught:
                    self.deps.collect_snapshot(self.root)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(self.deps._collect.call_count, 1)

    def test_actual_quote_uses_one_get_and_keeps_source_bid_ask_and_time(self):
        now = 1_800_000_000_000
        row = {"instId": "SOL-USDT-SWAP", "last": "100.5", "bidPx": "100.4",
               "askPx": "100.6", "ts": str(now - 4999)}
        io = mock.Mock(return_value=(200, {"code": "0", "data": [row]}))
        self._runtime_with_real_risk_client(io)
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        with mock.patch("demo_auto_run._now", return_value=now):
            result = self.deps._quote()
        self.assertEqual(result, {"last": "100.5", "bid": "100.4", "ask": "100.6",
                                  "timestamp_ms": now - 4999})
        self.assertEqual(io.call_count, 1)
        self.assertIn("/api/v5/market/ticker?instId=SOL-USDT-SWAP", io.call_args.args[0])
        self.assertEqual(row["ts"], str(now - 4999))

    def test_actual_quote_rejects_bad_missing_crossed_and_stale_data_before_routing(self):
        now = 1_800_000_000_000
        row = {"instId": "SOL-USDT-SWAP", "last": "100.5", "bidPx": "100.4",
               "askPx": "100.6", "ts": str(now)}
        cases = [("empty", []), ("multiple", [row, row]), ("not-row", [None]),
                 ("wrong-instrument", [{**row, "instId": "BTC-USDT-SWAP"}])]
        for field in ("last", "bidPx", "askPx"):
            for invalid in (None, "NaN", "Infinity", "-Infinity", "0", "-1", True, {}):
                cases.append((field + "-" + str(invalid), [{**row, field: invalid}]))
            cases.append(("missing-" + field, [{key: value for key, value in row.items() if key != field}]))
        cases.extend([("crossed", [{**row, "bidPx": "100.7"}]),
                      ("old-time", [{**row, "ts": str(now - 5001)}]),
                      ("future-time", [{**row, "ts": str(now + 1)}])])
        for invalid in (None, "", "1e3", "invalid", 0, -1, True, float(now), {}):
            cases.append(("time-" + str(invalid), [{**row, "ts": invalid}]))
        for name, payload in cases:
            with self.subTest(case=name):
                io = mock.Mock(return_value=(200, {"code": "0", "data": payload}))
                runtime, _ = self._runtime_with_real_risk_client(io)
                self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
                with mock.patch("demo_auto_run._now", return_value=now), \
                     self.assertRaises(D.TestRefused) as caught:
                    self.deps._quote()
                self.assertEqual(caught.exception.code, "DEMO_MARKET_UNAVAILABLE")
                self.assertEqual(io.call_count, 1)
                runtime["router"].execute.assert_not_called()
                self.assertEqual(runtime["client"].retry_events(), [])

    def test_actual_quote_accepts_locked_book_and_exact_freshness_boundary(self):
        now = 1_800_000_000_000
        io = mock.Mock(return_value=(200, {"code": "0", "data": [
            {"instId": "SOL-USDT-SWAP", "last": "99.9", "bidPx": "100", "askPx": "100",
             "ts": str(now - 5000)}]}))
        self._runtime_with_real_risk_client(io)
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        with mock.patch("demo_auto_run._now", return_value=now):
            self.assertEqual(self.deps._quote(), {"last": "99.9", "bid": "100", "ask": "100",
                                                 "timestamp_ms": now - 5000})
        self.assertEqual(io.call_count, 1)

    def test_actual_quote_reuses_service_check_despite_negative_collection_offset(self):
        import demo_auto_run as service
        from src import util
        now = 1_800_000_000_000
        row = {"instId": "SOL-USDT-SWAP", "last": "100.5", "bidPx": "100.4",
               "askPx": "100.6", "ts": str(now - 20)}
        io = mock.Mock(return_value=(200, {"code": "0", "data": [row]}))
        with mock.patch.object(service, "service_quote", wraps=service.service_quote) as shared:
            runtime, _ = self._runtime_with_real_risk_client(io)
            self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
            with mock.patch.object(util, "_server_offset_ms", -120), \
                 mock.patch.object(util, "now_ms", return_value=now), \
                 mock.patch.object(service, "_now", return_value=now):
                self.assertEqual(util.server_now_ms(), now - 120)
                result = self.deps._quote()
            shared.assert_called_once_with(runtime["client"])
        self.assertEqual(result, {"last": "100.5", "bid": "100.4", "ask": "100.6",
                                  "timestamp_ms": now - 20})
        self.assertEqual(row["ts"], str(now - 20))
        self.assertEqual(io.call_count, 1)
        runtime["router"].execute.assert_not_called()

    def test_actual_quote_rejects_local_clock_rollback_and_noninteger_clock_before_routing(self):
        now = 1_800_000_000_000
        row = {"instId": "SOL-USDT-SWAP", "last": "100", "bidPx": "99",
               "askPx": "101", "ts": str(now - 1)}
        io = mock.Mock(return_value=(200, {"code": "0", "data": [row]}))
        runtime, _ = self._runtime_with_real_risk_client(io)
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        for clocks in ([now, now - 1], [True, now], [now, True], [None, now], [now, str(now)]):
            with self.subTest(clocks=clocks), \
                 mock.patch("demo_auto_run._now", side_effect=clocks), \
                 self.assertRaises(D.TestRefused) as caught:
                self.deps._quote()
            self.assertEqual(caught.exception.code, "DEMO_MARKET_UNAVAILABLE")
        self.assertEqual(io.call_count, 5)
        runtime["router"].execute.assert_not_called()
        self.assertEqual(row["ts"], str(now - 1))

    def test_standard_restart_checks_router_halt_before_active_recovery_or_cleanup(self):
        from src.demo_route_transport import DemoRouteTransport
        from src.demo_transport import CREDENTIAL_NAMES
        from src.execution import build_intent
        from src.order_router import client_order_id
        from test_demo_entry_admission import PassiveExchange
        from test_execution import inputs, NOW
        for mode, expected in (("file", "LOCAL_HALT_PRESENT"), ("controls", "LOCAL_HALT_PRESENT"),
                               ("unreadable", "LOCAL_JOURNAL_UNREADABLE")):
            with self.subTest(mode=mode):
                root = self.root / mode
                previous = self.local_runtime(root)
                _, state, config = inputs()
                plan = D.build_plan("smoke", D.build_signal("smoke", "long", "100", "100.02", "0.01", "0.01", NOW))
                intent = build_intent(plan["signal"], state, config)
                previous["controller"]._claim(intent.signal_id)
                previous["controller"]._save(intent.signal_id, "SUBMITTING", "SUBMITTING", intent)
                previous["router"].account_identity = "a" * 64
                row, _ = previous["router"]._claim(intent, previous["router"]._intent_fingerprint(intent),
                                                  client_order_id(intent.signal_id))
                row.update(status="SUBMITTING", submit_attempted=True)
                previous["router"]._save(row)
                previous["router"]._release_owner(intent)
                plan_path = root / "logs" / "technical-tests" / "previous-run" / "plan.json"
                plan_path.parent.mkdir(parents=True)
                plan_path.write_text(json.dumps(plan), encoding="utf-8")
                original_plan = plan_path.read_bytes()
                if mode == "file":
                    previous["router"].halt_path.write_text("ENTRY_REFUSAL_PERSIST_FAILED\n", encoding="utf-8")
                elif mode == "controls":
                    with previous["router"]._db() as db:
                        db.execute("INSERT INTO controls VALUES('DEMO-AUTO-SOL-V1','ENTRY_REFUSAL_PERSIST_FAILED')")
                    self.assertFalse(previous["router"].halt_path.exists())
                # New instances have neither the old in-process proof nor a
                # controller HALT. Durable controller/router rows remain SUBMITTING.
                runtime = self.local_runtime(root)
                runtime["router"].account_identity = "a" * 64
                self.assertEqual(runtime["router"]._entry_not_sent_fingerprints, {})
                self.assertFalse(runtime["router"]._local_halted)
                self.assertEqual(runtime["controller"].status()["status"], "SUBMITTING")
                self.assertFalse(runtime["controller"].halt_path.exists())
                if mode == "unreadable":
                    runtime["router"].journal_path.write_bytes(b"not sqlite")
                exchange = PassiveExchange()
                runtime["router"].transport = DemoRouteTransport(
                    {name: "fixture" for name in CREDENTIAL_NAMES}, enable_writes=True,
                    http_get=exchange.get, http_post=exchange.post, log_root=root / "logs")
                production = self.deps

                class RestartDeps(FakeDeps):
                    def build_runtime(self, new_root):
                        return runtime

                    def prepare_run(self, current, new_root, options):
                        return production.prepare_run(current, new_root, options)

                    def safety_halt(self, current, code):
                        self.calls.append(("safety_halt", code))
                        return production.safety_halt(current, code)

                deps = RestartDeps(root)
                options = SimpleNamespace(scenario="smoke", side="long", run_id="previous-run",
                                          new_root=root, predecessor_root=root / "old")
                with mock.patch.object(D, "validate_plan", side_effect=AssertionError("NO_PLAN_READ_BEFORE_HALT")) as validate:
                    saved = D.execute_test(deps, options)
                report = json.loads(Path(saved["report_path"]).read_text(encoding="utf-8"))
                self.assertEqual(report["refusal"]["code"], expected)
                self.assertFalse(report["submit_attempted"])
                self.assertFalse(any(call[0] == "safety_halt" for call in deps.calls))
                self.assertEqual(exchange.posts, [])
                self.assertEqual(exchange.get_calls, 0)
                validate.assert_not_called()
                self.assertEqual(plan_path.read_bytes(), original_plan)
                self.assertEqual(runtime["controller"].status()["status"], "SUBMITTING")

    def test_controller_halt_semantics_and_stop_only_active_recovery_are_preserved(self):
        runtime = self.local_runtime()
        with runtime["controller"]._db() as db:
            db.execute("INSERT INTO controller_meta VALUES('halt','EXISTING_CONTROLLER_HALT')")
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.prepare_run(runtime, self.root, self.options())
        self.assertEqual(caught.exception.code, "CONTROLLER_HALTED")
        # STOP_NEW_ENTRIES prevents a new plan; it is not a router HALT and
        # this local guard must still allow existing-position recovery.
        root = self.root / "active-stop"
        runtime = self.local_runtime(root)
        now = int(time.time() * 1000)
        plan = D.build_plan("smoke", D.build_signal("smoke", "long", "100", "100.02", "0.01", "0.01", now))
        runtime["controller"] = StatusOnly({"status": "PENDING", "signal_id": plan["signal"]["id"]})
        path = root / "logs" / "technical-tests" / "active" / "plan.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(plan), encoding="utf-8")
        (root / "state" / "demo-auto" / "STOP_NEW_ENTRIES").write_text("STOP_NEW_ONLY", encoding="utf-8")
        with mock.patch.object(self.deps, "_registration_matches", return_value=True):
            self.assertEqual(self.deps.prepare_run(runtime, root, self.options())["mode"], "resume")


def _make_archive(root: Path, commit: str, files: dict):
    root.mkdir(parents=True, exist_ok=True)
    payload = dict(files)
    for name in ("scripts/demo_auto_run.py", "scripts/demo_auto_supervisor.py",
                 "scripts/demo_technical_test.py", "config.json",
                 "src/order_router.py", "src/demo_route_transport.py",
                 "src/demo_auto.py", "src/base.py"):
        payload.setdefault(name, "# fixture\n")
    manifest = {"schema": 1, "environment": "DEMO", "commit": commit, "files": {}}
    import hashlib
    for name, content in payload.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        manifest["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (root / "DEPLOYED_COMMIT").write_text(commit, encoding="utf-8")
    (root / "DEPLOYMENT_MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


class EntryQuoteDiagnosticTests(unittest.TestCase):
    """Numeric-only observations never become entry evidence or extra GETs."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "state/demo-auto").mkdir(parents=True)
        self.deps = D.ProductionDeps()
        self.deps._root = self.root
        self.plan = D.build_plan("smoke", D.build_signal(
            "smoke", "short", "99.99", "100.02", "0.01", "0.01", 10_000))
        self.fields = {"bid": "99.99", "ask": "100.02", "last": "100", "tick_size": "0.01",
                       "min_sz": "0.01", "evidence": {"timestamp_ms": 9999,
                           "generated_at_ms": 10_000, "data_fresh": True, "macro_blackout": False},
                       "snapshot": {"generated_at_ms": 10_000, "sources": {
                           "ticker": {"source_ts": 9990, "fetched_at_ms": 9999}}}}
        self.now = 10_000
        self.native = {"bid": "99.99", "ask": "100.02", "last": "100", "timestamp_ms": 9998}
        self.deps._quote = mock.Mock(side_effect=lambda: dict(self.native))
        self.deps._entry_evidence = SimpleNamespace(assert_entry_current=lambda now: {
            "valid_until_ms": 60_000, "code": "MACRO_BLACKOUT"})

    def create_plan(self, run_id="native"):
        self.deps.run_id_hint = run_id
        with mock.patch.object(D.time, "time_ns", side_effect=lambda: self.now * 1_000_000):
            return self.deps.resume_or_create_plan(self.root, "smoke", "short", self.fields, {})

    def assert_no_plan_files(self, run_id="native"):
        folder = self.root / "logs/technical-tests" / run_id
        self.assertFalse((folder / "plan.json").exists())
        self.assertFalse((folder / "plan.tmp").exists())

    def test_quote_observer_preserves_return_exception_and_one_call(self):
        quote = {"bid": "99.99", "ask": "100.02", "last": "100", "timestamp_ms": 9990}
        provider = mock.Mock(return_value=quote)
        observer = mock.Mock(side_effect=RuntimeError("PRIVATE_OBSERVER_FAILURE"))
        wrapped = D.ObservedQuoteProvider(provider, observer)
        self.assertIs(wrapped(), quote)
        self.assertEqual(provider.call_count, 1)
        failure = RuntimeError("PRIVATE_QUOTE_FAILURE")
        provider.side_effect = failure
        with self.assertRaises(RuntimeError) as caught:
            wrapped()
        self.assertIs(caught.exception, failure)
        self.assertEqual(provider.call_count, 2)
        self.assertIs(observer.call_args.args[0], None)

    def test_plan_diagnostics_do_not_change_saved_plan_or_fingerprint(self):
        self.deps.run_id_hint = "initial"
        fields_before = json.dumps(self.fields, sort_keys=True)
        with mock.patch.object(D.time, "time_ns", return_value=10_000_000_000):
            result = self.deps.resume_or_create_plan(self.root, "smoke", "short", self.fields, {})
        path = self.root / "logs/technical-tests/initial/plan.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(result, original)
        self.assertEqual(json.dumps(self.fields, sort_keys=True), fields_before)
        self.assertEqual(self.deps._initial_plan_quote, {"plan_side": "short", "plan_entry": "100.02",
            "plan_generated_at_ms": 10_000, "bid": "99.99", "ask": "100.02", "last": "100",
            "quote_source_timestamp_ms": 9998, "quote_fetched_at_ms": 10_000,
            "quote_source": "DEMO_SERVICE_QUOTE", "scan_source_timestamp_ms": 9990,
            "scan_fetched_at_ms": 9999, "scan_generated_at_ms": 10_000})
        self.assertEqual(self.deps._quote.call_count, 1)
        with mock.patch.object(self.deps, "_observe_plan_quote", side_effect=ValueError("PRIVATE")):
            self.assertEqual(self.deps.resume_or_create_plan(self.root, "smoke", "short", self.fields, {}), original)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), original)
        self.assertEqual(self.deps._quote.call_count, 1)

    def test_new_plan_uses_one_native_quote_and_keeps_original_scan_times(self):
        before = json.dumps(self.fields, sort_keys=True)
        self.native.update(bid="100.05", ask="100.06", last="100.05")
        plan = self.create_plan()
        self.assertEqual(plan["signal"]["entry"], "100.06")
        self.assertEqual(plan["signal"]["qty"], "0.01")
        self.assertEqual(self.deps._quote.call_count, 1)
        self.assertEqual(json.dumps(self.fields, sort_keys=True), before)
        self.assertEqual(self.deps._initial_plan_quote["quote_source_timestamp_ms"], 9998)
        self.assertEqual(self.deps._initial_plan_quote["scan_source_timestamp_ms"], 9990)
        self.assertEqual(self.deps._initial_plan_quote["scan_generated_at_ms"], 10_000)

    def test_original_public_source_budget_is_not_replaced_by_native_five_seconds(self):
        self.fields["snapshot"]["sources"]["ticker"]["source_ts"] = 4000
        plan = self.create_plan()
        self.assertEqual(plan["signal"]["entry"], "100.02")
        self.assertEqual(self.deps._initial_plan_quote["scan_source_timestamp_ms"], 4000)
        self.assertEqual(self.deps._initial_plan_quote["quote_source_timestamp_ms"], 9998)

    def test_bad_original_scan_times_refuse_before_native_get(self):
        original = json.loads(json.dumps(self.fields))
        paths = (("evidence", "generated_at_ms"), ("evidence", "timestamp_ms"),
                 ("snapshot", "generated_at_ms"), ("snapshot", "sources", "ticker", "fetched_at_ms"))
        for index, path in enumerate(paths):
            self.fields = json.loads(json.dumps(original))
            target = self.fields
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = self.now + 1
            self.deps._quote.reset_mock()
            run = "scan" + str(index)
            with self.subTest(path=path), self.assertRaises(D.TestRefused) as caught:
                self.create_plan(run)
            self.assertEqual(caught.exception.code, "PLAN_MARKET_CONTEXT_STALE")
            self.deps._quote.assert_not_called()
            self.assert_no_plan_files(run)

    def test_new_plan_diagnostic_failure_keeps_native_plan_and_exact_saved_bytes(self):
        self.native["ask"] = "100.06"
        with mock.patch.object(self.deps, "_observe_plan_quote", side_effect=ValueError("PRIVATE_DIAGNOSTIC")):
            plan = self.create_plan()
        raw = (self.root / "logs/technical-tests/native/plan.json").read_bytes()
        self.assertEqual(json.loads(raw), plan)
        self.assertEqual(plan["signal"]["entry"], "100.06")
        self.assertIsNone(self.deps._initial_plan_quote)
        self.assertNotIn(b"PRIVATE", raw)

    def test_existing_plan_does_not_read_clock_quote_or_rewrite_bytes(self):
        folder = self.root / "logs/technical-tests/old"
        folder.mkdir(parents=True)
        raw = json.dumps(self.plan, ensure_ascii=False, indent=3).encode()
        path = folder / "plan.json"; path.write_bytes(raw)
        self.deps.run_id_hint = "old"
        with mock.patch.object(self.deps, "_quote", side_effect=AssertionError("NO_NEW_GET")) as quote, \
                mock.patch.object(D.time, "time_ns", side_effect=AssertionError("NO_NEW_CLOCK")):
            plan = self.deps.resume_or_create_plan(self.root, "smoke", "short", {}, {})
        quote.assert_not_called()
        self.assertEqual(plan, self.plan)
        self.assertEqual(path.read_bytes(), raw)

    def test_native_quote_is_required_and_unavailable_never_falls_back_to_public(self):
        for index, provider in enumerate((None, mock.Mock(side_effect=RuntimeError("PRIVATE_NETWORK")))):
            self.deps._quote = provider
            with self.subTest(provider=index), self.assertRaises(D.TestRefused) as caught:
                self.create_plan("unavailable" + str(index))
            self.assertEqual(caught.exception.code, "PLAN_EXECUTION_QUOTE_UNAVAILABLE")
            self.assert_no_plan_files("unavailable" + str(index))

    def test_bad_native_quote_refuses_before_plan_persistence(self):
        bad = [None, {}, {**self.native, "bid": "0"}, {**self.native, "ask": "NaN"},
               {**self.native, "last": "-1"}, {**self.native, "bid": "101"},
               {**self.native, "timestamp_ms": self.now + 1},
               {**self.native, "timestamp_ms": self.now - 5001},
               {**self.native, "timestamp_ms": True}, {**self.native, "timestamp_ms": str(self.now)}]
        for index, quote in enumerate(bad):
            self.deps._quote = mock.Mock(return_value=quote)
            run = "bad" + str(index)
            with self.subTest(case=index), self.assertRaises(D.TestRefused) as caught:
                self.create_plan(run)
            self.assertEqual(caught.exception.code, "PLAN_EXECUTION_QUOTE_INVALID")
            self.assert_no_plan_files(run)
            self.assertEqual(self.deps._quote.call_count, 1)

    def test_original_scan_expiry_during_native_get_refuses_without_retimestamp(self):
        before = json.dumps(self.fields, sort_keys=True)
        def quote():
            self.now += 5001
            return {**self.native, "timestamp_ms": self.now}
        self.deps._quote = mock.Mock(side_effect=quote)
        with self.assertRaises(D.TestRefused) as caught:
            self.create_plan()
        self.assertEqual(caught.exception.code, "PLAN_MARKET_CONTEXT_STALE")
        self.assert_no_plan_files()
        self.assertEqual(json.dumps(self.fields, sort_keys=True), before)

    def test_source_quote_can_be_exactly_five_seconds_old_but_not_after_plan_work(self):
        self.native["timestamp_ms"] = self.now - 5000
        self.assertEqual(self.create_plan("boundary")["signal"]["entry"], "100.02")
        self.native["timestamp_ms"] = self.now - 4900
        self.deps.run_id_hint = "expired"
        with mock.patch.object(D.time, "time_ns", side_effect=[self.now * 1_000_000,
                self.now * 1_000_000, (self.now + 101) * 1_000_000]), self.assertRaises(D.TestRefused) as caught:
            self.deps.resume_or_create_plan(self.root, "smoke", "short", self.fields, {})
        self.assertEqual(caught.exception.code, "PLAN_EXECUTION_QUOTE_INVALID")
        self.assert_no_plan_files("expired")

    def test_native_get_and_plan_work_clock_rollback_or_invalid_clock_refuse(self):
        for index, values in enumerate(([10_000_000_000, 9_999_000_000],
                [10_000_000_000, 10_000_000_000, 9_999_000_000], [True], [0], [1], ["PRIVATE_CLOCK"])):
            run = "clock" + str(index); self.deps.run_id_hint = run
            with self.subTest(case=index), mock.patch.object(D.time, "time_ns", side_effect=values), \
                    self.assertRaises(D.TestRefused) as caught:
                self.deps.resume_or_create_plan(self.root, "smoke", "short", self.fields, {})
            self.assertEqual(caught.exception.code, "PLAN_CLOCK_INVALID")
            self.assert_no_plan_files(run)

    def test_stop_or_macro_boundary_during_native_get_refuses_before_plan_write(self):
        from src.order_router import RouterError
        def stop_quote():
            (self.root / "state/demo-auto/STOP_NEW_ENTRIES").write_text("OFFLINE_STOP")
            return dict(self.native)
        self.deps._quote = mock.Mock(side_effect=stop_quote)
        with self.assertRaises(D.TestRefused) as caught:
            self.create_plan("stopped")
        self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")
        self.assert_no_plan_files("stopped")
        (self.root / "state/demo-auto/STOP_NEW_ENTRIES").unlink()
        def quote():
            self.now += 1000
            return {**self.native, "timestamp_ms": self.now}
        self.deps._quote = mock.Mock(side_effect=quote)
        def window(now):
            if now >= 11_000:
                raise RouterError("MACRO_BLACKOUT")
            return {"valid_until_ms": 11_000, "code": "MACRO_BLACKOUT"}
        self.deps._entry_evidence = SimpleNamespace(assert_entry_current=window)
        with self.assertRaises(D.TestRefused) as caught:
            self.create_plan("macro")
        self.assertEqual(caught.exception.code, "MACRO_BLACKOUT")
        self.assert_no_plan_files("macro")

    def test_diagnostic_clock_failure_keeps_provider_result_exception_and_count(self):
        payload = {"bid": "100", "ask": "100.02"}
        provider, observer = mock.Mock(return_value=payload), mock.Mock()
        wrapped = D.ObservedQuoteProvider(provider, observer)
        with mock.patch.object(D.time, "time_ns", side_effect=RuntimeError("PRIVATE_CLOCK_ERROR")):
            self.assertIs(wrapped(), payload)
            observer.assert_called_once_with({**payload, "last": None, "timestamp_ms": None}, None, None)
            failure = RuntimeError("PRIVATE_PROVIDER_ERROR")
            provider.side_effect = failure
            with self.assertRaises(RuntimeError) as caught:
                wrapped()
        self.assertIs(caught.exception, failure)
        self.assertEqual(provider.call_count, 2)
        observer.assert_called_with(None, None, None)

    def test_quote_observer_cannot_mutate_live_result_or_receive_extra_fields(self):
        mutable = {"secret": "PRIVATE_MUTABLE"}
        quote = {"bid": "100", "ask": "100.02", "last": "100", "timestamp_ms": 10_000,
                 "uid": "PRIVATE_UID", "extra": mutable}
        seen = []
        def observer(value, _started, _completed):
            seen.append(dict(value))
            value["bid"] = "1"
            value["nested"] = {"dangerous": True}
        wrapped = D.ObservedQuoteProvider(lambda: quote, observer)
        self.assertIs(wrapped(), quote)
        self.assertEqual(quote["bid"], "100")
        self.assertNotIn("nested", quote)
        self.assertEqual(set(seen[0]), {"bid", "ask", "last", "timestamp_ms"})
        self.assertNotIn("PRIVATE", json.dumps(seen))
        quote["bid"] = mutable
        self.assertIsNone(D.ObservedQuoteProvider._quote_snapshot(quote)["bid"])

    def test_context_clears_on_return_and_exception_and_excludes_exit_quotes(self):
        returned = {"status": "REJECTED", "code": "OFFLINE"}
        quote = D.ObservedQuoteProvider(lambda: {"bid": "100", "ask": "100.02", "last": "100",
            "timestamp_ms": 10_000}, self.deps._observe_entry_quote)
        def process(plan, evidence):
            self.assertIs(plan, self.plan)
            self.assertEqual(self.deps._active_entry_plan, self.deps._diagnostic_plan(plan))
            quote()
            return returned
        controller = SimpleNamespace(process_demo_test=process)
        runtime = {"root": str(self.root), "controller": controller}
        self.assertIs(self.deps.start_test(runtime, self.plan, {}), returned)
        self.assertIsNone(self.deps._active_entry_plan)
        self.assertEqual(len(self.deps._entry_quote_observations), 1)
        quote()
        self.assertEqual(len(self.deps._entry_quote_observations), 1)
        failure = RuntimeError("PRIVATE_PROCESS_ERROR")
        controller.process_demo_test = mock.Mock(side_effect=failure)
        with self.assertRaises(RuntimeError) as caught:
            self.deps.start_test(runtime, self.plan, {})
        self.assertIs(caught.exception, failure)
        self.assertIsNone(self.deps._active_entry_plan)

    def test_observation_bound_keeps_initial_quote_without_extra_calls(self):
        self.deps._observe_plan_quote(self.plan, self.fields)
        initial = dict(self.deps._initial_plan_quote)
        self.deps._active_entry_plan = self.deps._diagnostic_plan(self.plan)
        provider = mock.Mock(return_value={"bid": "100", "ask": "100.02", "last": "100",
                                          "timestamp_ms": 10_000})
        quote = D.ObservedQuoteProvider(provider, self.deps._observe_entry_quote)
        for _ in range(40):
            quote()
        self.assertEqual(provider.call_count, 40)
        self.assertEqual(len(self.deps._entry_quote_observations), 16)
        self.assertEqual(self.deps._initial_plan_quote, initial)

    def test_whitelist_blocks_unknown_objects_and_sensitive_quote_fields(self):
        class Hostile:
            def __str__(self):
                raise AssertionError("PRIVATE_OBJECT_STR")
        self.deps._active_entry_plan = self.deps._diagnostic_plan(self.plan)
        self.deps._observe_entry_quote({"bid": "PRIVATE_BID", "ask": Hostile(), "last": "NaN",
            "timestamp_ms": True, "uid": "PRIVATE_UID", "header": "PRIVATE_HEADER"}, 1, 2)
        event = self.deps._entry_quote_observations[-1]
        self.assertIsNone(event["bid"]); self.assertIsNone(event["ask"])
        self.assertIsNone(event["last"]); self.assertIsNone(event["quote_source_timestamp_ms"])
        huge_decimal = Decimal((0, (1,) + (0,) * 200_000, -200_000))
        for invalid in (True, Decimal("Infinity"), "1E-999999", "1E999999", 10 ** 200, "x" * 100,
                        Decimal("1E-999999"), Decimal("1.0000000000000000001")):
            with self.subTest(type=type(invalid).__name__):
                self.assertIsNone(self.deps._diagnostic_price(invalid))
        bounded = self.deps._diagnostic_price(huge_decimal)
        self.assertLess(len(bounded), 32)
        self.assertEqual(Decimal(bounded), huge_decimal)
        saved = self.deps.write_report(self.root, {"run_id": "sanitized", "result": "FAIL"}, "FAIL")
        serialized = Path(saved["report_path"]).read_text(encoding="utf-8")
        self.assertNotIn("PRIVATE", serialized)
        self.assertNotIn("Hostile", serialized)

    def test_network_category_is_optional_and_strictly_whitelisted(self):
        base = {"path": "/api/v5/account/leverage-info", "code": "DEMO_NETWORK_ERROR",
                "numeric": None, "source_fault": "DEMO_SOURCE_REQUEST_FAILED"}
        faults = [base, *({**base, "network_category": category} for category in sorted(D.DIAGNOSTIC_NETWORK_CATEGORIES)),
                  {**base, "network_category": "PRIVATE_URL_OR_ERROR"},
                  {**base, "network_category": {"uid": "PRIVATE_UID"}},
                  {**base, "code": "DEMO_API_ERROR", "network_category": "DNS"}]
        self.deps._observe_risk({"request_faults": faults}, 1, 2)
        saved = self.deps._risk_observations[-1]["request_faults"]
        self.assertNotIn("network_category", saved[0])
        self.assertEqual([r["network_category"] for r in saved[1:13]], sorted(D.DIAGNOSTIC_NETWORK_CATEGORIES))
        self.assertEqual(saved[-3]["network_category"], "UNKNOWN")
        self.assertEqual(saved[-2]["network_category"], "UNKNOWN")
        self.assertNotIn("network_category", saved[-1])
        self.assertNotIn("PRIVATE", json.dumps(saved))


class TechnicalFinalEntryTests(unittest.TestCase):
    """Real technical start/controller/router/transport over TEMP fake HTTP."""
    def setUp(self):
        import test_demo_entry_admission as admission_fixtures
        import test_demo_planned_close as close_fixtures
        import test_execution as execution_fixtures
        import demo_auto_run as service
        from src.demo_auto import DemoAutoController
        from src.demo_route_transport import DemoRouteTransport
        from src.demo_transport import CREDENTIAL_NAMES
        from src.order_router import OrderRouter

        self.admission, self.closes, self.inputs, self.service = (
            admission_fixtures, close_fixtures, execution_fixtures, service)
        self.Controller, self.Transport, self.Router = DemoAutoController, DemoRouteTransport, OrderRouter
        self.credentials = {name: "offline-fixture" for name in CREDENTIAL_NAMES}
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root, self.serial = Path(self.temp.name).resolve(), 0
        self.addCleanup(mock.patch.stopall)
        mock.patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("socket.getaddrinfo", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        mock.patch("src.demo_route_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        mock.patch("src.order_router.time.monotonic_ns", side_effect=lambda: self.clock.mono).start()

    def fixture(self, *, boundary=None, pending=True):
        self.serial += 1
        root = self.root / str(self.serial)
        _, state, config = self.inputs.inputs()
        self.clock = self.admission.AdmissionClock()
        now = self.clock.wall
        state.update(demo_verified=True, observed_at_ms=now, account_hash="a" * 64,
                     week_start_equity="117.72", realized_pnl_week="0", refusal_reasons=[])
        book = {"last": "100", "bid": "99.99", "ask": "100.02", "timestamp_ms": now}
        sources = {name: {"ok": True, "source_ts": now, "fetched_at_ms": now}
                   for name in config["data"]["critical_sources"]}
        calendar = {"ok": True, "source_ts": now - 10_000, "fetched_at_ms": now, "data": []}
        deadline = None
        if boundary == "macro":
            calendar["data"] = [{"ts": now + 1000 + config["macro"]["blackout_minutes_before"] * 60_000,
                                  "country": "USD", "impact": "High"}]
            deadline = now + 1000
        elif boundary == "calendar":
            calendar["source_ts"] = now - self.service.DEMO_CALENDAR_MAX_AGE_MS + 1000
            deadline = now + 1001
        elif boundary == "critical":
            sources["order_book"]["source_ts"] = now - 45_000 + 1000
            deadline = now + 1001
        sources["economic_calendar"] = calendar
        scan = {"generated_at_ms": now, "health": {"usable": True},
                "features": {"macro": {"blackout": False}}, "sources": sources,
                "facts": {"price": "100", "bid": "99.99", "ask": "100.02"},
                "symbol": {"tickSz": "0.01", "minSz": "0.01"}}
        exchange = self.closes.PlannedExchange()
        exchange.pending_entry = pending
        deps = D.ProductionDeps()
        deps._root, deps._config = root, config
        deps._market_context = lambda _: {"observed_at_ms": self.clock.wall,
                                          "data_fresh": True, "macro_blackout": False}
        quote = D.ObservedQuoteProvider(lambda: dict(book), deps._observe_entry_quote)
        deps._quote = quote
        deps._entry_evidence = self.service.ServiceEntryEvidence(config, quote, clock=lambda: self.clock.wall)
        case = SimpleNamespace(root=root, state=state, config=config, book=book, scan=scan,
            deps=deps, exchange=exchange, deadline=deadline, risk_calls=0, risk_trigger=None)

        def snapshot(_context=None):
            case.risk_calls += 1
            if case.risk_trigger is not None:
                trigger, case.risk_trigger = case.risk_trigger, None
                trigger()
            return dict(state)

        risk = SimpleNamespace(snapshot=snapshot)
        transport = self.Transport(self.credentials, enable_writes=True,
            http_get=exchange.get, http_post=exchange.post, log_root=root / "logs")
        router = self.Router(transport, config=config, state_provider=snapshot, market_provider=quote,
            journal_path=root / "state/demo-auto/router.sqlite3", log_root=root / "logs",
            account_key="DEMO-AUTO-SOL-V1", account_identity="a" * 64, clock=lambda: self.clock.wall,
            require_demo_entry_admission=True, final_entry_check=deps._assert_entry_current)
        controller = self.Controller(config, router, risk, quote, root,
            clock=lambda: self.clock.wall, enable_demo_tests=True)
        runtime = {"controller": controller, "router": router, "risk": risk, "root": str(root)}
        self.assertEqual(deps.prepare_run(runtime, root, SimpleNamespace(run_id=None, scenario="smoke", side="long")), {"mode": "new"})
        with mock.patch.object(D.time, "time", return_value=now / 1000):
            fields = deps.market_fields(scan)
        case.runtime, case.router, case.transport, case.controller = runtime, router, transport, controller
        case.stop = root / "state/demo-auto/STOP_NEW_ENTRIES"
        case.plan = D.build_plan("smoke", D.build_signal("smoke", "long", "100", "100.02", "0.01", "0.01", now))
        case.evidence = fields["evidence"]
        return case

    def start(self, case):
        return case.deps.start_test(case.runtime, case.plan, case.evidence)

    def entry_posts(self, case):
        return [body for _, body in case.exchange.posts if isinstance(body, dict) and body.get("ordType") == "post_only"]

    def registration_count(self, case):
        with sqlite3.connect(case.root / "state/demo-auto/controller.sqlite3") as db:
            return db.execute("SELECT COUNT(*) FROM technical_tests").fetchone()[0]

    def assert_zero_send(self, case, result, code, *, receipt=False):
        self.assertEqual((result["status"], result["code"]), ("REJECTED", code))
        self.assertEqual(case.exchange.posts, [])
        self.assertFalse(case.router._local_halted)
        self.assertFalse(case.router.halt_path.exists())
        if receipt:
            with sqlite3.connect(case.router.journal_path) as db:
                owner, raw = db.execute("SELECT owner,data FROM routes").fetchone()
            row = json.loads(raw)
            self.assertEqual(owner, "")
            self.assertIs(row["entry_not_sent"], True)
            self.assertEqual(row["entry_not_sent_code"], code)
            self.assertEqual(case.transport._attempts, {})

    @contextlib.contextmanager
    def signature_hook(self, case, callback):
        original, used = case.transport._reader._headers, False

        def headers(method, path, body=""):
            nonlocal used
            value = original(method, path, body)
            if method == "POST" and not used:
                used = True
                callback()
            return value

        with mock.patch.object(case.transport._reader, "_headers", side_effect=headers):
            yield

    def test_runtime_root_mismatch_refuses_before_registration(self):
        case = self.fixture()
        for runtime in ({"controller": case.controller}, {**case.runtime, "root": str(self.root)}):
            with self.subTest(has_root="root" in runtime), self.assertRaises(D.TestRefused) as caught:
                case.deps.start_test(runtime, case.plan, case.evidence)
            self.assertEqual(caught.exception.code, "NEW_ROOT_MISMATCH")
        self.assertEqual(self.registration_count(case), 0)
        self.assertEqual(case.risk_calls, 0)
        self.assertEqual(case.exchange.posts, [])

    def test_moving_quote_diagnostics_keep_real_maker_refusal_and_zero_posts(self):
        case = self.fixture()
        case.deps._observe_plan_quote(case.plan, {"bid": "99.99", "ask": "100.02", "last": "100",
            "evidence": {"timestamp_ms": self.clock.wall}, "snapshot": case.scan})
        before = json.dumps(case.plan, sort_keys=True)
        case.risk_trigger = lambda: case.book.update(bid="99.97", ask="99.98", last="99.98")
        self.assert_zero_send(case, self.start(case), "MAKER_QUOTE_MOVED")
        self.assertEqual(case.risk_calls, 2)
        self.assertEqual(json.dumps(case.plan, sort_keys=True), before)
        events = list(case.deps._entry_quote_observations)
        self.assertTrue(events)
        self.assertEqual(events[-1]["plan_entry"], case.plan["signal"]["entry"])
        self.assertEqual(events[-1]["ask"], "99.98")
        self.assertEqual(case.deps._initial_plan_quote["ask"], "100.02")
        self.assertIsNone(case.deps._active_entry_plan)

    def create_native_plan(self, case, *, side="long", run="native"):
        case.deps.run_id_hint = run
        with mock.patch.object(D.time, "time", side_effect=lambda: self.clock.wall / 1000), \
                mock.patch.object(D.time, "time_ns", side_effect=lambda: self.clock.wall * 1_000_000):
            fields = case.deps.market_fields(case.scan)
            case.plan = case.deps.resume_or_create_plan(case.root, "smoke", side, fields, {})
        case.evidence = fields["evidence"]
        return fields

    def s24_trace_case(self):
        clock = self.admission.AdmissionClock()
        clock.wall = 1793379487617
        with mock.patch.object(self.admission, "AdmissionClock", return_value=clock):
            case = self.fixture()
        case.scan["facts"].update(bid="94.83", ask="94.84", price="94.83")
        case.scan["sources"]["ticker"]["source_ts"] = 1793379484271
        case.book.update(bid="94.86", ask="94.87", last="94.86", timestamp_ms=1793379487458)
        case.deps._quote = mock.Mock(wraps=case.deps._quote)
        return case

    def test_s24_trace_creates_native_plan_before_registration_and_keeps_two_risks(self):
        case = self.s24_trace_case()
        old = D.build_plan("smoke", D.build_signal("smoke", "short", "94.83", "94.84",
                           "0.01", "0.01", self.clock.wall))
        old_bytes = json.dumps(old, sort_keys=True)
        scan_bytes = json.dumps(case.scan, sort_keys=True)
        fields = self.create_native_plan(case, side="short")
        self.assertEqual(case.plan["signal"]["entry"], "94.87")
        self.assertEqual(case.plan["signal"]["qty"], "0.01")
        self.assertEqual(old["signal"]["entry"], "94.84")
        self.assertEqual(json.dumps(old, sort_keys=True), old_bytes)
        self.assertEqual(json.dumps(case.scan, sort_keys=True), scan_bytes)
        self.assertEqual(fields["evidence"]["generated_at_ms"], 1793379487617)
        self.assertEqual(case.deps._quote.call_count, 1)
        self.assertEqual(self.registration_count(case), 0)
        diagnostic = case.deps._initial_plan_quote
        self.assertEqual(diagnostic["quote_source"], "DEMO_SERVICE_QUOTE")
        self.assertEqual(diagnostic["quote_source_timestamp_ms"], 1793379487458)
        self.assertEqual(diagnostic["scan_source_timestamp_ms"], 1793379484271)
        result = self.start(case)
        self.assertEqual(result["status"], "PENDING")
        # Successful routing retains both original router preflights in
        # addition to the controller snapshot. The old crossed plan stopped
        # at the first router preflight, after only two snapshots in total.
        self.assertEqual(case.risk_calls, 3)
        self.assertEqual(len(self.entry_posts(case)), 1)
        self.assertEqual(self.entry_posts(case)[0]["px"], "94.87")
        with sqlite3.connect(case.root / "state/demo-auto/controller.sqlite3") as db:
            fingerprint, raw = db.execute("SELECT plan_fingerprint,signal_json FROM technical_tests").fetchone()
        self.assertEqual(fingerprint, case.controller._test_fingerprint(case.plan))
        self.assertEqual(json.loads(raw), case.plan["signal"])

    def test_new_native_plan_still_rejects_later_maker_cross_with_zero_posts(self):
        case = self.s24_trace_case()
        self.create_native_plan(case, side="short")
        case.risk_trigger = lambda: case.book.update(bid="94.89", ask="94.90", last="94.89")
        self.assert_zero_send(case, self.start(case), "MAKER_QUOTE_MOVED")
        self.assertEqual(case.risk_calls, 2)
        self.assertEqual(case.deps._quote.call_count, 1)

    def test_new_native_plan_preserves_stop_macro_and_five_second_final_guards(self):
        for mode in ("stop_risk", "stop_signature", "macro_signature", "stale_signature"):
            case = self.fixture(boundary="macro" if mode == "macro_signature" else None)
            self.create_native_plan(case)
            with self.subTest(mode=mode):
                if mode == "stop_risk":
                    case.risk_trigger = lambda: case.stop.write_text("OFFLINE_STOP")
                    self.assert_zero_send(case, self.start(case), "DEMO_ENTRY_ADMISSION_FAILED")
                else:
                    callback = ((lambda: case.stop.write_text("OFFLINE_STOP")) if mode == "stop_signature"
                        else (lambda: self.clock.advance(1000 if mode == "macro_signature" else 5001)))
                    code = {"stop_signature": "DEMO_ENTRY_ADMISSION_FAILED", "macro_signature": "MACRO_BLACKOUT",
                            "stale_signature": "DEMO_STATE_STALE"}[mode]
                    with self.signature_hook(case, callback):
                        self.assert_zero_send(case, self.start(case), code, receipt=True)

    def test_diagnostic_failure_preserves_stop_and_macro_zero_send_guards(self):
        case = self.fixture()
        case.risk_trigger = lambda: case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
        with mock.patch.object(case.deps, "_diagnostic_plan", side_effect=RuntimeError("PRIVATE")):
            self.assert_zero_send(case, self.start(case), "DEMO_ENTRY_ADMISSION_FAILED")
        self.assertIsNone(case.deps._active_entry_plan)
        case = self.fixture(boundary="macro")
        # Rebind the observer, as a real runtime captures its bound method once.
        with mock.patch.object(case.deps, "_observe_entry_quote", side_effect=RuntimeError("PRIVATE")):
            quote = D.ObservedQuoteProvider(lambda: dict(case.book), case.deps._observe_entry_quote)
            case.controller.market_provider = quote
            case.router.market_provider = quote
            with self.signature_hook(case, lambda: self.clock.advance(1000)):
                self.assert_zero_send(case, self.start(case), "MACRO_BLACKOUT", receipt=True)
        self.assertIsNone(case.deps._active_entry_plan)

    def test_stop_after_prepare_refuses_before_registration_or_risk(self):
        case = self.fixture()
        case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
        with self.assertRaises(D.TestRefused) as caught:
            self.start(case)
        self.assertEqual(caught.exception.code, "TECHNICAL_TEST_STATE_CONFLICT")
        self.assertEqual(self.registration_count(case), 0)
        with sqlite3.connect(case.root / "state/demo-auto/controller.sqlite3") as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM controller_signals").fetchone()[0], 0)
        self.assertEqual(case.risk_calls, 0)
        self.assertEqual(case.exchange.posts, [])

    def test_stop_during_risk_prevents_entry_without_halting_owned_state(self):
        case = self.fixture()
        case.risk_trigger = lambda: case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
        self.assert_zero_send(case, self.start(case), "DEMO_ENTRY_ADMISSION_FAILED")
        self.assertEqual(self.registration_count(case), 1)

    def test_stop_during_signature_records_proved_zero_send(self):
        case = self.fixture()
        with self.signature_hook(case, lambda: case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")):
            self.assert_zero_send(case, self.start(case), "DEMO_ENTRY_ADMISSION_FAILED", receipt=True)

    def test_stop_during_final_evidence_callback_is_rechecked_after_callback(self):
        case = self.fixture()
        original, calls = case.deps._entry_evidence.assert_entry_current, 0

        def evidence(now):
            nonlocal calls
            window = original(now)
            calls += 1
            if calls == 3:
                case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
            return window

        with mock.patch.object(case.deps._entry_evidence, "assert_entry_current", side_effect=evidence):
            self.assert_zero_send(case, self.start(case), "DEMO_ENTRY_ADMISSION_FAILED", receipt=True)
        self.assertEqual(calls, 3)

    def test_only_missing_flags_are_clear_and_unreadable_directory_refuses(self):
        for target in ("STOP_NEW_ENTRIES", "CONTROLLER_HALT"):
            case = self.fixture()
            blocked = case.root / "state/demo-auto" / target
            actual = Path.lstat

            def lstat(path, *args, **kwargs):
                if path == blocked:
                    raise PermissionError("OFFLINE_UNREADABLE")
                return actual(path, *args, **kwargs)

            with self.subTest(target=target), mock.patch.object(Path, "lstat", lstat), self.assertRaises(D.TestRefused) as caught:
                self.start(case)
            self.assertEqual(caught.exception.code, "LOCAL_ENTRY_FLAGS_UNREADABLE")
            self.assertEqual(self.registration_count(case), 0)
        case = self.fixture()
        actual = Path.iterdir

        def iterdir(path):
            if path == case.stop.parent:
                raise PermissionError("OFFLINE_UNREADABLE_DIRECTORY")
            return actual(path)

        with mock.patch.object(Path, "iterdir", iterdir), self.assertRaises(D.TestRefused) as caught:
            self.start(case)
        self.assertEqual(caught.exception.code, "LOCAL_ENTRY_FLAGS_UNREADABLE")
        self.assertEqual(self.registration_count(case), 0)

    def test_halt_and_dangling_reparse_flags_refuse_before_registration(self):
        for name in ("CONTROLLER_HALT", "HALT-offline"):
            case = self.fixture()
            (case.stop.parent / name).write_text("OFFLINE_HALT\n", encoding="utf-8")
            with self.subTest(name=name), self.assertRaises(D.TestRefused) as caught:
                self.start(case)
            self.assertEqual(caught.exception.code, "LOCAL_HALT_PRESENT")
            self.assertEqual(self.registration_count(case), 0)
        case = self.fixture()
        actual = Path.lstat

        def lstat(path, *args, **kwargs):
            if path == case.stop:
                # lstat sees even a dangling reparse flag; no follow-up exists
                # or target-resolution check may reinterpret it as absent.
                return SimpleNamespace(st_file_attributes=0x400, st_mode=0o120777)
            return actual(path, *args, **kwargs)

        with mock.patch.object(Path, "lstat", lstat), self.assertRaises(D.TestRefused) as caught:
            self.start(case)
        self.assertEqual(caught.exception.code, "TECHNICAL_TEST_STATE_CONFLICT")
        self.assertEqual(self.registration_count(case), 0)

    def test_macro_calendar_and_critical_expiry_during_signing_prove_zero_send(self):
        for boundary, code in (("macro", "MACRO_BLACKOUT"), ("calendar", "DEMO_CALENDAR_STALE"), ("critical", "MARKET_STALE")):
            case = self.fixture(boundary=boundary)
            with self.subTest(boundary=boundary), self.signature_hook(case, lambda: self.clock.advance(case.deadline - self.inputs.NOW)):
                self.assert_zero_send(case, self.start(case), code, receipt=True)

    def test_market_fields_keeps_original_scan_copy_and_final_hook_is_local(self):
        case = self.fixture(boundary="macro")
        original = json.loads(json.dumps(case.scan))
        case.scan["sources"]["economic_calendar"]["data"].clear()
        case.scan["sources"]["economic_calendar"]["source_ts"] = self.clock.wall
        case.scan["sources"]["order_book"]["source_ts"] += 100_000
        self.assertEqual(case.deps._entry_evidence.snapshot, original)
        with mock.patch.object(case.deps._entry_evidence, "quote", side_effect=AssertionError("NO_QUOTE_IO")):
            self.assertEqual(case.deps._assert_entry_current(self.clock.wall)["valid_until_ms"], case.deadline)
        with self.signature_hook(case, lambda: self.clock.advance(1000)):
            self.assert_zero_send(case, self.start(case), "MACRO_BLACKOUT", receipt=True)

    def test_existing_five_second_account_quote_guard_is_unchanged(self):
        case = self.fixture()
        with self.signature_hook(case, lambda: self.clock.advance(5001)):
            self.assert_zero_send(case, self.start(case), "DEMO_STATE_STALE", receipt=True)

    def test_stop_preserves_pending_cancel_and_filled_protection_planned_close(self):
        for pending, stop_during_fill in ((True, False), (False, False), (False, True)):
            case = self.fixture(pending=pending)
            actual_post = case.transport._http_post

            def post(url, headers, body, timeout):
                result = actual_post(url, headers, body, timeout)
                data = json.loads(body)
                if stop_during_fill and isinstance(data, dict) and data.get("ordType") == "post_only":
                    case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
                return result

            with self.subTest(pending=pending, stop_during_fill=stop_during_fill), mock.patch.object(case.transport, "_http_post", side_effect=post):
                result = self.start(case)
                self.assertEqual(result["status"], "PENDING" if pending else "PROTECTED")
                if not case.stop.exists():
                    case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
                if not pending:
                    self.assertEqual(case.deps.tick(case.runtime)["status"], "PROTECTED")
                    self.assertEqual(len(case.exchange.algos), 1)
                closed = case.deps.cancel_pending(case.runtime, case.plan["signal"]["id"]) if pending else case.deps.close_test(case.runtime, case.plan["signal"]["id"])
                self.assertEqual(closed["status"], "CANCELED" if pending else "CLOSED")
                self.assertEqual(len(self.entry_posts(case)), 1)
                self.assertEqual(case.exchange.positions, [])
                self.assertTrue(case.stop.exists())


class ProductionPathTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.deps = D.ProductionDeps()

    def test_locks_byte0_and_same_root(self):
        old = _make_archive(self.root / "old", D.EXPECTED_PREDECESSOR, {})
        new = _make_archive(self.root / "new", "a" * 40, {})
        with self.assertRaises(D.TestRefused) as caught:
            with self.deps.locks(old, old):
                pass
        self.assertEqual(caught.exception.code, "SAME_RUNTIME_ROOT")
        with self.deps.locks(old, new):
            lock = new / "state" / "demo-auto" / "process.lock"
            self.assertTrue(lock.exists())
            self.assertGreaterEqual(lock.stat().st_size, 1)
            with self.assertRaises(D.TestRefused) as caught:
                with self.deps.locks(old, new):
                    pass
        self.assertEqual(caught.exception.code, "PROCESS_LOCKED")

    def test_validate_roots_manifest(self):
        new = _make_archive(self.root / "new", "a" * 40, {"src.py": "ok"})
        old = _make_archive(self.root / "old", D.EXPECTED_PREDECESSOR, {"src.py": "old"})
        with mock.patch.object(D, "ROOT", new):
            self.assertEqual(self.deps.validate_roots(old, new), "a" * 40)
            (new / "src.py").write_text("tampered", encoding="utf-8")
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.validate_roots(old, new)
        self.assertEqual(caught.exception.code, "DEPLOYMENT_HASH_MISMATCH")

    def test_validate_roots_rejects_unknown_commit(self):
        new = _make_archive(self.root / "new", "UNKNOWN", {"src.py": "new"})
        old = _make_archive(self.root / "old", D.EXPECTED_PREDECESSOR, {"src.py": "old"})
        with mock.patch.object(D, "ROOT", new):
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.validate_roots(old, new)
        self.assertEqual(caught.exception.code, "DEPLOYED_COMMIT_UNKNOWN")

    def test_journal_reset_refused(self):
        old = self.root / "old"
        new = self.root / "new"
        for root in (old, new):
            (root / "state" / "demo-auto").mkdir(parents=True)
        for name in ("account-binding.json", "auto-authorization.json", "scan-seen.json"):
            (old / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
            (new / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
        (old / "state" / "demo-auto" / "verified-risk-v2").mkdir()
        for table in ("controller", "router"):
            source = old / "state" / "demo-auto" / f"{table}.sqlite3"
            target = new / "state" / "demo-auto" / f"{table}.sqlite3"
            for path, rows in ((source, 2), (target, 0)):
                con = sqlite3.connect(path)
                con.execute("CREATE TABLE t(x INTEGER)")
                con.executemany("INSERT INTO t VALUES(?)", [(1,)] * rows)
                con.commit()
                con.close()
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.preserve_state(old, new)
        self.assertIn("JOURNAL_HISTORY_MISSING", caught.exception.code)

    def test_recover_closing_only_waits_no_second_close(self):
        class ClosingDeps(FakeDeps):
            def prepare_run(self, runtime, new_root, options):
                plan = D.build_plan("smoke", D.build_signal(
                    "smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000))
                status = {"status": "CLOSING", "code": "DEMO_TECHNICAL_TEST_EXIT",
                          "signal_id": plan["signal"]["id"],
                          "filled_qty": "0.01", "protected_qty": "0.01"}
                self._prepared_state = status
                return {"mode": "resume", "plan": plan, "status": status}

            def tick(self, runtime):
                self.calls.append(("tick",))
                self._state = {"status": "CLOSED", "code": "DEMO_TECHNICAL_TEST_EXIT",
                               "signal_id": self.signal_id,
                               "filled_qty": "0", "protected_qty": "0"}
                return dict(self._state)

            def close_test(self, runtime, signal_id):
                self.calls.append(("close_test",))
                raise AssertionError("recovered CLOSING must not re-send close")

        deps = ClosingDeps(self.root)
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertFalse(any(call[0] == "close_test" for call in deps.calls))

    def test_network_blocked_during_production_checks(self):
        new = _make_archive(self.root / "new", "a" * 40, {"src.py": "new"})
        old = _make_archive(self.root / "old", D.EXPECTED_PREDECESSOR, {"src.py": "old"})

        def forbidden(*args, **kwargs):
            raise AssertionError("network forbidden")

        with mock.patch.object(D, "ROOT", new), \
                mock.patch("socket.socket", side_effect=forbidden), \
                mock.patch("urllib.request.urlopen", side_effect=forbidden):
            self.deps.validate_roots(old, new)
            with self.deps.locks(old, new):
                lock = new / "state" / "demo-auto" / "process.lock"
                self.assertTrue(lock.exists())

    def test_collect_signature_and_generated_evidence(self):
        snapshot = {"generated_at_ms": 1792400000000,
                    "sources": {"ticker": {"fetched_at_ms": 1792400000000,
                                           "data": {"last": "100"}}},
                    "facts": {"bid": "100.00", "ask": "100.02", "price": "100.01"},
                    "symbol": {"tickSz": "0.01", "minSz": "0.01"}}
        calls = []
        self.deps._config = {"symbols": []}
        self.deps._collect = lambda config, prev, **kwargs: (calls.append((config, prev, kwargs)) or snapshot)
        self.deps._market_context = lambda snap: {"observed_at_ms": 1792400000000,
                                                  "data_fresh": True, "macro_blackout": False}
        now = int(time.time() * 1000)
        snapshot["generated_at_ms"] = now
        snapshot["sources"]["ticker"]["fetched_at_ms"] = now
        self.deps.collect_snapshot(self.root)
        self.assertNotIn("force", calls[0][2])
        self.assertEqual(calls[0][2].get("calendar_max_age_s"), 1500)
        fields = self.deps.market_fields(snapshot)
        self.assertEqual(fields["evidence"]["generated_at_ms"], now)
        self.assertEqual(self.deps._context["last"], "100.01")
        self.assertEqual(self.deps._context["timestamp_ms"], now)

    def test_risk_context_120s_and_calendar_expiry(self):
        now = int(time.time() * 1000)
        self.deps._config = {"macro": {"blackout_minutes_before": 30,
                                       "blackout_minutes_after": 30,
                                       "block_impact": ["High"],
                                       "block_currencies": ["USD"]}}
        self.deps._quote = lambda: {"last": "100.0", "timestamp_ms": now}
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        clock = mock.patch.object(D.time, "time_ns", return_value=now * 1_000_000)
        clock.start()
        self.addCleanup(clock.stop)
        self.deps._macro_state = lambda quotes, events, cfg: {"blackout": False}
        self.deps._calendar = {"source_ts": now - 120_000,
                               "fetched_at_ms": now - 100_000,
                               "events": []}
        risk = FakeRisk()
        runtime = {"risk": risk, "root": str(self.root)}
        self.deps.risk_view(runtime)
        context = risk.contexts[-1]
        self.assertEqual(context["observed_at_ms"], now)   # 真实报价来源 ts，不是旧 context
        self.assertTrue(context["data_fresh"])
        self.assertFalse(context["macro_blackout"])
        # 原始日历 source_ts 过期：必须拒绝，不能借新 quote 假装新鲜
        self.deps._calendar = {"source_ts": now - 1_900_000,
                               "fetched_at_ms": now - 1_800_000,
                               "events": []}
        self.deps.risk_view(runtime)
        context = risk.contexts[-1]
        self.assertFalse(context["data_fresh"])
        self.assertIsNone(context["macro_blackout"])
        self.deps._quote = lambda: {"last": "100.0", "timestamp_ms": now - 6_000}
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.risk_view(runtime)
        self.assertEqual(caught.exception.code, "MARKET_REFRESH_FAILED")

    def test_fresh_risk_context_uses_local_clock_and_original_sources_despite_offset(self):
        from src import util
        now = 1_800_000_000_000
        self.deps._config = {}
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        self.deps._calendar = {"source_ts": now - 120_000,
                               "fetched_at_ms": now - 100_000, "events": []}
        original_calendar = dict(self.deps._calendar)
        self.deps._macro_state = lambda quotes, events, config: {"blackout": False}
        with mock.patch.object(util, "_server_offset_ms", -120), \
             mock.patch.object(util, "now_ms", return_value=now), \
             mock.patch.object(D.time, "time_ns", return_value=now * 1_000_000):
            self.assertEqual(util.server_now_ms(), now - 120)
            for age in (20, 5000):
                with self.subTest(age=age):
                    self.deps._quote = lambda: {"last": "100", "timestamp_ms": now - age}
                    self.assertEqual(self.deps._fresh_risk_context(), {
                        "observed_at_ms": now - age, "data_fresh": True, "macro_blackout": False})
            for age in (-1, 5001):
                with self.subTest(age=age):
                    self.deps._quote = lambda: {"last": "100", "timestamp_ms": now - age}
                    with self.assertRaises(D.TestRefused) as caught:
                        self.deps._fresh_risk_context()
                    self.assertEqual(caught.exception.code, "MARKET_REFRESH_FAILED")
        self.assertEqual(self.deps._calendar, original_calendar)

    def test_preserve_state_rejects_rotation_and_journal_reset(self):
        old = self.root / "old"
        new = self.root / "new"
        for root in (old, new):
            (root / "state" / "demo-auto").mkdir(parents=True)
        (old / "state" / "demo-auto" / "account-binding.json").write_text(
            json.dumps({"environment": "DEMO", "account_hash": "a" * 64}), encoding="utf-8")
        (new / "state" / "demo-auto" / "account-binding.json").write_text(
            json.dumps({"environment": "DEMO", "account_hash": "b" * 64}), encoding="utf-8")
        for name in ("auto-authorization.json", "controller.sqlite3", "router.sqlite3", "scan-seen.json"):
            (old / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
            (new / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
        (old / "state" / "demo-auto" / "verified-risk-v2").mkdir()
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.preserve_state(old, new)
        self.assertIn("ACCOUNT_OR_AUTHORIZATION_ROTATED", caught.exception.code)

    def test_prepare_run_recovers_existing_plan(self):
        state = self.root / "state" / "demo-auto"
        state.mkdir(parents=True)
        with sqlite3.connect(state / "router.sqlite3") as db:
            db.execute("CREATE TABLE controls(account TEXT,code TEXT)")
        sig = D.build_signal("smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000)
        plan = D.build_plan("smoke", sig)
        run_dir = self.root / "logs" / "technical-tests" / "test-recover"
        run_dir.mkdir(parents=True)
        (run_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
        runtime = {"controller": StatusOnly({"status": "PENDING", "code": "PENDING",
                                             "signal_id": sig["id"]})}
        options = SimpleNamespace(run_id="test-recover", scenario="smoke", side="long")
        with mock.patch.object(D.ProductionDeps, "_registration_matches",
                               return_value=True):
            prepared = self.deps.prepare_run(runtime, self.root, options)
            self.assertEqual(prepared["mode"], "resume")
            self.assertEqual(prepared["plan"]["signal"]["id"], sig["id"])
        with mock.patch.object(D.ProductionDeps, "_registration_matches",
                               return_value=False):
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.prepare_run(runtime, self.root, options)
            self.assertEqual(caught.exception.code, "ACTIVE_PLAN_MISMATCH")
        runtime_foreign = {"controller": StatusOnly({"status": "PENDING", "code": "PENDING",
                                                    "signal_id": "OTHER-1"})}
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.prepare_run(runtime_foreign, self.root, options)
        self.assertEqual(caught.exception.code, "FOREIGN_ACTIVE_INTENT")

    def test_lock_cleanup_order_for_unknown_post(self):
        deps = FakeDeps(self.root, behavior="protection_mismatch")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        names = [call[0] for call in deps.calls]
        self.assertLess(names.index("lock_enter"), names.index("safety_halt"))
        self.assertLess(names.index("safety_halt"), names.index("write_report"))
        self.assertLess(names.index("write_report"), names.index("lock_exit"))

    def test_unknown_post_failure_report_flags(self):
        deps = FakeDeps(self.root, behavior="protection_mismatch")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        report_path = self.root / "logs" / "technical-tests" / "test-fixed" / "report.json"
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["result"], "FAIL")
        self.assertTrue(report["submit_attempted"])
        self.assertEqual(report["orders_sent"], "UNKNOWN")
        self.assertEqual(report["trading_state_changed"], "UNKNOWN")

    def test_registration_fingerprint_real_sqlite(self):
        import hashlib
        sig = D.build_signal("smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000)
        plan = D.build_plan("smoke", sig)
        state = self.root / "state" / "demo-auto"
        state.mkdir(parents=True)
        digest = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":"),
                                           allow_nan=False).encode()).hexdigest()
        con = sqlite3.connect(state / "controller.sqlite3")
        con.execute("CREATE TABLE technical_tests (signal TEXT PRIMARY KEY,purpose TEXT NOT NULL,"
                    "scenario TEXT NOT NULL,authorization_id TEXT NOT NULL,"
                    "plan_fingerprint TEXT NOT NULL,signal_json TEXT NOT NULL,"
                    "registered_at_ms INTEGER NOT NULL)")
        con.execute("INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)",
                    (sig["id"], plan["purpose"], plan["scenario"],
                     plan["authorization_id"], digest, json.dumps(plan["signal"]), 1))
        con.commit()
        con.close()
        self.assertTrue(D.ProductionDeps._registration_matches(self.root, sig["id"], plan))
        con = sqlite3.connect(state / "controller.sqlite3")
        con.execute("UPDATE technical_tests SET plan_fingerprint='bad'")
        con.commit()
        con.close()
        self.assertFalse(D.ProductionDeps._registration_matches(self.root, sig["id"], plan))

    def test_natural_entry_closed_skips_close(self):
        deps = FakeDeps(self.root, behavior="natural_entry_closed")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertFalse(any(call[0] == "close_test" for call in deps.calls))
        self.assertTrue(any(call[0] == "verify_terminal" for call in deps.calls))

    def test_natural_observe_closed_skips_close(self):
        deps = FakeDeps(self.root, behavior="observe_closed")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertFalse(any(call[0] == "close_test" for call in deps.calls))
        self.assertTrue(any(call[0] == "verify_terminal" for call in deps.calls))

    def test_start_exception_reports_unknown(self):
        deps = FakeDeps(self.root, behavior="start_raise")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 2)
        report = json.loads((self.root / "logs" / "technical-tests" / "test-fixed"
                             / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["orders_sent"], "UNKNOWN")
        self.assertEqual(report["trading_state_changed"], "UNKNOWN")
        self.assertTrue(report["submit_attempted"])
        self.assertEqual(report["credentials_accessed"], True)
        self.assertTrue(any(call[0] == "safety_halt" for call in deps.calls))

    def test_real_router_recover_owners_cross_instance(self):
        from src.demo_route_transport import DemoRouteTransport
        from src.demo_transport import CREDENTIAL_NAMES
        from src.order_router import OrderRouter, client_order_id
        journal = self.root / "router.sqlite3"
        creds = {name: "fixture" for name in CREDENTIAL_NAMES}
        transport = DemoRouteTransport(creds, enable_writes=True, log_root=self.root)
        identity = "a" * 64
        router = OrderRouter(transport=transport, config={"symbols": []},
                             state_provider=lambda: {}, market_provider=lambda: {},
                             log_root=self.root, journal_path=journal,
                             account_key="DEMO-AUTO-SOL-V1", account_identity=identity)
        signal = "DEMO-TEST-SMOKE-1"
        client = client_order_id(signal)
        data = {"account_hash": identity, "signal_id": signal,
                "client_order_id": client, "status": "PENDING",
                "inst_id": "SOL-USDT-SWAP", "side": "long"}
        con = sqlite3.connect(journal)
        con.execute("INSERT INTO routes(account,signal,fingerprint,client,status,owner,data) "
                    "VALUES(?,?,?,?,?,?,?)",
                    ("DEMO-AUTO-SOL-V1", signal, "fp", client, "PENDING", "dead-owner",
                     json.dumps(data)))
        con.commit()
        con.close()
        self.assertEqual(router.recover_demo_owners(), 1)
        router2 = OrderRouter(transport=transport, config={"symbols": []},
                              state_provider=lambda: {}, market_provider=lambda: {},
                              log_root=self.root, journal_path=journal,
                              account_key="DEMO-AUTO-SOL-V1", account_identity=identity)
        self.assertEqual(router2.recover_demo_owners(), 0)

    def test_predecessor_halt_and_unreadable_journal_refused(self):
        old = self.root / "old"
        new = self.root / "new"
        for root in (old, new):
            (root / "state" / "demo-auto").mkdir(parents=True)
        for name in ("account-binding.json", "auto-authorization.json", "scan-seen.json"):
            (old / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
            (new / "state" / "demo-auto" / name).write_text("{}", encoding="utf-8")
        (old / "state" / "demo-auto" / "verified-risk-v2").mkdir()
        (old / "state" / "demo-auto" / "CONTROLLER_HALT").write_text("x", encoding="utf-8")
        (old / "state" / "demo-auto" / "controller.sqlite3").write_text("{}", encoding="utf-8")
        (old / "state" / "demo-auto" / "router.sqlite3").write_text("{}", encoding="utf-8")
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.preserve_state(old, new)
        self.assertIn("TECH_TEST_PREDECESSOR_HALT", caught.exception.code)
        (old / "state" / "demo-auto" / "CONTROLLER_HALT").unlink()
        (old / "state" / "demo-auto" / "controller.sqlite3").write_bytes(b"not sqlite")
        with self.assertRaises(D.TestRefused) as caught:
            self.deps.preserve_state(old, new)
        self.assertIn("JOURNAL_UNREADABLE", caught.exception.code)

    def test_recovery_natural_closed_direct_ledger(self):
        deps = FakeDeps(self.root, behavior="recover_natural_closed")
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertTrue(any(call[0] == "reconcile" for call in deps.calls))
        self.assertFalse(any(call[0] == "start_test" for call in deps.calls))
        self.assertFalse(any(call[0] == "close_test" for call in deps.calls))
        self.assertTrue(any(call[0] == "verify_terminal" for call in deps.calls))

    def test_recovery_protected_uses_reconcile_quantities(self):
        deps = FakeDeps(self.root, behavior="recover")
        deps._recovery_plan = D.build_plan("smoke", D.build_signal(
            "smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000))
        code, _ = self.run_main(self.base_argv(), deps)
        self.assertEqual(code, 0)
        self.assertTrue(any(call[0] == "reconcile" for call in deps.calls))
        self.assertFalse(any(call[0] == "start_test" for call in deps.calls))
        self.assertTrue(any(call[0] == "close_test" for call in deps.calls))

    def test_prepare_recovery_scenario_side_and_conflict(self):
        state = self.root / "state" / "demo-auto"
        state.mkdir(parents=True)
        with sqlite3.connect(state / "router.sqlite3") as db:
            db.execute("CREATE TABLE controls(account TEXT,code TEXT)")
        sig = D.build_signal("smoke", "long", "100.00", "100.02", "0.01", "0.01", 1792400000000)
        plan = D.build_plan("smoke", sig)
        for rid in ("test-a", "test-b"):
            run_dir = self.root / "logs" / "technical-tests" / rid
            run_dir.mkdir(parents=True)
            (run_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
        runtime = {"controller": StatusOnly({"status": "PENDING", "code": "PENDING",
                                             "signal_id": sig["id"]})}
        with mock.patch.object(D.ProductionDeps, "_registration_matches", return_value=True):
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.prepare_run(runtime, self.root,
                                      SimpleNamespace(run_id="test-a", scenario="smoke", side="long"))
            self.assertEqual(caught.exception.code, "PLAN_CONFLICT")
            (self.root / "logs" / "technical-tests" / "test-b" / "plan.json").unlink()
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.prepare_run(runtime, self.root,
                                      SimpleNamespace(run_id="test-a", scenario="swing", side="long"))
            self.assertEqual(caught.exception.code, "PLAN_SCENARIO_MISMATCH")
            with self.assertRaises(D.TestRefused) as caught:
                self.deps.prepare_run(runtime, self.root,
                                      SimpleNamespace(run_id="test-a", scenario="smoke", side="short"))
            self.assertEqual(caught.exception.code, "PLAN_SIDE_MISMATCH")

    def test_run_id_escape_refused(self):
        for bad in ("../evil", "a/b", "", "x" * 65, "a\\b"):
            with self.assertRaises(D.TestRefused):
                D.ProductionDeps.validate_run_id(bad)
        D.ProductionDeps.validate_run_id("test-ok_123")

    def run_main(self, argv, deps):
        out = io.StringIO()
        with redirect_stdout(out):
            code = D.main(argv, deps_factory=lambda: deps)
        return code, out.getvalue()

    def base_argv(self):
        return ["--run-test", "--enable-demo-test", "--scenario", "smoke",
                "--predecessor-root", str(self.root / "old"), "--new-root", str(self.root),
                "--allow-short-observation", "--observe-seconds", "1",
                "--entry-wait-seconds", "1"]

    def test_build_runtime_real_imports_and_market_provider_keys(self):
        sys.path.insert(0, str(ROOT / "scripts"))
        import demo_auto_run as dr
        from src import collect as collect_mod, demo_auto, demo_route_transport
        from src import demo_transport, demo_verified_risk, order_router, util
        new = self.root / "newrt"
        state = new / "state" / "demo-auto"
        state.mkdir(parents=True)
        identity = "a" * 64
        (state / "account-binding.json").write_text(
            json.dumps({"environment": "DEMO", "account_hash": identity}), encoding="utf-8")
        (state / "auto-authorization.json").write_text(json.dumps({
            "environment": "DEMO", "account_hash": identity,
            "authorization_recorded_at_ms": int(time.time() * 1000),
            "source": "user-approved-demo-activation",
            "scope": "SOL-USDT-SWAP_EXISTING_RISK"}), encoding="utf-8")
        captured = {}
        market_calls = []

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, path, params=None):
                market_calls.append((path, params))
                return [{"instId": "SOL-USDT-SWAP", "last": "100.5", "ts": "1792400000000",
                         "bidPx": "100.4", "askPx": "100.6"}]

        class FakeRouter:
            def __init__(self, **kwargs):
                captured["router"] = kwargs
                self.mode = "demo"
                self.account_key = "DEMO-AUTO-SOL-V1"

        class FakeController:
            def __init__(self, *args, **kwargs):
                captured["controller"] = kwargs
                captured["controller_market"] = args[3]

        with mock.patch.object(util, "load_config", return_value={"symbols": []}), \
                mock.patch.object(demo_transport, "load_demo_credentials", return_value={"k": "v"}), \
                mock.patch.object(demo_transport, "DemoReadOnlyClient", FakeClient), \
                mock.patch.object(demo_route_transport, "DemoRouteTransport",
                                  lambda *a, **k: SimpleNamespace(mode="demo", enable_writes=True)), \
                mock.patch.object(demo_verified_risk, "VerifiedDemoRiskProvider",
                                  lambda *a, **k: SimpleNamespace(snapshot=lambda c: {})), \
                mock.patch.object(order_router, "OrderRouter", FakeRouter), \
                mock.patch("src.demo_successor_gate.assert_activated_generation"), \
                mock.patch.object(demo_auto, "DemoAutoController", FakeController), \
                mock.patch.object(dr, "bind_demo_account", lambda *a, **k: identity), \
                mock.patch.object(dr, "market_context",
                                  lambda snap: {"observed_at_ms": 1, "data_fresh": True,
                                                "macro_blackout": False}):
            self.deps.build_runtime(new)
        self.assertTrue(captured["controller"]["enable_demo_tests"])
        provider = captured["router"]["market_provider"]
        self.assertIs(provider, captured["controller_market"])
        self.assertIs(provider, self.deps._quote)
        self.assertIs(provider, self.deps._entry_evidence.quote)
        self.deps._server_now = mock.Mock(side_effect=AssertionError("NO_CALIBRATED_CLOCK"))
        with mock.patch.object(dr, "_now", return_value=1792400000000):
            self.assertEqual(provider(), {"last": "100.5", "bid": "100.4", "ask": "100.6",
                                          "timestamp_ms": 1792400000000})
        self.assertEqual(len(market_calls), 1)


class VerifiedPostExitPauseTests(Helpers):
    NOW = 1792400000000

    def paused(self):
        return {"environment": "DEMO", "risk_adapter": "verified-demo-ledger-v2",
                "demo_verified": False, "paused": True, "ledger_verified": True,
                "baseline_trusted": True, "history_complete": True,
                "pause_evidence_verified": True, "account_snapshot_consistent": True,
                "data_fresh": True, "macro_blackout": False, "daily_loss_hit": False,
                "weekly_loss_hit": False, "refusal_reasons": ["CONSECUTIVE_LOSS_PAUSE"],
                "request_faults": [], "open_positions": [], "pending_orders": [],
                "protection_orders": [], "now_ms": self.NOW-20,
                "observed_at_ms": self.NOW-10, "collection_started_ms": self.NOW-20,
                "collection_completed_ms": self.NOW-2,
                "snapshot_window_start_ms": self.NOW-20, "snapshot_window_end_ms": self.NOW-3,
                "paused_until_ms": self.NOW+100000, "consecutive_losses": 4}

    def recognized(self, risk):
        with mock.patch.object(D.time, "time_ns", return_value=self.NOW*1_000_000):
            return D._verified_post_exit_pause(risk)

    def test_verified_pause_never_becomes_ledger_or_account_admission(self):
        risk = self.paused()
        self.assertTrue(self.recognized(risk))
        self.assertFalse(D._ledger_ok(risk))
        self.assertFalse(D._account_clear(risk))
        risk["diagnostic_only"] = False
        self.assertTrue(self.recognized(risk))

    def test_all_positive_proof_flags_must_be_explicit_true(self):
        for key in ("ledger_verified", "baseline_trusted", "history_complete", "pause_evidence_verified",
                    "account_snapshot_consistent", "data_fresh", "paused"):
            for value in (None, False, 1, "true"):
                with self.subTest(key=key, value=value):
                    risk = self.paused(); risk[key] = value
                    self.assertFalse(self.recognized(risk))
            risk = self.paused(); del risk[key]
            self.assertFalse(self.recognized(risk))

    def test_exact_ordinary_environment_policy_and_negative_flags_required(self):
        for key, values in (("environment", (None, "LIVE", "demo")),
                            ("risk_adapter", (None, "unverified", "verified-demo-ledger-v1")),
                            ("demo_verified", (None, True, 0)),
                            ("diagnostic_only", (None, True, 0)),
                            ("macro_blackout", (None, True, 0)),
                            ("daily_loss_hit", (None, True, 0)),
                            ("weekly_loss_hit", (None, True, 0))):
            for value in values:
                with self.subTest(key=key, value=value):
                    risk = self.paused(); risk[key] = value
                    self.assertFalse(self.recognized(risk))

    def test_unknown_extra_duplicate_and_malformed_refusals_never_recognized(self):
        class Text(str):
            pass
        for value in (None, [], (), "CONSECUTIVE_LOSS_PAUSE", [Text("CONSECUTIVE_LOSS_PAUSE")],
                      ["CONSECUTIVE_LOSS_PAUSE"]*2, ["CONSECUTIVE_LOSS_PAUSE", "ACCOUNT_STALE"],
                      ["BASELINE_MISSING"], [1]):
            risk = self.paused(); risk["refusal_reasons"] = value
            self.assertFalse(self.recognized(risk))

    def test_fault_or_nonempty_or_unknown_exposure_cannot_become_zero(self):
        for key in ("request_faults", "open_positions", "pending_orders", "protection_orders"):
            for value in (None, (), {}, "", [0], [{"code": "UNKNOWN"}]):
                risk = self.paused(); risk[key] = value
                self.assertFalse(self.recognized(risk))
            risk = self.paused(); del risk[key]
            self.assertFalse(self.recognized(risk))

    def test_wall_clock_freshness_order_and_pause_boundary_remain_strict(self):
        risk = self.paused(); risk.update(now_ms=self.NOW-5000, observed_at_ms=self.NOW-5000,
                                        collection_started_ms=self.NOW-5000,
                                        snapshot_window_start_ms=self.NOW-5000)
        self.assertTrue(self.recognized(risk))
        for key, value in (("now_ms", self.NOW), ("observed_at_ms", self.NOW-5001),
                           ("observed_at_ms", self.NOW+1), ("collection_started_ms", self.NOW-11),
                           ("collection_completed_ms", self.NOW-12),
                           ("collection_completed_ms", self.NOW+1),
                           ("paused_until_ms", self.NOW), ("consecutive_losses", 0)):
            risk = self.paused(); risk[key] = value
            self.assertFalse(self.recognized(risk))
        for key in ("now_ms", "observed_at_ms", "collection_started_ms", "collection_completed_ms",
                    "snapshot_window_start_ms", "snapshot_window_end_ms", "paused_until_ms", "consecutive_losses"):
            for value in (True, None, "1", 1.0, 2**63):
                risk = self.paused(); risk[key] = value
                self.assertFalse(self.recognized(risk))

    def test_two_wave_server_window_and_local_observation_use_distinct_clocks(self):
        for offset in (-2000, 1800):
            risk = self.paused()
            risk.update(collection_started_ms=self.NOW+offset,
                        snapshot_window_start_ms=self.NOW+offset,
                        snapshot_window_end_ms=self.NOW+offset+10)
            self.assertTrue(self.recognized(risk))
        risk = self.paused()
        risk.update(now_ms=self.NOW-9000, collection_started_ms=self.NOW-9000,
                    snapshot_window_start_ms=self.NOW-9000, snapshot_window_end_ms=self.NOW+1000)
        self.assertTrue(self.recognized(risk))
        risk["snapshot_window_end_ms"] += 1
        self.assertFalse(self.recognized(risk))
        for key, value in (("snapshot_window_end_ms", self.NOW-21),
                           ("snapshot_window_end_ms", self.NOW+2000),
                           ("collection_started_ms", self.NOW-2021)):
            risk = self.paused(); risk[key] = value
            if key == "collection_started_ms":risk["snapshot_window_start_ms"] = value
            self.assertFalse(self.recognized(risk))

    def test_invalid_or_failed_wall_clock_never_replaces_the_existing_polling_result(self):
        for value in (None, True, 0, "1", 1.0):
            with mock.patch.object(D.time, "time_ns", return_value=value):
                self.assertFalse(D._verified_post_exit_pause(self.paused()))
        with mock.patch.object(D.time, "time_ns", side_effect=RuntimeError("SYNTHETIC_CLOCK_FAILURE")):
            self.assertFalse(D._verified_post_exit_pause(self.paused()))

    def test_complete_pause_stops_after_one_ordinary_snapshot_without_sleep(self):
        risk = self.paused(); deps = SimpleNamespace(risk_view=mock.Mock(return_value=risk), poll_seconds=2)
        with mock.patch.object(D.time, "time_ns", return_value=self.NOW*1_000_000), \
                mock.patch.object(D.time, "sleep") as sleep:
            self.assertIs(D._wait_ledger(deps, object(), 120), risk)
        deps.risk_view.assert_called_once(); sleep.assert_not_called()

    def test_unverified_pause_keeps_original_polling_and_success_path_unchanged(self):
        risk = self.paused(); risk["history_complete"] = False
        deps = SimpleNamespace(risk_view=mock.Mock(return_value=risk), poll_seconds=2)
        with mock.patch.object(D.time, "time_ns", return_value=self.NOW*1_000_000), \
                mock.patch.object(D.time, "monotonic", side_effect=[0, 0, 121]), \
                mock.patch.object(D.time, "sleep") as sleep:
            self.assertIs(D._wait_ledger(deps, None, 120), risk)
        self.assertEqual(deps.risk_view.call_count, 2); sleep.assert_called_once_with(2)
        good = FakeDeps(self.root)._risk
        deps.risk_view = mock.Mock(return_value=good)
        with mock.patch.object(D.time, "sleep") as sleep:
            self.assertIs(D._wait_ledger(deps, None, 120), good)
        deps.risk_view.assert_called_once(); sleep.assert_not_called()

    def test_completed_protected_flow_reports_pause_but_never_pass_or_new_halt(self):
        deps = FakeDeps(self.root); deps._risk = self.paused()
        with mock.patch.object(D.time, "time_ns", return_value=self.NOW*1_000_000):
            code, output = self.run_main(self.base_argv(), deps)
        self.assertNotEqual(code, 0)
        report = json.loads(next((self.root/"logs/technical-tests").glob("*/report.json")).read_text())
        self.assertEqual(report["result"], "FAIL")
        self.assertEqual(report["refusal"]["code"], "POST_EXIT_RISK_PAUSED")
        self.assertEqual(report["stages"]["close"]["status"], "CLOSED")
        pause = report["stages"]["post_exit_risk"]
        self.assertIs(pause["exit_ledger_verified"], True)
        self.assertIs(pause["technical_closure_verified"], True)
        self.assertIs(pause["entry_admissible"], False)
        self.assertEqual(pause["paused_until_ms"], self.NOW+100000)
        self.assertEqual(deps.halted, [])
        self.assertEqual(sum(c[0] == "risk_view" for c in deps.calls), 1)
        self.assertEqual(sum(c[0] == "close_test" for c in deps.calls), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
