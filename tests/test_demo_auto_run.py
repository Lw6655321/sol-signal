"""Offline entry/service lifecycle safeguards; no credentials or account IO."""
import importlib.util
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("demo_auto_run_tested", Path(__file__).parents[1] / "scripts" / "demo_auto_run.py")
ENTRY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENTRY)


class ServiceSafetyTests(unittest.TestCase):
    def test_enabled_pool_begins_after_lock_and_both_gates_and_outlives_cleanup(self):
        events = []
        get = Mock()
        controller = Mock()
        controller.status.return_value = {"status": "PENDING"}
        controller.safety_halt.side_effect = lambda code: events.append("cleanup")

        @contextmanager
        def lock(path):
            events.append("lock-enter")
            try:
                yield
            finally:
                events.append("lock-exit")

        @contextmanager
        def pool():
            events.append("pool-enter")
            try:
                yield get
            finally:
                events.append("pool-close")

        def run(**options):
            self.assertIs(options["http_get"], get)
            events.append("runtime")
            with ENTRY.service_safety_guard(controller):
                raise OSError("FIXTURE_FAILURE")

        with patch.object(ENTRY, "process_lock", lock), \
                patch.object(ENTRY, "assert_service_recovery_ready", side_effect=lambda path: events.append("local-gate")), \
                patch("src.demo_successor_gate.assert_activated_generation", side_effect=lambda path: events.append("common-gate")), \
                patch("src.demo_runtime_get.runtime_demo_get", pool), \
                patch.object(ENTRY, "_run_locked_service", side_effect=run), \
                self.assertRaisesRegex(OSError, "FIXTURE_FAILURE"):
            ENTRY.run_service(enabled=True, continuous=True)
        self.assertEqual(events, ["lock-enter", "local-gate", "common-gate", "pool-enter",
                                  "runtime", "cleanup", "pool-close", "lock-exit"])
        controller.safety_halt.assert_called_once_with("SERVICE_STOPPED")

    def test_enabled_failed_local_or_common_gate_never_creates_pool_or_runtime(self):
        from src.demo_successor_gate import SuccessorGateRefused
        for local_failure in (True, False):
            with self.subTest(local_failure=local_failure), \
                    tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory, \
                    patch.object(ENTRY, "ROOT", Path(directory)), \
                    patch.object(ENTRY, "assert_service_recovery_ready",
                        side_effect=RuntimeError("DEMO_SERVICE_RECOVERY_HALTED") if local_failure else None) as local, \
                    patch("src.demo_successor_gate.assert_activated_generation",
                        side_effect=SuccessorGateRefused("SUCCESSOR_ACTIVATION_UNREADABLE")) as gate, \
                    patch("src.demo_runtime_get.runtime_demo_get") as pool, \
                    patch.object(ENTRY, "_run_locked_service") as run, \
                    patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")) as creds, \
                    self.assertRaises((RuntimeError, SuccessorGateRefused)):
                ENTRY.run_service(enabled=True, continuous=True)
            local.assert_called_once()
            self.assertEqual(gate.call_count, 0 if local_failure else 1)
            pool.assert_not_called()
            run.assert_not_called()
            creds.assert_not_called()

    def test_disabled_diagnostic_keeps_original_path_without_pool_or_proxy_selection(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory, \
                patch.object(ENTRY, "ROOT", Path(directory)), \
                patch.object(ENTRY, "assert_service_recovery_ready") as local, \
                patch("src.demo_successor_gate.assert_activated_generation") as gate, \
                patch("src.demo_runtime_get.runtime_demo_get") as pool, \
                patch("src.demo_runtime_get.selected_proxy_url", side_effect=AssertionError("NO_PROXY_READ")) as proxy, \
                patch.object(ENTRY, "_run_locked_service", return_value=7) as run:
            self.assertEqual(ENTRY.run_service(), 7)
        self.assertEqual(run.call_args.kwargs, {"enabled": False, "continuous": False,
                         "scan_interval": 60, "poll_interval": 2})
        local.assert_not_called()
        gate.assert_not_called()
        pool.assert_not_called()
        proxy.assert_not_called()

    def test_locked_runtime_shares_get_hook_and_preserves_each_retry_budget(self):
        get = Mock()
        identity_client, evidence_client = Mock(), Mock()
        provider, controller = Mock(), Mock()
        controller.status.return_value = {"status": "PENDING"}
        controller.tick.side_effect = OSError("FIXTURE_LOOP_STOP")
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory, \
                patch.object(ENTRY, "ROOT", Path(directory)), \
                patch.object(ENTRY, "assert_service_recovery_ready"), \
                patch("src.demo_successor_gate.assert_activated_generation"), \
                patch("src.util.load_config", return_value={"account": {"signal_creates_position": False}}), \
                patch("src.demo_transport.load_demo_credentials", return_value={}), \
                patch("src.demo_transport.DemoReadOnlyClient", side_effect=[identity_client, evidence_client]) as client, \
                patch.object(ENTRY, "bind_demo_account", return_value="a" * 64), \
                patch.object(ENTRY, "record_demo_authorization", return_value={}), \
                patch("src.demo_route_transport.DemoRouteTransport") as transport, \
                patch("src.demo_verified_risk.VerifiedDemoRiskProvider", return_value=provider), \
                patch("src.order_router.OrderRouter", return_value=Mock(_local_halted=False)), \
                patch("src.demo_auto.DemoAutoController", return_value=controller), \
                self.assertRaisesRegex(OSError, "FIXTURE_LOOP_STOP"):
            ENTRY._run_locked_service(enabled=True, continuous=True, http_get=get)
        self.assertEqual(client.call_count, 2)
        self.assertEqual(client.call_args_list[0].kwargs, {"http_get": get, "network_retries": 0})
        self.assertIs(client.call_args_list[1].kwargs["http_get"], get)
        self.assertEqual(client.call_args_list[1].kwargs["network_retries"], 1)
        client.call_args_list[1].kwargs["retry_budget"]("/api/v5/trade/fills-history")
        provider._reserve_get.assert_called_once_with("/api/v5/trade/fills-history")
        self.assertIs(transport.call_args.kwargs["http_get"], get)
        self.assertNotIn("http_post", transport.call_args.kwargs)
        controller.safety_halt.assert_called_once_with("SERVICE_STOPPED")

    def test_writing_single_pass_is_rejected_before_credentials(self):
        with patch.object(ENTRY, "run_service") as run:
            self.assertEqual(ENTRY.main(["--enable-demo-auto"]), 2)
            run.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "CONTINUOUS"):
            ENTRY.run_service(enabled=True, continuous=False)

    def test_continuous_run_requires_explicit_demo_capability(self):
        with patch.object(ENTRY, "run_service") as run:
            self.assertEqual(ENTRY.main(["--run"]), 2)
            run.assert_not_called()

    def test_default_is_diagnostic(self):
        with patch.object(ENTRY, "run_service", return_value=0) as run:
            self.assertEqual(ENTRY.main([]), 0)
            self.assertFalse(run.call_args.kwargs["enabled"])

    def test_service_error_with_pending_route_invokes_owned_safety_halt(self):
        controller = Mock()
        controller.status.return_value = {"status": "PENDING"}
        with self.assertRaises(OSError):
            with ENTRY.service_safety_guard(controller):
                raise OSError("simulated output file failure")
        controller.safety_halt.assert_called_once_with("SERVICE_STOPPED")

    def test_flat_service_exit_does_not_send_cleanup(self):
        controller = Mock()
        controller.status.return_value = {"status": "IDLE"}
        with ENTRY.service_safety_guard(controller):
            pass
        controller.safety_halt.assert_not_called()

    def test_duplicate_instance_fails_lock(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            path = Path(directory) / "process.lock"
            with ENTRY.process_lock(path):
                with self.assertRaisesRegex(RuntimeError, "ALREADY_RUNNING"):
                    with ENTRY.process_lock(path):
                        self.fail("second process obtained lock")

    def test_account_rotation_cannot_reuse_order_journal(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            client = Mock()
            client.get.return_value = [{"uid": "123"}]
            ENTRY.bind_demo_account(client, root)
            ENTRY.bind_demo_account(client, root)
            client.get.return_value = [{"uid": "456"}]
            with self.assertRaisesRegex(RuntimeError, "ACCOUNT_CHANGED"):
                ENTRY.bind_demo_account(client, root)
            client.get.return_value = [{"uid": "not-account-id"}]
            with self.assertRaisesRegex(RuntimeError, "ID_UNVERIFIED"):
                ENTRY.bind_demo_account(client, root)

    def test_existing_unbound_journal_cannot_be_adopted(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            (root / "router.sqlite3").touch()
            client = Mock()
            client.get.return_value = [{"uid": "123"}]
            with self.assertRaisesRegex(RuntimeError, "JOURNAL_UNBOUND"):
                ENTRY.bind_demo_account(client, root)

    def test_existing_authorization_record_is_frozen_and_account_bound(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            with patch.object(ENTRY, '_now', return_value=2000):
                first = ENTRY.record_demo_authorization(root, 'a'*64)
            with patch.object(ENTRY, '_now', return_value=3000):
                second = ENTRY.record_demo_authorization(root, 'a'*64)
                self.assertEqual(first, second)
                self.assertEqual(second['authorization_recorded_at_ms'], 2000)
                with self.assertRaisesRegex(RuntimeError, 'AUTHORIZATION_INVALID'):
                    ENTRY.record_demo_authorization(root, 'b'*64)
            with patch.object(ENTRY, '_now', return_value=1999):
                with self.assertRaisesRegex(RuntimeError, 'AUTHORIZATION_INVALID'):
                    ENTRY.record_demo_authorization(root, 'a'*64)

    def test_missing_or_old_macro_is_unknown_not_cleared(self):
        with patch.object(ENTRY, "_now", return_value=2000000):
            snapshot = {"health": {"usable": True}, "features": {"macro": {"blackout": False}}}
            self.assertIsNone(ENTRY.market_context(snapshot)["macro_blackout"])
            snapshot["sources"] = {"economic_calendar": {"ok": True, "fetched_at_ms": 1}}
            self.assertIsNone(ENTRY.market_context(snapshot)["macro_blackout"])
            snapshot["sources"]["economic_calendar"]["fetched_at_ms"] = 2000000
            self.assertIsNone(ENTRY.market_context(snapshot)["macro_blackout"])
            snapshot["sources"]["economic_calendar"]["source_ts"] = 2000000
            self.assertIs(ENTRY.market_context(snapshot)["macro_blackout"], False)

    def test_calendar_age_uses_original_source_not_new_cache_envelope(self):
        now = 10_000_000
        cases = [
            (now - 29 * 60_000, now, False),
            (now - 30 * 60_000, now, False),
            (now - 30 * 60_000 - 1, now, None),
            (now - 89 * 60_000, now - 28 * 60_000, None),
            (now + 1, now + 1, None),
            (now, now + 1, None),
            (now, now - 1, None),
            (0, now, None),
            (-1, now, None),
            (None, now, None),
            (True, now, None),
            (str(now), now, None),
            (float(now), now, None),
            (now, None, None),
            (now, True, None),
        ]
        with patch.object(ENTRY, "_now", return_value=now):
            for source_stamp, envelope_stamp, expected in cases:
                with self.subTest(source=source_stamp, envelope=envelope_stamp):
                    snapshot = {"health": {"usable": True},
                        "features": {"macro": {"blackout": False}},
                        "sources": {"economic_calendar": {"ok": True,
                            "source_ts": source_stamp, "fetched_at_ms": envelope_stamp}}}
                    self.assertIs(ENTRY.market_context(snapshot)["macro_blackout"], expected)

    def test_diagnostic_requests_same_strict_calendar_policy_as_auto_service(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            config = {"test": "no_credentials"}
            with patch("src.collect.collect", return_value={}) as collect, \
                    patch("src.demo_risk.DemoRiskProvider") as provider, \
                    patch.object(ENTRY, "_write"), patch("builtins.print"):
                provider.return_value.snapshot.return_value = {}
                self.assertEqual(ENTRY.diagnostic_once(Mock(), config, root, root), 0)
            collect.assert_called_once_with(config, calendar_max_age_s=1500)

    def test_same_scan_kind_side_does_not_become_repeated_entry(self):
        with patch.object(ENTRY, "_now", return_value=1000000):
            evaluation = {"signals": [{"id": "SIGNAL-A", "kind": "scalp_5m", "side": "long", "expiry_min": 15}],
                          "gates": {"pass": True}}
            seen = {}
            self.assertEqual(len(ENTRY.scan_to_payload({}, evaluation, seen, {})["new_signals"]), 1)
            evaluation["signals"][0]["id"] = "SIGNAL-B"
            self.assertEqual(ENTRY.scan_to_payload({}, evaluation, seen, {})["new_signals"], [])

    def test_operational_fault_summary_preserves_reason_without_private_fields(self):
        summary = ENTRY.service_risk_summary({
            'demo_verified': False, 'refusal_reasons': ['ACCOUNT_READ_FAILED'],
            'raw_uid': 'PRIVATE_SENTINEL', 'headers': 'PRIVATE_SENTINEL',
            'request_faults': [{'path': '/api/v5/account/balance', 'code': 'HTTP_REJECTED',
                'numeric': 50011, 'source_fault': 'DEMO_SOURCE_RATE_LIMITED',
                'body': 'PRIVATE_SENTINEL', 'headers': 'PRIVATE_SENTINEL'}]})
        self.assertEqual(summary['request_faults'][0]['numeric'], 50011)
        self.assertNotIn('PRIVATE_SENTINEL', str(summary))
        self.assertFalse(summary['demo_verified'])


class ServiceDynamicEntryTests(unittest.TestCase):
    """Use the real strict router/transport with only TEMP fake exchange IO."""
    def setUp(self):
        import test_demo_service_entry_evidence as evidence_fixtures

        self.helper = evidence_fixtures.ServiceEvidenceRouterIntegrationTests()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def fixture(self, side="long"):
        case = self.helper.fixture(boundary="none", side=side)
        case.flags = case.root / "state/demo-auto"
        case.flags.mkdir(parents=True)
        case.stop = case.flags / "STOP_NEW_ENTRIES"
        case.router.final_entry_check = lambda now: ENTRY.assert_service_entry_current(case.flags, case.evidence, now)
        return case

    def assert_zero_send(self, case, result, *, persisted=False):
        self.helper.helper.assert_local_rejection(case, result, "DEMO_ENTRY_ADMISSION_FAILED", persisted=persisted)
        self.assertEqual(case.transport._attempts, {})
        self.assertFalse((case.flags / "CONTROLLER_HALT").exists())
        if persisted:
            row = self.helper.helper.route(case)
            self.assertEqual(row["owner"], "")
            self.assertIs(row["data"]["entry_not_sent"], True)
            self.assertEqual(row["data"]["entry_not_sent_code"], "DEMO_ENTRY_ADMISSION_FAILED")

    def test_stop_after_runtime_preparation_blocks_new_entry(self):
        case = self.fixture()
        case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
        self.assert_zero_send(case, case.router.execute(case.intent))

    def test_stop_during_risk_snapshot_blocks_new_entry(self):
        case = self.fixture()
        original = case.router.state_provider

        def state():
            value = original()
            case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
            return value

        with patch.object(case.router, "state_provider", side_effect=state):
            self.assert_zero_send(case, case.router.execute(case.intent))

    def test_stop_during_signing_is_proved_unsent_and_never_retried(self):
        case = self.fixture()
        original = case.transport._reader._headers

        def signed(method, path, body=""):
            value = original(method, path, body)
            if method == "POST":
                case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
            return value

        with patch.object(case.transport._reader, "_headers", side_effect=signed):
            self.assert_zero_send(case, case.router.execute(case.intent), persisted=True)
        self.assertEqual(case.router.execute(case.intent).status, "REJECTED")
        self.assertEqual(case.exchange.posts, [])

    def test_stop_created_by_final_evidence_check_wins_after_callback(self):
        case = self.fixture()
        original, calls = case.evidence.assert_entry_current, 0

        def evidence(now):
            nonlocal calls
            window = original(now)
            calls += 1
            if calls == 3:
                case.stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
            return window

        with patch.object(case.evidence, "assert_entry_current", side_effect=evidence):
            self.assert_zero_send(case, case.router.execute(case.intent), persisted=True)
        self.assertEqual(calls, 3)

    def test_halt_dangling_and_unreadable_flags_fail_closed_without_flag_contents(self):
        from src.order_router import RouterError

        for name in ("CONTROLLER_HALT", "HALT-offline", "STOP_NEW_ENTRIES"):
            case = self.fixture()
            flag = case.flags / name
            actual = Path.lstat

            def lstat(path, *args, **kwargs):
                if path == flag:
                    # A dangling/reparse flag is present even if exists() would
                    # follow its missing target and return False.
                    return SimpleNamespace(st_file_attributes=0x400, st_mode=0o120777)
                return actual(path, *args, **kwargs)

            with self.subTest(flag=name), patch.object(Path, "lstat", lstat):
                if name.startswith("HALT-"):
                    flag.touch()
                self.assert_zero_send(case, case.router.execute(case.intent))
        for unreadable in ("STOP_NEW_ENTRIES", "CONTROLLER_HALT", "directory", "missing"):
            case = self.fixture()
            actual_lstat, actual_iterdir = Path.lstat, Path.iterdir

            def lstat(path, *args, **kwargs):
                if path == case.flags / unreadable:
                    raise PermissionError("OFFLINE_UNREADABLE_FLAG")
                return actual_lstat(path, *args, **kwargs)

            def iterdir(path):
                if path == case.flags:
                    raise (FileNotFoundError if unreadable == "missing" else PermissionError)("OFFLINE_UNREADABLE_DIRECTORY")
                return actual_iterdir(path)

            with self.subTest(unreadable=unreadable), patch.object(Path, "lstat", lstat), \
                    patch.object(Path, "iterdir", iterdir if unreadable in {"directory", "missing"} else actual_iterdir), \
                    self.assertRaises(RouterError) as caught:
                ENTRY.assert_service_entry_current(case.flags, case.evidence, self.helper.clock.wall)
            self.assertEqual(caught.exception.code, "DEMO_ENTRY_ADMISSION_FAILED")

    def test_clear_flags_preserve_original_deadline_and_natural_maker_plan(self):
        for side in ("long", "short"):
            case = self.fixture(side)
            original = case.intent.to_dict()
            expected = case.evidence.assert_entry_current(self.helper.clock.wall)
            with self.subTest(side=side), patch.object(case.evidence, "quote", side_effect=AssertionError("NO_CALLBACK_IO")):
                self.assertEqual(ENTRY.assert_service_entry_current(case.flags, case.evidence, self.helper.clock.wall), expected)
            self.assertEqual(case.router.execute(case.intent).status, "PENDING")
            self.assertEqual(len(case.exchange.posts), 1)
            self.assertEqual(case.exchange.posts[0][1]["ordType"], "post_only")
            self.assertEqual(case.intent.to_dict(), original)


class ServiceStopDrainTests(unittest.TestCase):
    @contextmanager
    def loop(self, *, old_tick="IDLE", entered="PROTECTED", stop_on_report=False):
        """Run the actual service loop with bounded local lifecycle fixtures."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            state = root / "state/demo-auto"
            current = {"status": "IDLE", "signal_id": ""}
            controller = Mock()
            controller.status.side_effect = lambda: dict(current)
            controller.tick.return_value = {"status": old_tick}

            def enter(_payload):
                if not stop_on_report:
                    current.update(status=entered, signal_id="SCA-OFFLINE-OWNED")
                    (state / "STOP_NEW_ENTRIES").write_text("OFFLINE_STOP\n", encoding="utf-8")
                return {"status": entered}

            controller.process_scan.side_effect = enter
            actual_write = ENTRY._write

            def write(path, value):
                actual_write(path, value)
                if stop_on_report and path.name == "service.json":
                    current.update(status=entered, signal_id="SCA-OFFLINE-OWNED")
                    (state / "STOP_NEW_ENTRIES").write_text("OFFLINE_STOP\n", encoding="utf-8")

            risk = {"demo_verified": True, "entries_today": 0, "observed_at_ms": 1_800_000_000_000}
            with ExitStack() as stack:
                for guard in (
                    patch.object(ENTRY, "ROOT", root), patch.object(ENTRY, "assert_service_recovery_ready"),
                    patch("src.demo_successor_gate.assert_activated_generation"),
                    patch("src.util.load_config", return_value={"account": {"signal_creates_position": False}, "risk": {"max_entries_per_day": 3}}),
                    patch("src.demo_transport.load_demo_credentials", return_value={}),
                    patch("src.demo_transport.DemoReadOnlyClient"),
                    patch.object(ENTRY, "bind_demo_account", return_value="a" * 64),
                    patch.object(ENTRY, "record_demo_authorization", return_value={}),
                    patch("src.demo_route_transport.DemoRouteTransport"),
                    patch("src.demo_verified_risk.VerifiedDemoRiskProvider"),
                    patch("src.order_router.OrderRouter", return_value=Mock(_local_halted=False)),
                    patch("src.demo_auto.DemoAutoController", return_value=controller),
                    patch.object(ENTRY, "ServiceEntryEvidence", return_value=Mock(fresh_context=Mock(return_value={}))),
                    patch.object(ENTRY, "ServiceRiskProvider", return_value=Mock(snapshot=Mock(return_value=risk))),
                    patch("src.collect.collect", return_value={}), patch("src.collect.trimmed", return_value={}),
                    patch("src.engine.evaluate", return_value={}), patch.object(ENTRY, "scan_to_payload", return_value={}),
                    patch.object(ENTRY, "_write", side_effect=write), patch.object(ENTRY.time, "monotonic", return_value=0),
                    patch("builtins.print"),
                ):
                    stack.enter_context(guard)
                yield SimpleNamespace(controller=controller, current=current, root=root)

    def test_old_idle_or_closed_tick_does_not_exit_with_current_owned_or_uncertain_state(self):
        for old_tick in ("IDLE", "CLOSED"):
            for current in ("PENDING", "PROTECTED", "CLOSING", "HALTED", "UNKNOWN"):
                with self.subTest(old_tick=old_tick, current=current), self.loop(old_tick=old_tick, entered=current) as case:
                    with patch.object(ENTRY.time, "sleep", side_effect=OSError("OFFLINE_CONTINUE_SUPERVISION")) as sleep, \
                            self.assertRaisesRegex(OSError, "OFFLINE_CONTINUE_SUPERVISION"):
                        ENTRY._run_locked_service(enabled=True, continuous=True)
                    sleep.assert_called_once_with(2)
                    case.controller.process_scan.assert_called_once()
                    # The deliberate fixture failure, not STOP, invokes cleanup.
                    case.controller.safety_halt.assert_called_once_with("SERVICE_STOPPED")

    def test_stop_after_status_write_also_rechecks_current_controller(self):
        with self.loop(entered="PENDING", stop_on_report=True) as case:
            with patch.object(ENTRY.time, "sleep", side_effect=OSError("OFFLINE_CONTINUE_SUPERVISION")), \
                    self.assertRaisesRegex(OSError, "OFFLINE_CONTINUE_SUPERVISION"):
                ENTRY._run_locked_service(enabled=True, continuous=True)
            self.assertEqual(case.current["status"], "PENDING")

    def test_stop_keeps_ticking_until_current_terminal_and_exits_without_emergency_halt(self):
        with self.loop() as case:
            def sleep(_seconds):
                if sleep.calls == 0:
                    case.current["status"] = "CLOSING"
                else:
                    case.current.update(status="IDLE", signal_id="")
                sleep.calls += 1
                self.assertLessEqual(sleep.calls, 2, "bounded drain must finish")
            sleep.calls = 0
            with patch.object(ENTRY.time, "sleep", side_effect=sleep):
                self.assertEqual(ENTRY._run_locked_service(enabled=True, continuous=True), 0)
            self.assertEqual(sleep.calls, 2)
            self.assertEqual(case.controller.tick.call_count, 4)
            case.controller.process_scan.assert_called_once()
            case.controller.safety_halt.assert_not_called()

    def test_stop_preserves_real_pending_cancel_and_filled_protection_natural_exit(self):
        import test_demo_natural_exit_lifecycle as natural_fixtures
        from src.execution import build_intent

        helper = natural_fixtures.NaturalExitLifecycleTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        for pending, fill_on_cancel in ((True, False), (True, True), (False, False)):
            signal, state, config = natural_fixtures.inputs()
            signal.update(id="SCA-LONG-SERVICE-DRAIN-" + str(helper.serial), generated_at_ms=natural_fixtures.NOW - 1000,
                          expires_at_ms=natural_fixtures.NOW + (1000 if pending else 60_000), qty="0.02")
            helper.serial += 1
            state.update(demo_verified=True, observed_at_ms=natural_fixtures.NOW, account_hash="a" * 64,
                         week_start_equity="117.72", realized_pnl_week="0", refusal_reasons=[])
            case = SimpleNamespace(root=helper.root / str(helper.serial), signal=signal, state=state,
                config=config, now=natural_fixtures.NOW, exchange=natural_fixtures.planned_fixtures.PlannedExchange(), risk_calls=[])
            case.exchange.pending_entry = pending
            actual_post = case.exchange.post

            def post(url, headers, body, timeout):
                result = actual_post(url, headers, body, timeout)
                if fill_on_cancel and url.endswith("/trade/cancel-order"):
                    # Complete fill won this cancel race. Later lifecycle
                    # binding requires the exchange's settled filled receipt.
                    row = case.exchange.orders[json.loads(body)["clOrdId"]]
                    self.assertEqual(row["accFillSz"], row["sz"])
                    row["state"] = "filled"
                return result

            case.exchange.post = post
            helper.reopen(case)
            flags = case.root / "state/demo-auto"
            evidence = Mock(assert_entry_current=Mock(return_value={"valid_until_ms": case.now + 60_000, "code": "MARKET_STALE"}))
            case.router.require_demo_entry_admission = True
            case.router.final_entry_check = lambda now: ENTRY.assert_service_entry_current(flags, evidence, now)
            with self.subTest(pending=pending, fill_on_cancel=fill_on_cancel):
                result = case.controller.process_scan(helper.payload(case))["results"][0]
                self.assertEqual(result["status"], "PENDING" if pending else "PROTECTED")
                case.intent = build_intent(signal, state, config)
                stop = flags / "STOP_NEW_ENTRIES"
                stop.write_text("OFFLINE_STOP\n", encoding="utf-8")
                evidence.assert_entry_current.reset_mock()
                if pending:
                    if fill_on_cancel:
                        case.exchange.fill_entry_on_cancel = case.intent.qty
                    case.now = case.intent.expires_at_ms
                result = case.controller.tick()
                if pending and not fill_on_cancel:
                    self.assertEqual(result["status"], "CANCELED")
                else:
                    self.assertEqual(result["status"], "PROTECTED")
                    self.assertTrue(any(row["state"] == "live" for row in case.exchange.algos.values()))
                    self.assertEqual(case.controller.tick()["status"], "PROTECTED")
                    case.now = helper.deadline(case)
                    helper.assert_clean_terminal(case, "MAX_HOLD_REACHED")
                self.assertEqual(case.controller.status()["status"], "IDLE")
                self.assertEqual(case.exchange.positions, [])
                self.assertEqual(sum(isinstance(body, dict) and body.get("ordType") == "post_only" for _, body in case.exchange.posts), 1)
                self.assertTrue(stop.exists())
                evidence.assert_entry_current.assert_not_called()
                self.assertFalse(case.controller.halt_path.exists())


if __name__ == "__main__":
    unittest.main()
