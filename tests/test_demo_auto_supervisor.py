"""Pure local supervisor tests. All child processes and waits are fakes."""
import hashlib
import builtins
import importlib.util
import io
import json
import socket
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "scripts" / "demo_auto_supervisor.py"
spec = importlib.util.spec_from_file_location("offline_demo_supervisor", SOURCE)
S = importlib.util.module_from_spec(spec)
spec.loader.exec_module(S)
COMMIT = "a" * 40


class FakeChild:
    pid = 12345

    def __init__(self, results):
        self.results = iter(results)

    def poll(self):
        return next(self.results)

    def kill(self):
        raise AssertionError("CHILD_MUST_NOT_BE_KILLED")

    terminate = kill


class IsolatedSupervisorBootstrapTests(unittest.TestCase):
    def clean_path(self):
        root = str(SOURCE.parents[1]).casefold()
        return [value for value in sys.path if str(value).casefold() not in {root, root + "\\scripts", root + "/scripts"}]

    def load(self):
        spec = importlib.util.spec_from_file_location("isolated_supervisor_bootstrap_test", SOURCE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_default_verification_bootstraps_own_root_without_account_or_proxy_imports(self):
        real_import, local_imports = builtins.__import__, []

        def imports(name, *args, **kwargs):
            if name.startswith("src"):
                local_imports.append(name)
                raise AssertionError("NO_ACCOUNT_OR_GATE_IMPORT_FOR_DEFAULT_VERIFICATION")
            return real_import(name, *args, **kwargs)

        with patch.object(sys, "path", self.clean_path()), patch("builtins.__import__", imports):
            module = self.load()
            with patch.object(module, "verify_deployment", return_value={"verified": True}) as verify, redirect_stdout(io.StringIO()):
                self.assertEqual(module.main(["--expected-commit", COMMIT]), 0)
            self.assertEqual(sys.path[0], str(SOURCE.parents[1]))
            verify.assert_called_once_with(module.ROOT, COMMIT)
        self.assertEqual(local_imports, [])

    def test_enabled_common_gate_refuses_before_child_or_state_write_with_clean_path(self):
        from src.demo_successor_gate import SuccessorGateRefused
        real_import, gate_imports = builtins.__import__, []

        def imports(name, *args, **kwargs):
            if name.startswith("src"):
                self.assertEqual(sys.path[0], str(SOURCE.parents[1]))
                self.assertEqual(name, "src.demo_successor_gate")
                gate_imports.append(name)
            return real_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory, \
                patch("src.demo_successor_gate.assert_activated_generation",
                      side_effect=SuccessorGateRefused("SUCCESSOR_PREPARATION_UNREADABLE")) as gate, \
                patch.object(sys, "path", self.clean_path()), patch("builtins.__import__", imports):
            module = self.load()
            process = unittest.mock.Mock(side_effect=AssertionError("NO_CHILD_PROCESS"))
            root = Path(directory)
            with patch.object(module, "verify_deployment", return_value={"verified": True}), \
                    self.assertRaises(module.SupervisorRefused) as caught:
                module.DemoAutoSupervisor(root, COMMIT, process_factory=process).run(enable_demo_auto=True)
            self.assertEqual(caught.exception.code, "SUCCESSOR_PREPARATION_UNREADABLE")
            self.assertFalse((root / "state").exists())
            gate.assert_called_once_with(root.resolve(), COMMIT)
            process.assert_not_called()
        self.assertEqual(gate_imports, ["src.demo_successor_gate"])


class DemoSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(patch.stopall)
        patch.object(socket.socket, "connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch.object(S.subprocess, "Popen", side_effect=AssertionError("NO_REAL_PROCESS")).start()
        # These historical tests isolate supervisor lifecycle behavior. The
        # real activation protocol is exercised without this patch in
        # test_demo_successor_gate.py.
        patch("src.demo_successor_gate.assert_activated_generation").start()
        self.contents = {"config.json": "{}", "src/example.py": "# fixture\n",
                         S.ENTRY: "# fixture child, never executed\n", S.SELF: "# fixture\n"}
        for name, content in self.contents.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        self.manifest = {"schema": 1, "environment": "DEMO", "commit": COMMIT,
                         "files": {name: hashlib.sha256((self.root / name).read_bytes()).hexdigest()
                                   for name in self.contents}}
        (self.root / S.COMMIT_MARKER).write_text(COMMIT + "\n", encoding="ascii")
        self.save_manifest()
        self.calls, self.waits = [], []

    def save_manifest(self):
        (self.root / S.MANIFEST).write_text(json.dumps(self.manifest), encoding="utf-8")

    def supervisor(self, outcomes, *, sleep=None, **kwargs):
        children = iter(outcomes)
        def factory(args, **options):
            self.calls.append((args, options))
            return FakeChild(next(children))
        return S.DemoAutoSupervisor(self.root, COMMIT, process_factory=factory,
                                   sleep=sleep or self.waits.append, clock=lambda: 1800000000000,
                                   windows=True, **kwargs)

    def test_default_cli_verifies_without_launch_or_state_creation(self):
        with patch.object(S, "ROOT", self.root), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(S.main(["--expected-commit", COMMIT]), 0)
        self.assertTrue(json.loads(output.getvalue())["verified"])
        self.assertFalse((self.root / "state").exists())

    def test_run_requires_both_flags_and_pinned_commit(self):
        for flags in (["--run"], ["--enable-demo-auto"]):
            with patch.object(S, "ROOT", self.root), redirect_stdout(io.StringIO()):
                self.assertEqual(S.main(flags + ["--expected-commit", COMMIT]), 2)
        with self.assertRaises(S.SupervisorRefused):
            self.supervisor([]).run()

    def test_fixed_child_arguments_cwd_hidden_window_and_separate_logs(self):
        supervisor = self.supervisor([[0]])
        self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        args, options = self.calls[0]
        self.assertEqual(args, [sys.executable, "-X", "utf8", "-B", str(self.root / S.ENTRY),
                                "--run", "--enable-demo-auto"])
        self.assertEqual(options["cwd"], str(self.root))
        self.assertEqual(options["creationflags"], S.CREATE_NO_WINDOW)
        self.assertNotIn("shell", options)
        self.assertNotIn("env", options)
        self.assertNotEqual(options["stdout"].name, options["stderr"].name)
        self.assertEqual(supervisor.last_status["code"], "CHILD_EXITED_NORMALLY")

    def test_normal_exit_never_restarts(self):
        self.assertEqual(self.supervisor([[0], [0]]).run(enable_demo_auto=True), 0)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.waits, [])

    def test_abnormal_exit_has_at_most_three_restarts_and_backoff(self):
        supervisor = self.supervisor([[2], [2], [2], [2], [0]])
        self.assertEqual(supervisor.run(enable_demo_auto=True), 2)
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(self.waits, [5, 10, 20])
        self.assertEqual(supervisor.last_status["code"], "RESTART_LIMIT_REACHED")
        files = list((self.root / "logs").rglob("*.log"))
        self.assertEqual(len(files), 8)
        self.assertEqual(len({p.name for p in files}), 8)

    def test_restarts_reverify_every_source_file(self):
        def change_source(delay):
            self.waits.append(delay)
            (self.root / "src" / "example.py").write_text("changed", encoding="utf-8")
        supervisor = self.supervisor([[2], [0]], sleep=change_source)
        self.assertEqual(supervisor.run(enable_demo_auto=True), 2)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(supervisor.last_status["code"], "DEPLOYMENT_HASH_MISMATCH")

    def test_stop_before_launch_never_starts_child_or_deletes_halt(self):
        supervisor = self.supervisor([])
        supervisor.state_root.mkdir(parents=True)
        supervisor.stop_path.touch()
        halt = supervisor.state_root / "CONTROLLER_HALT"
        halt.write_text("UNKNOWN\n", encoding="ascii")
        self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        self.assertEqual(self.calls, [])
        self.assertEqual(halt.read_text(encoding="ascii"), "UNKNOWN\n")

    def test_stop_during_child_preserves_it_until_self_exit(self):
        supervisor = None
        def wait(delay):
            self.waits.append(delay)
            supervisor.stop_path.touch()
        supervisor = self.supervisor([[None, None, 0], [0]], sleep=wait)
        self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.waits, [2, 2])
        self.assertTrue(supervisor.stop_path.exists())

    def test_stop_during_backoff_prevents_restart(self):
        supervisor = None
        def wait(delay):
            supervisor.stop_path.touch()
        supervisor = self.supervisor([[2], [0]], sleep=wait)
        self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        self.assertEqual(len(self.calls), 1)

    def test_stop_arriving_during_verification_prevents_launch(self):
        supervisor = self.supervisor([[0]])
        original = S.verify_deployment
        count = 0
        def verify(*args, **kwargs):
            nonlocal count
            result = original(*args, **kwargs)
            count += 1
            if count == 2:
                supervisor.stop_path.touch()
            return result
        with patch.object(S, "verify_deployment", side_effect=verify):
            self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        self.assertEqual(self.calls, [])

    def test_state_write_failure_does_not_abandon_the_live_child(self):
        supervisor = self.supervisor([[None, None, 0]])
        original = supervisor._record
        def record(status, code, **details):
            if status == "RUNNING":
                raise OSError("fixture disk problem")
            return original(status, code, **details)
        supervisor._record = record
        self.assertEqual(supervisor.run(enable_demo_auto=True), 0)
        self.assertEqual(self.waits, [2, 2])
        self.assertEqual(len(self.calls), 1)

    def test_exclusive_supervisor_lock_rejects_a_second_supervisor(self):
        supervisor = self.supervisor([[0]])
        with S._lock(supervisor.state_root / "supervisor.lock"):
            with self.assertRaises(S.SupervisorRefused) as caught:
                supervisor.run(enable_demo_auto=True)
        self.assertEqual(caught.exception.code, "SUPERVISOR_ALREADY_RUNNING")
        self.assertEqual(self.calls, [])

    def test_commit_and_marker_must_match_external_pin(self):
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, "b" * 40)
        (self.root / S.COMMIT_MARKER).write_text("b" * 40, encoding="ascii")
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, COMMIT)

    def test_manifest_commands_and_path_escape_are_data_not_execution(self):
        self.manifest["command"] = "never execute me"
        self.save_manifest()
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, COMMIT)
        del self.manifest["command"]
        for path in ("../escape.py", "C:/escape.py", "src/../escape.py", "src\\escape.py"):
            self.manifest["files"][path] = "a" * 64
            self.save_manifest()
            with self.assertRaises(S.SupervisorRefused):
                S.verify_deployment(self.root, COMMIT)
            del self.manifest["files"][path]

    def test_extra_source_missing_source_and_changed_config_refuse(self):
        extra = self.root / "src" / "extra.py"
        extra.write_text("# extra", encoding="utf-8")
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, COMMIT)
        extra.unlink()
        (self.root / "config.json").write_text("changed", encoding="utf-8")
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, COMMIT)
        (self.root / "config.json").unlink()
        with self.assertRaises(S.SupervisorRefused):
            S.verify_deployment(self.root, COMMIT)

    def test_runtime_state_and_logs_are_allowed_but_never_used_as_commands(self):
        for name in ("state/runtime.json", "logs/stdout.log"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("do not execute this data", encoding="utf-8")
        self.assertTrue(S.verify_deployment(self.root, COMMIT)["verified"])

    def test_real_calendar_cache_writer_and_its_atomic_tmp_are_runtime_data(self):
        from src import util
        cache_dir = self.root / "data" / "cache"
        original_replace = util.os.replace
        temporary_seen = []
        def replace(source, destination):
            temporary_seen.append(Path(source).relative_to(self.root).as_posix())
            self.assertTrue(Path(source).is_file())
            self.assertTrue(S.verify_deployment(self.root, COMMIT)["verified"])
            original_replace(source, destination)
        with patch.object(util, "CACHE_DIR", cache_dir), patch.object(util.os, "replace", side_effect=replace):
            data, _, cached = util.cached_payload("economic_calendar.json", 3600,
                                                  lambda: [{"title": "offline fixture"}])
        self.assertEqual(data, [{"title": "offline fixture"}])
        self.assertFalse(cached)
        self.assertEqual(temporary_seen, ["data/cache/economic_calendar.json.tmp"])
        self.assertTrue(S.verify_deployment(self.root, COMMIT)["verified"])

    def test_calendar_cache_exemption_does_not_hide_extra_code_or_data(self):
        cache_dir = self.root / "data" / "cache"
        cache_dir.mkdir(parents=True)
        for name in ("evil.py", "economic_calendar.json.py", "other.json"):
            extra = cache_dir / name
            extra.write_text("not allowlisted", encoding="utf-8")
            with self.assertRaises(S.SupervisorRefused):
                S.verify_deployment(self.root, COMMIT)
            extra.unlink()

    def test_runtime_calendar_cache_cannot_be_hashed_as_static_source(self):
        for name in S.RUNTIME_DATA_FILES:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
            self.manifest["files"][name] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.save_manifest()
            with self.assertRaises(S.SupervisorRefused) as caught:
                S.verify_deployment(self.root, COMMIT)
            self.assertEqual(caught.exception.code, "MANIFEST_FILE_PATH_INVALID")
            del self.manifest["files"][name]

    def test_runtime_calendar_cache_symlink_is_never_exempted(self):
        path = self.root / "data" / "cache" / "economic_calendar.json"
        path.parent.mkdir(parents=True)
        try:
            path.symlink_to(self.root / "config.json")
        except (OSError, NotImplementedError):
            # Windows can disallow symlink creation without Developer Mode;
            # simulate the filesystem flag so rejection is still exercised.
            path.write_text("{}", encoding="utf-8")
            original = Path.is_symlink
            with patch.object(Path, "is_symlink", autospec=True,
                              side_effect=lambda current: current == path or original(current)):
                with self.assertRaises(S.SupervisorRefused):
                    S.verify_deployment(self.root, COMMIT)
        else:
            with self.assertRaises(S.SupervisorRefused):
                S.verify_deployment(self.root, COMMIT)

    def test_duplicate_manifest_keys_are_refused(self):
        path = self.root / S.MANIFEST
        path.write_text('{"schema":1,"schema":1}', encoding="utf-8")
        with self.assertRaises(S.SupervisorRefused) as caught:
            S.verify_deployment(self.root, COMMIT)
        self.assertEqual(caught.exception.code, "MANIFEST_DUPLICATE_KEY")

    def test_process_start_exception_is_sanitized_and_not_retried(self):
        def fail(*args, **kwargs):
            raise RuntimeError("SECRET_MARKER_MUST_NOT_APPEAR")
        supervisor = self.supervisor([])
        supervisor.process_factory = fail
        self.assertEqual(supervisor.run(enable_demo_auto=True), 2)
        self.assertEqual(supervisor.last_status["code"], "CHILD_START_FAILED")
        self.assertNotIn("SECRET_MARKER", supervisor.state_path.read_text(encoding="utf-8"))
        self.assertEqual(self.waits, [])

    def test_uncertain_poll_never_launches_a_replacement(self):
        class UncertainChild(FakeChild):
            def poll(self):
                raise RuntimeError("untrusted exception")
        supervisor = self.supervisor([])
        supervisor.process_factory = lambda *args, **kwargs: UncertainChild([])
        self.assertEqual(supervisor.run(enable_demo_auto=True), 2)
        self.assertEqual(supervisor.last_status["code"], "CHILD_STATUS_UNCERTAIN")
        self.assertEqual(self.waits, [])


if __name__ == "__main__":
    unittest.main()
