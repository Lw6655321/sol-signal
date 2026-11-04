"""Local supervisor for a frozen, verified DEMO deployment; no account API IO.

The manifest contains data only. Commands, interpreter and launch arguments are
fixed by this source. Default invocation verifies files and exits. This module
does not install scheduled tasks, read credentials, or terminate its child.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
# Import only this deployment's local gates, including under Python -I.
sys.path.insert(0, str(ROOT))
MANIFEST = "DEPLOYMENT_MANIFEST.json"
COMMIT_MARKER = "DEPLOYED_COMMIT"
ENTRY = "scripts/demo_auto_run.py"
SELF = "scripts/demo_auto_supervisor.py"
CREATE_NO_WINDOW = 0x08000000
REQUIRED = {ENTRY, SELF, "config.json"}
RUNTIME_DIRS = {"state", "logs"}
# These exact files are emitted by util.cached_payload/write_json. The rest of
# data/ remains part of the immutable deployment inventory.
RUNTIME_DATA_FILES = frozenset({"data/cache/economic_calendar.json",
                                "data/cache/economic_calendar.json.tmp"})


class SupervisorRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise SupervisorRefused("MANIFEST_DUPLICATE_KEY")
        result[key] = value
    return result


def _file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _inventory_error(error):
    raise SupervisorRefused("DEPLOYMENT_INVENTORY_UNREADABLE")


def verify_deployment(root, expected_commit):
    """Validate the pinned commit, entire file table and absence of additions."""
    root = Path(root).resolve(strict=True)
    if not isinstance(expected_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise SupervisorRefused("EXPECTED_COMMIT_INVALID")
    manifest_path, marker_path = root / MANIFEST, root / COMMIT_MARKER
    try:
        if manifest_path.is_symlink() or marker_path.is_symlink():
            raise SupervisorRefused("MANIFEST_PATH_INVALID")
        if manifest_path.stat().st_size > 4_000_000 or marker_path.stat().st_size > 64:
            raise SupervisorRefused("MANIFEST_TOO_LARGE")
        data = json.loads(manifest_path.read_text(encoding="utf-8-sig"), object_pairs_hook=_pairs)
        marker = marker_path.read_text(encoding="ascii").strip()
    except SupervisorRefused:
        raise
    except Exception:
        raise SupervisorRefused("MANIFEST_UNREADABLE") from None
    if (not isinstance(data, dict) or set(data) != {"schema", "environment", "commit", "files"}
            or type(data.get("schema")) is not int or data["schema"] != 1
            or data.get("environment") != "DEMO"
            or data.get("commit") != expected_commit or marker != expected_commit):
        raise SupervisorRefused("DEPLOYMENT_COMMIT_MISMATCH")
    files = data.get("files")
    if not isinstance(files, dict) or not REQUIRED.issubset(files) or not any(
            isinstance(name, str) and name.startswith("src/") and name.endswith(".py") for name in files):
        raise SupervisorRefused("MANIFEST_FILES_INVALID")
    for name, digest in files.items():
        if (not isinstance(name, str) or not name or "\\" in name or ":" in name
                or name.startswith("/") or "\x00" in name
                or PurePosixPath(name).as_posix() != name
                or any(part in {"", ".", ".."} for part in name.split("/"))
                or name in {MANIFEST, COMMIT_MARKER}
                or name in RUNTIME_DATA_FILES
                or PurePosixPath(name).parts[0] in RUNTIME_DIRS
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise SupervisorRefused("MANIFEST_FILE_PATH_INVALID")
        path = root / name
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root)
            if not resolved.is_file() or any((root.joinpath(*PurePosixPath(name).parts[:i])).is_symlink()
                                             for i in range(1, len(PurePosixPath(name).parts) + 1)):
                raise SupervisorRefused("MANIFEST_FILE_PATH_INVALID")
            if _file_digest(resolved) != digest:
                raise SupervisorRefused("DEPLOYMENT_HASH_MISMATCH")
        except SupervisorRefused:
            raise
        except Exception:
            raise SupervisorRefused("DEPLOYMENT_FILE_UNREADABLE") from None
    try:
        for folder, directories, filenames in os.walk(root, followlinks=False, onerror=_inventory_error):
            folder = Path(folder)
            if folder == root:
                directories[:] = [name for name in directories if name not in RUNTIME_DIRS]
            for name in directories:
                path = folder / name
                if (path.relative_to(root).as_posix() in RUNTIME_DATA_FILES
                        or path.is_symlink() or not path.resolve(strict=True).is_relative_to(root)):
                    raise SupervisorRefused("DEPLOYMENT_EXTRA_FILE")
            for name in filenames:
                path = folder / name
                relative = path.relative_to(root).as_posix()
                if (path.is_symlink() or not path.resolve(strict=True).is_relative_to(root)
                        or relative not in files and relative not in {MANIFEST, COMMIT_MARKER}
                        and relative not in RUNTIME_DATA_FILES):
                    raise SupervisorRefused("DEPLOYMENT_EXTRA_FILE")
    except SupervisorRefused:
        raise
    except Exception:
        raise SupervisorRefused("DEPLOYMENT_INVENTORY_UNREADABLE") from None
    return {"verified": True, "environment": "DEMO", "commit": expected_commit, "file_count": len(files)}


@contextmanager
def _lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        handle.seek(0, 2)
        if not handle.tell():
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SupervisorRefused("SUPERVISOR_ALREADY_RUNNING") from None
        yield
    finally:
        handle.close()


class DemoAutoSupervisor:
    def __init__(self, root, expected_commit, *, process_factory=None, sleep=None, clock=None,
                 windows=None, max_restarts=3):
        if type(max_restarts) is not int or not 0 <= max_restarts <= 3:
            raise SupervisorRefused("RESTART_LIMIT_INVALID")
        self.root = Path(root).resolve(strict=True)
        self.commit = expected_commit
        self.process_factory = process_factory or subprocess.Popen
        self.sleep = sleep or time.sleep
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.windows = os.name == "nt" if windows is None else windows
        self.max_restarts = max_restarts
        self.state_root = self.root / "state" / "demo-auto"
        self.stop_path = self.state_root / "STOP_NEW_ENTRIES"
        self.state_path = self.state_root / "supervisor.json"
        self.last_status = {}

    def _runtime_scope(self, path):
        try:
            path.resolve().relative_to(self.root)
            relative = path.relative_to(self.root)
            if any(self.root.joinpath(*relative.parts[:i]).is_symlink()
                   for i in range(1, len(relative.parts) + 1)):
                raise ValueError()
        except (ValueError, OSError):
            raise SupervisorRefused("SUPERVISOR_RUNTIME_PATH_INVALID") from None

    def _record(self, status, code, **details):
        self._runtime_scope(self.state_root)
        self.last_status = {"environment": "DEMO", "commit": self.commit,
                            "observed_at_ms": self.clock(), "status": status, "code": code, **details}
        self.state_root.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name("supervisor-" + uuid.uuid4().hex + ".tmp")
        try:
            temporary.write_text(json.dumps(self.last_status, sort_keys=True), encoding="utf-8")
            os.replace(temporary, self.state_path)
        finally:
            temporary.unlink(missing_ok=True)

    def run(self, *, enable_demo_auto=False):
        if enable_demo_auto is not True:
            raise SupervisorRefused("DEMO_AUTO_EXPLICIT_ENABLE_REQUIRED")
        verify_deployment(self.root, self.commit)
        self._assert_activation()
        self._runtime_scope(self.state_root / "supervisor.lock")
        with _lock(self.state_root / "supervisor.lock"):
            return self._run_locked()

    def _assert_activation(self):
        from src.demo_successor_gate import SuccessorGateRefused, assert_activated_generation
        try:
            assert_activated_generation(self.root, self.commit)
        except SuccessorGateRefused as error:
            raise SupervisorRefused(error.code) from None

    def _run_locked(self):
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex
        log_dir = self.root / "logs" / "demo-auto-supervisor" / ("run-" + run_id)
        self._runtime_scope(log_dir)
        log_dir.mkdir(parents=True, exist_ok=False)
        restarts = 0
        launches = 0
        while True:
            if self.stop_path.exists():
                self._record("STOPPED", "STOP_NEW_ENTRIES", launches=launches, restarts=restarts)
                return 0
            try:
                verify_deployment(self.root, self.commit)
                self._assert_activation()
            except SupervisorRefused as error:
                self._record("REFUSED", error.code, launches=launches, restarts=restarts)
                return 2
            if self.stop_path.exists():
                self._record("STOPPED", "STOP_NEW_ENTRIES", launches=launches, restarts=restarts)
                return 0
            launches += 1
            stdout_path = log_dir / (f"child-{launches:03d}-" + uuid.uuid4().hex + ".stdout.log")
            stderr_path = stdout_path.with_name(stdout_path.name.replace(".stdout.log", ".stderr.log"))
            self._record("STARTING", "VERIFIED_CHILD_START", launches=launches, restarts=restarts)
            with stdout_path.open("xb") as output, stderr_path.open("xb") as error_output:
                if self.stop_path.exists():
                    self._record("STOPPED", "STOP_NEW_ENTRIES", launches=launches - 1, restarts=restarts)
                    return 0
                try:
                    child = self.process_factory(
                        [sys.executable, "-X", "utf8", "-B", str(self.root / ENTRY), "--run", "--enable-demo-auto"],
                        cwd=str(self.root), stdin=subprocess.DEVNULL, stdout=output, stderr=error_output,
                        creationflags=CREATE_NO_WINDOW if self.windows else 0, close_fds=True)
                except Exception:
                    self._record("FAILED", "CHILD_START_FAILED", launches=launches, restarts=restarts)
                    return 2  # Unknown launch outcome is never blindly retried.
                pid = child.pid if type(getattr(child, "pid", None)) is int else None
                state_write_failed = False
                while True:
                    try:
                        result = child.poll()
                    except Exception:
                        self._record("UNCERTAIN", "CHILD_STATUS_UNCERTAIN", pid=pid,
                                     launches=launches, restarts=restarts)
                        return 2  # Preserve the child; don't launch a replacement.
                    if result is not None:
                        if type(result) is not int:
                            self._record("UNCERTAIN", "CHILD_STATUS_UNCERTAIN", pid=pid)
                            return 2
                        break
                    try:
                        self._record("STOPPING" if self.stop_path.exists() else "RUNNING",
                                     "CHILD_DRAINING" if self.stop_path.exists() else "CHILD_RUNNING",
                                     pid=pid, launches=launches, restarts=restarts)
                    except OSError:
                        state_write_failed = True  # Keep supervising the active child.
                    self.sleep(2)
            if result == 0:
                self._record("EXITED", "CHILD_EXITED_NORMALLY", pid=pid, return_code=0,
                             launches=launches, restarts=restarts)
                return 0
            if state_write_failed:
                self._record("FAILED", "SUPERVISOR_STATE_WRITE_FAILED", return_code=result)
                return 2
            if self.stop_path.exists():
                self._record("STOPPED", "STOP_NEW_ENTRIES", return_code=result,
                             launches=launches, restarts=restarts)
                return 0
            if restarts >= self.max_restarts:
                self._record("FAILED", "RESTART_LIMIT_REACHED", return_code=result,
                             launches=launches, restarts=restarts)
                return 2
            delay = 5 * (2 ** restarts)
            restarts += 1
            self._record("BACKOFF", "CHILD_EXITED_ABNORMALLY", return_code=result,
                         launches=launches, restarts=restarts, retry_after_seconds=delay)
            self.sleep(delay)


def main(argv=None):
    parser = argparse.ArgumentParser(description="核验冻结部署；须双显式参数才监督模拟盘进程")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--enable-demo-auto", action="store_true")
    parser.add_argument("--expected-commit", required=True)
    args = parser.parse_args(argv)
    if args.run != args.enable_demo_auto:
        print("DEMO_SUPERVISOR_RUN_AND_ENABLE_REQUIRED")
        return 2
    try:
        if not args.run:
            print(json.dumps(verify_deployment(ROOT, args.expected_commit), sort_keys=True))
            return 0
        return DemoAutoSupervisor(ROOT, args.expected_commit).run(enable_demo_auto=True)
    except SupervisorRefused as error:
        print(error.code)
        return 2
    except Exception:
        print("DEMO_SUPERVISOR_LOCAL_FAILURE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
