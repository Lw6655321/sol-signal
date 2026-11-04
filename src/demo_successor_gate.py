"""Local gates for a prepared/activated, evidence-preserving DEMO generation.

No credentials, account requests, execution objects or state writes occur here.
An activation receipt records a past controlled activation; it is never a
current risk snapshot and never replaces ordinary per-entry admission.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from decimal import localcontext
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time

RECOVERY = "state/demo-auto/recovery"
PREPARATION_STOP = b"SUCCESSOR_PREPARED\n"
SIGNAL = "DEMO-TEST-SMOKE-1792501260297"
MAX_FILE = 64 * 1024 * 1024
MAX_TOTAL = 256 * 1024 * 1024
_OWNED_PROCESS_LOCKS = ContextVar("successor_owned_process_locks", default=None)
PREPARED_KEYS = frozenset("schema environment phase generation_id target_root target_commit target_manifest_sha256 account_hash authorization_sha256 source_root source_commit source_manifest_sha256 source_files source_directories signal_id proof_path proof_sha256 original_fail_path original_fail_sha256 seed_controller_sha256 seed_router_sha256".split())
ACTIVATION_KEYS = frozenset("schema environment phase generation_id target_root target_commit prepared_sha256 activated_at_ms ordinary_provider_verified diagnostic_only".split())


class SuccessorGateRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code):
    if not condition:
        raise SuccessorGateRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _hash(value, size=64):
    return type(value) is str and re.fullmatch("[0-9a-f]{" + str(size) + "}", value) is not None


def _relative(value):
    reserved = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} | {
        prefix + digit for prefix in ("COM", "LPT") for digit in "123456789¹²³"}
    _require(type(value) is str and 0 < len(value) <= 512 and "\\" not in value
             and PurePosixPath(value).as_posix() == value
             and all(part not in ("", ".", "..") and part == part.rstrip(" .")
                     and part.split(".", 1)[0].upper() not in reserved
                     and not any(ord(c) < 32 or ord(c) == 127 or c in '<>:"|?*' for c in part)
                     for part in value.split("/")), "SUCCESSOR_PATH_INVALID")
    return value


def _plain(path, *, directory=False):
    path = Path(path)
    _require(path.is_absolute(), "SUCCESSOR_PATH_INVALID")
    chain = list(reversed(path.parents)) + [path]
    for index, component in enumerate(chain):
        info = component.lstat()
        _require(not stat.S_ISLNK(info.st_mode)
                 and not (getattr(info, "st_file_attributes", 0) & 0x400),
                 "SUCCESSOR_PATH_INVALID")
        last = index == len(chain)-1
        _require(stat.S_ISDIR(info.st_mode) if not last or directory else stat.S_ISREG(info.st_mode),
                 "SUCCESSOR_PATH_INVALID")
        if last and not directory:
            _require(info.st_nlink == 1, "SUCCESSOR_PATH_INVALID")
    _require(str(path.resolve(strict=True)) == str(path), "SUCCESSOR_PATH_INVALID")
    return path


def _read(path):
    path = _plain(path)
    size = path.stat().st_size
    _require(0 <= size <= MAX_FILE, "SUCCESSOR_FILE_TOO_LARGE")
    held = (_OWNED_PROCESS_LOCKS.get() or {}).get(path)
    if held is not None:
        return _held_lock_bytes(held)
    with path.open("rb") as handle:
        raw = handle.read(size+1)
        if len(raw) == size+1 and len(raw) <= MAX_FILE:
            # Growth after stat still reads only the original bounded prefix.
            raw += handle.read(MAX_FILE+1-len(raw))
    _require(len(raw) <= MAX_FILE, "SUCCESSOR_FILE_TOO_LARGE")
    return raw


@dataclass(frozen=True, slots=True)
class _OwnedProcessLock:
    path: Path
    handle: object = field(repr=False)
    original: bytes = field(repr=False)
    identity: tuple[int, int, int, int] = field(repr=False)


def _lock_identity(info):
    _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
             and type(info.st_ino) is int and info.st_ino > 0
             and 0 < info.st_size <= MAX_FILE
             and not (getattr(info, "st_file_attributes", 0) & 0x400),
             "SUCCESSOR_PROCESS_LOCK_INVALID")
    return info.st_dev, info.st_ino, info.st_size, info.st_nlink


def _held_lock_bytes(held):
    """Read the locked byte through its owner; never reopen or skip the file."""
    try:
        path = _plain(held.path)
        _require(path.name == "process.lock" and path.parent.name == "demo-auto"
                 and path.parent.parent.name == "state"
                 and _lock_identity(path.stat()) == held.identity
                 and _lock_identity(os.fstat(held.handle.fileno())) == held.identity,
                 "SUCCESSOR_PROCESS_LOCK_CHANGED")
        position = held.handle.tell()
        try:
            held.handle.seek(0)
            raw = held.handle.read(held.identity[2]+1)
        finally:
            held.handle.seek(position)
        _require(raw == held.original
                 and _lock_identity(path.stat()) == held.identity
                 and _lock_identity(os.fstat(held.handle.fileno())) == held.identity,
                 "SUCCESSOR_PROCESS_LOCK_CHANGED")
        return raw
    except SuccessorGateRefused:
        raise
    except (OSError, ValueError, AttributeError):
        raise SuccessorGateRefused("SUCCESSOR_PROCESS_LOCK_UNREADABLE") from None


@contextmanager
def owned_process_locks(roots):
    """Own existing byte-zero process locks without changing any file bytes.

Only these exact, canonical ``state/demo-auto/process.lock`` paths can use an
already-owned handle during full inventory reads. Disjoint nested scopes work;
duplicate/nested ownership of the same path, aliases, missing/empty files,
hardlinks and reparse points refuse. Closing each handle releases its OS lock.
    """
    _require(type(roots) in (tuple, list) and 1 <= len(roots) <= 16,
             "SUCCESSOR_PROCESS_LOCK_ROOTS_INVALID")
    inherited = _OWNED_PROCESS_LOCKS.get() or {}
    handles, token = [], None
    try:
        try:
            paths = [_plain(_root(str(root)) / "state/demo-auto/process.lock") for root in roots]
            _require(len(set(paths)) == len(paths) and not set(paths).intersection(inherited),
                     "SUCCESSOR_PROCESS_LOCK_DUPLICATE")
            paths.sort(key=str)
            for path in paths:
                identity = _lock_identity(path.stat())
                original = _read(path)
                handle = path.open("r+b", buffering=0)
                try:
                    _require(_lock_identity(os.fstat(handle.fileno())) == identity,
                             "SUCCESSOR_PROCESS_LOCK_CHANGED")
                    handle.seek(0)
                    try:
                        if os.name == "nt":
                            import msvcrt
                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        else:
                            import fcntl
                            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        raise SuccessorGateRefused("SUCCESSOR_PROCESS_LOCKED") from None
                    held = _OwnedProcessLock(path, handle, original, identity)
                    _held_lock_bytes(held)
                except BaseException:
                    handle.close()
                    raise
                handles.append(held)
            token = _OWNED_PROCESS_LOCKS.set({**inherited, **{held.path: held for held in handles}})
        except SuccessorGateRefused:
            raise
        except OSError:
            raise SuccessorGateRefused("SUCCESSOR_PROCESS_LOCK_UNREADABLE") from None
        yield
    finally:
        try:
            for held in handles:
                _held_lock_bytes(held)
        finally:
            if token is not None:
                _OWNED_PROCESS_LOCKS.reset(token)
            for held in reversed(handles):
                held.handle.close()


def _pairs(items):
    result = {}
    for key, value in items:
        _require(key not in result, "SUCCESSOR_JSON_DUPLICATE")
        result[key] = value
    return result


def _json(raw):
    _require(0 < len(raw) <= 4_000_000, "SUCCESSOR_JSON_INVALID")
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                        parse_constant=lambda _: (_ for _ in ()).throw(SuccessorGateRefused("SUCCESSOR_JSON_INVALID")))
    def check(value, depth=0):
        _require(depth <= 32, "SUCCESSOR_JSON_INVALID")
        if type(value) is dict:
            for item in value.values():
                check(item, depth+1)
        elif type(value) is list:
            for item in value:
                check(item, depth+1)
        else:
            _require(type(value) in (str, int, bool, type(None)), "SUCCESSOR_JSON_INVALID")
    check(result)
    return result


def _root(value):
    _require(type(value) is str and str(Path(value)) == value and Path(value).is_absolute(),
             "SUCCESSOR_ROOT_INVALID")
    return _plain(Path(value), directory=True)


def _verify_code(root, commit, manifest_sha):
    # Delayed import avoids importing a CLI/execution module during explanation.
    from scripts.demo_auto_supervisor import verify_deployment
    _require(_sha(_read(root / "DEPLOYMENT_MANIFEST.json")) == manifest_sha,
             "SUCCESSOR_DEPLOYMENT_MISMATCH")
    _require(_read(root / "DEPLOYED_COMMIT").decode("ascii").strip() == commit,
             "SUCCESSOR_DEPLOYMENT_MISMATCH")
    try:
        verify_deployment(root, commit)
    except Exception:
        raise SuccessorGateRefused("SUCCESSOR_DEPLOYMENT_INVALID") from None


def _inventory(root, *, source_scope=False):
    files, directories, total = {}, [], 0
    bases = [root / "state", root / "logs"] if source_scope else [root]
    for base in bases:
        _plain(base, directory=True)
        def unreadable(_error):
            raise SuccessorGateRefused("SUCCESSOR_INVENTORY_UNREADABLE")
        for folder, dirs, names in os.walk(base, followlinks=False, onerror=unreadable):
            current = _plain(Path(folder), directory=True)
            relative = current.relative_to(root).as_posix()
            if relative != ".":
                directories.append(_relative(relative))
            for name in dirs:
                _plain(current / name, directory=True)
            for name in names:
                path = current / name
                relative = _relative(path.relative_to(root).as_posix())
                raw = _read(path)
                total += len(raw)
                _require(total <= MAX_TOTAL and len(files) < 4096, "SUCCESSOR_INVENTORY_TOO_LARGE")
                files[relative] = _sha(raw)
    if source_scope:
        for name in ("DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json"):
            files[name] = _sha(_read(root / name))
    _require(len({name.casefold() for name in files}) == len(files)
             and len({name.casefold() for name in directories}) == len(directories), "SUCCESSOR_PATH_INVALID")
    return files, sorted(directories)


@dataclass(frozen=True)
class PreparedGeneration:
    generation_id: str
    target_root: Path
    target_commit: str
    prepared_sha256: str
    source_root: Path
    account_hash: str = field(repr=False)


def _prepared(root, expected_commit):
    root = _root(str(root))
    recovery = _plain(root / RECOVERY, directory=True)
    raw = _read(recovery / "prepared.json")
    prepared = _json(raw)
    _require(type(prepared) is dict and set(prepared) == PREPARED_KEYS
             and type(prepared["schema"]) is int and prepared["schema"] == 1
             and prepared["environment"] == "DEMO" and prepared["phase"] == "PREPARED"
             and prepared["signal_id"] == SIGNAL and _hash(prepared["generation_id"], 32),
             "SUCCESSOR_PREPARED_INVALID")
    for key in ("target_commit", "source_commit"):
        _require(_hash(prepared[key], 40), "SUCCESSOR_PREPARED_INVALID")
    for key in ("target_manifest_sha256", "account_hash", "authorization_sha256", "source_manifest_sha256",
                "proof_sha256", "original_fail_sha256", "seed_controller_sha256", "seed_router_sha256"):
        _require(_hash(prepared[key]), "SUCCESSOR_PREPARED_INVALID")
    _require(prepared["target_root"] == str(root)
             and (expected_commit is None or expected_commit == prepared["target_commit"]),
             "SUCCESSOR_TARGET_MISMATCH")
    current_state = _plain(root / "state/demo-auto", directory=True)
    _require(not (current_state / "CONTROLLER_HALT").exists()
             and not any(current_state.glob("HALT-*")), "SUCCESSOR_CURRENT_HALTED")
    for name in ("controller.sqlite3", "router.sqlite3"):
        _require(not any((current_state / (name+suffix)).exists() for suffix in ("-wal", "-shm", "-journal")),
                 "SUCCESSOR_JOURNAL_SIDECAR")
    source = _root(prepared["source_root"])
    _require(source != root and source not in root.parents and root not in source.parents,
             "SUCCESSOR_SOURCE_INVALID")
    _verify_code(root, prepared["target_commit"], prepared["target_manifest_sha256"])
    _verify_code(source, prepared["source_commit"], prepared["source_manifest_sha256"])
    files, dirs = prepared["source_files"], prepared["source_directories"]
    _require(type(files) is dict and 1 <= len(files) <= 4096 and type(dirs) is list
             and dirs == sorted(set(dirs)) and len(dirs) <= 4096, "SUCCESSOR_INVENTORY_INVALID")
    for name, digest in files.items():
        _relative(name)
        _require(_hash(digest) and (name in {"DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json"}
                 or name.startswith(("state/", "logs/"))), "SUCCESSOR_INVENTORY_INVALID")
    for name in dirs:
        _relative(name)
        _require(name in {"state", "logs"} or name.startswith(("state/", "logs/")), "SUCCESSOR_INVENTORY_INVALID")
    base = "state/demo-auto/"
    flags = {base+"process.lock", base+"STOP_NEW_ENTRIES", base+"CONTROLLER_HALT",
             base+"HALT-"+_sha(b"DEMO-AUTO-SOL-V1")[:12]}
    required = flags | {base+"controller.sqlite3", base+"router.sqlite3", base+"account-binding.json",
                        base+"auto-authorization.json", "DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json"}
    proof, fail = (_relative(prepared[key]) for key in ("proof_path", "original_fail_path"))
    _require(proof.startswith("logs/") and proof.endswith("/halted-exit-"+prepared["proof_sha256"]+".json")
             and fail.startswith("logs/technical-tests/") and fail.endswith("/report.json")
             and required | {proof, fail} <= set(files), "SUCCESSOR_EVIDENCE_MISSING")
    _require(files[proof] == prepared["proof_sha256"] and files[fail] == prepared["original_fail_sha256"]
             and files[base+"auto-authorization.json"] == prepared["authorization_sha256"],
             "SUCCESSOR_EVIDENCE_MISMATCH")
    _require(_inventory(source, source_scope=True) == (files, dirs), "SUCCESSOR_SOURCE_CHANGED")
    copied = _plain(recovery / "source", directory=True)
    _require(_inventory(copied) == (files, dirs), "SUCCESSOR_HISTORY_CHANGED")
    for name in flags:
        _require(bool(_read(copied / name)), "SUCCESSOR_SOURCE_HALT_INVALID")
    binding = _json(_read(copied / (base+"account-binding.json")))
    _require(binding == {"environment": "DEMO", "account_hash": prepared["account_hash"]},
             "SUCCESSOR_ACCOUNT_MISMATCH")
    for name in ("account-binding.json", "auto-authorization.json"):
        _require(_read(root / base / name) == _read(copied / base / name), "SUCCESSOR_AUTHORIZATION_CHANGED")
    seed_controller = _read(recovery / "seed/controller.sqlite3")
    seed_router = _read(recovery / "seed/router.sqlite3")
    _require(_sha(seed_controller) == prepared["seed_controller_sha256"]
             and _sha(seed_router) == prepared["seed_router_sha256"], "SUCCESSOR_SEED_CHANGED")
    from .demo_successor import validate_journal_extension, validate_seed_against_source
    try:
        validate_seed_against_source(seed_controller, seed_router,
            _read(copied / (base+"controller.sqlite3")), _read(copied / (base+"router.sqlite3")))
        from . import demo_halted_resolution as R
        seed = R._database(seed_controller)
        record = R._json(R._rows(seed, "halted_resolutions", ("signal", "record"))[0]["record"].encode("ascii"))
        evidence = tuple(R.EvidenceFile(name, _read(copied / name)) for name in sorted(files)
                         if name.startswith((R.STATE, "logs/")))
        _require(record["source_inventory_sha256"] == R.archive_digest(evidence)
                 and record["source_commit"] == prepared["source_commit"]
                 and record["target_commit"] == prepared["target_commit"]
                 and record["account_hash"] == prepared["account_hash"]
                 and record["proof_sha256"] == prepared["proof_sha256"]
                 and record["original_fail_sha256"] == prepared["original_fail_sha256"],
                 "SUCCESSOR_RESOLUTION_CONTEXT_INVALID")
        with localcontext() as context:
            # Match the archived validator's exact decimal arithmetic. Caller
            # precision must not round a changed fill price back to its receipt.
            context.prec = 200
            intent = R._intent(record["original_controller_signal"]["intent"].encode("utf-8"))
            route = R._json(record["original_router_row"]["data"].encode("utf-8"))
            R._raw_closure(R._json(_read(copied / proof)), intent, route)
        auth = R._json(_read(copied / (base+"auto-authorization.json")))
        _require(auth.get("environment") == "DEMO" and auth.get("account_hash") == prepared["account_hash"]
                 and auth.get("source") == "user-approved-demo-activation"
                 and auth.get("scope") == "SOL-USDT-SWAP_EXISTING_RISK"
                 and R._integer(auth.get("authorization_recorded_at_ms")) <= intent["created_at_ms"],
                 "SUCCESSOR_AUTHORIZATION_CHANGED")
        validate_journal_extension(seed_controller, seed_router,
            _read(root / (base+"controller.sqlite3")), _read(root / (base+"router.sqlite3")))
    except SuccessorGateRefused:
        raise
    except Exception:
        raise SuccessorGateRefused("SUCCESSOR_JOURNAL_INVALID") from None
    expected_files = {"prepared.json", "seed/controller.sqlite3", "seed/router.sqlite3"} | {
        "source/"+name for name in files}
    if (recovery / "activation.json").exists():
        expected_files.add("activation.json")
    expected_dirs = {"seed", "source"} | {"source/"+name for name in dirs}
    actual_files, actual_dirs = _inventory(recovery)
    _require(set(actual_files) == expected_files and set(actual_dirs) == expected_dirs,
             "SUCCESSOR_RECOVERY_EXTRA_PATH")
    return PreparedGeneration(prepared["generation_id"], root, prepared["target_commit"],
                              _sha(raw), source, prepared["account_hash"])


def assert_prepared_generation(root, expected_commit=None):
    """Verify local preparation only; grants no account write or activation."""
    try:
        return _prepared(root, expected_commit)
    except SuccessorGateRefused:
        raise
    except Exception:
        raise SuccessorGateRefused("SUCCESSOR_PREPARATION_UNREADABLE") from None


def assert_activated_generation(root, expected_commit=None):
    """Verify the preserved generation before credentials/owner recovery.

    The caller still holds its process lock and performs ordinary local HALT
    checks. Operator STOP may drain owned work; preparation STOP never may.
    """
    prepared = assert_prepared_generation(root, expected_commit)
    try:
        activation = _json(_read(prepared.target_root / RECOVERY / "activation.json"))
        _require(type(activation) is dict and set(activation) == ACTIVATION_KEYS
                 and type(activation["schema"]) is int and activation["schema"] == 1
                 and activation["environment"] == "DEMO" and activation["phase"] == "ACTIVATED"
                 and activation["generation_id"] == prepared.generation_id
                 and activation["target_root"] == str(prepared.target_root)
                 and activation["target_commit"] == prepared.target_commit
                 and activation["prepared_sha256"] == prepared.prepared_sha256
                 and type(activation["activated_at_ms"]) is int
                 and 0 < activation["activated_at_ms"] <= time.time_ns() // 1_000_000
                 and activation["ordinary_provider_verified"] is True
                 and activation["diagnostic_only"] is False, "SUCCESSOR_ACTIVATION_INVALID")
        stop = prepared.target_root / "state/demo-auto/STOP_NEW_ENTRIES"
        if stop.exists():
            _require(_read(stop) != PREPARATION_STOP, "SUCCESSOR_ACTIVATION_INCOMPLETE")
        return prepared
    except SuccessorGateRefused:
        raise
    except Exception:
        raise SuccessorGateRefused("SUCCESSOR_ACTIVATION_UNREADABLE") from None
