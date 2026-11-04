"""Build a separate stopped DEMO generation without account or execution IO.

All original state, logs, empty directories and deployment markers are copied
into the immutable source archive. Only the new generation's working journals
receive the proven projection. Preparation never creates activation authority.
Partial builds are retained and remain non-executable for inspection.
"""
from __future__ import annotations

import ast
import hashlib
import os
from pathlib import Path

from . import demo_halted_resolution as R
from . import demo_successor as P
from . import demo_successor_gate as G


class PreparationRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(ok, code):
    if not ok:
        raise PreparationRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _new_path(path, workspace):
    path = Path(path)
    _require(path.is_absolute() and path == path.absolute() and path.resolve() == path
             and path != workspace and path.is_relative_to(workspace), "PREPARE_PATH_INVALID")
    _require(not path.exists() and not path.is_symlink(), "PREPARE_TARGET_EXISTS")
    G._relative(path.relative_to(workspace).as_posix())
    G._plain(path.parent, directory=True)
    return path


def _evidence(root, files):
    return tuple(R.EvidenceFile(name, G._read(root / name)) for name in sorted(files)
                 if name.startswith((R.STATE, "logs/")))


def _code(files, raw_manifest, commit):
    _require(type(files) is dict and files and G._hash(commit, 40)
             and type(raw_manifest) is bytes, "PREPARE_CODE_INVALID")
    manifest = G._json(raw_manifest)
    _require(type(manifest) is dict and set(manifest) == {"schema", "environment", "commit", "files"}
             and type(manifest["schema"]) is int and manifest["schema"] == 1
             and manifest["environment"] == "DEMO" and manifest["commit"] == commit
             and type(manifest["files"]) is dict, "PREPARE_CODE_INVALID")
    result = {}
    for name, raw in files.items():
        G._relative(name)
        _require(type(raw) is bytes and len(raw) <= G.MAX_FILE
                 and name not in {"DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json",
                     "scripts/capture_risk_baseline.py", "tests/test_capture_risk_baseline.py"}
                 and not name.startswith(("state/", "logs/", "data/cache/")), "PREPARE_CODE_INVALID")
        if name.endswith(".py"):
            ast.parse(raw, filename=name)
        result[name] = raw
    _require(len({name.casefold() for name in result}) == len(result)
             and all("/".join(name.split("/")[:i]) not in result
                     for name in result for i in range(1, len(name.split("/"))))
             and manifest["files"] == {name: _sha(raw) for name, raw in result.items()},
             "PREPARE_CODE_INVALID")
    _require({"config.json", "scripts/demo_auto_run.py", "scripts/demo_auto_supervisor.py",
              "src/demo_successor_gate.py", "src/demo_successor.py"} <= set(result), "PREPARE_CODE_INVALID")
    result["DEPLOYED_COMMIT"] = (commit + "\n").encode("ascii")
    result["DEPLOYMENT_MANIFEST.json"] = raw_manifest
    return result


def _parents(files, extra=()):
    result = set(extra)
    for name in files:
        result.update("/".join(name.split("/")[:i]) for i in range(1, len(name.split("/"))))
    return result


def _budget(roots, files, dirs):
    # This workstation has classic Windows path limits. Include the temporary
    # build root and every empty directory; no filename shortening is permitted.
    def units(path):
        return len(str(path).encode("utf-16-le")) // 2
    _require(all(units(root / name) <= 259 for root in roots for name in files)
             and all(units(root / name) <= 247 for root in roots for name in dirs),
             "PREPARE_WINDOWS_PATH_TOO_LONG")


def _exclusive(path, raw):
    G._plain(path.parent, directory=True)
    _require(not path.exists() and not path.is_symlink(), "PREPARE_WRITE_CONFLICT")
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def prepare_generation(*, workspace_root, original_root, source_root, target_root,
                       building_root, original_commit, original_manifest_sha256,
                       source_commit, source_manifest_sha256, target_commit,
                       target_manifest_raw, code_files, anchors, resolved_at_ms,
                       generation_id, cache_context=None):
    """Atomically publish a fully inventoried PREPARED/STOPPED generation.

Pins and closure anchors are explicit reviewed inputs. No credentials, account
request, shell command, source mutation or automatic activation occurs here.
Cache continuity is recomputed from original raw evidence by the projection.
    """
    try:
        workspace = G._plain(Path(workspace_root), directory=True)
        original, source = G._root(str(original_root)), G._root(str(source_root))
        target, building = (_new_path(path, workspace) for path in (target_root, building_root))
        roots = (original, source, target, building)
        _require(len(set(roots)) == 4 and all(not a.is_relative_to(b)
                 for a in roots for b in roots if a != b)
                 and all(path.is_relative_to(workspace) for path in roots), "PREPARE_ROOTS_OVERLAP")
        _require(all(G._hash(value, size) for value, size in (
            (original_commit, 40), (source_commit, 40), (target_commit, 40),
            (original_manifest_sha256, 64), (source_manifest_sha256, 64), (generation_id, 32)))
                 and source_commit != target_commit, "PREPARE_PINS_INVALID")
        exported = _code(code_files, target_manifest_raw, target_commit)
        _require(type(anchors) is R.ResolutionAnchors and anchors.observed_root == str(source),
                 "PREPARE_ANCHORS_INVALID")
        with G.owned_process_locks((original, source)):
            G._verify_code(original, original_commit, original_manifest_sha256)
            G._verify_code(source, source_commit, source_manifest_sha256)
            old_files, old_dirs = G._inventory(original, source_scope=True)
            src_files, src_dirs = G._inventory(source, source_scope=True)
            _require(all(name in {"DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json"}
                         or name.startswith((R.STATE, "logs/")) for name in set(old_files) | set(src_files)),
                     "PREPARE_STATE_SCOPE_UNSUPPORTED")
            _require(set(old_dirs) <= set(src_dirs), "PREPARE_SOURCE_HISTORY_CHANGED")
            a, b = _evidence(original, old_files), _evidence(source, src_files)
            cache_change = None
            cache_name = R.STATE + "verified-risk-v2/" + anchors.account_hash + "/verified.json"
            old_evidence, src_evidence = {f.name: f.raw for f in a}, {f.name: f.raw for f in b}
            if old_evidence.get(cache_name) != src_evidence.get(cache_name):
                from .demo_cache_continuity import validate_cache_continuity
                _require(type(cache_context) is tuple and len(cache_context) in (2, 3),
                         "PREPARE_CACHE_CONTEXT_REQUIRED")
                cache_change = validate_cache_continuity(old_evidence[cache_name], src_evidence[cache_name],
                    src_evidence[R.AUTHORIZATION], cache_context[0], cache_context[1],
                    new_account_views_raw=cache_context[2] if len(cache_context) == 3 else None)
            else:
                _require(cache_context is None, "PREPARE_CACHE_CONTEXT_UNEXPECTED")
            seed = P.project_validated_resolution(a, b, anchors, source_commit=source_commit,
                target_commit=target_commit, resolved_at_ms=resolved_at_ms,
                cache_change=cache_change, cache_context=cache_context)
            prepared = dict(schema=1, environment="DEMO", phase="PREPARED", generation_id=generation_id,
                target_root=str(target), target_commit=target_commit, target_manifest_sha256=_sha(target_manifest_raw),
                account_hash=anchors.account_hash, authorization_sha256=anchors.authorization_sha256,
                source_root=str(source), source_commit=source_commit, source_manifest_sha256=source_manifest_sha256,
                source_files=src_files, source_directories=src_dirs, signal_id=R.SIGNAL,
                proof_path=anchors.proof_path, proof_sha256=anchors.proof_sha256,
                original_fail_path=anchors.original_fail_path, original_fail_sha256=anchors.original_fail_sha256,
                seed_controller_sha256=seed.controller_sha256, seed_router_sha256=seed.router_sha256)
            files = dict(exported)
            flags = {R.STATE+"CONTROLLER_HALT", R.STATE+"HALT-"+_sha(R.ACCOUNT_KEY.encode())[:12],
                     R.STATE+"STOP_NEW_ENTRIES"}
            for name in src_files:
                raw = G._read(source / name)
                files[G.RECOVERY+"/source/"+name] = raw
                if name.startswith((R.STATE, "logs/")) and name not in flags:
                    # Empty old sidecars remain in the immutable archive only.
                    if name.casefold() in {R.CONTROLLER+s for s in ("-wal", "-shm", "-journal")} | {
                            R.ROUTER+s for s in ("-wal", "-shm", "-journal")}:
                        _require(not raw, "PREPARE_SQLITE_SIDECAR_NONEMPTY")
                        continue
                    files[name] = raw
            files[R.CONTROLLER], files[R.ROUTER] = seed.controller, seed.router
            files[R.STATE+"STOP_NEW_ENTRIES"] = G.PREPARATION_STOP
            files[G.RECOVERY+"/seed/controller.sqlite3"] = seed.controller
            files[G.RECOVERY+"/seed/router.sqlite3"] = seed.router
            receipt = G.RECOVERY+"/prepared.json"
            files[receipt] = R.canonical_json(prepared)
            dirs = _parents(files, set(src_dirs) | {G.RECOVERY+"/source/"+name for name in src_dirs})
            _require(sum(map(len, files.values())) <= G.MAX_TOTAL and len(files) <= 4096,
                     "PREPARE_INVENTORY_TOO_LARGE")
            _budget((building, target), files, dirs)
            # Refuse before any write unless no-overwrite directory rename is
            # available. On Windows rename fails even for an existing empty dir.
            _require(os.name == "nt" and building.parent.stat().st_dev == target.parent.stat().st_dev,
                     "PREPARE_EXCLUSIVE_PUBLISH_UNAVAILABLE")
            building.mkdir(exist_ok=False)
            for name in sorted(dirs, key=lambda value: (value.count("/"), value)):
                (building / name).mkdir(exist_ok=False)
            stop_name = R.STATE+"STOP_NEW_ENTRIES"
            order = [stop_name] + sorted(set(files)-{stop_name, receipt}) + [receipt]
            for name in order:
                _exclusive(building / name, files[name])
            expected = ({name: _sha(raw) for name, raw in files.items()}, sorted(dirs))
            _require(G._inventory(building) == expected, "PREPARE_BUILD_INVENTORY_CHANGED")
            for root, commit, digest in ((original, original_commit, original_manifest_sha256),
                                         (source, source_commit, source_manifest_sha256)):
                G._verify_code(root, commit, digest)
            _require(G._inventory(original, source_scope=True) == (old_files, old_dirs)
                     and G._inventory(source, source_scope=True) == (src_files, src_dirs),
                     "PREPARE_SOURCE_CHANGED")
            _new_path(target, workspace)
            building.rename(target)
            with G.owned_process_locks((target,)):
                result = G.assert_prepared_generation(target, target_commit)
                _require(G._read(target / stop_name) == G.PREPARATION_STOP
                         and not (target / G.RECOVERY / "activation.json").exists(), "PREPARE_STOP_INVALID")
            _require(G._inventory(original, source_scope=True) == (old_files, old_dirs)
                     and G._inventory(source, source_scope=True) == (src_files, src_dirs),
                     "PREPARE_SOURCE_CHANGED")
            return result
    except PreparationRefused:
        raise
    except (G.SuccessorGateRefused, P.SuccessorRefused, R.ResolutionRefused) as error:
        raise PreparationRefused(error.code) from None
    except Exception:
        raise PreparationRefused("PREPARE_UNVERIFIED") from None
