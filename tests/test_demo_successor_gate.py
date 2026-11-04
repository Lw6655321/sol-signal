"""Real local protocol + real SQLite projection; no account or credential IO.

Separate entry-order tests patch only unrelated construction, never the gate.
"""
from dataclasses import replace
from contextlib import nullcontext
from decimal import localcontext
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
from src import demo_halted_resolution as R
from src import demo_successor as P
from src import demo_successor_gate as G
import test_demo_halted_resolution as HISTORY
from scripts import demo_auto_run as SERVICE
from scripts import demo_auto_supervisor as SUPERVISOR
from scripts import demo_technical_test as TECH


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def edit_database(raw, callback):
    with sqlite3.connect(":memory:") as db:
        db.deserialize(raw)
        callback(db)
        db.commit()
        return db.serialize()


def archive_code(root, commit):
    root.mkdir(parents=True, exist_ok=True)
    files = {}
    for name in ("config.json", "scripts/demo_auto_run.py", "scripts/demo_auto_supervisor.py",
                 "scripts/demo_technical_test.py", "src/demo_successor_gate.py", "src/demo_successor.py"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"{}" if name.endswith(".json") else b"# never executed fixture\n")
        files[name] = digest(path.read_bytes())
    (root / "DEPLOYED_COMMIT").write_bytes(commit.encode()+b"\n")
    (root / "DEPLOYMENT_MANIFEST.json").write_bytes(encoded(dict(schema=1, environment="DEMO", commit=commit, files=files)))


class GateFixture:
    def __init__(self, directory):
        self.base = Path(directory)
        self.source, self.root = self.base / "source", self.base / "target"
        self.old_commit, self.new_commit = "a"*40, "b"*40
        archive_code(self.source, self.old_commit)
        archive_code(self.root, self.new_commit)
        example = HISTORY.HaltedResolutionTests(methodName="runTest")
        example.setUp()
        example.root = str(self.source)
        original, observed, anchors = example.bundle()

        def complete_other(db):
            names = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "routes" in names:
                row = dict(signal_id="prior-canceled", status="CANCELED", account_hash="a"*64,
                           inst_id="SOL-USDT-SWAP", client_order_id="priorcanceled", filled_qty="0")
                db.execute("UPDATE routes SET data=? WHERE signal='prior-canceled'", (json.dumps(row),))
            else:
                db.execute("UPDATE controller_signals SET intent=? WHERE signal='prior-canceled'",
                           (json.dumps(dict(signal_id="prior-canceled", inst_id="SOL-USDT-SWAP")),))

        def complete(files):
            return tuple(R.EvidenceFile(item.name, edit_database(item.raw, complete_other)
                         if item.name in (R.CONTROLLER, R.ROUTER) else item.raw) for item in files)
        original, observed = complete(original), complete(observed)
        anchors = replace(anchors, original_inventory_sha256=R.archive_digest(original),
                          observed_inventory_sha256=R.archive_digest(observed))
        projected = P.project_validated_resolution(original, observed, anchors,
                    source_commit=self.old_commit, target_commit=self.new_commit,
                    resolved_at_ms=example.end+100)
        for item in observed:
            path = self.source / item.name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(item.raw)
        # An empty directory is part of preservation, too.
        (self.source / "logs/empty-history").mkdir()
        self.recovery = self.root / G.RECOVERY
        copied = self.recovery / "source"
        copied.mkdir(parents=True)
        for name in ("state", "logs"):
            shutil.copytree(self.source / name, copied / name)
        for name in ("DEPLOYED_COMMIT", "DEPLOYMENT_MANIFEST.json"):
            shutil.copyfile(self.source / name, copied / name)
        seed = self.recovery / "seed"
        seed.mkdir()
        (seed / "controller.sqlite3").write_bytes(projected.controller)
        (seed / "router.sqlite3").write_bytes(projected.router)
        state = self.root / "state/demo-auto"
        for name in ("account-binding.json", "auto-authorization.json", "scan-seen.json", "process.lock"):
            shutil.copyfile(self.source / "state/demo-auto" / name, state / name)
        (state / "controller.sqlite3").write_bytes(projected.controller)
        (state / "router.sqlite3").write_bytes(projected.router)
        self.prepared = dict(schema=1, environment="DEMO", phase="PREPARED", generation_id="c"*32,
            target_root=str(self.root), target_commit=self.new_commit,
            target_manifest_sha256=digest((self.root / "DEPLOYMENT_MANIFEST.json").read_bytes()),
            account_hash="a"*64, authorization_sha256=anchors.authorization_sha256,
            source_root=str(self.source), source_commit=self.old_commit,
            source_manifest_sha256=digest((self.source / "DEPLOYMENT_MANIFEST.json").read_bytes()),
            source_files=G._inventory(self.source, source_scope=True)[0],
            source_directories=G._inventory(self.source, source_scope=True)[1], signal_id=R.SIGNAL,
            proof_path=anchors.proof_path, proof_sha256=anchors.proof_sha256,
            original_fail_path=anchors.original_fail_path, original_fail_sha256=anchors.original_fail_sha256,
            seed_controller_sha256=projected.controller_sha256, seed_router_sha256=projected.router_sha256)
        self.save_prepared()
        self.activation = dict(schema=1, environment="DEMO", phase="ACTIVATED", generation_id="c"*32,
            target_root=str(self.root), target_commit=self.new_commit,
            prepared_sha256=digest((self.recovery / "prepared.json").read_bytes()), activated_at_ms=1,
            ordinary_provider_verified=True, diagnostic_only=False)
        self.save_activation()

    def save_prepared(self):
        (self.recovery / "prepared.json").write_bytes(encoded(self.prepared))

    def save_activation(self):
        (self.recovery / "activation.json").write_bytes(encoded(self.activation))

    def db_change(self, name, callback, *, seed=False):
        path = self.recovery / "seed" / name if seed else self.root / "state/demo-auto" / name
        path.write_bytes(edit_database(path.read_bytes(), callback))


class SuccessorGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        self.case = GateFixture(self.temporary.name)

    def refused(self, code=None):
        with self.assertRaises(G.SuccessorGateRefused) as caught:
            G.assert_activated_generation(self.case.root, self.case.new_commit)
        if code:
            self.assertEqual(caught.exception.code, code)
        self.assertRegex(str(caught.exception), r"^SUCCESSOR_[A-Z0-9_]+$")

    def test_real_projection_and_complete_local_protocol_allow_activated_generation(self):
        before = G._inventory(self.case.source, source_scope=True)
        value = G.assert_activated_generation(self.case.root, self.case.new_commit)
        self.assertEqual(value.target_commit, self.case.new_commit)
        self.assertEqual(value.source_root, self.case.source)
        self.assertNotIn("a"*64, repr(value))
        self.assertEqual(before, G._inventory(self.case.source, source_scope=True))

    def test_prepared_only_is_not_execution_authority(self):
        (self.case.recovery / "activation.json").unlink()
        self.assertEqual(G.assert_prepared_generation(self.case.root).generation_id, "c"*32)
        self.refused("SUCCESSOR_ACTIVATION_UNREADABLE")

    def test_no_marker_or_empty_fresh_directory_never_falls_back(self):
        empty = Path(self.temporary.name) / "empty"
        empty.mkdir()
        with self.assertRaises(G.SuccessorGateRefused):
            G.assert_activated_generation(empty)
        self.assertEqual(list(empty.iterdir()), [])

    def test_preparation_stop_blocks_receipt_before_last_activation_step(self):
        stop = self.case.root / "state/demo-auto/STOP_NEW_ENTRIES"
        stop.write_bytes(G.PREPARATION_STOP)
        self.refused("SUCCESSOR_ACTIVATION_INCOMPLETE")
        stop.write_bytes(b"OPERATOR_STOP\n")
        G.assert_activated_generation(self.case.root)

    def test_activation_is_exactly_bound_and_never_accepts_diagnostic_snapshot(self):
        for key, value in (("diagnostic_only", True), ("ordinary_provider_verified", 1),
                           ("prepared_sha256", "f"*64), ("phase", "PREPARED"),
                           ("target_commit", "d"*40), ("activated_at_ms", True),
                           ("activated_at_ms", 10**30), ("risk_snapshot", {"demo_verified": True})):
            with self.subTest(key=key, value=value):
                saved = dict(self.case.activation)
                self.case.activation[key] = value
                self.case.save_activation()
                self.refused("SUCCESSOR_ACTIVATION_INVALID")
                self.case.activation = saved
        self.case.save_activation()

    def test_duplicate_json_key_is_refused(self):
        path = self.case.recovery / "activation.json"
        path.write_bytes(b'{"schema":1,'+path.read_bytes()[1:])
        self.refused("SUCCESSOR_JSON_DUPLICATE")

    def test_unknown_prepared_field_or_path_alias_is_refused(self):
        self.case.prepared["allow_fresh"] = True
        self.case.save_prepared()
        self.refused("SUCCESSOR_PREPARED_INVALID")
        del self.case.prepared["allow_fresh"]
        self.case.prepared["source_files"]["logs/../escape"] = "f"*64
        self.case.save_prepared()
        self.refused("SUCCESSOR_PATH_INVALID")

    def test_moved_generation_cannot_reuse_receipt(self):
        moved = self.case.root.with_name("moved")
        self.case.root.rename(moved)
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_TARGET_MISMATCH"):
            G.assert_activated_generation(moved)

    def test_code_and_external_source_mutations_are_refused(self):
        (self.case.source / "src/demo_successor.py").write_bytes(b"changed")
        self.refused("SUCCESSOR_DEPLOYMENT_INVALID")

    def test_external_source_halt_and_empty_directory_are_bound(self):
        (self.case.source / "logs/empty-history").rmdir()
        self.refused("SUCCESSOR_SOURCE_CHANGED")

    def test_archived_source_flags_cannot_be_replaced_or_omitted(self):
        (self.case.recovery / "source/state/demo-auto/CONTROLLER_HALT").write_bytes(b"changed")
        self.refused("SUCCESSOR_HISTORY_CHANGED")

    def test_authorization_and_seed_mutation_are_refused(self):
        path = self.case.root / "state/demo-auto/auto-authorization.json"
        path.write_bytes(path.read_bytes()+b" ")
        self.refused("SUCCESSOR_AUTHORIZATION_CHANGED")

    def test_seed_digest_and_current_history_are_independently_enforced(self):
        self.case.db_change("controller.sqlite3", lambda db: db.execute(
            "DELETE FROM controller_signals WHERE signal='prior-canceled'"))
        self.refused("SUCCESSOR_JOURNAL_INVALID")

    def test_seed_bytes_cannot_change_even_before_projection_semantics_are_checked(self):
        path = self.case.recovery / "seed/router.sqlite3"
        path.write_bytes(path.read_bytes()+b"changed")
        self.refused("SUCCESSOR_SEED_CHANGED")

    def test_current_halt_file_and_sqlite_sidecar_are_not_execution_authority(self):
        state = self.case.root / "state/demo-auto"
        halt = state / "CONTROLLER_HALT"
        halt.write_bytes(b"NEW_FAILURE\n")
        self.refused("SUCCESSOR_CURRENT_HALTED")
        halt.unlink()
        (state / "router.sqlite3-wal").write_bytes(b"unobserved-current-state")
        self.refused("SUCCESSOR_JOURNAL_SIDECAR")

    def test_seed_deletion_cannot_be_hidden_by_updating_prepared_hash_and_receipt(self):
        self.case.db_change("controller.sqlite3", lambda db: db.execute(
            "DELETE FROM controller_signals WHERE signal='prior-canceled'"), seed=True)
        self.case.prepared["seed_controller_sha256"] = digest((self.case.recovery / "seed/controller.sqlite3").read_bytes())
        self.case.save_prepared()
        self.case.activation["prepared_sha256"] = digest((self.case.recovery / "prepared.json").read_bytes())
        self.case.save_activation()
        self.refused("SUCCESSOR_JOURNAL_INVALID")

    def test_valid_later_completed_trade_survives_restart_without_rehashing_mutable_databases(self):
        intent = json.dumps(dict(signal_id="NEXT", inst_id="SOL-USDT-SWAP"))
        self.case.db_change("controller.sqlite3", lambda db: db.execute(
            "INSERT INTO controller_signals VALUES('NEXT','CANCELED','DONE',?,2)", (intent,)))
        row = dict(signal_id="NEXT", status="CANCELED", inst_id="SOL-USDT-SWAP", client_order_id="nextclient")
        self.case.db_change("router.sqlite3", lambda db: db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
            (R.ACCOUNT_KEY, "NEXT", "e"*64, "nextclient", "CANCELED", "", json.dumps(row))))
        G.assert_activated_generation(self.case.root)
        SERVICE.assert_service_recovery_ready(self.case.root / "state/demo-auto")

    def test_extra_recovery_file_or_unpublished_temporary_is_refused(self):
        (self.case.recovery / "activation.tmp").write_bytes(b"partial")
        self.refused("SUCCESSOR_RECOVERY_EXTRA_PATH")

    def test_rehashed_proof_cannot_round_a_changed_fill_price_back_to_its_receipt(self):
        # Construct internally pinned source/seed images without calling the
        # public projection validator: a rehashed archive is not proof validity.
        old_path = self.case.prepared["proof_path"]
        proof = R._json((self.case.source / old_path).read_bytes())
        proof["fills"][0]["fillPx"] = "100.0000000000000000000000000001"
        proof_raw = R.canonical_json(proof)
        proof_sha = digest(proof_raw)
        new_path = str(Path(old_path).with_name("halted-exit-"+proof_sha+".json")).replace("\\", "/")
        router_path = self.case.source / R.ROUTER
        source_router = R._database(router_path.read_bytes())
        row = [row for row in R._rows(source_router, "routes", ("signal", "data")) if row["signal"] == R.SIGNAL][0]
        route = R._json(row["data"].encode("utf-8"))
        route.update(exit_reconcile_sha256=proof_sha,
                     exit_reconcile_proof_path=str(self.case.source / new_path))
        changed_router = edit_database(router_path.read_bytes(), lambda db: db.execute(
            "UPDATE routes SET data=? WHERE signal=?", (json.dumps(route), R.SIGNAL)))
        for root in (self.case.source, self.case.recovery / "source"):
            (root / old_path).unlink()
            (root / new_path).write_bytes(proof_raw)
            (root / R.ROUTER).write_bytes(changed_router)
        record = R._json(R._rows(R._database((self.case.recovery / "seed/controller.sqlite3").read_bytes()),
            "halted_resolutions", ("record",))[0]["record"].encode("ascii"))
        changed_row = [row for row in R._rows(R._database(changed_router), "routes", ("signal",))
                       if row["signal"] == R.SIGNAL][0]
        inventory, directories = G._inventory(self.case.source, source_scope=True)
        evidence = tuple(R.EvidenceFile(name, (self.case.source / name).read_bytes())
                         for name in sorted(inventory) if name.startswith((R.STATE, "logs/")))
        record.update(source_router_sha256=digest(changed_router), original_router_row=changed_row,
                      proof_sha256=proof_sha, source_inventory_sha256=R.archive_digest(evidence))
        source_controller = (self.case.source / R.CONTROLLER).read_bytes()
        seeds = P._project_images(source_controller, changed_router, R.canonical_json(record),
                                 record["resolved_at_ms"], route)
        P.validate_seed_against_source(*seeds, source_controller, changed_router)
        for name, raw in zip(("controller.sqlite3", "router.sqlite3"), seeds):
            (self.case.recovery / "seed" / name).write_bytes(raw)
            (self.case.root / "state/demo-auto" / name).write_bytes(raw)
        self.case.prepared.update(proof_path=new_path, proof_sha256=proof_sha,
            source_files=inventory, source_directories=directories,
            seed_controller_sha256=digest(seeds[0]), seed_router_sha256=digest(seeds[1]))
        self.case.save_prepared()
        self.case.activation["prepared_sha256"] = digest((self.case.recovery / "prepared.json").read_bytes())
        self.case.save_activation()
        intent = R._intent(record["original_controller_signal"]["intent"].encode("utf-8"))
        with localcontext() as context:
            context.prec = 28
            R._raw_closure(proof, intent, route)  # Reproduces the rounding trap.
            self.refused("SUCCESSOR_JOURNAL_INVALID")


class SuccessorOwnedProcessLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        self.case = GateFixture(self.temporary.name)
        self.source = self.case.source
        self.target = self.case.root
        self.paths = [root / "state/demo-auto/process.lock" for root in (self.source, self.target)]
        self.original = {path: path.read_bytes() for path in self.paths}
        self.addCleanup(self.assert_no_registry)

    def assert_no_registry(self):
        self.assertFalse(G._OWNED_PROCESS_LOCKS.get())

    def assert_unchanged(self):
        self.assertEqual({path: path.read_bytes() for path in self.paths}, self.original)

    def test_full_gate_and_inventory_read_the_owned_source_lock_without_reopening(self):
        before = G._inventory(self.source, source_scope=True)
        real_open = Path.open
        def no_second_lock_open(path, *args, **kwargs):
            self.assertNotIn(path, self.paths, "LOCK_OWNER_MUST_NOT_REOPEN")
            return real_open(path, *args, **kwargs)
        with G.owned_process_locks((self.source, self.target)):
            with patch.object(Path, "open", no_second_lock_open):
                self.assertEqual(G._inventory(self.source, source_scope=True), before)
                value = G.assert_activated_generation(self.target, self.case.new_commit)
                self.assertEqual(value.source_root, self.source)
        self.assert_unchanged()

    def test_technical_double_lock_scope_uses_the_same_common_gate(self):
        deps = TECH.ActivatedProductionDeps()
        with patch.object(TECH, "runtime_demo_get", return_value=nullcontext(Mock()), create=True), deps.locks(self.source, self.target):
            self.assertEqual(deps._activated(self.target).source_root, self.source)
            self.assertEqual(set(G._OWNED_PROCESS_LOCKS.get()), set(self.paths))
        self.assert_unchanged()

    def test_owned_reader_restores_handle_position_and_other_files_use_plain_read(self):
        ordinary = self.source / "state/demo-auto/account-binding.json"
        original = ordinary.read_bytes()
        real_open, opened = Path.open, []
        def tracking(path, *args, **kwargs):
            opened.append(path)
            return real_open(path, *args, **kwargs)
        with G.owned_process_locks((self.source,)):
            held = G._OWNED_PROCESS_LOCKS.get()[self.paths[0]]
            held.handle.seek(1)
            with patch.object(Path, "open", tracking):
                self.assertEqual(G._read(self.paths[0]), self.original[self.paths[0]])
                self.assertEqual(G._read(ordinary), original)
            self.assertEqual(held.handle.tell(), 1)
            self.assertEqual(opened, [ordinary])
        self.assert_unchanged()

    def test_duplicate_and_nested_same_root_refuse_without_releasing_outer_lock(self):
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_DUPLICATE"):
            with G.owned_process_locks((self.source, self.source)):
                self.fail("DUPLICATE_LOCK_ENTERED")
        with G.owned_process_locks((self.source,)):
            held = G._OWNED_PROCESS_LOCKS.get()[self.paths[0]]
            with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_DUPLICATE"):
                with G.owned_process_locks((self.source,)):
                    self.fail("NESTED_DUPLICATE_ENTERED")
            self.assertIs(G._OWNED_PROCESS_LOCKS.get()[self.paths[0]], held)
            self.assertFalse(held.handle.closed)
        self.assert_unchanged()

    def test_disjoint_nested_scopes_preserve_outer_ownership(self):
        with G.owned_process_locks((self.source,)):
            first = G._OWNED_PROCESS_LOCKS.get()[self.paths[0]]
            with G.owned_process_locks((self.target,)):
                self.assertEqual(set(G._OWNED_PROCESS_LOCKS.get()), set(self.paths))
                G.assert_activated_generation(self.target)
            self.assertEqual(set(G._OWNED_PROCESS_LOCKS.get()), {self.paths[0]})
            self.assertIs(G._OWNED_PROCESS_LOCKS.get()[self.paths[0]], first)
            self.assertFalse(first.handle.closed)
        self.assert_unchanged()

    def test_exception_releases_all_handles_and_preserves_body_exception(self):
        handles = []
        with self.assertRaisesRegex(OSError, "SYNTHETIC_BODY_FAILURE"):
            with G.owned_process_locks((self.source, self.target)):
                handles = [held.handle for held in G._OWNED_PROCESS_LOCKS.get().values()]
                raise OSError("SYNTHETIC_BODY_FAILURE")
        self.assertTrue(all(handle.closed for handle in handles))
        self.assert_no_registry()
        with G.owned_process_locks((self.source, self.target)):
            G.assert_activated_generation(self.target)
        self.assert_unchanged()

    def test_missing_empty_or_aliased_lock_is_never_initialized(self):
        missing = self.target / "missing"
        missing.mkdir()
        with self.assertRaises(G.SuccessorGateRefused):
            with G.owned_process_locks((missing,)):
                self.fail("MISSING_LOCK_ENTERED")
        self.assertEqual(list(missing.iterdir()), [])
        self.paths[1].write_bytes(b"")
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_INVALID"):
            with G.owned_process_locks((self.source, self.target)):
                self.fail("EMPTY_LOCK_ENTERED")
        self.assertEqual(self.paths[1].read_bytes(), b"")
        with G.owned_process_locks((self.source,)):
            self.assertEqual(G._read(self.paths[0]), self.original[self.paths[0]])
        self.paths[1].write_bytes(self.original[self.paths[1]])
        alias = self.source / ".." / self.source.name
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PATH_INVALID"):
            with G.owned_process_locks((alias,)):
                self.fail("ALIASED_LOCK_ENTERED")
        self.assert_unchanged()

    def test_hardlinked_lock_is_refused_before_ownership(self):
        link = self.source / "second-lock-link"
        os.link(self.paths[0], link)
        try:
            with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PATH_INVALID"):
                with G.owned_process_locks((self.source,)):
                    self.fail("HARDLINK_LOCK_ENTERED")
        finally:
            link.unlink()
        self.assert_unchanged()

    def test_non_lock_source_mutation_still_refuses_full_inventory_under_locks(self):
        with G.owned_process_locks((self.source, self.target)):
            (self.source / "logs/foreign-record").write_bytes(b"NEW_HISTORY")
            with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_SOURCE_CHANGED"):
                G.assert_activated_generation(self.target)
        self.assert_unchanged()

    def test_locked_bytes_change_is_detected_and_handles_close_on_exit(self):
        holder = []
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_CHANGED"):
            with G.owned_process_locks((self.source,)):
                held = G._OWNED_PROCESS_LOCKS.get()[self.paths[0]]
                holder.append(held.handle)
                held.handle.seek(0)
                held.handle.write(b"X")  # Synthetic TEMP corruption, never a runtime.
        self.assertTrue(holder[0].closed)
        self.assert_no_registry()

    @unittest.skipUnless(os.name == "nt", "Windows byte-range lock regression")
    def test_windows_second_handle_cannot_read_or_acquire_owned_byte_zero(self):
        import msvcrt
        with G.owned_process_locks((self.source,)):
            with self.paths[0].open("r+b", buffering=0) as competitor:
                competitor.seek(0)
                with self.assertRaises(OSError):
                    msvcrt.locking(competitor.fileno(), msvcrt.LK_NBLCK, 1)
                with self.assertRaises(OSError):
                    competitor.read(1)
            self.assertEqual(G._read(self.paths[0]), self.original[self.paths[0]])
        self.assert_unchanged()


class SuccessorEntryOrderingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_service_missing_activation_precedes_credentials_and_constructor(self):
        with patch.object(SERVICE, "ROOT", self.root), patch("src.util.load_config", return_value={
                "account": {"signal_creates_position": False}}), \
             patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")) as credentials, \
             patch("src.order_router.OrderRouter", side_effect=AssertionError("NO_WRITABLE_ROUTER")) as router:
            with self.assertRaises(G.SuccessorGateRefused):
                SERVICE._run_locked_service(enabled=True, continuous=True)
        credentials.assert_not_called()
        router.assert_not_called()

    def test_diagnostic_service_does_not_require_activation(self):
        with patch.object(SERVICE, "ROOT", self.root), patch("src.util.load_config", return_value={
                "account": {"signal_creates_position": False}}), \
             patch("src.demo_transport.load_demo_credentials", return_value={}), \
             patch("src.demo_transport.DemoReadOnlyClient"), patch.object(SERVICE, "bind_demo_account"), \
             patch.object(SERVICE, "diagnostic_once", return_value=0) as diagnostic, \
             patch.object(G, "assert_activated_generation", side_effect=AssertionError("DIAGNOSTIC_IS_READ_ONLY")):
            self.assertEqual(SERVICE._run_locked_service(enabled=False), 0)
        diagnostic.assert_called_once()

    def test_supervisor_missing_activation_precedes_child_or_supervisor_state(self):
        archive_code(self.root, "a"*40)
        child = Mock(side_effect=AssertionError("NO_CHILD"))
        supervisor = SUPERVISOR.DemoAutoSupervisor(self.root, "a"*40, process_factory=child)
        with self.assertRaises(SUPERVISOR.SupervisorRefused):
            supervisor.run(enable_demo_auto=True)
        child.assert_not_called()
        self.assertFalse((self.root / "state").exists())

    def test_technical_missing_activation_precedes_credentials_and_owner_recovery(self):
        deps = TECH.ProductionDeps()
        with patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")) as credentials:
            with self.assertRaises(TECH.TestRefused):
                deps.build_runtime(self.root)
        credentials.assert_not_called()
        router = Mock()
        with self.assertRaises(TECH.TestRefused):
            deps.recover_owners(dict(root=str(self.root), router=router))
        router.recover_demo_owners.assert_not_called()

    def test_new_technical_production_does_not_call_legacy_migration(self):
        source = self.root / "source"
        source.mkdir()
        prepared = Mock(source_root=source, target_commit="a"*40)
        deps = TECH.ActivatedProductionDeps()
        with patch.object(TECH, "ROOT", self.root), patch.object(G, "assert_activated_generation", return_value=prepared), \
             patch.object(deps, "_verify_archive", return_value="a"*40), \
             patch.object(TECH.ProductionDeps, "preserve_state", side_effect=AssertionError("NO_MIGRATION")), \
             patch.object(TECH.ProductionDeps, "check_predecessor", side_effect=AssertionError("NO_LEGACY_PREDECESSOR")):
            deps.validate_roots(source, self.root)
            deps.check_predecessor(source)
            deps.preserve_state(source, self.root)


class BoundedReadTests(unittest.TestCase):
    """Real TEMP files, including changes between the size hint and open."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / "sample.bin"
        self.calls = []

    def read_changed(self, initial, after=None, *, limit=128, chunk=None):
        self.path.write_bytes(initial)
        self.calls = []
        actual_open, calls, target = Path.open, self.calls, self.path

        class Reader:
            def __init__(self, handle):
                self.handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.handle.close()

            def read(self, count):
                calls.append(count)
                return self.handle.read(count if chunk is None else min(count, chunk))

        def opened(path, mode="r", *args, **kwargs):
            if path == target and mode == "rb":
                if after is not None:
                    with actual_open(path, "wb") as handle:
                        handle.write(after)
                return Reader(actual_open(path, mode, *args, **kwargs))
            return actual_open(path, mode, *args, **kwargs)

        with patch.object(G, "MAX_FILE", limit), patch.object(Path, "open", opened):
            return G._read(self.path)

    def lock_path(self):
        path = self.root / "state/demo-auto/process.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"LOCKS")
        return path

    @staticmethod
    def spy_handle(handle, calls, error=None):
        class Handle:
            def __getattr__(self, name):
                return getattr(handle, name)

            def read(self, count):
                calls.append(count)
                if error is not None:
                    raise error
                return handle.read(count)
        return Handle()

    def test_stable_empty_small_and_exact_limit_use_size_hint(self):
        for size in (0, 1, 8, 127, 128):
            with self.subTest(size=size):
                data = b"S" * size
                self.assertEqual(self.read_changed(data), data)
                self.assertEqual(self.calls, [size + 1])

    def test_growth_keeps_original_bounded_prefix_and_oversize_refusal(self):
        for before, after in ((0, 40), (8, 9), (8, 120), (8, 128), (127, 128),
                              (8, 129), (8, 200), (128, 129)):
            with self.subTest(before=before, after=after):
                initial, changed = b"G" * before, b"G" * after
                if after > 128:
                    with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_FILE_TOO_LARGE"):
                        self.read_changed(initial, changed)
                else:
                    self.assertEqual(self.read_changed(initial, changed), changed)
                self.assertTrue(all(type(n) is int and 0 < n <= 129 for n in self.calls))
                self.assertLessEqual(sum(self.calls), 129)
                self.assertEqual(self.calls[0], before + 1)
                if before < 128:
                    self.assertEqual(sum(self.calls), 129)

    def test_truncation_after_hint_keeps_short_or_empty_bytes(self):
        for size in (0, 1, 8, 120):
            with self.subTest(size=size):
                self.assertEqual(self.read_changed(b"T" * 128, b"T" * size), b"T" * size)
                self.assertEqual(self.calls, [129])

    def test_oversize_and_invalid_negative_hint_refuse_before_open(self):
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_FILE_TOO_LARGE"):
            self.read_changed(b"X" * 129)
        self.assertEqual(self.calls, [])
        # Negative stat sizes are not valid regular-file observations. They
        # must never reach read(-1), whose meaning would be unbounded read-all.
        with patch.object(G, "_plain", return_value=self.path), \
             patch.object(Path, "stat", return_value=SimpleNamespace(st_size=-1)), \
             patch.object(Path, "open", side_effect=AssertionError("NO_UNBOUNDED_READ")) as opened:
            with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_FILE_TOO_LARGE"):
                G._read(self.path)
        opened.assert_not_called()

    def test_short_first_read_does_not_force_an_extra_read(self):
        self.assertEqual(self.read_changed(b"SHORT_DATA", chunk=3), b"SHO")
        self.assertEqual(self.calls, [11])

    def test_held_read_uses_identity_size_checks_both_identities_and_restores_offset(self):
        path = self.lock_path()
        with G.owned_process_locks((self.root,)):
            held = G._OWNED_PROCESS_LOCKS.get()[path]
            held.handle.seek(2)
            calls = []
            spied = replace(held, handle=self.spy_handle(held.handle, calls))
            with patch.object(G, "_lock_identity", wraps=G._lock_identity) as identity:
                self.assertEqual(G._held_lock_bytes(spied), b"LOCKS")
            self.assertEqual(identity.call_count, 4)
            self.assertEqual(calls, [6])
            self.assertEqual(held.handle.tell(), 2)
            # The inventory path still uses its owner's handle, including on
            # Windows where a second handle cannot read the locked byte.
            with patch.object(Path, "open", side_effect=AssertionError("NO_REOPEN")):
                self.assertEqual(G._read(path), b"LOCKS")
        self.assertTrue(held.handle.closed)
        self.assertEqual(path.read_bytes(), b"LOCKS")

    def test_held_growth_truncation_and_same_size_corruption_remain_rejected(self):
        path = self.lock_path()
        for changed in (b"WRONG", b"LOCKSX", b"LOCK"):
            with self.subTest(length=len(changed)), G.owned_process_locks((self.root,)):
                held = G._OWNED_PROCESS_LOCKS.get()[path]
                try:
                    held.handle.seek(0)
                    held.handle.write(changed)
                    held.handle.truncate(len(changed))
                    held.handle.seek(2)
                    with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_CHANGED"):
                        G._held_lock_bytes(held)
                    self.assertEqual(held.handle.tell(), 2)
                finally:
                    held.handle.seek(0)
                    held.handle.write(b"LOCKS")
                    held.handle.truncate(5)
            self.assertTrue(held.handle.closed)
        self.assertEqual(path.read_bytes(), b"LOCKS")

    def test_held_read_error_keeps_offset_and_fixed_refusal(self):
        path = self.lock_path()
        with G.owned_process_locks((self.root,)):
            held = G._OWNED_PROCESS_LOCKS.get()[path]
            held.handle.seek(2)
            calls = []
            spied = replace(held, handle=self.spy_handle(held.handle, calls, OSError("TEMP_READ_FAILURE")))
            with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_PROCESS_LOCK_UNREADABLE"):
                G._held_lock_bytes(spied)
            self.assertEqual(calls, [6])
            self.assertEqual(held.handle.tell(), 2)
        self.assertTrue(held.handle.closed)
        self.assertEqual(path.read_bytes(), b"LOCKS")


if __name__ == "__main__":
    unittest.main()
