"""Real TEMP filesystem publication; no credentials, account or execution IO."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src import demo_halted_resolution as R
from src import demo_successor_gate as G
from src import demo_successor_prepare as S
from tests import test_demo_halted_resolution as H


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class PreparationFixture:
    def __init__(self, path):
        self.base = Path(path)
        self.original, self.source = self.base / "o", self.base / "s"
        self.target, self.building = self.base / "t", self.base / "b"
        self.code = {name: b"{}" if name.endswith(".json") else b"# fixture only\n" for name in (
            "config.json", "scripts/demo_auto_run.py", "scripts/demo_auto_supervisor.py",
            "src/demo_successor.py", "src/demo_successor_gate.py")}
        self.commits = "1"*40, "2"*40, "3"*40
        self.manifests = [encoded(dict(schema=1, environment="DEMO", commit=commit,
            files={name: sha(raw) for name, raw in self.code.items()})) for commit in self.commits]
        for root, commit, manifest in zip((self.original, self.source), self.commits, self.manifests):
            root.mkdir()
            for name, raw in dict(self.code, DEPLOYED_COMMIT=(commit+"\n").encode(),
                                 DEPLOYMENT_MANIFEST_JSON=manifest).items():
                if name == "DEPLOYMENT_MANIFEST_JSON":
                    name = "DEPLOYMENT_MANIFEST.json"
                dest = root / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(raw)
        example = H.HaltedResolutionTests(methodName="runTest")
        example.setUp()
        example.root = str(self.source)
        a, b, self.anchors = example.bundle()
        self.resolved = example.end+100
        for root, files in ((self.original, a), (self.source, b)):
            for item in files:
                path = root / item.name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(item.raw)
            (root / "logs/old-empty").mkdir()
            (root / "state/demo-auto/old-empty").mkdir()
        self.options = dict(workspace_root=self.base, original_root=self.original,
            source_root=self.source, target_root=self.target, building_root=self.building,
            original_commit=self.commits[0], original_manifest_sha256=sha(self.manifests[0]),
            source_commit=self.commits[1], source_manifest_sha256=sha(self.manifests[1]),
            target_commit=self.commits[2], target_manifest_raw=self.manifests[2],
            code_files=self.code, anchors=self.anchors, resolved_at_ms=self.resolved,
            generation_id="4"*32)

    def run(self, **options):
        return S.prepare_generation(**dict(self.options, **options))


@unittest.skipUnless(os.name == "nt", "No-overwrite Windows directory publication")
class PreparationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="sp-", ignore_cleanup_errors=True)
        self.addCleanup(temp.cleanup)
        self.case = PreparationFixture(temp.name)
        self.addCleanup(patch.stopall)
        patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        patch("subprocess.Popen", side_effect=AssertionError("NO_SUBPROCESS")).start()
        patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")).start()
        self.before = {root: G._inventory(root, source_scope=True) for root in (self.case.original, self.case.source)}

    def unchanged(self):
        for root, inventory in self.before.items():
            self.assertEqual(inventory, G._inventory(root, source_scope=True))

    def test_real_atomic_preparation_retains_every_old_file_directory_and_stop(self):
        value = self.case.run()
        self.unchanged()
        self.assertEqual(value.target_root, self.case.target)
        self.assertFalse(self.case.building.exists())
        copied = self.case.target / G.RECOVERY / "source"
        self.assertEqual(G._inventory(copied), self.before[self.case.source])
        self.assertEqual((self.case.target / (R.STATE+"STOP_NEW_ENTRIES")).read_bytes(), G.PREPARATION_STOP)
        self.assertFalse((self.case.target / (R.STATE+"CONTROLLER_HALT")).exists())
        self.assertFalse((self.case.target / G.RECOVERY / "activation.json").exists())
        self.assertTrue((self.case.target / "logs/old-empty").is_dir())
        self.assertTrue((self.case.target / "state/demo-auto/old-empty").is_dir())
        with self.assertRaisesRegex(G.SuccessorGateRefused, "SUCCESSOR_ACTIVATION_UNREADABLE"):
            G.assert_activated_generation(self.case.target)

    def test_already_existing_target_build_or_source_destination_never_overwrites(self):
        for name in ("target_root", "building_root"):
            dest = self.case.options[name]
            dest.mkdir()
            with self.subTest(name=name), self.assertRaisesRegex(S.PreparationRefused, "PREPARE_TARGET_EXISTS"):
                self.case.run()
            self.assertEqual(list(dest.iterdir()), [])
            dest.rmdir()
        with self.assertRaises(S.PreparationRefused):
            self.case.run(target_root=self.case.source)
        self.unchanged()

    def test_bad_code_hash_commit_or_missing_entry_refuses_before_build(self):
        variants = ({"target_commit": "not-a-commit"},
                    {"target_manifest_raw": self.case.manifests[0]},
                    {"source_manifest_sha256": "0"*64},
                    {"code_files": {**self.case.code, "config.json": b'{"changed":true}'}})
        for options in variants:
            with self.subTest(options=list(options)), self.assertRaises(S.PreparationRefused):
                self.case.run(**options)
            self.assertFalse(self.case.target.exists())
            self.assertFalse(self.case.building.exists())
        self.unchanged()

    def test_stale_inventory_anchor_cannot_prepare(self):
        for field in ("original_inventory_sha256", "observed_inventory_sha256", "proof_sha256"):
            with self.subTest(field=field), self.assertRaises(S.PreparationRefused):
                self.case.run(anchors=replace(self.case.anchors, **{field: "0"*64}))
        self.assertFalse(self.case.building.exists())
        self.unchanged()

    def test_empty_source_lock_refuses_without_initializing_it(self):
        path = self.case.source / (R.STATE+"process.lock")
        path.write_bytes(b"")
        with self.assertRaises(S.PreparationRefused):
            self.case.run()
        self.assertEqual(path.read_bytes(), b"")
        self.assertFalse(self.case.target.exists())

    def test_unknown_original_or_source_state_is_never_silently_filtered(self):
        for root in (self.case.original, self.case.source):
            path = root / "state/foreign.txt"
            path.write_bytes(b"unknown state must not be dropped")
            with self.subTest(root=root.name), self.assertRaisesRegex(
                    S.PreparationRefused, "PREPARE_STATE_SCOPE_UNSUPPORTED"):
                self.case.run()
            self.assertFalse(self.case.target.exists())
            self.assertFalse(self.case.building.exists())
            self.assertEqual(path.read_bytes(), b"unknown state must not be dropped")
            path.unlink()
        self.unchanged()

    def test_write_failure_retains_stopped_partial_build_and_releases_source_locks(self):
        original = S._exclusive
        calls = []
        def fail(path, raw):
            calls.append(path)
            if len(calls) == 3:
                raise OSError("simulated disk failure")
            return original(path, raw)
        with patch.object(S, "_exclusive", side_effect=fail):
            with self.assertRaisesRegex(S.PreparationRefused, "PREPARE_UNVERIFIED"):
                self.case.run()
        self.assertFalse(self.case.target.exists())
        self.assertEqual((self.case.building / (R.STATE+"STOP_NEW_ENTRIES")).read_bytes(), G.PREPARATION_STOP)
        with G.owned_process_locks((self.case.original, self.case.source)):
            pass
        self.unchanged()

    def test_source_race_prevents_publish_without_resetting_the_source(self):
        original = S._exclusive
        changed = False
        path = self.case.source / "logs/foreign-append.txt"
        def append(path_arg, raw):
            nonlocal changed
            original(path_arg, raw)
            if not changed:
                path.write_bytes(b"external concurrent change")
                changed = True
        with patch.object(S, "_exclusive", side_effect=append):
            with self.assertRaisesRegex(S.PreparationRefused, "PREPARE_SOURCE_CHANGED"):
                self.case.run()
        self.assertFalse(self.case.target.exists())
        self.assertEqual(path.read_bytes(), b"external concurrent change")
        self.assertEqual(G._inventory(self.case.original, source_scope=True), self.before[self.case.original])

    def test_failure_after_publish_still_has_exact_preparation_stop(self):
        with patch.object(G, "assert_prepared_generation", side_effect=G.SuccessorGateRefused("SUCCESSOR_TEST_FAILURE")):
            with self.assertRaisesRegex(S.PreparationRefused, "SUCCESSOR_TEST_FAILURE"):
                self.case.run()
        self.assertTrue(self.case.target.is_dir())
        self.assertEqual((self.case.target / (R.STATE+"STOP_NEW_ENTRIES")).read_bytes(), G.PREPARATION_STOP)
        self.assertFalse((self.case.target / G.RECOVERY / "activation.json").exists())
        self.unchanged()

    def test_publish_conflict_preserves_external_target_and_partial_tree(self):
        rename = Path.rename
        def collide(path, target):
            if path == self.case.building:
                self.case.target.mkdir()
                (self.case.target / "external.txt").write_bytes(b"retained")
            return rename(path, target)
        with patch.object(Path, "rename", collide):
            with self.assertRaises(S.PreparationRefused):
                self.case.run()
        self.assertEqual((self.case.target / "external.txt").read_bytes(), b"retained")
        self.assertTrue(self.case.building.is_dir())
        self.unchanged()

    def test_path_budget_includes_temporary_root_before_any_write(self):
        long = self.case.base / ("b"*180)
        with self.assertRaisesRegex(S.PreparationRefused, "PREPARE_WINDOWS_PATH_TOO_LONG"):
            self.case.run(building_root=long)
        self.assertFalse(long.exists())
        self.assertFalse(self.case.target.exists())
        self.unchanged()

    def test_no_baseline_reset_or_source_authorization_replacement(self):
        self.case.run()
        for name in ("scan-seen.json", "auto-authorization.json", "account-binding.json", "verified-risk-v2/FAKE/verified.json"):
            self.assertEqual((self.case.source / R.STATE / name).read_bytes(),
                             (self.case.target / R.STATE / name).read_bytes())
        self.unchanged()


if __name__ == "__main__":
    unittest.main()
