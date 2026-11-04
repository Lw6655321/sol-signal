"""Synthetic activation protocol and real ordinary-provider/fake-GET coverage."""
from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from src import demo_successor_activation as A
from src import demo_successor_gate as G
from src import demo_halted_resolution as R
from src.demo_risk import _boundaries
from tests.test_demo_successor_gate import GateFixture, HISTORY


class Clock:
    def __init__(self, value):
        self.value, self.mono = value, value*1_000_000

    def now(self):
        return self.value

    def monotonic(self):
        return self.mono

    def advance(self, milliseconds):
        self.value += milliseconds
        self.mono += milliseconds*1_000_000


class ActivationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        bundle = HISTORY.HaltedResolutionTests.bundle
        def preserved_cache(example):
            original, observed, anchors = bundle(example)
            day, week = _boundaries(example.created)
            # Storage-only fixture. Protocol tests still inject a collector;
            # the separate real-provider test below builds its actual cache.
            baseline = dict(schema=2, environment="DEMO", account_hash=example.account,
                authorization_digest=G._sha(R.canonical_json(example.auth)), anchor={"uTime":"1"},
                day_start_ms=day, week_start_ms=week, day_start_equity="100", week_start_equity="100",
                baseline_source="complete-ledger-flat-boundary-reconstruction")
            prefix = R.STATE+"verified-risk-v2/"+example.account+"/"
            def updated(files):
                return tuple(item for item in files if "/verified-risk-v2/" not in item.name) + (
                    R.EvidenceFile(prefix+"verified.json", R.canonical_json(baseline)),
                    R.EvidenceFile(prefix+"authorization-binding.json", R.canonical_json(example.auth)))
            original, observed = updated(original), updated(observed)
            return original, observed, replace(anchors,
                original_inventory_sha256=R.archive_digest(original),
                observed_inventory_sha256=R.archive_digest(observed))
        with mock.patch.object(HISTORY.HaltedResolutionTests,"bundle",preserved_cache):
            self.case = GateFixture(self.temp.name)
        self.cache = self.case.root / "state/demo-auto/verified-risk-v2"
        shutil.copytree(self.case.source / "state/demo-auto/verified-risk-v2", self.cache)
        self.receipt = self.case.recovery / "activation.json"
        self.receipt.unlink()
        self.stop = self.case.root / "state/demo-auto/STOP_NEW_ENTRIES"
        self.stop.write_bytes(G.PREPARATION_STOP)
        self.authorization = json.loads((self.case.root / "state/demo-auto/auto-authorization.json").read_bytes())
        self.clock = Clock(self.authorization["authorization_recorded_at_ms"]+100_000)
        self.events, self.hook = [], lambda event: None
        self.changes = {}
        self.addCleanup(mock.patch.stopall)
        mock.patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")).start()
        mock.patch("src.demo_transport.load_demo_credentials", side_effect=AssertionError("NO_REAL_CREDENTIALS")).start()

    def factory(self, root, prepared, clock, mono):
        outer = self
        class Collector:
            def __enter__(self):
                outer.events.append("enter"); outer.hook("enter")
                self.refreshes = 0
                return self

            def __exit__(self, *_):
                outer.events.append("close"); outer.hook("close")

            def collect(self):
                outer.events.append("collect"); outer.hook("collect")
                current, current_mono = clock(), mono()
                day, week = _boundaries(current)
                risk = dict(environment="DEMO",account_hash=prepared.account_hash,
                    risk_adapter="verified-demo-ledger-v2",live_verified=False,halted=False,
                    demo_verified=True,ledger_verified=True,baseline_trusted=True,history_complete=True,
                    pause_evidence_verified=True,account_snapshot_consistent=True,data_fresh=True,
                    refusal_reasons=[],macro_blackout=False,daily_loss_hit=False,weekly_loss_hit=False,
                    paused=False,open_positions=[],pending_orders=[],protection_orders=[],
                    authorization_recorded_at_ms=outer.authorization["authorization_recorded_at_ms"],
                    observed_at_ms=current-500,collection_completed_ms=current,
                    snapshot_window_start_ms=current-1000,snapshot_window_end_ms=current,
                    day_start_ms=day,week_start_ms=week)
                risk.update(outer.changes)
                return A._Collection(risk,current-1000,current,current_mono-1_000_000_000,current_mono)

            def refresh_market(self):
                self.refreshes += 1
                event = "refresh"+str(self.refreshes)
                outer.events.append(event); outer.hook(event)

            def assert_local_current(self, now):
                outer.events.append("local"); outer.hook("local")
        return Collector()

    def activate(self):
        return A._activate(self.case.root, self.case.new_commit,
            collector_factory=self.factory,clock=self.clock.now,monotonic_ns=self.clock.monotonic)

    def refused(self):
        with self.assertRaises((A.ActivationRefused,G.SuccessorGateRefused)) as caught:
            self.activate()
        self.assertRegex(str(caught.exception), r"^(ACTIVATION|SUCCESSOR)_[A-Z0-9_]+$")
        self.assertEqual(self.stop.read_bytes(), G.PREPARATION_STOP)
        return caught.exception

    def test_success_preserves_source_and_closes_collector_before_removing_only_preparation_stop(self):
        before = G._inventory(self.case.source,source_scope=True)
        result = self.activate()
        self.assertFalse(self.stop.exists())
        self.assertEqual(before,G._inventory(self.case.source,source_scope=True))
        self.assertEqual(set(json.loads(self.receipt.read_bytes())),G.ACTIVATION_KEYS)
        self.assertEqual(self.events,["enter","collect","refresh1","close","local","local"])
        self.assertFalse(result.service_started)
        self.assertFalse(result.orders_sent)
        G.assert_activated_generation(self.case.root,self.case.new_commit)

    def test_missing_marker_refuses_before_collector_construction(self):
        (self.case.recovery / "prepared.json").unlink()
        self.refused()
        self.assertEqual(self.events,[])

    def cache_refused_before_factory(self):
        factory = mock.Mock(side_effect=AssertionError("COLLECTOR_CONSTRUCTED"))
        with self.assertRaisesRegex(A.ActivationRefused,"^ACTIVATION_CACHE_[A-Z_]+$"):
            A._activate(self.case.root,self.case.new_commit,collector_factory=factory,
                clock=self.clock.now,monotonic_ns=self.clock.monotonic)
        factory.assert_not_called()
        self.assertEqual(self.stop.read_bytes(),G.PREPARATION_STOP)
        self.assertFalse(self.receipt.exists())

    def test_missing_existing_cache_namespace_refuses_before_collector(self):
        self.cache.rename(self.cache.with_name("preserved-but-missing"))
        self.cache_refused_before_factory()

    def test_missing_baseline_or_authorization_binding_cannot_be_bootstrapped(self):
        directory=self.cache / self.authorization["account_hash"]
        for name in ("verified.json","authorization-binding.json"):
            with self.subTest(name=name):
                path=directory / name;original=path.read_bytes();path.unlink()
                try:self.cache_refused_before_factory()
                finally:path.write_bytes(original)

    def test_changed_cache_or_extra_empty_directory_refuses_before_collector(self):
        path=self.cache / self.authorization["account_hash"] / "verified.json"
        original=path.read_bytes();path.write_bytes(original+b"\n")
        self.cache_refused_before_factory();path.write_bytes(original)
        (self.cache / "unexpected-empty").mkdir()
        self.cache_refused_before_factory()

    def test_hardlinked_target_cache_file_refuses_before_collector(self):
        path=self.cache / self.authorization["account_hash"] / "verified.json"
        external=self.case.base / "external-checkpoint.json"
        external.write_bytes(path.read_bytes());path.unlink();os.link(external,path)
        self.cache_refused_before_factory()

    def test_reparse_target_cache_directory_refuses_before_collector(self):
        renamed=self.cache.with_name("preserved-cache")
        self.cache.rename(renamed)
        try:
            try:os.symlink(renamed,self.cache,target_is_directory=True)
            except OSError as error:self.skipTest("directory symlink unavailable: "+str(error.winerror))
            self.cache_refused_before_factory()
        finally:
            if self.cache.is_symlink():self.cache.unlink()
            if not self.cache.exists():renamed.rename(self.cache)

    def test_os_reported_reparse_attribute_refuses_before_collector(self):
        original=Path.lstat
        def flagged(path,*args,**kwargs):
            info=original(path,*args,**kwargs)
            if path==self.cache:
                return SimpleNamespace(st_mode=info.st_mode,
                    st_file_attributes=getattr(info,"st_file_attributes",0)|0x400)
            return info
        with mock.patch.object(Path,"lstat",flagged):self.cache_refused_before_factory()

    def test_preserved_tree_without_account_baseline_is_not_a_bootstrap_permission(self):
        account=self.authorization["account_hash"]
        archive=self.case.recovery / "source/state/demo-auto/verified-risk-v2"
        (self.cache / account / "verified.json").unlink()
        (archive / account / "verified.json").unlink()
        prepared=SimpleNamespace(account_hash=account)
        with self.assertRaisesRegex(A.ActivationRefused,"ACTIVATION_CACHE_INVALID"):
            A._cache_storage(self.case.root,prepared,self.authorization,initial=True)

    def test_production_collector_checks_storage_before_demo_credentials(self):
        (self.case.root / "config.json").write_text(json.dumps({"account":{"signal_creates_position":False}}))
        (self.cache / self.authorization["account_hash"] / "verified.json").unlink()
        with mock.patch("src.demo_transport.load_demo_credentials") as credentials, \
             mock.patch("src.demo_get_pool.DemoGetPool") as pool:
            with self.assertRaisesRegex(A.ActivationRefused,"^ACTIVATION_CACHE_"):
                with A._ProductionCollector(self.case.root,SimpleNamespace(account_hash=self.authorization["account_hash"]),
                                            None,self.clock.now,self.clock.monotonic):
                    self.fail("unsafe collector entered")
            credentials.assert_not_called();pool.assert_not_called()

    def test_changed_cache_path_after_collection_keeps_stop_without_receipt(self):
        def hook(event):
            if event=="close":
                path=self.cache / self.authorization["account_hash"] / "verified.json"
                external=self.case.base / "external-checkpoint.json"
                external.write_bytes(path.read_bytes());path.unlink();os.link(external,path)
        self.hook=hook
        self.assertEqual(self.refused().code,"ACTIVATION_CACHE_INVALID")
        self.assertFalse(self.receipt.exists())

    def test_preexisting_receipt_cannot_resume_or_delete_stop(self):
        self.case.save_activation()
        self.assertEqual(self.refused().code,"ACTIVATION_ALREADY_RECORDED")
        self.assertEqual(self.events,[])

    def test_operator_stop_is_never_deleted(self):
        self.stop.write_bytes(b"OPERATOR_STOP\n")
        with self.assertRaisesRegex(A.ActivationRefused,"ACTIVATION_PREPARATION_STOP_REQUIRED"):
            self.activate()
        self.assertEqual(self.stop.read_bytes(),b"OPERATOR_STOP\n")
        self.assertEqual(self.events,[])

    def test_both_original_byte_zero_locks_are_held_before_collection(self):
        def check(event):
            if event == "collect":
                with self.assertRaises(G.SuccessorGateRefused):
                    with G.owned_process_locks((self.case.source,self.case.root)):
                        self.fail("duplicate source/target lock acquired")
        self.hook = check
        self.activate()

    def test_diagnostic_or_generic_adapter_never_admits(self):
        for change in ({"diagnostic_only":True},{"risk_adapter":"diagnostic"},
                       {"demo_verified":1},{"live_verified":True},{"environment":"LIVE"}):
            with self.subTest(change=change):
                self.changes = change
                self.refused()
                self.assertFalse(self.receipt.exists())

    def test_ledger_account_waves_risk_and_flatness_require_exact_evidence_fields(self):
        for change in ({"ledger_verified":False},{"history_complete":False},
                       {"account_snapshot_consistent":False},{"baseline_trusted":False},
                       {"refusal_reasons":["ACCOUNT_STALE"]},{"paused":True},
                       {"daily_loss_hit":True},{"weekly_loss_hit":True},
                       {"open_positions":[{}]},{"pending_orders":[{}]},{"protection_orders":[{}]}):
            with self.subTest(change=change):
                self.changes = change
                self.refused()
                self.assertFalse(self.receipt.exists())

    def test_account_or_authorization_binding_cannot_be_replaced(self):
        for change in ({"account_hash":"f"*64},{"authorization_recorded_at_ms":1}):
            with self.subTest(change=change):
                self.changes=change;self.refused()

    def test_expired_account_before_publication_keeps_stop_and_no_receipt(self):
        self.hook=lambda event:self.clock.advance(5001) if event=="refresh1" else None
        self.refused()
        self.assertFalse(self.receipt.exists())

    def test_expired_account_after_publication_keeps_exact_stop_and_non_executable_receipt(self):
        publish=A._publish_receipt
        def delayed(*args):
            publish(*args);self.clock.advance(5001)
        with mock.patch.object(A,"_publish_receipt",delayed):self.refused()
        self.assertTrue(self.receipt.exists())
        with self.assertRaisesRegex(G.SuccessorGateRefused,"SUCCESSOR_ACTIVATION_INCOMPLETE"):
            G.assert_activated_generation(self.case.root)
        self.assertEqual(self.refused().code,"ACTIVATION_ALREADY_RECORDED")

    def test_cleanup_delay_is_rechecked_before_final_unlink(self):
        self.hook=lambda event:self.clock.advance(5001) if event=="close" else None
        self.refused()
        self.assertFalse(self.receipt.exists())

    def test_cleanup_failure_keeps_stop_without_receipt(self):
        def hook(event):
            if event=="close":raise RuntimeError("cleanup failed")
        self.hook=hook
        with self.assertRaises(RuntimeError):self.activate()
        self.assertEqual(self.stop.read_bytes(),G.PREPARATION_STOP)
        self.assertFalse(self.receipt.exists())

    def test_source_changed_during_last_quote_is_caught_after_collector_close(self):
        def hook(event):
            if event=="refresh1":
                (self.case.source / "logs/changed.log").write_bytes(b"new")
        self.hook=hook;self.refused()
        self.assertFalse(self.receipt.exists())

    def test_source_changed_during_close_is_caught_by_last_complete_gate(self):
        def hook(event):
            if event=="close":(self.case.source / "logs/changed-at-close.log").write_bytes(b"new")
        self.hook=hook;self.refused()
        self.assertFalse(self.receipt.exists())

    def test_last_full_gate_elapsed_time_still_consumes_original_five_second_age(self):
        verify=G.assert_prepared_generation
        completed=[]
        def slow(*args,**kwargs):
            result=verify(*args,**kwargs);completed.append(True)
            if len(completed)==3:self.clock.advance(5001)
            return result
        with mock.patch.object(G,"assert_prepared_generation",slow):self.refused()
        self.assertEqual(len(completed),3)
        self.assertFalse(self.receipt.exists())

    def test_snapshot_wall_window_and_observation_time_are_not_relaxed(self):
        current=self.clock.now()
        for change in ({"observed_at_ms":current-5001},{"observed_at_ms":current+1},
                       {"snapshot_window_end_ms":current+10001},
                       {"snapshot_window_start_ms":current+1},{"collection_completed_ms":True}):
            with self.subTest(change=change):
                self.changes=change;self.refused()

    def test_monotonic_deadline_blocks_wall_clock_rollback(self):
        def hook(event):
            if event=="refresh1":self.clock.mono+=5_000_000_001
        self.hook=hook
        self.assertEqual(self.refused().code,"ACTIVATION_MONOTONIC_STALE")

    def test_monotonic_deadline_includes_collection_before_wall_clock_rollback(self):
        def hook(event):
            if event=="refresh1":self.clock.mono+=4_200_000_000
        self.hook=hook
        self.assertEqual(self.refused().code,"ACTIVATION_MONOTONIC_STALE")

    def test_utc_boundary_cannot_change_after_ordinary_snapshot(self):
        self.changes={"day_start_ms":1}
        self.assertEqual(self.refused().code,"ACTIVATION_BOUNDARY_CHANGED")

    def test_receipt_publication_failure_never_removes_stop(self):
        with mock.patch.object(A.os,"link",side_effect=OSError("not exposed")):
            with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.stop.read_bytes(),G.PREPARATION_STOP)
        self.assertFalse(self.receipt.exists())
        self.assertFalse(list(self.case.recovery.glob(".activation-*")))

    def test_partial_or_changed_receipt_never_clears_stop(self):
        def partial(root,raw):self.receipt.write_bytes(raw[:20])
        with mock.patch.object(A,"_publish_receipt",partial):
            self.assertEqual(self.refused().code,"ACTIVATION_RECEIPT_CHANGED")
        self.assertTrue(self.receipt.exists())
        with self.assertRaises(ValueError):G.assert_activated_generation(self.case.root)

    def test_failure_after_receipt_cannot_overwrite_operator_stop(self):
        publish=A._publish_receipt
        def operator(*args):
            publish(*args);self.stop.write_bytes(b"OPERATOR_STOP\n")
        with mock.patch.object(A,"_publish_receipt",operator):
            with self.assertRaisesRegex(A.ActivationRefused,"ACTIVATION_PREPARATION_STOP_CHANGED"):
                self.activate()
        self.assertEqual(self.stop.read_bytes(),b"OPERATOR_STOP\n")

    def test_operator_stop_replacement_at_final_rename_is_restored_and_never_deleted(self):
        rename=Path.rename
        def replace(path,target):
            if path==self.stop:path.write_bytes(b"OPERATOR_STOP\n")
            return rename(path,target)
        with mock.patch.object(Path,"rename",replace):
            with self.assertRaisesRegex(A.ActivationRefused,"ACTIVATION_PREPARATION_STOP_CHANGED"):
                self.activate()
        self.assertEqual(self.stop.read_bytes(),b"OPERATOR_STOP\n")
        self.assertFalse((self.case.recovery / "activation-stop.pending").exists())

    def test_new_operator_stop_at_old_path_is_not_removed_by_successful_activation(self):
        rename=Path.rename
        def replace(path,target):
            value=rename(path,target)
            if path==self.stop:self.stop.write_bytes(b"OPERATOR_STOP\n")
            return value
        with mock.patch.object(Path,"rename",replace):self.activate()
        self.assertEqual(self.stop.read_bytes(),b"OPERATOR_STOP\n")
        G.assert_activated_generation(self.case.root)

    def test_expiry_during_final_rename_restores_stop(self):
        rename=Path.rename
        def delayed(path,target):
            result=rename(path,target)
            if path==self.stop:self.clock.advance(5001)
            return result
        with mock.patch.object(Path,"rename",delayed):self.refused()
        self.assertFalse((self.case.recovery / "activation-stop.pending").exists())

    def test_crash_during_finalization_is_never_executable(self):
        self.case.save_activation()
        self.stop.rename(self.case.recovery / "activation-stop.pending")
        with self.assertRaisesRegex(G.SuccessorGateRefused,"SUCCESSOR_RECOVERY_EXTRA_PATH"):
            G.assert_activated_generation(self.case.root)

    def test_failed_finalization_never_overwrites_concurrent_operator_stop_during_restore(self):
        rename=Path.rename
        def conflict(path,target):
            result=rename(path,target)
            if path==self.stop:
                self.stop.write_bytes(b"OPERATOR_STOP\n")
                self.clock.advance(5001)
            return result
        with mock.patch.object(Path,"rename",conflict):
            with self.assertRaises(A.ActivationRefused):self.activate()
        self.assertEqual(self.stop.read_bytes(),b"OPERATOR_STOP\n")
        self.assertEqual((self.case.recovery / "activation-stop.pending").read_bytes(),G.PREPARATION_STOP)
        with self.assertRaisesRegex(G.SuccessorGateRefused,"SUCCESSOR_RECOVERY_EXTRA_PATH"):
            G.assert_activated_generation(self.case.root)

    def test_public_entry_only_uses_own_fixed_deployment_and_sanitizes_errors(self):
        with self.assertRaisesRegex(A.ActivationRefused,"ACTIVATION_OWN_DEPLOYMENT_REQUIRED"):
            A.activate_generation(self.case.root,self.case.new_commit)
        with mock.patch.object(A,"ROOT",self.case.root), \
             mock.patch.object(A,"_ProductionCollector",side_effect=ValueError("SECRET_DO_NOT_FORWARD")):
            with self.assertRaisesRegex(A.ActivationRefused,"^ACTIVATION_UNVERIFIED$"):
                A.activate_generation(self.case.root,self.case.new_commit)
        self.assertEqual(self.stop.read_bytes(),G.PREPARATION_STOP)


class ProductionCollectionTests(unittest.TestCase):
    def test_fixed_readonly_client_real_ordinary_provider_with_fake_gets(self):
        from src import demo_transport as T
        from src import demo_verified_risk as V
        from tests.test_demo_verified_risk import VerifiedRiskTests,NonContendingFixtureQuotaClock
        fixture=VerifiedRiskTests(methodName="runTest");fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        root=fixture.root;state=root / "state/demo-auto";state.mkdir(parents=True)
        shutil.copyfile(fixture.journal,state / "router.sqlite3")
        config=deepcopy(fixture.config)
        config["account"]["signal_creates_position"]=False
        config.update(data=dict(critical_sources=["ticker"],stale_seconds={"ticker":60}),
            macro=dict(blackout_minutes_before=15,blackout_minutes_after=15,block_impact=["high"],block_currencies=["US"]))
        (root / "config.json").write_text(json.dumps(config),encoding="utf-8")
        (state / "auto-authorization.json").write_text(json.dumps(fixture.authorization),encoding="utf-8")
        # A distinct preparation epoch produces an existing real baseline with
        # fake API data. Activation itself still makes exactly one snapshot.
        self.assertTrue(fixture.snapshot()["demo_verified"])
        shutil.copytree(root / "verified-risk", state / "verified-risk-v2")
        archive=root / G.RECOVERY / "source/state/demo-auto/verified-risk-v2"
        shutil.copytree(root / "verified-risk", archive)
        clock=Clock(fixture.now)
        snapshot=dict(generated_at_ms=fixture.now,health={"usable":True},sources={
            "ticker":dict(ok=True,source_ts=fixture.now,fetched_at_ms=fixture.now),
            "economic_calendar":dict(ok=True,source_ts=fixture.now,fetched_at_ms=fixture.now,
                data=[dict(ts=fixture.now+15*60_000+1000,impact="high",country="US")])})
        calls=[]
        class Pool:
            def __init__(self,**kwargs):self.closed=False;self.proxy=kwargs
            def get(self,url,headers,timeout):
                self.assert_open()
                parts=urlsplit(url);params={k:v[0] for k,v in parse_qs(parts.query).items()}
                calls.append(parts.path)
                if parts.path=="/api/v5/market/ticker":
                    data=[dict(instId="SOL-USDT-SWAP",last="100",bidPx="99",askPx="101",ts=str(fixture.now))]
                else:data=fixture.client.get(parts.path,params)
                return 200,dict(code="0",data=data)
            def assert_open(self):
                if self.closed:raise AssertionError("POOL_CLOSED")
            def close(self):self.closed=True
        cls=V.VerifiedDemoRiskProvider
        def ordinary(*args,**kwargs):
            self.assertIs(kwargs["diagnostic_halted_ledger"],False)
            return cls(*args,**kwargs,rate_clock=NonContendingFixtureQuotaClock())
        credentials={name:"fake-offline-only" for name in T.CREDENTIAL_NAMES}
        with mock.patch("src.demo_get_pool.DemoGetPool",Pool), \
             mock.patch.object(T,"load_demo_credentials",return_value=credentials), \
             mock.patch.object(V,"VerifiedDemoRiskProvider",side_effect=ordinary), \
             mock.patch("src.collect.collect",return_value=snapshot), \
             mock.patch("socket.socket.connect",side_effect=AssertionError("NO_NETWORK")):
            prepared=SimpleNamespace(account_hash=fixture.authorization["account_hash"])
            with A._ProductionCollector(root,prepared,None,clock.now,clock.monotonic) as collector:
                result=collector.collect()
                A._check_collection(result,prepared,fixture.authorization["authorization_recorded_at_ms"],clock.now(),clock.monotonic())
                self.assertEqual(collector.client._network_retries,0)
                self.assertEqual(len(collector.provider._views),2)
                self.assertTrue(result.risk["ledger_verified"])
                collector.refresh_market()
            self.assertTrue(collector.pool.closed)
            self.assertGreater(len(calls),25)
            self.assertEqual(calls.count("/api/v5/market/ticker"),2)
            count=len(calls)
            collector.assert_local_current(clock.now())
            clock.advance(1000)
            with self.assertRaisesRegex(ValueError,"MACRO_BLACKOUT"):
                collector.assert_local_current(clock.now())
            self.assertEqual(len(calls),count)  # Final checks never reopen a pool or call GET.


if __name__ == "__main__":
    unittest.main()
