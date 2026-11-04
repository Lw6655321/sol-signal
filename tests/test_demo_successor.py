"""Synthetic exact-projection and later-journal preservation boundaries."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
from unittest import mock

from src import demo_halted_resolution as R
from src import demo_successor as S
from tests import test_demo_halted_resolution as HISTORY


def mutate(raw, statement, args=()):
    with sqlite3.connect(":memory:") as db:
        db.deserialize(raw)
        db.execute(statement, args)
        db.commit()
        result = db.serialize()
    db.close()
    return result


class SuccessorTests(unittest.TestCase):
    def setUp(self):
        self.fixture = HISTORY.HaltedResolutionTests()
        self.fixture.setUp()
        self.bundle = self.fixture.bundle()
        self.original, self.source, self.anchors = self.bundle
        self.source_files = {file.name: file.raw for file in self.source}
        self.options = dict(source_commit="1"*40, target_commit="2"*40,
                            resolved_at_ms=self.fixture.end+100)

    def project(self, **changes):
        return S.project_validated_resolution(*self.bundle, **dict(self.options, **changes))

    def check_source(self, seed, c=None, r=None):
        return S.validate_seed_against_source(
            seed.controller if c is None else c, seed.router if r is None else r,
            self.source_files[R.CONTROLLER], self.source_files[R.ROUTER])

    def check_extension(self, seed, c=None, r=None):
        return S.validate_journal_extension(seed.controller, seed.router,
            seed.controller if c is None else c, seed.router if r is None else r)

    def test_projection_closes_only_proven_incident_and_keeps_old_failure_bytes(self):
        before = deepcopy(self.bundle)
        seed = self.project()
        self.check_source(seed)
        self.check_extension(seed)
        self.assertEqual(before, self.bundle)
        self.assertEqual(self.source_files[self.fixture.fail_path],
                         {file.name: file.raw for file in self.original}[self.fixture.fail_path])
        record = R._json(seed.resolution)
        self.assertEqual(record["source_inventory_sha256"], R.archive_digest(self.source))
        self.assertEqual(record["original_controller_signal"]["status"], "HALTED")
        self.assertEqual(record["original_router_row"]["status"], "HALTED")
        self.assertEqual(record["original_controller_halt"], [{"key": "halt", "value": "ORIGINAL_HALT"}])
        self.assertEqual(record["original_router_controls"], [{"account": R.ACCOUNT_KEY, "code": "ORIGINAL_HALT"}])
        self.assertEqual(hashlib.sha256(seed.controller).hexdigest(), seed.controller_sha256)
        self.assertEqual(hashlib.sha256(seed.router).hexdigest(), seed.router_sha256)

    def test_projection_is_immutable_and_grants_no_activation_or_account_admission(self):
        seed = self.project()
        self.assertFalse(seed.activation_allowed)
        self.assertFalse(seed.fresh_account_verified)
        self.assertTrue(seed.preparation_only)
        with self.assertRaises(FrozenInstanceError):
            seed.activation_allowed = True
        with self.assertRaises(ValueError):
            replace(seed, activation_allowed=True)

    def test_all_sqlite_connections_are_memory_and_no_file_reads_occur(self):
        connect = sqlite3.connect
        calls = []
        def memory(path, *args, **kwargs):
            calls.append(path)
            self.assertEqual(path, ":memory:")
            return connect(path, *args, **kwargs)
        with mock.patch.object(sqlite3, "connect", side_effect=memory), \
             mock.patch("builtins.open", side_effect=AssertionError("FILE_READ")), \
             mock.patch.object(Path, "read_bytes", side_effect=AssertionError("FILE_READ")):
            seed = self.project()
            self.check_source(seed)
            self.check_extension(seed)
        self.assertGreater(len(calls), 10)

    def test_bad_commit_and_time_boundaries_refuse(self):
        for key, value in (("source_commit", "bad"), ("target_commit", "1"*40),
                           ("target_commit", "F"*40), ("resolved_at_ms", True),
                           ("resolved_at_ms", self.fixture.end)):
            with self.subTest(key=key, value=value), self.assertRaises(S.SuccessorRefused):
                self.project(**{key: value})

    def test_bare_cache_hash_tuple_does_not_grant_continuity(self):
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_CACHE_CONTINUITY_REQUIRED"):
            self.project(cache_change=("fake", "1"*64, "2"*64))

    def test_hand_constructed_cache_certificate_requires_raw_context(self):
        from src.demo_cache_continuity import CacheContinuity
        forged = CacheContinuity("1"*64, "2"*64, "3"*64, "4"*64, "5"*64,
            self.fixture.account, 1, 2, 1, 1, 1, 1, "100", True, False)
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_CACHE_CONTEXT_REQUIRED"):
            self.project(cache_change=forged)
        with self.assertRaises(S.SuccessorRefused):
            self.project(cache_change=forged, cache_context=(b"{}", b"{}"))

    def test_context_cannot_be_silently_ignored_without_cache_change(self):
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_CACHE_CONTEXT_UNEXPECTED"):
            self.project(cache_context=(b"{}", b"{}"))

    def test_projection_keeps_original_high_precision_price_rejection(self):
        from decimal import localcontext
        self.fixture.proof["fills"][0]["fillPx"] = "100.0000000000000000000000000001"
        self.bundle = self.fixture.bundle()
        with localcontext() as context:
            context.prec = 9
            with self.assertRaisesRegex(R.ResolutionRefused, "RESOLUTION_FILL_INVALID"):
                R.validate_archived_halted_resolution(*self.bundle)
            with self.assertRaisesRegex(S.SuccessorRefused, "RESOLUTION_FILL_INVALID"):
                self.project()
            self.assertEqual(context.prec, 9)

    def test_unproven_source_and_original_history_drift_refuse(self):
        for changes in ({"exit_reconciled": False}, {"filled_qty": "0"}):
            with self.subTest(changes=changes):
                self.fixture.current_changes = changes
                self.bundle = self.fixture.bundle()
                with self.assertRaises(S.SuccessorRefused):
                    self.project()

    def test_seed_cannot_delete_any_old_journal_or_technical_test_row(self):
        seed = self.project()
        for kind, table, where in (("controller", "controller_signals", "signal='prior-canceled'"),
                                   ("controller", "technical_tests", "1=1"),
                                   ("router", "routes", "signal='prior-canceled'")):
            with self.subTest(kind=kind, table=table):
                changed = mutate(getattr(seed, kind), "DELETE FROM " + table + " WHERE " + where)
                with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_SOURCE_HISTORY_CHANGED"):
                    self.check_source(seed, **{"c" if kind == "controller" else "r": changed})

    def test_source_bytes_must_be_exactly_the_pinned_database(self):
        seed = self.project()
        changed = mutate(self.source_files[R.CONTROLLER], "UPDATE controller_signals SET code='different' WHERE signal='prior-canceled'")
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_SOURCE_JOURNAL_MISMATCH"):
            S.validate_seed_against_source(seed.controller, seed.router, changed, self.source_files[R.ROUTER])

    def test_original_rows_stored_in_resolution_cannot_be_self_attested(self):
        seed = self.project()
        record = R._json(seed.resolution)
        record["original_controller_halt"][0]["value"] = "FORGED"
        value = R.canonical_json(record).decode("ascii")
        c = mutate(seed.controller, "UPDATE halted_resolutions SET record=?", (value,))
        r = mutate(seed.router, "UPDATE halted_resolutions SET record=?", (value,))
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_SOURCE_INCIDENT_MISMATCH"):
            self.check_source(seed, c, r)

    def test_schema_extra_columns_tables_and_rewritten_old_types_refuse(self):
        seed = self.project()
        for sql in ("CREATE TABLE extra(value TEXT)",
                    "ALTER TABLE technical_tests ADD COLUMN forged TEXT",
                    "UPDATE technical_tests SET registered_at_ms='forged'"):
            with self.subTest(sql=sql), self.assertRaises(S.SuccessorRefused):
                self.check_source(seed, c=mutate(seed.controller, sql))

    def test_resolved_incident_quantity_and_immutable_metadata_cannot_change(self):
        seed = self.project()
        db = R._database(seed.router)
        route = [x for x in R._rows(db, "routes", ("signal", "data")) if x["signal"] == R.SIGNAL][0]
        for change in ({"filled_qty": "0"}, {"confirmed_closed_qty": "0"},
                       {"protected_qty": "0.01"}, {"exit_reconciled": 1},
                       {"created_at_ms": float(self.fixture.created)}):
            with self.subTest(change=change):
                data = dict(R._json(route["data"].encode()), **change)
                r = mutate(seed.router, "UPDATE routes SET data=? WHERE signal=?", (json.dumps(data), R.SIGNAL))
                with self.assertRaises(S.SuccessorRefused):
                    self.check_source(seed, r=r)

    def test_resolution_copies_must_match_and_cannot_disappear(self):
        seed = self.project()
        for sql in ("DELETE FROM halted_resolutions", "UPDATE halted_resolutions SET record='{}'"):
            with self.subTest(sql=sql), self.assertRaises(S.SuccessorRefused):
                self.check_extension(seed, c=mutate(seed.controller, sql))

    def test_corrupt_sqlite_and_views_are_rejected(self):
        seed = self.project()
        for raw in (b"NOT_SQLITE", mutate(seed.controller, "CREATE VIEW bad AS SELECT 1")):
            with self.subTest(length=len(raw)), self.assertRaises(S.SuccessorRefused):
                self.check_extension(seed, c=raw)

    def test_restoring_halt_or_controls_rejects_restart(self):
        seed = self.project()
        c = mutate(seed.controller, "INSERT INTO controller_meta VALUES('halt','NEW_HALT')")
        r = mutate(seed.router, "INSERT INTO controls VALUES(?,?)", (R.ACCOUNT_KEY, "NEW_HALT"))
        for ca, ra in ((c, None), (None, r)):
            with self.subTest(controller=ca is not None), self.assertRaises(S.SuccessorRefused):
                self.check_extension(seed, ca, ra)

    def test_new_valid_controller_and_route_survive_restart_while_old_dedupe_remains(self):
        seed = self.project()
        for status in ("PENDING", "PROTECTED", "CLOSING", "CLOSED", "CANCELED"):
            with self.subTest(status=status):
                intent = json.dumps({"signal_id": "new-signal"})
                c = mutate(seed.controller, "INSERT INTO controller_signals VALUES(?,?,?,?,?)",
                           ("new-signal", status, "NEW", intent, self.fixture.end+1000))
                data = json.dumps(dict(signal_id="new-signal", status=status,
                    client_order_id="newclient", inst_id="SOL-USDT-SWAP"))
                r = mutate(seed.router, "INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
                    (R.ACCOUNT_KEY, "new-signal", "f"*64, "newclient", status, "", data))
                self.check_extension(seed, c, r)
                self.assertEqual(len(R._rows(R._database(c), "controller_signals", ("signal",))), 3)

    def test_new_rejected_signal_without_route_is_retained_as_dedupe(self):
        seed = self.project()
        c = mutate(seed.controller, "INSERT INTO controller_signals VALUES('new-rejected','REJECTED','RISK','{}',1)")
        self.check_extension(seed, c)

    def test_future_halted_unknown_or_unmatched_active_rows_refuse(self):
        seed = self.project()
        for status in ("HALTED", "UNKNOWN", "PENDING"):
            with self.subTest(status=status):
                c = mutate(seed.controller, "INSERT INTO controller_signals VALUES(?,?,?,?,1)",
                    ("new-signal", status, "NEW", '{"signal_id":"new-signal"}'))
                with self.assertRaises(S.SuccessorRefused):
                    self.check_extension(seed, c)

    def test_new_foreign_or_unmatched_route_refuses(self):
        seed = self.project()
        for account, instrument in (("LIVE", "SOL-USDT-SWAP"), (R.ACCOUNT_KEY, "BTC-USDT-SWAP")):
            with self.subTest(account=account, instrument=instrument):
                c = mutate(seed.controller, "INSERT INTO controller_signals VALUES('new-signal','CLOSED','NEW','{}',1)")
                data = json.dumps(dict(signal_id="new-signal", status="CLOSED", client_order_id="newclient", inst_id=instrument))
                r = mutate(seed.router, "INSERT INTO routes VALUES(?,?,?,?,?,?,?)",
                    (account, "new-signal", "f"*64, "newclient", "CLOSED", "", data))
                with self.assertRaises(S.SuccessorRefused):
                    self.check_extension(seed, c, r)

    def test_new_natural_exit_record_can_extend_but_cannot_edit_old_one(self):
        seed = self.project()
        c = mutate(seed.controller, "INSERT INTO natural_exits VALUES('new','{}')")
        self.check_extension(seed, c)
        extra = mutate(self.source_files[R.CONTROLLER], "CREATE TABLE natural_exits(signal TEXT PRIMARY KEY,record TEXT NOT NULL)")
        extra = mutate(extra, "INSERT INTO natural_exits VALUES('prior','old')")
        src = tuple(R.EvidenceFile(f.name, extra if f.name == R.CONTROLLER else f.raw) for f in self.source)
        old = tuple(R.EvidenceFile(f.name, extra if f.name == R.CONTROLLER else f.raw) for f in self.original)
        anchors = replace(self.anchors, original_inventory_sha256=R.archive_digest(old), observed_inventory_sha256=R.archive_digest(src))
        projected = S.project_validated_resolution(old, src, anchors, **self.options)
        c = mutate(projected.controller, "UPDATE natural_exits SET record='changed' WHERE signal='prior'")
        with self.assertRaisesRegex(S.SuccessorRefused, "SUCCESSOR_HISTORY_CHANGED"):
            self.check_extension(projected, c)


if __name__ == "__main__":
    unittest.main()
