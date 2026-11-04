"""Fixed refusal details preserve the closed-proof caller's original gates."""
from copy import deepcopy
import hashlib
import io
import json
import sqlite3
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import test_demo_halted_exit_receipts as fixtures
from src.demo_route_transport import DemoRouteError


PREFIX = "DEMO_HALTED_EXIT_"
SENTINEL = "PRIVATE_RESPONSE_ID_HEADER_SENTINEL"


class NoTextError(Exception):
    def __str__(self):
        raise AssertionError("EXCEPTION_TEXT_MUST_NOT_BE_READ")


class HaltedExitDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.ClosedHaltedReceipts()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.proof = self.helper.verify()
        self.router = self.helper.router()
        self.original = self.helper.route(self.router)
        self.helper.gets.clear()

    def reset_row(self, **changes):
        row = dict(deepcopy(self.original), **changes)
        with self.router._db() as db:
            db.execute("UPDATE routes SET data=?,status=?,owner='' WHERE account=?",
                       (json.dumps(row, sort_keys=True), row["status"], self.router.account_key))
        return row

    def reconcile(self, proof, *, wall=(fixtures.NOW, fixtures.NOW), mono=(0, 0), error=None):
        clock = iter(wall)
        self.router.clock = lambda: next(clock, wall[-1])
        output = io.StringIO()
        with patch.object(self.helper.transport, "verify_closed_position_lifecycle",
                          return_value=proof, side_effect=error) as verify, \
                patch("src.order_router.time.monotonic_ns", side_effect=mono), \
                patch.object(self.router, "_emergency", side_effect=AssertionError("NO_EMERGENCY")), \
                patch.object(self.router, "_halt", side_effect=AssertionError("NO_HALT_REWRITE")), \
                redirect_stdout(output), redirect_stderr(output):
            result = self.router.reconcile_halted_exit(self.helper.intent)
        self.assertEqual(output.getvalue(), "")
        return result, verify

    def assert_refusal(self, result, suffix, *, original=None, archived=False):
        self.assertEqual((result.status, result.code), ("HALTED", "OBSERVE_HALTED"))
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        row = self.helper.route(self.router)
        expected_code = suffix if suffix.startswith("DEMO_") or suffix in {"CLOCK", "INVALID_SNAPSHOT"} else PREFIX+suffix
        self.assertEqual(row, dict(original or self.original, exit_reconcile_code=expected_code))
        self.assertFalse(row.get("exit_reconciled", False))
        if not archived:
            self.assertEqual(list(self.router.run_dir.glob("halted-exit-*")), [])
        with self.router._db() as db:
            self.assertEqual(db.execute("SELECT owner FROM routes").fetchall(), [("",)])
        self.helper.assert_halt(self.router)
        self.assertEqual(self.helper.gets, [])
        self.assertNotIn(SENTINEL, self.router.audit_path.read_text(encoding="utf-8"))
        self.assertNotIn(SENTINEL, row["exit_reconcile_code"])

    def test_shape_and_each_strict_true_flag_have_fixed_codes(self):
        result, _ = self.reconcile(None)
        self.assert_refusal(result, "PROOF_SHAPE_INVALID")
        for field, suffix in (("closed_lifecycle_verified", "CLOSED_FLAG_INVALID"),
                              ("position_binding_verified", "BINDING_FLAG_INVALID"),
                              ("exit_reconciled", "RECONCILED_FLAG_INVALID")):
            for value in (None, False, 1, "true"):
                with self.subTest(field=field, value=value):
                    result, _ = self.reconcile(dict(self.proof, **{field: value}))
                    self.assert_refusal(result, suffix)

    def test_account_and_flat_equality_gates_are_unchanged(self):
        for field, value, suffix in (("account_hash", SENTINEL, "ACCOUNT_MISMATCH"),
                                     ("flat_observed_at_ms", self.helper.flat+1, "FLAT_MISMATCH")):
            with self.subTest(field=field):
                result, _ = self.reconcile(dict(self.proof, **{field: value}))
                self.assert_refusal(result, suffix)

    def test_original_short_circuit_order_selects_first_failed_gate(self):
        cases = [({"closed_lifecycle_verified": False, "position_binding_verified": False}, "CLOSED_FLAG_INVALID"),
                 ({"position_binding_verified": False, "exit_reconciled": False}, "BINDING_FLAG_INVALID"),
                 ({"exit_reconciled": False, "account_hash": SENTINEL}, "RECONCILED_FLAG_INVALID"),
                 ({"account_hash": SENTINEL, "flat_observed_at_ms": -1}, "ACCOUNT_MISMATCH"),
                 ({"flat_observed_at_ms": -1}, "FLAT_MISMATCH")]
        for changes, suffix in cases:
            with self.subTest(suffix=suffix):
                result, _ = self.reconcile(dict(self.proof, **changes),
                                           wall=(fixtures.NOW, fixtures.NOW+5001), mono=(0, -1))
                self.assert_refusal(result, suffix)

    def test_collection_start_wall_and_monotonic_types_refuse_before_transport(self):
        for wall, mono, suffix in ((0, 0, "START_WALL_INVALID"),
                                   (fixtures.NOW, True, "START_MONO_INVALID"),
                                   (fixtures.NOW, 1.0, "START_MONO_INVALID"),
                                   (fixtures.NOW, "1", "START_MONO_INVALID"),
                                   (fixtures.NOW, -1, "START_MONO_INVALID")):
            with self.subTest(suffix=suffix, mono_type=type(mono).__name__):
                result, verify = self.reconcile(self.proof, wall=(wall,), mono=(mono,))
                verify.assert_not_called()
                self.assert_refusal(result, suffix)

    def test_existing_wall_clock_parser_refusal_codes_are_preserved(self):
        for value, code in ((None, "INVALID_SNAPSHOT"), (True, "INVALID_SNAPSHOT"),
                             (-1, "INVALID_SNAPSHOT"), (float("nan"), "INVALID_SNAPSHOT"),
                             (1.5, "CLOCK")):
            with self.subTest(value_type=type(value).__name__, code=code):
                result, verify = self.reconcile(self.proof, wall=(value,))
                verify.assert_not_called()
                self.assert_refusal(result, code)

    def test_completion_monotonic_and_source_time_keep_exact_int_semantics(self):
        for value in (None, True, 1.0, "1"):
            with self.subTest(mono_type=type(value).__name__):
                result, _ = self.reconcile(self.proof, mono=(0, value))
                self.assert_refusal(result, "CALLER_MONO_TYPE_INVALID")
            with self.subTest(source_type=type(value).__name__):
                result, _ = self.reconcile(dict(self.proof, observed_at_ms=value))
                self.assert_refusal(result, "SOURCE_TIME_TYPE_INVALID")

    def test_time_failure_priority_and_closed_interval_boundaries(self):
        for wall, mono, source, suffix in ((5001, -1, -1, "CALLER_WALL_STALE"),
                                           (0, -1, -1, "CALLER_MONO_STALE"),
                                           (0, 5_000_000_001, 0, "CALLER_MONO_STALE"),
                                           (0, 0, -5001, "SOURCE_TIME_STALE"),
                                           (0, 0, 1, "SOURCE_TIME_STALE")):
            with self.subTest(suffix=suffix, wall=wall, mono=mono):
                result, _ = self.reconcile(dict(self.proof, observed_at_ms=fixtures.NOW+source),
                    wall=(fixtures.NOW, fixtures.NOW+wall), mono=(0, mono))
                self.assert_refusal(result, suffix)

    def test_existing_position_binding_mismatch_refuses_without_rebinding(self):
        row = self.reset_row(position_id=SENTINEL, position_opened_at_ms=self.helper.opened)
        result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "POSITION_ID_MISMATCH", original=row)
        row = self.reset_row(position_id=self.proof["position_id"], position_opened_at_ms=self.helper.opened)
        result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "POSITION_TIME_MISMATCH", original=row)

    def test_success_preserves_missing_empty_and_equal_legacy_bindings_and_proof_bytes(self):
        for changes in ({}, {"position_id": "", "position_opened_at_ms": None},
                        {"position_id": self.proof["position_id"],
                         "position_opened_at_ms": self.proof["position_opened_at_ms"]}):
            with self.subTest(binding_shape=tuple(changes)):
                self.reset_row(**changes)
                result, _ = self.reconcile(self.proof, wall=(fixtures.NOW, fixtures.NOW+5000),
                                           mono=(0, 5_000_000_000))
                self.assertEqual(result.emergency_status, "FLAT_EXIT_RECONCILED_HALTED")
                row = self.helper.route(self.router)
                self.assertEqual(row["exit_reconcile_code"], PREFIX+"VERIFIED")
                expected = json.dumps(self.proof, sort_keys=True, default=str).encode("utf-8")
                self.assertEqual(row["exit_reconcile_sha256"], hashlib.sha256(expected).hexdigest())
                from pathlib import Path
                self.assertEqual(Path(row["exit_reconcile_proof_path"]).read_bytes(), expected)
                self.helper.assert_halt(self.router)

    def test_unexpected_collect_errors_use_only_fixed_type_categories_without_text(self):
        for error, category in ((OSError(SENTINEL), "IO"), (sqlite3.DatabaseError(SENTINEL), "SQLITE"),
                                (TypeError(SENTINEL), "TYPE"), (ValueError(SENTINEL), "VALUE"),
                                (KeyError(SENTINEL), "LOOKUP"), (ZeroDivisionError(SENTINEL), "ARITHMETIC"),
                                (NoTextError(SENTINEL), "OTHER")):
            with self.subTest(category=category):
                result, _ = self.reconcile(self.proof, error=error)
                self.assert_refusal(result, "COLLECT_"+category+"_ERROR")

    def test_existing_domain_codes_and_invalid_code_sanitization_are_preserved(self):
        for code, expected in (("DEMO_LIFECYCLE_STALE", "DEMO_LIFECYCLE_STALE"),
                               (SENTINEL+"!", PREFIX+"UNPROVEN")):
            result, _ = self.reconcile(self.proof, error=DemoRouteError(code))
            self.assert_refusal(result, expected)

    def test_missing_binding_field_is_fixed_bind_lookup_and_never_archives(self):
        proof = deepcopy(self.proof)
        del proof["position_closed_at_ms"]
        result, _ = self.reconcile(proof)
        self.assert_refusal(result, "BIND_LOOKUP_ERROR")

    def test_unexpected_completion_clock_error_has_fixed_end_category(self):
        def clock():
            if getattr(clock, "called", False):
                raise OSError(SENTINEL)
            clock.called = True
            return fixtures.NOW

        with patch.object(self.router, "_now", side_effect=clock):
            result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "END_IO_ERROR")

    def test_unexpected_proof_access_error_has_fixed_check_category(self):
        class BrokenProof(dict):
            def get(self, field, default=None):
                if field == "closed_lifecycle_verified":
                    raise NoTextError(SENTINEL)
                return super().get(field, default)

        result, _ = self.reconcile(BrokenProof(self.proof))
        self.assert_refusal(result, "CHECK_OTHER_ERROR")

    def test_archive_io_failure_keeps_flags_owner_and_halt_without_proof(self):
        with patch.object(self.router, "_store_halted_exit_proof", side_effect=OSError(SENTINEL)):
            result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "ARCHIVE_IO_ERROR")

    def test_serialization_type_failure_does_not_change_fallback_or_archive(self):
        original_dumps = json.dumps

        def dumps(value, *args, **kwargs):
            if value is self.proof:
                raise TypeError(SENTINEL)
            return original_dumps(value, *args, **kwargs)

        with patch("src.order_router.json.dumps", side_effect=dumps):
            result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "SERIALIZE_TYPE_ERROR")

    def test_journal_audit_failure_retains_preexisting_archive_order_and_flags(self):
        with patch.object(self.router, "_audit", side_effect=OSError(SENTINEL)):
            result, _ = self.reconcile(self.proof)
        self.assert_refusal(result, "JOURNAL_IO_ERROR", archived=True)
        expected = json.dumps(self.proof, sort_keys=True, default=str).encode("utf-8")
        archives = list(self.router.run_dir.glob("halted-exit-*.json"))
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0].read_bytes(), expected)

    def test_claim_unexpected_error_keeps_existing_unknown_fallback_shape(self):
        with patch.object(self.router, "_demo_halted_readonly_claim", side_effect=NoTextError(SENTINEL)):
            result, verify = self.reconcile(self.proof)
        verify.assert_not_called()
        self.assertEqual((result.status, result.code), ("UNKNOWN", PREFIX+"CLAIM_OTHER_ERROR"))
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertEqual(self.helper.route(self.router), self.original)
        self.helper.assert_halt(self.router)


if __name__ == "__main__":
    unittest.main()
