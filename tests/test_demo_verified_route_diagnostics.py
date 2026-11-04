"""Offline route-field refusals retain the diagnostic channel's original gates."""
from __future__ import annotations

import contextlib
import copy
import io
import json
import unittest

from src import demo_verified_risk as V
from tests import test_demo_halted_ledger_diagnostic as H
from tests.test_demo_ledger import ACCOUNT, reservation


CODE = "VERIF_ROUTE_POSITION_TIME_INVALID"
MISSING = object()


class VerifiedRouteDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.case = H.HaltedLedgerDiagnosticTests(methodName="runTest")
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.route, _ = self.case.halted_position()
        self.original_route = copy.deepcopy(self.route)

    def route_time(self, value):
        self.route.clear()
        self.route.update(copy.deepcopy(self.original_route))
        if value is MISSING:
            self.route.pop("position_opened_at_ms")
        else:
            self.route["position_opened_at_ms"] = value
        self.case.write_journal()

    def reader(self):
        provider = self.case.make_provider(diagnostic=True)
        provider._auth = provider._authorization_record()
        provider._router_halt_observations = []
        return provider

    def test_positive_filled_route_invalid_time_has_only_fixed_safe_code(self):
        values = (MISSING, None, True, False, "", " ", "PRIVATE_FIELD_SENTINEL",
                  "NaN", "Infinity", float("nan"), float("inf"), 1.5, -1)
        output = io.StringIO()
        for value in values:
            with self.subTest(kind="missing" if value is MISSING else type(value).__name__):
                self.route_time(value)
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                    with self.assertRaises(V.D.EvidenceRefused) as caught:
                        self.reader()._read_routes()
                self.assertEqual(caught.exception.code, CODE)
                self.assertEqual(str(caught.exception), CODE)
                self.assertNotIn("PRIVATE_FIELD_SENTINEL", str(caught.exception))
                self.assertNotIn("246810", str(caught.exception))
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(self.case.client.calls, [])

    def test_existing_integer_equivalence_and_activation_refusal_remain(self):
        opened = self.original_route["position_opened_at_ms"]
        # The existing Decimal-based integer parser accepts an integral float.
        # This diagnostic-code change neither expands nor narrows that rule.
        for value in (opened, str(opened), float(opened)):
            with self.subTest(kind=type(value).__name__):
                self.route_time(value)
                rows = self.reader()._read_routes()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["position_opened_at_ms"], value)
                self.assertEqual(rows[0]["status"], "HALTED")
        self.route_time(self.case.authorization["authorization_recorded_at_ms"] - 1)
        with self.assertRaisesRegex(V.D.EvidenceRefused, "^ROUTE_PRECEDES_ACTIVATION$"):
            self.reader()._read_routes()

    def test_snapshot_refusal_preserves_existing_cache_halt_and_nonadmission(self):
        self.case.provider = self.case.make_provider(diagnostic=True)
        initial = self.case.snapshot()
        self.case.locked(initial, complete=True)
        cache = self.case.root / "verified-risk" / ACCOUNT / "verified.json"
        old_cache = cache.read_bytes()
        self.route_time(MISSING)
        old_journal, old_halt = self.case.journal.read_bytes(), self.case.halt.read_bytes()
        self.case.client.calls.clear()
        self.case.provider = self.case.make_provider(diagnostic=True)
        state = self.case.snapshot()
        self.case.locked(state, code=CODE)
        self.assertEqual(state["refusal_reasons"], [CODE, "BASELINE_MISSING",
                         "LEDGER_ADAPTER_UNVERIFIED", "ROUTER_HALTED"])
        for key in ("baseline_trusted", "ledger_verified", "history_complete",
                    "account_snapshot_consistent"):
            self.assertFalse(state[key])
        self.assertIsNone(state["day_start_equity"])
        self.assertIsNone(state["week_start_equity"])
        self.assertEqual(state["request_faults"], [])
        self.assertEqual((cache.read_bytes(), self.case.journal.read_bytes(), self.case.halt.read_bytes()),
                         (old_cache, old_journal, old_halt))
        self.assertFalse(any(path.endswith(("positions-history", "fills-history", "bills-archive"))
                             for path, _ in self.case.client.calls))

    def test_default_halt_short_circuit_remains_unchanged(self):
        self.route_time(MISSING)
        self.case.provider = self.case.make_provider()
        state = self.case.snapshot()
        self.assertIn("ROUTER_HALTED", state["refusal_reasons"])
        self.assertNotIn(CODE, state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])
        self.assertNotIn("diagnostic_only", state)

    def test_other_account_numeric_failure_is_not_relabelled_as_route_time(self):
        self.route_time(MISSING)
        self.case.client.balance["details"][0]["eq"] = ""
        self.case.provider = self.case.make_provider(diagnostic=True)
        state = self.case.snapshot()
        self.case.locked(state, code="ACCOUNT_DATA_INVALID")
        self.assertNotIn(CODE, state["refusal_reasons"])
        self.assertFalse(state["history_complete"])
        self.assertNotIn("246810", json.dumps(state))

    def test_zero_filled_unresolved_route_keeps_its_original_ledger_refusal(self):
        self.case.data["routes"] = [dict(reservation(), status="UNKNOWN")]
        self.case.write_journal()
        self.case.provider = self.case.make_provider(diagnostic=True)
        state = self.case.snapshot()
        self.case.locked(state, code="LEDGER_UNRESOLVED_OWNED_ROUTE")
        self.assertNotIn(CODE, state["refusal_reasons"])

    def test_filled_quantity_parser_keeps_its_original_refusal(self):
        self.route["filled_qty"] = "PRIVATE_QUANTITY_SENTINEL"
        self.case.write_journal()
        with self.assertRaisesRegex(V.D.EvidenceRefused, "^ACCOUNT_DATA_INVALID$"):
            self.reader()._read_routes()


if __name__ == "__main__":
    unittest.main()
