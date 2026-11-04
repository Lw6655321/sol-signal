"""HALT-preserving, non-admitting ledger diagnostics with offline raw fixtures."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import unittest
from contextlib import closing
from unittest import mock

from src import demo_verified_risk as V
from tests import test_demo_verified_risk as fixtures
from tests.test_demo_ledger import ACCOUNT, reservation


class HaltedLedgerDiagnosticTests(unittest.TestCase):
    # Reuse fixture construction without inheriting/rediscovering its test suite.
    write_journal = fixtures.VerifiedRiskTests.write_journal
    snapshot = fixtures.VerifiedRiskTests.snapshot
    add = fixtures.VerifiedRiskTests.add

    def setUp(self):
        fixtures.VerifiedRiskTests.setUp(self)
        block = mock.patch("socket.socket.connect", side_effect=AssertionError("NO_NETWORK"))
        block.start()
        self.addCleanup(block.stop)
        self.halt = self.root / (
            "HALT-" + hashlib.sha256(b"DEMO-AUTO-SOL-V1").hexdigest()[:12])

    def make_provider(self, authorization=None, *, diagnostic=False):
        return V.VerifiedDemoRiskProvider(
            self.client, self.config, self.root / "verified-risk",
            authorization=self.authorization if authorization is None else authorization,
            router_journal_path=self.journal, clock=lambda: self.now,
            rate_clock=fixtures.NonContendingFixtureQuotaClock(),
            diagnostic_halted_ledger=diagnostic)

    def halted_position(self, *, status="HALTED"):
        route, history = self.add(gross="3", funding="-0.1")
        route.update(status=status, exit_reconciled=True)
        self.write_journal()
        self.set_controls([("DEMO-AUTO-SOL-V1", "LEDGER_TIME_MISMATCH")])
        self.halt.write_bytes(b"LEDGER_TIME_MISMATCH\r\n")
        return route, history

    def set_controls(self, values):
        with closing(sqlite3.connect(self.journal)) as db, db:
            db.execute("DELETE FROM controls")
            db.executemany("INSERT INTO controls VALUES(?,?)", values)

    def locked(self, state, *, complete=False, code=None):
        self.assertTrue(state["diagnostic_only"])
        self.assertFalse(state["demo_verified"])
        self.assertTrue(state["halted"])
        self.assertFalse(state["live_verified"])
        self.assertIn("ROUTER_HALTED", state["refusal_reasons"])
        self.assertEqual(state["diagnostic_ledger_complete"], complete)
        if code:
            self.assertIn(code, state["refusal_reasons"])
        self.assertNotIn("246810", json.dumps(state))

    def test_default_provider_preserves_immediate_halt_refusal(self):
        self.halted_position()
        before = self.journal.read_bytes(), self.halt.read_bytes()
        state = self.snapshot()
        self.assertIn("ROUTER_HALTED", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["demo_verified"])
        self.assertNotIn("diagnostic_only", state)
        self.assertFalse(any(path.endswith("positions-history") for path, _ in self.client.calls))
        self.assertEqual((self.journal.read_bytes(), self.halt.read_bytes()), before)

    def test_opt_in_proves_complete_closed_ledger_but_preserves_halt_and_bytes(self):
        route, _ = self.halted_position()
        before = self.journal.read_bytes(), self.halt.read_bytes(), copy.deepcopy(route)
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, complete=True)
        self.assertEqual(state["refusal_reasons"], ["ROUTER_HALTED"])
        self.assertTrue(state["ledger_verified"])
        self.assertEqual(state["realized_pnl_today"], "2.7")
        self.assertEqual(state["entries_today"], 1)
        self.assertEqual(len(state["router_halt_observations"]), 2)
        self.assertTrue(all(row == {"control_codes": ["LEDGER_TIME_MISMATCH"],
            "route_status_counts": {"HALTED": 1}, "halt_flag_present": True}
            for row in state["router_halt_observations"]))
        self.assertEqual((self.journal.read_bytes(), self.halt.read_bytes(), route), before)
        self.assertTrue(all(path in V.SAFE_GET_PATHS for path, _ in self.client.calls))
        # A subsequent default provider still sees the original HALT.
        self.provider = self.make_provider()
        self.assertIn("ROUTER_HALTED", self.snapshot()["refusal_reasons"])

    def test_diagnostic_channel_is_locked_even_if_router_has_no_halt(self):
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, complete=True)
        self.assertTrue(state["ledger_verified"])
        self.assertTrue(all(row == {"control_codes": [], "route_status_counts": {},
            "halt_flag_present": False} for row in state["router_halt_observations"]))

    def test_halt_flag_and_halted_route_remain_facts_without_a_control_row(self):
        self.halted_position()
        self.set_controls([])
        before = self.journal.read_bytes(), self.halt.read_bytes()
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, complete=True)
        self.assertTrue(all(row == {"control_codes": [],
            "route_status_counts": {"HALTED": 1}, "halt_flag_present": True}
            for row in state["router_halt_observations"]))
        self.assertEqual((self.journal.read_bytes(), self.halt.read_bytes()), before)

    def test_flag_requires_an_actual_bool_and_default_remains_false(self):
        for value in (None, 0, 1, "true", "False", [], {}):
            with self.subTest(value=value):
                with self.assertRaisesRegex(V.D.EvidenceRefused, "DIAGNOSTIC_HALTED_LEDGER_INVALID"):
                    self.make_provider(diagnostic=value)
        self.assertFalse(self.provider.diagnostic_halted_ledger)

    def test_canonical_unknown_status_is_recorded_but_unresolved_route_still_refuses(self):
        route = dict(reservation(), status="UNKNOWN")
        self.data["routes"] = [route]
        self.write_journal()
        self.set_controls([("DEMO-AUTO-SOL-V1", "UNKNOWN_OUTCOME")])
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, code="LEDGER_UNRESOLVED_OWNED_ROUTE")
        self.assertFalse(state["ledger_verified"])
        self.assertEqual(state["router_halt_observations"][0]["route_status_counts"], {"UNKNOWN": 1})

    def test_wrong_or_missing_route_account_binding_is_never_adopted(self):
        for change in ("missing", "foreign", "wrong-column"):
            with self.subTest(change=change):
                self.data = fixtures.evidence()
                route, _ = self.add()
                route["status"] = "HALTED"
                if change == "missing":
                    route.pop("account_hash")
                elif change == "foreign":
                    route["account_hash"] = "f" * 64
                self.write_journal()
                if change == "wrong-column":
                    with closing(sqlite3.connect(self.journal)) as db, db:
                        db.execute("UPDATE routes SET account='FOREIGN'")
                self.provider = self.make_provider(diagnostic=True)
                state = self.snapshot()
                self.locked(state, code="ROUTER_JOURNAL_INVALID")
                self.assertFalse(state["ledger_verified"])

    def test_foreign_duplicate_or_malformed_controls_are_not_ignored(self):
        bad_controls = [ [("FOREIGN", "UNKNOWN_OUTCOME")],
            [("DEMO-AUTO-SOL-V1", "UNKNOWN_OUTCOME")] * 2,
            [("DEMO-AUTO-SOL-V1", None)], [("DEMO-AUTO-SOL-V1", "BAD\nCODE")],
            [("DEMO-AUTO-SOL-V1", "unknown")], [("DEMO-AUTO-SOL-V1", "X" * 129)] ]
        for controls in bad_controls:
            with self.subTest(controls=controls):
                self.set_controls(controls)
                before = self.journal.read_bytes()
                self.provider = self.make_provider(diagnostic=True)
                state = self.snapshot()
                self.locked(state, code="ROUTER_JOURNAL_INVALID")
                self.assertFalse(state["ledger_verified"])
                self.assertEqual(self.journal.read_bytes(), before)

    def test_malicious_json_or_unknown_status_cannot_be_normalized_to_closed(self):
        route, _ = self.halted_position()
        for corrupt in ("not-json", json.dumps(dict(route, status="NEW_MAGIC_STATUS"))):
            with self.subTest(corrupt=corrupt):
                with closing(sqlite3.connect(self.journal)) as db, db:
                    db.execute("UPDATE routes SET data=?,status=?", (
                        corrupt, "HALTED" if corrupt == "not-json" else "NEW_MAGIC_STATUS"))
                self.provider = self.make_provider(diagnostic=True)
                state = self.snapshot()
                self.locked(state, code="ROUTER_JOURNAL_INVALID")
                self.assertFalse(state["ledger_verified"])

    def test_corrupt_or_missing_journal_remains_refused_and_is_not_created(self):
        self.journal.write_bytes(b"not a sqlite database")
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, code="ROUTER_JOURNAL_INVALID")
        self.assertEqual(self.journal.read_bytes(), b"not a sqlite database")
        self.journal.unlink()
        self.provider = self.make_provider(diagnostic=True)
        self.locked(self.snapshot(), code="ROUTER_JOURNAL_MISSING")
        self.assertFalse(self.journal.exists())

    def test_opt_in_does_not_bypass_account_identity_or_activation_binding(self):
        self.halted_position()
        self.provider = self.make_provider(dict(self.authorization, account_hash="f" * 64), diagnostic=True)
        self.locked(self.snapshot(), code="DEMO_AUTHORIZATION_ACCOUNT_MISMATCH")
        self.provider = self.make_provider(diagnostic=True)
        self.locked(self.snapshot(), complete=True)
        changed = dict(self.authorization, authorization_recorded_at_ms=self.now - 1000)
        self.provider = self.make_provider(changed, diagnostic=True)
        self.locked(self.snapshot(), code="DEMO_AUTHORIZATION_CHANGED")

    def test_bill_mismatch_missing_history_and_cash_flow_preserve_all_original_refusals(self):
        self.halted_position()
        original = copy.deepcopy(self.data)
        for changed, code in (("bill", "LEDGER_TRADE_BILL_MISMATCH"),
                              ("history", "LEDGER_POSITION_HISTORY_MISSING"),
                              ("cash", "LEDGER_EXTERNAL_CASH_FLOW")):
            with self.subTest(changed=changed):
                self.data.clear()
                self.data.update(copy.deepcopy(original))
                if changed == "bill":
                    self.data["bills"][0]["fee"] = "-999"
                elif changed == "history":
                    self.data["positions_history"].pop()
                else:
                    self.data["bills"].append(dict(billId="99999", ts=str(self.now - 1),
                        type="1", ccy="USDT", instId=V.D.INST, instType="SWAP", balChg="1"))
                self.provider = self.make_provider(diagnostic=True)
                state = self.snapshot()
                self.locked(state, code=code)
                self.assertFalse(state["ledger_verified"])

    def test_original_market_refusal_survives_a_verified_ledger(self):
        self.halted_position()
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot(macro_blackout=True)
        self.locked(state, code="MACRO_BLACKOUT")
        self.assertTrue(state["ledger_verified"])

    def test_account_changes_prevent_complete_diagnostic_even_with_closed_history(self):
        self.halted_position()
        reads = 0

        def changed_balance(params):
            nonlocal reads
            reads += 1
            balance = copy.deepcopy(self.client.balance)
            if reads > 1:
                balance["details"][0]["eq"] = "2501"
                balance["totalEq"] = "2501"
            return [balance]

        self.client.overrides["/api/v5/account/balance"] = changed_balance
        self.provider = self.make_provider(diagnostic=True)
        state = self.snapshot()
        self.locked(state, code="ACCOUNT_CHANGED_DURING_SNAPSHOT")
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["account_snapshot_consistent"])

    def test_stale_account_never_claims_complete_diagnostic(self):
        self.halted_position()
        self.provider = self.make_provider(diagnostic=True)
        original_history = self.provider._account_history

        def advance_after_ledger(*args, **kwargs):
            original_history(*args, **kwargs)
            self.now += V.D.MAX_ACCOUNT_AGE_MS + 1

        with mock.patch.object(self.provider, "_account_history", side_effect=advance_after_ledger):
            state = self.snapshot()
        self.locked(state, code="ACCOUNT_STALE")
        # Existing field describes the completed pure-ledger proof only.
        self.assertTrue(state["ledger_verified"])

    def test_controls_changed_during_collection_cannot_complete_diagnostic(self):
        self.halted_position()
        self.provider = self.make_provider(diagnostic=True)
        original = self.provider._positions_history
        changed = False

        def changed_controls(*args, **kwargs):
            nonlocal changed
            if not changed:
                changed = True
                self.set_controls([("DEMO-AUTO-SOL-V1", "ANOTHER_HALT")])
            return original(*args, **kwargs)

        with mock.patch.object(self.provider, "_positions_history", side_effect=changed_controls):
            state = self.snapshot()
        self.locked(state, code="ROUTER_CHANGED_DURING_SNAPSHOT")
        self.assertFalse(state["ledger_verified"])


if __name__ == "__main__":
    unittest.main()
