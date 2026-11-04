"""Offline collector/ledger integration using raw API-shaped fixtures only."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from unittest import mock

from src import demo_verified_risk as V
from tests.test_demo_ledger import ACCOUNT, MONDAY, NOW, add_position, evidence, reservation
from tests.test_demo_risk import CONFIG, ElapsedClock, FakeClient


class HistoricalClient(FakeClient):
    """Honor the actual endpoint windows/cursors; never fill missing API fields."""
    def __init__(self, now, data):
        super().__init__(now)
        for field in V.FUTURES_NA_FIELDS:
            self.balance["details"][0][field] = "0"
        self.evidence = data
        self.overrides = {}

    def get(self, path, params):
        if path in self.overrides:
            self.calls.append((path, dict(params)))
            value = self.overrides[path]
            return value(params) if callable(value) else copy.deepcopy(value)
        if path.endswith("fills-history"):
            self.calls.append((path, dict(params)))
            rows = [row for row in self.evidence["fills"] if row["instType"] == params["instType"]]
            return self.page(rows, params, "fillTime", "billId")
        if path.endswith("bills-archive"):
            self.calls.append((path, dict(params)))
            return self.page(self.evidence["bills"], params, "ts", "billId")
        if path.endswith("positions-history"):
            self.calls.append((path, dict(params)))
            rows = [row for row in self.evidence["positions_history"]
                    if row["instType"] == params["instType"] and int(row["uTime"]) < int(params["after"])]
            return copy.deepcopy(sorted(rows, key=lambda row: int(row["uTime"]), reverse=True)
                                 [:int(params["limit"])])
        if path == "/api/v5/trade/order":
            self.calls.append((path, dict(params)))
            row = self.evidence["order_details"].get(params["ordId"])
            return [copy.deepcopy(row)] if row else []
        return super().get(path, params)

    @staticmethod
    def page(rows, params, time_key, id_key):
        rows = [row for row in rows if int(params["begin"]) <= int(row[time_key]) <= int(params["end"])
                and ("after" not in params or int(row[id_key]) < int(params["after"]))]
        return copy.deepcopy(sorted(rows, key=lambda row: int(row[id_key]), reverse=True)
                             [:int(params["limit"])])


class NonContendingFixtureQuotaClock:
    """Ordinary ledger cases do not simulate repeated real-time service polls.

    Advance only the separate quota clock; account-evidence clock is unchanged.
    Dedicated tests below exercise the production limiter and actual delays.
    """
    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            self.value += 3
            return self.value


class VerifiedRiskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.now = NOW
        self.data = evidence()
        self.config = copy.deepcopy(CONFIG)
        self.client = HistoricalClient(self.now, self.data)
        self.authorization = {"environment": "DEMO", "account_hash": ACCOUNT,
                              "authorization_recorded_at_ms": MONDAY - 4*3_600_000,
                              "source": V.AUTH_SOURCE, "scope": V.AUTH_SCOPE}
        self.journal = self.root / "router.sqlite3"
        self.write_journal()
        self.provider = self.make_provider()
        self.no_network = mock.patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK"))
        self.no_network.start()
        self.addCleanup(self.no_network.stop)

    def make_provider(self, authorization=None):
        return V.VerifiedDemoRiskProvider(self.client, self.config, self.root / "verified-risk",
                  authorization=self.authorization if authorization is None else authorization,
                  router_journal_path=self.journal, clock=lambda: self.now,
                  rate_clock=NonContendingFixtureQuotaClock())

    def write_journal(self):
        with closing(sqlite3.connect(self.journal)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS routes(account TEXT, signal TEXT, status TEXT, client TEXT, data TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS controls(account TEXT,code TEXT)")
            db.execute("DELETE FROM routes")
            for route in self.data["routes"]:
                db.execute("INSERT INTO routes VALUES(?,?,?,?,?)", ("DEMO-AUTO-SOL-V1",
                    route["signal_id"], route["status"], route["client_order_id"], json.dumps(route)))

    def snapshot(self, **changes):
        market = {"data_fresh": True, "macro_blackout": False, "observed_at_ms": self.now}
        market.update(changes)
        return self.provider.snapshot(market)

    def add(self, number=1, opened=None, closed=None, **kwargs):
        opened = MONDAY + number*600_000 if opened is None else opened
        closed = opened+300_000 if closed is None else closed
        route, history = add_position(self.data, number, opened, closed, **kwargs)
        route.update(created_at_ms=opened-1000, submit_attempted=True, inst_id=V.D.INST)
        self.write_journal()
        return route, history

    def test_complete_legacy_anchor_and_empty_new_period_admit_late_flat_reconstruction(self):
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertTrue(state["ledger_verified"])
        self.assertTrue(state["pause_evidence_verified"])
        self.assertFalse(state["live_verified"])
        self.assertEqual(state["day_start_equity"], "2500")
        self.assertEqual(state["week_start_equity"], "2500")
        self.assertGreater(state["observed_at_ms"], MONDAY+120_000)
        self.assertIn("reconstruction", state["baseline_source"])
        self.assertEqual(state["account_hash"], ACCOUNT)
        self.assertNotIn("246810", json.dumps(state))

    def test_completed_owned_position_reconciles_actual_order_details_and_net_equity(self):
        self.add(gross="3", funding="-0.1")
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(Decimal(state["realized_pnl_today"]), Decimal("2.7"))
        self.assertEqual(Decimal(state["day_start_equity"]), Decimal("2497.3"))
        self.assertEqual(state["entries_today"], 1)
        orders = {p["ordId"] for path, p in self.client.calls if path == "/api/v5/trade/order"}
        self.assertEqual(orders, set(self.data["order_details"]))

    def test_daily_entry_and_loss_gates_still_block_after_three_losses(self):
        for number in (1, 2, 3):
            self.add(number, gross="-100")
        state = self.snapshot()
        self.assertTrue(state["ledger_verified"], state["refusal_reasons"])
        self.assertEqual(state["consecutive_losses"], 3)
        self.assertIs(state["consecutive_loss_cooldown_enabled"], False)
        self.assertIs(state["paused"], False)
        self.assertEqual(state["paused_until_ms"], 0)
        self.assertNotIn("CONSECUTIVE_LOSS_PAUSE", state["refusal_reasons"])
        for code in ("DAILY_ENTRY_LIMIT", "DAILY_LOSS_LIMIT", "WEEKLY_LOSS_LIMIT"):
            self.assertIn(code, state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])

    def test_prior_day_loss_streak_alone_does_not_block_demo_entry(self):
        for number in (1, 2, 3):
            opened = MONDAY+23*3_600_000+number*300_000
            self.add(number, opened=opened, closed=opened+60_000)
        self.now = MONDAY+86_400_000+600_000
        self.client.now = self.now
        self.client.balance["uTime"] = str(self.now)
        state = self.snapshot()
        self.assertEqual(state["entries_today"], 0)
        self.assertEqual(Decimal(state["realized_pnl_today"]), 0)
        self.assertEqual(state["consecutive_losses"], 3)
        self.assertIs(state["paused"], False)
        self.assertEqual(state["paused_until_ms"], 0)
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])

    def test_position_crossing_period_boundary_cannot_reconstruct_unrealized_equity(self):
        self.add(opened=MONDAY-60_000, closed=MONDAY+60_000, gross="3")
        state = self.snapshot()
        self.assertTrue(state["ledger_verified"])
        self.assertIn("BASELINE_BOUNDARY_NOT_FLAT", state["refusal_reasons"])
        self.assertIsNone(state["day_start_equity"])
        self.assertFalse(state["demo_verified"])

    def test_new_unknown_cash_flow_refuses_without_claiming_trusted_pnl(self):
        self.data["bills"].append({"billId": "99999", "ts": str(MONDAY+1000),
          "type": "1", "ccy": "USDT", "instId": V.D.INST, "instType": "SWAP", "balChg": "100"})
        state = self.snapshot()
        self.assertIn("LEDGER_EXTERNAL_CASH_FLOW", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])
        self.assertIsNone(state["realized_pnl_today"])

    def test_old_cash_flow_before_anchor_is_not_an_infinite_diagnostic_latch(self):
        self.data["bills"].append({"billId": "99999", "ts": str(int(self.data["positions_history"][0]["uTime"])-1000),
          "type": "1", "ccy": "USDT", "instId": "", "instType": "", "balChg": "100"})
        old_diagnostic = self.root / "old-risk" / ACCOUNT / "risk.json"
        old_diagnostic.parent.mkdir(parents=True)
        old_diagnostic.write_text('{"funding_flow_latched":true}', encoding="utf-8")
        self.assertTrue(self.snapshot()["demo_verified"])
        self.assertEqual(old_diagnostic.read_text(encoding="utf-8"), '{"funding_flow_latched":true}')

    def test_no_authorization_record_is_a_refusal(self):
        self.provider = self.make_provider({})
        state = self.snapshot()
        self.assertIn("DEMO_AUTHORIZATION_INVALID", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])

    def test_changed_persisted_activation_record_cannot_select_a_new_anchor(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        changed = dict(self.authorization, authorization_recorded_at_ms=MONDAY-1000)
        self.provider = self.make_provider(changed)
        self.assertIn("DEMO_AUTHORIZATION_CHANGED", self.snapshot()["refusal_reasons"])

    def test_authorization_for_another_account_is_rejected(self):
        self.provider = self.make_provider(dict(self.authorization, account_hash="f"*64))
        self.assertIn("DEMO_AUTHORIZATION_ACCOUNT_MISMATCH", self.snapshot()["refusal_reasons"])

    def test_anchor_after_recorded_activation_or_after_week_start_is_rejected(self):
        anchor = self.data["positions_history"][0]
        anchor["uTime"] = str(MONDAY+1000)
        self.assertIn("LEDGER_LOSS_ANCHOR_UNVERIFIED", self.snapshot()["refusal_reasons"])

    def test_empty_history_alone_never_creates_a_loss_streak_anchor(self):
        self.data["positions_history"] = []
        self.assertIn("LEDGER_LOSS_ANCHOR_UNVERIFIED", self.snapshot()["refusal_reasons"])

    def test_recent_loss_before_the_legacy_win_does_not_erase_prior_cooldown(self):
        row = copy.deepcopy(self.data["positions_history"][0])
        row.update(posId="old-loss", uTime=str(int(row["uTime"])-1000),
                   pnl="-10", fee="-1", fundingFee="0", realizedPnl="-11")
        self.data["positions_history"].append(row)
        self.assertIn("LEDGER_ANCHOR_COOLDOWN_UNPROVEN", self.snapshot()["refusal_reasons"])

    def test_unowned_actual_completed_position_is_not_inferred_from_sol_symbol(self):
        self.add()
        self.data["routes"] = []
        self.write_journal()
        self.assertIn("LEDGER_EXTERNAL_POSITION", self.snapshot()["refusal_reasons"])

    def test_missing_raw_order_detail_or_algo_link_cannot_be_fabricated(self):
        self.add()
        del self.data["order_details"]["201"]["algoId"]
        state = self.snapshot()
        self.assertIn("LEDGER_FILL_OWNERSHIP_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])

    def test_order_detail_not_found_is_not_an_empty_ledger(self):
        self.add()
        self.data["order_details"].pop("201")
        self.assertIn("ACCOUNT_RESPONSE_INVALID", self.snapshot()["refusal_reasons"])

    def test_missing_journal_is_never_created_by_provider(self):
        self.journal.unlink()
        state = self.snapshot()
        self.assertIn("ROUTER_JOURNAL_MISSING", state["refusal_reasons"])
        self.assertFalse(self.journal.exists())

    def test_existing_router_halt_is_preserved(self):
        with closing(sqlite3.connect(self.journal)) as db, db:
            db.execute("INSERT INTO controls VALUES('DEMO-AUTO-SOL-V1','UNKNOWN_OUTCOME')")
        halt = self.root / "HALT"
        halt.write_text("UNKNOWN_OUTCOME", encoding="utf-8")
        self.assertIn("ROUTER_HALTED", self.snapshot()["refusal_reasons"])
        self.assertEqual(halt.read_text(encoding="utf-8"), "UNKNOWN_OUTCOME")
        with closing(sqlite3.connect(self.journal)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM controls").fetchone()[0], 1)

    def test_foreign_pending_orders_and_macro_errors_survive_successful_ledger(self):
        self.client.pending = [{"instId": "BTC-USDT", "ordId": "foreign"}]
        state = self.snapshot(macro_blackout=True)
        self.assertIn("UNKNOWN_OPEN_ORDER", state["refusal_reasons"])
        self.assertIn("MACRO_BLACKOUT", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])

    def test_unknown_history_page_or_missing_product_scope_cannot_be_self_certified(self):
        self.client.overrides["/api/v5/trade/fills-history"] = None
        self.assertIn("ACCOUNT_RESPONSE_INVALID", self.snapshot()["refusal_reasons"])
        self.client.overrides.clear()
        with mock.patch.object(V.D, "FILL_TYPES", ("SWAP",)):
            self.assertIn("VERIFIED_ACCOUNT_SCOPE_UNPROVEN", self.snapshot()["refusal_reasons"])

    def test_full_timestamp_page_with_unseen_tie_possibility_remains_unproven(self):
        anchor = self.data["positions_history"][0]
        self.data["positions_history"] = [dict(anchor, posId=f"anchor-{i}",
               uTime=str(int(anchor["uTime"])+i)) for i in range(100)]
        self.assertIn("POSITION_HISTORY_BOUNDARY_UNPROVEN", self.snapshot()["refusal_reasons"])

    def test_audit_cache_preserves_actual_observation_and_never_contains_raw_uid(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        text = path.read_text(encoding="utf-8")
        cache = json.loads(text)
        self.assertNotIn("246810", text)
        self.assertEqual(cache["day_start_ms"], MONDAY)
        self.assertEqual(cache["account_observed_at_ms"], NOW)
        self.assertNotEqual(cache["account_observed_at_ms"], cache["day_start_ms"])
        self.assertEqual(len(cache["balance_observations"]), 2)
        self.assertEqual(set(cache["fill_product_scopes"]), V.REQUIRED_FILLS)

    def test_exact_unsubmitted_real_reservation_can_pass_second_preflight_without_counting_entry(self):
        self.data["routes"] = [reservation()]
        self.write_journal()
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(state["entries_today"], 0)

    def test_attempted_reservation_or_uncertain_exchange_state_is_not_discarded(self):
        self.data["routes"] = [dict(reservation(), submit_attempted=True)]
        self.write_journal()
        self.assertIn("LEDGER_UNRESOLVED_OWNED_ROUTE", self.snapshot()["refusal_reasons"])

    def test_reservation_requires_actual_empty_account_not_only_journal_claim(self):
        self.data["routes"] = [reservation()]
        self.write_journal()
        self.client.algos["chase"] = [{"instId": V.D.INST, "algoId": "unowned"}]
        state = self.snapshot()
        self.assertIn("UNKNOWN_ALGO_ORDER", state["refusal_reasons"])
        self.assertIn("LEDGER_UNRESOLVED_OWNED_ROUTE", state["refusal_reasons"])

    def test_journal_changed_while_gets_in_flight_has_no_complete_owned_route_proof(self):
        changed = False
        lock = threading.Lock()

        def concurrent_change(params):
            nonlocal changed
            with lock:
                if not changed:
                    changed = True
                    route = dict(reservation(), status="REJECTED")
                    with closing(sqlite3.connect(self.journal)) as db, db:
                        db.execute("INSERT INTO routes VALUES(?,?,?,?,?)", ("DEMO-AUTO-SOL-V1",
                           route["signal_id"], route["status"], route["client_order_id"], json.dumps(route)))
            return [copy.deepcopy(self.data["positions_history"][0])] if params["instType"] == "SWAP" else []
        self.client.overrides["/api/v5/account/positions-history"] = concurrent_change
        self.assertIn("ROUTER_CHANGED_DURING_SNAPSHOT", self.snapshot()["refusal_reasons"])

    def test_tail_activity_after_initial_server_is_not_dropped_from_verified_ledger(self):
        first = self.now
        reads = 0

        def server_progress(params):
            nonlocal reads
            reads += 1
            return [{"ts": str(first if reads == 1 else first+1000)}]
        self.client.overrides["/api/v5/public/time"] = server_progress
        self.data["bills"].append({"billId": "9999", "ts": str(first+100),
                   "type": "1", "ccy": "USDT", "instType": "SWAP", "instId": V.D.INST, "balChg": "100"})
        state = self.snapshot()
        self.assertIn("LEDGER_EXTERNAL_CASH_FLOW", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])
        self.assertEqual(state["history_through_ms"], first+1000)

    def test_endpoint_ignoring_product_filter_cannot_certify_all_product_coverage(self):
        anchor = copy.deepcopy(self.data["positions_history"][0])
        self.client.overrides["/api/v5/account/positions-history"] = [anchor]
        self.assertIn("VERIFIED_POSITION_SCOPE_MISMATCH", self.snapshot()["refusal_reasons"])

    def test_non_usdt_legacy_profit_is_not_a_usdt_wallet_loss_reset(self):
        self.data["positions_history"][0].update(ccy="BTC", instId="BTC-USD-SWAP")
        self.assertIn("LEDGER_LOSS_ANCHOR_UNVERIFIED", self.snapshot()["refusal_reasons"])

    def test_tampered_cached_baseline_cannot_raise_the_next_admission_budget(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        cache["day_start_equity"] = "999999999"
        path.write_text(json.dumps(cache), encoding="utf-8")
        state = self.snapshot()
        self.assertIn("BASELINE_RECONSTRUCTION_CHANGED", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])
        self.assertIsNone(state["day_start_equity"])

    def test_tampered_anchor_hint_must_match_actual_authenticated_history(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        cache["anchor"]["posId"] = "forged"
        path.write_text(json.dumps(cache), encoding="utf-8")
        self.assertIn("LEDGER_LOSS_ANCHOR_UNVERIFIED", self.snapshot()["refusal_reasons"])

    def test_real_delayed_gets_keep_actual_post_observation_and_five_second_deadline(self):
        clock = ElapsedClock(NOW)

        class SlowHistorical(HistoricalClient):
            provider = None

            def get(self, path, params):
                stage = self.provider._stage
                time.sleep(5.1 if stage == "history-gap" else 0.01)
                if path.endswith("public/time"):
                    self.now = clock()
                return super().get(path, params)

        client = SlowHistorical(NOW, self.data)
        provider = V.VerifiedDemoRiskProvider(client, self.config, self.root / "verified-risk",
                    authorization=self.authorization, router_journal_path=self.journal, clock=clock)
        client.provider = provider
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False, "observed_at_ms": clock()})
        self.assertTrue(state["ledger_verified"], state["refusal_reasons"])
        self.assertIn("ACCOUNT_STALE", state["refusal_reasons"])
        after = [row for row in state["request_timings"] if row["stage"] == "account-after"]
        self.assertLessEqual(state["observed_at_ms"], min(row["started_at_ms"] for row in after))
        self.assertEqual(state["observed_at_ms"], state["account_get_batches"][-1]["queued_at_ms"])
        self.assertGreater(state["collection_completed_ms"] - state["observed_at_ms"], 5000)
        self.assertFalse(state["demo_verified"])

    def test_mode_two_explicit_not_applicable_fields_keep_raw_evidence_and_allow_proven_no_borrowing(self):
        for field in V.FUTURES_NA_FIELDS:
            self.client.balance["details"][0][field] = ""
        original = copy.deepcopy(self.client.balance)
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(self.client.balance, original)
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        for observation in cache["account_mode_applicability"]:
            self.assertEqual({row["field"] for row in observation["not_applicable_fields"]},
                             set(V.FUTURES_NA_FIELDS))
            self.assertTrue(all(row["raw_value"] == "" for row in observation["not_applicable_fields"]))
        self.assertEqual(cache["evidence"]["histories"], self.data["positions_history"])

    def test_mode_three_empty_liability_fields_are_not_given_futures_applicability(self):
        self.client.config["acctLv"] = "3"
        for field in V.FUTURES_NA_FIELDS:
            self.client.balance["details"][0][field] = ""
        state = self.snapshot()
        self.assertFalse(state["demo_verified"])
        self.assertIn("ACCOUNT_MODE_UNSUPPORTED", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])

    def test_missing_na_field_is_unknown_rather_than_implicitly_zero(self):
        self.client.balance["details"][0].pop("borrowFroz")
        self.assertIn("FUTURES_APPLICABILITY_FIELD_MISSING", self.snapshot()["refusal_reasons"])

    def test_nonzero_or_malformed_na_field_remains_an_explicit_refusal(self):
        for value in ("1", "-1", "NaN", "N/A", None):
            with self.subTest(value=value):
                self.client.balance["details"][0]["liab"] = value
                self.assertFalse(self.snapshot()["demo_verified"])

    def test_raw_na_zero_change_between_snapshots_is_not_hidden_by_compatibility_mapping(self):
        reads = 0

        def values(params):
            nonlocal reads
            reads += 1
            balance = copy.deepcopy(self.client.balance)
            if reads == 1:
                for field in V.FUTURES_NA_FIELDS:
                    balance["details"][0][field] = ""
            return [balance]
        self.client.overrides["/api/v5/account/balance"] = values
        self.assertIn("ACCOUNT_CHANGED_DURING_SNAPSHOT", self.snapshot()["refusal_reasons"])

    def test_unexplained_cash_change_cannot_silently_rebase_an_already_proved_period(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        self.client.balance["details"][0]["eq"] = "2510"
        self.client.balance["totalEq"] = "2510"
        self.assertIn("BASELINE_RECONSTRUCTION_CHANGED", self.snapshot()["refusal_reasons"])

    def test_realized_account_change_matching_new_owned_net_keeps_baseline_constant(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        self.add(gross="3", funding="-0.1")
        self.client.balance["details"][0]["eq"] = "2502.7"
        self.client.balance["totalEq"] = "2502.7"
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(state["day_start_equity"], "2500.0")

    def test_a_fill_returned_under_the_wrong_product_query_cannot_certify_scope(self):
        self.add()
        row = copy.deepcopy(self.data["fills"][0])

        def ignore_product(params):
            return [row]
        self.client.overrides["/api/v5/trade/fills-history"] = ignore_product
        state = self.snapshot()
        self.assertIn("VERIFIED_FILL_SCOPE_MISMATCH", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])

    def test_cached_period_labels_cannot_be_changed_to_skip_prior_baseline_comparison(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        cache.update(day_start_ms=MONDAY-86_400_000, week_start_ms=MONDAY-7*86_400_000)
        path.write_text(json.dumps(cache), encoding="utf-8")
        self.client.balance["details"][0]["eq"] = "2510"
        self.client.balance["totalEq"] = "2510"
        self.assertIn("VERIFIED_STATE_INVALID", self.snapshot()["refusal_reasons"])

    def test_relabeling_cache_complete_time_without_actual_get_timing_is_invalid(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        cache["complete_through_ms"] = MONDAY-7*86_400_000+1000
        cache["day_start_ms"], cache["week_start_ms"] = V.D._boundaries(cache["complete_through_ms"])
        path.write_text(json.dumps(cache), encoding="utf-8")
        self.assertIn("VERIFIED_STATE_INVALID", self.snapshot()["refusal_reasons"])

    def test_verified_scope_adds_events_without_changing_frozen_parent_types(self):
        state = self.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        scopes = {params["instType"] for path, params in self.client.calls if path.endswith("fills-history")}
        self.assertEqual(scopes, V.REQUIRED_FILLS)
        self.assertIn("EVENTS", scopes)
        self.assertNotIn("EVENTS", V.D.FILL_TYPES)
        self.assertEqual(state["risk_adapter"], "verified-demo-ledger-v2")
        cache = json.loads((self.root / "verified-risk" / ACCOUNT / "verified.json").read_text(encoding="utf-8"))
        self.assertEqual(cache["schema"], 2)
        self.assertEqual(set(cache["fill_product_scopes"]), V.REQUIRED_FILLS)

    def test_events_fill_is_unknown_exposure_rather_than_ignored_activity(self):
        self.data["fills"].append({"billId": "7777", "tradeId": "999", "ordId": "88",
          "instType": "EVENTS", "instId": "FOREIGN-USDT-EVENTS", "fillTime": str(MONDAY+1000)})
        self.assertIn("LEDGER_FILL_SCOPE", self.snapshot()["refusal_reasons"])

    def test_unsupported_events_endpoint_never_means_empty_events_history(self):
        def unavailable(params):
            if params["instType"] == "EVENTS":
                raise V.DemoTransportError("DEMO_API_ERROR", 50014)
            return []
        self.client.overrides["/api/v5/trade/fills-history"] = unavailable
        state = self.snapshot()
        self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])
        self.assertFalse(state["history_complete"])
        self.assertFalse(state["demo_verified"])
        self.assertEqual(state["request_faults"][0]["numeric"], 50014)

    def test_known_client_rate_errors_have_fixed_faults_and_are_not_retried(self):
        for numeric, code in ((429, "DEMO_HTTP_ERROR"), (50011, "DEMO_API_ERROR")):
            with self.subTest(numeric=numeric):
                self.client.calls.clear()
                def failed(params):
                    raise V.DemoTransportError(code, numeric)
                self.client.overrides["/api/v5/public/time"] = failed
                state = self.snapshot()
                self.assertEqual(state["request_faults"], [{"path": "/api/v5/public/time",
                     "code": code, "numeric": numeric, "source_fault": "DEMO_SOURCE_RATE_LIMITED"}])
                self.assertEqual(len(self.client.calls), 1)
                self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])

    def test_generic_program_errors_remain_account_read_failed_without_raw_exception(self):
        def failed(params):
            raise ValueError("secret-url-and-body-marker")
        self.client.overrides["/api/v5/public/time"] = failed
        state = self.snapshot()
        self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])
        self.assertEqual(state["request_faults"], [])
        self.assertNotIn("secret-url-and-body-marker", json.dumps(state))

    def test_network_categories_are_diagnostic_only_and_preserve_refusal_and_call_count(self):
        for category in sorted(V.NETWORK_FAILURE_CATEGORIES):
            with self.subTest(category=category):
                self.client.calls.clear()
                error = V.DemoTransportError("DEMO_NETWORK_ERROR")
                error.network_category = category
                def failed(params):
                    raise error
                self.client.overrides["/api/v5/public/time"] = failed
                state = self.snapshot()
                self.assertFalse(state["demo_verified"])
                self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])
                self.assertEqual(state["request_faults"], [{"path": "/api/v5/public/time",
                    "code": "DEMO_NETWORK_ERROR", "numeric": None,
                    "source_fault": "DEMO_SOURCE_REQUEST_FAILED", "network_category": category}])
                self.assertEqual(len(self.client.calls), 1)
                self.assertEqual(str(error), "DEMO_NETWORK_ERROR")

    def test_network_category_never_echoes_untrusted_values_or_nonnetwork_errors(self):
        class Dangerous:
            def __str__(self):
                raise AssertionError("DIAGNOSTIC_MUST_NOT_STRINGIFY")
            def __repr__(self):
                raise AssertionError("DIAGNOSTIC_MUST_NOT_REPR")
        for category in ("PRIVATE_CATEGORY_SENTINEL", Dangerous(), ["DNS"], 7):
            with self.subTest(category_type=type(category).__name__):
                error = V.DemoTransportError("DEMO_NETWORK_ERROR")
                error.network_category = category
                self.client.overrides["/api/v5/public/time"] = lambda params: (_ for _ in ()).throw(error)
                state = self.snapshot()
                self.assertEqual(state["request_faults"][0]["network_category"], "UNKNOWN")
                self.assertNotIn("PRIVATE_CATEGORY_SENTINEL", json.dumps(state))
        error = V.DemoTransportError("DEMO_API_ERROR", 50014)
        error.network_category = "DNS"
        self.client.overrides["/api/v5/public/time"] = lambda params: (_ for _ in ()).throw(error)
        state = self.snapshot()
        self.assertNotIn("network_category", state["request_faults"][0])

    def test_unreadable_optional_network_category_does_not_replace_transport_error(self):
        class Unreadable(V.DemoTransportError):
            @property
            def network_category(self):
                raise ValueError("PRIVATE_PROPERTY_SENTINEL")
        error = Unreadable("DEMO_NETWORK_ERROR")
        def failed(params):
            raise error
        self.client.overrides["/api/v5/public/time"] = failed
        state = self.snapshot()
        self.assertFalse(state["demo_verified"])
        self.assertEqual(state["request_faults"], [{"path": "/api/v5/public/time",
            "code": "DEMO_NETWORK_ERROR", "numeric": None,
            "source_fault": "DEMO_SOURCE_REQUEST_FAILED"}])
        self.assertNotIn("PRIVATE_PROPERTY_SENTINEL", json.dumps(state))

    def test_fault_code_is_a_literal_whitelist_instead_of_echoing_unknown_uppercase(self):
        def failed(params):
            raise V.DemoTransportError("SECRET_TOKEN_MARKER", 999)
        self.client.overrides["/api/v5/public/time"] = failed
        state = self.snapshot()
        self.assertEqual(state["request_faults"][0]["code"], "DEMO_SOURCE_REQUEST_FAILED")
        self.assertNotIn("SECRET_TOKEN_MARKER", json.dumps(state))

    def test_sixteen_worker_batch_preserves_phase_barriers_and_conservative_observation(self):
        clock = ElapsedClock(NOW)
        barrier = threading.Barrier(14)
        lock = threading.Lock()
        active = peak = 0

        class ConcurrentHistory(HistoricalClient):
            provider = None

            def get(inner, path, params):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    if inner.provider._stage == "account-before":
                        barrier.wait(timeout=3)
                    time.sleep(0.01)
                    if path.endswith("public/time"):
                        inner.now = clock()
                    return super().get(path, params)
                finally:
                    with lock:
                        active -= 1

        client = ConcurrentHistory(NOW, self.data)
        provider = V.VerifiedDemoRiskProvider(client, self.config, self.root / "verified-risk",
              authorization=self.authorization, router_journal_path=self.journal, clock=clock,
              rate_clock=NonContendingFixtureQuotaClock())
        client.provider = provider
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False, "observed_at_ms": clock()})
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(peak, 14)
        self.assertEqual(active, 0)
        groups = {name: [row for row in state["request_timings"] if row["stage"] == name]
                  for name in ("account-before", "history", "account-after", "history-gap")}
        for first, second in (("account-before", "history"), ("history", "account-after"),
                               ("account-after", "history-gap")):
            self.assertLessEqual(max(row["finished_at_ms"] for row in groups[first]),
                                 min(row["started_at_ms"] for row in groups[second]))
        self.assertEqual(state["observed_at_ms"], state["account_get_batches"][-1]["queued_at_ms"])
        self.assertLessEqual(state["observed_at_ms"],
                             min(row["started_at_ms"] for row in groups["account-after"]))

    def test_monotonic_endpoint_budget_is_shared_across_products_and_stage_boundaries(self):
        clock = ElapsedClock(NOW)
        provider = V.VerifiedDemoRiskProvider(self.client, self.config, self.root / "verified-risk",
              authorization=self.authorization, router_journal_path=self.journal, clock=clock)
        params = {"instType": "SWAP", "begin": str(MONDAY), "end": str(NOW), "limit": "100"}
        provider._stage = "history"
        provider._parallel({str(i): lambda: provider._get("/api/v5/trade/fills-history", params)
                            for i in range(6)})
        provider._stage = "history-gap"
        provider._parallel({str(i): lambda: provider._get("/api/v5/trade/fills-history",
                         dict(params, instType="EVENTS")) for i in range(6)})
        starts = sorted(row["started_at_ms"] for row in provider._request_timings)
        self.assertEqual(len(starts), 12)
        self.assertGreaterEqual(starts[10]-starts[0], 2000)
        for start in starts:
            self.assertLessEqual(sum(start <= item < start+2000 for item in starts), 10)
        self.assertEqual({row["stage"] for row in provider._request_timings}, {"history", "history-gap"})

    def test_five_product_cache_cannot_be_silently_migrated_to_six_product_proof(self):
        self.assertTrue(self.snapshot()["demo_verified"])
        path = self.root / "verified-risk" / ACCOUNT / "verified.json"
        cache = json.loads(path.read_text(encoding="utf-8"))
        cache["schema"] = 1
        cache["fill_product_scopes"].remove("EVENTS")
        path.write_text(json.dumps(cache), encoding="utf-8")
        self.assertIn("VERIFIED_STATE_INVALID", self.snapshot()["refusal_reasons"])


if __name__ == "__main__":
    unittest.main()
