"""Offline acceptance of DEMO evidence; no credentials, network or account IO."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from src import demo_risk as R

MONDAY = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
CONFIG = {"account": {"equity_usdt": 100, "margin_mode": "cross", "position_mode": "net",
                      "leverage_setting": 8},
          "risk": {"daily_loss_limit_pct": 5, "weekly_loss_limit_pct": 10,
                   "max_entries_per_day": 3, "consecutive_loss_pause_count": 3,
                   "consecutive_loss_pause_hours": 24},
          "symbols": [{"instId": R.INST, "ctVal": 1, "lotSz": 0.01,
                       "minSz": 0.01, "tickSz": 0.01}]}


def bill(identifier, now, *, kind="8", amount="-0.1", ccy="USDT", inst=R.INST):
    return {"billId": str(identifier), "ts": str(now), "type": kind,
            "balChg": amount, "ccy": ccy, "instId": inst}


def fill(identifier, now, *, order="order-1", pnl="-1", fee="-0.1", trade=None):
    return {"billId": str(identifier), "fillTime": str(now), "tradeId": trade or str(identifier),
            "ordId": order, "instId": R.INST, "instType": "SWAP",
            "feeCcy": "USDT", "fillPnl": pnl, "fee": fee}


class FakeClient:
    def __init__(self, now):
        self.calls = []
        self.now = now
        self.config = {"uid": "246810", "posMode": "net_mode", "acctLv": "2"}
        self.balance = {"totalEq": "2500", "uTime": str(now), "details":
                        [{"ccy": "USDT", "eq": "2500", "liab": "0"}]}
        self.positions = []
        self.pending = []
        self.algos = {}
        self.lever = {"instId": R.INST, "mgnMode": "cross", "posSide": "net", "lever": "8"}
        self.spec = dict(CONFIG["symbols"][0], state="live", ctType="linear",
                         settleCcy="USDT", ctValCcy="SOL")
        self.bills = []
        self.fills = {kind: [] for kind in R.FILL_TYPES}
        self.lifecycle = {kind: [] for kind in R.POSITION_TYPES}
        self.hook = None

    def get(self, path, params):
        self.calls.append((path, dict(params)))
        if self.hook:
            value = self.hook(path, params)
            if value is not NotImplemented:
                return copy.deepcopy(value)
        if path == "/api/v5/public/time":
            value = [{"ts": str(self.now)}]
        elif path.endswith("account/config"):
            value = [self.config]
        elif path.endswith("account/balance"):
            value = [self.balance]
        elif path.endswith("account/positions"):
            value = self.positions
        elif path.endswith("orders-pending"):
            value = self.pending
        elif path.endswith("orders-algo-pending"):
            value = self.algos.get(params["ordType"], [])
        elif path.endswith("leverage-info"):
            value = [self.lever]
        elif path.endswith("public/instruments"):
            value = [self.spec]
        elif path.endswith("bills-archive"):
            value = self.bills
        elif path.endswith("fills-history"):
            value = self.fills[params["instType"]]
        elif path.endswith("positions-history"):
            value = self.lifecycle[params["instType"]]
        else:
            raise AssertionError("Unexpected read endpoint")
        return copy.deepcopy(value)

    def post(self, *args, **kwargs):
        raise AssertionError("Any account write is forbidden in risk collection")


class ElapsedClock:
    """Real elapsed wall time, mapped to a deterministic UTC fixture date."""
    def __init__(self, base):
        self.base = base
        self.started = time.monotonic_ns()

    def __call__(self):
        return self.base + (time.monotonic_ns() - self.started) // 1_000_000


class DelayedClient(FakeClient):
    def __init__(self, clock, delay=0.02):
        super().__init__(clock())
        self.clock = clock
        self.delay = delay
        self.stage = lambda: ""
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def get(self, path, params):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            delay = self.delay(path, params, self.stage()) if callable(self.delay) else self.delay
            time.sleep(delay)
            if path.endswith("public/time"):
                self.now = self.clock()
            return super().get(path, params)
        finally:
            with self.lock:
                self.active -= 1


class DemoRiskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.now = MONDAY + 30_000
        self.client = FakeClient(self.now)
        self.cfg = copy.deepcopy(CONFIG)
        self.provider = R.DemoRiskProvider(self.client, self.cfg, self.tmp.name,
                                            clock=lambda: self.now)

    def snapshot(self, **overrides):
        market = {"data_fresh": True, "macro_blackout": False, "observed_at_ms": self.now}
        market.update(overrides)
        return self.provider.snapshot(market)

    def state_path(self, uid=None):
        uid = uid or self.client.config["uid"]
        name = hashlib.sha256(("DEMO\0" + uid).encode()).hexdigest()
        return Path(self.tmp.name) / name / "risk.json"

    def move_time(self, stamp):
        self.now = stamp
        self.client.now = stamp
        self.client.balance["uTime"] = str(stamp)

    def assert_account_evidence_only(self, state):
        self.assertTrue(state["account_snapshot_consistent"], state["refusal_reasons"])
        self.assertTrue(state["history_complete"])
        self.assertTrue(state["activity_free_coverage"])
        self.assertFalse(state["demo_verified"])
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["pause_evidence_verified"])
        self.assertIn("LEDGER_ADAPTER_UNVERIFIED", state["refusal_reasons"])

    def test_boundary_empty_account_can_establish_both_real_baselines(self):
        state = self.snapshot()
        self.assert_account_evidence_only(state)
        self.assertFalse(state["live_verified"])
        self.assertEqual(state["equity_estimate"], 2500)
        self.assertEqual(state["day_start_equity"], 2500)
        self.assertEqual(state["week_start_equity"], 2500)
        self.assertNotEqual(state["equity_estimate"], self.cfg["account"]["equity_usdt"])
        self.assertTrue(state["baseline_trusted"])

    def test_usdt_wallet_excludes_idle_other_currency_cash_in_account_mode_two(self):
        self.client.balance["totalEq"] = "999999"
        self.client.balance["details"].append({"ccy": "BTC", "eq": "3", "liab": "0"})
        state = self.snapshot()
        self.assert_account_evidence_only(state)
        self.assertEqual(state["equity_estimate"], 2500)
        self.assertEqual(state["equity_scope"], "DEMO-USDT-FUTURES-WALLET")

    def test_multi_currency_margin_mode_is_rejected(self):
        self.client.config["acctLv"] = "3"
        self.assertIn("ACCOUNT_MODE_UNSUPPORTED", self.snapshot()["refusal_reasons"])

    def test_liability_in_any_currency_rejected(self):
        self.client.balance["details"].append({"ccy": "BTC", "eq": "3", "liab": "0.01"})
        self.assertIn("ACCOUNT_LIABILITY_UNSUPPORTED", self.snapshot()["refusal_reasons"])

    def test_missing_liability_evidence_cannot_mean_zero(self):
        self.client.balance["details"][0].pop("liab")
        self.assertIn("ACCOUNT_DATA_INVALID", self.snapshot()["refusal_reasons"])

    def test_current_equity_at_arbitrary_time_never_becomes_start_of_day(self):
        self.move_time(MONDAY + 3_600_000)
        state = self.snapshot()
        self.assertFalse(state["demo_verified"])
        self.assertIsNone(state["day_start_equity"])
        self.assertIsNone(state["week_start_equity"])
        self.assertIn("BASELINE_MISSING", state["refusal_reasons"])

    def test_boundary_window_is_strict_and_does_not_expand_to_fifteen_minutes(self):
        self.move_time(MONDAY + 120_001)
        self.assertIn("BASELINE_MISSING", self.snapshot()["refusal_reasons"])

    def test_daily_boundary_cannot_fabricate_prior_week_equity(self):
        self.move_time(MONDAY + 86_400_000 + 30_000)
        state = self.snapshot()
        self.assertEqual(state["day_start_equity"], 2500)
        self.assertIsNone(state["week_start_equity"])
        self.assertIn("WEEK_BASELINE_MISSING", state["refusal_reasons"])

    def test_all_positions_query_is_not_filtered_to_sol_or_swaps(self):
        self.client.positions = [{"instId": "BTC-USD-261225", "pos": "1", "instType": "FUTURES"}]
        self.assertIn("UNSUPPORTED_ACCOUNT_POSITION", self.snapshot()["refusal_reasons"])
        queries = [p for path, p in self.client.calls if path.endswith("account/positions")]
        self.assertEqual(queries, [{}, {}])

    def test_zero_position_rows_do_not_count_as_exposure(self):
        self.client.positions = [{"instId": "ETH-USDT-SWAP", "pos": "0"}]
        self.assert_account_evidence_only(self.snapshot())

    def test_pending_other_instrument_refuses_and_all_algorithms_checked(self):
        self.client.pending = [{"instId": "BTC-USDT", "ordId": "foreign"}]
        self.client.algos["twap"] = [{"instId": "BTC-USDT-SWAP", "algoId": "foreign"}]
        state = self.snapshot()
        self.assertIn("UNKNOWN_OPEN_ORDER", state["refusal_reasons"])
        self.assertIn("UNKNOWN_ALGO_ORDER", state["refusal_reasons"])
        queried = {p["ordType"] for path, p in self.client.calls if path.endswith("orders-algo-pending")}
        self.assertEqual(queried, set(R.ALGO_TYPES))

    def test_mode_margin_leverage_spec_mismatch_cannot_capture_baselines(self):
        self.client.config["posMode"] = "long_short_mode"
        self.client.lever["lever"] = "50"
        self.client.spec["tickSz"] = "0.1"
        state = self.snapshot()
        for code in ("POSITION_MODE_MISMATCH", "LEVERAGE_MARGIN_MISMATCH", "INSTRUMENT_SPEC_MISMATCH"):
            self.assertIn(code, state["refusal_reasons"])
        self.assertIsNone(state["day_start_equity"])

    def test_future_balance_time_rejected(self):
        self.client.balance["uTime"] = str(self.now + 2001)
        self.assertIn("BALANCE_TIME_FUTURE", self.snapshot()["refusal_reasons"])

    def test_nonfinite_equity_and_zero_equity_rejected(self):
        for value in ("NaN", "Infinity", "0", "-1", "1e10000"):
            with self.subTest(value=value):
                self.client.balance["details"][0]["eq"] = value
                self.assertFalse(self.snapshot()["demo_verified"])

    def test_complete_none_history_is_not_treated_as_empty(self):
        self.client.hook = lambda path, p: None if path.endswith("bills-archive") else NotImplemented
        self.assertIn("ACCOUNT_RESPONSE_INVALID", self.snapshot()["refusal_reasons"])

    def test_fills_have_required_instrument_type_and_fixed_pause_week_window(self):
        state = self.snapshot()
        self.assert_account_evidence_only(state)
        queries = [p for path, p in self.client.calls if path.endswith("fills-history")]
        self.assertEqual({p["instType"] for p in queries}, set(R.FILL_TYPES))
        start = min(MONDAY, self.now - R.PAUSE_LOOKBACK_MS)
        self.assertTrue(all(p["begin"] == str(start) and p["end"] == str(self.now) for p in queries))

    def test_conflicting_history_duplicate_rejected(self):
        self.client.bills = [bill(4, self.now), bill(4, self.now, amount="-9")]
        self.assertIn("HISTORY_DUPLICATE_CONFLICT", self.snapshot()["refusal_reasons"])

    def test_out_of_window_history_rejected(self):
        self.client.bills = [bill(4, self.now - R.PAUSE_LOOKBACK_MS - 1)]
        self.assertIn("HISTORY_OUTSIDE_WINDOW", self.snapshot()["refusal_reasons"])

    def test_history_full_page_limit_is_not_complete(self):
        def pages(path, params):
            if not path.endswith("bills-archive"):
                return NotImplemented
            top = int(params.get("after", "10000"))
            return [bill(top - i - 1, self.now) for i in range(R.PAGE_LIMIT)]
        self.client.hook = pages
        with mock.patch.object(R, "MAX_PAGES", 2):
            self.assertIn("HISTORY_INCOMPLETE", self.snapshot()["refusal_reasons"])

    def test_trade_fees_and_funding_are_included_without_trade_bill_double_count(self):
        self.client.fills["SWAP"] = [fill(4, self.now, pnl="-5", fee="-0.2"),
                                     fill(3, self.now, pnl="2", fee="-0.1", order="order-2")]
        self.client.bills = [bill(7, self.now, amount="-0.3"),
                             bill(6, self.now, kind="2", amount="-5.2")]
        state = self.snapshot()
        self.assertAlmostEqual(state["realized_pnl_today"], -3.6)
        self.assertAlmostEqual(state["realized_pnl_week"], -3.6)
        self.assertIn("LEDGER_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])
        self.assertFalse(state["ledger_verified"])

    def test_entry_diagnostic_deduplicates_partial_fills_by_order(self):
        self.client.fills["SWAP"] = [fill(4, self.now), fill(3, self.now)]
        state = self.snapshot()
        self.assertEqual(state["entries_today"], 1)
        self.assertFalse(state["demo_verified"])

    def test_non_usdt_funding_never_added_to_usdt_pnl(self):
        self.client.bills = [bill(7, self.now, amount="100", ccy="BTC", inst="BTC-USD-SWAP")]
        state = self.snapshot()
        self.assertIn("UNSUPPORTED_ACCOUNT_ACTIVITY", state["refusal_reasons"])
        self.assertEqual(state["realized_pnl_today"], 0)

    def test_deposit_latches_across_restart_and_cannot_be_hidden_by_next_empty_window(self):
        self.client.bills = [bill(7, self.now, kind="1", amount="100")]
        self.assertIn("ACCOUNT_FUNDING_FLOW_DETECTED", self.snapshot()["refusal_reasons"])
        self.client.bills = []
        new = R.DemoRiskProvider(self.client, self.cfg, self.tmp.name, clock=lambda: self.now)
        state = new.snapshot({"data_fresh": True, "macro_blackout": False, "observed_at_ms": self.now})
        self.assertIn("ACCOUNT_FUNDING_FLOW_DETECTED", state["refusal_reasons"])

    def test_unknown_zero_amount_bill_still_refuses(self):
        self.client.bills = [bill(7, self.now, kind="999", amount="0")]
        self.assertIn("ACCOUNT_FUNDING_FLOW_DETECTED", self.snapshot()["refusal_reasons"])

    def test_trade_at_end_of_week_cannot_disappear_and_unlock_next_week(self):
        self.client.fills["SWAP"] = [fill(4, self.now)]
        self.assertIn("LEDGER_UNVERIFIED", self.snapshot()["refusal_reasons"])
        self.client.fills["SWAP"] = []
        self.move_time(MONDAY + 7 * 86_400_000 + 30_000)
        state = self.snapshot()
        self.assertIn("LEDGER_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["demo_verified"])

    def test_market_blackout_does_not_prevent_account_only_boundary_evidence(self):
        initial = self.snapshot(macro_blackout=True)
        self.assertFalse(initial["demo_verified"])
        self.assertEqual(initial["week_start_equity"], 2500)
        self.move_time(MONDAY + 600_000)
        self.assert_account_evidence_only(self.snapshot())

    def test_account_namespaces_cannot_reuse_another_uid_baseline(self):
        self.assert_account_evidence_only(self.snapshot())
        first = self.state_path()
        self.client.config["uid"] = "97531"
        self.move_time(MONDAY + 600_000)
        state = self.snapshot()
        self.assertIn("BASELINE_MISSING", state["refusal_reasons"])
        self.assertNotEqual(first, self.state_path())
        self.assertTrue(first.exists())

    def test_replaced_state_account_or_environment_is_rejected(self):
        self.snapshot()
        path = self.state_path()
        data = json.loads(path.read_text(encoding="utf-8"))
        data["environment"] = "LIVE"
        path.write_text(json.dumps(data), encoding="utf-8")
        self.assertIn("LOCAL_STATE_INVALID", self.snapshot()["refusal_reasons"])

    def test_convenient_midday_baseline_in_local_file_is_not_accepted(self):
        self.snapshot()
        path = self.state_path()
        data = json.loads(path.read_text(encoding="utf-8"))
        data["baselines"]["day"]["observed_at_ms"] = MONDAY + 999999
        path.write_text(json.dumps(data), encoding="utf-8")
        self.assertIn("BASELINE_INVALID", self.snapshot()["refusal_reasons"])

    def test_unexplained_equity_change_cannot_pass_empty_history(self):
        self.assert_account_evidence_only(self.snapshot())
        self.client.balance["details"][0]["eq"] = "2400"
        self.client.balance["totalEq"] = "2400"
        self.assertIn("UNEXPLAINED_EQUITY_CHANGE", self.snapshot()["refusal_reasons"])

    def test_changed_persisted_bill_refuses(self):
        self.client.bills = [bill(7, self.now)]
        self.snapshot()
        self.client.bills[0]["balChg"] = "-0.2"
        self.assertIn("PERSISTED_BILL_CONFLICT", self.snapshot()["refusal_reasons"])

    def test_missing_stale_future_macro_and_blackout_are_fail_closed(self):
        self.assertIn("MARKET_EVIDENCE_MISSING", self.provider.snapshot()["refusal_reasons"])
        self.assertIn("MARKET_STALE", self.snapshot(observed_at_ms=self.now-60001)["refusal_reasons"])
        self.assertIn("MARKET_STALE", self.snapshot(observed_at_ms=self.now+1)["refusal_reasons"])
        self.assertIn("MACRO_BLACKOUT", self.snapshot(macro_blackout=True)["refusal_reasons"])

    def test_account_errors_are_sanitized(self):
        self.client.hook = lambda *a: (_ for _ in ()).throw(RuntimeError("secret-key-marker"))
        state = self.snapshot()
        self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])
        self.assertNotIn("secret-key-marker", json.dumps(state))

    def test_no_credential_or_uid_is_written_to_local_state(self):
        self.snapshot()
        text = self.state_path().read_text(encoding="utf-8")
        self.assertNotIn(self.client.config["uid"], text)
        self.assertNotIn("API_KEY", text)
        state = self.snapshot()
        expected = hashlib.sha256(("DEMO\0" + self.client.config["uid"]).encode()).hexdigest()
        self.assertEqual(state["account_hash"], expected)
        self.assertNotIn(self.client.config["uid"], json.dumps(state))

    def test_smart_iceberg_and_chase_are_not_omitted_from_account_order_scope(self):
        for kind in ("smart_iceberg", "chase"):
            with self.subTest(kind=kind):
                self.client.algos = {kind: [{"instId": "BTC-USDT-SWAP", "algoId": kind}]}
                state = self.snapshot()
                self.assertIn("UNKNOWN_ALGO_ORDER", state["refusal_reasons"])
                self.assertFalse(state["demo_verified"])
        self.assertEqual(len(R.ALGO_TYPES), 8)

    def test_unsupported_algo_query_is_failure_instead_of_empty_order_evidence(self):
        def unsupported(path, params):
            if path.endswith("orders-algo-pending") and params["ordType"] == "chase":
                raise RuntimeError("server-rejected-unknown-kind-sensitive-body")
            return NotImplemented
        self.client.hook = unsupported
        state = self.snapshot()
        self.assertIn("ACCOUNT_READ_FAILED", state["refusal_reasons"])
        self.assertFalse(state["account_snapshot_consistent"])
        self.assertFalse(state["demo_verified"])
        self.assertNotIn("sensitive-body", json.dumps(state))

    def test_balance_changed_after_history_read_does_not_build_current_baseline(self):
        count = 0

        def changing_balance(path, params):
            nonlocal count
            if path.endswith("account/balance"):
                count += 1
                result = copy.deepcopy(self.client.balance)
                if count == 2:
                    result["details"][0]["eq"] = "9999"
                    result["totalEq"] = "9999"
                return [result]
            return NotImplemented
        self.client.hook = changing_balance
        state = self.snapshot()
        self.assertIn("ACCOUNT_CHANGED_DURING_SNAPSHOT", state["refusal_reasons"])
        self.assertIsNone(state["week_start_equity"])
        self.assertFalse(state["history_complete"])

    def test_new_position_after_first_snapshot_cannot_be_hidden_by_old_empty_history(self):
        count = 0

        def opening_position(path, params):
            nonlocal count
            if path.endswith("account/positions"):
                count += 1
                return [] if count == 1 else [{"instId": "ETH-USDT-SWAP", "pos": "1"}]
            return NotImplemented
        self.client.hook = opening_position
        state = self.snapshot()
        self.assertIn("ACCOUNT_CHANGED_DURING_SNAPSHOT", state["refusal_reasons"])
        self.assertFalse(state["account_snapshot_consistent"])

    def test_post_snapshot_account_mode_change_rejected(self):
        count = 0

        def mode_change(path, params):
            nonlocal count
            if path.endswith("account/config"):
                count += 1
                result = dict(self.client.config)
                if count == 2:
                    result["acctLv"] = "3"
                return [result]
            return NotImplemented
        self.client.hook = mode_change
        self.assertIn("ACCOUNT_CHANGED_DURING_SNAPSHOT", self.snapshot()["refusal_reasons"])

    def test_post_history_time_gap_is_queried_and_activity_blocks_admission(self):
        first = self.now
        count = 0

        def during_collection(path, params):
            nonlocal count
            if path.endswith("public/time"):
                count += 1
                return [{"ts": str(first if count == 1 else first + 500)}]
            if path.endswith("bills-archive") and int(params["begin"]) > first:
                return [bill(42, first + 200, kind="1", amount="100")]
            return NotImplemented
        self.client.hook = during_collection
        state = self.snapshot()
        self.assertIn("ACCOUNT_FUNDING_FLOW_DETECTED", state["refusal_reasons"])
        self.assertEqual(state["observed_at_ms"], first)
        self.assertEqual(state["history_through_ms"], first + 500)
        gap_queries = [p for path, p in self.client.calls if path.endswith("fills-history")
                       and p["begin"] == str(first + 1)]
        self.assertEqual(len(gap_queries), len(R.FILL_TYPES))
        self.assertFalse(state["demo_verified"])

    def test_observed_time_is_oldest_evidence_not_completion_time(self):
        first = self.now
        count = 0

        def progress(path, params):
            nonlocal count
            if path.endswith("public/time"):
                count += 1
                if count == 2:
                    self.now = first + 1000
                return [{"ts": str(self.now)}]
            return NotImplemented
        self.client.hook = progress
        state = self.snapshot()
        self.assertEqual(state["observed_at_ms"], first)
        self.assertEqual(state["snapshot_window_end_ms"], first + 1000)
        self.assertNotEqual(state["observed_at_ms"], self.now)
        self.assert_account_evidence_only(state)

    def test_long_collection_cannot_claim_stable_current_evidence(self):
        first = self.now
        count = 0

        def slow(path, params):
            nonlocal count
            if path.endswith("public/time"):
                count += 1
                if count == 2:
                    self.now = first + R.MAX_SNAPSHOT_WINDOW_MS + 1
                return [{"ts": str(self.now)}]
            return NotImplemented
        self.client.hook = slow
        state = self.snapshot()
        self.assertIn("ACCOUNT_SNAPSHOT_UNSTABLE", state["refusal_reasons"])
        self.assertEqual(state["observed_at_ms"], first)
        self.assertFalse(state["history_complete"])

    def test_sunday_losses_are_not_erased_by_a_new_monday_week(self):
        self.client.fills["SWAP"] = [fill(7-i, MONDAY - (i+1)*1000, order=f"loss-{i}")
                                     for i in range(3)]
        state = self.snapshot()
        self.assertIn("LEDGER_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["pause_evidence_verified"])
        self.assertEqual(state["realized_pnl_week"], 0)
        self.assertEqual(state["entries_today"], 0)
        self.assertFalse(state["demo_verified"])

    def test_lifecycle_close_detects_unknown_loss_even_when_fills_and_bills_are_empty(self):
        self.client.lifecycle["SWAP"] = [{"instId": R.INST, "posId": "reused-id",
                                           "cTime": str(MONDAY - 2*86_400_000),
                                           "uTime": str(MONDAY - 1000), "realizedPnl": "-10"}]
        state = self.snapshot()
        self.assertIn("TRADE_LIFECYCLE_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["activity_free_coverage"])
        self.assertFalse(state["demo_verified"])

    def test_positions_history_required_for_every_derivative_type(self):
        self.snapshot()
        queries = [p for path, p in self.client.calls if path.endswith("positions-history")]
        self.assertEqual({p["instType"] for p in queries}, set(R.POSITION_TYPES))
        self.assertTrue(all(p["after"] == str(self.now + 1) for p in queries))

    def test_missing_lifecycle_page_does_not_mean_no_closed_trades(self):
        self.client.hook = lambda path, params: None if path.endswith("positions-history") else NotImplemented
        state = self.snapshot()
        self.assertIn("ACCOUNT_RESPONSE_INVALID", state["refusal_reasons"])
        self.assertFalse(state["history_complete"])
        self.assertFalse(state["demo_verified"])

    def test_short_empty_window_never_self_certifies_a_known_consecutive_loss_chain(self):
        state = self.snapshot()
        self.assertTrue(state["activity_free_coverage"])
        self.assertTrue(state["baseline_trusted"])
        self.assertIn("LEDGER_ADAPTER_UNVERIFIED", state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])
        self.assertFalse(state["pause_evidence_verified"])
        self.assertFalse(state["demo_verified"])

    def delayed_provider(self, *, base=None, delay=0.02):
        clock = ElapsedClock(MONDAY + 30_000 if base is None else base)
        client = DelayedClient(clock, delay)
        provider = R.DemoRiskProvider(client, self.cfg, self.tmp.name, clock=clock)
        client.stage = lambda: provider._stage
        return clock, client, provider

    def test_delayed_independent_gets_run_in_parallel_but_evidence_phases_do_not_overlap(self):
        clock, client, provider = self.delayed_provider()
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False,
                                   "observed_at_ms": clock()})
        self.assertGreater(client.max_active, 1)
        self.assertLessEqual(client.max_active, 8)
        self.assertEqual(client.active, 0)
        self.assert_account_evidence_only(state)
        timings = state["request_timings"]
        self.assertTrue(all(row["finished_at_ms"] > row["started_at_ms"] for row in timings))
        stages = {name: [row for row in timings if row["stage"] == name]
                  for name in ("account-before", "history", "account-after", "history-gap")}
        for before, after in (("account-before", "history"), ("history", "account-after"),
                              ("account-after", "history-gap")):
            self.assertLessEqual(max(row["finished_at_ms"] for row in stages[before]),
                                 min(row["started_at_ms"] for row in stages[after]))
        self.assertEqual(state["observed_at_ms"],
                         min(row["started_at_ms"] for row in stages["account-after"]))
        self.assertGreater(state["observed_at_ms"], state["collection_started_ms"])
        self.assertLessEqual(state["collection_completed_ms"] - state["observed_at_ms"], 5000)

    def test_delayed_history_pagination_remains_one_sequential_cursor_chain(self):
        clock, client, provider = self.delayed_provider(delay=0.01)
        stamp = clock()

        def pages(path, params):
            if not path.endswith("bills-archive"):
                return NotImplemented
            if not int(params["begin"]) <= stamp <= int(params["end"]) or "after" in params:
                return []
            return [bill(1000-i, stamp) for i in range(100)]
        client.hook = pages
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False,
                                   "observed_at_ms": clock()})
        requests = [row for row in state["request_timings"]
                    if row["stage"] == "history" and row["path"].endswith("bills-archive")]
        self.assertEqual(len(requests), 2)
        self.assertNotIn("after", requests[0]["scope"])
        self.assertEqual(requests[1]["scope"]["after"], "901")
        self.assertGreaterEqual(requests[1]["started_at_ms"], requests[0]["finished_at_ms"])
        self.assertFalse(state["demo_verified"])

    def test_baseline_capture_uses_late_server_not_initial_119_second_clock(self):
        clock, client, provider = self.delayed_provider(base=MONDAY + 119_000, delay=0.27)
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False,
                                   "observed_at_ms": clock()})
        self.assertLess(state["collection_started_ms"], MONDAY + 120_000)
        self.assertGreaterEqual(state["snapshot_window_end_ms"], MONDAY + 121_000)
        self.assertIsNone(state["day_start_equity"])
        self.assertIsNone(state["week_start_equity"])
        self.assertIn("BASELINE_MISSING", state["refusal_reasons"])

    def test_gap_queries_older_than_five_seconds_leave_post_view_stale(self):
        def delay(path, params, stage):
            return 2.6 if stage == "history-gap" else 0.01
        clock, client, provider = self.delayed_provider(delay=delay)
        state = provider.snapshot({"data_fresh": True, "macro_blackout": False,
                                   "observed_at_ms": clock()})
        self.assertGreater(state["collection_completed_ms"] - state["observed_at_ms"], 5000)
        self.assertIn("ACCOUNT_STALE", state["refusal_reasons"])
        self.assertFalse(state["demo_verified"])
        self.assertEqual(client.active, 0)


if __name__ == "__main__":
    unittest.main()
