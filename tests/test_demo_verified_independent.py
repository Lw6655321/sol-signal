"""Independent fail-closed cache and source review; fake HTTP only."""
from __future__ import annotations

import json
import unittest

from tests import test_demo_verified_risk as B


class VerifiedIndependentTests(unittest.TestCase):
    def setUp(self):
        self.case = B.VerifiedRiskTests(methodName="runTest")
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def cache(self):
        initial = self.case.snapshot()
        self.assertTrue(initial["demo_verified"], initial["refusal_reasons"])
        path = self.case.root / "verified-risk" / B.ACCOUNT / "verified.json"
        return path, json.loads(path.read_text(encoding="utf-8"))

    def test_modified_period_metadata_cannot_skip_unexplained_equity_rebase_check(self):
        path, cache = self.cache()
        cache["day_start_ms"] = B.MONDAY-86_400_000
        cache["week_start_ms"] = B.MONDAY-7*86_400_000
        path.write_text(json.dumps(cache), encoding="utf-8")
        self.case.client.balance["details"][0]["eq"] = "9999"
        self.case.client.balance["totalEq"] = "9999"
        state = self.case.snapshot()
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])
        self.assertIsNone(state["day_start_equity"])

    def test_non_demo_cached_environment_is_not_silently_rewritten_as_trusted_demo(self):
        path, cache = self.cache()
        cache["environment"] = "LIVE"
        path.write_text(json.dumps(cache), encoding="utf-8")
        state = self.case.snapshot()
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])

    def test_future_cache_complete_through_time_is_inconsistent_evidence(self):
        path, cache = self.cache()
        cache["complete_through_ms"] = self.case.now+86_400_000
        path.write_text(json.dumps(cache), encoding="utf-8")
        state = self.case.snapshot()
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])

    def test_cached_source_range_cannot_change_without_actual_anchor_match(self):
        path, cache = self.cache()
        cache["coverage_start_ms"] += 1
        path.write_text(json.dumps(cache), encoding="utf-8")
        state = self.case.snapshot()
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])

    def test_real_utc_day_rollover_can_reconstruct_new_day_without_bypassing_same_week_proof(self):
        self.cache()
        self.case.now = B.MONDAY+86_400_000+600_000
        self.case.client.now = self.case.now
        self.case.client.balance["uTime"] = str(self.case.now)
        state = self.case.snapshot()
        self.assertTrue(state["demo_verified"], state["refusal_reasons"])
        self.assertEqual(state["day_start_ms"], B.MONDAY+86_400_000)
        self.assertEqual(state["day_start_equity"], "2500")
        self.assertEqual(state["week_start_equity"], "2500")

    def test_mode_two_missing_actual_equity_is_not_na_compatibility(self):
        for field in B.V.FUTURES_NA_FIELDS:
            self.case.client.balance["details"][0][field] = ""
        self.case.client.balance["details"][0]["eq"] = ""
        state = self.case.snapshot()
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])
        self.assertFalse(state["ledger_verified"])

    def test_even_verified_ledger_does_not_remove_actual_market_or_account_refusals(self):
        state = self.case.snapshot(macro_blackout=True)
        self.assertTrue(state["ledger_verified"], state["refusal_reasons"])
        self.assertFalse(state["demo_verified"], state["refusal_reasons"])
        self.assertIn("MACRO_BLACKOUT", state["refusal_reasons"])
        self.assertNotIn("LEDGER_ADAPTER_UNVERIFIED", state["refusal_reasons"])


if __name__ == "__main__":
    unittest.main()
