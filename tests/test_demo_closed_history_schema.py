"""Exact OKX SWAP settlement schema over the real closed proof, fake HTTP only."""
from copy import deepcopy
from pathlib import Path
import json
import unittest
from unittest.mock import patch

from src import demo_route_transport as T
import test_demo_halted_exit_receipts as fixtures


class ClosedHistorySchemaTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.ClosedHaltedReceipts()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)

    def test_swap_empty_settlement_is_inapplicable_without_rewriting_raw_receipt(self):
        h = self.helper
        h.history["settledPnl"] = ""
        original = deepcopy((h.entry, h.parent, h.child, h.fills, h.history))
        proof = h.verify()
        self.assertTrue(proof["closed_lifecycle_verified"])
        self.assertEqual(proof["position_history"]["settledPnl"], "")
        self.assertEqual((h.entry, h.parent, h.child, h.fills, h.history), original)
        proof["position_history"]["settledPnl"] = "CHANGED_COPY"
        self.assertEqual(h.history["settledPnl"], "")
        self.assertEqual(h.posts, [])

    def test_missing_and_explicit_zero_keep_previous_compatibility(self):
        h = self.helper
        h.history.pop("settledPnl")
        proof = h.verify()
        self.assertTrue(proof["closed_lifecycle_verified"])
        self.assertNotIn("settledPnl", proof["position_history"])
        h.history["settledPnl"] = "0"
        self.assertTrue(h.verify()["closed_lifecycle_verified"])
        self.assertEqual(h.posts, [])

    def test_empty_settlement_never_accepts_another_scope(self):
        h = self.helper
        original = dict(h.history, settledPnl="")
        for field, value in (("instType", "FUTURES"), ("instType", None),
                             ("instId", "BTC-USDT-SWAP"), ("instId", None),
                             ("posSide", "long"), ("posSide", None),
                             ("mgnMode", "isolated"), ("mgnMode", None)):
            with self.subTest(field=field, value=value):
                h.history = dict(original, **{field: value})
                h.refused()

    def test_nonzero_settlement_is_still_an_unsupported_adjustment(self):
        h = self.helper
        for value in ("1", "-1", "0.00000001"):
            with self.subTest(value=value):
                h.history["settledPnl"] = value
                h.refused("DEMO_CLOSED_HISTORY_MISMATCH")

    def test_invalid_settlement_does_not_become_zero(self):
        h = self.helper
        for value in (None, True, False, " ", "NaN", "Infinity", "-Infinity", "bad", [], {}):
            with self.subTest(value=value):
                h.history["settledPnl"] = value
                h.refused("DEMO_INVALID_NUMBER")

    def test_liquidation_penalty_keeps_strict_numeric_requirements(self):
        h = self.helper
        h.history["settledPnl"] = ""
        for value in ("", None, True, "NaN", "Infinity", " "):
            with self.subTest(value=value):
                h.history["liqPenalty"] = value
                h.refused("DEMO_INVALID_NUMBER")
        h.history.pop("liqPenalty")
        h.refused("DEMO_INVALID_NUMBER")
        h.history["liqPenalty"] = "1"
        h.refused("DEMO_CLOSED_HISTORY_MISMATCH")

    def test_critical_price_and_quantity_blanks_remain_invalid(self):
        h = self.helper
        h.history["settledPnl"] = ""
        fields = ((h.entry, "sz"), (h.entry, "accFillSz"), (h.entry, "px"),
                  (h.entry, "avgPx"), (h.parent, "sz"), (h.parent, "slTriggerPx"),
                  (h.parent, "tpTriggerPx"), (h.child, "sz"), (h.child, "accFillSz"),
                  (h.child, "avgPx"), (h.fills[0], "fillSz"), (h.fills[0], "fillPx"),
                  (h.history, "openMaxPos"), (h.history, "closeTotalPos"),
                  (h.history, "openAvgPx"), (h.history, "closeAvgPx"))
        for row, field in fields:
            with self.subTest(field=field):
                original = row[field]
                try:
                    row[field] = ""
                    h.refused("DEMO_INVALID_NUMBER")
                finally:
                    row[field] = original

    def test_global_number_parser_still_rejects_blank(self):
        for options in ({}, {"signed": True}, {"positive": True}):
            with self.subTest(options=options), self.assertRaises(T.DemoRouteError) as caught:
                T._number("", **options)
            self.assertEqual(caught.exception.code, "DEMO_INVALID_NUMBER")

    def test_empty_settlement_still_requires_real_tail_and_both_flat_waves(self):
        h = self.helper
        h.history["settledPnl"] = ""
        times = 0

        def advance(path, _params):
            nonlocal times
            if path.endswith("/public/time"):
                times += 1
                if times == 2:
                    h.server += 1

        h.on_get = advance
        proof = h.verify()
        self.assertTrue(proof["closed_lifecycle_verified"])
        self.assertEqual(proof["history_through_ms"], proof["observed_at_ms"] + 1)
        self.assertEqual(len(h.gets), 31)
        self.assertEqual((h.position_reads, h.parent_reads), (2, 2))
        self.assertEqual(proof["position_history"]["settledPnl"], "")
        self.assertEqual(h.posts, [])

    def test_empty_settlement_does_not_relax_five_second_deadline(self):
        h = self.helper
        h.history["settledPnl"] = ""
        ticks = iter((0, 0))
        with patch.object(T.time, "monotonic", side_effect=lambda: next(ticks, 6)):
            h.refused()

    def test_halted_router_archives_empty_receipt_without_resuming(self):
        h = self.helper
        h.history["settledPnl"] = ""
        router = h.router()
        with patch.object(router, "_emergency", side_effect=AssertionError("NO_EMERGENCY")), \
             patch.object(router, "_halt", side_effect=AssertionError("NO_HALT_REWRITE")):
            result = router.reconcile_halted_exit(h.intent)
        self.assertEqual((result.status, result.code), ("HALTED", "OBSERVE_HALTED"))
        self.assertEqual(result.emergency_status, "FLAT_EXIT_RECONCILED_HALTED")
        row = h.route(router)
        self.assertTrue(row["exit_reconciled"])
        proof = json.loads(Path(row["exit_reconcile_proof_path"]).read_text(encoding="utf-8"))
        self.assertEqual(proof["position_history"]["settledPnl"], "")
        self.assertEqual(h.history["settledPnl"], "")
        h.assert_halt(router)
