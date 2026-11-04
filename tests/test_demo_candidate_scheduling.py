"""Offline scheduling over the real engine and pure execution risk planner."""
from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import engine
from src.execution import ExecutionRejected, build_intent

SPEC = importlib.util.spec_from_file_location("demo_candidate_scheduling_entry", ROOT / "scripts" / "demo_auto_run.py")
ENTRY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENTRY)

NOW = 1_800_000_000_000


class DemoCandidateSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
        self.snapshot = {"inst": "SOL-USDT-SWAP", "equity_estimate": 100,
                         "symbol": deepcopy(self.config["symbols"][0]), "features": {"book": {}}}
        self.raw = [("pullback", "scalp_5m", "long", 95),
                    ("pullback", "swing_1h4h", "long", 85)]
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in (
                ("server_now_ms", lambda: NOW),
                ("global_gates", lambda *_: {"pass": True, "reasons": [], "score_penalty": 0}),
                ("_market_note", lambda *_: {}),
                ("_pullback", self.generator("pullback")),
                ("_breakout_play", self.generator("breakout")),
                ("_exhaustion", self.generator("exhaustion"))):
            self.stack.enter_context(patch.object(engine, name, value))
        self.stack.enter_context(patch.object(ENTRY, "_now", return_value=NOW))

    def generator(self, setup):
        def generate(_snapshot, config, side, kind):
            for source, book, direction, score in self.raw:
                if (source, book, direction) == (setup, kind, side):
                    return {"kind": kind, "side": side, "entry": 100,
                            "stop": 99 if side == "long" else 101,
                            "params": engine._params(kind, config),
                            "conds": [{"name": "setup", "ok": True, "required": True, "weight": score,
                                       "detail": "synthetic satisfied setup"},
                                      {"name": "optional", "ok": False, "required": False,
                                       "weight": 100 - score, "detail": "synthetic optional factor"}]}
            return None
        return generate

    def evaluate(self, entries=0, seen=None, *, filtered=True):
        options = {"signal_filter": ENTRY.current_scan_signal_filter({} if seen is None else seen)} if filtered else {}
        return engine.evaluate(deepcopy(self.snapshot), self.config, {"entries_today": entries}, **options)

    def state(self, entries=0):
        return {"now_ms": NOW, "data_fresh": True, "macro_blackout": False,
                "equity_estimate": 100, "day_start_equity": 100, "week_start_equity": 100,
                "realized_pnl_today": 0, "realized_pnl_week": 0,
                "entries_today": entries, "open_positions": [], "pending_orders": []}

    def payload(self, evaluation, seen):
        return ENTRY.scan_to_payload({}, evaluation, seen, self.config)

    def test_cooled_leader_does_not_hide_independent_cost_qualified_swing(self):
        baseline = self.evaluate(filtered=False)
        self.assertEqual([signal["kind"] for signal in baseline["signals"]], ["scalp_5m", "swing_1h4h"])
        for signal in baseline["signals"]:
            intent = build_intent(signal, self.state(2), self.config)
            self.assertLessEqual(intent.effective_risk, intent.risk_budget)
            self.assertGreaterEqual(intent.net_rr, self.config["risk"]["min_net_rr"])
        self.assertEqual(self.evaluate(2, filtered=False)["signals"][0]["kind"], "scalp_5m")
        seen = {"scalp_5m|long": NOW - 60_000}
        original = deepcopy(seen)
        evaluation = self.evaluate(2, seen)
        self.assertEqual([signal["kind"] for signal in evaluation["signals"]], ["swing_1h4h"])
        self.assertEqual(seen, original)
        payload = self.payload(evaluation, seen)
        self.assertEqual([signal["kind"] for signal in payload["new_signals"]], ["swing_1h4h"])
        self.assertEqual(seen, {**original, "swing_1h4h|long": NOW})

    def test_same_round_kind_side_duplicates_cannot_crowd_out_other_kind(self):
        self.raw.insert(1, ("breakout", "scalp_5m", "long", 90))
        seen = {}
        evaluation = self.evaluate(1, seen)
        self.assertEqual([signal["kind"] for signal in evaluation["signals"]], ["scalp_5m", "swing_1h4h"])
        self.assertEqual(seen, {})
        self.assertEqual(len(self.payload(evaluation, seen)["new_signals"]), 2)

    def test_default_scanner_api_and_explicit_none_preserve_old_output(self):
        self.raw.insert(1, ("breakout", "scalp_5m", "long", 90))
        default = self.evaluate(1, filtered=False)
        explicit = engine.evaluate(deepcopy(self.snapshot), self.config, {"entries_today": 1}, signal_filter=None)
        self.assertEqual(default, explicit)
        self.assertEqual([signal["kind"] for signal in default["signals"]], ["scalp_5m", "scalp_5m"])

    def test_no_cooldown_retains_candidate_order_and_plans(self):
        self.assertEqual(self.evaluate(filtered=False)["signals"], self.evaluate()["signals"])
        for signal in self.evaluate()["signals"]:
            self.assertEqual(build_intent(signal, self.state(), self.config).kind, signal["kind"])

    def test_every_round_selects_at_most_existing_daily_allowance(self):
        self.raw.extend([("pullback", "scalp_5m", "short", 80),
                         ("pullback", "swing_1h4h", "short", 75)])
        for entries in (0, 1, 2, 3, 4):
            with self.subTest(entries=entries):
                seen = {}
                evaluation = self.evaluate(entries, seen)
                self.assertEqual(seen, {})
                payload = self.payload(evaluation, seen)
                allowance = max(0, self.config["risk"]["max_entries_per_day"] - entries)
                self.assertEqual(len(payload["new_signals"]), allowance)
                self.assertEqual(len(seen), allowance)
                self.assertLessEqual(len(evaluation["signals"]), allowance)

    def test_unselected_candidate_is_not_consumed_by_preview(self):
        seen = {}
        evaluation = self.evaluate(2, seen)
        self.payload(evaluation, seen)
        self.assertEqual(seen, {"scalp_5m|long": NOW})
        self.assertTrue(ENTRY.current_scan_signal_filter(seen)(self.evaluate(filtered=False)["signals"][1]))

    def test_exact_cooldown_boundaries_keep_existing_scalp_and_swing_policy(self):
        for kind, minutes in (("scalp_5m", 30), ("swing_1h4h", 180)):
            signal = next(signal for signal in self.evaluate(filtered=False)["signals"] if signal["kind"] == kind)
            for offset, expected in ((minutes * 60_000 - 1, False), (minutes * 60_000, True)):
                with self.subTest(kind=kind, offset=offset):
                    seen = {f"{kind}|long": NOW - offset}
                    self.assertEqual(ENTRY.current_scan_signal_filter(seen)(signal), expected)
                    self.assertEqual(seen, {f"{kind}|long": NOW - offset})

    def test_different_side_and_kind_are_independent_but_same_signal_is_not_repeated(self):
        signal = self.evaluate(filtered=False)["signals"][0]
        other_side = {**signal, "id": "SCA-SHORT-INDEPENDENT", "side": "short"}
        other_kind = {**signal, "id": "SWI-LONG-INDEPENDENT", "kind": "swing_1h4h"}
        eligible = ENTRY.current_scan_signal_filter({"scalp_5m|long": NOW - 1})
        self.assertFalse(eligible(signal))
        self.assertTrue(eligible(other_side))
        self.assertFalse(eligible(other_side))
        self.assertTrue(eligible(other_kind))

    def test_expired_and_research_signals_do_not_consume_preview_cooldown(self):
        signal = self.evaluate(filtered=False)["signals"][0]
        for changes in ({"expires_at_ms": NOW}, {"expires_at_ms": NOW - 1},
                        {"expires_at_ms": str(NOW + 1)}, {"expires_at_utc": "invalid"},
                        {"expires_at_utc": datetime.fromtimestamp(NOW / 1000, timezone.utc).isoformat()},
                        {"expires_at_utc": "2027-01-15T08:00:00"},
                        {"id": "rScH-NEAR-MISS"}, {"research": True}, {"id": "DEMO-TEST-SMOKE"}):
            with self.subTest(changes=changes):
                seen = {}
                eligible = ENTRY.current_scan_signal_filter(seen)
                self.assertFalse(eligible({**signal, **changes}))
                self.assertTrue(eligible(signal))
                self.assertEqual(seen, {})

    def test_preview_rejection_occurs_before_daily_quota(self):
        for mark in ("expired", "research"):
            with self.subTest(mark=mark):
                eligible = ENTRY.current_scan_signal_filter({})
                def reject_top(signal):
                    changes = ({"expires_at_ms": NOW} if mark == "expired" else {"research": True})
                    return eligible({**signal, **changes} if signal["kind"] == "scalp_5m" else signal)
                evaluation = engine.evaluate(deepcopy(self.snapshot), self.config, {"entries_today": 2},
                                             signal_filter=reject_top)
                self.assertEqual([signal["kind"] for signal in evaluation["signals"]], ["swing_1h4h"])

    def test_filter_failure_and_non_boolean_results_fail_closed(self):
        for value in (None, 1, [], "yes"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "^SIGNAL_FILTER_FAILED$"):
                    engine.evaluate(self.snapshot, self.config, {}, signal_filter=lambda _: value)
        calls = []
        def fail_after_first(signal):
            calls.append(signal["id"])
            if len(calls) == 2:
                raise RuntimeError("unexpected preview failure")
            return True
        with self.assertRaisesRegex(ValueError, "^SIGNAL_FILTER_FAILED$"):
            engine.evaluate(self.snapshot, self.config, {}, signal_filter=fail_after_first)
        self.assertEqual(len(calls), 2)

    def test_execution_still_refuses_full_daily_allowance_or_busy_single_slot(self):
        signal = self.evaluate(filtered=False)["signals"][1]
        for changes, code in (({"entries_today": 3}, "DAILY_ENTRIES"),
                              ({"open_positions": [{}]}, "POSITION_LIMIT"),
                              ({"pending_orders": [{}]}, "POSITION_LIMIT")):
            with self.subTest(code=code, changes=changes):
                with self.assertRaises(ExecutionRejected) as refused:
                    build_intent(signal, {**self.state(), **changes}, self.config)
                self.assertEqual(refused.exception.code, code)


if __name__ == "__main__":
    unittest.main()
