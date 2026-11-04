# -*- coding: utf-8 -*-
"""Offline acceptance tests for T1; temporary ledgers never touch live state."""
from __future__ import annotations

import ast
import copy
import csv
import json
import os
import socket
import tempfile
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from src.execution import ExecutionRejected, build_intent, notional_limit
from src.paper_broker import PaperBroker


D = Decimal
ROOT = Path(__file__).resolve().parents[1]
NOW = 1800000000000


def inputs():
    config = json.loads((ROOT / "config.json").read_text(encoding="utf-8-sig"))
    signal = {"id": "PAPER-T1-001", "instId": "SOL-USDT-SWAP", "kind": "scalp_5m",
              "side": "long", "entry": "100", "stop": "99", "tp1": "103", "tp2": "104",
              "generated_at_ms": NOW - 1000, "expires_at_ms": NOW + 60000}
    state = {"now_ms": NOW, "equity_estimate": "117.72", "day_start_equity": "117.72",
             "realized_pnl_today": "0", "entries_today": 0, "open_positions": [],
             "data_fresh": True, "macro_blackout": False, "halted": False,
             "paused_until_ms": 0, "slippage_bps": "0"}
    return signal, state, config


class IntentTests(unittest.TestCase):
    def rejected(self, code, signal=None, state=None, config=None):
        default_signal, default_state, default_config = inputs()
        with self.assertRaises(ExecutionRejected) as caught:
            build_intent(signal if signal is not None else default_signal,
                         state if state is not None else default_state,
                         config if config is not None else default_config)
        self.assertEqual(caught.exception.code, code)

    def test_live_equity_and_total_loss_budget_are_used_without_mutation(self):
        signal, state, config = inputs()
        state["slippage_bps"] = "5"
        before = copy.deepcopy((signal, state, config))
        intent = build_intent(signal, state, config)
        self.assertEqual(intent.risk_budget, D("2.3544"))
        self.assertGreater(intent.qty, 0)
        self.assertLessEqual(intent.effective_risk, intent.risk_budget)
        self.assertLessEqual(intent.notional, D("400"))
        self.assertEqual(intent.kind, "scalp_5m")
        self.assertEqual(intent.to_dict()["kind"], "scalp_5m")
        self.assertEqual((signal, state, config), before)
        lower_state = {**state, "equity_estimate": "100"}
        self.assertGreater(intent.qty, build_intent(signal, lower_state, config).qty)
        self.assertIsInstance(intent.to_dict()["qty"], str)

    def test_no_fallback_to_fixed_config_equity(self):
        _, state, _ = inputs()
        del state["equity_estimate"]
        self.rejected("MISSING_INPUT", state=state)

    def test_daily_limits_and_position_reservation(self):
        for changes, code in [({"realized_pnl_today": "-5.886"}, "DAILY_LOSS"),
                              ({"entries_today": 3}, "DAILY_ENTRIES"),
                              ({"open_positions": [{"signal_id": "other"}]}, "POSITION_LIMIT"),
                              ({"pending_orders": [{"signal_id": "pending"}]}, "POSITION_LIMIT"),
                              ({"halted": True}, "HALTED"),
                              ({"paused_until_ms": NOW + 1}, "PAUSED")]:
            with self.subTest(changes=changes):
                _, state, _ = inputs()
                state.update(changes)
                self.rejected(code, state=state)

    def test_weekly_limit_and_remaining_daily_budget(self):
        signal, state, config = inputs()
        state.update(week_start_equity="117.72", realized_pnl_week="-11.772")
        self.rejected("WEEKLY_LOSS", state=state)
        del state["realized_pnl_week"]
        state["realized_pnl_today"] = "-5.5"
        intent = build_intent(signal, state, config)
        self.assertEqual(intent.risk_budget, D("0.386"))
        self.assertLessEqual(intent.effective_risk, D("0.386"))

    def test_freshness_macro_and_timestamp_gates_fail_closed(self):
        for name, value, code in [("data_fresh", False, "STALE_DATA"),
                                  ("data_fresh", "true", "INVALID_INPUT"),
                                  ("macro_blackout", True, "MACRO_BLACKOUT")]:
            with self.subTest(name=name, value=value):
                _, state, _ = inputs()
                state[name] = value
                self.rejected(code, state=state)
        _, state, _ = inputs()
        del state["data_fresh"]
        self.rejected("MISSING_INPUT", state=state)
        signal, _, _ = inputs()
        signal["expires_at_ms"] = NOW
        self.rejected("EXPIRED", signal=signal)
        signal, _, _ = inputs()
        signal["generated_at_ms"] = NOW + 1
        self.rejected("EXPIRED", signal=signal)

    def test_iso_milliseconds_and_instrument_whitelist(self):
        signal, state, config = inputs()
        del signal["generated_at_ms"], signal["expires_at_ms"]
        signal["generated_at_utc"] = "2026-10-14T10:10:10.833+00:00"
        signal["expires_at_utc"] = "2026-10-14T10:11:10.833+00:00"
        state["now_ms"] = 1791972620833
        intent = build_intent(signal, state, config)
        self.assertEqual(intent.created_at_ms, 1791972610833)
        signal["instId"] = "BTC-USDT-SWAP"
        self.rejected("INSTRUMENT", signal=signal, state=state, config=config)

    def test_stop_tick_rounding_only_tightens_and_short_geometry(self):
        signal, state, config = inputs()
        signal.update(entry="100.003", stop="99.003")
        long = build_intent(signal, state, config)
        self.assertGreaterEqual(long.stop, D("99.003"))
        self.assertLessEqual(long.entry, D("100.003"))
        signal.update(side="short", entry="100.003", stop="101.007", tp1="97", tp2="96")
        short = build_intent(signal, state, config)
        self.assertLessEqual(short.stop, D("101.007"))
        self.assertGreaterEqual(short.entry, D("100.003"))
        signal["stop"] = "100.001"
        self.rejected("PRICE_GEOMETRY", signal=signal)

    def test_fee_share_and_net_reward_risk_are_recomputed(self):
        signal, state, config = inputs()
        signal.update(stop="99.7", tp1="103", tp2="104", net_rr1=999, fee_share_of_risk=0)
        self.rejected("FEE_SHARE", signal=signal)
        signal.update(stop="99", tp1="101.8", tp2="103")
        state["slippage_bps"] = "5"
        self.rejected("RR", signal=signal, state=state)

    def test_fee_gate_counts_tp_and_three_taker_legs_even_for_maker_entry(self):
        signal, state, config = inputs()
        signal.update(stop="99.4", tp1="101.8", tp2="102.4")
        max_share = D(str(config["risk"]["fee"]["max_fee_share_of_risk"]))
        # Entry plus stop alone pass even at taker; adding the TP must reject.
        self.assertLess((D("100") + D("99.4")) * D("0.0005") / D("0.6"), max_share)
        self.assertGreater((D("100") + D("99.4") + D("101.8")) * D("0.0005") /
                           D("0.6"), max_share)
        self.rejected("FEE_SHARE", signal=signal, state=state, config=config)
        # A wider stop passes. Risk sizing still includes actual maker entry
        # plus the stop only, while net RR budgets a taker TP exit.
        signal.update(stop="99.3", tp1="102.1", tp2="102.8")
        intent = build_intent(signal, state, config)
        loss_unit = D("0.7") + D("100") * D("0.0002") + D("99.3") * D("0.0005")
        reward_unit = D("2.1") - D("100") * D("0.0002") - D("102.1") * D("0.0005")
        self.assertTrue(intent.require_maker)
        self.assertEqual(intent.effective_risk, intent.qty * loss_unit)
        self.assertEqual(intent.net_rr, reward_unit / loss_unit)

    def test_notional_limit_combines_equity_hard_cap_and_all_config_spellings(self):
        signal, state, config = inputs()
        self.assertEqual(notional_limit("117.72", config), D("400"))
        self.assertEqual(notional_limit("10", config), D("80"))
        for scope in ("root", "risk", "account"):
            for field in ("max_notional", "max_notional_usd"):
                with self.subTest(scope=scope, field=field):
                    modified = copy.deepcopy(config)
                    target = modified if scope == "root" else modified[scope]
                    target[field] = "125"
                    self.assertEqual(notional_limit("117.72", modified), D("125"))
                    self.assertLessEqual(build_intent(signal, state, modified).notional, D("125"))
                    target[field] = "900"
                    self.assertEqual(notional_limit("117.72", modified), D("400"))
        config["max_notional"] = "350"
        config["risk"]["max_notional_usd"] = "275"
        config["account"]["max_notional"] = "200"
        self.assertEqual(notional_limit("117.72", config), D("200"))
        self.assertEqual(notional_limit("10", config), D("80"))
        state.update(equity_estimate="1000", day_start_equity="1000")
        uncapped_config = inputs()[2]
        self.assertEqual(build_intent(signal, state, uncapped_config).notional, D("400"))

    def test_present_notional_caps_cannot_be_invalid_or_silently_ignored(self):
        for scope in ("root", "risk", "account"):
            for field in ("max_notional", "max_notional_usd"):
                for invalid in (None, 0, -1, "NaN", True):
                    with self.subTest(scope=scope, field=field, invalid=invalid):
                        signal, state, config = inputs()
                        target = config if scope == "root" else config[scope]
                        target[field] = invalid
                        self.rejected("INVALID_INPUT", signal=signal, state=state, config=config)

    def test_contract_value_notional_cap_and_minimum_quantity(self):
        signal, state, config = inputs()
        standard = build_intent(signal, state, config)
        config["symbols"][0]["ctVal"] = 10
        intent = build_intent(signal, state, config)
        self.assertLess(intent.qty, standard.qty)
        self.assertEqual(intent.notional, intent.qty * D("10") * intent.entry)
        self.assertLessEqual(intent.effective_risk, intent.risk_budget)
        config["risk"]["max_notional_usd"] = "20"
        self.assertLessEqual(build_intent(signal, state, config).notional, D("20"))
        config["symbols"][0]["minSz"] = 1
        self.rejected("MIN_QTY", signal=signal, state=state, config=config)

    def test_bad_numbers_unknown_playbook_and_bad_stop_are_rejected(self):
        for changes, code in [({"qty": "NaN"}, "INVALID_INPUT"),
                              ({"entry": "Infinity"}, "INVALID_INPUT"),
                              ({"stop": "101"}, "PRICE_GEOMETRY"),
                              ({"kind": "missing"}, "PLAYBOOK_UNKNOWN"),
                              ({"kind": None}, "PLAYBOOK_UNKNOWN")]:
            with self.subTest(changes=changes):
                signal, _, _ = inputs()
                signal.update(changes)
                self.rejected(code, signal=signal)

    def test_source_has_no_private_transport_import_and_network_is_unused(self):
        for filename in ("execution.py", "paper_broker.py"):
            tree = ast.parse((ROOT / "src" / filename).read_text(encoding="utf-8"))
            imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
            imports.extend(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
                           for alias in node.names)
            self.assertFalse(any("okx_private" in name for name in imports))
        with patch.object(socket, "socket", side_effect=AssertionError("offline test")):
            self.assertGreater(build_intent(*inputs()).qty, 0)


class PaperBrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.broker = PaperBroker(log_dir=self.temp.name, clock=lambda: NOW)
        signal, state, config = inputs()
        signal["qty"] = "0.5"
        self.intent = build_intent(signal, state, config)

    def enter(self, quantity=None):
        self.broker.on_price(bid="100.9", ask="101", timestamp_ms=NOW)
        order = self.broker.place_limit(self.intent)
        self.broker.on_price(bid="99.99", ask="100", available_qty=quantity, timestamp_ms=NOW + 1)
        return order

    def test_limit_waits_then_partial_fill_installs_protection(self):
        self.broker.on_price(bid="101", ask="101.1", timestamp_ms=NOW)
        order = self.broker.place_limit(self.intent)
        self.broker.on_price(bid="100.1", ask="100.2", timestamp_ms=NOW + 1)
        self.assertEqual(order.filled_qty, 0)
        self.assertEqual(order.entry_fee, 0)
        self.broker.on_price(bid="99.99", ask="100", available_qty="0.2", timestamp_ms=NOW + 2)
        self.assertEqual(order.filled_qty, D("0.2"))
        self.assertEqual(order.open_qty, D("0.2"))
        self.assertTrue(order.protection_active)
        self.assertLessEqual(order.avg_entry_price, self.intent.entry)
        self.assertEqual(order.entry_fee, order.avg_entry_price * D("0.2") * D("0.0002"))

    def test_full_entry_and_exit_fees_and_csv_ledger(self):
        order = self.enter()
        self.assertEqual(order.open_qty, D("0.5"))
        trade = self.broker.close(order.order_id, "101", timestamp_ms=NOW + 2)
        expected = (D("101") - order.avg_entry_price) * D("0.5") - order.entry_fee - D("0.02525")
        self.assertEqual(trade["net_pnl"], expected)
        self.assertEqual(order.open_qty, 0)
        self.assertEqual(order.realized_pnl, expected)
        for filename in ("paper_orders.csv", "paper_trades.csv"):
            with (Path(self.temp.name) / filename).open(encoding="utf-8-sig", newline="") as file:
                rows = list(csv.DictReader(file))
            self.assertTrue(rows, filename)
            self.assertTrue(all(row.get("mode") == "paper" for row in rows))

    def test_stop_gap_cancels_unfilled_remainder_and_does_not_reopen(self):
        order = self.enter("0.2")
        self.broker.on_price(bid="98", ask="98.01", timestamp_ms=NOW + 2)
        self.assertEqual(order.open_qty, 0)
        self.assertEqual(order.remaining_qty, 0)
        self.assertEqual(order.filled_qty, D("0.2"))
        self.assertLess(order.gross_pnl, (self.intent.stop - order.avg_entry_price) * D("0.2"))
        self.broker.on_price(bid="99.9", ask="100", timestamp_ms=NOW + 3)
        self.assertEqual(order.open_qty, 0)
        self.assertEqual(order.filled_qty, D("0.2"))

    def test_expiry_cancels_only_unfilled_entry_and_keeps_stop(self):
        order = self.enter("0.2")
        self.broker.on_price(bid="100.9", ask="101", timestamp_ms=self.intent.expires_at_ms)
        self.assertEqual(order.remaining_qty, 0)
        self.assertEqual(order.open_qty, D("0.2"))
        self.assertTrue(order.protection_active)
        self.broker.on_price(bid="98", ask="98.01", timestamp_ms=self.intent.expires_at_ms + 1)
        self.assertEqual(order.open_qty, 0)

    def test_target_exits_full_position_before_another_entry(self):
        order = self.enter("0.2")
        self.broker.on_price(bid="103", ask="103.01", timestamp_ms=NOW + 2)
        self.assertEqual(order.open_qty, 0)
        self.assertEqual(order.remaining_qty, 0)
        self.assertGreater(order.realized_pnl, 0)
        # The paper TP is deliberately a resting maker limit. The intent's
        # conservative fee gate does not assume this for the separate router.
        self.assertEqual(order.exit_fee, D("0.2") * D("103") * D("0.0002"))

    def test_manual_partial_close_allocates_entry_fees_once(self):
        order = self.enter()
        first = self.broker.close(order.order_id, "101", qty="0.2", timestamp_ms=NOW + 2)
        second = self.broker.close(order.order_id, "102", timestamp_ms=NOW + 3)
        self.assertEqual(first["entry_fee"] + second["entry_fee"], order.entry_fee)
        self.assertEqual(first["net_pnl"] + second["net_pnl"], order.realized_pnl)

    def test_market_crossing_maker_entry_is_rejected_and_duplicate_cannot_fill_twice(self):
        self.broker.on_price(bid="99.8", ask="99.9", timestamp_ms=NOW)
        try:
            order = self.broker.place_limit(self.intent)
        except ValueError:
            order = None
        if order is not None:
            self.assertEqual(order.filled_qty, 0)
            self.assertIn(order.status, ("rejected", "cancelled"))
        with self.assertRaises(ValueError):
            self.broker.place_limit(self.intent)

    def test_taker_entry_uses_taker_fee_and_stays_within_limit(self):
        signal, state, config = inputs()
        config["playbooks"]["scalp_5m"]["entry_style"] = "limit"
        signal["qty"] = "0.5"
        intent = build_intent(signal, state, config)
        self.broker.on_price(bid="99.8", ask="99.9", timestamp_ms=NOW)
        order = self.broker.place_limit(intent)
        self.broker.on_price(bid="99.8", ask="99.9", timestamp_ms=NOW + 1)
        self.assertGreater(order.filled_qty, 0)
        self.assertLessEqual(order.avg_entry_price, intent.entry)
        self.assertEqual(order.entry_fee, order.avg_entry_price * order.filled_qty * D("0.0005"))

    def test_short_stop_and_slippage_have_adverse_direction(self):
        signal, state, config = inputs()
        signal.update(side="short", stop="101", tp1="97", tp2="96", qty="0.5")
        state["slippage_bps"] = "10"
        intent = build_intent(signal, state, config)
        self.broker.on_price(bid="99", ask="99.1", timestamp_ms=NOW)
        order = self.broker.place_limit(intent)
        self.broker.on_price(bid="100", ask="100.01", timestamp_ms=NOW + 1)
        self.broker.on_price(bid="101.9", ask="102", timestamp_ms=NOW + 2)
        self.assertEqual(order.open_qty, 0)
        self.assertLess(order.gross_pnl, (order.avg_entry_price - D("102")) * D("0.5"))

    def test_restart_cannot_silently_forget_existing_orders(self):
        self.enter("0.2")
        with self.assertRaises((ValueError, RuntimeError)):
            PaperBroker(log_dir=self.temp.name, clock=lambda: NOW)

    def test_default_brokers_use_unique_run_directories_even_at_the_same_time(self):
        previous = os.getcwd()
        try:
            os.chdir(self.temp.name)
            first = PaperBroker(clock=lambda: NOW)
            second = PaperBroker(clock=lambda: NOW)
            self.assertNotEqual(first.log_dir, second.log_dir)
            for broker in (first, second):
                self.assertEqual(broker.log_dir.parent, Path("logs") / "paper")
                self.assertTrue(broker.log_dir.name.startswith("run-"))
                self.assertTrue(broker.orders_path.exists())
                self.assertTrue(broker.trades_path.exists())
        finally:
            os.chdir(previous)

    def test_stale_independent_intents_cannot_bypass_single_position_limit(self):
        self.broker.on_price(bid="100.9", ask="101", timestamp_ms=NOW)
        first = self.broker.place_limit(self.intent)
        second_intent = replace(self.intent, signal_id="PAPER-T1-002")
        with self.assertRaises(ValueError):
            self.broker.place_limit(second_intent)
        self.broker.on_price(bid="99.99", ask="100", available_qty="0.2", timestamp_ms=NOW + 1)
        with self.assertRaises(ValueError):
            self.broker.place_limit(second_intent)
        self.broker.close(first.order_id, "101", timestamp_ms=NOW + 2)
        self.broker.on_price(bid="100.9", ask="101", timestamp_ms=NOW + 3)
        second = self.broker.place_limit(second_intent)
        self.assertEqual(second.status, "pending")
        self.assertEqual(len(self.broker.positions), 0)

    def test_triggered_stop_waits_for_liquidity_and_stays_triggered_on_rebound(self):
        order = self.enter("0.2")
        self.broker.on_price(bid="98", ask="98.01", available_qty="0.1", timestamp_ms=NOW + 2)
        self.assertEqual(order.open_qty, D("0.1"))
        self.assertEqual(order.remaining_qty, 0)
        self.assertTrue(order.protection_active)
        self.broker.on_price(bid="101", ask="101.01", available_qty="0.1", timestamp_ms=NOW + 3)
        self.assertEqual(order.open_qty, 0)
        self.assertEqual(order.reason, "stop_loss")

    def test_zero_liquidity_and_unfilled_expiry_do_not_charge_fees(self):
        self.broker.on_price(bid="100.9", ask="101", timestamp_ms=NOW)
        order = self.broker.place_limit(self.intent)
        self.broker.on_price(bid="99.99", ask="100", available_qty=0, timestamp_ms=NOW + 1)
        self.assertEqual(order.filled_qty, 0)
        self.assertEqual(order.entry_fee, 0)
        self.broker.on_price(bid="100.9", ask="101", available_qty=0,
                             timestamp_ms=self.intent.expires_at_ms)
        self.assertEqual(order.status, "expired")
        self.assertEqual(order.remaining_qty, 0)
        self.assertEqual(order.realized_pnl, 0)

    def test_offline_broker_works_even_when_network_is_disabled(self):
        with patch.object(socket, "socket", side_effect=AssertionError("offline test")):
            order = self.enter()
            self.broker.close(order.order_id, "101", timestamp_ms=NOW + 2)
            self.assertEqual(order.open_qty, 0)


if __name__ == "__main__":
    unittest.main()
