"""Pure strong-link ledger fixtures, not assertions about undocumented API fields."""
from __future__ import annotations

import copy
import hashlib
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from src import demo_ledger as L


D = Decimal
MONDAY = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
ACCOUNT = hashlib.sha256(b"DEMO\x00246810").hexdigest()
NOW = MONDAY + 2 * 3_600_000
ANCHOR_CLOSED = MONDAY - 2 * L.DAY_MS


def evidence():
    anchor = dict(posId="900", instId="ETH-USDT-SWAP", instType="SWAP",
                  cTime=str(ANCHOR_CLOSED - L.DAY_MS), uTime=str(ANCHOR_CLOSED),
                  openMaxPos="321.88", closeTotalPos="321.88", ccy="USDT",
                  pnl="100", fee="-1", fundingFee="-0.1", liqPenalty="0", realizedPnl="98.9")
    coverage = {flag: True for flag in L.COMPLETE_FLAGS}
    coverage.update(environment="DEMO", account_hash=ACCOUNT,
                    account_config={"uid": "246810", "acctLv": "2", "posMode": "net_mode"},
                    start_ms=ANCHOR_CLOSED + 1, end_ms=NOW,
                    authorization_at_ms=MONDAY - 4 * 3_600_000,
                    anchor_lookback_complete=True,
                    anchor_lookback_start_ms=ANCHOR_CLOSED-L.DAY_MS,
                    loss_anchor=dict(source="completed-position-reset", environment="DEMO",
                                     account_hash=ACCOUNT, record=copy.deepcopy(anchor)))
    return dict(fills=[], bills=[], positions_history=[anchor], routes=[], order_details={},
                now_ms=NOW, day_start_ms=MONDAY, week_start_ms=MONDAY, coverage=coverage)


def add_position(data, number, opened, closed, *, gross="-1", funding="0", oco=True, pos_id=None):
    entry, exit_order, algo = str(100 + number), str(200 + number), str(300 + number)
    pos_id = pos_id or str(400 + number)
    quantity = D("1")
    route = dict(account_hash=ACCOUNT, signal_id=f"signal{number}", inst_id=L.INST,
                 side="long", status="CLOSED", position_id=pos_id,
                 position_opened_at_ms=opened, order_id=entry,
                 client_order_id=f"entry{number}", protection_order_id=algo if oco else "",
                 close_order_ids=[] if oco else [exit_order], filled_qty=str(quantity))
    data["routes"].append(route)
    net = D(gross) - D("0.2") + D(funding)
    history = dict(posId=pos_id, instId=L.INST, instType="SWAP", posSide="net", direction="long",
                   mgnMode="cross", ccy="USDT", cTime=str(opened), uTime=str(closed),
                   openMaxPos=str(quantity), closeTotalPos=str(quantity),
                   pnl=gross, fee="-0.2", fundingFee=funding, liqPenalty="0", realizedPnl=str(net))
    data["positions_history"].append(history)
    for offset, order, stamp, pnl, side in ((1, entry, opened, "0", "buy"),
                                          (2, exit_order, closed, gross, "sell")):
        bid, tid = str(10000 + number * 10 + offset), str(50000 + number * 10 + offset)
        fill = dict(instId=L.INST, instType="SWAP", posSide="net", ordId=order, tradeId=tid,
                    billId=bid, fillTime=str(stamp), fillSz=str(quantity),
                    fillPnl=pnl, fee="-0.1", feeCcy="USDT", side=side)
        data["fills"].append(fill)
        data["bills"].append(dict(billId=bid, instId=L.INST, instType="SWAP", ccy="USDT",
                                  ordId=order, tradeId=tid, ts=str(stamp), type="2",
                                  pnl=pnl, fee="-0.1", balChg=str(D(pnl) - D("0.1"))))
        detail = dict(ordId=order, instId=L.INST, posSide="net", tdMode="cross", side=side,
                      state="filled", sz=str(quantity), accFillSz=str(quantity),
                      ordType="post_only" if offset == 1 else "market", reduceOnly=offset != 1,
                      clOrdId=f"entry{number}" if offset == 1 else f"close{number}")
        if offset == 2 and oco:
            detail["algoId"] = algo
        data["order_details"][order] = detail
    if D(funding):
        data["bills"].append(dict(billId=str(80000+number), instId=L.INST, instType="SWAP",
                                  ccy="USDT", posId=pos_id, ts=str((opened+closed)//2),
                                  type="8", balChg=funding))
    return route, history


def reservation():
    return dict(account_hash=ACCOUNT, signal_id="reserved1", client_order_id="r12345",
                inst_id=L.INST, side="long", status="RESERVED", submit_attempted=False,
                filled_qty="0", protected_qty="0", order_id="", entry_state="",
                protection_order_id="", close_order_id="", close_id="",
                confirmed_closed_qty="0", close_order_ids=[], close_qty="0", avg_price="0",
                protection_requested_qty="", emergency_status="", cancel_requested=False)


class DemoLedgerTests(unittest.TestCase):
    def setUp(self):
        self.data = evidence()
        self.network = patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def validate(self):
        return L.validate_demo_ledger(**self.data)

    def refused(self, code):
        result = self.validate()
        self.assertFalse(result["verified"])
        self.assertIn(code, result["refusals"])
        self.assertIsNone(result["realized_pnl_today"])
        self.assertIsNone(result["paused_until_ms"])
        return result

    def add(self, number=1, **kwargs):
        return add_position(self.data, number, MONDAY+number*600_000,
                            MONDAY+number*600_000+300_000, **kwargs)

    def test_empty_new_period_requires_explicit_completed_legacy_reset_anchor(self):
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual((result["realized_pnl_today"], result["entries_today"]), (D("0"), 0))
        self.assertEqual(result["completed_positions"], [])
        self.assertFalse(result["paused"])

    def test_multiple_owned_positions_positive_and_negative_are_fully_verified(self):
        add_position(self.data, 1, MONDAY-3_600_000, MONDAY-1_800_000, gross="3")
        self.add(2, gross="-1")
        self.add(3, gross="2")
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["realized_pnl_today"], D("0.6"))
        self.assertEqual(result["realized_pnl_week"], D("0.6"))
        self.assertEqual(result["entries_today"], 2)
        self.assertEqual(len(result["completed_positions"]), 3)
        self.assertEqual(result["consecutive_losses"], 0)

    def test_fees_and_funding_count_once_trade_bill_and_position_pnl_not_added(self):
        self.add(gross="5", funding="-0.05")
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["realized_pnl_today"], D("4.75"))
        self.assertEqual(result["completed_positions"][0]["net_pnl"], D("4.75"))

    def test_three_completed_losses_pause_for_24h_across_day_and_week(self):
        add_position(self.data, 1, MONDAY-3_600_000, MONDAY-1_800_000)
        self.add(2)
        _, last = self.add(3)
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["consecutive_losses"], 3)
        self.assertEqual(result["paused_until_ms"], int(last["uTime"])+L.DAY_MS)
        self.assertTrue(result["paused"])
        self.assertEqual(result["entries_today"], 2)

    def test_win_does_not_erase_already_triggered_pause(self):
        self.add(1)
        self.add(2)
        _, third = self.add(3)
        self.add(4, gross="5")
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["consecutive_losses"], 0)
        self.assertEqual(result["paused_until_ms"], int(third["uTime"])+L.DAY_MS)

    def test_no_fresh_flat_boolean_or_week_only_anchor_can_establish_readiness(self):
        self.data["coverage"]["loss_anchor"] = dict(verified=True, flat=True,
            source="previous-verified-demo-ledger", account_hash=ACCOUNT)
        self.refused("LEDGER_LOSS_ANCHOR_UNVERIFIED")

    def test_legacy_anchor_must_be_full_nonlosing_and_reconcile_exact_fields(self):
        for field, value in (("realizedPnl", "-1"), ("closeTotalPos", "1"),
                             ("fee", "-100"), ("uTime", str(MONDAY+1))):
            with self.subTest(field=field):
                self.data = evidence()
                self.data["coverage"]["loss_anchor"]["record"][field] = value
                self.refused("LEDGER_LOSS_ANCHOR_UNVERIFIED")

    def test_anchor_record_must_exist_in_complete_position_history(self):
        self.data["positions_history"] = []
        self.refused("LEDGER_LOSS_ANCHOR_RECORD_MISSING")

    def test_anchor_needs_prior_cooldown_window_evidence(self):
        self.data["coverage"]["anchor_lookback_complete"] = False
        self.refused("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")

    def test_prior_recent_loss_cannot_be_erased_by_legacy_win(self):
        previous = copy.deepcopy(self.data["positions_history"][0])
        previous.update(posId="899", uTime=str(ANCHOR_CLOSED-1000),
                        pnl="-1", fee="-1", fundingFee="-0.1", realizedPnl="-2.1")
        self.data["positions_history"].append(previous)
        self.refused("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")

    def test_partial_history_flags_scope_or_window_refuse(self):
        for flag in L.COMPLETE_FLAGS:
            with self.subTest(flag=flag):
                self.data = evidence()
                self.data["coverage"][flag] = False
                self.refused("LEDGER_HISTORY_INCOMPLETE")
        self.data = evidence()
        self.data["coverage"]["end_ms"] -= 1
        self.refused("LEDGER_COVERAGE_WINDOW")

    def test_environment_and_account_binding_are_mandatory(self):
        self.data["coverage"]["environment"] = "LIVE"
        self.refused("LEDGER_ENVIRONMENT")
        self.data = evidence()
        self.add()
        self.data["routes"][0]["account_hash"] = "b"*64
        self.refused("LEDGER_ACCOUNT_BINDING")

    def test_actual_oco_order_detail_link_used_without_fill_algo_id(self):
        self.add()
        self.assertTrue(all("algoId" not in fill for fill in self.data["fills"]))
        self.assertTrue(self.validate()["verified"])
        del self.data["order_details"]["201"]["algoId"]
        self.refused("LEDGER_FILL_OWNERSHIP_UNVERIFIED")

    def test_emergency_exit_ordid_must_be_persisted_and_reduce_only(self):
        self.add(oco=False)
        self.assertTrue(self.validate()["verified"])
        self.data["order_details"]["201"]["reduceOnly"] = False
        self.refused("LEDGER_FILL_ROLE_MISMATCH")

    def test_unknown_external_order_ids_never_adopted_by_time_or_position_side(self):
        self.add()
        self.data["fills"][-1]["ordId"] = "9999"
        self.data["order_details"]["9999"] = self.data["order_details"].pop("201")
        self.data["order_details"]["9999"].update(ordId="9999", algoId="foreign")
        self.refused("LEDGER_FILL_OWNERSHIP_UNVERIFIED")

    def test_missing_raw_detail_fields_cannot_be_inferred_from_request(self):
        for field in ("tdMode", "posSide", "reduceOnly", "side", "state", "ordId"):
            with self.subTest(field=field):
                self.data = evidence()
                self.add()
                del self.data["order_details"]["201"][field]
                self.assertFalse(self.validate()["verified"])

    def test_order_detail_cumulative_size_proves_complete_fills(self):
        self.add()
        self.data["order_details"]["201"]["accFillSz"] = "2"
        self.refused("LEDGER_ORDER_DETAIL_SIZE_MISMATCH")

    def test_fill_fee_currency_and_missing_pnl_refuse(self):
        self.add()
        self.data["fills"][0]["feeCcy"] = "BTC"
        self.refused("LEDGER_FILL_SCOPE")
        self.data = evidence()
        self.add()
        del self.data["fills"][0]["fillPnl"]
        self.refused("LEDGER_NUMBER_INVALID")

    def test_trade_bill_requires_all_bill_order_trade_identifiers_and_amounts(self):
        for field, value in (("tradeId", "other"), ("ordId", "other"), ("billId", "other"),
                             ("pnl", "99"), ("fee", "99"), ("balChg", "99")):
            with self.subTest(field=field):
                self.data = evidence()
                self.add()
                self.data["bills"][0][field] = value
                self.assertFalse(self.validate()["verified"])

    def test_missing_trade_bill_or_extra_trade_bill_is_unverified(self):
        self.add()
        self.data["bills"].pop()
        self.refused("LEDGER_TRADE_BILL_MISSING")
        self.data = evidence()
        self.add()
        extra = dict(self.data["bills"][0], billId="extra")
        self.data["bills"].append(extra)
        self.refused("LEDGER_TRADE_BILL_UNMATCHED")

    def test_funding_without_position_id_uses_unique_proven_owned_net_lifecycle(self):
        self.add(funding="-0.1")
        del self.data["bills"][-1]["posId"]
        self.assertTrue(self.validate()["verified"])
        self.data["bills"][-1]["posId"] = "foreign"
        self.refused("LEDGER_FUNDING_LINK_UNVERIFIED")
        self.data = evidence()
        self.add(funding="-0.1")
        self.data["bills"][-1]["ccy"] = "BTC"
        self.refused("LEDGER_BILL_SCOPE")

    def test_transfer_or_unknown_cash_flow_refuses_even_zero_amount(self):
        self.add()
        self.data["bills"].append(dict(billId="external", instId=L.INST, instType="SWAP", ccy="USDT",
                                         ts=str(MONDAY+1), type="1", balChg="0"))
        self.refused("LEDGER_EXTERNAL_CASH_FLOW")

    def test_identical_duplicates_deduplicate_without_double_pnl(self):
        self.add()
        for field in ("fills", "bills", "positions_history"):
            self.data[field].append(copy.deepcopy(self.data[field][-1]))
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["realized_pnl_today"], D("-1.2"))

    def test_conflicting_duplicate_fills_bills_and_position_events_refuse(self):
        for field, changed, code in (("fills", "fee", "LEDGER_FILL_DUPLICATE_CONFLICT"),
                                     ("bills", "balChg", "LEDGER_BILL_DUPLICATE_CONFLICT"),
                                     ("positions_history", "realizedPnl", "LEDGER_POSITION_DUPLICATE_CONFLICT")):
            with self.subTest(field=field):
                self.data = evidence()
                self.add()
                conflict = copy.deepcopy(self.data[field][-1])
                conflict[changed] = "999"
                self.data[field].append(conflict)
                self.refused(code)

    def test_reused_position_id_is_joined_with_opening_time(self):
        self.add(1, pos_id="same")
        self.add(2, pos_id="same")
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(len(result["completed_positions"]), 2)

    def test_partial_position_or_missing_position_binding_refuses(self):
        _, history = self.add()
        history["closeTotalPos"] = "0.5"
        self.refused("LEDGER_PARTIAL_POSITION_HISTORY")
        self.data = evidence()
        route, _ = self.add()
        del route["position_id"]
        self.refused("LEDGER_ID_INVALID")

    def test_realized_position_pnl_is_checked_not_trusted_as_extra_profit(self):
        _, history = self.add()
        history["realizedPnl"] = "999"
        self.refused("LEDGER_POSITION_PNL_MISMATCH")

    def test_rounding_tolerance_cannot_turn_small_loss_into_streak_reset(self):
        _, history = self.add(gross="0.2")
        history["realizedPnl"] = "-0.000000001"
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["consecutive_losses"], 1)
        self.assertTrue(result["completed_positions"][0]["loss_for_pause"])

    def test_account_mode_and_funding_interval_are_proved(self):
        self.data["coverage"]["account_config"]["acctLv"] = "3"
        self.refused("LEDGER_ACCOUNT_SCOPE")
        self.data = evidence()
        self.add(funding="-0.1")
        self.data["bills"][-1].pop("posId")
        self.data["bills"][-1]["ts"] = self.data["positions_history"][-1]["cTime"]
        self.refused("LEDGER_FUNDING_LINK_UNVERIFIED")

    def test_partial_entry_fills_count_one_owned_order(self):
        self.add()
        first = self.data["fills"][0]
        first["fillSz"], first["fee"] = "0.4", "-0.04"
        bill = self.data["bills"][0]
        bill["fee"], bill["balChg"] = "-0.04", "-0.04"
        second = dict(first, tradeId="extra", billId="extra", fillSz="0.6", fee="-0.06")
        self.data["fills"].append(second)
        self.data["bills"].append(dict(bill, tradeId="extra", billId="extra", fee="-0.06", balChg="-0.06"))
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["entries_today"], 1)

    def test_input_is_not_mutated_and_no_file_io_occurs(self):
        self.add()
        before = copy.deepcopy(self.data)
        with patch("builtins.open", side_effect=AssertionError("NO_FILE_IO")):
            self.assertTrue(self.validate()["verified"])
        self.assertEqual(self.data, before)

    def test_invalid_boundaries_future_activity_and_nonfinite_money_refuse(self):
        self.data["day_start_ms"] += 1
        self.refused("LEDGER_BOUNDARIES_INVALID")
        self.data = evidence()
        self.add()
        self.data["fills"][0]["fillTime"] = str(NOW+1)
        self.refused("LEDGER_FILL_OUTSIDE_COVERAGE")
        self.data = evidence()
        self.add()
        self.data["fills"][0]["fee"] = "NaN"
        self.refused("LEDGER_NUMBER_INVALID")

    def test_active_or_unresolved_route_is_not_an_empty_account(self):
        self.data["routes"].append(dict(account_hash=ACCOUNT, signal_id="unresolved", filled_qty="0", status="PENDING"))
        self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_one_proven_unsent_reservation_does_not_block_second_preflight_or_count_entry(self):
        self.data["routes"].append(reservation())
        self.data["coverage"]["account_flat_and_empty"] = True
        result = self.validate()
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["entries_today"], 0)
        self.assertEqual(result["completed_positions"], [])

    def test_reservation_requires_actual_collector_flat_empty_flag(self):
        self.data["routes"].append(reservation())
        self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")
        for value in (False, None, 1, "True"):
            with self.subTest(value=value):
                self.data["coverage"]["account_flat_and_empty"] = value
                self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_unsubmitted_reserved_must_not_adopt_submitting_pending_unknown_or_protected(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        for status in ("SUBMITTING", "PENDING", "UNKNOWN", "HALTED", "PROTECTED", "PROTECTING"):
            with self.subTest(status=status):
                self.data["routes"] = [dict(reservation(), status=status)]
                self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_reservation_submit_attempted_must_be_explicit_false(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        for value in (True, None, 0, "False"):
            with self.subTest(value=value):
                self.data["routes"] = [dict(reservation(), submit_attempted=value)]
                self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")
        route = reservation()
        del route["submit_attempted"]
        self.data["routes"] = [route]
        self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_reservation_exchange_fields_must_be_present_and_exactly_empty(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        for field in ("order_id", "entry_state", "protection_order_id", "close_order_id", "close_id"):
            for operation in ("missing", "none", "populated"):
                with self.subTest(field=field, operation=operation):
                    route = reservation()
                    if operation == "missing":
                        del route[field]
                    else:
                        route[field] = None if operation == "none" else "unexpected"
                    self.data["routes"] = [route]
                    self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_reservation_quantities_must_be_explicit_finite_zero(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        for field in ("protected_qty", "confirmed_closed_qty"):
            for value in (None, "1", "-1", "NaN", False):
                with self.subTest(field=field, value=value):
                    self.data["routes"] = [dict(reservation(), **{field: value})]
                    self.assertFalse(self.validate()["verified"])
            route = reservation()
            del route[field]
            self.data["routes"] = [route]
            self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")

    def test_reservation_close_ids_and_prior_action_fields_are_not_silently_ignored(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        cases = [("close_order_ids", ["old"]), ("close_order_ids", None),
                 ("close_order_ids", ()), ("position_id", "old"),
                 ("position_opened_at_ms", 123), ("position_opened_at_ms", False),
                 ("protection_requested_qty", "1"), ("cancel_requested", True),
                 ("emergency_status", "CLOSE_UNCONFIRMED"), ("avg_price", "100"), ("close_qty", "1")]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.data["routes"] = [dict(reservation(), **{field: value})]
                self.assertFalse(self.validate()["verified"])

    def test_reservation_validates_account_instrument_side_and_client_id(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        for field, value in (("account_hash", "b"*64), ("inst_id", "ETH-USDT-SWAP"),
                             ("side", "buy"), ("client_order_id", ""), ("client_order_id", "bad_id")):
            with self.subTest(field=field):
                self.data["routes"] = [dict(reservation(), **{field: value})]
                self.assertFalse(self.validate()["verified"])

    def test_two_unsent_reservations_are_not_ignored_as_an_empty_account(self):
        self.data["coverage"]["account_flat_and_empty"] = True
        self.data["routes"] = [reservation(), dict(reservation(), signal_id="reserved2", client_order_id="r67890")]
        self.refused("LEDGER_UNRESOLVED_OWNED_ROUTE")


if __name__ == "__main__":
    unittest.main()
