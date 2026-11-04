"""Offline accounting proofs keep execution time distinct from record time."""
from __future__ import annotations

import copy
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from src import demo_ledger as L
from tests.test_demo_ledger import MONDAY, NOW, add_position, evidence


D = Decimal
REAL_ENTRY_FILL = 1792501291999


def position(*, opened=MONDAY + 600_000, closed=MONDAY + 900_000):
    data = evidence()
    route, history = add_position(data, 1, opened, closed)
    for fill in data["fills"]:
        fill["ts"] = fill["fillTime"]
    return data, route, history


def split_fill(data, index, *, later_by=0):
    """Two independently billed executions, with unchanged order total/PnL."""
    first, first_bill = data["fills"][index], data["bills"][index]
    second, second_bill = copy.deepcopy(first), copy.deepcopy(first_bill)
    original_pnl = D(first["fillPnl"])
    for fill, bill, part in ((first, first_bill, D("0.4")),
                             (second, second_bill, D("0.6"))):
        pnl, fee = original_pnl * part, D("-0.1") * part
        fill.update(fillSz=str(part), fillPnl=str(pnl), fee=str(fee))
        bill.update(pnl=str(pnl), fee=str(fee), balChg=str(pnl + fee))
    second.update(tradeId=second["tradeId"] + "x", billId=second["billId"] + "x",
                  fillTime=str(int(second["fillTime"]) + later_by))
    second["ts"] = second["fillTime"]
    second_bill.update(tradeId=second["tradeId"], billId=second["billId"],
                       ts=second["fillTime"])
    data["fills"].append(second)
    data["bills"].append(second_bill)
    return first, second, first_bill, second_bill


class DemoLedgerExchangeTimestampTests(unittest.TestCase):
    def setUp(self):
        for name in ("socket.create_connection", "socket.socket.connect"):
            block = patch(name, side_effect=AssertionError("NO_NETWORK"))
            block.start()
            self.addCleanup(block.stop)

    def valid(self, data):
        result = L.validate_demo_ledger(**data)
        self.assertTrue(result["verified"], result["refusals"])
        return result

    def refused(self, data, code=None):
        result = L.validate_demo_ledger(**data)
        self.assertFalse(result["verified"])
        if code:
            self.assertEqual(result["refusals"], [code])
        for field in ("realized_pnl_today", "realized_pnl_week", "entries_today",
                      "consecutive_losses", "paused_until_ms"):
            self.assertIsNone(result[field])
        self.assertTrue(result["paused"])
        return result

    def test_real_one_ms_entry_exit_and_linked_bill_record_times(self):
        data, route, history = position(opened=REAL_ENTRY_FILL,
                                         closed=REAL_ENTRY_FILL + 60_000)
        now = REAL_ENTRY_FILL + 120_000
        day = int(datetime.fromtimestamp(now / 1000, timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        data.update(now_ms=now, day_start_ms=day)
        data["coverage"]["end_ms"] = now
        for fill, bill in zip(data["fills"], data["bills"]):
            fill["ts"] = bill["ts"] = str(int(fill["fillTime"]) + 1)
        route["position_opened_at_ms"] += 1
        history["cTime"] = str(route["position_opened_at_ms"])
        history["uTime"] = str(int(history["uTime"]) + 1)
        before = copy.deepcopy(data)
        result = self.valid(data)
        self.assertEqual(result["entries_today"], 1)
        self.assertEqual(result["realized_pnl_today"], D("-1.2"))
        completed = result["completed_positions"][0]
        self.assertEqual(completed["opened_at_ms"], REAL_ENTRY_FILL + 1)
        self.assertEqual(completed["closed_at_ms"], REAL_ENTRY_FILL + 60_001)
        self.assertEqual(data, before)

    def test_missing_record_time_preserves_original_exact_evidence(self):
        data, _, _ = position()
        for fill in data["fills"]:
            del fill["ts"]
        self.assertEqual(self.valid(data)["realized_pnl_today"], D("-1.2"))

    def test_missing_record_time_cannot_relax_entry_exit_or_bill_time(self):
        for changed in ("entry", "exit", "bill"):
            with self.subTest(changed=changed):
                data, route, history = position()
                for fill in data["fills"]:
                    del fill["ts"]
                if changed == "entry":
                    route["position_opened_at_ms"] += 1
                    history["cTime"] = str(route["position_opened_at_ms"])
                elif changed == "exit":
                    history["uTime"] = str(int(history["uTime"]) + 1)
                else:
                    data["bills"][0]["ts"] = str(int(data["bills"][0]["ts"]) + 1)
                self.refused(data)

    def test_present_invalid_record_time_refuses_even_exact_history_and_bill(self):
        stamp = MONDAY + 600_000
        bad_values = (None, False, True, "", "unknown", "NaN", "Infinity", "-1", "1.5", [], {},
                      float(stamp), D(stamp), str(stamp) + ".0", str(stamp) + "e0",
                      "+" + str(stamp), " " + str(stamp),
                      str(stamp).translate(str.maketrans("0123456789", "０１２３４５６７８９")))
        for index in (0, 1):
            for value in bad_values:
                with self.subTest(index=index, value=value):
                    data, _, _ = position()
                    data["fills"][index]["ts"] = value
                    self.refused(data, "LEDGER_FILL_RECORD_TIME_INVALID")

    def test_strict_integer_and_ascii_digit_record_times_are_accepted(self):
        data, _, _ = position()
        data["fills"][0]["ts"] = int(data["fills"][0]["fillTime"])
        self.valid(data)

    def test_record_before_execution_or_over_five_seconds_refuses_even_exact(self):
        for index in (0, 1):
            for delay in (-1, 5_001):
                with self.subTest(index=index, delay=delay):
                    data, _, _ = position()
                    fill = data["fills"][index]
                    fill["ts"] = str(int(fill["fillTime"]) + delay)
                    self.refused(data, "LEDGER_FILL_RECORD_TIME_INVALID")

    def test_closed_five_second_record_and_history_boundaries_are_accepted(self):
        data, route, history = position()
        for fill, bill in zip(data["fills"], data["bills"]):
            fill["ts"] = bill["ts"] = str(int(fill["fillTime"]) + 5_000)
        route["position_opened_at_ms"] += 5_000
        history["cTime"] = str(route["position_opened_at_ms"])
        history["uTime"] = str(int(history["uTime"]) + 5_000)
        self.assertEqual(self.valid(data)["entries_today"], 1)

    def test_record_after_observation_now_refuses_even_with_exact_history(self):
        data, _, _ = position(closed=NOW - 1)
        data["fills"][1]["ts"] = str(NOW + 1)
        self.refused(data, "LEDGER_FILL_RECORD_TIME_INVALID")

    def test_record_at_observation_now_is_accepted(self):
        data, _, history = position(closed=NOW - 1)
        data["fills"][1]["ts"] = data["bills"][1]["ts"] = str(NOW)
        history["uTime"] = str(NOW)
        self.valid(data)

    def test_history_open_cannot_precede_execution_or_follow_own_record(self):
        for offset in (-1, 2):
            with self.subTest(offset=offset):
                data, route, history = position()
                first = data["fills"][0]
                first["ts"] = str(int(first["fillTime"]) + 1)
                route["position_opened_at_ms"] += offset
                history["cTime"] = str(route["position_opened_at_ms"])
                self.refused(data)

    def test_history_close_cannot_precede_execution_or_follow_own_record(self):
        for offset in (-1, 2):
            with self.subTest(offset=offset):
                data, _, history = position()
                last = data["fills"][1]
                last["ts"] = str(int(last["fillTime"]) + 1)
                history["uTime"] = str(int(history["uTime"]) + offset)
                self.refused(data)

    def test_later_entry_record_cannot_expand_first_execution_boundary(self):
        data, route, history = position()
        first, second, _, _ = split_fill(data, 0, later_by=1)
        first["ts"] = str(int(first["fillTime"]) + 1)
        second["ts"] = str(int(first["fillTime"]) + 5)
        route["position_opened_at_ms"] += 2
        history["cTime"] = str(route["position_opened_at_ms"])
        self.refused(data, "LEDGER_LIFECYCLE_TIME_MISMATCH")

    def test_actual_first_execution_controls_open_not_every_later_record(self):
        data, route, history = position()
        first, second, _, _ = split_fill(data, 0, later_by=1)
        first["ts"] = str(int(first["fillTime"]) + 4)
        second["ts"] = str(int(second["fillTime"]) + 1)
        route["position_opened_at_ms"] += 3
        history["cTime"] = str(route["position_opened_at_ms"])
        self.assertEqual(self.valid(data)["entries_today"], 1)

    def test_earlier_exit_record_cannot_expand_last_execution_boundary(self):
        data, _, history = position()
        first, last, _, _ = split_fill(data, 1, later_by=1)
        first["ts"] = str(int(last["fillTime"]) + 5)
        last["ts"] = str(int(last["fillTime"]) + 1)
        history["uTime"] = str(int(last["fillTime"]) + 2)
        self.refused(data, "LEDGER_PARTIAL_POSITION_HISTORY")

    def test_tied_boundary_fills_require_intersection_not_longest_record(self):
        for index in (0, 1):
            with self.subTest(index=index):
                data, route, history = position()
                first, other, _, _ = split_fill(data, index)
                first["ts"] = str(int(first["fillTime"]) + 1)
                other["ts"] = str(int(other["fillTime"]) + 5)
                if index == 0:
                    route["position_opened_at_ms"] += 2
                    history["cTime"] = str(route["position_opened_at_ms"])
                else:
                    history["uTime"] = str(int(history["uTime"]) + 2)
                self.refused(data)

    def test_each_bill_is_confined_to_its_own_fill_interval(self):
        for index in (0, 1):
            for offset in (-1, 2):
                with self.subTest(index=index, offset=offset):
                    data, _, _ = position()
                    fill, bill = data["fills"][index], data["bills"][index]
                    fill["ts"] = str(int(fill["fillTime"]) + 1)
                    bill["ts"] = str(int(fill["fillTime"]) + offset)
                    self.refused(data, "LEDGER_TRADE_BILL_MISMATCH")

    def test_bill_inside_closed_fill_interval_is_accepted(self):
        data, _, _ = position()
        for fill, bill in zip(data["fills"], data["bills"]):
            fill["ts"] = str(int(fill["fillTime"]) + 2)
            bill["ts"] = str(int(fill["fillTime"]) + 1)
        self.valid(data)

    def test_other_fill_record_cannot_expand_bill_interval(self):
        data, _, _ = position()
        first, second, first_bill, _ = split_fill(data, 0, later_by=1)
        first["ts"] = str(int(first["fillTime"]) + 1)
        second["ts"] = str(int(first["fillTime"]) + 10)
        first_bill["ts"] = str(int(first["fillTime"]) + 2)
        self.refused(data, "LEDGER_TRADE_BILL_MISMATCH")

    def test_bill_identifiers_still_require_exact_independent_link(self):
        for field in ("billId", "tradeId", "ordId"):
            with self.subTest(field=field):
                data, _, _ = position()
                fill, bill = data["fills"][0], data["bills"][0]
                fill["ts"] = bill["ts"] = str(int(fill["fillTime"]) + 1)
                bill[field] += "foreign"
                self.refused(data, "LEDGER_TRADE_BILL_UNMATCHED")

    def test_bill_pnl_fee_and_balance_amounts_still_match_exactly(self):
        for field in ("pnl", "fee", "balChg"):
            with self.subTest(field=field):
                data, _, _ = position()
                fill, bill = data["fills"][1], data["bills"][1]
                fill["ts"] = bill["ts"] = str(int(fill["fillTime"]) + 1)
                bill[field] = str(D(bill[field]) + D("0.00000001"))
                self.refused(data, "LEDGER_TRADE_BILL_MISMATCH")

    def test_supplied_fill_position_id_is_an_additional_constraint(self):
        for index in (0, 1):
            with self.subTest(index=index):
                data, route, _ = position()
                data["fills"][index]["posId"] = route["position_id"]
                self.valid(data)
                data["fills"][index]["posId"] = "foreign"
                self.refused(data, "LEDGER_FILL_OWNERSHIP_UNVERIFIED")

    def test_wrong_route_or_history_position_id_cannot_be_adopted_by_time(self):
        for field in ("route", "history"):
            with self.subTest(field=field):
                data, route, history = position()
                if field == "route":
                    route["position_id"] = "foreign"
                else:
                    history["posId"] = "foreign"
                self.refused(data, "LEDGER_EXTERNAL_POSITION")

    def test_duplicate_record_conflict_and_external_fill_remain_refused(self):
        data, _, _ = position()
        duplicate = copy.deepcopy(data["fills"][0])
        duplicate["ts"] = str(int(duplicate["ts"]) + 1)
        data["fills"].append(duplicate)
        self.refused(data, "LEDGER_FILL_DUPLICATE_CONFLICT")
        data, _, _ = position()
        external = copy.deepcopy(data["fills"][0])
        external.update(tradeId="foreigntrade", billId="foreignbill", ordId="foreignorder")
        data["order_details"]["foreignorder"] = dict(
            data["order_details"][data["fills"][0]["ordId"]], ordId="foreignorder")
        data["fills"].append(external)
        self.refused(data, "LEDGER_FILL_OWNERSHIP_UNVERIFIED")

    def test_identical_repeated_fill_record_still_deduplicates(self):
        data, _, _ = position()
        data["fills"][0]["ts"] = str(int(data["fills"][0]["fillTime"]) + 1)
        data["fills"].append(copy.deepcopy(data["fills"][0]))
        self.assertEqual(self.valid(data)["realized_pnl_today"], D("-1.2"))

    def test_day_week_pnl_and_entry_counts_use_execution_not_record_time(self):
        data, route, history = position(opened=MONDAY - 1, closed=MONDAY + 60_000)
        entry, bill = data["fills"][0], data["bills"][0]
        entry["ts"] = bill["ts"] = str(MONDAY)
        route["position_opened_at_ms"] = MONDAY
        history["cTime"] = str(MONDAY)
        result = self.valid(data)
        self.assertEqual(result["entries_today"], 0)
        self.assertEqual(result["realized_pnl_today"], D("-1.1"))
        self.assertEqual(result["realized_pnl_week"], D("-1.1"))

    def test_delayed_history_open_cannot_hide_overlapping_actual_entry(self):
        prior_close = MONDAY + 900_000
        for same_position_id in (False, True):
            for offset in (-1, 0):
                with self.subTest(same_position_id=same_position_id, offset=offset):
                    data, first_route, _ = position(closed=prior_close)
                    route, history = add_position(
                        data, 2, prior_close + offset, prior_close + 300_000,
                        pos_id=first_route["position_id"] if same_position_id else None)
                    entry = data["fills"][-2]
                    entry["ts"] = str(prior_close + 1)
                    route["position_opened_at_ms"] = prior_close + 1
                    history["cTime"] = str(prior_close + 1)
                    # The individual history/record proofs are all valid, and
                    # history.cTime is after the previous close. Actual entry
                    # still overlaps its already-proved lifecycle boundary.
                    self.refused(data, "LEDGER_LIFECYCLE_OVERLAP")

    def test_next_entry_must_follow_previous_recorded_history_close(self):
        actual_exit = MONDAY + 900_000
        data, _, history = position(closed=actual_exit)
        prior_exit = data["fills"][1]
        prior_exit["ts"] = data["bills"][1]["ts"] = str(actual_exit + 2)
        history["uTime"] = str(actual_exit + 2)
        route, history = add_position(data, 2, actual_exit + 1, actual_exit + 300_000)
        entry = data["fills"][-2]
        entry["ts"] = str(actual_exit + 3)
        route["position_opened_at_ms"] = actual_exit + 3
        history["cTime"] = str(actual_exit + 3)
        # Even nonoverlapping execution timestamps cannot bypass the previous
        # position's later authenticated uTime boundary.
        self.refused(data, "LEDGER_LIFECYCLE_OVERLAP")

    def test_strictly_later_entry_with_delayed_record_remains_valid(self):
        prior_close = MONDAY + 900_000
        data, first_route, _ = position(closed=prior_close)
        route, history = add_position(data, 2, prior_close + 1, prior_close + 300_000,
                                      pos_id=first_route["position_id"])
        entry = data["fills"][-2]
        entry["ts"] = str(prior_close + 2)
        route["position_opened_at_ms"] = prior_close + 2
        history["cTime"] = str(prior_close + 2)
        result = self.valid(data)
        self.assertEqual(result["entries_today"], 2)
        self.assertEqual(len(result["completed_positions"]), 2)
        self.assertEqual(result["realized_pnl_today"], D("-2.4"))

    def test_coverage_start_close_cannot_hide_same_time_next_entry(self):
        data = evidence()
        start = data["coverage"]["start_ms"]
        first_route, _ = add_position(data, 1, start, start)
        route, history = add_position(data, 2, start, start + 600_000,
                                      pos_id=first_route["position_id"])
        entry = data["fills"][-2]
        entry["ts"] = str(start + 1)
        route["position_opened_at_ms"] = start + 1
        history["cTime"] = str(start + 1)
        self.refused(data, "LEDGER_LIFECYCLE_OVERLAP")

    def test_coverage_start_close_allows_strictly_later_actual_entry(self):
        data = evidence()
        start = data["coverage"]["start_ms"]
        first_route, _ = add_position(data, 1, start, start)
        route, history = add_position(data, 2, start + 1, start + 600_000,
                                      pos_id=first_route["position_id"])
        entry = data["fills"][-2]
        entry["ts"] = str(start + 2)
        route["position_opened_at_ms"] = start + 2
        history["cTime"] = str(start + 2)
        result = self.valid(data)
        self.assertEqual(len(result["completed_positions"]), 2)

    def test_loss_cooldown_uses_actual_history_close_without_rewriting(self):
        data = evidence()
        for number in (1, 2, 3):
            _, history = add_position(data, number, MONDAY + number * 600_000,
                                      MONDAY + number * 600_000 + 300_000)
            exit_fill = data["fills"][-1]
            exit_fill["ts"] = str(int(exit_fill["fillTime"]) + 1)
            history["uTime"] = exit_fill["ts"]
        before = copy.deepcopy(data)
        result = self.valid(data)
        self.assertEqual(result["consecutive_losses"], 3)
        self.assertEqual(result["paused_until_ms"], int(history["uTime"]) + L.DAY_MS)
        self.assertEqual(data, before)


if __name__ == "__main__":
    unittest.main()
