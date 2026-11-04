"""Offline lifecycle proofs distinguish execution from record-generation time."""
from copy import deepcopy
from decimal import Decimal
import json
import sqlite3
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from src.order_router import RouterError
from test_execution import NOW
import test_demo_route_transport as transport_fixture
import test_demo_router_integration as router_fixture


class LifecycleExchangeTimestampTests(unittest.TestCase):
    setUp = transport_fixture.DemoRouteTests.setUp
    make = transport_fixture.DemoRouteTests.make
    rejected = transport_fixture.DemoRouteTests.rejected
    lifecycle = transport_fixture.DemoRouteTests.lifecycle
    verify_lifecycle = transport_fixture.DemoRouteTests.verify_lifecycle

    def delayed_record(self, difference=1):
        self.lifecycle()
        self.life_fills[0]["ts"] = str(self.opened_ms + difference)
        self.life_position["cTime"] = str(self.opened_ms + difference)

    def second_fill(self, execution_offset, record_offset):
        original = Decimal(self.life_fills[0]["fillSz"])
        self.life_fills[0]["fillSz"] = "0.01"
        self.life_fills.append(dict(self.life_fills[0], tradeId="trade002", billId="bill002",
                                   fillSz=str(original - Decimal("0.01")),
                                   fillTime=str(self.opened_ms + execution_offset),
                                   ts=str(self.opened_ms + record_offset)))

    def test_execution_one_ms_before_position_creation_returns_real_creation_time(self):
        self.delayed_record()
        originals = deepcopy((self.life_fills, self.life_position))
        proof = self.verify_lifecycle()
        self.assertTrue(proof["position_binding_verified"])
        self.assertEqual(proof["position_opened_at_ms"], self.opened_ms + 1)
        self.assertNotEqual(proof["position_opened_at_ms"], int(proof["fills"][0]["fillTime"]))
        self.assertEqual((self.life_fills, self.life_position), originals)
        self.assertEqual(self.wire.posts, [])

    def test_exact_old_fixture_without_record_time_remains_supported(self):
        self.lifecycle()
        self.assertEqual(self.verify_lifecycle()["position_opened_at_ms"], self.opened_ms)
        self.life_position["cTime"] = str(self.opened_ms + 1)
        self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)

    def test_record_time_closed_interval_does_not_authorize_adjacent_creation(self):
        self.delayed_record(3)
        for opened in (self.opened_ms, self.opened_ms + 3):
            self.life_position["cTime"] = str(opened)
            self.assertEqual(self.verify_lifecycle()["position_opened_at_ms"], opened)
        for opened in (self.opened_ms - 1, self.opened_ms + 4):
            self.life_position["cTime"] = str(opened)
            self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)

    def test_invalid_present_record_time_is_rejected_even_for_exact_creation(self):
        for value in (None, 0, True, float(NOW), str(NOW) + ".0", "１２３", "NaN"):
            with self.subTest(value=value):
                self.lifecycle()
                self.life_fills[0]["ts"] = value
                self.rejected("DEMO_LIFECYCLE_TIME_INVALID", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])

    def test_integral_record_timestamp_is_accepted_without_rewriting_source(self):
        self.delayed_record()
        self.life_fills[0]["ts"] = self.opened_ms + 1
        self.assertEqual(self.verify_lifecycle()["fills"][0]["ts"], self.opened_ms + 1)

    def test_record_time_before_execution_or_after_final_server_time_is_rejected(self):
        for offset in (-1, 501):
            with self.subTest(offset=offset):
                self.lifecycle()
                self.life_fills[0]["ts"] = str(self.opened_ms + offset)
                self.rejected("DEMO_LIFECYCLE_TIME_INVALID", self.verify_lifecycle)

    def test_record_delay_is_bounded_at_five_seconds_without_changing_read_deadline(self):
        for delay in (5000, 5001):
            with self.subTest(delay=delay):
                self.delayed_record(delay)
                self.server_ms = self.opened_ms + delay
                self.wire.rows["/api/v5/public/time"] = [{"ts": str(self.server_ms)}]
                if delay == 5000:
                    self.assertEqual(self.verify_lifecycle()["position_opened_at_ms"], self.server_ms)
                else:
                    self.rejected("DEMO_LIFECYCLE_TIME_INVALID", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])

    def test_later_execution_record_cannot_widen_first_execution_interval(self):
        self.delayed_record()
        self.second_fill(execution_offset=2, record_offset=100)
        self.life_position["cTime"] = str(self.opened_ms + 3)
        self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)
        self.life_position["cTime"] = str(self.opened_ms + 1)
        self.assertTrue(self.verify_lifecycle()["position_binding_verified"])

    def test_tied_first_executions_use_the_earliest_corresponding_record(self):
        self.delayed_record()
        self.second_fill(execution_offset=0, record_offset=2)
        self.life_position["cTime"] = str(self.opened_ms + 2)
        self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)
        self.life_position["cTime"] = str(self.opened_ms + 1)
        self.assertTrue(self.verify_lifecycle()["position_binding_verified"])

    def test_missing_later_record_keeps_only_exact_legacy_creation_supported(self):
        self.delayed_record()
        self.second_fill(execution_offset=2, record_offset=3)
        del self.life_fills[1]["ts"]
        self.rejected("DEMO_LIFECYCLE_POSITION_MISMATCH", self.verify_lifecycle)
        self.life_position["cTime"] = str(self.opened_ms)
        self.assertTrue(self.verify_lifecycle()["position_binding_verified"])

    def test_creation_time_change_between_exposure_waves_is_not_hidden_by_interval(self):
        self.delayed_record(3)
        reads = 0

        def positions(_params):
            nonlocal reads
            reads += 1
            return [dict(self.life_position, cTime=str(self.opened_ms + reads))]

        self.wire.rows["/api/v5/account/positions"] = positions
        self.rejected("DEMO_LIFECYCLE_POSITION_CHANGED", self.verify_lifecycle)
        self.assertEqual(self.wire.posts, [])

    def test_first_wave_record_time_can_follow_initial_server_cutoff(self):
        self.delayed_record()
        server_times = iter((self.opened_ms, self.opened_ms + 2))
        self.wire.rows["/api/v5/public/time"] = lambda _p: [{"ts": str(next(server_times))}]
        proof = self.verify_lifecycle()
        self.assertEqual(proof["position_opened_at_ms"], self.opened_ms + 1)
        self.assertEqual(proof["history_through_ms"], self.opened_ms + 2)
        queries = [call for call in self.wire.gets
                   if urlsplit(call[0]).path.endswith("/fills-history")]
        self.assertEqual(len(queries), 2)
        self.assertLessEqual(len(self.wire.gets), 25)
        self.assertEqual(self.wire.posts, [])


class _DelayedRecordExchange(router_fixture.Exchange):
    def get(self, url, headers, timeout):
        status, payload = super().get(url, headers, timeout)
        if urlsplit(url).path.endswith("/fills-history"):
            for row in payload["data"]:
                row["fillTime"] = str(NOW - 1)
                row["ts"] = str(NOW)
        return status, payload


class LifecycleTimestampRouterTests(unittest.TestCase):
    def setUp(self):
        with patch.object(router_fixture, "Exchange", _DelayedRecordExchange):
            router_fixture.DemoRouterIntegration.setUp(self)
        self.state["observed_at_ms"] = NOW - 2000

    def route_row(self):
        with sqlite3.connect(self.router.journal_path) as db:
            return json.loads(db.execute("SELECT data FROM routes").fetchone()[0])

    def test_real_router_binds_exchange_ctime_and_reconciliation_adds_no_post(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        posts = len(self.exchange.posts)
        self.assertEqual(self.router.reconcile_exit(self.intent).status, "PROTECTED")
        row = self.route_row()
        self.assertEqual(row["position_opened_at_ms"], NOW)
        self.assertEqual(row["position_id"], "position1")
        self.assertEqual(len(self.exchange.posts), posts)

    def test_real_router_caller_still_requires_exact_observed_position_identity(self):
        self.assertEqual(self.router.execute(self.intent).status, "PROTECTED")
        posts = len(self.exchange.posts)
        for position_id, opened in (("position1", NOW - 1), ("foreignposition", NOW)):
            with self.subTest(position_id=position_id, opened=opened):
                with self.assertRaises(RouterError) as caught:
                    self.router._prove_demo_lifecycle(self.intent, self.route_row(), position_id, opened)
                self.assertEqual(caught.exception.code, "POSITION_LIFECYCLE_UNPROVEN")
        self.assertEqual(len(self.exchange.posts), posts)


if __name__ == "__main__":
    unittest.main()
