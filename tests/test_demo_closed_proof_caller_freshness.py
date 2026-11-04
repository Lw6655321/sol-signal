"""The receiver's complete call interval, not a later server split, ages proof."""
from copy import deepcopy
import unittest
from unittest.mock import patch

import test_demo_halted_exit_receipts as fixtures


class ClosedProofCallerFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.ClosedHaltedReceipts()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.proof = self.helper.verify()
        self.router = self.helper.router()
        self.original = self.helper.route(self.router)
        self.helper.gets.clear()

    def reconcile(self, wall_elapsed, mono_elapsed, *, source_offset=1000, first_mono=1_000_000_000):
        proof = deepcopy(self.proof)
        proof['observed_at_ms'] = fixtures.NOW + source_offset
        calls = 0

        def clock():
            nonlocal calls
            calls += 1
            return fixtures.NOW if calls == 1 else fixtures.NOW + wall_elapsed

        self.router.clock = clock
        with patch.object(self.helper.transport, 'verify_closed_position_lifecycle', return_value=proof) as verify, \
                patch('src.order_router.time.monotonic_ns', side_effect=[first_mono, first_mono + mono_elapsed]):
            result = self.router.reconcile_halted_exit(self.helper.intent)
        return result, verify

    def assert_refused_without_success_metadata(self, result, code):
        self.assertEqual((result.status, result.code), ('HALTED', 'OBSERVE_HALTED'))
        self.assertEqual(result.emergency_status, 'EXIT_RECONCILIATION_UNVERIFIED')
        row = self.helper.route(self.router)
        self.assertEqual(row['exit_reconcile_code'], code)
        self.assertFalse(row.get('exit_reconciled', False))
        self.assertFalse(any(key.startswith('exit_reconcile_collection_') for key in row))
        for key, value in self.original.items():
            self.assertEqual(row[key], value)
        self.helper.assert_halt(self.router)
        self.assertEqual(self.helper.gets, [])

    def test_later_server_start_cannot_hide_early_exposure_and_receiver_delay(self):
        # Earliest GET t0, server start t1, transport return t4.9, receiver
        # scheduled at t5.7: old source-only guard saw 4.7 and could accept.
        result, verify = self.reconcile(5700, 5_700_000_000)
        verify.assert_called_once()
        self.assertLessEqual((fixtures.NOW + 5700) - (fixtures.NOW + 1000), 5000)
        self.assert_refused_without_success_metadata(result, 'DEMO_HALTED_EXIT_CALLER_WALL_STALE')

    def test_monotonic_call_age_over_five_seconds_refuses_stalled_wall(self):
        result, _ = self.reconcile(4900, 5_700_000_000)
        self.assert_refused_without_success_metadata(result, 'DEMO_HALTED_EXIT_CALLER_MONO_STALE')

    def test_wall_age_over_five_seconds_refuses_short_monotonic_interval(self):
        result, _ = self.reconcile(5700, 4_900_000_000)
        self.assert_refused_without_success_metadata(result, 'DEMO_HALTED_EXIT_CALLER_WALL_STALE')

    def test_wall_rollback_and_monotonic_rollback_both_refuse(self):
        for wall, mono in ((-1, 0), (0, -1)):
            with self.subTest(wall=wall, mono=mono):
                result, _ = self.reconcile(wall, mono, source_offset=-100)
                code = 'DEMO_HALTED_EXIT_CALLER_WALL_STALE' if wall < 0 else 'DEMO_HALTED_EXIT_CALLER_MONO_STALE'
                self.assert_refused_without_success_metadata(result, code)

    def test_exact_five_second_caller_boundary_retains_real_source_time(self):
        result, _ = self.reconcile(5000, 5_000_000_000, source_offset=0)
        self.assertEqual(result.emergency_status, 'FLAT_EXIT_RECONCILED_HALTED')
        row = self.helper.route(self.router)
        self.assertEqual(row['exit_reconciled_observed_at_ms'], fixtures.NOW)
        self.assertEqual(row['exit_reconcile_collection_started_at_ms'], fixtures.NOW)
        self.assertEqual(row['exit_reconcile_collection_completed_at_ms'], fixtures.NOW + 5000)
        self.assertEqual(row['exit_reconcile_collection_elapsed_monotonic_ns'], 5_000_000_000)
        self.helper.assert_halt(self.router)

    def test_original_server_source_age_and_future_guard_are_still_required(self):
        for source in (-5001, 1):
            with self.subTest(source=source):
                result, _ = self.reconcile(0, 0, source_offset=source)
                self.assert_refused_without_success_metadata(result, 'DEMO_HALTED_EXIT_SOURCE_TIME_STALE')

    def test_illegal_monotonic_start_refuses_before_transport(self):
        for value in (True, -1, 1.0, '1'):
            with self.subTest(value=value), \
                    patch.object(self.helper.transport, 'verify_closed_position_lifecycle') as verify, \
                    patch('src.order_router.time.monotonic_ns', return_value=value):
                result = self.router.reconcile_halted_exit(self.helper.intent)
                verify.assert_not_called()
                self.assert_refused_without_success_metadata(result, 'DEMO_HALTED_EXIT_START_MONO_INVALID')


if __name__ == '__main__':
    unittest.main()
