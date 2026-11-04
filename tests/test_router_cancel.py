"""Regression tests for cancellation/fill races and native state provenance."""
from dataclasses import replace
from decimal import Decimal
import unittest

import test_order_router as fixtures
from src.order_router import RouterError


class CancelReconcileTests(unittest.TestCase):
    setUp = fixtures.RouterTests.setUp
    router = fixtures.RouterTests.router

    def pending(self):
        self.transport.entry_snapshots = [("live", Decimal("0"))]
        router = self.router()
        self.assertEqual(router.execute(self.intent).status, "PENDING")
        return router

    def test_cancel_unknown_intent_never_submits_or_reserves(self):
        router = self.router()
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual((result.status, result.code), ("REJECTED", "CANCEL_NOT_FOUND"))
        self.assertFalse(self.transport.calls)
        with router._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM routes").fetchone()[0], 0)

    def test_cancel_before_expiry_waits_for_confirmed_terminal(self):
        router = self.pending()
        self.transport.entry_snapshots = [("live", Decimal("0")), ("canceled", Decimal("0"))]
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual((result.status, result.code), ("CANCELED", "CANCELED"))
        self.assertEqual(len(self.transport.named("submit")), 1)
        self.assertEqual(len(self.transport.named("cancel")), 1)
        self.assertFalse(self.transport.named("close"))

    def test_cancel_racing_full_fill_is_protected(self):
        router = self.pending()
        self.transport.entry_snapshots = [("live", Decimal("0")), ("filled", self.intent.qty)]
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual(result.status, "PROTECTED")
        self.assertEqual(result.filled_qty, self.intent.qty)
        self.assertEqual(result.protected_qty, self.intent.qty)
        self.assertEqual(len(self.transport.named("submit")), 1)

    def test_partial_fill_is_protected_before_cancel_and_race_resize(self):
        router = self.pending()
        partial = self.intent.lot_size
        self.transport.entry_snapshots = [("partially_filled", partial), ("filled", self.intent.qty)]
        result = router.cancel_and_reconcile(self.intent)
        names = [call[0] for call in self.transport.calls]
        self.assertLess(names.index("protect"), names.index("cancel"))
        self.assertEqual(result.status, "PROTECTED")
        self.assertEqual(result.protected_qty, self.intent.qty)
        self.assertEqual([call[1] for call in self.transport.named("protect")], [partial, self.intent.qty])

    def test_cancel_rejection_is_not_reported_as_canceled(self):
        router = self.pending()
        self.transport.cancel_ack = {"sCode": "51603"}
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual((result.status, result.code), ("UNKNOWN", "CANCEL_UNKNOWN"))
        self.assertNotEqual(result.emergency_status, "FLAT_CONFIRMED")
        self.assertTrue(router.halt_path.exists())
        self.assertEqual(len(self.transport.named("cancel")), 1)

    def test_cancel_fill_protection_failure_confirms_emergency_close(self):
        router = self.pending()
        self.transport.entry_snapshots = [("filled", self.intent.qty)]
        self.transport.protection_overrides = {"reduceOnly": "false"}
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual(result.status, "HALTED")
        self.assertEqual(result.code, "PROTECTION_FAILED")
        self.assertEqual(result.emergency_status, "FLAT_CONFIRMED")
        self.assertEqual(len(self.transport.named("close")), 1)

    def test_cancel_existing_entry_does_not_depend_on_new_entry_providers(self):
        self.pending()
        self.transport.entry_snapshots = [("canceled", Decimal("0"))]
        def unavailable():
            raise AssertionError("New-entry state must not block safety reconciliation")
        router = self.router(state_provider=unavailable, market_provider=unavailable)
        result = router.cancel_and_reconcile(self.intent)
        self.assertEqual(result.status, "CANCELED")

    def test_canceled_signal_identity_is_retained_across_instances(self):
        router = self.pending()
        self.transport.entry_snapshots = [("canceled", Decimal("0"))]
        router.cancel_and_reconcile(self.intent)
        result = self.router().execute(self.intent)
        self.assertEqual(result.status, "CANCELED")
        self.assertEqual(len(self.transport.named("submit")), 1)
        changed = self.router().execute(replace(self.intent, tp2=self.intent.tp2 + 1))
        self.assertEqual(changed.code, "IDEMPOTENCY_CONFLICT")

    def test_native_preflight_requires_verified_fresh_account_state(self):
        router = self.router()
        router.mode = "native-live"
        for fields, expected in (({}, "LIVE_STATE_UNVERIFIED"),
                                 ({"live_verified": False, "observed_at_ms": fixtures.NOW}, "LIVE_STATE_UNVERIFIED"),
                                 ({"live_verified": True, "observed_at_ms": fixtures.NOW - 5001}, "LIVE_STATE_STALE"),
                                 ({"live_verified": True, "observed_at_ms": fixtures.NOW + 1}, "LIVE_STATE_STALE")):
            with self.subTest(fields=fields):
                self.state.pop("live_verified", None)
                self.state.pop("observed_at_ms", None)
                self.state.update(fields)
                with self.assertRaises(RouterError) as caught:
                    router._preflight(self.intent)
                self.assertEqual(caught.exception.code, expected)
        self.state.update(live_verified=True, observed_at_ms=fixtures.NOW)
        router._preflight(self.intent)


if __name__ == "__main__":
    unittest.main()
