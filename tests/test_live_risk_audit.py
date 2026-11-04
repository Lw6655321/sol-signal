"""Diagnostic cannot acquire trading authority or leak response/credential text."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "audit_live_risk", Path(__file__).resolve().parents[1] / "scripts" / "audit_live_risk.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class AuditTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket", "socket.create_connection", "socket.getaddrinfo",
                       "urllib.request.urlopen"):
            self.enterContext(patch(target, side_effect=AssertionError("network forbidden")))

    def test_default_cli_never_collects_or_loads_account_config(self):
        with patch.object(audit, "collect_report", side_effect=AssertionError("no evidence access")), \
             patch.object(audit, "load_config", side_effect=AssertionError("no configuration access")):
            self.assertEqual(audit.main([]), 0)

    def test_missing_evidence_is_blocked_with_no_raw_exception(self):
        def unavailable(cfg):
            raise RuntimeError("sensitive-offline-marker")
        report = audit.collect_report({}, provider=unavailable)
        self.assertIn("EVIDENCE_UNAVAILABLE", report["blocking_reasons"])
        self.assertNotIn("sensitive-offline-marker", json.dumps(report))
        self.assertIs(report["release_ready"], False)

    def test_context_cannot_be_used_as_report_destination(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            context = Path(tmp) / "source"
            context.mkdir()
            original = context / "existing.txt"
            original.write_bytes(b"unchanged")
            for destination in (context, context / "logs" / "audit"):
                with self.subTest(destination=destination), \
                     patch.object(audit, "load_config", side_effect=AssertionError("no config access")), \
                     patch.object(audit, "read_only_evidence", side_effect=AssertionError("no account access")), \
                     contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
                    audit.main(["--read-only", "--context-root", str(context),
                                "--log-root", str(destination)])
                self.assertEqual(result.exception.code, 2)
            self.assertEqual(list(context.iterdir()), [original])
            self.assertEqual(original.read_bytes(), b"unchanged")

    def test_verified_data_alone_cannot_approve_release(self):
        state = dict(live_verified=True, week_start_equity="100", realized_pnl_week="0",
                     open_positions=[], pending_orders=[], protection_orders=[], refusal_reasons=[])
        report = audit.collect_report({}, provider=lambda cfg: state)
        self.assertEqual(report["blocking_reasons"], ["RELEASE_REVIEW_REQUIRED"])
        self.assertIs(report["release_ready"], False)
        self.assertEqual(report["orders_sent"], 0)
        self.assertIs(report["trading_state_changed"], False)

    def test_diagnostic_keeps_adapter_and_weekly_blockers(self):
        state = dict(live_verified=False, refusal_reasons=["ADAPTER_UNVERIFIED"],
                     equity_estimate="117", entries_today=0, open_positions=[], pending_orders=[])
        report = audit.collect_report({}, provider=lambda cfg: state)
        self.assertIn("ADAPTER_UNVERIFIED", report["blocking_reasons"])
        self.assertIn("WEEKLY_EVIDENCE_MISSING", report["blocking_reasons"])
        self.assertEqual(report["account"]["equity_estimate"], "117")

    def test_reports_never_copy_unapproved_provider_fields(self):
        state = dict(live_verified=False, refusal_reasons=[], raw_headers="sensitive-marker",
                     credentials="sensitive-marker", open_positions=[], pending_orders=[])
        report = audit.collect_report({}, provider=lambda cfg: state)
        self.assertNotIn("sensitive-marker", json.dumps(report))

    def test_untrusted_refusal_text_is_not_serialized(self):
        state = dict(live_verified=False, refusal_reasons=["raw confidential response"],
                     open_positions=[], pending_orders=[])
        report = audit.collect_report({}, provider=lambda cfg: state)
        self.assertNotIn("raw confidential response", json.dumps(report))
        self.assertIn("EVIDENCE_UNAVAILABLE", report["blocking_reasons"])

    def test_independent_run_reports_are_never_overwritten(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            report = audit.collect_report({}, provider=lambda cfg: {})
            first = audit.save_report(report, tmp)
            second = audit.save_report(report, tmp)
            self.assertNotEqual(first.parent, second.parent)
            self.assertEqual(json.loads(first.read_text(encoding="utf-8"))["orders_sent"], 0)

    def test_collection_error_codes_are_useful_but_raw_responses_are_excluded(self):
        state = dict(live_verified=False, refusal_reasons=["FILLS_INCOMPLETE"],
                     open_positions=[], pending_orders=[],
                     evidence={"fills_errors": ["PAGE_LIMIT"],
                               "funding_errors": ["raw confidential response"]})
        report = audit.collect_report({}, provider=lambda cfg: state)
        self.assertEqual(report["collection_errors"]["fills_errors"], ["PAGE_LIMIT"])
        self.assertEqual(report["collection_errors"]["funding_errors"], ["INVALID_DIAGNOSTIC_CODES"])
        self.assertNotIn("raw confidential response", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
