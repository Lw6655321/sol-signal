"""Official SWAP history empty settlement field; pure ledger evidence only."""
from copy import deepcopy
from decimal import Decimal
import unittest

from src import demo_ledger as L
from test_demo_ledger import evidence, add_position, MONDAY


class DemoLedgerHistorySchemaTests(unittest.TestCase):
    def data(self):
        data = evidence()
        _, history = add_position(data, 1, MONDAY + 600_000, MONDAY + 900_000)
        return data, history

    def test_inapplicable_blank_settlement_preserves_raw_evidence_and_exact_pnl(self):
        data, history = self.data()
        history["settledPnl"] = ""
        before = deepcopy(data)
        result = L.validate_demo_ledger(**data)
        self.assertTrue(result["verified"], result["refusals"])
        self.assertEqual(result["realized_pnl_today"], Decimal("-1.2"))
        self.assertEqual(data, before)
        self.assertEqual(history["settledPnl"], "")

    def test_legacy_missing_and_numeric_zero_keep_their_existing_meaning(self):
        for value in (None, "0", "0.00", "-0", 0):
            with self.subTest(value=value):
                data, history = self.data()
                if value is not None:
                    history["settledPnl"] = value
                result = L.validate_demo_ledger(**data)
                self.assertTrue(result["verified"], result["refusals"])

    def test_wrong_history_scope_cannot_use_blank_compatibility(self):
        for field, value in (("instType", "FUTURES"), ("instId", "BTC-USDT-SWAP"),
                             ("posSide", "long"), ("mgnMode", "isolated")):
            with self.subTest(field=field):
                data, history = self.data()
                history.update(settledPnl="")
                history[field] = value
                result = L.validate_demo_ledger(**data)
                self.assertFalse(result["verified"])
                self.assertIn("LEDGER_POSITION_SCOPE", result["refusals"])

    def test_unknown_or_malformed_settlement_is_still_unknown(self):
        for value in (None, True, False, " ", "bad", "NaN", "Infinity", "-Infinity", [], {}):
            with self.subTest(value=value):
                data, history = self.data()
                history["settledPnl"] = value
                result = L.validate_demo_ledger(**data)
                self.assertFalse(result["verified"])
                self.assertIn("LEDGER_NUMBER_INVALID", result["refusals"])
                self.assertIsNone(result["realized_pnl_today"])

    def test_nonzero_settlement_and_liquidation_still_refuse(self):
        for field, value in (("settledPnl", "1"), ("settledPnl", "-1"),
                             ("liqPenalty", "1"), ("liqPenalty", "")):
            with self.subTest(field=field, value=value):
                data, history = self.data()
                history["settledPnl"] = ""
                history[field] = value
                result = L.validate_demo_ledger(**data)
                self.assertFalse(result["verified"])
                self.assertIsNone(result["realized_pnl_today"])

    def test_blank_required_numbers_are_never_filled_in(self):
        for field in ("openMaxPos", "closeTotalPos", "pnl", "fee", "fundingFee", "realizedPnl"):
            with self.subTest(field=field):
                data, history = self.data()
                history["settledPnl"] = ""
                history[field] = ""
                result = L.validate_demo_ledger(**data)
                self.assertFalse(result["verified"])
                self.assertIsNone(result["realized_pnl_today"])

    def test_general_number_parser_still_refuses_blank(self):
        with self.assertRaises(L.LedgerRefused) as refused:
            L._number("")
        self.assertEqual(refused.exception.code, "LEDGER_NUMBER_INVALID")
