# -*- coding: utf-8 -*-
"""live_state v2 的离线桩测试（隔离副本；无网络、无凭据、无写接口）。

覆盖 Codex 复核要求：
  - 默认采集器 ADAPTER_UNVERIFIED 拒绝；
  - fills 盈亏字段 fillPnl（不是 pnl）、after=billId 分页、tradeId 去重、覆盖完整性；
  - 归属必须来自 journal known_orders，字符串样式不算 bot；
  - 基线必须是 UTC 00:00–00:15 + 可信来源；
  - 拒绝未来时间 / NaN / Inf / 非布尔宏观；宏观缺失不得当 False；
  - 资金费币种/品种归属 + 分页完整；
  - 正常路径数字与 canonical 列表。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from src import live_state as L  # noqa: E402


def _ms(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp() * 1000)


NOW = _ms(2026, 10, 18, 15, 50, 0)
DAY_START = _ms(2026, 10, 18)
ENTRY_ID = "h" + "a" * 31
EXIT_ID = "FLAT1799999999999"
PROTECT_ID = "r" + "b" * 31


def _fill_row(identity="T1", bill_id="1000", ts=NOW, **overrides):
    row = {"tradeId": identity, "billId": bill_id, "instId": L.INST,
           "fillTime": str(ts), "fillPnl": "0", "fee": "-0.01",
           "feeCcy": "USDT", "clOrdId": ENTRY_ID}
    row.update(overrides)
    return row


def _funding_row(identity="1000", ts=NOW, **overrides):
    row = {"billId": identity, "ts": str(ts), "instId": L.INST,
           "type": "8", "ccy": "USDT", "balChg": "-0.03"}
    row.update(overrides)
    return row


def _account(equity: float = 117.72, fetched_at: int = NOW,
             positions=(), pending=(), protections=()) -> L.AccountSnapshot:
    return L.AccountSnapshot(fetched_at_ms=fetched_at, equity=equity,
                             positions=tuple(positions), pending_orders=tuple(pending),
                             protection_orders=tuple(protections))


def _fills(fills=(), complete: bool = True, fetched_at: int = NOW,
           coverage_from: int = DAY_START, coverage_to: int = NOW) -> L.FillsEvidence:
    return L.FillsEvidence(fetched_at_ms=fetched_at, fills=tuple(fills),
                           coverage_from_ms=coverage_from, coverage_to_ms=coverage_to,
                           complete=complete)


def _funding(bills=(), complete: bool = True, fetched_at: int = NOW) -> L.FundingEvidence:
    return L.FundingEvidence(fetched_at_ms=fetched_at, bills=tuple(bills),
                             coverage_from_ms=DAY_START, coverage_to_ms=NOW,
                             complete=complete)


def _baseline(**overrides) -> L.Baseline:
    base = L.Baseline(date="2026-10-18", equity=100.0,
                      observed_at_ms=DAY_START + 60_000, source="t4_reconcile")
    return replace(base, **overrides) if overrides else base


def _evidence(**overrides) -> L.Evidence:
    default = L.Evidence(
        clock_ms=NOW, server_time_ms=NOW, fetched_at_ms=NOW,
        account=_account(), fills=_fills(), funding=_funding(),
        macro=L.MacroState(observed_at_ms=NOW - 60_000, blackout=False, source="state/latest.json"),
        baseline=_baseline(), known_orders={}, halted=False, adapter_verified=True)
    return replace(default, **overrides) if overrides else default


def _fill(ts_ms: int, cl_ord_id: str = "", algo_cl_ord_id: str = "",
          pnl: float = 0.0, fee: float = 0.0, trade_id: str = "t1",
          inst_id: str = "SOL-USDT-SWAP") -> L.Fill:
    return L.Fill(ts_ms=ts_ms, trade_id=trade_id, cl_ord_id=cl_ord_id,
                  algo_cl_ord_id=algo_cl_ord_id, inst_id=inst_id, pnl=pnl, fee=fee)


def _bill(ts_ms: int, amount: float = 0.05, bill_id: str = "b1",
          ccy: str = "USDT", inst_id: str = "") -> L.FundingBill:
    return L.FundingBill(ts_ms=ts_ms, bill_id=bill_id, amount=amount, ccy=ccy, inst_id=inst_id)


class LiveStateVerifiedTests(unittest.TestCase):
    def test_happy_path_verified_and_numbers(self):
        ev = _evidence(
            known_orders={ENTRY_ID: "bot_entry", EXIT_ID: "bot_exit"},
            fills=_fills(fills=[
                _fill(NOW - 3_600_000, cl_ord_id=ENTRY_ID, fee=-0.10, trade_id="t1"),
                _fill(NOW - 1_000_000, cl_ord_id=EXIT_ID, pnl=5.0, fee=-0.20, trade_id="t2"),
            ]),
            funding=_funding(bills=[_bill(NOW - 500_000, 0.05)]),
        )
        out = L.live_state_provider({}, ev)
        self.assertTrue(out["live_verified"], out["refusal_reasons"])
        self.assertAlmostEqual(out["equity_estimate"], 117.72)
        self.assertAlmostEqual(out["day_start_equity"], 100.0)
        self.assertAlmostEqual(out["realized_pnl_today"], 4.75)
        self.assertEqual(out["entries_today"], 1)
        self.assertTrue(out["data_fresh"])
        self.assertFalse(out["macro_blackout"])

    def test_positions_orders_protection_are_canonical_lists(self):
        pos = L.Position("SOL-USDT-SWAP", "net", 2.02, 102.91, 8.0, "cross")
        order = L.OrderRef("SOL-USDT-SWAP", "sell", 2.02, 102.91, "live",
                           ENTRY_ID, "4021", "post_only", "net")
        protect = L.OrderRef("SOL-USDT-SWAP", "sell", 2.02, 0.0, "live",
                             PROTECT_ID, "4022", "oco", "net")
        ev = _evidence(known_orders={ENTRY_ID: "bot_entry", PROTECT_ID: "bot_exit"},
                       account=_account(positions=[pos], pending=[order], protections=[protect]))
        out = L.live_state_provider({}, ev)
        self.assertTrue(out["live_verified"], out["refusal_reasons"])
        self.assertEqual(out["open_positions"][0]["qty"], 2.02)
        self.assertEqual(out["protection_orders"][0]["ord_type"], "oco")


class LiveStateRefusalTests(unittest.TestCase):
    def assertRefused(self, out, code: str):
        self.assertFalse(out["live_verified"])
        self.assertIn(code, out["refusal_reasons"])

    def test_default_adapter_unverified_refuses(self):
        import unittest.mock as mock
        unverified = _evidence(adapter_verified=False)
        with mock.patch.object(L, "read_only_evidence", return_value=unverified):
            out = L.live_state_provider({}, None)
        self.assertRefused(out, "ADAPTER_UNVERIFIED")

    def test_evidence_read_failure_refuses(self):
        import unittest.mock as mock
        with mock.patch.object(L, "read_only_evidence", side_effect=RuntimeError("offline")):
            out = L.live_state_provider({}, None)
        self.assertRefused(out, "EVIDENCE_UNAVAILABLE")

    def test_missing_baseline_refuses(self):
        self.assertRefused(L.live_state_provider({}, _evidence(baseline=None)), "BASELINE_MISSING")

    def test_baseline_not_near_day_start_refuses(self):
        ev = _evidence(baseline=_baseline(observed_at_ms=DAY_START + 20 * 60_000))
        self.assertRefused(L.live_state_provider({}, ev), "BASELINE_NOT_NEAR_DAY_START")

    def test_baseline_untrusted_source_refuses(self):
        ev = _evidence(baseline=_baseline(source="arbitrary-source"))
        self.assertRefused(L.live_state_provider({}, ev), "BASELINE_UNTRUSTED")

    def test_baseline_wrong_day_refuses(self):
        self.assertRefused(L.live_state_provider({}, _evidence(baseline=_baseline(date="2026-10-17"))),
                           "BASELINE_STALE")

    def test_fillpnl_is_not_pnl_field(self):
        # fillPnl is required; the unrelated pnl field cannot substitute for it.
        fills = L._parse_fills([_fill_row("1", fillPnl="3.5", pnl="999", fee="-0.1")])
        self.assertAlmostEqual(fills[0].pnl, 3.5)
        self.assertAlmostEqual(fills[0].fee, -0.1)

    def test_parse_fills_dedupes_by_trade_id(self):
        rows = [_fill_row(fillPnl="2"), _fill_row(fillPnl="2")]
        self.assertEqual(len(L._parse_fills(rows)), 1)

    def test_collect_fills_uses_after_billid_and_marks_complete(self):
        calls = []

        def gateway(method, path, params=None):
            calls.append(dict(params or {}))
            page = calls[-1].get("after")
            if page is None:
                return [_fill_row(f"T{i}", str(1000 - i)) for i in range(100)]
            return [_fill_row("T999", "500", DAY_START)]
        ev = L._collect_fills(gateway, DAY_START, NOW)
        self.assertIn("after", calls[1])          # 必须用 after=billId 翻页
        self.assertTrue(ev.complete)
        self.assertLessEqual(ev.coverage_from_ms, DAY_START)

    def test_collect_fills_incomplete_when_page_cap_without_after(self):
        def gateway(method, path, params=None):
            return [{"tradeId": f"T{i}", "fillPnl": "0", "fillTime": str(NOW),
                     "clOrdId": ENTRY_ID} for i in range(100)]   # 无 billId，无法翻页
        ev = L._collect_fills(gateway, DAY_START, NOW)
        self.assertFalse(ev.complete)

    def test_incomplete_fills_refuse(self):
        self.assertRefused(L.live_state_provider({}, _evidence(fills=_fills(complete=False))),
                           "FILLS_INCOMPLETE")

    def test_unmapped_fill_refuses_even_if_looks_like_bot_id(self):
        ev = _evidence(known_orders={}, fills=_fills(fills=[
            _fill(NOW - 1000, cl_ord_id=ENTRY_ID, fee=-0.01)]))
        self.assertRefused(L.live_state_provider({}, ev), "ATTRIBUTION_UNVERIFIED")

    def test_external_mapping_refuses(self):
        ev = _evidence(known_orders={ENTRY_ID: "external"}, fills=_fills(fills=[
            _fill(NOW - 1000, cl_ord_id=ENTRY_ID, fee=-0.01)]))
        self.assertRefused(L.live_state_provider({}, ev), "UNKNOWN_EXTERNAL_TRADE")

    def test_funding_foreign_ccy_refuses(self):
        ev = _evidence(known_orders={}, funding=_funding(bills=[_bill(NOW - 1000, ccy="BTC")]))
        self.assertRefused(L.live_state_provider({}, ev), "FUNDING_ATTRIBUTION_UNVERIFIED")

    def test_funding_incomplete_refuses(self):
        self.assertRefused(L.live_state_provider({}, _evidence(funding=_funding(complete=False))),
                           "FUNDING_INCOMPLETE")

    def test_duplicate_fill_trade_ids_refuse(self):
        ev = _evidence(known_orders={ENTRY_ID: "bot_entry"},
                       fills=_fills(fills=[_fill(NOW - 2000, cl_ord_id=ENTRY_ID, trade_id="T1"),
                                           _fill(NOW - 1000, cl_ord_id=ENTRY_ID, trade_id="T1")]))
        self.assertRefused(L.live_state_provider({}, ev), "FILL_DUPLICATED")

    def test_macro_unknown_refuses_not_false(self):
        ev = _evidence(macro=L.MacroState(observed_at_ms=NOW - 1000, blackout=None, source=""))
        out = L.live_state_provider({}, ev)
        self.assertRefused(out, "MACRO_UNAVAILABLE")
        self.assertIsNone(out["macro_blackout"])

    def test_macro_non_bool_refuses(self):
        ev = _evidence(macro=L.MacroState(observed_at_ms=NOW - 1000, blackout="false",
                                          source="state/latest.json"))
        self.assertRefused(L.live_state_provider({}, ev), "MACRO_UNAVAILABLE")

    def test_macro_stale_refuses(self):
        ev = _evidence(macro=L.MacroState(observed_at_ms=NOW - L.MACRO_STALE_MS - 1,
                                          blackout=False, source="state/latest.json"))
        self.assertRefused(L.live_state_provider({}, ev), "STALE_MACRO")

    def test_non_finite_nan_refuses(self):
        ev = _evidence(known_orders={ENTRY_ID: "bot_entry"},
                       fills=_fills(fills=[_fill(NOW - 1000, cl_ord_id=ENTRY_ID,
                                                 pnl=float("nan"), trade_id="T1")]))
        self.assertRefused(L.live_state_provider({}, ev), "NON_FINITE_VALUE")

    def test_future_timestamp_refuses(self):
        ev = _evidence(server_time_ms=0,
                       fills=_fills(fetched_at=NOW, coverage_to=NOW + 60_000))
        self.assertRefused(L.live_state_provider({}, ev), "FUTURE_TIMESTAMP")

    def test_clock_skew_refuses(self):
        ev = _evidence(server_time_ms=NOW + L.MAX_CLOCK_SKEW_MS + 1)
        self.assertRefused(L.live_state_provider({}, ev), "CLOCK_SKEW")

    def test_stale_account_refuses(self):
        ev = _evidence(account=_account(fetched_at=NOW - L.ACCOUNT_STALE_MS - 1))
        out = L.live_state_provider({}, ev)
        self.assertRefused(out, "STALE_ACCOUNT")
        self.assertFalse(out["data_fresh"])

    def test_halted_refuses(self):
        out = L.live_state_provider({}, _evidence(halted=True))
        self.assertRefused(out, "HALTED")
        self.assertTrue(out["halted"])


class LiveStateStrictTests(unittest.TestCase):
    def assertRefused(self, out, code: str):
        self.assertFalse(out["live_verified"])
        self.assertIn(code, out["refusal_reasons"])

    def test_adapter_verified_string_false_refuses(self):
        out = L.live_state_provider({}, _evidence(adapter_verified="false"))
        self.assertRefused(out, "ADAPTER_UNVERIFIED")

    def test_fills_complete_string_false_refuses(self):
        ev = _evidence(fills=replace(_fills(), complete="false"))
        self.assertRefused(L.live_state_provider({}, ev), "FILLS_INCOMPLETE")

    def test_funding_complete_string_false_refuses(self):
        ev = _evidence(funding=replace(_funding(), complete="false"))
        self.assertRefused(L.live_state_provider({}, ev), "FUNDING_INCOMPLETE")

    def test_bool_pnl_is_not_finite(self):
        ev = _evidence(known_orders={ENTRY_ID: "bot_entry"}, fills=_fills(fills=[
            _fill(NOW - 1000, cl_ord_id=ENTRY_ID, pnl=True, trade_id="T-bool")]))
        self.assertRefused(L.live_state_provider({}, ev), "NON_FINITE_VALUE")

    def test_future_fill_timestamp_refuses(self):
        ev = _evidence(known_orders={ENTRY_ID: "bot_entry"}, fills=_fills(fills=[
            _fill(NOW + 60_000, cl_ord_id=ENTRY_ID, trade_id="T-future")]))
        self.assertRefused(L.live_state_provider({}, ev), "FUTURE_TIMESTAMP")

    def test_future_funding_bill_timestamp_refuses(self):
        ev = _evidence(funding=_funding(bills=[_bill(NOW + 60_000, bill_id="B-future")]))
        self.assertRefused(L.live_state_provider({}, ev), "FUTURE_TIMESTAMP")

    def test_journal_schema_mapping_and_account_filter(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "journal.sqlite3"
            con = sqlite3.connect(str(path))
            con.execute(
                "CREATE TABLE routes (account TEXT, signal TEXT, fingerprint TEXT, "
                "client TEXT, status TEXT, owner TEXT, data TEXT)")
            live_client = "h" + "a" * 31
            protection = "r" + "c" * 31
            data = {"client_order_id": "closed-1", "protection_id": protection,
                    "close_id": "FLAT123"}
            con.execute("INSERT INTO routes VALUES (?,?,?,?,?,?,?)",
                        ("T3-live", "s1", "f1", live_client, "PROTECTED", "", json.dumps(data)))
            con.execute("INSERT INTO routes VALUES (?,?,?,?,?,?,?)",
                        ("T3-mock", "s2", "f2", "h" + "b" * 31, "PROTECTED", "", "{}"))
            con.commit()
            con.close()
            mapping = L._load_known_orders("T3-live", path)
        self.assertEqual(mapping.get(live_client), "bot_entry")
        self.assertEqual(mapping.get("closed-1"), "bot_entry")
        self.assertEqual(mapping.get(protection), "bot_exit")
        self.assertEqual(mapping.get("FLAT123"), "bot_exit")
        self.assertNotIn("h" + "b" * 31, mapping)     # account 精确筛选，mock 行不混入


class ReadOnlyCollectorRegressionTests(unittest.TestCase):
    def setUp(self):
        import unittest.mock as mock
        from src import okx_private as private

        def forbidden(*args, **kwargs):
            raise AssertionError("external I/O forbidden in offline regression")
        for target in ("socket.socket", "socket.create_connection", "urllib.request.urlopen"):
            patcher = mock.patch(target, side_effect=forbidden)
            patcher.start()
            self.addCleanup(patcher.stop)
        for name in ("private", "creds"):
            patcher = mock.patch.object(private, name, side_effect=forbidden)
            patcher.start()
            self.addCleanup(patcher.stop)

    def pages(self, pages):
        calls = []
        def gateway(method, path, params):
            self.assertEqual(method, "GET")
            calls.append((path, dict(params)))
            index = len(calls) - 1
            result = pages[min(index, len(pages) - 1)]
            if isinstance(result, Exception):
                raise result
            return result
        return gateway, calls

    def test_three_full_pages_decrease_cursor_and_fixed_week_window(self):
        start = L._week_start_ms(NOW)
        pages = [[_fill_row(f"T{n + i}", str(10000 - n - i)) for i in range(100)]
                 for n in (0, 100, 200)] + [[]]
        gateway, calls = self.pages(pages)
        out = L._collect_fills(gateway, start, NOW)
        self.assertTrue(out.complete, out.errors)
        self.assertEqual(len(out.fills), 300)
        self.assertEqual([p["after"] for _, p in calls[1:]], ["9901", "9801", "9701"])
        self.assertEqual(out.coverage_from_ms, start)
        self.assertTrue(all(p["begin"] == str(start) and p["end"] == str(NOW) for _, p in calls))

    def test_empty_fixed_window_proves_no_fills(self):
        gateway, calls = self.pages([[]])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertTrue(out.complete)
        self.assertEqual(out.fills, ())
        self.assertEqual(calls[0][1]["begin"], str(DAY_START))

    def test_recent_short_page_proves_window_without_fake_old_fill(self):
        gateway, _ = self.pages([[_fill_row()]])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertTrue(out.complete)
        self.assertEqual(out.coverage_from_ms, DAY_START)

    def test_page_cap_never_complete(self):
        import unittest.mock as mock
        gateway, _ = self.pages([[_fill_row(f"T{i}", str(1000 - i)) for i in range(100)]])
        with mock.patch.object(L, "HISTORY_MAX_PAGES", 1):
            out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertFalse(out.complete)
        self.assertIn("HISTORY_PAGE_CAP", out.errors)

    def test_repeated_full_page_without_progress_is_incomplete(self):
        gateway, calls = self.pages([[_fill_row(f"T{i}", str(1000 - i)) for i in range(100)]])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertFalse(out.complete)
        self.assertEqual(len(calls), 2)

    def test_mixed_newer_and_older_short_page_cannot_prove_coverage(self):
        first = [_fill_row(f"T{i}", str(1000 - i)) for i in range(100)]
        second = [_fill_row("mixed-newer", "950"), _fill_row("mixed-older", "800")]
        gateway, calls = self.pages([first, second])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertEqual(calls[1][1]["after"], "901")
        self.assertFalse(out.complete)
        self.assertEqual(out.errors, ("HISTORY_ROW_INVALID",))

    def test_cursor_boundary_duplicate_short_page_is_incomplete(self):
        first = [_fill_row(f"T{i}", str(1000 - i)) for i in range(100)]
        second = [dict(first[-1]), _fill_row("older", "800")]
        gateway, _ = self.pages([first, second])
        self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_funding_mixed_cursor_boundary_short_page_is_incomplete(self):
        first = [_funding_row(str(1000 - i)) for i in range(100)]
        for boundary in ("950", "901"):
            with self.subTest(boundary=boundary):
                gateway, _ = self.pages([first, [_funding_row(boundary), _funding_row("800")]])
                self.assertFalse(L._collect_funding(gateway, DAY_START, NOW).complete)

    def test_exception_keeps_partial_but_never_complete_or_leaks_error(self):
        gateway, _ = self.pages([[_fill_row(f"T{i}", str(1000 - i)) for i in range(100)],
                                  RuntimeError("FAKE_PRIVATE_MARKER")])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertFalse(out.complete)
        self.assertEqual(len(out.fills), 100)
        self.assertNotIn("FAKE_PRIVATE_MARKER", repr(out))

    def test_none_or_error_object_is_not_empty_page(self):
        for response in (None, {"code": "50000", "data": []}):
            with self.subTest(response=response):
                gateway, _ = self.pages([response])
                self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_missing_fill_fields_never_invent_zero_or_instrument(self):
        for key in ("billId", "tradeId", "instId", "fillTime", "fillPnl", "fee", "feeCcy"):
            with self.subTest(field=key):
                row = _fill_row()
                row.pop(key)
                gateway, _ = self.pages([[row]])
                self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_pnl_cannot_substitute_missing_fillpnl(self):
        row = _fill_row(pnl="99")
        row.pop("fillPnl")
        with self.assertRaises(ValueError):
            L._parse_fills([row])

    def test_non_usdt_fee_and_wrong_instrument_refuse(self):
        for row in (_fill_row(feeCcy="BTC"), _fill_row(instId="BTC-USDT-SWAP")):
            gateway, _ = self.pages([[row]])
            self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_invalid_numbers_refuse(self):
        for value in ("nan", "inf", "", None, True):
            with self.subTest(value=value):
                gateway, _ = self.pages([[_fill_row(fee=value)]])
                self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_conflicting_fill_duplicate_refuses(self):
        gateway, _ = self.pages([[_fill_row(), _fill_row(fillPnl="1")]])
        self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)
        with self.assertRaises(ValueError):
            L._parse_fills([_fill_row(), _fill_row(fillPnl="1")])

    def test_identical_fill_duplicate_dedupes(self):
        gateway, _ = self.pages([[_fill_row(), _fill_row()]])
        out = L._collect_fills(gateway, DAY_START, NOW)
        self.assertTrue(out.complete)
        self.assertEqual(len(out.fills), 1)

    def test_same_billid_conflicting_tradeid_refuses(self):
        gateway, _ = self.pages([[_fill_row(), _fill_row(identity="T2")]])
        self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_outside_window_rejected(self):
        for ts in (DAY_START - 1, NOW + 1):
            gateway, _ = self.pages([[_fill_row(ts=ts)]])
            self.assertFalse(L._collect_fills(gateway, DAY_START, NOW).complete)

    def test_funding_three_pages_and_empty_final_page(self):
        pages = [[_funding_row(str(10000 - n - i)) for i in range(100)]
                 for n in (0, 100, 200)] + [[]]
        gateway, calls = self.pages(pages)
        out = L._collect_funding(gateway, DAY_START, NOW)
        self.assertTrue(out.complete, out.errors)
        self.assertEqual(len(out.bills), 300)
        self.assertEqual([p["after"] for _, p in calls[1:]], ["9901", "9801", "9701"])

    def test_funding_no_bills_and_nonfunding_only_are_complete(self):
        for page in ([], [_funding_row(type="2")]):
            gateway, _ = self.pages([page])
            out = L._collect_funding(gateway, DAY_START, NOW)
            self.assertTrue(out.complete)
            self.assertEqual(out.bills, ())

    def test_funding_exception_and_repeated_page_are_incomplete(self):
        page = [_funding_row(str(1000 - i)) for i in range(100)]
        for pages in ([page], [page, RuntimeError("offline")]):
            gateway, _ = self.pages(pages)
            self.assertFalse(L._collect_funding(gateway, DAY_START, NOW).complete)

    def test_funding_bad_fields_and_conflicting_duplicate_refuse(self):
        for field in ("billId", "ts", "type", "balChg", "ccy", "instId"):
            with self.subTest(field=field):
                row = _funding_row()
                row.pop(field)
                gateway, _ = self.pages([[row]])
                self.assertFalse(L._collect_funding(gateway, DAY_START, NOW).complete)
        gateway, _ = self.pages([[_funding_row(), _funding_row(balChg="1")]])
        self.assertFalse(L._collect_funding(gateway, DAY_START, NOW).complete)

    def test_macro_actual_trimmed_schema_uses_original_time(self):
        payload = {"updated_ms": NOW, "snapshot": {"generated_at_ms": NOW - 1000,
                   "features": {"macro": {"blackout": False}},
                   "facts": {"macro_blackout": False}}}
        macro = L._macro_from_payload(payload)
        self.assertIs(macro.blackout, False)
        self.assertEqual(macro.observed_at_ms, NOW - 1000)

    def test_macro_snapshot_macro_schema_and_facts_agree(self):
        payload = {"updated_ms": NOW, "snapshot": {"generated_at_ms": NOW,
                   "macro": {"blackout": True}, "facts": {"macro_blackout": True}}}
        self.assertIs(L._macro_from_payload(payload).blackout, True)

    def test_macro_missing_contradictory_or_nonbool_facts_refuse(self):
        for facts in ({}, {"macro_blackout": True}, {"macro_blackout": "false"}):
            payload = {"updated_ms": NOW, "snapshot": {"generated_at_ms": NOW,
                       "features": {"macro": {"blackout": False}}, "facts": facts}}
            self.assertIsNone(L._macro_from_payload(payload).blackout)

    def test_macro_fresh_write_cannot_refresh_stale_snapshot(self):
        generated = NOW - L.MACRO_STALE_MS - 1
        payload = {"updated_ms": NOW, "snapshot": {"generated_at_ms": generated,
                   "features": {"macro": {"blackout": False}},
                   "facts": {"macro_blackout": False}}}
        out = L.live_state_provider({}, _evidence(macro=L._macro_from_payload(payload)))
        self.assertIn("STALE_MACRO", out["refusal_reasons"])

    def test_default_adapter_stays_locked_with_valid_readonly_fixture(self):
        import unittest.mock as mock
        payload = {"updated_ms": NOW, "snapshot": {"generated_at_ms": NOW,
                   "macro": {"blackout": False}, "facts": {"macro_blackout": False}}}
        def get(path, params):
            if path.endswith("/time"):
                return [{"ts": str(NOW)}]
            if path.endswith("/balance"):
                return [{"totalEq": "117.72"}]
            return []
        with mock.patch.object(Path, "read_text", return_value=json.dumps(payload)), \
                mock.patch.object(L, "_load_known_orders", return_value={}):
            ev = L.read_only_evidence({}, L._ReadOnlyGateway(get), NOW)
        self.assertIs(ev.adapter_verified, False)
        self.assertEqual(ev.fills.coverage_from_ms, L._week_start_ms(NOW))
        self.assertIn("ADAPTER_UNVERIFIED", L.live_state_provider({}, ev)["refusal_reasons"])

    def test_explicit_context_reads_existing_files_without_writing_source(self):
        def get(path, params):
            if path.endswith("/time"):
                return [{"ts": str(NOW)}]
            if path.endswith("/balance"):
                return [{"totalEq": "117.72"}]
            return []
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            context = Path(tmp)
            state = context / "state"
            journal_dir = context / "logs" / "router"
            state.mkdir()
            journal_dir.mkdir(parents=True)
            (state / "latest.json").write_text(json.dumps({"updated_ms": NOW,
                "snapshot": {"generated_at_ms": NOW, "macro": {"blackout": False},
                             "facts": {"macro_blackout": False}}}), encoding="utf-8")
            (state / "live_state_baseline.json").write_text(json.dumps({
                "date": "2026-10-18", "equity": 99.25, "observed_at_ms": DAY_START,
                "source": "t4_reconcile"}), encoding="utf-8")
            (state / "HALT").write_text("existing external pause", encoding="utf-8")
            db = sqlite3.connect(journal_dir / "journal.sqlite3")
            db.execute("CREATE TABLE routes (account TEXT, client TEXT, data TEXT)")
            db.execute("INSERT INTO routes VALUES (?,?,?)", ("T3-live", ENTRY_ID, "{}"))
            db.commit()
            db.close()
            before = {str(p.relative_to(context)): p.read_bytes()
                      for p in context.rglob("*") if p.is_file()}
            ev = L.read_only_evidence({}, L._ReadOnlyGateway(get), NOW, context_root=context)
            after = {str(p.relative_to(context)): p.read_bytes()
                     for p in context.rglob("*") if p.is_file()}
            self.assertEqual(before, after)
            self.assertEqual(ev.baseline.equity, 99.25)
            self.assertEqual(ev.known_orders, {ENTRY_ID: "bot_entry"})
            self.assertTrue(ev.halted)
            self.assertIs(ev.macro.blackout, False)
            self.assertIs(ev.adapter_verified, False)

    def test_nonexistent_context_is_not_created(self):
        def get(path, params):
            return ([{"ts": str(NOW)}] if path.endswith("/time") else
                    [{"totalEq": "117.72"}] if path.endswith("/balance") else [])
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "not-created"
            ev = L.read_only_evidence({}, L._ReadOnlyGateway(get), NOW, context_root=path)
            self.assertFalse(path.exists())
            self.assertIsNone(ev.baseline)
            self.assertEqual(ev.known_orders, {})
            self.assertFalse(ev.adapter_verified)

    def test_external_sqlite_wal_never_read_as_checkpointed_main(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = Path(tmp) / "journal.sqlite3"
            path.write_bytes(b"not read while sidecar exists")
            wal = Path(str(path) + "-wal")
            wal.write_bytes(b"uncheckpointed")
            before = {p.name: p.read_bytes() for p in Path(tmp).iterdir()}
            self.assertEqual(L._load_known_orders("T3-live", path, immutable=True), {})
            self.assertEqual(before, {p.name: p.read_bytes() for p in Path(tmp).iterdir()})


class LiveStatePurityTests(unittest.TestCase):
    def test_no_external_requests_with_failclosed_stubs(self):
        import unittest.mock as mock
        from src import okx_private as P

        def forbidden(*args, **kwargs):
            raise AssertionError("network/credential access is forbidden")

        out = None
        with mock.patch("socket.socket", side_effect=forbidden), \
                mock.patch("socket.create_connection", side_effect=forbidden), \
                mock.patch("urllib.request.urlopen", side_effect=forbidden), \
                mock.patch.object(P, "private", side_effect=forbidden), \
                mock.patch.object(P, "creds", side_effect=forbidden):
            out = L.live_state_provider({}, _evidence(
                known_orders={ENTRY_ID: "bot_entry"},
                fills=_fills(fills=[_fill(NOW - 1000, cl_ord_id=ENTRY_ID)])))
        self.assertTrue(out["live_verified"], out["refusal_reasons"])

    def test_refusal_payload_is_canonical(self):
        out = L.live_state_provider({}, _evidence(baseline=None))
        for key in ("now_ms", "observed_at_ms", "equity_estimate", "day_start_equity",
                    "realized_pnl_today", "entries_today", "open_positions", "pending_orders",
                    "protection_orders", "data_fresh", "macro_blackout", "halted",
                    "live_verified", "refusal_reasons"):
            self.assertIn(key, out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
