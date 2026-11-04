"""Offline calendar polling with the real collector and real atomic disk cache."""
import copy
import json
import socket
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from src import collect as C
from src import sources_ext as E
from src import util as U


class DemoCalendarCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.temp.cleanup)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.clock = 1800000000000
        self.start = self.clock
        self.upstream_times = []
        self.outage = False
        self.cache_dir = Path(self.temp.name) / "cache"
        self.config = {
            "symbols": [{"instId": "SOL-USDT-SWAP", "ccy": "SOL", "index": "SOL-USDT"}],
            "data": {"poll_seconds": {"ticker": 5, "orderbook": 5, "candles": 60,
                                       "funding": 60, "open_interest": 15, "rubik": 300,
                                       "sentiment": 600, "onchain": 900, "macro": 1800,
                                       "calendar": 3600}, "depth_levels": 400},
            "macro": {"cnbc_symbols": {}},
        }
        self.original_config = copy.deepcopy(self.config)
        # All actual network is prohibited, including accidental public/account requests.
        for name in ("connect", "connect_ex"):
            self.stack.enter_context(patch.object(socket.socket, name,
                side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch.object(socket, "create_connection",
            side_effect=AssertionError("NO_NETWORK")))
        for name in ("http_request", "http_json", "okx"):
            self.stack.enter_context(patch.object(U, name,
                side_effect=AssertionError("NO_REAL_HTTP_OR_ACCOUNT")))
        self.stack.enter_context(patch.object(U, "CACHE_DIR", self.cache_dir))
        for module in (U, E, C):
            self.stack.enter_context(patch.object(module, "now_ms", side_effect=lambda: self.clock))
        self.stack.enter_context(patch.object(C, "server_now_ms", side_effect=lambda: self.clock))
        self.stack.enter_context(patch.object(C, "sync_server_time", return_value={"drift_ok": True}))
        self.stack.enter_context(patch.object(C, "build_features", return_value={}))
        self.stack.enter_context(patch.object(C, "build_facts", return_value={}))
        self.stack.enter_context(patch.object(C, "check_freshness", return_value=({}, {})))
        self.stack.enter_context(patch.object(E, "log"))
        self.stack.enter_context(patch.object(U, "log"))
        self.http = self.stack.enter_context(patch.object(E, "http_json", side_effect=self.upstream))
        self.ff = self.stack.enter_context(patch.object(E, "http_request",
            side_effect=U.DataError("offline fallback also unavailable")))
        # Keep the production task builder/calendar callable; unrelated datasets are
        # excluded so these tests measure this collection path without 31 fake sources.
        self.real_build_tasks = C.build_tasks
        self.tasks = self.stack.enter_context(patch.object(C, "build_tasks",
            side_effect=lambda cfg, **kw: [row for row in self.real_build_tasks(cfg, **kw)
                                          if row[0] == "economic_calendar"]))

    def upstream(self, url, **kwargs):
        self.assertTrue(url.startswith("https://economic-calendar.tradingview.com/events?"))
        self.upstream_times.append(self.clock)
        if self.outage:
            raise U.DataError("offline upstream unavailable")
        return {"result": [{"date": "2027-01-20T12:00:00Z", "country": "US",
                            "importance": 1, "title": "offline fixture"}]}

    def scan_at(self, minute, previous=None, *, override=1500, force=False):
        self.clock = self.start + minute * 60000
        if override is None:
            return C.collect(self.config, previous, force)
        return C.collect(self.config, previous, force, calendar_max_age_s=override)

    def cached_record(self):
        return json.loads((self.cache_dir / "economic_calendar.json").read_text(encoding="utf-8"))

    def test_demo_really_refetches_every_25_minutes_over_two_and_a_half_hours(self):
        previous = None
        by_minute = {}
        for minute in range(151):
            previous = self.scan_at(minute, previous)
            env = previous["sources"]["economic_calendar"]
            expected = self.start + (minute // 25) * 25 * 60000
            self.assertEqual(env["source_ts"], expected, minute)
            self.assertEqual(env["fetched_at_ms"], expected, minute)
            self.assertEqual(self.cached_record()["fetched_at_ms"], expected, minute)
            by_minute[minute] = env
        self.assertEqual(self.upstream_times,
                         [self.start + m * 60000 for m in range(0, 151, 25)])
        self.assertFalse(by_minute[60]["cached"])
        self.assertEqual(by_minute[60]["source_ts"], self.start + 50 * 60000)
        self.assertEqual(self.config, self.original_config)
        self.tasks.assert_called_with(self.config, calendar_max_age_s=1500)

    def test_default_scanner_retains_one_hour_poll_and_two_hour_cache(self):
        previous = None
        by_minute = {}
        for minute in range(151):
            previous = self.scan_at(minute, previous, override=None)
            by_minute[minute] = previous["sources"]["economic_calendar"]
        self.assertEqual(self.upstream_times, [self.start, self.start + 120 * 60000])
        self.assertEqual(by_minute[60]["source_ts"], self.start)
        self.assertEqual(by_minute[60]["fetched_at_ms"], self.start + 60 * 60000)
        self.assertTrue(by_minute[60]["cached"])
        self.tasks.assert_called_with(self.config)
        calendar = next(row for row in self.real_build_tasks(self.config)
                        if row[0] == "economic_calendar")
        self.assertEqual(calendar[1], 3600)
        self.assertIs(calendar[2], E.economic_calendar)
        self.assertEqual(self.config, self.original_config)

    def test_outage_never_promotes_old_disk_cache_source_time(self):
        previous = self.scan_at(0)
        original_cache = (self.cache_dir / "economic_calendar.json").read_bytes()
        self.outage = True
        for minute in (25, 31, 50, 60, 75, 100, 125, 150):
            previous = self.scan_at(minute, previous)
            env = previous["sources"]["economic_calendar"]
            self.assertTrue(env["ok"])
            self.assertTrue(env["cached"])
            self.assertEqual(env["source_ts"], self.start)
            self.assertEqual(self.cached_record()["fetched_at_ms"], self.start)
        self.assertGreater(self.clock - env["source_ts"], 1800 * 1000)
        self.assertGreater(env["fetched_at_ms"], env["source_ts"])
        self.assertEqual((self.cache_dir / "economic_calendar.json").read_bytes(), original_cache)
        self.assertTrue(self.ff.called)

    def test_default_two_hour_disk_cache_is_refetched_by_strict_execution_policy(self):
        initial = self.scan_at(0, override=None)
        self.clock = self.start + 61 * 60000
        snapshot = C.collect(self.config, initial, calendar_max_age_s=1500)
        env = snapshot["sources"]["economic_calendar"]
        self.assertEqual(env["source_ts"], self.clock)
        self.assertFalse(env["cached"])
        self.assertEqual(len(self.upstream_times), 2)

    def test_force_refresh_preserves_disk_cache_original_source_stamp(self):
        previous = self.scan_at(0)
        current = self.scan_at(1, previous, force=True)
        env = current["sources"]["economic_calendar"]
        self.assertEqual(env["fetched_at_ms"], self.clock)
        self.assertEqual(env["source_ts"], self.start)
        self.assertTrue(env["cached"])
        self.assertEqual(self.upstream_times, [self.start])

    def test_short_upstream_outage_is_retried_next_scan_despite_new_envelope(self):
        previous = self.scan_at(0)
        self.outage = True
        fallback = self.scan_at(25, previous)
        env = fallback["sources"]["economic_calendar"]
        self.assertEqual(env["fetched_at_ms"], self.start + 25 * 60000)
        self.assertEqual(env["source_ts"], self.start)
        self.outage = False
        recovered = self.scan_at(26, fallback)["sources"]["economic_calendar"]
        self.assertEqual(recovered["source_ts"], self.start + 26 * 60000)
        self.assertFalse(recovered["cached"])
        self.assertEqual(self.upstream_times,
                         [self.start, self.start + 25 * 60000, self.start + 26 * 60000])

    def test_execution_does_not_reuse_invalid_or_future_calendar_source_stamp(self):
        initial = self.scan_at(0)
        for invalid in (None, True, 0, -1, "1800000000000", float(self.start),
                        self.start + 2 * 60000):
            with self.subTest(source_ts=invalid):
                previous = copy.deepcopy(initial)
                previous["sources"]["economic_calendar"]["source_ts"] = invalid
                self.clock = self.start + 60000
                with patch.object(E, "economic_calendar", wraps=E.economic_calendar) as fetch:
                    current = C.collect(self.config, previous, calendar_max_age_s=1500)
                fetch.assert_called_once_with(cache_ttl_s=1500)
                self.assertEqual(current["sources"]["economic_calendar"]["source_ts"], self.start)

    def test_failed_fetch_without_cache_is_explicit_failure(self):
        self.outage = True
        env = self.scan_at(0)["sources"]["economic_calendar"]
        self.assertFalse(env["ok"])
        self.assertIsNone(env["source_ts"])
        self.assertIsNone(env["data"])
        self.assertFalse((self.cache_dir / "economic_calendar.json").exists())

    def test_override_never_slows_an_already_shorter_poll(self):
        self.config["data"]["poll_seconds"]["calendar"] = 600
        calendar = next(row for row in self.real_build_tasks(self.config, calendar_max_age_s=1500)
                        if row[0] == "economic_calendar")
        self.assertEqual(calendar[1], 600)
        with patch.object(E, "economic_calendar", return_value={}) as fetch:
            calendar[2]()
        fetch.assert_called_once_with(cache_ttl_s=1500)

    def test_execution_override_rejects_invalid_seconds_before_build_or_io(self):
        for value in (True, False, 0, -1, 1500.0, "1500", 1801, 7200, float("nan")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    C.collect(self.config, calendar_max_age_s=value)
                with self.assertRaises(ValueError):
                    self.real_build_tasks(self.config, calendar_max_age_s=value)
        self.tasks.assert_not_called()
        self.http.assert_not_called()
        self.assertFalse(self.cache_dir.exists())
        for value in (1, 1500, 1800):
            self.assertTrue(self.real_build_tasks(self.config, calendar_max_age_s=value))

    def test_generic_cache_ttl_rejects_nonpositive_and_noninteger_values(self):
        for value in (None, True, False, 0, -1, 1500.0, "1500", float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                E.economic_calendar(cache_ttl_s=value)
        self.http.assert_not_called()
        self.assertFalse(self.cache_dir.exists())

    def test_calendar_without_override_preserves_original_cache_call(self):
        with patch.object(E, "cached_payload", wraps=U.cached_payload) as cache:
            E.economic_calendar()
        self.assertEqual(cache.call_args.args[:2], ("economic_calendar.json", 7200))


if __name__ == "__main__":
    unittest.main()
