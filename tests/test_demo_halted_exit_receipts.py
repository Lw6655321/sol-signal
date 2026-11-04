"""Closed HALTED receipts use the real router/transport over offline fake HTTP."""
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from src import demo_route_transport as T
from src.execution import build_intent
from src.order_router import OrderRouter, client_order_id
from test_demo_route_transport import FAKE
from test_execution import NOW, inputs


class ClosedHaltedReceipts(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name in ("socket.create_connection", "socket.socket.connect"):
            self.stack.enter_context(patch(name, side_effect=AssertionError("NO_NETWORK")))
        self.stack.enter_context(patch.object(T, "load_demo_credentials", side_effect=AssertionError("NO_CREDENTIALS")))
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(ignore_cleanup_errors=True)))
        signal, self.state, self.config = inputs()
        self.intent = build_intent(signal, self.state, self.config)
        self.client = client_order_id(self.intent.signal_id)
        self.protection = "r" + hashlib.sha256((self.intent.signal_id+":protection").encode()).hexdigest()[:31]
        self.account = hashlib.sha256(b"DEMO\x007").hexdigest()
        self.flat = self.intent.created_at_ms+10
        self.opened = self.flat+100
        self.closed = self.flat+500
        self.server = NOW
        qty = str(self.intent.qty)
        self.entry = dict(instId=T.INST, ordId="101", clOrdId=self.client, side="buy", posSide="net",
            tdMode="cross", ordType="post_only" if self.intent.require_maker else "limit", state="filled",
            sz=qty, accFillSz=qty, px=str(self.intent.entry), avgPx="100", reduceOnly="false", algoId="",
            cTime=str(self.opened-1), fillTime=str(self.opened), uTime=str(self.opened+1))
        self.parent = dict(instId=T.INST, algoId="201", algoClOrdId=self.protection, side="sell", posSide="net",
            tdMode="cross", ordType="oco", state="effective", sz=qty, reduceOnly="true", actualSide="tp",
            slTriggerPx=str(self.intent.stop), tpTriggerPx=str(self.intent.tp1), slOrdPx="-1", tpOrdPx="-1",
            ordIdList=["301"], ordId="301", cTime=str(self.opened+2), triggerTime=str(self.closed-1),
            uTime=str(self.closed-1))
        self.child = dict(instId=T.INST, ordId="301", clOrdId="", algoId="201", algoClOrdId=self.protection,
            side="sell", posSide="net", tdMode="cross", ordType="market", state="filled", sz=qty,
            accFillSz=qty, avgPx="103", reduceOnly="true", cTime=str(self.closed-1),
            fillTime=str(self.closed), uTime=str(self.closed))
        self.fills = [dict(instId=T.INST, instType="SWAP", posSide="net", side="buy", ordId="101",
            clOrdId=self.client, tradeId="t1", billId="b1", fillTime=str(self.opened), ts=str(self.opened+1),
            fillSz=qty, fillPx="100", fillPnl="0", fee="-0.01", feeCcy="USDT", subType="1", posId="401"),
            dict(instId=T.INST, instType="SWAP", posSide="net", side="sell", ordId="301", clOrdId="",
            tradeId="t2", billId="b2", fillTime=str(self.closed), ts=str(self.closed), fillSz=qty,
            fillPx="103", fillPnl=str(Decimal("3")*self.intent.qty), fee="-0.01", feeCcy="USDT", subType="2", posId="401")]
        self.history = dict(instId=T.INST, instType="SWAP", posId="401", posSide="net", mgnMode="cross",
            ccy="USDT", direction="long", cTime=str(self.opened+1), uTime=str(self.closed), openMaxPos=qty,
            closeTotalPos=qty, openAvgPx="100", closeAvgPx="103", liqPenalty="0", settledPnl="0")
        self.gets, self.posts = [], []
        self.positions, self.pending, self.algos = [], [], []
        self.parent_reads = 0
        self.position_reads = 0
        self.on_get = None
        self.history_extra = []
        self.fill_override = None
        self.transport = T.DemoRouteTransport(FAKE, enable_writes=False, http_get=self.get,
                         http_post=self.post, log_root=self.root)
        self.stack.enter_context(patch.object(self.transport, "assert_demo_enabled", side_effect=AssertionError("NO_WRITE_CAPABILITY")))

    def get(self, url, headers, timeout):
        self.assertEqual(headers["x-simulated-trading"], "1")
        self.assertTrue(url.startswith(T.BASE+"/api/v5/"))
        path, params = urlsplit(url).path, parse_qs(urlsplit(url).query)
        self.gets.append((path, params))
        if self.on_get:
            self.on_get(path, params)
        if path.endswith("/public/time"):
            data = [{"ts": str(self.server)}]
        elif path.endswith("/account/config"):
            data = [{"uid": "7", "posMode": "net_mode"}]
        elif path.endswith("/trade/order"):
            data = [self.entry if params.get("clOrdId") == [self.client] else self.child]
        elif path.endswith("/trade/order-algo"):
            self.parent_reads += 1
            data = [self.parent]
        elif path.endswith("/trade/fills-history"):
            self.assertNotIn("ordId", params)
            self.assertNotIn("clOrdId", params)
            data = self.fill_override if self.fill_override is not None else [row for row in self.fills
                if int(params["begin"][0]) <= int(row["fillTime"]) <= int(params["end"][0])]
        elif path.endswith("/account/positions-history"):
            self.assertEqual(params["after"], [str(self.server+1)])
            data = [self.history] + self.history_extra
        elif path.endswith("/account/positions"):
            self.position_reads += 1
            data = self.positions
        elif path.endswith("/trade/orders-pending"):
            data = self.pending
        elif path.endswith("/trade/orders-algo-pending"):
            data = self.algos if params["ordType"] == ["oco"] else []
        else:
            raise AssertionError("UNEXPECTED_GET")
        return 200, {"code": "0", "data": deepcopy(data)}

    def post(self, *args):
        self.posts.append(args)
        raise AssertionError("NO_POST")

    def verify(self, **changes):
        options = dict(intent=self.intent, order_id="101", client_id=self.client, filled_qty=self.intent.qty,
            flat_observed_at_ms=self.flat, protection_id=self.protection, algo_id="201")
        options.update(changes)
        return self.transport.verify_closed_position_lifecycle(**options)

    def refused(self, code=None):
        with self.assertRaises(T.DemoRouteError) as caught:
            self.verify()
        if code:
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.posts, [])

    def router(self):
        router = OrderRouter(self.transport, config=self.config, state_provider=lambda: self.fail("NO_STATE_PROVIDER"),
            market_provider=lambda: self.fail("NO_MARKET_PROVIDER"), log_root=self.root,
            journal_path=self.root/"route.sqlite3", account_key="DEMO-CLOSED", account_identity=self.account,
            clock=lambda: NOW)
        router._last_verified_account_time = self.flat
        fingerprint = hashlib.sha256(json.dumps(self.intent.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        row, new = router._claim(self.intent, fingerprint, self.client)
        self.assertTrue(new)
        row.update(status="HALTED", code="OBSERVE_HALTED", order_id="101", entry_state="filled",
            filled_qty=str(self.intent.qty), protected_qty=str(self.intent.qty), protection_order_id="201",
            submit_attempted=True, emergency_status="EXPOSURE_UNVERIFIED")
        router._save(row)
        router._release_owner(self.intent)
        router.halt_path.write_bytes(b"ORIGINAL_HALT\n")
        with router._db() as db:
            db.execute("INSERT INTO controls VALUES(?,?)", (router.account_key, "ORIGINAL_HALT"))
        return router

    def route(self, router):
        with router._db() as db:
            return json.loads(db.execute("SELECT data FROM routes WHERE account=?", (router.account_key,)).fetchone()[0])

    def assert_halt(self, router):
        self.assertEqual(router.halt_path.read_bytes(), b"ORIGINAL_HALT\n")
        with router._db() as db:
            self.assertEqual(db.execute("SELECT code FROM controls").fetchall(), [("ORIGINAL_HALT",)])
        self.assertEqual(self.posts, [])

    def test_disabled_writes_can_prove_real_closed_binding_and_preserve_raw_sources(self):
        original = deepcopy((self.entry, self.parent, self.child, self.fills, self.history))
        proof = self.verify()
        self.assertTrue(proof["closed_lifecycle_verified"])
        self.assertEqual(proof["position_id"], "401")
        self.assertEqual(proof["position_opened_at_ms"], self.opened+1)
        self.assertEqual(proof["position_closed_at_ms"], self.closed)
        self.assertEqual(proof["exit_order_ids"], ["301"])
        self.assertEqual(proof["account_hash"], self.account)
        self.assertEqual(proof["fills"], self.fills)
        self.assertEqual((self.entry, self.parent, self.child, self.fills, self.history), original)
        self.assertEqual(self.position_reads, 2)
        self.assertEqual(self.parent_reads, 2)
        self.assertEqual(self.posts, [])

    def test_parent_identity_shape_prices_actual_side_and_children_are_required(self):
        original = deepcopy(self.parent)
        for key, value in (("algoId", "foreign"), ("algoClOrdId", "foreign"), ("state", "canceled"),
                ("side", "buy"), ("reduceOnly", "false"), ("slTriggerPx", "98"), ("tpTriggerPx", "104"),
                ("slOrdPx", "99"), ("tpOrdPx", "103"), ("actualSide", ""), ("ordIdList", []),
                ("ordIdList", ["301", "301"]), ("sz", "0.01"), ("slTriggerPxType", "mark")):
            with self.subTest(key=key, value=value):
                self.parent = dict(original, **{key: value})
                self.refused()
        self.parent = original

    def test_child_must_be_real_fully_filled_reduce_only_exact_parent(self):
        original = deepcopy(self.child)
        for key, value in (("algoId", "foreign"), ("ordId", "foreign"), ("algoClOrdId", "foreign"),
                ("reduceOnly", "false"), ("side", "buy"), ("state", "live"), ("posSide", "long"),
                ("accFillSz", "0"), ("ordType", "limit"), ("clOrdId", "foreign"), ("avgPx", "104")):
            with self.subTest(key=key):
                self.child = dict(original, **{key: value})
                self.refused()

    def test_entry_immutable_price_client_fill_quantity_and_terminal_state(self):
        original = deepcopy(self.entry)
        for key, value in (("ordId", "foreign"), ("clOrdId", "foreign"), ("px", "99"),
                ("side", "sell"), ("reduceOnly", "true"), ("state", "live"), ("algoId", "foreign"),
                ("accFillSz", "0.01"), ("sz", "0.01"), ("avgPx", "99")):
            with self.subTest(key=key):
                self.entry = dict(original, **{key: value})
                self.refused()

    def test_history_unique_exact_scope_quantity_real_position_and_prices(self):
        original = deepcopy(self.history)
        for key, value in (("posId", "foreign"), ("cTime", str(self.opened+2)), ("uTime", str(self.closed+1)),
                ("direction", "short"), ("ccy", "BTC"), ("openMaxPos", "0.01"), ("closeTotalPos", "0.01"),
                ("mgnMode", "isolated"), ("liqPenalty", "1"), ("settledPnl", "1"), ("openAvgPx", "101"),
                ("closeAvgPx", "104"), ("instType", "FUTURES"), ("posSide", "long")):
            with self.subTest(key=key):
                self.history = dict(original, **{key: value})
                self.refused()

    def test_external_history_or_duplicate_history_is_never_time_matched(self):
        self.history_extra = [deepcopy(self.history)]
        self.refused()
        self.history_extra[0]["posId"] = "foreign"
        self.refused()
        self.history_extra = [deepcopy(self.history) for _ in range(99)]
        self.refused("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")

    def test_every_unfiltered_fill_must_match_order_client_side_type_ids_and_quantity(self):
        originals = deepcopy(self.fills)
        for index, key, value in ((0, "ordId", "foreign"), (1, "clOrdId", "foreign"), (1, "algoId", "foreign"),
                (0, "fillPnl", "1"), (0, "side", "sell"), (1, "subType", "100"), (0, "fillSz", "0.01"),
                (1, "posId", "foreign"), (0, "instType", "FUTURES"), (1, "feeCcy", "BTC"), (1, "fillPx", "104")):
            with self.subTest(index=index, key=key):
                self.fills = deepcopy(originals)
                self.fills[index][key] = value
                self.refused()

    def test_missing_fill_duplicate_trade_or_bill_and_full_page_refuse(self):
        originals = deepcopy(self.fills)
        for values in ([originals[0]], [originals[1]], [], originals+[deepcopy(originals[1])],
                       originals+[dict(originals[1], tradeId="different")],
                       originals+[dict(originals[1], billId="different")]):
            with self.subTest(values=len(values)):
                self.fills = deepcopy(values)
                self.refused()
        self.fill_override = [deepcopy(originals[0]) for _ in range(100)]
        self.refused("DEMO_LIFECYCLE_HISTORY_INCOMPLETE")

    def test_missing_fields_and_record_execution_interval_are_strict(self):
        original = deepcopy(self.fills)
        for key in ("tradeId", "billId", "fillTime", "ts", "fillSz", "fillPx", "fillPnl", "fee", "feeCcy", "subType", "clOrdId"):
            with self.subTest(key=key):
                self.fills = deepcopy(original)
                del self.fills[0][key]
                self.refused()
        for value in (None, True, str(self.opened-1), str(self.server+1), "NaN", "１２３"):
            self.fills = deepcopy(original)
            self.fills[0]["ts"] = value
            self.refused()

    def test_parent_trigger_and_order_timestamps_cannot_be_widened(self):
        for target, key, value in ((self.parent, "triggerTime", str(self.closed+1)),
                (self.parent, "cTime", str(self.opened-1)), (self.parent, "uTime", str(self.closed+1)),
                (self.child, "fillTime", str(self.closed-1)), (self.entry, "uTime", str(self.opened+2))):
            old = target[key]
            target[key] = value
            self.refused()
            target[key] = old

    def test_first_or_second_full_account_exposure_nonempty_refuses(self):
        for wave in (1, 2):
            def changed(path, _params):
                if path.endswith("/account/positions") and self.position_reads+1 == wave:
                    self.positions = [dict(instId="BTC-USDT-SWAP", posSide="net", pos="1")]
            self.position_reads = 0
            self.positions = []
            self.on_get = changed
            self.refused("DEMO_CLOSED_EXPOSURE_NOT_FLAT")

    def test_any_pending_or_algorithm_even_other_instrument_prevents_flat_proof(self):
        self.pending = [dict(instId="BTC-USDT-SWAP", ordId="external", state="live", sz="1", accFillSz="0")]
        self.refused("DEMO_CLOSED_EXPOSURE_NOT_FLAT")
        self.pending = []
        self.algos = [dict(instId="BTC-USDT-SWAP", algoId="external", ordType="oco")]
        self.refused("DEMO_CLOSED_EXPOSURE_NOT_FLAT")

    def test_parent_changes_between_reads_refuse(self):
        def changed(path, _params):
            if path.endswith("/trade/order-algo") and self.parent_reads == 1:
                self.parent["actualSide"] = "sl"
        self.on_get = changed
        self.refused("DEMO_CLOSED_PROTECTION_CHANGED")

    def test_five_second_monotonic_deadline_is_not_relaxed(self):
        # A concurrent scheduler may sample the same expired clock in several
        # threads. Keep the clock expired instead of exhausting a call list.
        ticks = iter((0, 0))
        with patch.object(T.time, "monotonic", side_effect=lambda: next(ticks, 6)):
            self.refused()

    def test_closed_transport_does_not_accept_missing_or_changed_durable_flat_anchor(self):
        for value in (None, True, 0, NOW+1, NOW-T.LIFECYCLE_LOOKBACK_MS-1):
            with self.subTest(value=value), self.assertRaises(T.DemoRouteError):
                self.verify(flat_observed_at_ms=value)

    def test_router_preserves_halt_code_controls_and_archives_success_without_resume(self):
        router = self.router()
        with patch.object(router, "_emergency", side_effect=AssertionError("NO_EMERGENCY")):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual((result.status, result.code), ("HALTED", "OBSERVE_HALTED"))
        self.assertEqual(result.emergency_status, "FLAT_EXIT_RECONCILED_HALTED")
        row = self.route(router)
        self.assertTrue(row["exit_reconciled"])
        self.assertEqual(row["position_id"], "401")
        self.assertEqual(row["exit_order_ids"], ["301"])
        self.assertEqual(row["protected_qty"], "0")
        self.assertEqual(row["confirmed_closed_qty"], str(self.intent.qty))
        self.assertEqual(row["emergency_status"], "EXPOSURE_UNVERIFIED")
        self.assertTrue(Path(row["exit_reconcile_proof_path"]).is_file())
        self.assert_halt(router)

    def test_router_failure_preserves_original_halt_and_flags_no_emergency(self):
        router = self.router()
        original = self.route(router)
        self.fills[0]["clOrdId"] = "external"
        with patch.object(router, "_emergency", side_effect=AssertionError("NO_EMERGENCY")), \
             patch.object(router, "_halt", side_effect=AssertionError("NO_HALT_REWRITE")):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual((result.status, result.code), ("HALTED", "OBSERVE_HALTED"))
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        row = self.route(router)
        for key, value in original.items():
            self.assertEqual(row[key], value)
        self.assertFalse(row.get("exit_reconciled", False))
        self.assert_halt(router)

    def test_readonly_claim_never_creates_entry_and_requires_exact_intent_account_type(self):
        router = self.router()
        for changed in (replace(self.intent, signal_id="NEVER-CREATE"), replace(self.intent, stop=Decimal("98"))):
            result = router.reconcile_halted_exit(changed)
            self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertEqual(self.gets, [])
        router.account_identity = "a"*64
        self.assertEqual(router.reconcile_halted_exit(self.intent).code, "DEMO_HALTED_ROUTE_MISMATCH")
        router.account_identity = self.account
        router.transport = type("FakeDemo", (), {"mode": "demo"})()
        self.assertEqual(router.reconcile_halted_exit(self.intent).code, "DEMO_HALTED_READONLY_SCOPE")
        self.assert_halt(router)

    def test_router_account_source_binding_must_equal_durable_account_hash(self):
        router = self.router()
        router.account_identity = "a"*64
        with router._db() as db:
            row = self.route(router)
            row["account_hash"] = "a"*64
            db.execute("UPDATE routes SET data=?", (json.dumps(row),))
        result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertFalse(self.route(router).get("exit_reconciled", False))
        self.assert_halt(router)

    def test_router_audit_or_proof_persist_failure_cannot_confirm_or_release(self):
        router = self.router()
        original = self.route(router)
        with patch.object(router, "_audit", side_effect=OSError("LOCAL_DISK")):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        row = self.route(router)
        self.assertFalse(row.get("exit_reconciled", False))
        self.assertEqual(row["protected_qty"], original["protected_qty"])
        self.assert_halt(router)

    def test_existing_success_cannot_be_rebound_and_new_entry_stays_blocked(self):
        router = self.router()
        router.reconcile_halted_exit(self.intent)
        first = self.route(router)
        self.history["posId"] = "402"
        for fill in self.fills:
            fill["posId"] = "402"
        result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        row = self.route(router)
        self.assertTrue(row["exit_reconciled"])
        self.assertEqual(row["position_id"], first["position_id"])
        self.assertEqual(row["exit_reconcile_sha256"], first["exit_reconcile_sha256"])
        self.assertEqual(router.execute(replace(self.intent, signal_id="NEW-ENTRY")).code, "DEMO_TRADING_DISABLED")
        with router._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM routes").fetchone()[0], 1)
        self.assert_halt(router)

    def test_identical_proof_reuses_bytes_and_never_opens_confirmed_file_for_writing(self):
        router = self.router()
        router.reconcile_halted_exit(self.intent)
        old_row = self.route(router)
        proof_path = Path(old_row["exit_reconcile_proof_path"])
        original_bytes = proof_path.read_bytes()
        original_open = Path.open
        attempted = []

        def destructive_open(path, mode="r", *args, **kwargs):
            if path == proof_path and any(character in mode for character in "wax+"):
                attempted.append(mode)
                # This is the former bug's real truncation, not a mock that
                # raises before the operating system changes any file bytes.
                with original_open(path, "w", encoding="utf-8") as handle:
                    handle.write("PARTIAL")
                raise OSError("PARTIAL_THEN_DISK_FAILURE")
            return original_open(path, mode, *args, **kwargs)

        with patch.object(Path, "open", destructive_open):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "FLAT_EXIT_RECONCILED_HALTED")
        self.assertEqual(attempted, [])
        self.assertEqual(proof_path.read_bytes(), original_bytes)
        self.assertEqual(self.route(router)["exit_reconcile_sha256"], old_row["exit_reconcile_sha256"])
        self.assert_halt(router)

    def test_existing_corrupt_proof_is_refused_without_overwrite_or_releasing_halt(self):
        router = self.router()
        router.reconcile_halted_exit(self.intent)
        old_row = self.route(router)
        proof_path = Path(old_row["exit_reconcile_proof_path"])
        proof_path.write_bytes(b"CORRUPT_SOURCE")
        result = router.reconcile_halted_exit(self.intent)
        row = self.route(router)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertEqual(row["exit_reconcile_code"], "DEMO_HALTED_EXIT_PROOF_INVALID")
        self.assertEqual(proof_path.read_bytes(), b"CORRUPT_SOURCE")
        for key in ("status", "code", "exit_reconciled", "exit_reconcile_sha256", "exit_reconcile_proof_path",
                    "position_id", "position_opened_at_ms", "position_closed_at_ms"):
            self.assertEqual(row[key], old_row[key])
        self.assert_halt(router)

    def test_new_proof_partial_temp_write_failure_does_not_create_canonical_or_change_flags(self):
        router = self.router()
        old_row = self.route(router)
        original_open = Path.open

        def partial_temp(path, mode="r", *args, **kwargs):
            if path.suffix == ".tmp" and mode == "xb":
                with original_open(path, mode, *args, **kwargs) as handle:
                    handle.write(b"PARTIAL")
                    handle.flush()
                raise OSError("PARTIAL_THEN_DISK_FAILURE")
            return original_open(path, mode, *args, **kwargs)

        with patch.object(Path, "open", partial_temp):
            result = router.reconcile_halted_exit(self.intent)
        row = self.route(router)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertFalse(row.get("exit_reconciled", False))
        self.assertEqual(list(router.run_dir.glob("halted-exit-*.json")), [])
        self.assertEqual(list(router.run_dir.glob("halted-exit-*.tmp")), [])
        for key, value in old_row.items():
            self.assertEqual(row[key], value)
        self.assert_halt(router)

    def test_new_proof_fsync_or_publish_failure_does_not_modify_prior_confirmed_proof(self):
        router = self.router()
        router.reconcile_halted_exit(self.intent)
        old_row = self.route(router)
        proof_path = Path(old_row["exit_reconcile_proof_path"])
        old_bytes = proof_path.read_bytes()
        # Changing the server observation creates a genuinely new digest while
        # the position/order binding stays unchanged.
        self.server += 1
        router.clock = lambda: self.server
        for name in ("fsync", "link"):
            with self.subTest(name=name), patch("src.order_router.os."+name, side_effect=OSError("LOCAL_DISK")) as fault:
                result = router.reconcile_halted_exit(self.intent)
                self.assertTrue(fault.called)
            self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
            self.assertEqual(proof_path.read_bytes(), old_bytes)
            row = self.route(router)
            for key in ("exit_reconciled", "exit_reconcile_sha256", "exit_reconcile_proof_path",
                        "position_id", "position_opened_at_ms", "position_closed_at_ms"):
                self.assertEqual(row[key], old_row[key])
            self.assertEqual(list(router.run_dir.glob("halted-exit-*.tmp")), [])
            self.assertEqual(list(router.run_dir.glob("halted-exit-*.json")), [proof_path])
            self.assert_halt(router)

    def test_atomic_publish_race_reuses_only_exact_existing_bytes_without_overwrite(self):
        router = self.router()
        original_link = __import__("os").link
        # Another publisher may win between the existence check and os.link.
        def racing_link(source, destination):
            Path(destination).write_bytes(Path(source).read_bytes())
            return original_link(source, destination)
        with patch("src.order_router.os.link", side_effect=racing_link):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "FLAT_EXIT_RECONCILED_HALTED")
        row = self.route(router)
        path = Path(row["exit_reconcile_proof_path"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), row["exit_reconcile_sha256"])
        self.assert_halt(router)

    def test_atomic_publish_race_with_different_bytes_refuses_without_overwrite(self):
        router = self.router()
        original_link = __import__("os").link
        def racing_link(source, destination):
            Path(destination).write_bytes(b"CONFLICTING_PUBLISHER")
            return original_link(source, destination)
        with patch("src.order_router.os.link", side_effect=racing_link):
            result = router.reconcile_halted_exit(self.intent)
        self.assertEqual(result.emergency_status, "EXIT_RECONCILIATION_UNVERIFIED")
        self.assertFalse(self.route(router).get("exit_reconciled", False))
        self.assertEqual(next(router.run_dir.glob("halted-exit-*.json")).read_bytes(), b"CONFLICTING_PUBLISHER")
        self.assert_halt(router)


if __name__ == "__main__":
    unittest.main()
