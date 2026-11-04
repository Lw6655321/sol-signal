"""Synthetic byte-image projection/import; no runtime, account or network IO."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import json
import sqlite3
import unittest
from unittest.mock import patch

from src import demo_emergency_projection as E
from src import demo_halted_resolution as R
from src import demo_successor as S
from tests import test_demo_halted_resolution as HISTORY


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return R.canonical_json(value)


def mutate(raw, sql, params=()):
    db = sqlite3.connect(":memory:")
    try:
        db.deserialize(raw)
        db.execute(sql, params)
        db.commit()
        return db.serialize()
    finally:
        db.close()


def rows(raw, table):
    db = R._database(raw)
    return R._rows(db, table, db[1][table][0])


class EmergencyProjectionTests(unittest.TestCase):
    def setUp(self):
        for target in ("socket.create_connection", "socket.socket.connect"):
            p = patch(target, side_effect=AssertionError("NO_NETWORK"))
            p.start(); self.addCleanup(p.stop)
        self.legacy = HISTORY.HaltedResolutionTests()
        self.legacy.setUp()
        self.addCleanup(self.legacy.doCleanups)
        bundle = self.legacy.bundle()
        self.source_seed = S.project_validated_resolution(*bundle, source_commit="1"*40,
            target_commit="2"*40, resolved_at_ms=self.legacy.end+100)
        self.target_seed = S.project_validated_resolution(*bundle, source_commit="1"*40,
            target_commit="3"*40, resolved_at_ms=self.legacy.end+200)
        self.signal = "DEMO-TEST-SWING-SYNTHETIC"
        self.client = "h"+sha(self.signal.encode())[:31]
        self.created = self.legacy.created+10000
        self.flat, self.opened, self.closed = self.created+10, self.created+100, self.created+500
        self.observed = self.created+1000
        self.resolved = self.observed+200
        self.intent = dict(self.legacy.intent, signal_id=self.signal, side="short", kind="swing_1h4h",
            created_at_ms=self.created, expires_at_ms=self.created+60000, stop="102", tp1="97", tp2="96")
        self.route = dict(signal_id=self.signal, client_order_id=self.client, status="HALTED", code="PROTECTION_FAILED",
            account_hash=self.legacy.account, side="short", inst_id="SOL-USDT-SWAP", created_at_ms=self.created,
            expires_at_ms=self.created+60000, submit_attempted=True, filled_qty="0.01", protected_qty="0",
            order_id="501", entry_state="filled", protection_id="syntheticprotection", protection_order_id="",
            flat_observed_at_ms=self.flat, position_id=None, position_opened_at_ms=0,
            confirmed_closed_qty="0.01", emergency_status="FLAT_CONFIRMED", close_order_ids=["601"],
            close_id="", close_order_id="", close_qty="0", cancel_requested=False,
            protection_requested_qty="0.01", marker={"preserve": [1, True, "1"]})
        self.plan_signal = dict(id=self.signal, instId="SOL-USDT-SWAP", kind="swing_1h4h", side="short",
            entry="100", stop="102", tp1="97", tp2="96", qty="0.01", generated_at_ms=self.created,
            expires_at_ms=self.created+60000, require_maker=True)
        self.plan = dict(purpose="DEMO_TECHNICAL_TEST", scenario="swing",
            authorization_id="user-approved-demo-technical-tests-20261019", signal=self.plan_signal)
        self.working_c = self.add_signal(self.source_seed.controller, self.signal, "HALTED", "PROTECTION_FAILED", self.intent)
        self.working_c = mutate(self.working_c, "INSERT INTO controller_meta VALUES('halt','PROTECTION_FAILED')")
        self.working_c = mutate(self.working_c, "INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)", (
            self.signal, self.plan["purpose"], self.plan["scenario"], self.plan["authorization_id"],
            sha(canonical(self.plan)), canonical(self.plan_signal).decode(), self.created))
        self.working_r = self.add_route(self.source_seed.router, self.signal, "HALTED", self.intent, self.route)
        self.working_r = mutate(self.working_r, "INSERT INTO controls VALUES(?,?)", (R.ACCOUNT_KEY, "PROTECTION_FAILED"))
        close_client = "r"+sha((self.signal+":emergency:0:0.01").encode())[:31]
        entry = dict(instId="SOL-USDT-SWAP", ordId="501", clOrdId=self.client, side="sell", posSide="net",
            tdMode="cross", ordType="post_only", state="filled", sz="0.01", accFillSz="0.01", px="100",
            avgPx="100", reduceOnly="false", algoId="", cTime=str(self.opened-1), fillTime=str(self.opened), uTime=str(self.opened+1))
        close = dict(instId="SOL-USDT-SWAP", ordId="601", clOrdId=close_client, side="buy", posSide="net",
            tdMode="cross", ordType="market", state="filled", sz="0.01", accFillSz="0.01", avgPx="99",
            reduceOnly="true", algoId="", cTime=str(self.closed-1), fillTime=str(self.closed), uTime=str(self.closed))
        fills = []
        for identity, client, side, stamp, price, pnl, subtype, index in (
                ("501", self.client, "sell", self.opened, "100", "0", "4", "1"),
                ("601", close_client, "buy", self.closed, "99", "0.01", "6", "2")):
            fills.append(dict(instId="SOL-USDT-SWAP", instType="SWAP", posSide="net", side=side,
                ordId=identity, clOrdId=client, tradeId="t"+index, billId="b"+index, fillTime=str(stamp),
                ts=str(stamp+1), fillSz="0.01", fillPx=price, fillPnl=pnl, fee="-0.001", feeCcy="USDT", subType=subtype, posId="701"))
        exposure_raw = {"positions": [], "pending": [], **{"algo:"+k: [] for k in E.ALGO_TYPES}}
        self.proof = dict(schema=1, environment="DEMO", purpose="HALTED_EMERGENCY_CLOSURE_RECEIPT_ONLY",
            diagnostic_only=True, can_resume=False, can_activate=False, emergency_closed_lifecycle_verified=True,
            position_binding_verified=True, closed_lifecycle_verified=True, account_hash=self.legacy.account,
            intent_sha256=sha(canonical(self.intent)), entry_order_id="501", entry_client_id=self.client,
            retained_close_order_ids=["601"], position_id="701", position_opened_at_ms=self.opened+1,
            position_closed_at_ms=self.closed+1, flat_observed_at_ms=self.flat, observed_at_ms=self.observed,
            history_through_ms=self.observed+10, collection_started_at_ms=self.observed,
            collection_completed_at_ms=self.observed+100, collection_elapsed_monotonic_ns=100_000_000,
            confirmed_closed_qty="0.01", order=entry, closes=[close], fills=fills,
            position_history=dict(instId="SOL-USDT-SWAP", instType="SWAP", posId="701", posSide="net",
                mgnMode="cross", ccy="USDT", direction="short", cTime=str(self.opened+1), uTime=str(self.closed+1),
                openMaxPos="0.01", closeTotalPos="0.01", openAvgPx="100", closeAvgPx="99", pnl="0.01",
                fee="-0.002", fundingFee="0", realizedPnl="0.008", liqPenalty="0", settledPnl="0"),
            exposure_before={"positions": [], "pending": [], "algos": []},
            exposure_after={"positions": [], "pending": [], "algos": []}, exposure_raw_before=exposure_raw,
            exposure_raw_after=deepcopy(exposure_raw), exposure_algo_types=list(E.ALGO_TYPES))

    def add_signal(self, raw, signal, status, code, intent):
        return mutate(raw, "INSERT INTO controller_signals VALUES(?,?,?,?,?)", (
            signal, status, code, json.dumps(intent, sort_keys=True), self.created))

    def add_route(self, raw, signal, status, intent, data):
        return mutate(raw, "INSERT INTO routes VALUES(?,?,?,?,?,?,?)", (
            R.ACCOUNT_KEY, signal, sha(canonical(intent)), data["client_order_id"], status, "", json.dumps(data, sort_keys=True)))

    def arguments(self, **changes):
        args = dict(source_seed_controller=self.source_seed.controller, source_seed_router=self.source_seed.router,
            source_working_controller=self.working_c, source_working_router=self.working_r,
            target_seed_controller=self.target_seed.controller, target_seed_router=self.target_seed.router,
            receipt_raw=canonical(self.proof), resolved_at_ms=self.resolved)
        args.update(changes)
        names = ("source_seed_controller", "source_seed_router", "source_working_controller", "source_working_router", "target_seed_controller", "target_seed_router")
        pin_names = ("source_seed_controller", "source_seed_router", "source_controller", "source_router", "target_seed_controller", "target_seed_router")
        pins = {p+"_sha256": sha(args[n]) for n, p in zip(names, pin_names)}
        pins.update(receipt_sha256=sha(canonical(R._json(args["receipt_raw"]))), intent_sha256=sha(canonical(self.intent)),
            incident_route_sha256=sha(canonical(self.route)), account_hash=self.legacy.account, signal_id=self.signal)
        args["pins"] = E.EmergencyProjectionPins(**pins)
        return args

    def project(self, **changes):
        return E.project_emergency_terminal_extension(**self.arguments(**changes))

    def refused(self, **changes):
        with self.assertRaises(E.EmergencyProjectionRefused):
            self.project(**changes)

    def route_change(self, **changes):
        data = dict(self.route, **changes)
        raw = mutate(self.working_r, "UPDATE routes SET data=? WHERE signal=?", (json.dumps(data), self.signal))
        return data, raw

    def test_success_retains_target_legacy_and_exact_terminal_delta(self):
        before = (self.working_c, self.working_r, deepcopy(self.proof), deepcopy(self.intent), deepcopy(self.route))
        plan = self.project()
        self.assertEqual(plan.imported_counts, (("controller.controller_signals", 1), ("controller.technical_tests", 1), ("router.routes", 1)))
        for table in ("controller_meta", "halted_resolutions", "natural_exits"):
            self.assertEqual(rows(plan.controller, table), rows(self.target_seed.controller, table))
        for table in ("controls", "halted_resolutions"):
            self.assertEqual(rows(plan.router, table), rows(self.target_seed.router, table))
        route = [r for r in rows(plan.router, "routes") if r["signal"] == self.signal][0]
        projected = json.loads(route["data"])
        expected = dict(self.route, status="CLOSED", code=E.CLOSED_CODE, position_id="701",
            position_opened_at_ms=self.opened+1, position_closed_at_ms=self.closed+1,
            emergency_close_receipt_sha256=sha(canonical(self.proof)), emergency_close_receipt_path=plan.receipt_path)
        self.assertEqual(projected, expected)
        self.assertEqual(route["fingerprint"], sha(canonical(self.intent)))
        self.assertEqual(route["client"], self.client)
        self.assertFalse(any(k.startswith("exit_reconcile") for k in projected))
        self.assertTrue(all(json.loads(row["data"])["account_hash"] == self.legacy.account
                            and row["account"] == R.ACCOUNT_KEY for row in rows(plan.router, "routes")))
        S.validate_journal_extension(self.source_seed.controller, self.source_seed.router, plan.projected_source_controller, plan.projected_source_router)
        S.validate_journal_extension(self.target_seed.controller, self.target_seed.router, plan.controller, plan.router)
        self.assertEqual(before, (self.working_c, self.working_r, self.proof, self.intent, self.route))

    def test_all_output_hashes_and_provenance_are_exact_and_immutable(self):
        plan = self.project()
        for name in ("controller", "router", "projected_source_controller", "projected_source_router", "receipt", "provenance"):
            self.assertEqual(sha(getattr(plan, name)), getattr(plan, name+"_sha256"))
        for name in ("can_resume", "can_activate", "activation_allowed", "fresh_account_verified", "exchange_authenticity_verified"):
            self.assertIs(getattr(plan, name), False)
        self.assertIs(plan.preparation_only, True)
        self.assertEqual(plan.receipt_path, "logs/emergency-receipts/"+plan.receipt_sha256+".json")
        with self.assertRaises(FrozenInstanceError):
            plan.can_resume = True
        self.assertEqual(self.project(), plan)

    def test_every_pin_is_external_and_mismatch_refuses(self):
        for descriptor in fields(E.EmergencyProjectionPins):
            with self.subTest(pin=descriptor.name):
                args = self.arguments()
                replacement = "wrong-signal" if descriptor.name == "signal_id" else "0"*64
                args["pins"] = replace(args["pins"], **{descriptor.name: replacement})
                with self.assertRaises(E.EmergencyProjectionRefused):
                    E.project_emergency_terminal_extension(**args)

    def test_receipt_is_full_typed_nonadmitting_contract(self):
        original = deepcopy(self.proof)
        mutations = {"schema": True, "environment": "LIVE", "purpose": "OTHER", "diagnostic_only": 1,
            "can_resume": True, "can_activate": True, "position_binding_verified": False,
            "closed_lifecycle_verified": False, "emergency_closed_lifecycle_verified": False,
            "account_hash": "b"*64, "intent_sha256": "0"*64, "entry_order_id": "502", "entry_client_id": "other",
            "retained_close_order_ids": ["602"], "confirmed_closed_qty": "0", "position_id": "",
            "flat_observed_at_ms": self.flat+1}
        for key, value in mutations.items():
            with self.subTest(field=key):
                self.refused(receipt_raw=canonical(dict(original, **{key: value})))
        for key in original:
            with self.subTest(missing=key):
                reduced = {k: v for k, v in original.items() if k != key}
                self.refused(receipt_raw=canonical(reduced))
        self.refused(receipt_raw=canonical(dict(original, exit_reconciled=True)))

    def test_duplicate_json_and_nonfinite_are_rejected(self):
        args = self.arguments()
        for raw in (b'{"schema":1,"schema":1}', b'{"x":NaN}', b'{"x":Infinity}', b'[]', b'null'):
            with self.subTest(raw=raw), self.assertRaises(E.EmergencyProjectionRefused):
                E.project_emergency_terminal_extension(**dict(args, receipt_raw=raw))

    def test_canonical_receipt_pin_accepts_formatting_without_losing_raw_provenance(self):
        raw = json.dumps(self.proof, indent=2).encode()+b"\n"
        plan = self.project(receipt_raw=raw)
        self.assertEqual(plan.receipt, canonical(self.proof))
        self.assertEqual(json.loads(plan.provenance)["receipt_input_sha256"], sha(raw))

    def test_wall_monotonic_order_and_resolution_deadlines(self):
        mutations = {"collection_completed_at_ms": self.observed+5001,
            "collection_started_at_ms": self.observed+101, "collection_elapsed_monotonic_ns": 5_000_000_001,
            "position_closed_at_ms": self.observed+1, "observed_at_ms": self.closed,
            "history_through_ms": self.observed+101}
        for key, value in mutations.items():
            with self.subTest(field=key):
                self.refused(receipt_raw=canonical(dict(self.proof, **{key: value})))
        self.refused(resolved_at_ms=self.observed+99)
        for value in (True, 1.0, -1, 0):
            self.refused(resolved_at_ms=value)

    def test_no_source_history_row_may_change_or_disappear(self):
        for sql, params in (("DELETE FROM controller_signals WHERE signal=?", (R.SIGNAL,)),
                ("UPDATE controller_signals SET updated_ms=updated_ms+1 WHERE signal=?", (R.SIGNAL,)),
                ("UPDATE technical_tests SET registered_at_ms=registered_at_ms+1 WHERE signal=?", (R.SIGNAL,)),
                ("UPDATE halted_resolutions SET record='{}'", ())):
            with self.subTest(sql=sql):
                self.refused(source_working_controller=mutate(self.working_c, sql, params))
        self.refused(source_working_router=mutate(self.working_r, "DELETE FROM routes WHERE signal=?", (R.SIGNAL,)))

    def test_only_exact_known_halt_and_control_are_removable(self):
        for value in ("UNKNOWN", "PROTECTION_FAILED ", "", "LIVE"):
            with self.subTest(code=value):
                self.refused(source_working_controller=mutate(self.working_c, "UPDATE controller_meta SET value=? WHERE key='halt'", (value,)))
                self.refused(source_working_router=mutate(self.working_r, "UPDATE controls SET code=?", (value,)))
        self.refused(source_working_router=mutate(self.working_r, "INSERT INTO controls VALUES('FOREIGN','PROTECTION_FAILED')"))
        self.refused(source_working_controller=mutate(self.working_c, "INSERT INTO controller_meta VALUES('extra','x')"))
        self.refused(source_working_controller=mutate(self.working_c, "DELETE FROM controller_meta WHERE key='halt'"))

    def test_active_unknown_or_owned_work_cannot_be_imported(self):
        for status in ("HALTED", "UNKNOWN", "PENDING", "PROTECTED", "CLOSING"):
            with self.subTest(status=status):
                c = self.add_signal(self.working_c, "extra", status, "OTHER", {})
                self.refused(source_working_controller=c)
        self.refused(source_working_router=mutate(self.working_r, "UPDATE routes SET owner='owner' WHERE signal=?", (self.signal,)))

    def test_all_existing_terminal_outcomes_are_imported_without_loss(self):
        c, r = self.working_c, self.working_r
        for index, status in enumerate(("CLOSED", "CANCELED", "REJECTED")):
            signal = "natural"+str(index)
            intent = dict(self.intent, signal_id=signal)
            data = dict(self.route, signal_id=signal, status=status, code="OTHER", client_order_id=signal)
            c = self.add_signal(c, signal, status, "OTHER", intent)
            r = self.add_route(r, signal, status, intent, data)
        result = self.project(source_working_controller=c, source_working_router=r)
        self.assertEqual(dict(result.imported_counts)["controller.controller_signals"], 4)
        self.assertEqual(dict(result.imported_counts)["router.routes"], 4)
        self.assertEqual(len(rows(result.router, "routes")), len(rows(self.target_seed.router, "routes"))+4)

    def test_terminal_controller_dedupe_without_route_survives(self):
        c = self.add_signal(self.working_c, "risk-rejected", "REJECTED", "RISK", {})
        result = self.project(source_working_controller=c)
        self.assertIn("risk-rejected", {r["signal"] for r in rows(result.controller, "controller_signals")})

    def test_only_rejected_controller_rows_may_lack_a_route(self):
        for status in ("CLOSED", "CANCELED"):
            with self.subTest(status=status):
                c = self.add_signal(self.working_c, "unmatched", status, "OTHER", {})
                self.refused(source_working_controller=c)

    def test_target_same_key_collision_never_replaces_or_ignores(self):
        c = self.add_signal(self.target_seed.controller, self.signal, "CLOSED", "OTHER", self.intent)
        self.refused(target_seed_controller=c)
        r = self.add_route(self.target_seed.router, self.signal, "CLOSED", self.intent, dict(self.route, status="CLOSED"))
        self.refused(target_seed_router=r)

    def test_target_cannot_drop_or_modify_seed_history(self):
        self.refused(target_seed_controller=mutate(self.target_seed.controller, "DELETE FROM technical_tests"))
        self.refused(target_seed_router=mutate(self.target_seed.router, "DELETE FROM routes WHERE signal='prior-canceled'"))
        self.refused(target_seed_controller=mutate(self.target_seed.controller, "UPDATE controller_signals SET code='CHANGED' WHERE signal='prior-canceled'"))

    def test_foreign_new_receipt_and_route_cannot_rebind_retained_seed_account(self):
        foreign = "b"*64
        data, working = self.route_change(account_hash=foreign)
        proof = dict(self.proof, account_hash=foreign)
        args = self.arguments(source_working_router=working, receipt_raw=canonical(proof))
        args["pins"] = replace(args["pins"], account_hash=foreign,
            incident_route_sha256=sha(canonical(data)))
        with self.assertRaisesRegex(E.EmergencyProjectionRefused, "EMERGENCY_SEED_ACCOUNT_MISMATCH"):
            E.project_emergency_terminal_extension(**args)

    def test_self_consistent_legacy_seed_rebinding_cannot_mix_new_account_history(self):
        def rebind(craw, rraw):
            record = json.loads(rows(craw, "halted_resolutions")[0]["record"])
            record["account_hash"] = "b"*64
            original = json.loads(record["original_router_row"]["data"])
            record["original_router_row"]["data"] = json.dumps(dict(original, account_hash="b"*64))
            craw = mutate(craw, "UPDATE halted_resolutions SET record=?", (json.dumps(record),))
            rraw = mutate(rraw, "UPDATE halted_resolutions SET record=?", (json.dumps(record),))
            legacy = next(row for row in rows(rraw, "routes") if row["signal"] == R.SIGNAL)
            data = dict(json.loads(legacy["data"]), account_hash="b"*64)
            rraw = mutate(rraw, "UPDATE routes SET data=? WHERE signal=?", (json.dumps(data), R.SIGNAL))
            return craw, rraw
        sc, sr = rebind(self.source_seed.controller, self.source_seed.router)
        tc, tr = rebind(self.target_seed.controller, self.target_seed.router)
        wc, wr = rebind(self.working_c, self.working_r)
        # Both existing seed gates still accept these internally consistent
        # historical pairs; the external account binding must reject the mix.
        S.validate_journal_extension(sc, sr, sc, sr)
        S.validate_journal_extension(tc, tr, tc, tr)
        with self.assertRaisesRegex(E.EmergencyProjectionRefused, "EMERGENCY_SEED_ACCOUNT_MISMATCH"):
            self.project(source_seed_controller=sc, source_seed_router=sr,
                source_working_controller=wc, source_working_router=wr,
                target_seed_controller=tc, target_seed_router=tr)

    def test_every_retained_seed_route_account_is_required_even_when_history_matches(self):
        for foreign, sql_account in (("b"*64, R.ACCOUNT_KEY), (None, R.ACCOUNT_KEY), (self.legacy.account, "FOREIGN")):
            def change(raw):
                row = next(row for row in rows(raw, "routes") if row["signal"] == "prior-canceled")
                data = json.loads(row["data"])
                if foreign is None:
                    del data["account_hash"]
                else:
                    data["account_hash"] = foreign
                return mutate(raw, "UPDATE routes SET account=?,data=? WHERE signal='prior-canceled'",
                    (sql_account, json.dumps(data)))
            with self.subTest(foreign=foreign, sql_account=sql_account), \
                 self.assertRaisesRegex(E.EmergencyProjectionRefused, "EMERGENCY_SEED_ACCOUNT_MISMATCH"):
                self.project(source_seed_router=change(self.source_seed.router),
                    source_working_router=change(self.working_r), target_seed_router=change(self.target_seed.router))

    def test_unknown_table_view_trigger_and_schema_change_refuse(self):
        statements = ("CREATE TABLE unknown(value TEXT)", "CREATE VIEW hostile AS SELECT 1", "ALTER TABLE controller_signals ADD COLUMN extra TEXT",
            "CREATE TRIGGER hostile AFTER UPDATE ON controller_signals BEGIN DELETE FROM technical_tests; END")
        for sql in statements:
            with self.subTest(sql=sql):
                self.refused(source_working_controller=mutate(self.working_c, sql))
        self.refused(source_working_router=b"NOT SQLITE")
        self.refused(source_working_controller=bytearray(self.working_c))

    def test_unknown_natural_exit_delta_is_not_silently_copied(self):
        c = mutate(self.working_c, "INSERT INTO natural_exits VALUES('new','{}')")
        self.refused(source_working_controller=c)

    def test_registry_counter_edit_or_orphan_or_fingerprint_refuses(self):
        for sql, params in (("UPDATE technical_tests SET plan_fingerprint='bad' WHERE signal=?", (self.signal,)),
                ("UPDATE technical_tests SET signal_json='{}' WHERE signal=?", (self.signal,)),
                ("UPDATE technical_tests SET purpose='UNKNOWN' WHERE signal=?", (self.signal,)),
                ("UPDATE technical_tests SET signal='orphan' WHERE signal=?", (self.signal,))):
            with self.subTest(sql=sql):
                self.refused(source_working_controller=mutate(self.working_c, sql, params))

    def test_route_fingerprint_client_and_account_cannot_change(self):
        for sql, params in (("UPDATE routes SET fingerprint='bad' WHERE signal=?", (self.signal,)),
                ("UPDATE routes SET client='other' WHERE signal=?", (self.signal,)),
                ("UPDATE routes SET account='FOREIGN' WHERE signal=?", (self.signal,))):
            with self.subTest(sql=sql):
                self.refused(source_working_router=mutate(self.working_r, sql, params))

    def test_route_safety_predicates_are_required_even_with_matching_pin(self):
        for key, value in (("emergency_status", "CLOSE_UNCONFIRMED"), ("entry_state", "partially_filled"),
                ("submit_attempted", False), ("protected_qty", "0.01"), ("filled_qty", "0.02"),
                ("confirmed_closed_qty", "0"), ("account_hash", "b"*64), ("exit_reconciled", True)):
            with self.subTest(field=key):
                data, raw = self.route_change(**{key: value})
                args = self.arguments(source_working_router=raw)
                args["pins"] = replace(args["pins"], incident_route_sha256=sha(canonical(data)))
                with self.assertRaises(E.EmergencyProjectionRefused):
                    E.project_emergency_terminal_extension(**args)

    def test_retained_close_role_or_quantity_cannot_be_guessed(self):
        for field, value in (("clOrdId", "unrelated"), ("ordType", "limit"), ("side", "sell"),
                ("reduceOnly", "false"), ("sz", "0.02"), ("algoId", "999")):
            with self.subTest(field=field):
                proof = deepcopy(self.proof); proof["closes"][0][field] = value
                self.refused(receipt_raw=canonical(proof))
        proof = deepcopy(self.proof); proof["closes"].append(deepcopy(proof["closes"][0]))
        self.refused(receipt_raw=canonical(proof))

    def test_missing_or_duplicate_or_external_fills_refuse(self):
        for fills in ([], [self.proof["fills"][0]], self.proof["fills"]*2):
            with self.subTest(size=len(fills)):
                self.refused(receipt_raw=canonical(dict(self.proof, fills=fills)))
        proof = deepcopy(self.proof); proof["fills"][1]["ordId"] = "999"
        self.refused(receipt_raw=canonical(proof))

    def test_price_fee_pnl_and_liquidation_contradictions_refuse(self):
        for location, field, value in (("fill", "fee", "-1"), ("fill", "fillPx", "90"),
                ("fill", "fillPnl", "99"), ("history", "fee", "-1"), ("history", "pnl", "1"),
                ("history", "realizedPnl", "0"), ("history", "liqPenalty", "1"),
                ("history", "openAvgPx", "99"), ("close", "avgPx", "98")):
            with self.subTest(location=location, field=field):
                proof = deepcopy(self.proof)
                record = proof["fills"][1] if location == "fill" else proof["closes"][0] if location == "close" else proof["position_history"]
                record[field] = value
                self.refused(receipt_raw=canonical(proof))

    def test_all_eight_algo_types_and_both_raw_flat_waves_are_required(self):
        for suffix in ("before", "after"):
            for kind in E.ALGO_TYPES:
                with self.subTest(wave=suffix, kind=kind):
                    proof = deepcopy(self.proof); del proof["exposure_raw_"+suffix]["algo:"+kind]
                    self.refused(receipt_raw=canonical(proof))
            proof = deepcopy(self.proof); proof["exposure_raw_"+suffix]["positions"] = [{"instId":"SOL-USDT-SWAP", "posSide":"net", "pos":"-0.01"}]
            self.refused(receipt_raw=canonical(proof))
        self.refused(receipt_raw=canonical(dict(self.proof, exposure_algo_types=list(E.ALGO_TYPES[:-1]))))

    def test_conflicting_existing_position_binding_refuses(self):
        for key, value in (("position_id", "other"), ("position_opened_at_ms", self.opened+2), ("position_closed_at_ms", self.closed+2)):
            data, raw = self.route_change(**{key: value})
            args = self.arguments(source_working_router=raw)
            args["pins"] = replace(args["pins"], incident_route_sha256=sha(canonical(data)))
            with self.assertRaises(E.EmergencyProjectionRefused):
                E.project_emergency_terminal_extension(**args)

    def test_no_filesystem_open_or_connect_other_than_memory(self):
        args = self.arguments()
        connect = sqlite3.connect
        def memory_only(name, *positional, **kwargs):
            self.assertEqual(name, ":memory:")
            return connect(name, *positional, **kwargs)
        with patch("builtins.open", side_effect=AssertionError("NO_FILESYSTEM")), \
             patch("pathlib.Path.open", side_effect=AssertionError("NO_FILESYSTEM")), \
             patch.object(E.sqlite3, "connect", side_effect=memory_only):
            plan = E.project_emergency_terminal_extension(**args)
        self.assertFalse(plan.can_activate)

    def test_existing_typed_verifier_receipt_is_compatible_over_fake_http(self):
        from test_demo_emergency_receipt import EmergencyClosedReceiptTests
        fixture = EmergencyClosedReceiptTests()
        fixture.setUp()
        try:
            proof = fixture.verify()
            intent = fixture.f.intent.to_dict()
            data = dict(self.route, signal_id=intent["signal_id"], client_order_id=fixture.f.client,
                order_id="101", close_order_ids=fixture.ids, filled_qty=str(fixture.f.intent.qty),
                flat_observed_at_ms=fixture.f.flat)
            pins = replace(self.arguments()["pins"], signal_id=intent["signal_id"], account_hash=fixture.f.account,
                receipt_sha256=sha(canonical(proof)), intent_sha256=sha(canonical(intent)))
            checked = E._receipt(canonical(proof), pins, data, intent, proof["collection_completed_at_ms"]+100)
            self.assertEqual(checked, proof)
            self.assertEqual(fixture.f.posts, [])
        finally:
            fixture.doCleanups()

    def test_both_existing_extension_gates_are_required(self):
        args = self.arguments()
        original = S.validate_journal_extension
        for index in (3, 4):
            calls = []
            def checked(*values):
                calls.append(values)
                if len(calls) == index:
                    raise S.SuccessorRefused("SYNTHETIC_GATE_REFUSED")
                return original(*values)
            with self.subTest(gate=index), patch.object(S, "validate_journal_extension", side_effect=checked):
                with self.assertRaisesRegex(E.EmergencyProjectionRefused, "SYNTHETIC_GATE_REFUSED"):
                    E.project_emergency_terminal_extension(**args)
            self.assertEqual(len(calls), index)


if __name__ == "__main__":
    unittest.main()
