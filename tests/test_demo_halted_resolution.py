"""Synthetic bytes only: no actual report, runtime, account or credentials."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import base64
import hashlib
import json
from pathlib import Path, PureWindowsPath
import sqlite3
import unittest
from unittest import mock

from src import demo_halted_resolution as R


def raw(value):
    return json.dumps(value, sort_keys=True).encode("utf-8")


def sha(value):
    return hashlib.sha256(value).hexdigest()


def database(kind, row, intent, plan, *, owner="", extra_status=None, view=False):
    """Produce genuine SQLite bytes entirely in memory, never a DB path."""
    with sqlite3.connect(":memory:") as db:
        if kind == "router":
            db.execute("CREATE TABLE routes(account TEXT,signal TEXT,fingerprint TEXT,client TEXT,status TEXT,owner TEXT,data TEXT,PRIMARY KEY(account,signal))")
            db.execute("CREATE TABLE controls(account TEXT PRIMARY KEY,code TEXT)")
            db.execute("INSERT INTO controls VALUES(?,?)", (R.ACCOUNT_KEY, "ORIGINAL_HALT"))
            db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)", (R.ACCOUNT_KEY, R.SIGNAL,
                sha(R.canonical_json(intent)), row["client_order_id"], row["status"], owner, json.dumps(row)))
            other = {"signal_id": "prior-canceled", "status": extra_status or "CANCELED", "account_hash": "a"*64}
            db.execute("INSERT INTO routes VALUES(?,?,?,?,?,?,?)", (R.ACCOUNT_KEY, other["signal_id"],
                "f"*64, "priorcanceled", other["status"], "", json.dumps(other)))
        else:
            db.execute("CREATE TABLE controller_meta(key TEXT PRIMARY KEY,value TEXT)")
            db.executemany("INSERT INTO controller_meta VALUES(?,?)", (("account", R.ACCOUNT_KEY), ("schema", "1"), ("halt", "ORIGINAL_HALT")))
            db.execute("CREATE TABLE controller_signals(signal TEXT PRIMARY KEY,status TEXT,code TEXT,intent TEXT,updated_ms INTEGER)")
            db.execute("INSERT INTO controller_signals VALUES(?,?,?,?,?)", (R.SIGNAL, "HALTED", "OBSERVE_HALTED", json.dumps(intent), intent["created_at_ms"]))
            db.execute("INSERT INTO controller_signals VALUES('prior-canceled','CANCELED','OLD','{}',1)")
            db.execute("CREATE TABLE technical_tests(signal TEXT PRIMARY KEY,purpose TEXT,scenario TEXT,authorization_id TEXT,plan_fingerprint TEXT,signal_json TEXT,registered_at_ms INTEGER)")
            db.execute("INSERT INTO technical_tests VALUES(?,?,?,?,?,?,?)", (R.SIGNAL, plan["purpose"], plan["scenario"],
                plan["authorization_id"], sha(R.canonical_json(plan)), json.dumps(plan["signal"]), intent["created_at_ms"]+1))
        if view:
            db.execute("CREATE VIEW untrusted_view AS SELECT 1")
        db.commit()
        result = db.serialize()
    db.close()
    return result


class HaltedResolutionTests(unittest.TestCase):
    def setUp(self):
        self.created = 1792501260297
        self.flat = self.created + 10
        self.opened, self.closed = self.flat + 100, self.flat + 500
        self.start, self.end = self.flat + 1000, self.flat + 1100
        self.account = "a"*64
        self.client = "h" + sha(R.SIGNAL.encode())[:31]
        self.protection = "r" + sha((R.SIGNAL + ":protection").encode())[:31]
        self.intent = dict(signal_id=R.SIGNAL, inst_id="SOL-USDT-SWAP", side="long", entry="100", stop="98",
            tp1="103", tp2="105", qty="0.01", notional="1", contract_value="1", lot_size="0.01", tick_size="0.01",
            created_at_ms=self.created, expires_at_ms=self.created+60000, maker_fee_rate="0.0002", taker_fee_rate="0.0005",
            slippage_bps="2", require_maker=True, risk_budget="0.02", effective_risk="0.02", rr="1.5", net_rr="1.2", kind="DEMO_SMOKE")
        self.plan = dict(purpose="DEMO_TECHNICAL_TEST", scenario="smoke", authorization_id="user-approved-demo-technical-tests-20261019",
            signal=dict(id=R.SIGNAL, instId="SOL-USDT-SWAP", side="long", kind="DEMO_SMOKE", entry="100", stop="98",
                tp1="103", tp2="105", qty="0.01", generated_at_ms=self.created, expires_at_ms=self.created+60000, require_maker=True))
        self.old = dict(signal_id=R.SIGNAL, client_order_id=self.client, status="HALTED", code="OBSERVE_HALTED",
            account_hash=self.account, side="long", inst_id="SOL-USDT-SWAP", created_at_ms=self.created,
            expires_at_ms=self.created+60000, submit_attempted=True, filled_qty="0.01", protected_qty="0.01",
            order_id="101", entry_state="filled", protection_id=self.protection, protection_order_id="201",
            flat_observed_at_ms=self.flat, position_id=None, confirmed_closed_qty="0", emergency_status="EXPOSURE_UNVERIFIED",
            close_id="stableoriginalclose", close_order_ids=[], marker=True)
        entry = dict(instId="SOL-USDT-SWAP", ordId="101", clOrdId=self.client, side="buy", posSide="net", tdMode="cross",
            ordType="post_only", state="filled", sz="0.01", accFillSz="0.01", px="100", avgPx="100", reduceOnly="false", algoId="",
            cTime=str(self.opened-1), fillTime=str(self.opened), uTime=str(self.opened+1))
        parent = dict(instId="SOL-USDT-SWAP", algoId="201", algoClOrdId=self.protection, side="sell", posSide="net", tdMode="cross",
            ordType="oco", state="effective", sz="0.01", reduceOnly="true", actualSide="tp", slTriggerPx="98", tpTriggerPx="103",
            slTriggerPxType="last", tpTriggerPxType="last", slOrdPx="-1", tpOrdPx="-1", ordIdList=["301"], ordId="301",
            cTime=str(self.opened+2), triggerTime=str(self.closed-1), uTime=str(self.closed-1))
        child = dict(instId="SOL-USDT-SWAP", ordId="301", clOrdId="", algoId="201", algoClOrdId=self.protection,
            side="sell", posSide="net", tdMode="cross", ordType="market", state="filled", sz="0.01", accFillSz="0.01",
            avgPx="103", reduceOnly="true", cTime=str(self.closed-1), fillTime=str(self.closed), uTime=str(self.closed))
        fills = [dict(instId="SOL-USDT-SWAP", instType="SWAP", posSide="net", side="buy", ordId="101", clOrdId=self.client,
            tradeId="t1", billId="b1", fillTime=str(self.opened), ts=str(self.opened+1), fillSz="0.01", fillPx="100", fillPnl="0",
            fee="-0.01", feeCcy="USDT", subType="1", posId="401"),
            dict(instId="SOL-USDT-SWAP", instType="SWAP", posSide="net", side="sell", ordId="301", clOrdId="",
            tradeId="t2", billId="b2", fillTime=str(self.closed), ts=str(self.closed), fillSz="0.01", fillPx="103", fillPnl="0.03",
            fee="-0.01", feeCcy="USDT", subType="2", posId="401")]
        history = dict(instId="SOL-USDT-SWAP", instType="SWAP", posId="401", posSide="net", mgnMode="cross", ccy="USDT",
            direction="long", cTime=str(self.opened+1), uTime=str(self.closed), openMaxPos="0.01", closeTotalPos="0.01",
            openAvgPx="100", closeAvgPx="103", liqPenalty="0", settledPnl="")
        self.proof = dict(position_binding_verified=True, closed_lifecycle_verified=True, exit_reconciled=True,
            account_hash=self.account, position_id="401", position_opened_at_ms=self.opened+1, position_closed_at_ms=self.closed,
            flat_observed_at_ms=self.flat, observed_at_ms=self.start, history_through_ms=self.end, confirmed_closed_qty="0.01",
            exit_order_ids=["301"], order=entry, parent=parent, children=[child], fills=fills, position_history=history,
            exposure_before=dict(positions=[], pending=[], algos=[]), exposure_after=dict(positions=[], pending=[], algos=[]))
        self.auth = dict(environment="DEMO", account_hash=self.account, authorization_recorded_at_ms=self.created-100,
            source="user-approved-demo-activation", scope="SOL-USDT-SWAP_EXISTING_RISK")
        self.fail_path = "logs/technical-tests/smoke-FAKE/report.json"
        self.fail = dict(purpose="DEMO_TECHNICAL_TEST_REPORT", scenario="smoke", result="FAIL", submit_attempted=True, plan=self.plan)
        self.owner = ""
        self.extra_status = None
        self.view = False
        self.current_changes = {}
        self.original_extra = {}
        self.observed_extra = {}
        self.proof_bytes = None
        self.root = r"C:\FAKE\demo-archive"
        self.proof_log_root = "logs"

    def bundle(self):
        payload = raw(self.proof) if self.proof_bytes is None else self.proof_bytes
        proof_hash = sha(payload)
        proof_path = self.proof_log_root + "/router/run-FAKE/halted-exit-" + proof_hash + ".json"
        current = dict(self.old, **{key: self.proof.get(key) for key in (
            "position_id", "position_opened_at_ms", "position_closed_at_ms", "exit_order_ids", "confirmed_closed_qty")})
        current.update(exit_reconciled=True, protected_qty="0", exit_reconcile_code="DEMO_HALTED_EXIT_VERIFIED",
            exit_reconciled_observed_at_ms=self.proof.get("observed_at_ms"), exit_reconcile_sha256=proof_hash,
            exit_reconcile_proof_path=str(PureWindowsPath(self.root).joinpath(*proof_path.split("/"))),
            exit_reconcile_collection_started_at_ms=self.start-20, exit_reconcile_collection_completed_at_ms=self.end+20,
            exit_reconcile_collection_elapsed_monotonic_ns=140_000_000)
        current.update(self.current_changes)
        original = {
            R.CONTROLLER: database("controller", self.old, self.intent, self.plan, view=self.view),
            R.ROUTER: database("router", self.old, self.intent, self.plan, owner=self.owner, extra_status=self.extra_status),
            R.AUTHORIZATION: raw(self.auth), R.BINDING: raw(dict(environment="DEMO", account_hash=self.account)),
            R.STATE + "process.lock": b"0", R.STATE + "STOP_NEW_ENTRIES": b"ORIGINAL_STOP",
            R.STATE + "CONTROLLER_HALT": b"ORIGINAL_HALT",
            R.STATE + "HALT-" + sha(R.ACCOUNT_KEY.encode())[:12]: b"ORIGINAL_ROUTER_HALT",
            R.STATE + "scan-seen.json": b'{"old-dedupe":"retained"}',
            R.STATE + "verified-risk-v2/FAKE/verified.json": b'{"baseline":"retained-not-reconstructed"}',
            self.fail_path: raw(self.fail), "logs/technical-tests/smoke-FAKE/plan.json": raw(self.plan),
            "logs/r11-forensic-FAKE/old.log": b"OLD_R11_BYTES", "logs/r14-forensic-FAKE/old.log": b"OLD_R14_BYTES",
        }
        original.update(self.original_extra)
        observed = dict(original)
        observed[R.ROUTER] = database("router", current, self.intent, self.plan, owner=self.owner, extra_status=self.extra_status)
        observed[proof_path] = payload
        observed.update(self.observed_extra)
        a = tuple(R.EvidenceFile(name, value) for name, value in sorted(original.items()))
        b = tuple(R.EvidenceFile(name, value) for name, value in sorted(observed.items()))
        anchors = R.ResolutionAnchors(self.account, sha(R.canonical_json(self.intent)), sha(raw(self.auth)), sha(raw(self.fail)),
            R.archive_digest(a), R.archive_digest(b), proof_hash, self.fail_path, proof_path, self.root)
        return a, b, anchors

    def validate(self):
        return R.validate_archived_halted_resolution(*self.bundle())

    def refused(self, code=None):
        with self.assertRaises(R.ResolutionRefused) as caught:
            self.validate()
        if code:
            self.assertEqual(caught.exception.code, code)
        self.assertRegex(str(caught.exception), r"^RESOLUTION_[A-Z0-9_]+$")

    def test_consistent_archived_closure_is_frozen_and_never_successor_ready(self):
        plan = self.validate()
        self.assertFalse(plan.activation_allowed)
        self.assertFalse(plan.successor_ready)
        self.assertFalse(plan.diagnostic_admission_allowed)
        self.assertTrue(plan.proposed_only)
        self.assertEqual(plan.validation_scope, "ARCHIVED_CLOSURE_CONSISTENCY_ONLY")
        self.assertEqual(plan.blocking_gaps, R.BLOCKING_GAPS)
        self.assertEqual((plan.original_status, plan.original_code), ("HALTED", "OBSERVE_HALTED"))
        self.assertEqual(plan.exit_order_ids, ("301",))
        with self.assertRaises(FrozenInstanceError):
            plan.activation_allowed = True
        with self.assertRaises(ValueError):
            replace(plan, activation_allowed=True)

    def test_canonical_f15_uuid22_parent_preserves_archive_only_validation(self):
        for uuid_bytes in (bytes(16), bytes(range(16)), bytes([255]) * 16, bytes([251]) * 16):
            token = base64.urlsafe_b64encode(uuid_bytes).decode("ascii").rstrip("=")
            self.assertEqual(len(token), 22)
            self.proof_log_root = "logs/f15-" + token
            with self.subTest(token=token), \
                    mock.patch("builtins.open", side_effect=AssertionError("NO_FILE_READER")), \
                    mock.patch.object(Path, "read_bytes", side_effect=AssertionError("NO_FILE_READER")):
                result = self.validate()
            self.assertFalse(result.activation_allowed)
            self.assertFalse(result.successor_ready)
            self.assertEqual(result.validation_scope, "ARCHIVED_CLOSURE_CONSISTENCY_ONLY")

    def test_f15_parent_rejects_noncanonical_pad_bits_and_incomplete_uuid_bytes(self):
        # Different final characters can decode to the same 16 bytes; only the
        # exact URL-safe base64 round trip is accepted, without padding.
        for token in ("A" * 21 + "B", "A" * 21 + "_", "A" * 21, "A" * 23,
                      "A" * 22 + "==", "A" * 21 + "+", "A" * 21 + "/"):
            with self.subTest(token=token):
                self.proof_log_root = "logs/f15-" + token
                self.refused("RESOLUTION_PATH_INVALID")

    def test_only_one_exact_f15_optional_parent_is_accepted(self):
        token = "A" * 22
        for parent in ("logs/other", "logs/f14-" + token, "logs/F15-" + token,
                       "logs/f15-" + token + "/extra", "logs/extra/f15-" + token,
                       "logs/f15-" + token + "/f15-" + token):
            with self.subTest(parent=parent):
                self.proof_log_root = parent
                self.refused("RESOLUTION_PATH_INVALID")

    def test_f15_reference_must_still_match_exact_absolute_route_path(self):
        self.proof_log_root = "logs/f15-" + "A" * 22
        _, _, anchors = self.bundle()
        old_path = anchors.proof_path.replace("/f15-" + "A" * 22, "")
        self.current_changes["exit_reconcile_proof_path"] = str(
            PureWindowsPath(self.root).joinpath(*old_path.split("/")))
        self.refused("RESOLUTION_INCIDENT_DRIFT")

    def test_f15_layout_keeps_hash_anchors_and_five_second_collection_limit(self):
        self.proof_log_root = "logs/f15-" + "A" * 22
        original, observed, anchors = self.bundle()
        changed = tuple(R.EvidenceFile(item.name, item.raw + b" ")
                        if item.name == anchors.proof_path else item for item in observed)
        anchors = replace(anchors, observed_inventory_sha256=R.archive_digest(changed))
        with self.assertRaisesRegex(R.ResolutionRefused, "RESOLUTION_EVIDENCE_HASH_MISMATCH"):
            R.validate_archived_halted_resolution(original, changed, anchors)
        self.current_changes["exit_reconcile_collection_completed_at_ms"] = self.start + 5001
        self.refused("RESOLUTION_COLLECTION_STALE")

    def test_validation_does_not_mutate_any_supplied_bytes_or_source_records(self):
        bundle = self.bundle()
        before = deepcopy(bundle)
        R.validate_archived_halted_resolution(*bundle)
        self.assertEqual(bundle, before)

    def test_no_path_reader_environment_or_account_dependencies_are_needed(self):
        bundle = self.bundle()
        with mock.patch("builtins.open", side_effect=AssertionError("NO_FILE_READER")), \
             mock.patch.object(Path, "read_bytes", side_effect=AssertionError("NO_FILE_READER")):
            self.assertFalse(R.validate_archived_halted_resolution(*bundle).activation_allowed)

    def test_old_archive_age_neither_invalidates_consistency_nor_grants_authority(self):
        self.assertFalse(self.validate().activation_allowed)
        self.assertIn("FRESH_ORDINARY_PROVIDER_AND_ACTIVATION_INTERFACE_NOT_IMPLEMENTED", self.validate().blocking_gaps)

    def test_diagnostic_success_snapshot_only_adds_archived_bytes(self):
        self.observed_extra["logs/diagnostic-FAKE/snapshot.json"] = raw(dict(diagnostic_only=True, demo_verified=True, ledger_complete=True))
        result = self.validate()
        self.assertFalse(result.successor_ready)
        self.assertFalse(result.diagnostic_admission_allowed)
        self.assertIn("FULL_LEDGER_COVERAGE_AND_CASHFLOW_BASELINE_NOT_VALIDATED", result.blocking_gaps)

    def test_real_unproven_state_is_rejected_even_with_success_booleans_elsewhere(self):
        self.current_changes.update(exit_reconciled=False, exit_reconcile_code="DEMO_HALTED_EXIT_UNPROVEN")
        self.observed_extra["logs/diagnostic-FAKE/report.json"] = raw(dict(result="PASS", closed_lifecycle_verified=True))
        self.refused("RESOLUTION_STORED_PROOF_NOT_VERIFIED")

    def test_report_success_is_not_a_raw_proof(self):
        self.proof_bytes = raw(dict(result="PASS", closed_lifecycle_verified=True, position_binding_verified=True, exit_reconciled=True))
        self.refused("RESOLUTION_PROOF_SHAPE_INVALID")

    def test_missing_raw_proof_refuses(self):
        a, b, anchor = self.bundle()
        b = tuple(item for item in b if item.name != anchor.proof_path)
        anchor = replace(anchor, observed_inventory_sha256=R.archive_digest(b))
        with self.assertRaisesRegex(R.ResolutionRefused, "RESOLUTION_EVIDENCE_MISSING"):
            R.validate_archived_halted_resolution(a, b, anchor)

    def test_every_external_digest_anchor_must_match(self):
        a, b, anchors = self.bundle()
        for key in ("intent_sha256", "authorization_sha256", "original_fail_sha256", "original_inventory_sha256", "observed_inventory_sha256", "proof_sha256", "account_hash"):
            with self.subTest(key=key), self.assertRaises(R.ResolutionRefused):
                R.validate_archived_halted_resolution(a, b, replace(anchors, **{key: "b"*64}))

    def test_duplicate_and_nonfinite_json_are_rejected(self):
        for payload in (b'{"a":1,"a":1}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":1e999}'):
            with self.subTest(payload=payload), self.assertRaises(R.ResolutionRefused):
                R.archive_digest((R.EvidenceFile("logs/fake.json", payload),))

    def test_canonical_json_preserves_types_and_rejects_non_json_values(self):
        self.assertEqual(len({R.canonical_json(v) for v in (True, 1, 1.0)}), 3)
        for value in (float("nan"), {1: "bad-key"}, (1, 2), {"x": object()}):
            with self.subTest(value=type(value).__name__), self.assertRaises(R.ResolutionRefused):
                R.canonical_json(value)

    def test_equal_boolean_integer_and_integer_float_incident_drift_refuses(self):
        for change in ({"marker": 1}, {"created_at_ms": float(self.created)}, {"submit_attempted": 1}):
            with self.subTest(change=change):
                self.current_changes = change
                self.refused("RESOLUTION_INCIDENT_DRIFT")

    def test_extra_incident_metadata_or_original_checkpoint_changes_refuse(self):
        for change in ({"extra": "forged"}, {"close_id": "replacement"}, {"emergency_status": "CLEARED"}, {"filled_qty": "0"}):
            with self.subTest(change=change):
                self.current_changes = change
                self.refused("RESOLUTION_INCIDENT_DRIFT")

    def test_original_fail_cannot_be_promoted_or_detached_from_plan(self):
        for key, value in (("result", "PASS"), ("submit_attempted", 1), ("scenario", "swing")):
            with self.subTest(key=key):
                before = self.fail[key]
                self.fail[key] = value
                self.refused("RESOLUTION_ORIGINAL_FAIL_INVALID")
                self.fail[key] = before
        self.plan["signal"]["qty"] = "0.02"
        self.refused("RESOLUTION_TECHNICAL_PLAN_INVALID")

    def test_historical_sanitized_plan_requires_complete_original_registration(self):
        self.fail["plan"] = {key: deepcopy(value) for key, value in self.plan.items()
                             if key != "authorization_id"}
        bundle = self.bundle()
        before = deepcopy(bundle)
        result = R.validate_archived_halted_resolution(*bundle)
        self.assertEqual(bundle, before)
        self.assertFalse(result.activation_allowed)
        self.assertFalse(result.successor_ready)
        self.assertFalse(result.diagnostic_admission_allowed)
        self.assertEqual(result.validation_scope, "ARCHIVED_CLOSURE_CONSISTENCY_ONLY")

    def test_sanitized_plan_cannot_infer_missing_or_different_registered_authority(self):
        self.fail["plan"] = {key: deepcopy(value) for key, value in self.plan.items()
                             if key != "authorization_id"}
        original_id = self.plan["authorization_id"]
        for value in (None, "", "other-authorization", True, 1):
            with self.subTest(value=value):
                self.plan["authorization_id"] = value
                self.refused("RESOLUTION_TECHNICAL_PLAN_INVALID")
        self.plan["authorization_id"] = original_id

    def test_sanitized_report_signal_still_must_match_full_registered_fingerprint(self):
        self.fail["plan"] = {key: deepcopy(value) for key, value in self.plan.items()
                             if key != "authorization_id"}
        for key, value in (("qty", "0.02"), ("entry", "100.0000000000000000000000000001"),
                           ("generated_at_ms", float(self.created)), ("require_maker", 1)):
            old = self.fail["plan"]["signal"][key]
            with self.subTest(key=key):
                self.fail["plan"]["signal"][key] = value
                self.refused("RESOLUTION_TECHNICAL_PLAN_INVALID")
            self.fail["plan"]["signal"][key] = old

    def test_sanitization_compatibility_accepts_only_the_exact_authorization_key_projection(self):
        reduced = {key: deepcopy(value) for key, value in self.plan.items()
                   if key != "authorization_id"}
        variants = ({**reduced, "extra": "ignored"},
                    {key: value for key, value in reduced.items() if key != "purpose"},
                    {key: value for key, value in reduced.items() if key != "scenario"},
                    {**reduced, "authorization_id": None},
                    {**reduced, "authorization_id": "wrong"})
        for plan in variants:
            self.fail["plan"] = plan
            with self.subTest(keys=sorted(plan)):
                self.refused("RESOLUTION_TECHNICAL_PLAN_INVALID")

    def test_reduced_plan_fingerprint_or_missing_registration_still_refuse(self):
        self.fail["plan"] = {key: deepcopy(value) for key, value in self.plan.items()
                             if key != "authorization_id"}
        for missing in (False, True):
            original, observed, anchors = self.bundle()
            rewritten = []
            for bundle in (original, observed):
                result = []
                for item in bundle:
                    if item.name == R.CONTROLLER:
                        with sqlite3.connect(":memory:") as db:
                            db.deserialize(item.raw)
                            if missing:
                                db.execute("DELETE FROM technical_tests WHERE signal=?", (R.SIGNAL,))
                            else:
                                db.execute("UPDATE technical_tests SET plan_fingerprint=? WHERE signal=?",
                                           (sha(R.canonical_json(self.fail["plan"])), R.SIGNAL))
                            db.commit()
                            item = R.EvidenceFile(item.name, db.serialize())
                    result.append(item)
                rewritten.append(tuple(result))
            a, b = rewritten
            fixed = replace(anchors, original_inventory_sha256=R.archive_digest(a),
                            observed_inventory_sha256=R.archive_digest(b))
            with self.subTest(missing=missing), self.assertRaisesRegex(
                    R.ResolutionRefused, "RESOLUTION_TECHNICAL_PLAN_INVALID"):
                R.validate_archived_halted_resolution(a, b, fixed)

    def test_authorization_identity_scope_type_and_original_boundary_refuse(self):
        for key, value in (("environment", "LIVE"), ("account_hash", "b"*64), ("scope", "ALL"),
                           ("authorization_recorded_at_ms", True), ("authorization_recorded_at_ms", self.created+1)):
            with self.subTest(key=key):
                original = self.auth[key]
                self.auth[key] = value
                self.refused()
                self.auth[key] = original

    def test_busy_owner_additional_unknown_or_already_reconciled_original_refuse(self):
        self.owner = "busy"
        self.refused("RESOLUTION_ROUTE_INVALID")
        self.owner = ""
        self.extra_status = "UNKNOWN"
        self.refused("RESOLUTION_EXTRA_UNRESOLVED")
        self.extra_status = None
        self.old["exit_reconciled"] = True
        self.refused("RESOLUTION_ORIGINAL_ALREADY_RECONCILED")

    def test_old_state_baseline_halt_and_history_bytes_cannot_change(self):
        for path in (R.STATE + "scan-seen.json", R.STATE + "verified-risk-v2/FAKE/verified.json", self.fail_path,
                     R.STATE + "STOP_NEW_ENTRIES", "logs/r11-forensic-FAKE/old.log", "logs/r14-forensic-FAKE/old.log"):
            with self.subTest(path=path):
                self.observed_extra = {path: raw(dict(changed=True))}
                self.refused("RESOLUTION_HISTORY_CHANGED")

    def test_new_state_or_additional_halt_is_never_authorized(self):
        self.observed_extra[R.STATE + "new-baseline.json"] = b"{}"
        self.refused("RESOLUTION_HISTORY_CHANGED")
        self.observed_extra = {R.STATE + "HALT-extra": b"HALT"}
        self.refused("RESOLUTION_HALT_INVALID")

    def test_sqlite_corruption_nonempty_sidecars_and_views_refuse(self):
        self.original_extra[R.ROUTER + "-wal"] = b"UNFINISHED"
        self.refused("RESOLUTION_SQLITE_SIDECAR_NONEMPTY")
        self.original_extra = {R.ROUTER: b"NOT_SQLITE"}
        self.refused("RESOLUTION_SQLITE_INVALID")
        self.original_extra = {}
        self.view = True
        self.refused("RESOLUTION_SQLITE_SCHEMA_INVALID")

    def test_case_insensitive_sqlite_sidecars_and_json_extensions_refuse(self):
        for suffix in ("-WAL", "-JOURNAL", "-SHM", "-WaL"):
            with self.subTest(suffix=suffix):
                self.original_extra = {R.ROUTER + suffix: b"UNFINISHED"}
                self.refused("RESOLUTION_SQLITE_SIDECAR_NONEMPTY")
        self.original_extra = {}
        for payload in (b'{"duplicate":1,"duplicate":1}', b'{"number":NaN}'):
            with self.subTest(payload=payload):
                self.observed_extra = {"logs/diagnostic-FAKE/snapshot.JSON": payload}
                self.refused()

    def test_controller_or_other_journal_history_cannot_be_overwritten(self):
        self.observed_extra[R.CONTROLLER] = database("controller", self.old, {**self.intent, "kind": "other"}, self.plan)
        self.refused("RESOLUTION_JOURNAL_HISTORY_CHANGED")

    def test_collection_wall_and_monotonic_each_keep_original_five_second_limit(self):
        for change in ({"exit_reconcile_collection_started_at_ms": self.end-6000},
                       {"exit_reconcile_collection_started_at_ms": self.end+21},
                       {"exit_reconcile_collection_elapsed_monotonic_ns": 5_000_000_001},
                       {"exit_reconcile_collection_elapsed_monotonic_ns": -1},
                       {"exit_reconcile_collection_elapsed_monotonic_ns": True},
                       {"exit_reconcile_collection_completed_at_ms": self.start-1},
                       {"exit_reconcile_collection_completed_at_ms": self.start+5001}):
            with self.subTest(change=change):
                self.current_changes = change
                self.refused()

    def test_exact_original_five_second_bounds_remain_archival_only(self):
        self.current_changes = dict(exit_reconcile_collection_started_at_ms=self.start,
            exit_reconcile_collection_completed_at_ms=self.start+5000,
            exit_reconcile_collection_elapsed_monotonic_ns=5_000_000_000)
        self.assertFalse(self.validate().successor_ready)

    def test_original_stop_and_halt_evidence_is_mandatory_even_with_matching_hashes(self):
        a, b, anchors = self.bundle()
        for name in (R.STATE + "STOP_NEW_ENTRIES", R.STATE + "CONTROLLER_HALT"):
            missing_a = tuple(item for item in a if item.name != name)
            missing_b = tuple(item for item in b if item.name != name)
            changed = replace(anchors, original_inventory_sha256=R.archive_digest(missing_a),
                observed_inventory_sha256=R.archive_digest(missing_b))
            with self.subTest(name=name), self.assertRaisesRegex(R.ResolutionRefused, "RESOLUTION_EVIDENCE_MISSING"):
                R.validate_archived_halted_resolution(missing_a, missing_b, changed)

    def test_missing_collection_metadata_is_not_defaulted(self):
        self.current_changes["exit_reconcile_collection_elapsed_monotonic_ns"] = None
        self.refused("RESOLUTION_TIME_INVALID")

    def test_proof_boolean_flags_and_quantities_are_strict(self):
        for key, value in (("position_binding_verified", 1), ("closed_lifecycle_verified", "true"),
                           ("confirmed_closed_qty", "0"), ("confirmed_closed_qty", 0.01), ("position_opened_at_ms", None)):
            with self.subTest(key=key):
                old = self.proof[key]
                self.proof[key] = value
                self.refused()
                self.proof[key] = old

    def test_exposure_other_instrument_or_missing_wave_refuses(self):
        self.proof["exposure_after"]["positions"] = [{"instId": "BTC-USDT-SWAP", "pos": "1"}]
        self.refused("RESOLUTION_EXPOSURE_NOT_FLAT")
        self.proof["exposure_after"] = {"positions": [], "pending": []}
        self.refused("RESOLUTION_EXPOSURE_NOT_FLAT")

    def test_entry_parent_exit_receipt_identity_and_fill_price_refuse(self):
        for target, key, value in (("order", "clOrdId", "foreign"), ("order", "accFillSz", "0"),
                                   ("parent", "actualSide", ""), ("parent", "slTriggerPxType", "mark"),
                                   ("position_history", "posId", "foreign"), ("position_history", "settledPnl", "1")):
            with self.subTest(target=target, key=key):
                old = self.proof[target][key]
                self.proof[target][key] = value
                self.refused()
                self.proof[target][key] = old
        self.proof["children"][0]["reduceOnly"] = False
        self.refused("RESOLUTION_EXIT_INVALID")

    def test_external_duplicate_missing_and_price_changed_fills_refuse(self):
        original = deepcopy(self.proof["fills"])
        for values in (original + [deepcopy(original[1])], original[:1], [dict(original[0], ordId="foreign"), original[1]],
                       [original[0], dict(original[1], fillPx="104")]):
            with self.subTest(length=len(values)):
                self.proof["fills"] = values
                self.refused()

    def test_no_position_id_or_open_time_is_guessed_from_report(self):
        self.old["position_id"] = "different"
        self.refused("RESOLUTION_BINDING_CHANGED")
        self.old["position_id"] = None
        del self.proof["position_opened_at_ms"]
        self.refused("RESOLUTION_PROOF_SHAPE_INVALID")

    def test_raw_history_and_window_time_cannot_be_widened(self):
        self.proof["history_through_ms"] = self.start+5001
        self.refused("RESOLUTION_TIME_INVALID")
        self.proof["history_through_ms"] = self.end
        self.proof["fills"][0]["ts"] = str(self.opened-1)
        self.refused("RESOLUTION_FILL_INVALID")

    def test_duplicate_casefold_and_windows_path_aliases_refuse_without_open(self):
        for path in ("logs/../escape.json", "logs/CON.json", "logs/a.json.", "logs/a:stream", "logs/a\\b.json", "src/unrelated.py"):
            with self.subTest(path=path), self.assertRaises(R.ResolutionRefused):
                R.archive_digest((R.EvidenceFile(path, b"{}"),))
        with self.assertRaisesRegex(R.ResolutionRefused, "RESOLUTION_ARCHIVE_DUPLICATE"):
            R.archive_digest((R.EvidenceFile("logs/a.json", b"{}"), R.EvidenceFile("logs/A.json", b"{}")))
        with self.assertRaises(R.ResolutionRefused):
            R.archive_digest((R.EvidenceFile("logs/a", b"x"), R.EvidenceFile("logs/a/b", b"x")))
        with self.assertRaises(R.ResolutionRefused):
            R.archive_digest((R.EvidenceFile("logs/A", b"x"), R.EvidenceFile("logs/a/b", b"x")))

    def test_mutable_or_path_inputs_are_not_accepted_as_bytes(self):
        for value in (bytearray(b"{}"), Path("NEVER_OPEN"), "{}"):
            with self.subTest(kind=type(value).__name__), self.assertRaises(R.ResolutionRefused):
                R.archive_digest((R.EvidenceFile("logs/fake.json", value),))

    def test_reference_path_mismatch_cannot_be_repaired_by_a_hash(self):
        self.current_changes["exit_reconcile_proof_path"] = r"C:\unrelated\proof.json"
        self.refused("RESOLUTION_INCIDENT_DRIFT")
        self.current_changes = {}
        a, b, anchors = self.bundle()
        for root in (r"\\network\share", r"C:\fake\..\elsewhere", r"C:relative", r"C:\bad:stream", r"C:\CON"):
            with self.subTest(root=root), self.assertRaises(R.ResolutionRefused):
                R.validate_archived_halted_resolution(a, b, replace(anchors, observed_root=root))


if __name__ == "__main__":
    unittest.main()
