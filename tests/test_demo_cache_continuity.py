"""Synthetic archived bytes; no actual account, report, runtime or network."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from unittest import mock

from src import demo_cache_continuity as C
from tests.test_demo_ledger import ACCOUNT, MONDAY, evidence, add_position, reservation


def raw(value):
    return json.dumps(value, sort_keys=True).encode()


def digest(value):
    return hashlib.sha256(C._canonical(value)).hexdigest()


class CacheContinuityTests(unittest.TestCase):
    def setUp(self):
        self.cfg = dict(uid="246810", acctLv="2", posMode="net_mode")
        self.auth = dict(environment="DEMO", account_hash=ACCOUNT,
            authorization_recorded_at_ms=MONDAY-4*3_600_000,
            source="user-approved-demo-activation", scope="SOL-USDT-SWAP_EXISTING_RISK")
        self.policy = dict(consecutive_loss_pause_count=3, consecutive_loss_pause_hours=24)
        self.old_data, self.new_data = evidence(), evidence()
        self.old_time, self.new_time = MONDAY+1000, MONDAY+10000
        self.old_equity = self.new_equity = "2500"
        self.old, _ = self.cache(self.old_data, self.old_time, self.old_equity)
        self.new, self.views = self.cache(self.new_data, self.new_time, self.new_equity)

    def cache(self, data, through, equity):
        day, week = C._periods(through)
        anchor = deepcopy(data["positions_history"][0])
        lower, lookback = int(anchor["uTime"])+1, int(anchor["uTime"])-C.DAY_MS
        views = [dict(config=deepcopy(self.cfg), balance=dict(totalEq=equity, uTime=str(through-5),
                details=[dict(ccy="USDT", eq=equity, **{key:"0" for key in C.NA_FIELDS})]),
                positions=[], pending=[], algos={key:[] for key in C.ALGO_SCOPES},
                leverage=dict(instId="SOL-USDT-SWAP", mgnMode="cross", posSide="net", lever="8"),
                spec=dict(instId="SOL-USDT-SWAP", state="live", ctType="linear", settleCcy="USDT", ctValCcy="SOL",
                          ctVal="1", lotSz="0.01", minSz="0.01", tickSz="0.01")) for _ in range(2)]
        applicability = []
        for view in views:
            stable = dict(config=deepcopy(self.cfg), wallets=deepcopy(view["balance"]["details"]),
                positions=[], pending=[], algos=deepcopy(view["algos"]), leverage=deepcopy(view["leverage"]), spec=deepcopy(view["spec"]))
            applicability.append(dict(raw_fingerprint=digest(stable), not_applicable_fields=[]))
        ev = dict(fills=deepcopy(data["fills"]), bills=deepcopy(data["bills"]), histories=deepcopy(data["positions_history"]),
                  routes=deepcopy(data["routes"]), order_details=deepcopy(data["order_details"]))
        pnl = lambda boundary: sum((Decimal(row["fillPnl"])+Decimal(row["fee"]) for row in ev["fills"] if int(row["fillTime"]) >= boundary),Decimal(0))+sum((Decimal(row["balChg"]) for row in ev["bills"] if row.get("type")=="8" and int(row["ts"])>=boundary),Decimal(0))
        result = dict(schema=2, environment="DEMO", account_hash=ACCOUNT, authorization_digest=digest(self.auth), anchor=anchor,
            complete_through_ms=through, coverage_start_ms=lower, collection_started_ms=through-50,
            anchor_lookback_start_ms=lookback, ledger_evidence_digest=digest(ev), evidence=ev,
            day_start_ms=day, week_start_ms=week, day_start_equity=str(Decimal(equity)-pnl(day)),
            week_start_equity=str(Decimal(equity)-pnl(week)), baseline_source=C.BASELINE,
            account_observed_at_ms=through-10,
            balance_observations=[dict(path="/api/v5/account/balance", stage=stage, started_at_ms=through+start,
                   finished_at_ms=through+end, scope={}) for stage,start,end in (("account-before",-40,-30),("account-after",-10,-5))],
            fill_product_scopes=sorted(C.FILL_SCOPES), position_product_scopes=sorted(C.POSITION_SCOPES), algo_scopes=sorted(C.ALGO_SCOPES),
            bills_currency_filter=None,
            source_coverage=dict(fills=[dict(start_ms=lower,end_ms=through,products=sorted(C.FILL_SCOPES))],
                bills=[dict(start_ms=lower,end_ms=through,currency_filter=None)],
                positions=[dict(start_ms=lookback,end_ms=through,products=sorted(C.POSITION_SCOPES))]),
            account_mode_applicability=applicability, equity_usdt_observed=equity,
            account_mode=dict(acctLv="2",posMode="net_mode"))
        return result, views

    def refresh(self):
        self.old, _ = self.cache(self.old_data,self.old_time,self.old_equity)
        self.new, self.views = self.cache(self.new_data,self.new_time,self.new_equity)

    def validate(self, *, views=False):
        return C.validate_demo_cache_continuity(raw(self.old), raw(self.new), raw(self.auth), raw(self.cfg),raw(self.policy),
                    new_account_views_raw=raw(self.views) if views else None)

    def refused(self, code=None, *, views=False):
        with self.assertRaises(C.CacheContinuityRefused) as caught:
            self.validate(views=views)
        if code:
            self.assertEqual(caught.exception.code,code)
        self.assertRegex(str(caught.exception),r"^CACHE_[A-Z0-9_]+$")
        self.assertNotIn(self.cfg["uid"], str(caught.exception))

    def transition(self):
        old = dict(reservation(),signal_id=C.INCIDENT,client_order_id="entry1",created_at_ms=self.old_time-100,
                   expires_at_ms=self.new_time+1000, protection_id="rprotection",flat_observed_at_ms=self.old_time-100)
        self.old_data["routes"]=[old]
        route,_ = add_position(self.new_data,1,self.old_time+1000,self.old_time+2000,gross="3",funding="-0.1")
        self.new_data["routes"][0]={**old,**route,"signal_id":C.INCIDENT,"status":"HALTED","submit_attempted":True,
              "protected_qty":"0","confirmed_closed_qty":"1"}
        self.new_equity="2502.7"
        self.refresh()

    def test_empty_checkpoint_continuity_never_claims_admission(self):
        result=self.validate()
        self.assertEqual(result.validation_scope,"ARCHIVED_CACHE_CONTINUITY_ONLY")
        self.assertFalse(result.activation_allowed)
        self.assertFalse(result.successor_ready)
        self.assertFalse(result.old_snapshot_admission_reverified)
        self.assertFalse(result.exchange_origin_authenticated)
        self.assertFalse(result.new_account_waves_consistent)
        self.assertEqual(result.anchor_equity,"2500")

    def test_optional_raw_new_waves_bind_exact_cache_and_flatness(self):
        self.assertTrue(self.validate(views=True).new_account_waves_consistent)

    def test_unique_unsent_incident_can_progress_with_complete_new_receipts(self):
        self.transition()
        result=self.validate(views=True)
        self.assertTrue(result.incident_reservation_progressed)
        self.assertFalse(result.old_snapshot_admission_reverified)
        self.assertEqual(Decimal(result.anchor_equity),Decimal("2500"))

    def test_inputs_immutable_and_uid_not_in_return_repr(self):
        before=deepcopy((self.old,self.new,self.cfg,self.auth,self.policy,self.views))
        result=self.validate(views=True)
        self.assertEqual(before,(self.old,self.new,self.cfg,self.auth,self.policy,self.views))
        self.assertNotIn(self.cfg["uid"],repr(result))
        self.assertNotIn(ACCOUNT,repr(result))
        with self.assertRaises(FrozenInstanceError):
            result.activation_allowed=True
        with self.assertRaises(ValueError):
            replace(result,activation_allowed=True)

    def test_no_io_environment_network_or_sqlite_is_used(self):
        with mock.patch("builtins.open",side_effect=AssertionError("NO_IO")),mock.patch.object(Path,"read_bytes",side_effect=AssertionError("NO_IO")),\
             mock.patch("socket.create_connection",side_effect=AssertionError("NO_NETWORK")),\
             mock.patch("sqlite3.connect",side_effect=AssertionError("NO_SQL")):
            self.assertFalse(self.validate().activation_allowed)

    def test_duplicate_json_nan_and_unbounded_input_refuse_safely(self):
        for value in (b'{"uid":"246810","uid":"246810"}',b'{"uid":NaN}',b'{"x":1e999}',b"x"*(C.MAX_BYTES+1)):
            with self.subTest(size=len(value)),self.assertRaises(C.CacheContinuityRefused) as caught:
                C.validate_demo_cache_continuity(raw(self.old),raw(self.new),raw(self.auth),value,raw(self.policy))
            self.assertNotIn("246810",str(caught.exception))

    def test_missing_schema_field_or_unknown_field_refuses(self):
        self.new["invented_admission"]=True
        self.refused("CACHE_SCHEMA_INVALID")
        del self.new["invented_admission"]
        del self.new["evidence"]
        self.refused("CACHE_SCHEMA_INVALID")

    def test_digest_mismatch_and_duplicate_history_refuse(self):
        self.new["ledger_evidence_digest"]="0"*64
        self.refused("CACHE_EVIDENCE_DIGEST_INVALID")
        self.new["evidence"]["histories"]*=2
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_HISTORY_DUPLICATE")

    def test_uid_or_authorization_rotation_refuses(self):
        self.cfg["uid"]="9999"
        self.refused("CACHE_ACCOUNT_BINDING")
        self.cfg["uid"]="246810"
        self.auth["authorization_recorded_at_ms"]-=1
        self.refused("CACHE_ACCOUNT_BINDING")

    def test_utc_labels_cannot_be_rewritten_to_skip_comparison(self):
        self.new["day_start_ms"]-=C.DAY_MS
        self.refused("CACHE_BOUNDARY_INVALID")

    def test_missing_product_and_duplicate_scope_refuse(self):
        self.new["fill_product_scopes"].remove("EVENTS")
        self.refused("CACHE_SCOPE_INCOMPLETE")
        self.refresh()
        self.new["source_coverage"]["fills"][0]["products"].append("SWAP")
        self.refused("CACHE_SCOPE_INCOMPLETE")

    def test_bills_gap_and_currency_filtered_history_refuse(self):
        self.new["source_coverage"]["bills"][0]["start_ms"]+=1
        self.refused("CACHE_COVERAGE_GAP")
        self.refresh()
        self.new["source_coverage"]["bills"][0]["currency_filter"]="USDT"
        self.refused("CACHE_COVERAGE_GAP")

    def test_missing_position_prefix_refuses(self):
        self.new["source_coverage"]["positions"][0]["start_ms"]=self.new["coverage_start_ms"]
        self.refused()

    def test_actual_get_clock_bounds_and_stage_order_are_required(self):
        self.new["balance_observations"][1]["started_at_ms"]=self.new_time+3000
        self.refused("CACHE_BALANCE_CLOCK_INVALID")
        self.refresh()
        self.new["balance_observations"].reverse()
        self.refused("CACHE_BALANCE_CLOCK_INVALID")

    def test_retroactive_new_receipt_and_anchor_drift_refuse(self):
        self.new["evidence"]["bills"].append(dict(billId="late",ts=str(self.old_time-1),balChg="0"))
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_HISTORY_PREFIX_CHANGED")
        self.refresh()
        self.new["anchor"]["pnl"]="999"
        self.refused("CACHE_ANCHOR_CHANGED")

    def test_removed_or_changed_old_receipts_refuse(self):
        add_position(self.old_data,2,MONDAY+100,MONDAY+200,gross="1")
        self.old_equity="2500.8"
        self.new_data=deepcopy(self.old_data)
        self.new_equity=self.old_equity
        self.refresh()
        self.assertFalse(self.validate().incident_reservation_progressed)
        self.new["evidence"]["fills"][0]["fee"]="-0.2"
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_HISTORY_PREFIX_CHANGED")

    def test_unexplained_cash_change_never_rebases_same_period(self):
        self.new_equity="2600"
        self.refresh()
        self.refused("CACHE_SAME_PERIOD_BASELINE_CHANGED")

    def test_cross_day_and_cross_week_continuity_keeps_anchor_cash(self):
        self.transition()
        for offset in (C.DAY_MS,7*C.DAY_MS):
            self.new_time=MONDAY+offset+10000
            self.refresh()
            self.assertFalse(self.validate().activation_allowed)
            self.assertNotEqual(self.old["day_start_ms"],self.new["day_start_ms"])
        self.new_equity="2600"
        self.refresh()
        self.refused("CACHE_ANCHOR_CASH_CHANGED")

    def test_invalid_baseline_formula_refuses_even_if_positive(self):
        self.transition()
        self.new["week_start_equity"]="2502.7"
        self.refused("CACHE_BASELINE_RECONSTRUCTION_CHANGED")

    def test_new_external_cash_flow_or_missing_trade_bill_refuses(self):
        self.transition()
        self.new["evidence"]["bills"].pop()
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_NEW_LEDGER_INCONSISTENT")

    def test_nonincident_route_cannot_progress_or_disappear(self):
        self.transition()
        self.old["evidence"]["routes"][0]["signal_id"]="another"
        self.old["ledger_evidence_digest"]=digest(self.old["evidence"])
        self.refused("CACHE_ROUTE_HISTORY_CHANGED")

    def test_unsent_claim_must_have_no_previous_request_or_exposure(self):
        self.transition()
        self.old["evidence"]["routes"][0]["submit_attempted"]=True
        self.old["ledger_evidence_digest"]=digest(self.old["evidence"])
        self.refused("CACHE_ROUTE_TRANSITION_INVALID")

    def test_immutable_incident_binding_cannot_be_replaced(self):
        self.transition()
        self.new["evidence"]["routes"][0]["client_order_id"]="replacement"
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_ROUTE_TRANSITION_INVALID")

    def test_optional_new_wave_fingerprint_cannot_be_substituted(self):
        self.views[1]["leverage"]["lever"]="20"
        self.refused("CACHE_ACCOUNT_FINGERPRINT_MISMATCH",views=True)

    def test_optional_new_wave_exposure_or_unknown_algo_is_refused(self):
        self.views[1]["positions"]=[dict(pos="1")]
        self.refused("CACHE_ACCOUNT_NOT_FLAT",views=True)
        self.refresh()
        del self.views[0]["algos"]["trigger"]
        self.refused("CACHE_ACCOUNT_NOT_FLAT",views=True)

    def test_ledger_policy_does_not_accept_boolean_limits(self):
        self.policy["consecutive_loss_pause_count"]=True
        self.refused("CACHE_POLICY_INVALID")

    def test_original_reservation_null_position_does_not_mean_unsent(self):
        self.transition()
        for key,value in (("position_id",None),("position_opened_at_ms",None),("position_opened_at_ms",False)):
            with self.subTest(field=key,value=value):
                old=deepcopy(self.old)
                self.old["evidence"]["routes"][0][key]=value
                self.old["ledger_evidence_digest"]=digest(self.old["evidence"])
                self.refused("CACHE_ROUTE_TRANSITION_INVALID")
                self.old=old

    def test_history_and_cash_are_compared_independent_of_array_order(self):
        self.transition()
        self.new["evidence"]["fills"].reverse()
        self.new["evidence"]["bills"].reverse()
        self.new["evidence"]["histories"].reverse()
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.assertEqual(Decimal(self.validate().anchor_equity),Decimal("2500"))

    def test_replacing_old_order_detail_with_same_order_id_refuses(self):
        add_position(self.old_data,2,MONDAY+100,MONDAY+200,gross="1")
        self.old_equity="2500.8"
        self.new_data=deepcopy(self.old_data)
        self.new_equity=self.old_equity
        self.refresh()
        self.new["evidence"]["order_details"]["102"]["state"]="canceled"
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_HISTORY_PREFIX_CHANGED")

    def test_one_more_nonincident_old_route_is_not_silently_discarded(self):
        row=dict(account_hash=ACCOUNT,signal_id="old-canceled",status="CANCELED",filled_qty="0",order_id="")
        self.old["evidence"]["routes"].append(row)
        self.old["ledger_evidence_digest"]=digest(self.old["evidence"])
        self.refused("CACHE_ROUTE_HISTORY_CHANGED")

    def test_raw_account_view_does_not_enable_an_otherwise_bad_ledger(self):
        self.transition()
        self.new["evidence"]["fills"][0]["ordId"]="foreign"
        self.new["ledger_evidence_digest"]=digest(self.new["evidence"])
        self.refused("CACHE_NEW_LEDGER_INCONSISTENT",views=True)

    def test_real_cache_producer_schema_and_official_empty_na_fields_are_compatible(self):
        # This is the actual producer and raw-API-shaped fake transport, never
        # a network response or an altered cache written for the validator.
        from tests.test_demo_verified_risk import VerifiedRiskTests
        fixture=VerifiedRiskTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        for field in C.NA_FIELDS:
            fixture.client.balance["details"][0][field]=""
        first=fixture.snapshot()
        self.assertTrue(first["demo_verified"],first["refusal_reasons"])
        cache_path=fixture.root/"verified-risk"/ACCOUNT/"verified.json"
        previous=cache_path.read_bytes()
        fixture.now+=1000
        fixture.client.now=fixture.now
        fixture.client.balance["uTime"]=str(fixture.now)
        second=fixture.snapshot()
        self.assertTrue(second["demo_verified"],second["refusal_reasons"])
        result=C.validate_cache_continuity(previous,cache_path.read_bytes(),raw(fixture.authorization),
            raw(fixture.client.config),raw(fixture.config["risk"]),new_account_views_raw=raw(fixture.provider._views))
        self.assertTrue(result.new_account_waves_consistent)
        self.assertFalse(result.activation_allowed)

    def test_trade_pnl_uses_fill_execution_time_when_bill_records_next_day(self):
        boundary=MONDAY+C.DAY_MS
        add_position(self.old_data,2,boundary-400,boundary-300,gross="1")
        for row in self.old_data["fills"]:
            row["ts"]=str(boundary+100)
        for row in self.old_data["bills"]:
            row["ts"]=str(boundary+100)
        self.new_data=deepcopy(self.old_data)
        self.old_time,self.new_time=boundary+1000,boundary+2000
        self.old_equity=self.new_equity="2500.8"
        self.refresh()
        self.assertEqual(self.old["day_start_equity"],"2500.8")
        self.assertEqual(self.old["week_start_equity"],"2500.0")
        self.assertEqual(Decimal(self.validate().anchor_equity),Decimal("2500"))

    def test_later_trade_bill_cannot_complete_an_originally_incomplete_checkpoint(self):
        add_position(self.old_data,2,MONDAY+100,MONDAY+200,gross="1")
        for row in self.old_data["fills"]:
            row["ts"]=str(self.old_time+100)
        self.new_data=deepcopy(self.old_data)
        for row in self.new_data["bills"]:
            row["ts"]=str(self.old_time+100)
        self.old_data["bills"]=[]
        self.old_equity=self.new_equity="2500.8"
        self.refresh()
        self.refused("CACHE_OLD_TRADE_BILL_INCOMPLETE")


if __name__=="__main__":
    unittest.main()
