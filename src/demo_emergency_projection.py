"""Pure, non-admitting projection of a pinned emergency-closure receipt.

Inputs and outputs are byte images. Only in-memory SQLite is opened. Explicit
caller pins establish provenance; this module cannot establish that supplied
receipt bytes came from an exchange. Publishing, fresh account admission,
activation and filesystem HALT handling remain separate operations.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass, field, fields
from decimal import Decimal, localcontext
import hashlib
import json
import re
import sqlite3

from . import demo_halted_resolution as R
from . import demo_successor as S


CLOSED_CODE = "DEMO_EMERGENCY_EXIT_VERIFIED"
HALT_CODE = "PROTECTION_FAILED"
TERMINAL = frozenset({"CLOSED", "CANCELED", "REJECTED"})
ALGO_TYPES = ("oco", "conditional", "trigger", "move_order_stop", "iceberg",
              "twap", "smart_iceberg", "chase")
CONTROLLER_TABLES = {
    "controller_meta": ("key", "value"),
    "controller_signals": ("signal", "status", "code", "intent", "updated_ms"),
    "technical_tests": ("signal", "purpose", "scenario", "authorization_id", "plan_fingerprint", "signal_json", "registered_at_ms"),
    "natural_exits": ("signal", "record"),
    "halted_resolutions": ("signal", "record"),
}
ROUTER_TABLES = {
    "controls": ("account", "code"),
    "routes": ("account", "signal", "fingerprint", "client", "status", "owner", "data"),
    "halted_resolutions": ("signal", "record"),
}
RECEIPT_FIELDS = frozenset({
    "schema", "environment", "purpose", "diagnostic_only", "can_resume", "can_activate",
    "emergency_closed_lifecycle_verified", "position_binding_verified", "closed_lifecycle_verified",
    "account_hash", "intent_sha256", "entry_order_id", "entry_client_id", "retained_close_order_ids",
    "position_id", "position_opened_at_ms", "position_closed_at_ms", "flat_observed_at_ms",
    "observed_at_ms", "history_through_ms", "collection_started_at_ms", "collection_completed_at_ms",
    "collection_elapsed_monotonic_ns", "confirmed_closed_qty", "order", "closes", "fills",
    "position_history", "exposure_before", "exposure_after", "exposure_raw_before",
    "exposure_raw_after", "exposure_algo_types",
})


class EmergencyProjectionRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code="EMERGENCY_PROJECTION_INVALID"):
    if not condition:
        raise EmergencyProjectionRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return R.canonical_json(value)


@dataclass(frozen=True, slots=True)
class EmergencyProjectionPins:
    source_seed_controller_sha256: str
    source_seed_router_sha256: str
    source_controller_sha256: str
    source_router_sha256: str
    target_seed_controller_sha256: str
    target_seed_router_sha256: str
    receipt_sha256: str
    intent_sha256: str
    incident_route_sha256: str
    account_hash: str
    signal_id: str


@dataclass(frozen=True, slots=True)
class EmergencyProjectionPlan:
    controller: bytes = field(repr=False)
    router: bytes = field(repr=False)
    projected_source_controller: bytes = field(repr=False)
    projected_source_router: bytes = field(repr=False)
    receipt: bytes = field(repr=False)
    provenance: bytes = field(repr=False)
    controller_sha256: str
    router_sha256: str
    projected_source_controller_sha256: str
    projected_source_router_sha256: str
    receipt_sha256: str
    receipt_path: str
    provenance_sha256: str
    imported_counts: tuple[tuple[str, int], ...]
    can_resume: bool = field(default=False, init=False)
    can_activate: bool = field(default=False, init=False)
    activation_allowed: bool = field(default=False, init=False)
    fresh_account_verified: bool = field(default=False, init=False)
    exchange_authenticity_verified: bool = field(default=False, init=False)
    preparation_only: bool = field(default=True, init=False)


def _database(raw, expected):
    _require(type(raw) is bytes and 0 < len(raw) <= R.MAX_FILE_BYTES,
             "EMERGENCY_DATABASE_BYTES_INVALID")
    db = R._database(raw)
    _require(set(db[1]) == set(expected), "EMERGENCY_SCHEMA_INVALID")
    for name, columns in expected.items():
        _require(db[1][name][0] == columns, "EMERGENCY_SCHEMA_INVALID")
    return db


def _rows(db, table):
    return R._rows(db, table, db[1][table][0])


def _multiset(rows):
    return S._row_multiset(rows)


def _same_rows(left, right, name):
    return _multiset(left[1][name][1]) == _multiset(right[1][name][1])


def _seed_accounts(source, target, pins):
    """Every retained seed record belongs to the explicitly pinned account."""
    for pair in (source, target):
        for database in pair:
            resolutions = _rows(database, "halted_resolutions")
            _require(len(resolutions) == 1, "EMERGENCY_SEED_ACCOUNT_MISMATCH")
            record = R._json(resolutions[0]["record"].encode())
            _require(type(record) is dict and record.get("account_hash") == pins.account_hash,
                     "EMERGENCY_SEED_ACCOUNT_MISMATCH")
        for row in _rows(pair[1], "routes"):
            data = R._json(row["data"].encode())
            _require(row["account"] == R.ACCOUNT_KEY and type(data) is dict
                     and data.get("account_hash") == pins.account_hash,
                     "EMERGENCY_SEED_ACCOUNT_MISMATCH")


def _seed_pair(source, target):
    """Only the new legacy projection's commit/time may differ between seeds."""
    for left, right in zip(source, target):
        _require(left[0] == right[0], "EMERGENCY_SCHEMA_CHANGED")
        for name in left[1]:
            if name == "halted_resolutions":
                lrows, rrows = _rows(left, name), _rows(right, name)
                _require(len(lrows) == len(rrows) == 1 and lrows[0]["signal"] == rrows[0]["signal"] == R.SIGNAL,
                         "EMERGENCY_SEED_ORIGIN_CHANGED")
                records = [R._json(rows[0]["record"].encode()) for rows in (lrows, rrows)]
                _require(_canonical({k: v for k, v in records[0].items() if k not in {"target_commit", "resolved_at_ms"}})
                         == _canonical({k: v for k, v in records[1].items() if k not in {"target_commit", "resolved_at_ms"}}),
                         "EMERGENCY_SEED_ORIGIN_CHANGED")
            elif name == "controller_signals":
                normalize = lambda db: _multiset(tuple(row[k] if not (row["signal"] == R.SIGNAL and k == "updated_ms") else 0
                    for k in db[1][name][0]) for row in _rows(db, name))
                _require(normalize(left) == normalize(right), "EMERGENCY_SEED_HISTORY_CHANGED")
            else:
                _require(_same_rows(left, right, name), "EMERGENCY_SEED_HISTORY_CHANGED")


def _validate_working(seeds, working, pins):
    for seed, now in zip(seeds, working):
        _require(seed[0] == now[0], "EMERGENCY_SCHEMA_CHANGED")
        for name in seed[1]:
            _require(not (_multiset(seed[1][name][1]) - _multiset(now[1][name][1])),
                     "EMERGENCY_SOURCE_HISTORY_CHANGED")
            if name not in {"controller_signals", "routes", "technical_tests", "controller_meta", "controls"}:
                _require(_same_rows(seed, now, name), "EMERGENCY_UNEXPECTED_DELTA")
    c, r = working
    meta = _rows(c, "controller_meta")
    seed_meta = _rows(seeds[0], "controller_meta")
    _require(_canonical(sorted(meta, key=lambda v: v["key"])) ==
             _canonical(sorted(seed_meta + [{"key": "halt", "value": HALT_CODE}], key=lambda v: v["key"])),
             "EMERGENCY_HALT_MISMATCH")
    _require(_rows(r, "controls") == [{"account": R.ACCOUNT_KEY, "code": HALT_CODE}],
             "EMERGENCY_HALT_MISMATCH")
    signals, routes = _rows(c, "controller_signals"), _rows(r, "routes")
    incidents_c = [row for row in signals if row["status"] == "HALTED"]
    incidents_r = [row for row in routes if row["status"] == "HALTED"]
    _require(len(incidents_c) == len(incidents_r) == 1
             and incidents_c[0]["signal"] == incidents_r[0]["signal"] == pins.signal_id
             and pins.signal_id != R.SIGNAL
             and pins.signal_id not in {row["signal"] for row in _rows(seeds[0], "controller_signals")}
             and pins.signal_id not in {row["signal"] for row in _rows(seeds[1], "routes")},
             "EMERGENCY_INCIDENT_NOT_UNIQUE")
    _require(all(row["status"] in TERMINAL | {"HALTED"} for row in signals + routes)
             and all(row["owner"] == "" for row in routes), "EMERGENCY_ACTIVE_WORK_PRESENT")
    signal_map = {row["signal"]: row for row in signals}
    _require(len(signal_map) == len(signals), "EMERGENCY_JOURNAL_INVALID")
    route_ids = {row["signal"] for row in routes}
    _require(all(row["signal"] in route_ids or row["status"] == "REJECTED" for row in signals),
             "EMERGENCY_UNMATCHED_TERMINAL")
    old_routes = {row["signal"] for row in _rows(seeds[1], "routes")}
    for row in routes:
        if row["signal"] in old_routes:
            continue
        payload = R._json(row["data"].encode())
        controller = signal_map.get(row["signal"])
        _require(controller is not None and type(controller["intent"]) is str,
                 "EMERGENCY_JOURNAL_INVALID")
        saved_intent = R._json(controller["intent"].encode())
        _require(row["account"] == R.ACCOUNT_KEY and payload.get("account_hash") == pins.account_hash
                 and payload.get("signal_id") == row["signal"] == saved_intent.get("signal_id")
                 and payload.get("status") == row["status"] == controller["status"]
                 and payload.get("inst_id") == saved_intent.get("inst_id") == "SOL-USDT-SWAP"
                 and payload.get("client_order_id") == row["client"]
                 and _sha(_canonical(saved_intent)) == row["fingerprint"], "EMERGENCY_JOURNAL_INVALID")
    old_tests = {row["signal"] for row in _rows(seeds[0], "technical_tests")}
    for row in _rows(c, "technical_tests"):
        if row["signal"] in old_tests:
            continue
        signal = R._json(row["signal_json"].encode())
        plan = {k: row[k] for k in ("purpose", "scenario", "authorization_id")}
        plan["signal"] = signal
        _require(row["signal"] in signal_map and row["purpose"] == "DEMO_TECHNICAL_TEST"
                 and row["scenario"] in {"smoke", "swing"}
                 and row["authorization_id"] == "user-approved-demo-technical-tests-20261019"
                 and signal.get("id") == row["signal"]
                 and row["signal"].startswith("DEMO-TEST-"+row["scenario"].upper()+"-")
                 and signal.get("instId") == "SOL-USDT-SWAP"
                 and signal.get("kind") == {"smoke": "scalp_5m", "swing": "swing_1h4h"}[row["scenario"]]
                 and row["plan_fingerprint"] == _sha(_canonical(plan)), "EMERGENCY_REGISTRY_INVALID")
        R._integer(row["registered_at_ms"])
        saved = signal_map[row["signal"]]["intent"]
        if saved is not None:
            saved = R._json(saved.encode())
            _require(saved.get("signal_id") == row["signal"] and saved.get("side") == signal.get("side")
                     and saved.get("kind") == signal.get("kind"), "EMERGENCY_REGISTRY_INVALID")
    incident_c, incident_r = incidents_c[0], incidents_r[0]
    _require(incident_c["code"] == HALT_CODE and incident_r["account"] == R.ACCOUNT_KEY,
             "EMERGENCY_INCIDENT_MISMATCH")
    data = R._json(incident_r["data"].encode())
    intent = R._json(incident_c["intent"].encode())
    _require(type(data) is dict and type(intent) is dict
             and _sha(_canonical(data)) == pins.incident_route_sha256
             and _sha(_canonical(intent)) == pins.intent_sha256 == incident_r["fingerprint"],
             "EMERGENCY_INCIDENT_PIN_MISMATCH")
    for key in ("created_at_ms", "expires_at_ms"):
        R._integer(intent.get(key))
        R._integer(data.get(key))
    R._integer(data.get("flat_observed_at_ms"))
    R._integer(incident_c["updated_ms"])
    expected_client = (pins.signal_id if re.fullmatch(r"[A-Za-z0-9]{1,32}", pins.signal_id)
                       else "h" + _sha(pins.signal_id.encode())[:31])
    _require(data.get("signal_id") == intent.get("signal_id") == pins.signal_id
             and data.get("account_hash") == pins.account_hash
             and data.get("client_order_id") == incident_r["client"] == expected_client
             and data.get("inst_id") == intent.get("inst_id") == "SOL-USDT-SWAP"
             and data.get("side") == intent.get("side") and intent["side"] in {"long", "short"}
             and data.get("status") == "HALTED" and data.get("code") == HALT_CODE
             and data.get("entry_state") == "filled" and data.get("submit_attempted") is True
             and data.get("emergency_status") == "FLAT_CONFIRMED"
             and R._number(data.get("protected_qty")) == 0
             and R._number(data.get("filled_qty"), positive=True) == R._number(intent.get("qty"), positive=True)
             == R._number(data.get("confirmed_closed_qty"), positive=True)
             and data.get("created_at_ms") == intent.get("created_at_ms")
             and data.get("expires_at_ms") == intent.get("expires_at_ms")
             and not any(key.startswith("exit_reconcile") or key.startswith("emergency_close_receipt") for key in data),
             "EMERGENCY_INCIDENT_MISMATCH")
    return incident_c, incident_r, data, intent


def _receipt(raw, pins, data, intent, resolved):
    proof = R._json(raw)
    _require(type(proof) is dict and set(proof) == RECEIPT_FIELDS
             and _sha(_canonical(proof)) == pins.receipt_sha256, "EMERGENCY_RECEIPT_PIN_MISMATCH")
    _require(type(proof["schema"]) is int and proof["schema"] == 1
             and proof["environment"] == "DEMO"
             and proof["purpose"] == "HALTED_EMERGENCY_CLOSURE_RECEIPT_ONLY"
             and all(proof[k] is True for k in ("diagnostic_only", "emergency_closed_lifecycle_verified", "position_binding_verified", "closed_lifecycle_verified"))
             and proof["can_resume"] is False and proof["can_activate"] is False
             and proof["account_hash"] == pins.account_hash and proof["intent_sha256"] == pins.intent_sha256,
             "EMERGENCY_RECEIPT_INVALID")
    for key in ("position_opened_at_ms", "position_closed_at_ms", "flat_observed_at_ms", "observed_at_ms", "history_through_ms",
                "collection_started_at_ms", "collection_completed_at_ms"):
        R._integer(proof[key])
    R._integer(proof["collection_elapsed_monotonic_ns"], zero=True)
    R._integer(resolved)
    flat, opened, closed = (proof[k] for k in ("flat_observed_at_ms", "position_opened_at_ms", "position_closed_at_ms"))
    start, end = proof["collection_started_at_ms"], proof["collection_completed_at_ms"]
    _require(flat == data.get("flat_observed_at_ms") and flat <= opened <= closed <= proof["observed_at_ms"]
             <= proof["history_through_ms"] <= end <= resolved
             and 0 <= end-start <= 5000 and 0 <= end-proof["observed_at_ms"] <= 5000
             and proof["collection_elapsed_monotonic_ns"] <= 5_000_000_000,
             "EMERGENCY_RECEIPT_TIME_INVALID")
    position = R._identity(proof["position_id"])
    for key, value in (("position_id", position), ("position_opened_at_ms", opened), ("position_closed_at_ms", closed)):
        prior = data.get(key)
        _require(prior is None or type(prior) is str and prior == ""
                 or type(prior) is int and prior == 0
                 or type(prior) is type(value) and prior == value,
                 "EMERGENCY_POSITION_CONFLICT")
    ids = data.get("close_order_ids")
    _require(type(ids) is list and len(ids) == 1 and type(ids[0]) is str
             and re.fullmatch(r"[0-9]{1,32}", ids[0])
             and proof["retained_close_order_ids"] == ids and ids[0] != data.get("order_id")
             and proof["entry_order_id"] == data.get("order_id")
             and proof["entry_client_id"] == data.get("client_order_id")
             and R._number(proof["confirmed_closed_qty"], positive=True) == R._number(data["filled_qty"], positive=True),
             "EMERGENCY_RECEIPT_BINDING_INVALID")
    size = R._number(data["filled_qty"], positive=True)
    entry, closes, history = proof["order"], proof["closes"], proof["position_history"]
    _require(type(entry) is dict and type(closes) is list and len(closes) == 1
             and type(closes[0]) is dict and type(history) is dict, "EMERGENCY_RECEIPT_INVALID")
    close = closes[0]
    expected_close = "r" + _sha((pins.signal_id + ":emergency:0:" + str(size)).encode())[:31]
    entry_side, close_side = ("buy", "sell") if intent["side"] == "long" else ("sell", "buy")
    for order, identity, client, side, kind, reduce in (
            (entry, data["order_id"], data["client_order_id"], entry_side, "post_only" if intent.get("require_maker") else "limit", "false"),
            (close, ids[0], expected_close, close_side, "market", "true")):
        _require(order.get("ordId") == identity and order.get("clOrdId") == client
                 and order.get("instId") == "SOL-USDT-SWAP" and order.get("posSide") == "net"
                 and order.get("tdMode") == "cross" and order.get("state") == "filled" and order.get("side") == side
                 and order.get("ordType") == kind and str(order.get("reduceOnly", "")).lower() == reduce
                 and order.get("algoId") in (None, "") and order.get("algoClOrdId") in (None, "")
                 and R._number(order.get("sz"), positive=True) == size == R._number(order.get("accFillSz"), positive=True),
                 "EMERGENCY_ORDER_BINDING_INVALID")
    _require(R._number(entry.get("px"), positive=True) == R._number(intent.get("entry"), positive=True)
             and history.get("posId") == position and history.get("instId") == "SOL-USDT-SWAP"
             and history.get("instType") == "SWAP"
             and history.get("posSide") == "net" and history.get("mgnMode") == "cross"
             and history.get("direction") == intent["side"] and history.get("ccy") == "USDT"
             and R._stamp(history.get("cTime")) == opened and R._stamp(history.get("uTime")) == closed
             and R._number(history.get("openMaxPos"), positive=True) == size
             == R._number(history.get("closeTotalPos"), positive=True), "EMERGENCY_HISTORY_BINDING_INVALID")
    fills = proof["fills"]
    _require(type(fills) is list and 2 <= len(fills) < 100 and all(type(f) is dict for f in fills), "EMERGENCY_FILLS_INVALID")
    grouped = {data["order_id"]: [], ids[0]: []}
    trades, bills = set(), set()
    for fill in fills:
        order = entry if fill.get("ordId") == data["order_id"] else close
        entry_fill = fill.get("ordId") == data["order_id"]
        subtypes = ({"1", "3"} if intent["side"] == "long" else {"2", "4"}) if entry_fill else (
                    {"2", "5"} if intent["side"] == "long" else {"1", "6"})
        _require(fill.get("ordId") in grouped and fill.get("clOrdId") == order["clOrdId"]
                 and fill.get("instId") == "SOL-USDT-SWAP" and fill.get("instType") == "SWAP"
                 and fill.get("posSide") == "net" and fill.get("side") == order["side"]
                 and fill.get("posId") in (None, "", position) and fill.get("feeCcy") == "USDT"
                 and fill.get("subType") in subtypes and fill.get("algoId") in (None, ""),
                 "EMERGENCY_FILLS_INVALID")
        trade, bill = R._identity(fill.get("tradeId")), R._identity(fill.get("billId"))
        _require(trade not in trades and bill not in bills, "EMERGENCY_FILLS_INVALID")
        trades.add(trade); bills.add(bill)
        execution, recorded = R._stamp(fill.get("fillTime")), R._stamp(fill.get("ts"))
        _require(flat <= execution <= recorded <= min(proof["history_through_ms"], execution+5000)
                 and R._number(fill.get("fillSz"), positive=True) % R._number(intent.get("lot_size"), positive=True) == 0
                 and (not entry_fill or R._number(fill.get("fillPnl"), signed=True) == 0),
                 "EMERGENCY_FILLS_INVALID")
        grouped[fill["ordId"]].append(fill)
    for identity, records in grouped.items():
        order = entry if identity == data["order_id"] else close
        amount = sum((R._number(f.get("fillSz"), positive=True)*R._number(f.get("fillPx"), positive=True) for f in records), Decimal(0))
        _require(records and sum((R._number(f.get("fillSz"), positive=True) for f in records), Decimal(0)) == size,
                 "EMERGENCY_FILLS_INVALID")
        _require(amount == R._number(order.get("avgPx"), positive=True)*size
                 == R._number(history.get("openAvgPx" if identity == data["order_id"] else "closeAvgPx"), positive=True)*size,
                 "EMERGENCY_RECEIPT_ACCOUNTING_INVALID")
        first, last = min(R._stamp(f["fillTime"]) for f in records), max(R._stamp(f["fillTime"]) for f in records)
        recorded = min(R._stamp(f["ts"]) for f in records if R._stamp(f["fillTime"]) == last)
        _require(max(flat, intent["created_at_ms"]) <= R._stamp(order.get("cTime")) <= first
                 and R._stamp(order.get("fillTime")) == last <= R._stamp(order.get("uTime")) <= recorded,
                 "EMERGENCY_RECEIPT_TIME_INVALID")
    entries, exits = grouped[data["order_id"]], grouped[ids[0]]
    first, last = min(R._stamp(f["fillTime"]) for f in entries), max(R._stamp(f["fillTime"]) for f in exits)
    _require(first <= opened <= min(R._stamp(f["ts"]) for f in entries if R._stamp(f["fillTime"]) == first)
             and last <= closed <= min(R._stamp(f["ts"]) for f in exits if R._stamp(f["fillTime"]) == last)
             and R._stamp(close.get("cTime")) >= max(R._stamp(f["fillTime"]) for f in entries),
             "EMERGENCY_RECEIPT_TIME_INVALID")
    pnl = sum((R._number(f.get("fillPnl"), signed=True) for f in fills), Decimal(0))
    fee = sum((R._number(f.get("fee"), signed=True) for f in fills), Decimal(0))
    funding = R._number(history.get("fundingFee"), signed=True)
    _require(R._number(history.get("pnl"), signed=True) == pnl
             and R._number(history.get("fee"), signed=True) == fee
             and R._number(history.get("realizedPnl"), signed=True) == pnl+fee+funding
             and R._number(history.get("liqPenalty")) == 0
             and R._number("0" if history.get("settledPnl") == "" else history.get("settledPnl"), signed=True) == 0,
             "EMERGENCY_RECEIPT_ACCOUNTING_INVALID")
    _require(proof["exposure_algo_types"] == list(ALGO_TYPES), "EMERGENCY_EXPOSURE_INVALID")
    for suffix in ("before", "after"):
        _require(proof["exposure_"+suffix] == {"positions": [], "pending": [], "algos": []}, "EMERGENCY_EXPOSURE_INVALID")
        exposure = proof["exposure_raw_"+suffix]
        _require(type(exposure) is dict and set(exposure) == {"positions", "pending", *("algo:"+k for k in ALGO_TYPES)}
                 and all(type(v) is list and len(v) < 100 for v in exposure.values())
                 and exposure["pending"] == [] and all(exposure["algo:"+k] == [] for k in ALGO_TYPES),
                 "EMERGENCY_EXPOSURE_INVALID")
        _require(all(type(row) is dict and type(row.get("instId")) is str and row["instId"]
                     and row.get("posSide") in {"net", "long", "short"} and R._number(row.get("pos"), signed=True) == 0
                     for row in exposure["positions"]), "EMERGENCY_EXPOSURE_INVALID")
    return proof


def _project(craw, rraw, pins, data, proof, receipt_path, resolved):
    projected = dict(data, status="CLOSED", code=CLOSED_CODE,
        position_id=proof["position_id"], position_opened_at_ms=proof["position_opened_at_ms"],
        position_closed_at_ms=proof["position_closed_at_ms"],
        emergency_close_receipt_sha256=pins.receipt_sha256, emergency_close_receipt_path=receipt_path)
    outputs = []
    for kind, raw in (("controller", craw), ("router", rraw)):
        with closing(sqlite3.connect(":memory:")) as db:
            db.deserialize(raw)
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA temp_store=MEMORY")
            if kind == "controller":
                changed = db.execute("UPDATE controller_signals SET status='CLOSED',code=?,updated_ms=? WHERE signal=? AND status='HALTED' AND code=?",
                                     (CLOSED_CODE, resolved, pins.signal_id, HALT_CODE))
                removed = db.execute("DELETE FROM controller_meta WHERE key='halt' AND value=?", (HALT_CODE,))
            else:
                changed = db.execute("UPDATE routes SET status='CLOSED',data=? WHERE account=? AND signal=? AND status='HALTED' AND owner=''",
                                     (json.dumps(projected, sort_keys=True), R.ACCOUNT_KEY, pins.signal_id))
                removed = db.execute("DELETE FROM controls WHERE account=? AND code=?", (R.ACCOUNT_KEY, HALT_CODE))
            _require(changed.rowcount == removed.rowcount == 1, "EMERGENCY_PROJECTION_CONFLICT")
            db.commit()
            outputs.append(db.serialize())
    return outputs


def _import_delta(seed, projected, target, target_raw, allowed, prefix):
    deltas, counts = {}, []
    for name, (_, rows) in seed[1].items():
        now = projected[1][name][1]
        difference = _multiset(now) - _multiset(rows)
        _require(not (_multiset(rows) - _multiset(now)), "EMERGENCY_SOURCE_HISTORY_CHANGED")
        _require(name in allowed or not difference, "EMERGENCY_UNEXPECTED_DELTA")
        if difference:
            additions = []
            for row in now:
                key = next(iter(_multiset([row])))
                if difference[key]:
                    additions.append(row)
                    difference[key] -= 1
            deltas[name] = additions
            counts.append((prefix+"."+name, len(additions)))
    with closing(sqlite3.connect(":memory:")) as db:
        db.deserialize(target_raw)
        db.execute("PRAGMA trusted_schema=OFF")
        db.execute("PRAGMA temp_store=MEMORY")
        for name, additions in sorted(deltas.items()):
            columns = target[1][name][0]
            quoted = '"' + name + '"'
            sql = "INSERT INTO " + quoted + " VALUES(" + ",".join("?" for _ in columns) + ")"
            db.executemany(sql, additions)  # No replace/ignore: every collision refuses.
        db.commit()
        output = db.serialize()
    actual = R._database(output)
    _require(actual[0] == target[0], "EMERGENCY_SCHEMA_CHANGED")
    for name, (_, rows) in target[1].items():
        _require(_multiset(actual[1][name][1]) == _multiset(rows) + _multiset(deltas.get(name, [])),
                 "EMERGENCY_IMPORT_CHANGED")
    return output, counts


def project_emergency_terminal_extension(source_seed_controller: bytes, source_seed_router: bytes,
        source_working_controller: bytes, source_working_router: bytes,
        target_seed_controller: bytes, target_seed_router: bytes, receipt_raw: bytes, *,
        pins: EmergencyProjectionPins, resolved_at_ms: int) -> EmergencyProjectionPlan:
    """Prepare terminal history only; pins must be supplied independently.

``incident_route_sha256`` hashes canonical JSON of the original router ``data``
object, not its SQL wrapper. Every database pin hashes its exact input bytes.
The receipt pin hashes canonical JSON, so callers may retain original formatting
separately. Returned receipt/path describe bytes to retain during publication.
"""
    try:
        with localcontext() as context:
            context.prec = 200
            _require(type(pins) is EmergencyProjectionPins, "EMERGENCY_PINS_INVALID")
            values = {f.name: getattr(pins, f.name) for f in fields(pins)}
            _require(all(type(v) is str and re.fullmatch(r"[0-9a-f]{64}", v) for k, v in values.items() if k != "signal_id")
                     and type(pins.signal_id) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,96}", pins.signal_id), "EMERGENCY_PINS_INVALID")
            images = (source_seed_controller, source_seed_router, source_working_controller,
                      source_working_router, target_seed_controller, target_seed_router)
            names = ("source_seed_controller", "source_seed_router", "source_controller", "source_router", "target_seed_controller", "target_seed_router")
            _require(all(type(raw) is bytes and _sha(raw) == values[name+"_sha256"] for name, raw in zip(names, images)),
                     "EMERGENCY_DATABASE_PIN_MISMATCH")
            databases = tuple(_database(raw, CONTROLLER_TABLES if i % 2 == 0 else ROUTER_TABLES) for i, raw in enumerate(images))
            seeds, working, target = databases[:2], databases[2:4], databases[4:]
            S.validate_journal_extension(*images[:2], *images[:2])
            S.validate_journal_extension(*images[4:], *images[4:])
            _seed_accounts(seeds, target, pins)
            _seed_pair(seeds, target)
            incident_c, incident_r, data, intent = _validate_working(seeds, working, pins)
            proof = _receipt(receipt_raw, pins, data, intent, resolved_at_ms)
            _require(resolved_at_ms >= incident_c["updated_ms"], "EMERGENCY_RECEIPT_TIME_INVALID")
            receipt = _canonical(proof)
            path = "logs/emergency-receipts/" + pins.receipt_sha256 + ".json"
            projected = _project(*images[2:4], pins, data, proof, path, resolved_at_ms)
            S.validate_journal_extension(*images[:2], *projected)
            projected_db = (R._database(projected[0]), R._database(projected[1]))
            # The existing gate permits active extensions, so explicitly narrow
            # every extension to terminal/unowned records before importing it.
            _require(all(row["status"] in TERMINAL for row in _rows(projected_db[0], "controller_signals"))
                     and all(row["status"] in TERMINAL and row["owner"] == "" for row in _rows(projected_db[1], "routes")),
                     "EMERGENCY_ACTIVE_WORK_PRESENT")
            controller, cc = _import_delta(seeds[0], projected_db[0], target[0], images[4],
                                          {"controller_signals", "technical_tests"}, "controller")
            router, rc = _import_delta(seeds[1], projected_db[1], target[1], images[5], {"routes"}, "router")
            S.validate_journal_extension(*images[4:], controller, router)
            counts = tuple(sorted(cc+rc))
            provenance = _canonical(dict(schema=1, environment="DEMO", purpose="EMERGENCY_TERMINAL_EXTENSION_PREPARATION_ONLY",
                pins=values, resolved_at_ms=resolved_at_ms, imported_counts=[list(pair) for pair in counts],
                original_controller_row_sha256=_sha(_canonical(incident_c)), original_router_row_sha256=_sha(_canonical(incident_r)),
                receipt_input_sha256=_sha(receipt_raw), receipt_path=path,
                can_resume=False, can_activate=False, exchange_authenticity_verified=False))
            return EmergencyProjectionPlan(controller, router, *projected, receipt, provenance,
                _sha(controller), _sha(router), _sha(projected[0]), _sha(projected[1]), pins.receipt_sha256, path, _sha(provenance), counts)
    except EmergencyProjectionRefused:
        raise
    except (R.ResolutionRefused, S.SuccessorRefused) as error:
        raise EmergencyProjectionRefused(error.code) from None
    except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError, ArithmeticError, UnicodeError):
        raise EmergencyProjectionRefused("EMERGENCY_PROJECTION_INVALID") from None
