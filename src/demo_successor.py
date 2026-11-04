"""Pure journal projection for a separately gated DEMO successor generation.

Only explicitly supplied bytes enter this module. Original journals and receipts
are retained by the caller; this module opens only in-memory SQLite databases.
Projection is preparation, never an activation or a fresh account observation.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from dataclasses import dataclass, field
from decimal import localcontext
import hashlib
import json
import re
import sqlite3

from . import demo_halted_resolution as R


RESOLUTIONS = "halted_resolutions"
CLOSED_CODE = "DEMO_HALTED_EXIT_VERIFIED"
TERMINAL = {"CLOSED", "CANCELED", "REJECTED"}
ACTIVE = {"RESERVED", "SUBMITTING", "PENDING", "PROTECTING", "PROTECTED", "CLOSING"}


class SuccessorRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code="SUCCESSOR_EVIDENCE_INVALID"):
    if not condition:
        raise SuccessorRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _commit(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{40}", value) is not None


@dataclass(frozen=True, slots=True)
class SuccessorJournals:
    controller: bytes = field(repr=False)
    router: bytes = field(repr=False)
    resolution: bytes = field(repr=False)
    controller_sha256: str
    router_sha256: str
    resolution_sha256: str
    activation_allowed: bool = field(default=False, init=False)
    fresh_account_verified: bool = field(default=False, init=False)
    preparation_only: bool = field(default=True, init=False)


def _files(files):
    return R._archive(files)


def _project_validated_resolution(original_files, observed_files, anchors, *,
                                 source_commit, target_commit, resolved_at_ms,
                                 cache_change=None, cache_context=None) -> SuccessorJournals:
    """Create two consistent seed images from fully pinned closure evidence.

``cache_context`` supplies account-config/risk-config JSON bytes and optionally
new raw account-view bytes. A cache certificate is recomputed from the exact
archive bytes; a manually constructed dataclass is not accepted as proof.
Neither archived consistency nor projection grants current account admission.
    """
    try:
        _require(_commit(source_commit) and _commit(target_commit)
                 and source_commit != target_commit, "SUCCESSOR_COMMIT_INVALID")
        R._integer(resolved_at_ms)
        old, source = _files(original_files), _files(observed_files)
        change = None
        if cache_change is not None:
            # Import lazily so ordinary immutable closure projection has no
            # account-provider or transport dependencies.
            from .demo_cache_continuity import CacheContinuity, validate_cache_continuity
            _require(type(cache_change) is CacheContinuity,
                     "SUCCESSOR_CACHE_CONTINUITY_REQUIRED")
            name = R.STATE + "verified-risk-v2/" + anchors.account_hash + "/verified.json"
            _require(cache_change.account_hash == anchors.account_hash,
                     "SUCCESSOR_CACHE_ACCOUNT_MISMATCH")
            _require(type(cache_context) is tuple and len(cache_context) in (2, 3)
                     and all(type(item) is bytes for item in cache_context),
                     "SUCCESSOR_CACHE_CONTEXT_REQUIRED")
            checked = validate_cache_continuity(old[name], source[name], source[R.AUTHORIZATION],
                cache_context[0], cache_context[1],
                new_account_views_raw=cache_context[2] if len(cache_context) == 3 else None)
            _require(checked == cache_change, "SUCCESSOR_CACHE_CONTINUITY_MISMATCH")
            change = (name, cache_change.old_sha256, cache_change.new_sha256)
        else:
            _require(cache_context is None, "SUCCESSOR_CACHE_CONTEXT_UNEXPECTED")
        plan = R._validate(original_files, observed_files, anchors, _cache_change=change)
        _require(plan.activation_allowed is False and plan.successor_ready is False
                 and resolved_at_ms >= plan.collection_completed_at_ms,
                 "SUCCESSOR_PLAN_INVALID")
        controller_db = R._database(source[R.CONTROLLER])
        router_db = R._database(source[R.ROUTER])
        signal, route_sql, route = R._journals(controller_db, router_db, plan.account_hash)
        if change is not None:
            cache_routes = R._json(source[change[0]])["evidence"]["routes"]
            actual_routes = [R._json(row["data"].encode("utf-8")) for row in
                R._rows(router_db, "routes", ("signal", "data"))]
            _require(R.canonical_json(sorted(cache_routes, key=lambda row: row["signal_id"])) ==
                     R.canonical_json(sorted(actual_routes, key=lambda row: row["signal_id"])),
                     "SUCCESSOR_CACHE_ROUTE_MISMATCH")
        _require(RESOLUTIONS not in controller_db[1] and RESOLUTIONS not in router_db[1],
                 "SUCCESSOR_ALREADY_PROJECTED")
        meta = R._rows(controller_db, "controller_meta", ("key", "value"))
        controls = R._rows(router_db, "controls", ("account", "code"))
        record = {
            "schema": 1, "environment": "DEMO",
            "purpose": "DEMO_HALTED_SUCCESSOR_RESOLUTION",
            "signal_id": R.SIGNAL, "account_hash": plan.account_hash,
            "source_commit": source_commit, "target_commit": target_commit,
            "resolved_at_ms": resolved_at_ms,
            "original_inventory_sha256": plan.original_inventory_sha256,
            "source_inventory_sha256": plan.observed_inventory_sha256,
            "source_controller_sha256": _sha(source[R.CONTROLLER]),
            "source_router_sha256": _sha(source[R.ROUTER]),
            "proof_sha256": plan.proof_sha256,
            "original_fail_sha256": plan.original_fail_sha256,
            "intent_sha256": plan.intent_sha256,
            "original_controller_signal": signal,
            "original_router_row": route_sql,
            "original_controller_halt": [row for row in meta if row["key"] == "halt"],
            "original_router_controls": controls,
            "projection": {"status": "CLOSED", "code": CLOSED_CODE},
            "cache_change": list(change) if change else None,
            "original_fail_retained": True, "activation_allowed": False,
        }
        raw_record = R.canonical_json(record)
        results = _project_images(source[R.CONTROLLER], source[R.ROUTER], raw_record,
                                  resolved_at_ms, route)
        _validate_seed(results[0], results[1])
        validate_seed_against_source(results[0], results[1], source[R.CONTROLLER], source[R.ROUTER])
        return SuccessorJournals(results[0], results[1], raw_record,
                                 _sha(results[0]), _sha(results[1]), _sha(raw_record))
    except SuccessorRefused:
        raise
    except R.ResolutionRefused as error:
        raise SuccessorRefused(error.code) from None
    except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
        raise SuccessorRefused("SUCCESSOR_PROJECTION_INVALID") from None


def project_validated_resolution(original_files, observed_files, anchors, *,
                                 source_commit, target_commit, resolved_at_ms,
                                 cache_change=None, cache_context=None) -> SuccessorJournals:
    """Use the original archived validator's precision for every numeric check."""
    with localcontext() as context:
        context.prec = 200
        return _project_validated_resolution(original_files, observed_files, anchors,
            source_commit=source_commit, target_commit=target_commit, resolved_at_ms=resolved_at_ms,
            cache_change=cache_change, cache_context=cache_context)


def _project_images(controller_raw, router_raw, record_raw, resolved_at_ms, route):
    """Perform the sole allowed projection, exclusively in memory."""
    projected = dict(route, status="CLOSED", code=CLOSED_CODE)
    results = []
    for kind, raw in (("controller", controller_raw), ("router", router_raw)):
        with closing(sqlite3.connect(":memory:")) as db:
            db.deserialize(raw)
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA temp_store=MEMORY")
            db.execute("CREATE TABLE halted_resolutions (signal TEXT PRIMARY KEY,record TEXT NOT NULL)")
            db.execute("INSERT INTO halted_resolutions VALUES(?,?)", (R.SIGNAL, record_raw.decode("ascii")))
            if kind == "controller":
                changed = db.execute("UPDATE controller_signals SET status='CLOSED',code=?,updated_ms=? WHERE signal=? AND status='HALTED'",
                                     (CLOSED_CODE, resolved_at_ms, R.SIGNAL))
                _require(changed.rowcount == 1, "SUCCESSOR_PROJECTION_CONFLICT")
                deleted = db.execute("DELETE FROM controller_meta WHERE key='halt'")
                _require(deleted.rowcount == 1, "SUCCESSOR_PROJECTION_CONFLICT")
                db.execute("CREATE TABLE IF NOT EXISTS natural_exits (signal TEXT PRIMARY KEY,record TEXT NOT NULL)")
            else:
                changed = db.execute("UPDATE routes SET status='CLOSED',data=? WHERE account=? AND signal=? AND status='HALTED' AND owner=''",
                                     (json.dumps(projected, sort_keys=True), R.ACCOUNT_KEY, R.SIGNAL))
                _require(changed.rowcount == 1, "SUCCESSOR_PROJECTION_CONFLICT")
                deleted = db.execute("DELETE FROM controls WHERE account=?", (R.ACCOUNT_KEY,))
                _require(deleted.rowcount == 1, "SUCCESSOR_PROJECTION_CONFLICT")
            db.commit()
            results.append(db.serialize())
    return results


def _row_multiset(rows):
    def cell(value):
        return [type(value).__name__, value.hex() if type(value) is bytes else value]
    return Counter(R.canonical_json([cell(v) for v in row]) for row in rows)


def _validate_seed(controller_raw, router_raw):
    c, r = R._database(controller_raw), R._database(router_raw)
    records = []
    for database in (c, r):
        rows = R._rows(database, RESOLUTIONS, ("signal", "record"))
        _require(len(rows) == 1 and rows[0]["signal"] == R.SIGNAL
                 and type(rows[0]["record"]) is str, "SUCCESSOR_RESOLUTION_INVALID")
        records.append(R._json(rows[0]["record"].encode("ascii")))
    _require(R.canonical_json(records[0]) == R.canonical_json(records[1]),
             "SUCCESSOR_RESOLUTION_MISMATCH")
    record = records[0]
    _require(record.get("schema") == 1 and type(record.get("schema")) is int
             and record.get("environment") == "DEMO"
             and record.get("purpose") == "DEMO_HALTED_SUCCESSOR_RESOLUTION"
             and record.get("signal_id") == R.SIGNAL
             and record.get("activation_allowed") is False
             and record.get("original_fail_retained") is True
             and record.get("projection") == {"status": "CLOSED", "code": CLOSED_CODE},
             "SUCCESSOR_RESOLUTION_INVALID")
    meta = dict((row["key"], row["value"]) for row in R._rows(c, "controller_meta", ("key", "value")))
    _require(meta == {"account": R.ACCOUNT_KEY, "schema": "1"}
             and not R._rows(r, "controls", ("account", "code")),
             "SUCCESSOR_SEED_HALTED")
    signals = R._rows(c, "controller_signals", ("signal", "status", "code", "intent", "updated_ms"))
    routes = R._rows(r, "routes", ("account", "signal", "fingerprint", "client", "status", "owner", "data"))
    incident_c = [x for x in signals if x["signal"] == R.SIGNAL]
    incident_r = [x for x in routes if x["signal"] == R.SIGNAL]
    _require(len(incident_c) == len(incident_r) == 1
             and all(x["status"] in TERMINAL for x in signals + routes),
             "SUCCESSOR_SEED_UNRESOLVED")
    original_c, original_r = record["original_controller_signal"], record["original_router_row"]
    _require(original_c["status"] == original_r["status"] == "HALTED",
             "SUCCESSOR_RESOLUTION_INVALID")
    expected_c = dict(original_c, status="CLOSED", code=CLOSED_CODE, updated_ms=record["resolved_at_ms"])
    original_data = R._json(original_r["data"].encode("utf-8"))
    expected_data = dict(original_data, status="CLOSED", code=CLOSED_CODE)
    actual_r = incident_r[0]
    _require(R.canonical_json(incident_c[0]) == R.canonical_json(expected_c)
             and R.canonical_json({k: v for k, v in actual_r.items() if k != "data"}) ==
                 R.canonical_json({k: v for k, v in dict(original_r, status="CLOSED").items() if k != "data"})
             and R.canonical_json(R._json(actual_r["data"].encode("utf-8"))) == R.canonical_json(expected_data)
             and actual_r["owner"] == "" and expected_data.get("exit_reconciled") is True
             and expected_data.get("exit_reconcile_code") == CLOSED_CODE
             and expected_data.get("exit_reconcile_sha256") == record["proof_sha256"]
             and expected_data.get("account_hash") == record["account_hash"]
             and R._number(expected_data.get("filled_qty"), positive=True) ==
                 R._number(expected_data.get("confirmed_closed_qty"), positive=True)
             and R._number(expected_data.get("protected_qty")) == 0,
             "SUCCESSOR_INCIDENT_PROJECTION_CHANGED")
    return c, r


def validate_seed_against_source(seed_controller: bytes, seed_router: bytes,
                                 source_controller: bytes, source_router: bytes) -> None:
    """Prove the seed is exactly the controlled projection of pinned source bytes.

The caller must independently pin the complete source tree and raw closure
proof. This comparison preserves all source schemas and typed rows, including
unrelated history, technical-test counters and any existing natural exits.
    """
    try:
        seeds = _validate_seed(seed_controller, seed_router)
        sources = (R._database(source_controller), R._database(source_router))
        record = R._json(R._rows(seeds[0], RESOLUTIONS, ("signal", "record"))[0]["record"].encode("ascii"))
        _require(record["source_controller_sha256"] == _sha(source_controller)
                 and record["source_router_sha256"] == _sha(source_router),
                 "SUCCESSOR_SOURCE_JOURNAL_MISMATCH")
        signal, route_sql, route = R._journals(*sources, record["account_hash"])
        _require(all(RESOLUTIONS not in db[1] for db in sources), "SUCCESSOR_ALREADY_PROJECTED")
        meta = R._rows(sources[0], "controller_meta", ("key", "value"))
        controls = R._rows(sources[1], "controls", ("account", "code"))
        _require(R.canonical_json(signal) == R.canonical_json(record["original_controller_signal"])
                 and R.canonical_json(route_sql) == R.canonical_json(record["original_router_row"])
                 and R.canonical_json([row for row in meta if row["key"] == "halt"]) ==
                     R.canonical_json(record["original_controller_halt"])
                 and R.canonical_json(controls) == R.canonical_json(record["original_router_controls"]),
                 "SUCCESSOR_SOURCE_INCIDENT_MISMATCH")
        resolved = R._integer(record["resolved_at_ms"])
        _require(resolved >= R._integer(route["exit_reconcile_collection_completed_at_ms"])
                 and _commit(record["source_commit"]) and _commit(record["target_commit"])
                 and record["source_commit"] != record["target_commit"],
                 "SUCCESSOR_RESOLUTION_INVALID")
        expected = _project_images(source_controller, source_router,
                                   R.canonical_json(record), resolved, route)
        for seed, raw in zip(seeds, expected):
            _require(R._snapshot(seed) == R._snapshot(R._database(raw)),
                     "SUCCESSOR_SOURCE_HISTORY_CHANGED")
    except SuccessorRefused:
        raise
    except R.ResolutionRefused as error:
        raise SuccessorRefused(error.code) from None
    except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
        raise SuccessorRefused("SUCCESSOR_SOURCE_PROJECTION_INVALID") from None


def validate_journal_extension(seed_controller: bytes, seed_router: bytes,
                               current_controller: bytes, current_router: bytes) -> None:
    """Retain all immutable seed rows while allowing later valid journal rows.

This is a historical-preservation gate. Fresh account/entry checks, deployment
pins, process locks and operator STOP handling remain separate required gates.
    """
    try:
        seeds = _validate_seed(seed_controller, seed_router)
        currents = (R._database(current_controller), R._database(current_router))
        for seed, current in zip(seeds, currents):
            _require(seed[0] == current[0], "SUCCESSOR_JOURNAL_SCHEMA_CHANGED")
            for name, (columns, rows) in seed[1].items():
                now_columns, now_rows = current[1][name]
                _require(columns == now_columns and not (_row_multiset(rows) - _row_multiset(now_rows)),
                         "SUCCESSOR_HISTORY_CHANGED")
                if name in {RESOLUTIONS, "controller_meta", "controls"}:
                    _require(_row_multiset(rows) == _row_multiset(now_rows),
                             "SUCCESSOR_CONTROL_OR_RESOLUTION_CHANGED")
        c, r = currents
        signals = R._rows(c, "controller_signals", ("signal", "status", "code", "intent", "updated_ms"))
        routes = R._rows(r, "routes", ("account", "signal", "fingerprint", "client", "status", "owner", "data"))
        old_signals = {row["signal"] for row in R._rows(seeds[0], "controller_signals", ("signal",))}
        old_routes = {row["signal"] for row in R._rows(seeds[1], "routes", ("signal",))}
        signal_ids = {row["signal"] for row in signals}
        _require(len(signal_ids) == len(signals) and len({row["signal"] for row in routes}) == len(routes),
                 "SUCCESSOR_JOURNAL_INVALID")
        active_c, active_r = set(), set()
        for row in signals:
            if row["signal"] in old_signals:
                continue  # Exact typed historical rows were already retained.
            _require(type(row["signal"]) is str and bool(row["signal"])
                     and row["status"] in TERMINAL | (ACTIVE - {"PROTECTING"}),
                     "SUCCESSOR_JOURNAL_UNRESOLVED")
            if row["status"] in ACTIVE:
                _require(type(row["intent"]) is str
                         and R._json(row["intent"].encode()).get("signal_id") == row["signal"],
                         "SUCCESSOR_JOURNAL_INVALID")
                active_c.add(row["signal"])
        route_ids = set()
        for row in routes:
            if row["signal"] in old_routes:
                route_ids.add(row["signal"])
                continue
            data = R._json(row["data"].encode("utf-8"))
            _require(row["account"] == R.ACCOUNT_KEY and row["signal"] in signal_ids
                     and row["status"] in TERMINAL | ACTIVE
                     and data.get("signal_id") == row["signal"] and data.get("status") == row["status"]
                     and data.get("inst_id") == "SOL-USDT-SWAP"
                     and data.get("client_order_id") == row["client"],
                     "SUCCESSOR_JOURNAL_INVALID")
            route_ids.add(row["signal"])
            if row["status"] in ACTIVE:
                active_r.add(row["signal"])
        _require(len(active_c) <= 1 and len(active_r) <= 1
                 and active_c <= route_ids and active_r <= active_c,
                 "SUCCESSOR_JOURNAL_INVALID")
    except SuccessorRefused:
        raise
    except R.ResolutionRefused as error:
        raise SuccessorRefused(error.code) from None
    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
        raise SuccessorRefused("SUCCESSOR_JOURNAL_INVALID") from None
