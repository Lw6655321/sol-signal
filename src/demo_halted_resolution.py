"""Phase 1: consistency of explicitly supplied archived HALTED evidence only.

No file paths are opened, no project execution/account modules are imported, and
no SQL, runtime copy, clearance or activation instructions are returned. Hashes
anchor caller-supplied bytes; they do not authenticate an exchange response.
The existing proof omits some original read waves and full ledger coverage.
Consequently even a consistent archive is NEVER successor-ready or executable.
"""
from __future__ import annotations

from contextlib import closing
import base64
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
from pathlib import PureWindowsPath
import re
import sqlite3


SIGNAL = "DEMO-TEST-SMOKE-1792501260297"
ACCOUNT_KEY = "DEMO-AUTO-SOL-V1"
STATE = "state/demo-auto/"
CONTROLLER = STATE + "controller.sqlite3"
ROUTER = STATE + "router.sqlite3"
AUTHORIZATION = STATE + "auto-authorization.json"
BINDING = STATE + "account-binding.json"
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
BLOCKING_GAPS = (
    "EXCHANGE_ORIGIN_NOT_AUTHENTICATED_OFFLINE",
    "CONFIG_FULL_HISTORY_AND_REPEATED_READ_WAVES_NOT_ARCHIVED",
    "MONOTONIC_ORIGIN_NOT_ARCHIVED_ONLY_STORED_DURATION_CHECKED",
    "FULL_LEDGER_COVERAGE_AND_CASHFLOW_BASELINE_NOT_VALIDATED",
    "FILESYSTEM_LOCKS_REPARSE_EMPTY_DIRECTORIES_AND_CODE_PINS_NOT_VALIDATED",
    "FRESH_ORDINARY_PROVIDER_AND_ACTIVATION_INTERFACE_NOT_IMPLEMENTED",
)


class ResolutionRefused(ValueError):
    """Only fixed local refusal codes, never input or SQLite exception text."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition, code):
    if not condition:
        raise ResolutionRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _hash(value):
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _json_types(value, depth=0):
    _require(depth <= 64, "RESOLUTION_JSON_INVALID")
    if type(value) is dict:
        _require(all(type(key) is str for key in value), "RESOLUTION_JSON_INVALID")
        for item in value.values():
            _json_types(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _json_types(item, depth + 1)
    else:
        _require(type(value) in (str, int, float, bool, type(None)), "RESOLUTION_JSON_INVALID")
        _require(type(value) is not float or math.isfinite(value), "RESOLUTION_JSON_INVALID")


def canonical_json(value) -> bytes:
    """JSON type identity matters: true, 1 and 1.0 are different evidence."""
    try:
        _json_types(value)
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeError):
        raise ResolutionRefused("RESOLUTION_JSON_INVALID") from None


def _pairs(items):
    result = {}
    for key, value in items:
        _require(key not in result, "RESOLUTION_JSON_DUPLICATE")
        result[key] = value
    return result


def _json(raw):
    _require(type(raw) is bytes and 0 < len(raw) <= MAX_FILE_BYTES, "RESOLUTION_BYTES_INVALID")
    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(
                                ResolutionRefused("RESOLUTION_JSON_INVALID")))
        canonical_json(result)
        return result
    except ResolutionRefused:
        raise
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise ResolutionRefused("RESOLUTION_JSON_INVALID") from None


def _relative(name):
    _require(type(name) is str and len(name) <= 512 and name.startswith((STATE, "logs/"))
             and "\\" not in name and ":" not in name, "RESOLUTION_PATH_INVALID")
    reserved = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} | {
        prefix + digit for prefix in ("COM", "LPT") for digit in "123456789¹²³"}
    _require(all(part not in ("", ".", "..") and part == part.rstrip(" .")
                 and not any(ord(c) < 32 or ord(c) == 127 or c in '<>"|?*' for c in part)
                 and part.split(".", 1)[0].upper() not in reserved for part in name.split("/")),
             "RESOLUTION_PATH_INVALID")
    return name


def _proof_path(name, digest):
    # R15's reviewed short log directory encodes all 16 UUID bytes. Accept
    # exactly that one optional parent; no arbitrary nested log ancestry.
    match = re.fullmatch(r"logs/(?:f15-([A-Za-z0-9_-]{22})/)?router/run-[A-Za-z0-9-]{1,96}/halted-exit-"
                         + digest + r"\.json", name)
    _require(match is not None, "RESOLUTION_PATH_INVALID")
    token = match.group(1)
    if token is not None:
        try:
            uuid_bytes = base64.b64decode(token + "==", altchars=b"-_", validate=True)
        except ValueError:
            raise ResolutionRefused("RESOLUTION_PATH_INVALID") from None
        _require(len(uuid_bytes) == 16
                 and base64.urlsafe_b64encode(uuid_bytes).decode("ascii").rstrip("=") == token,
                 "RESOLUTION_PATH_INVALID")
    return name


@dataclass(frozen=True, slots=True)
class EvidenceFile:
    name: str
    raw: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ResolutionAnchors:
    """Explicit caller anchors, not credentials or independent authenticity."""

    account_hash: str = field(repr=False)
    intent_sha256: str
    authorization_sha256: str
    original_fail_sha256: str
    original_inventory_sha256: str
    observed_inventory_sha256: str
    proof_sha256: str
    original_fail_path: str
    proof_path: str
    observed_root: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ResolutionPlan:
    signal_id: str
    account_hash: str = field(repr=False)
    intent_sha256: str
    authorization_sha256: str
    original_fail_sha256: str
    original_inventory_sha256: str
    observed_inventory_sha256: str
    proof_sha256: str
    position_id: str
    position_opened_at_ms: int
    position_closed_at_ms: int
    exit_order_ids: tuple[str, ...]
    confirmed_closed_qty: str
    original_status: str
    original_code: str
    collection_started_at_ms: int
    collection_completed_at_ms: int
    collection_elapsed_monotonic_ns: int
    original_file_sha256: tuple[tuple[str, str], ...]
    observed_file_sha256: tuple[tuple[str, str], ...]
    validation_scope: str = field(default="ARCHIVED_CLOSURE_CONSISTENCY_ONLY", init=False)
    activation_allowed: bool = field(default=False, init=False)
    successor_ready: bool = field(default=False, init=False)
    diagnostic_admission_allowed: bool = field(default=False, init=False)
    proposed_only: bool = field(default=True, init=False)
    blocking_gaps: tuple[str, ...] = field(default=BLOCKING_GAPS, init=False)


def _archive(files):
    _require(type(files) is tuple and 1 <= len(files) <= 4096, "RESOLUTION_ARCHIVE_INVALID")
    result, folded, total = {}, set(), 0
    for item in files:
        _require(type(item) is EvidenceFile and type(item.raw) is bytes
                 and len(item.raw) <= MAX_FILE_BYTES, "RESOLUTION_ARCHIVE_INVALID")
        name = _relative(item.name)
        _require(name.casefold() not in folded, "RESOLUTION_ARCHIVE_DUPLICATE")
        folded.add(name.casefold())
        total += len(item.raw)
        _require(total <= MAX_ARCHIVE_BYTES, "RESOLUTION_ARCHIVE_INVALID")
        if name.casefold().endswith(("-wal", "-journal", "-shm")):
            _require(not item.raw, "RESOLUTION_SQLITE_SIDECAR_NONEMPTY")
        if name.casefold().endswith(".json"):
            _json(item.raw)
        result[name] = item.raw
    _require(all("/".join(name.split("/")[:index]).casefold() not in folded
                 for name in result for index in range(1, len(name.split("/")))),
             "RESOLUTION_ARCHIVE_INVALID")
    return result


def archive_digest(files: tuple[EvidenceFile, ...]) -> str:
    """Digest the exact supplied file set; empty directories are not represented."""
    table = [[name, _sha(raw)] for name, raw in sorted(_archive(files).items())]
    return _sha(canonical_json(table))


def _integer(value, code="RESOLUTION_TIME_INVALID", *, zero=False):
    _require(type(value) is int and (0 if zero else 1) <= value <= 2**63 - 1, code)
    return value


def _number(value, *, positive=False, signed=False):
    _require(type(value) is str and re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]+)?)(?:[Ee][+-]?[0-9]{1,2})?", value)
             and len(value) <= 96, "RESOLUTION_NUMBER_INVALID")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ResolutionRefused("RESOLUTION_NUMBER_INVALID") from None
    _require(result.is_finite() and len(result.as_tuple().digits) <= 40
             and -40 <= result.as_tuple().exponent <= 40
             and (signed or result >= 0) and (not positive or result > 0), "RESOLUTION_NUMBER_INVALID")
    return result


def _identity(value):
    _require(type(value) is str and re.fullmatch(r"[A-Za-z0-9]{1,32}", value), "RESOLUTION_ID_INVALID")
    return value


def _stamp(value):
    if type(value) is str:
        _require(value.isascii() and value.isdigit() and len(value) <= 19, "RESOLUTION_TIME_INVALID")
        value = int(value)
    return _integer(value)


def _database(raw):
    _require(type(raw) is bytes and raw.startswith(b"SQLite format 3\0")
             and raw[18:20] == b"\x01\x01", "RESOLUTION_SQLITE_INVALID")
    try:
        with closing(sqlite3.connect(":memory:")) as db:
            _require(hasattr(db, "deserialize"), "RESOLUTION_SQLITE_UNSUPPORTED")
            db.deserialize(raw)
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA temp_store=MEMORY")
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_FILE_BYTES)
            db.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 65536)
            db.setlimit(sqlite3.SQLITE_LIMIT_COLUMN, 256)
            db.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
            db.set_authorizer(lambda action, first, second, _database, _origin:
                sqlite3.SQLITE_OK if action in {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ}
                or action == sqlite3.SQLITE_PRAGMA and first in {"integrity_check", "table_info"}
                else sqlite3.SQLITE_DENY)
            steps = 0
            def progress():
                nonlocal steps
                steps += 1
                return int(steps > 2000)
            db.set_progress_handler(progress, 1000)
            schema = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
            _require(all(row[0] == "index" and row[3] is None or row[0] == "table"
                         and type(row[3]) is str and not re.search(r"\b(VIRTUAL|GENERATED|CHECK)\b", row[3], re.I)
                         for row in schema),
                     "RESOLUTION_SQLITE_SCHEMA_INVALID")
            _require(db.execute("PRAGMA integrity_check").fetchall() == [("ok",)], "RESOLUTION_SQLITE_INVALID")
            tables = {}
            for kind, name, _, _sql in schema:
                if kind != "table":
                    continue
                quoted = '"' + name.replace('"', '""') + '"'
                columns = tuple(row[1] for row in db.execute("PRAGMA table_info(" + quoted + ")"))
                rows = db.execute("SELECT * FROM " + quoted).fetchmany(50001)
                _require(len(rows) <= 50000, "RESOLUTION_SQLITE_TOO_LARGE")
                tables[name] = (columns, rows)
            return schema, tables
    except ResolutionRefused:
        raise
    except (sqlite3.Error, ValueError, TypeError, OverflowError):
        raise ResolutionRefused("RESOLUTION_SQLITE_INVALID") from None


def _snapshot(database, *, mask_incident=False):
    schema, tables = database
    encoded = []
    for name, (columns, rows) in sorted(tables.items()):
        values = []
        for row in rows:
            row = list(row)
            if mask_incident and name == "routes" and row[columns.index("signal")] == SIGNAL:
                row[columns.index("data")] = "<INCIDENT_DATA>"
            cells = [[type(v).__name__, v.hex() if type(v) is bytes else v] for v in row]
            values.append(canonical_json(cells).decode("ascii"))
        encoded.append([name, list(columns), sorted(values)])
    return canonical_json([[list(row) for row in schema], encoded])


def _rows(database, table, required):
    _require(table in database[1], "RESOLUTION_SQLITE_SCHEMA_INVALID")
    columns, rows = database[1][table]
    _require(set(required) <= set(columns), "RESOLUTION_SQLITE_SCHEMA_INVALID")
    return [dict(zip(columns, row)) for row in rows]


def _journals(controller, router, account):
    meta = _rows(controller, "controller_meta", ("key", "value"))
    _require(len({row["key"] for row in meta}) == len(meta), "RESOLUTION_CONTROLLER_INVALID")
    meta = {row["key"]: row["value"] for row in meta}
    _require(meta.get("account") == ACCOUNT_KEY and meta.get("schema") == "1"
             and type(meta.get("halt")) is str and bool(meta["halt"]), "RESOLUTION_CONTROLLER_INVALID")
    signals = _rows(controller, "controller_signals", ("signal", "status", "code", "intent", "updated_ms"))
    _require(len({row["signal"] for row in signals}) == len(signals), "RESOLUTION_CONTROLLER_INVALID")
    incidents = [row for row in signals if row["signal"] == SIGNAL]
    _require(len(incidents) == 1 and incidents[0]["status"] == "HALTED"
             and all(row["status"] in {"REJECTED", "CANCELED", "CLOSED"} for row in signals if row["signal"] != SIGNAL)
             and all(row.get("owner", "") == "" for row in signals), "RESOLUTION_CONTROLLER_INVALID")
    controls = _rows(router, "controls", ("account", "code"))
    _require(len(controls) == 1 and controls[0]["account"] == ACCOUNT_KEY
             and type(controls[0]["code"]) is str and bool(controls[0]["code"]), "RESOLUTION_HALT_INVALID")
    routes = _rows(router, "routes", ("account", "signal", "fingerprint", "client", "status", "owner", "data"))
    _require(len({row["signal"] for row in routes}) == len(routes), "RESOLUTION_ROUTE_INVALID")
    result = []
    for row in routes:
        data = _json(row["data"].encode("utf-8")) if type(row["data"]) is str else None
        _require(type(data) is dict and row["account"] == ACCOUNT_KEY and row["owner"] == ""
                 and data.get("signal_id") == row["signal"] and data.get("status") == row["status"]
                 and data.get("account_hash") == account, "RESOLUTION_ROUTE_INVALID")
        _require(row["status"] == "HALTED" if row["signal"] == SIGNAL else
                 row["status"] in {"REJECTED", "CANCELED", "CLOSED"}, "RESOLUTION_EXTRA_UNRESOLVED")
        if row["signal"] == SIGNAL:
            result.append((row, data))
    _require(len(result) == 1, "RESOLUTION_ROUTE_INVALID")
    _require(type(incidents[0]["intent"]) is str, "RESOLUTION_INTENT_INVALID")
    return incidents[0], result[0][0], result[0][1]


def _intent(raw):
    value = _json(raw)
    numbers = {"entry", "stop", "tp1", "tp2", "qty", "notional", "contract_value", "lot_size", "tick_size",
               "maker_fee_rate", "taker_fee_rate", "slippage_bps", "risk_budget", "effective_risk", "rr", "net_rr"}
    _require(type(value) is dict and set(value) == numbers | {
        "signal_id", "inst_id", "side", "created_at_ms", "expires_at_ms", "require_maker", "kind"},
        "RESOLUTION_INTENT_INVALID")
    _require(value["signal_id"] == SIGNAL and value["inst_id"] == "SOL-USDT-SWAP"
             and value["side"] in {"long", "short"} and type(value["require_maker"]) is bool
             and type(value["kind"]) is str, "RESOLUTION_INTENT_INVALID")
    _require(_integer(value["expires_at_ms"]) > _integer(value["created_at_ms"]), "RESOLUTION_INTENT_INVALID")
    for key in numbers:
        _number(value[key], positive=key not in {"maker_fee_rate", "taker_fee_rate", "slippage_bps"})
    return value


def _flag(value, expected):
    return value is expected if type(value) is bool else type(value) is str and value == str(expected).lower()


def _raw_closure(proof, intent, route):
    """Check retained raw owned receipts, not missing waves or provenance."""
    required = {"position_binding_verified", "closed_lifecycle_verified", "exit_reconciled", "account_hash",
                "position_id", "position_opened_at_ms", "position_closed_at_ms", "flat_observed_at_ms",
                "observed_at_ms", "history_through_ms", "confirmed_closed_qty", "exit_order_ids", "order",
                "parent", "children", "fills", "position_history", "exposure_before", "exposure_after"}
    _require(type(proof) is dict and set(proof) == required, "RESOLUTION_PROOF_SHAPE_INVALID")
    _require(all(proof[key] is True for key in ("position_binding_verified", "closed_lifecycle_verified", "exit_reconciled")),
             "RESOLUTION_PROOF_NOT_VERIFIED")
    for wave in ("exposure_before", "exposure_after"):
        _require(type(proof[wave]) is dict and set(proof[wave]) == {"positions", "pending", "algos"}
                 and all(type(v) is list and not v for v in proof[wave].values()), "RESOLUTION_EXPOSURE_NOT_FLAT")
    size, lot = _number(intent["qty"], positive=True), _number(intent["lot_size"], positive=True)
    _require(_number(route.get("filled_qty"), positive=True) == size
             and _number(proof["confirmed_closed_qty"], positive=True) == size and not size % lot,
             "RESOLUTION_QUANTITY_INVALID")
    order_id, client, algo = (_identity(route.get(key)) for key in ("order_id", "client_order_id", "protection_order_id"))
    protection = "r" + _sha((SIGNAL + ":protection").encode())[:31]
    _require(route.get("protection_id") == protection, "RESOLUTION_PROTECTION_INVALID")
    flat, opened, closed, start, end = (_integer(proof[key]) for key in (
        "flat_observed_at_ms", "position_opened_at_ms", "position_closed_at_ms", "observed_at_ms", "history_through_ms"))
    _require(intent["created_at_ms"] <= flat <= opened <= closed <= start <= end
             and end-start <= 5000 and start-flat <= 28*86_400_000
             and type(route.get("flat_observed_at_ms")) is int and route["flat_observed_at_ms"] == flat,
             "RESOLUTION_TIME_INVALID")
    for key in ("position_id", "position_opened_at_ms"):
        old = route.get(key)
        _require(old is None or key == "position_id" and old == ""
                 or canonical_json(old) == canonical_json(proof[key]), "RESOLUTION_BINDING_CHANGED")
    position = _identity(proof["position_id"])
    entry, parent, history = (proof[key] for key in ("order", "parent", "position_history"))
    children, fills, ids = (proof[key] for key in ("children", "fills", "exit_order_ids"))
    _require(all(type(row) is dict for row in (entry, parent, history))
             and type(children) is list and all(type(row) is dict for row in children)
             and type(fills) is list and 2 <= len(fills) < 100 and all(type(row) is dict for row in fills)
             and type(ids) is list and 1 <= len(ids) <= 4
             and all(type(v) is str and re.fullmatch(r"[0-9]{1,32}", v) for v in ids)
             and len(set(ids)) == len(ids) and order_id not in ids and len(children) == len(ids),
             "RESOLUTION_RAW_RECEIPTS_INVALID")
    side = "buy" if intent["side"] == "long" else "sell"
    exit_side = "sell" if side == "buy" else "buy"
    _require(all(entry.get(k) == v for k, v in {
        "instId": "SOL-USDT-SWAP", "ordId": order_id, "clOrdId": client, "side": side,
        "posSide": "net", "tdMode": "cross", "state": "filled",
        "ordType": "post_only" if intent["require_maker"] else "limit"}.items())
        and _flag(entry.get("reduceOnly"), False) and entry.get("algoId") == ""
        and _number(entry.get("sz"), positive=True) == size
        and _number(entry.get("accFillSz"), positive=True) == size
        and _number(entry.get("px"), positive=True) == _number(intent["entry"], positive=True),
        "RESOLUTION_ENTRY_INVALID")
    _require(all(parent.get(k) == v for k, v in {
        "instId": "SOL-USDT-SWAP", "algoId": algo, "algoClOrdId": protection, "state": "effective",
        "ordType": "oco", "side": exit_side, "posSide": "net", "tdMode": "cross"}.items())
        and _flag(parent.get("reduceOnly"), True) and parent.get("actualSide") in {"sl", "tp"}
        and canonical_json(parent.get("ordIdList")) == canonical_json(ids)
        and parent.get("ordId") in ("", *ids)
        and parent.get("slOrdPx") == "-1" and parent.get("tpOrdPx") == "-1"
        and all(parent.get(key) == "last" for key in ("slTriggerPxType", "tpTriggerPxType"))
        and _number(parent.get("sz"), positive=True) == size
        and _number(parent.get("slTriggerPx"), positive=True) == _number(intent["stop"], positive=True)
        and _number(parent.get("tpTriggerPx"), positive=True) == _number(intent["tp1"], positive=True),
        "RESOLUTION_PROTECTION_INVALID")
    orders = {order_id: entry}
    for identity, child in zip(ids, children):
        _require(all(child.get(k) == v for k, v in {
            "instId": "SOL-USDT-SWAP", "ordId": identity, "algoId": algo, "posSide": "net",
            "tdMode": "cross", "side": exit_side, "state": "filled", "ordType": "market"}.items())
            and child.get("algoClOrdId") in (None, "", protection)
            and type(child.get("clOrdId")) is str and _flag(child.get("reduceOnly"), True)
            and 0 < _number(child.get("sz"), positive=True) == _number(child.get("accFillSz"), positive=True) <= size,
            "RESOLUTION_EXIT_INVALID")
        orders[identity] = child
    _require(sum(_number(row["accFillSz"]) for row in children) == size, "RESOLUTION_QUANTITY_INVALID")
    grouped, trades, bills = {identity: [] for identity in orders}, set(), set()
    for fill in fills:
        identity = fill.get("ordId")
        _require(type(identity) is str and identity in orders, "RESOLUTION_EXTERNAL_FILL")
        order = orders[identity]
        subtypes = ({"1", "3"} if side == "buy" else {"2", "4"}) if identity == order_id else (
            {"2", "5"} if side == "buy" else {"1", "6"})
        _require(all(fill.get(k) == v for k, v in {
            "instId": "SOL-USDT-SWAP", "instType": "SWAP", "clOrdId": order["clOrdId"],
            "side": order["side"], "posSide": "net", "feeCcy": "USDT"}.items())
            and fill.get("subType") in subtypes and fill.get("posId") in (None, "", position)
            and fill.get("algoId") in ((None, "") if identity == order_id else (None, "", algo)),
            "RESOLUTION_EXTERNAL_FILL")
        trade, bill = _identity(fill.get("tradeId")), _identity(fill.get("billId"))
        _require(trade not in trades and bill not in bills, "RESOLUTION_DUPLICATE_FILL")
        trades.add(trade)
        bills.add(bill)
        execution, recorded = _stamp(fill.get("fillTime")), _stamp(fill.get("ts"))
        quantity, price = _number(fill.get("fillSz"), positive=True), _number(fill.get("fillPx"), positive=True)
        _require(flat <= execution <= recorded <= min(end, execution+5000) and not quantity % lot,
                 "RESOLUTION_FILL_INVALID")
        _number(fill.get("fee"), signed=True)
        pnl = _number(fill.get("fillPnl"), signed=True)
        _require(identity != order_id or pnl == 0, "RESOLUTION_EXTERNAL_FILL")
        grouped[identity].append((execution, recorded, quantity, price))
    for identity, order in orders.items():
        records = grouped[identity]
        _require(bool(records) and sum(v[2] for v in records) == _number(order["accFillSz"])
                 and sum(v[2]*v[3] for v in records) == _number(order.get("avgPx"), positive=True)*_number(order["accFillSz"]),
                 "RESOLUTION_FILL_INVALID")
        last = max(v[0] for v in records)
        recorded = min(v[1] for v in records if v[0] == last)
        _require(_stamp(order.get("fillTime")) == last and last <= _stamp(order.get("uTime")) <= recorded
                 and flat <= _stamp(order.get("cTime")) <= min(v[0] for v in records), "RESOLUTION_TIME_INVALID")
    entries, exits = grouped[order_id], [record for identity in ids for record in grouped[identity]]
    first, last = min(v[0] for v in entries), max(v[0] for v in exits)
    trigger = _stamp(parent.get("triggerTime"))
    _require(first <= opened <= min(v[1] for v in entries if v[0] == first)
             and last <= closed <= min(v[1] for v in exits if v[0] == last)
             and max(v[0] for v in entries) <= _stamp(parent.get("cTime")) <= trigger <= min(v[0] for v in exits)
             and _stamp(parent.get("uTime")) == trigger, "RESOLUTION_TIME_INVALID")
    _require(all(history.get(k) == v for k, v in {
        "instId": "SOL-USDT-SWAP", "instType": "SWAP", "posId": position, "posSide": "net",
        "mgnMode": "cross", "direction": intent["side"], "ccy": "USDT"}.items())
        and _stamp(history.get("cTime")) == opened and _stamp(history.get("uTime")) == closed
        and _number(history.get("openMaxPos"), positive=True) == size
        and _number(history.get("closeTotalPos"), positive=True) == size
        and _number(history.get("liqPenalty")) == 0
        and "settledPnl" in history and (history["settledPnl"] == "" or _number(history["settledPnl"], signed=True) == 0),
        "RESOLUTION_POSITION_HISTORY_INVALID")
    for records, field in ((entries, "openAvgPx"), (exits, "closeAvgPx")):
        _require(sum(v[2]*v[3] for v in records) == _number(history.get(field), positive=True)*size,
                 "RESOLUTION_POSITION_HISTORY_INVALID")


def _validate(original_files, observed_files, anchors, *, _cache_change=None):
    _require(type(anchors) is ResolutionAnchors, "RESOLUTION_ANCHORS_INVALID")
    for name in ("account_hash", "intent_sha256", "authorization_sha256", "original_fail_sha256",
                 "original_inventory_sha256", "observed_inventory_sha256", "proof_sha256"):
        _require(_hash(getattr(anchors, name)), "RESOLUTION_ANCHORS_INVALID")
    fail_path, proof_path = _relative(anchors.original_fail_path), _relative(anchors.proof_path)
    _require(fail_path.startswith("logs/technical-tests/") and fail_path.endswith("/report.json")
             and _proof_path(proof_path, anchors.proof_sha256),
             "RESOLUTION_PATH_INVALID")
    root = anchors.observed_root
    _require(type(root) is str and len(root) <= 512 and re.match(r"[A-Za-z]:\\", root)
             and str(PureWindowsPath(root)) == root and not any(
                 part in ("", ".", "..") or part != part.rstrip(" .")
                 or any(ord(c) < 32 or ord(c) == 127 or c in '<>"|?*:' for c in part)
                 for part in root[3:].split("\\")),
             "RESOLUTION_PATH_INVALID")
    # Reuse the relative Windows-component checks without touching this root.
    _relative("logs/" + root[3:].replace("\\", "/"))
    original, observed = _archive(original_files), _archive(observed_files)
    _require(archive_digest(original_files) == anchors.original_inventory_sha256
             and archive_digest(observed_files) == anchors.observed_inventory_sha256, "RESOLUTION_INVENTORY_MISMATCH")
    required = {CONTROLLER, ROUTER, AUTHORIZATION, BINDING, STATE + "process.lock", STATE + "STOP_NEW_ENTRIES",
                STATE + "CONTROLLER_HALT", fail_path}
    halt_name = STATE + "HALT-" + _sha(ACCOUNT_KEY.encode())[:12]
    required.add(halt_name)
    _require(required <= set(original) and required | {proof_path} <= set(observed), "RESOLUTION_EVIDENCE_MISSING")
    _require({name for name in original if name.startswith(STATE + "HALT-")} == {halt_name}
             and {name for name in observed if name.startswith(STATE + "HALT-")} == {halt_name}
             and all(original[name] for name in (STATE + "process.lock", STATE + "STOP_NEW_ENTRIES", STATE + "CONTROLLER_HALT", halt_name)),
             "RESOLUTION_HALT_INVALID")
    if _cache_change is not None:
        name, before_sha, after_sha = _cache_change
        _require(name == STATE + "verified-risk-v2/" + anchors.account_hash + "/verified.json"
                 and name in original and name in observed
                 and _sha(original[name]) == before_sha and _sha(observed[name]) == after_sha,
                 "RESOLUTION_CACHE_CHANGE_INVALID")
    allowed_changes = {CONTROLLER, ROUTER} | ({_cache_change[0]} if _cache_change else set())
    _require(all(name in observed and (name in allowed_changes or observed[name] == raw)
                 for name, raw in original.items())
             and all(name.startswith("logs/") for name in set(observed)-set(original)), "RESOLUTION_HISTORY_CHANGED")
    _require(_sha(original[AUTHORIZATION]) == anchors.authorization_sha256
             and _sha(original[fail_path]) == anchors.original_fail_sha256
             and _sha(observed[proof_path]) == anchors.proof_sha256, "RESOLUTION_EVIDENCE_HASH_MISMATCH")
    binding, auth, fail = (_json(original[name]) for name in (BINDING, AUTHORIZATION, fail_path))
    _require(canonical_json(binding) == canonical_json({"environment": "DEMO", "account_hash": anchors.account_hash}),
             "RESOLUTION_ACCOUNT_MISMATCH")
    _require(type(auth) is dict and auth.get("environment") == "DEMO" and auth.get("account_hash") == anchors.account_hash
             and auth.get("source") == "user-approved-demo-activation"
             and auth.get("scope") == "SOL-USDT-SWAP_EXISTING_RISK", "RESOLUTION_AUTHORIZATION_INVALID")
    authorization_time = _integer(auth.get("authorization_recorded_at_ms"), "RESOLUTION_AUTHORIZATION_INVALID")
    if "service_started_at_ms" in auth:
        _require(authorization_time <= _integer(auth["service_started_at_ms"]), "RESOLUTION_AUTHORIZATION_INVALID")
    _require(type(fail) is dict and fail.get("purpose") == "DEMO_TECHNICAL_TEST_REPORT" and fail.get("scenario") == "smoke"
             and fail.get("result") == "FAIL" and fail.get("submit_attempted") is True
             and type(fail.get("plan")) is dict and type(fail["plan"].get("signal")) is dict
             and fail["plan"]["signal"].get("id") == SIGNAL, "RESOLUTION_ORIGINAL_FAIL_INVALID")
    oc, nc, old_db, new_db = (_database(raw) for raw in (
        original[CONTROLLER], observed[CONTROLLER], original[ROUTER], observed[ROUTER]))
    _require(_snapshot(oc) == _snapshot(nc) and _snapshot(old_db, mask_incident=True) == _snapshot(new_db, mask_incident=True),
             "RESOLUTION_JOURNAL_HISTORY_CHANGED")
    controller, old_sql, old = _journals(oc, old_db, anchors.account_hash)
    _, new_sql, current = _journals(nc, new_db, anchors.account_hash)
    intent = _intent(controller["intent"].encode("utf-8"))
    fingerprint = _sha(canonical_json(intent))
    client = "h" + _sha(SIGNAL.encode())[:31]
    _require(fingerprint == anchors.intent_sha256 == old_sql["fingerprint"] == new_sql["fingerprint"]
             and old_sql["client"] == new_sql["client"] == client
             and old.get("client_order_id") == client and old.get("submit_attempted") is True
             and old.get("inst_id") == intent["inst_id"] and old.get("side") == intent["side"]
             and canonical_json(old.get("created_at_ms")) == canonical_json(intent["created_at_ms"])
             and canonical_json(old.get("expires_at_ms")) == canonical_json(intent["expires_at_ms"])
             and type(old.get("code")) is str and bool(old["code"])
             and authorization_time <= intent["created_at_ms"], "RESOLUTION_INTENT_BINDING_INVALID")
    _require(old.get("exit_reconciled") is None or old.get("exit_reconciled") is False,
             "RESOLUTION_ORIGINAL_ALREADY_RECONCILED")
    _require(not any(key in old for key in ("exit_reconcile_sha256", "exit_reconcile_proof_path")),
             "RESOLUTION_ORIGINAL_ALREADY_RECONCILED")
    tests = _rows(oc, "technical_tests", ("signal", "purpose", "scenario", "authorization_id", "plan_fingerprint", "signal_json", "registered_at_ms"))
    registrations = [row for row in tests if row["signal"] == SIGNAL]
    plan = fail["plan"]
    authorization_id = "user-approved-demo-technical-tests-20261019"
    _require(len(registrations) == 1
             and set(plan) in ({"purpose", "scenario", "authorization_id", "signal"},
                               {"purpose", "scenario", "signal"})
             and plan.get("purpose") == "DEMO_TECHNICAL_TEST"
             and plan.get("scenario") == "smoke"
             and ("authorization_id" not in plan or plan["authorization_id"] == authorization_id),
             "RESOLUTION_TECHNICAL_PLAN_INVALID")
    registered = registrations[0]
    # Historical report sanitization removed keys containing "authorization".
    # Only that exact projection is compatible: the original registration must
    # retain the fixed authorization and the complete four-field fingerprint.
    # Never mutate the FAIL, accept a reduced fingerprint or infer authority
    # from a report that did not retain its immutable registration.
    registered_plan = {**plan, "authorization_id": authorization_id}
    _require(all(registered[key] == registered_plan[key]
                 for key in ("purpose", "scenario", "authorization_id"))
             and registered["plan_fingerprint"] == _sha(canonical_json(registered_plan))
             and type(registered["signal_json"]) is str
             and canonical_json(_json(registered["signal_json"].encode())) == canonical_json(plan["signal"])
             and authorization_time <= _integer(registered["registered_at_ms"]) <= _integer(old.get("flat_observed_at_ms")),
             "RESOLUTION_TECHNICAL_PLAN_INVALID")
    signal = plan["signal"]
    _require(signal.get("instId") == intent["inst_id"] and signal.get("side") == intent["side"]
             and signal.get("kind") == intent["kind"]
             and canonical_json(signal.get("generated_at_ms")) == canonical_json(intent["created_at_ms"])
             and canonical_json(signal.get("expires_at_ms")) == canonical_json(intent["expires_at_ms"])
             and all(_number(signal.get(key), positive=True) == _number(intent[key], positive=True)
                     for key in ("entry", "stop", "tp1", "tp2", "qty"))
             and ("require_maker" not in signal or signal["require_maker"] is intent["require_maker"]),
             "RESOLUTION_TECHNICAL_PLAN_INVALID")
    _require(current.get("exit_reconciled") is True and current.get("exit_reconcile_code") == "DEMO_HALTED_EXIT_VERIFIED",
             "RESOLUTION_STORED_PROOF_NOT_VERIFIED")
    proof = _json(observed[proof_path])
    _raw_closure(proof, intent, old)
    _require(proof["account_hash"] == anchors.account_hash, "RESOLUTION_ACCOUNT_MISMATCH")
    started, completed, elapsed = (_integer(current.get(key), zero=key.endswith("ns")) for key in (
        "exit_reconcile_collection_started_at_ms", "exit_reconcile_collection_completed_at_ms", "exit_reconcile_collection_elapsed_monotonic_ns"))
    _require(0 <= completed-started <= 5000 and 0 <= elapsed <= 5_000_000_000
             and 0 <= completed-proof["observed_at_ms"] <= 5000, "RESOLUTION_COLLECTION_STALE")
    updates = {key: proof[key] for key in ("position_id", "position_opened_at_ms", "position_closed_at_ms", "exit_order_ids", "confirmed_closed_qty")}
    updates.update(exit_reconciled=True, protected_qty="0", exit_reconcile_code="DEMO_HALTED_EXIT_VERIFIED",
        exit_reconciled_observed_at_ms=proof["observed_at_ms"], exit_reconcile_sha256=anchors.proof_sha256,
        exit_reconcile_proof_path=str(PureWindowsPath(root).joinpath(*proof_path.split("/"))),
        exit_reconcile_collection_started_at_ms=started, exit_reconcile_collection_completed_at_ms=completed,
        exit_reconcile_collection_elapsed_monotonic_ns=elapsed)
    _require(canonical_json(current) == canonical_json({**old, **updates}), "RESOLUTION_INCIDENT_DRIFT")
    return ResolutionPlan(SIGNAL, anchors.account_hash, fingerprint, anchors.authorization_sha256, anchors.original_fail_sha256,
        anchors.original_inventory_sha256, anchors.observed_inventory_sha256, anchors.proof_sha256,
        proof["position_id"], proof["position_opened_at_ms"], proof["position_closed_at_ms"], tuple(proof["exit_order_ids"]),
        proof["confirmed_closed_qty"], old["status"], old["code"], started, completed, elapsed,
        tuple((name, _sha(raw)) for name, raw in sorted(original.items())),
        tuple((name, _sha(raw)) for name, raw in sorted(observed.items())))


def validate_archived_halted_resolution(original_files: tuple[EvidenceFile, ...],
                                       observed_files: tuple[EvidenceFile, ...],
                                       anchors: ResolutionAnchors) -> ResolutionPlan:
    """Return an immutable, nonactivatable consistency plan or a fixed refusal.

    Callers supply bytes and separately obtained anchors. No filesystem reader,
    journal updater, successor builder or activation consumer is implemented.
    A diagnostic snapshot in the archive is only a hash-preserved JSON file.
    Archived age is not compared with the current clock and grants no authority.
    """
    try:
        with localcontext() as context:
            context.prec = 200
            return _validate(original_files, observed_files, anchors)
    except ResolutionRefused:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, ArithmeticError, RecursionError):
        raise ResolutionRefused("RESOLUTION_EVIDENCE_INVALID") from None
