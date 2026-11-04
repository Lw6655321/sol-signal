"""Pure archived-cache continuity, never a DEMO admission or activation gate.

The old cache is a preserved accounting checkpoint, not a replayable account
snapshot. Its original exposure waves were not stored by verified-risk-v2.
This validator therefore NEVER claims to reverify old snapshot admission. It
checks exact history prefixes against a complete, internally linked new ledger,
UTC baselines and anchor cash conservation. Optional new raw account views can
add archived-wave consistency; they cannot authenticate an exchange offline.
No files, credentials, project execution objects or network clients are used.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re

from .demo_ledger import validate_demo_ledger


DAY_MS = 86_400_000
INCIDENT = "DEMO-TEST-SMOKE-1792501260297"
FILL_SCOPES = {"SWAP", "FUTURES", "OPTION", "SPOT", "MARGIN", "EVENTS"}
POSITION_SCOPES = {"SWAP", "FUTURES", "OPTION", "MARGIN"}
ALGO_SCOPES = {"conditional", "oco", "trigger", "move_order_stop", "iceberg", "twap", "smart_iceberg", "chase"}
NA_FIELDS = ("liab", "crossLiab", "isoLiab", "interest", "borrowFroz")
BASELINE = "complete-ledger-flat-boundary-reconstruction"
MAX_BYTES = 8 * 1024 * 1024
CACHE_FIELDS = {
    "schema", "environment", "account_hash", "authorization_digest", "anchor",
    "complete_through_ms", "coverage_start_ms", "collection_started_ms",
    "anchor_lookback_start_ms", "ledger_evidence_digest", "evidence", "day_start_ms",
    "week_start_ms", "day_start_equity", "week_start_equity", "baseline_source",
    "account_observed_at_ms", "balance_observations", "fill_product_scopes",
    "position_product_scopes", "algo_scopes", "bills_currency_filter", "source_coverage",
    "account_mode_applicability", "equity_usdt_observed", "account_mode"}
MUTABLE_INCIDENT_FIELDS = {
    "status", "code", "filled_qty", "protected_qty", "avg_price", "order_id", "entry_state",
    "protection_order_id", "protection_requested_qty", "cancel_requested", "close_id",
    "close_order_id", "close_qty", "confirmed_closed_qty", "submit_attempted", "emergency_status",
    "close_order_ids", "entry_admission", "position_id", "position_opened_at_ms",
    "position_closed_at_ms", "exit_order_ids", "exit_reconciled", "exit_reconcile_code",
    "exit_reconciled_observed_at_ms", "exit_reconcile_sha256", "exit_reconcile_proof_path",
    "exit_reconcile_collection_started_at_ms", "exit_reconcile_collection_completed_at_ms",
    "exit_reconcile_collection_elapsed_monotonic_ns"}


class CacheContinuityRefused(ValueError):
    """Fixed codes only; never include input, UID or parser/ledger exceptions."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(ok, code):
    if not ok:
        raise CacheContinuityRefused(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _types(value, depth=0):
    _require(depth <= 64, "CACHE_JSON_INVALID")
    if type(value) is dict:
        _require(all(type(key) is str for key in value), "CACHE_JSON_INVALID")
        for item in value.values():
            _types(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _types(item, depth + 1)
    else:
        _require(type(value) in (str, int, float, bool, type(None)), "CACHE_JSON_INVALID")
        _require(type(value) is not float or math.isfinite(value), "CACHE_JSON_INVALID")


def _canonical(value):
    _types(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _pairs(rows):
    result = {}
    for key, value in rows:
        _require(key not in result, "CACHE_JSON_DUPLICATE")
        result[key] = value
    return result


def _json(raw):
    _require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, "CACHE_BYTES_INVALID")
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(CacheContinuityRefused("CACHE_JSON_INVALID")))
    _canonical(value)
    return value


def _int(value):
    _require(type(value) is int and 0 < value <= 253402300799999, "CACHE_TIME_INVALID")
    return value


def _stamp(value):
    _require(type(value) is str and value.isascii() and value.isdigit() and len(value) <= 15,
             "CACHE_TIME_INVALID")
    return _int(int(value))


def _number(value):
    _require(type(value) is str and len(value) <= 96 and
             re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?(?:[Ee][+-]?[0-9]{1,2})?", value) is not None,
             "CACHE_NUMBER_INVALID")
    number = Decimal(value)
    _require(number.is_finite() and len(number.as_tuple().digits) <= 40 and
             -40 <= number.as_tuple().exponent <= 40, "CACHE_NUMBER_INVALID")
    return number


def _periods(now):
    day = datetime.fromtimestamp(now // 1000, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    week = day - timedelta(days=day.weekday())
    return int(day.timestamp()) * 1000, int(week.timestamp()) * 1000


def _scope(value, wanted):
    return type(value) is list and len(value) == len(wanted) and all(type(x) is str for x in value) and set(value) == wanted


def _index(rows, keys):
    _require(type(rows) is list and len(rows) <= 50000, "CACHE_HISTORY_INVALID")
    result = {}
    for row in rows:
        _require(type(row) is dict, "CACHE_HISTORY_INVALID")
        key = tuple(row.get(field) for field in keys)
        _require(all(type(x) is str and 0 < len(x) <= 128 for x in key), "CACHE_HISTORY_INVALID")
        _require(key not in result, "CACHE_HISTORY_DUPLICATE")
        result[key] = row
    return result


def _coverage(cache, account_config, auth, pause_hours):
    _require(type(cache) is dict and set(cache) == CACHE_FIELDS, "CACHE_SCHEMA_INVALID")
    _require(type(cache["schema"]) is int and cache["schema"] == 2 and cache["environment"] == "DEMO",
             "CACHE_SCHEMA_INVALID")
    _require(cache["account_hash"] == auth["account_hash"] and cache["authorization_digest"] == _sha(_canonical(auth)),
             "CACHE_ACCOUNT_BINDING")
    _require(cache["baseline_source"] == BASELINE and cache["account_mode"] == {"acctLv": "2", "posMode": "net_mode"},
             "CACHE_BASELINE_INVALID")
    _require(_scope(cache["fill_product_scopes"], FILL_SCOPES) and
             _scope(cache["position_product_scopes"], POSITION_SCOPES) and
             _scope(cache["algo_scopes"], ALGO_SCOPES) and cache["bills_currency_filter"] is None,
             "CACHE_SCOPE_INCOMPLETE")
    through, began, observed = (_int(cache[key]) for key in
                               ("complete_through_ms", "collection_started_ms", "account_observed_at_ms"))
    day, week = _periods(through)
    _require(type(cache["day_start_ms"]) is int and type(cache["week_start_ms"]) is int and
             (cache["day_start_ms"], cache["week_start_ms"]) == (day, week), "CACHE_BOUNDARY_INVALID")
    _require(0 <= through - began <= 10000 and began - 2000 <= observed <= through + 2000,
             "CACHE_COLLECTION_INVALID")
    anchor = cache["anchor"]
    _require(type(anchor) is dict, "CACHE_ANCHOR_INVALID")
    closed = _stamp(anchor.get("uTime"))
    lower, lookback = _int(cache["coverage_start_ms"]), _int(cache["anchor_lookback_start_ms"])
    _require(lower == closed + 1 and lookback <= closed - pause_hours * 3_600_000 and
             lower <= min(day, week) and through - lookback <= 28 * DAY_MS, "CACHE_ANCHOR_INVALID")
    source = cache["source_coverage"]
    _require(type(source) is dict and set(source) == {"fills", "bills", "positions"}, "CACHE_COVERAGE_INVALID")
    expected, windows = lower, []
    _require(type(source["bills"]) is list and 1 <= len(source["bills"]) <= 100, "CACHE_COVERAGE_INVALID")
    for row in source["bills"]:
        _require(type(row) is dict and set(row) == {"start_ms", "end_ms", "currency_filter"}, "CACHE_COVERAGE_INVALID")
        start, end = _int(row["start_ms"]), _int(row["end_ms"])
        _require(start == expected and start <= end and row["currency_filter"] is None, "CACHE_COVERAGE_GAP")
        windows.append((start, end))
        expected = end + 1
    _require(expected == through + 1, "CACHE_COVERAGE_GAP")
    _require(type(source["fills"]) is list and len(source["fills"]) == len(windows), "CACHE_COVERAGE_INVALID")
    fill_windows = []
    for row in source["fills"]:
        _require(type(row) is dict and set(row) == {"start_ms", "end_ms", "products"} and
                 _scope(row["products"], FILL_SCOPES), "CACHE_SCOPE_INCOMPLETE")
        fill_windows.append((_int(row["start_ms"]), _int(row["end_ms"])))
    _require(fill_windows == windows, "CACHE_COVERAGE_GAP")
    _require(type(source["positions"]) is list and 1 <= len(source["positions"]) <= 102, "CACHE_COVERAGE_INVALID")
    positions = []
    for row in source["positions"]:
        _require(type(row) is dict and set(row) == {"start_ms", "end_ms", "products"} and
                 _scope(row["products"], POSITION_SCOPES), "CACHE_SCOPE_INCOMPLETE")
        start, end = _int(row["start_ms"]), _int(row["end_ms"])
        _require(start <= end and (end == windows[0][1] and start <= closed or (start, end) in windows[1:]),
                 "CACHE_COVERAGE_INVALID")
        positions.append((start, end))
    prefix = any(start <= lookback and end == windows[0][1] for start, end in positions)
    histories_complete = prefix and set(windows[1:]) <= set(positions)
    _require(histories_complete and len(set(positions)) == len(positions), "CACHE_COVERAGE_GAP")
    timings = cache["balance_observations"]
    _require(type(timings) is list and len(timings) == 2, "CACHE_BALANCE_CLOCK_INVALID")
    stages, timing_bounds = [], []
    for row in timings:
        _require(type(row) is dict and set(row) == {"path", "stage", "started_at_ms", "finished_at_ms", "scope"} and
                 row["path"] == "/api/v5/account/balance" and row["scope"] == {}, "CACHE_BALANCE_CLOCK_INVALID")
        start, end = _int(row["started_at_ms"]), _int(row["finished_at_ms"])
        _require(began - 2000 <= start <= end <= through + 2000, "CACHE_BALANCE_CLOCK_INVALID")
        stages.append(row["stage"])
        timing_bounds.append((start, end))
    _require(stages == ["account-before", "account-after"] and timing_bounds[0][1] <= timing_bounds[1][0] and
             observed <= timing_bounds[1][0], "CACHE_BALANCE_CLOCK_INVALID")
    evidence = cache["evidence"]
    _require(type(evidence) is dict and set(evidence) == {"fills", "bills", "histories", "routes", "order_details"} and
             _sha(_canonical(evidence)) == cache["ledger_evidence_digest"], "CACHE_EVIDENCE_DIGEST_INVALID")
    for name, keys in (("fills", ("instId", "tradeId")), ("bills", ("billId",)),
                       ("histories", ("posId", "cTime", "uTime")), ("routes", ("signal_id",))):
        _index(evidence[name], keys)
    _require(type(evidence["order_details"]) is dict and all(type(k) is str and type(v) is dict and v.get("ordId") == k
             for k, v in evidence["order_details"].items()), "CACHE_HISTORY_INVALID")
    for name in ("day_start_equity", "week_start_equity", "equity_usdt_observed"):
        _require(_number(cache[name]) > 0, "CACHE_BASELINE_INVALID")
    applicability = cache["account_mode_applicability"]
    _require(type(applicability) is list and len(applicability) == 2, "CACHE_ACCOUNT_WAVES_INVALID")
    for row in applicability:
        _require(type(row) is dict and set(row) == {"raw_fingerprint", "not_applicable_fields"} and
                 type(row["raw_fingerprint"]) is str and re.fullmatch(r"[0-9a-f]{64}", row["raw_fingerprint"]) is not None and
                 type(row["not_applicable_fields"]) is list, "CACHE_ACCOUNT_WAVES_INVALID")
        for na in row["not_applicable_fields"]:
            _require(type(na) is dict and set(na) == {"ccy", "field", "raw_value", "account_level", "meaning"} and
                     type(na["ccy"]) is str and na["field"] in NA_FIELDS and na["raw_value"] == "" and
                     na["account_level"] == "2" and na["meaning"] == "official-not-applicable", "CACHE_ACCOUNT_WAVES_INVALID")
    # These are independently derived *archived coverage consistency* facts,
    # not provenance/authenticity or a claim about missing old account waves.
    return dict(complete=histories_complete, all_account_products=histories_complete,
                all_account_currencies=all(row["currency_filter"] is None for row in source["bills"]),
                fills_complete=fill_windows == windows, bills_complete=expected == through + 1,
                positions_history_complete=histories_complete, owned_routes_complete=True,
                environment="DEMO", account_hash=auth["account_hash"], account_config=account_config,
                start_ms=lower, end_ms=through, authorization_at_ms=auth["authorization_recorded_at_ms"],
                anchor_lookback_complete=prefix, anchor_lookback_start_ms=lookback,
                loss_anchor=dict(source="completed-position-reset", environment="DEMO", account_hash=auth["account_hash"], record=anchor))


def _views(cache, views, config):
    _require(type(views) is list and len(views) == 2, "CACHE_ACCOUNT_WAVES_INVALID")
    fingerprints = []
    for view, applicability in zip(views, cache["account_mode_applicability"]):
        _require(type(view) is dict and set(view) == {"config", "balance", "positions", "pending", "algos", "leverage", "spec"} and
                 type(view["config"]) is dict and all(view["config"].get(k) == config[k] for k in ("uid", "acctLv", "posMode")),
                 "CACHE_ACCOUNT_WAVES_INVALID")
        _require(type(view["positions"]) is list and all(type(p) is dict and _number(p.get("pos")) == 0 for p in view["positions"]) and
                 type(view["pending"]) is list and not view["pending"] and type(view["algos"]) is dict and
                 set(view["algos"]) == ALGO_SCOPES and all(type(v) is list and not v for v in view["algos"].values()),
                 "CACHE_ACCOUNT_NOT_FLAT")
        balance = view["balance"]
        _require(type(balance) is dict and type(balance.get("details")) is list and
                 all(type(row) is dict for row in balance["details"]), "CACHE_ACCOUNT_WAVES_INVALID")
        wallets, normalized, na = [], [], []
        for row in balance["details"]:
            _require(type(row.get("ccy")) is str and _number(row.get("eq")) >= 0, "CACHE_ACCOUNT_WAVES_INVALID")
            wallet = {key: row.get(key) for key in ("ccy", "eq", *NA_FIELDS)}
            adjusted = dict(wallet)
            for key in NA_FIELDS:
                _require(key in row, "CACHE_ACCOUNT_WAVES_INVALID")
                if row[key] == "":
                    adjusted[key] = "0"
                    na.append(dict(ccy=row["ccy"], field=key, raw_value="", account_level="2", meaning="official-not-applicable"))
                else:
                    _require(_number(row[key]) == 0, "CACHE_ACCOUNT_WAVES_INVALID")
            wallets.append(wallet)
            normalized.append(adjusted)
        usdt = [row for row in wallets if row["ccy"] == "USDT"]
        _require(len(usdt) == 1 and _number(usdt[0]["eq"]) == _number(cache["equity_usdt_observed"]) and
                 len({row["ccy"] for row in wallets}) == len(wallets) and _number(balance.get("totalEq")) > 0,
                 "CACHE_ACCOUNT_BALANCE_MISMATCH")
        _require(_stamp(balance.get("uTime")) <= cache["complete_through_ms"] + 2000 and
                 type(view["leverage"]) is dict and type(view["spec"]) is dict, "CACHE_ACCOUNT_WAVES_INVALID")
        stable = dict(config={key: view["config"].get(key) for key in ("uid", "posMode", "acctLv")},
                      wallets=sorted(wallets, key=lambda row: row["ccy"]), positions=view["positions"], pending=view["pending"],
                      algos=view["algos"], leverage=view["leverage"],
                      spec={key: view["spec"].get(key) for key in ("instId", "state", "ctType", "settleCcy", "ctValCcy", "ctVal", "lotSz", "minSz", "tickSz")})
        _require(_sha(_canonical(stable)) == applicability["raw_fingerprint"] and
                 _canonical(na) == _canonical(applicability["not_applicable_fields"]), "CACHE_ACCOUNT_FINGERPRINT_MISMATCH")
        stable["wallets"] = sorted(normalized, key=lambda row: row["ccy"])
        fingerprints.append(_sha(_canonical(dict(compatibility=_canonical(stable).decode(), raw_fingerprint=applicability["raw_fingerprint"]))))
    _require(fingerprints[0] == fingerprints[1], "CACHE_ACCOUNT_CHANGED")


def _unsent(route):
    _require(route.get("status") == "RESERVED" and route.get("submit_attempted") is False and
             route.get("inst_id") == "SOL-USDT-SWAP" and route.get("side") in {"long", "short"}, "CACHE_ROUTE_TRANSITION_INVALID")
    for key in ("filled_qty", "protected_qty", "confirmed_closed_qty", "close_qty", "avg_price"):
        _require(_number(route.get(key)) == 0, "CACHE_ROUTE_TRANSITION_INVALID")
    for key in ("order_id", "entry_state", "protection_order_id", "close_order_id", "close_id", "protection_requested_qty", "emergency_status"):
        _require(route.get(key) == "", "CACHE_ROUTE_TRANSITION_INVALID")
    _require(type(route.get("client_order_id")) is str and re.fullmatch(r"[A-Za-z0-9]{1,32}",route["client_order_id"]) is not None and
             route.get("close_order_ids") == [] and route.get("cancel_requested") is False and route.get("position_id", "") == "" and
             type(route.get("position_opened_at_ms", 0)) is int and route.get("position_opened_at_ms", 0) == 0,
             "CACHE_ROUTE_TRANSITION_INVALID")


def _prefix_and_routes(old, new):
    before, after, through = old["evidence"], new["evidence"], old["complete_through_ms"]
    for name, keys, clock in (("fills", ("instId", "tradeId"), "fillTime"), ("bills", ("billId",), "ts"),
                              ("histories", ("posId", "cTime", "uTime"), "uTime")):
        previous = _index(before[name], keys)
        prefix = _index([row for row in after[name] if _stamp(row.get(clock)) <= through], keys)
        _require(set(previous) == set(prefix) and all(_canonical(row) == _canonical(prefix[key]) for key,row in previous.items()),
                 "CACHE_HISTORY_PREFIX_CHANGED")
    old_details = before["order_details"]
    _require(all(key in after["order_details"] and _canonical(row) == _canonical(after["order_details"][key]) for key, row in old_details.items()),
             "CACHE_HISTORY_PREFIX_CHANGED")
    _require(set(old_details) == {row["ordId"] for row in before["fills"]}, "CACHE_HISTORY_INVALID")
    # A receipt that only appears in a later bill window cannot retroactively
    # complete the old checkpoint. The full new ledger verifies all link values;
    # this additional prefix bijection checks their old-window presence.
    old_trade_bills = {row["billId"] for row in before["bills"] if row.get("type") == "2"}
    _require(old_trade_bills == {row["billId"] for row in before["fills"]}, "CACHE_OLD_TRADE_BILL_INCOMPLETE")
    for row in before["fills"]:
        if "ts" in row:
            recorded = _stamp(row["ts"]) if type(row["ts"]) is str else _int(row["ts"])
            _require(recorded <= through,"CACHE_OLD_RECEIPT_NOT_RECORDED")
    old_routes, new_routes = (_index(value["routes"], ("signal_id",)) for value in (before, after))
    _require(set(old_routes) == set(new_routes), "CACHE_ROUTE_HISTORY_CHANGED")
    transitioned = False
    for key, route in old_routes.items():
        later = new_routes[key]
        if _canonical(route) == _canonical(later):
            _require(route.get("status") in {"CANCELED", "REJECTED", "CLOSED"} or key == (INCIDENT,) and route.get("status") == "HALTED",
                     "CACHE_ROUTE_TRANSITION_INVALID")
            continue
        _require(key == (INCIDENT,), "CACHE_ROUTE_HISTORY_CHANGED")
        _unsent(route)
        _require(later.get("status") in {"HALTED", "CLOSED"} and later.get("submit_attempted") is True and
                 _number(later.get("filled_qty")) > 0 and _int(later.get("position_opened_at_ms")) > through,
                 "CACHE_ROUTE_TRANSITION_INVALID")
        immutable_old = {k: v for k, v in route.items() if k not in MUTABLE_INCIDENT_FIELDS}
        immutable_new = {k: v for k, v in later.items() if k not in MUTABLE_INCIDENT_FIELDS}
        _require(_canonical(immutable_old) == _canonical(immutable_new), "CACHE_ROUTE_TRANSITION_INVALID")
        transitioned = True
    return transitioned


def _period_pnl(evidence, boundary):
    # The original ledger assigns trade cash to execution time, not the bill's
    # potentially later recording time. Funding uses its own bill timestamp.
    trades = sum((_number(row["fillPnl"]) + _number(row["fee"]) for row in evidence["fills"]
                  if _stamp(row["fillTime"]) >= boundary), Decimal(0))
    funding = sum((_number(row["balChg"]) for row in evidence["bills"]
                   if row.get("type") == "8" and _stamp(row["ts"]) >= boundary), Decimal(0))
    return trades + funding


@dataclass(frozen=True, slots=True)
class CacheContinuity:
    old_sha256: str
    new_sha256: str
    authorization_sha256: str
    account_config_sha256: str = field(repr=False)
    risk_config_sha256: str
    account_hash: str = field(repr=False)
    old_through_ms: int
    new_through_ms: int
    old_day_start_ms: int
    new_day_start_ms: int
    old_week_start_ms: int
    new_week_start_ms: int
    anchor_equity: str = field(repr=False)
    incident_reservation_progressed: bool
    new_account_waves_consistent: bool
    validation_scope: str = field(default="ARCHIVED_CACHE_CONTINUITY_ONLY", init=False)
    old_snapshot_admission_reverified: bool = field(default=False, init=False)
    new_snapshot_admission_reverified: bool = field(default=False, init=False)
    exchange_origin_authenticated: bool = field(default=False, init=False)
    activation_allowed: bool = field(default=False, init=False)
    successor_ready: bool = field(default=False, init=False)


def _validate(old_raw, new_raw, authorization_raw, account_config_raw, risk_config_raw, new_account_views_raw):
    old, new, auth, cfg, policy = map(_json, (old_raw, new_raw, authorization_raw, account_config_raw, risk_config_raw))
    _require(type(auth) is dict and {"environment", "account_hash", "authorization_recorded_at_ms", "source", "scope"} <= set(auth) <=
             {"environment", "account_hash", "authorization_recorded_at_ms", "source", "scope", "service_started_at_ms"}, "CACHE_AUTHORIZATION_INVALID")
    _require(auth["environment"] == "DEMO" and auth["source"] == "user-approved-demo-activation" and
             auth["scope"] == "SOL-USDT-SWAP_EXISTING_RISK", "CACHE_AUTHORIZATION_INVALID")
    stamp = _int(auth["authorization_recorded_at_ms"])
    if "service_started_at_ms" in auth:
        _require(stamp <= _int(auth["service_started_at_ms"]) <= _int(old.get("collection_started_ms")), "CACHE_AUTHORIZATION_INVALID")
    auth = {key: auth[key] for key in ("environment", "account_hash", "authorization_recorded_at_ms", "source", "scope")}
    _require(type(cfg) is dict and type(cfg.get("uid")) is str and re.fullmatch(r"[0-9]{1,32}", cfg["uid"]) is not None and
             cfg.get("acctLv") == "2" and cfg.get("posMode") == "net_mode" and
             _sha(("DEMO\0" + cfg["uid"]).encode()) == auth["account_hash"], "CACHE_ACCOUNT_BINDING")
    _require(type(policy) is dict, "CACHE_POLICY_INVALID")
    count, hours = (policy.get(key) for key in ("consecutive_loss_pause_count", "consecutive_loss_pause_hours"))
    _require(type(count) is int and 1 <= count <= 100 and type(hours) is int and 1 <= hours <= 8760, "CACHE_POLICY_INVALID")
    old_coverage, new_coverage = (_coverage(value, cfg, auth, hours) for value in (old, new))
    _require(old["complete_through_ms"] < new["complete_through_ms"] and stamp <= old["collection_started_ms"], "CACHE_TIME_INVALID")
    for key in ("anchor", "coverage_start_ms", "anchor_lookback_start_ms", "baseline_source", "account_hash", "authorization_digest"):
        _require(_canonical(old[key]) == _canonical(new[key]), "CACHE_ANCHOR_CHANGED")
    changed = _prefix_and_routes(old, new)
    # No missing old account waves are invented. The new raw ledger checks all
    # filled ownership and exact receipts; the old prefix is compared in full.
    data = new["evidence"]
    _require(all(row.get("status") in {"CANCELED", "REJECTED", "CLOSED"} or row.get("signal_id") == INCIDENT and row.get("status") == "HALTED"
                 for row in data["routes"]), "CACHE_UNRESOLVED_ROUTE")
    result = validate_demo_ledger(fills=data["fills"], bills=data["bills"], positions_history=data["histories"], routes=data["routes"],
              order_details=data["order_details"], now_ms=new["complete_through_ms"], day_start_ms=new["day_start_ms"],
              week_start_ms=new["week_start_ms"], coverage=new_coverage, consecutive_loss_limit=count, pause_hours=hours)
    _require(result.get("verified") is True, "CACHE_NEW_LEDGER_INCONSISTENT")
    for position in result["completed_positions"]:
        entry_times = [_stamp(row["fillTime"]) for row in data["fills"] if row["ordId"] == position["entry_order_id"]]
        _require(not min(entry_times) <= old["complete_through_ms"] < position["closed_at_ms"], "CACHE_OLD_CHECKPOINT_NOT_FLAT")
    for cache in (old, new):
        evidence = cache["evidence"]
        for boundary, label in ((cache["day_start_ms"], "day"), (cache["week_start_ms"], "week")):
            # No partially open lifecycle can be used to reconstruct cash at a
            # period boundary, even when its cash sums happen to match.
            _require(not any(_stamp(row.get("cTime")) < boundary <= _stamp(row.get("uTime")) for row in evidence["histories"]),
                     "CACHE_BOUNDARY_NOT_FLAT")
            pnl = _period_pnl(evidence,boundary)
            _require(_number(cache[label + "_start_equity"]) == _number(cache["equity_usdt_observed"]) - pnl,
                     "CACHE_BASELINE_RECONSTRUCTION_CHANGED")
    for label in ("day", "week"):
        if old[label + "_start_ms"] == new[label + "_start_ms"]:
            _require(_number(old[label + "_start_equity"]) == _number(new[label + "_start_equity"]), "CACHE_SAME_PERIOD_BASELINE_CHANGED")
    equities = [_number(value["equity_usdt_observed"]) - sum((_number(row["balChg"]) for row in value["evidence"]["bills"]), Decimal(0))
                for value in (old, new)]
    _require(equities[0] == equities[1] and equities[0] > 0, "CACHE_ANCHOR_CASH_CHANGED")
    views_consistent = new_account_views_raw is not None
    if views_consistent:
        _views(new, _json(new_account_views_raw), cfg)
    return CacheContinuity(_sha(old_raw), _sha(new_raw), _sha(authorization_raw), _sha(account_config_raw), _sha(risk_config_raw),
        auth["account_hash"], old["complete_through_ms"], new["complete_through_ms"], old["day_start_ms"], new["day_start_ms"],
        old["week_start_ms"], new["week_start_ms"], str(equities[0]), changed, views_consistent)


def validate_demo_cache_continuity(old_raw: bytes, new_raw: bytes, authorization_raw: bytes,
                                  account_config_raw: bytes, risk_config_raw: bytes, *,
                                  new_account_views_raw: bytes | None = None) -> CacheContinuity:
    """Return frozen archived consistency only; never replace fresh admission.

    Full original inventories must be retained by the caller. Both cache bytes
    remain untouched. Source coverage declarations are structurally checked,
    not authenticated offline. An absent new raw-wave archive is explicit in
    the result, and old account-snapshot admission is never claimed reverified.
    Account UID is accepted in memory only and excluded from results/errors.
    """
    try:
        with localcontext() as context:
            context.prec = 200
            return _validate(old_raw, new_raw, authorization_raw, account_config_raw, risk_config_raw, new_account_views_raw)
    except CacheContinuityRefused:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, ArithmeticError, RecursionError, UnicodeError):
        raise CacheContinuityRefused("CACHE_EVIDENCE_INVALID") from None


# Short public spelling used by the separate successor projection consumer.
validate_cache_continuity = validate_demo_cache_continuity
