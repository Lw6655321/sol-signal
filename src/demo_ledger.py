"""Pure conservative ledger validation; no credentials, IO or HTTP.

This is an evidence contract, NOT an OKX collector or a release switch. Raw API
responses do not necessarily expose every link below. A collector must refuse
missing links, never manufacture ``algoId``, ``posId`` or a position observation.

Required route evidence beyond today's router rows:
``account_hash`` (DEMO UID binding), ``side``, ``position_id``,
``position_opened_at_ms`` (persisted exchange observation), ``filled_qty``,
``order_id`` (entry), ``protection_order_id`` and/or ``close_order_ids`` (exchange
order IDs). History matches the complete (posId, cTime) lifecycle, not posId alone.
An OCO child fill needs its authenticated raw order detail's algoId to match the
owned parent (fills-history itself need not expose algoId). Funding can join the
unique fully proved net-mode SOL lifecycle; a raw posId, when supplied, adds a
constraint. Missing ownership evidence is an expected refusal.

Coverage/anchor provenance must be checked by the collector. The pure validator
checks consistency but cannot authenticate caller-supplied evidence. In
particular a fresh flat account or an empty current-week window is NOT a prior
verified ledger anchor. Nothing in this module creates such an anchor.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Mapping


INST = "SOL-USDT-SWAP"
ZERO = Decimal("0")
DAY_MS = 86_400_000
PNL_QUANTUM = Decimal("0.00000001")
COMPLETE_FLAGS = ("complete", "all_account_products", "all_account_currencies",
                  "fills_complete", "bills_complete", "positions_history_complete",
                  "owned_routes_complete")


class LedgerRefused(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _refuse(code: str):
    raise LedgerRefused(code)


def _number(value, code="LEDGER_NUMBER_INVALID", *, positive=False):
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        result = Decimal(str(value))
        if not result.is_finite() or (positive and result <= ZERO):
            raise ValueError
        return result
    except (InvalidOperation, ValueError, TypeError):
        _refuse(code)


def _integer(value, code="LEDGER_TIME_INVALID"):
    result = _number(value, code)
    if result < ZERO or result != result.to_integral_value():
        _refuse(code)
    return int(result)


def _fill_record_time(fill, stamp, now):
    # fillTime is execution time; ts is when this individual trade was recorded.
    # Missing legacy ts proves only the original exact-time relationship.
    if "ts" not in fill:
        return stamp
    raw = fill["ts"]
    if not (type(raw) is int or
            (isinstance(raw, str) and raw and raw.isascii() and raw.isdigit())):
        _refuse("LEDGER_FILL_RECORD_TIME_INVALID")
    recorded = _integer(raw, "LEDGER_FILL_RECORD_TIME_INVALID")
    if not stamp <= recorded <= min(stamp + 5_000, now):
        _refuse("LEDGER_FILL_RECORD_TIME_INVALID")
    return recorded


def _text(value, code="LEDGER_ID_INVALID"):
    if not isinstance(value, str) or not value or len(value) > 128 or not value.isascii():
        _refuse(code)
    if not all(c.isalnum() or c in "_-" for c in value):
        _refuse(code)
    return value


def _rows(value):
    if not isinstance(value, (tuple, list)) or any(not isinstance(row, Mapping) for row in value):
        _refuse("LEDGER_ROWS_INVALID")
    return list(value)


def _deduplicate(rows, key_fields, code):
    result = {}
    for row in _rows(rows):
        key = tuple(_text(str(row[field])) if field in row and row[field] is not None
                    else _text(None) for field in key_fields)
        if key in result and result[key] != row:
            _refuse(code)
        result[key] = dict(row)
    return list(result.values())


def _periods(now):
    try:
        day = datetime.fromtimestamp(now / 1000, timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        week = day - timedelta(days=day.weekday())
        return int(day.timestamp() * 1000), int(week.timestamp() * 1000)
    except (ValueError, OverflowError, OSError):
        _refuse("LEDGER_TIME_INVALID")


def _coverage(coverage, now, day, week, count_limit, pause_ms):
    if not isinstance(coverage, Mapping) or any(coverage.get(key) is not True for key in COMPLETE_FLAGS):
        _refuse("LEDGER_HISTORY_INCOMPLETE")
    if coverage.get("environment") != "DEMO":
        _refuse("LEDGER_ENVIRONMENT")
    account = _text(coverage.get("account_hash"), "LEDGER_ACCOUNT_BINDING")
    if len(account) != 64 or any(c not in "0123456789abcdef" for c in account):
        _refuse("LEDGER_ACCOUNT_BINDING")
    cfg = coverage.get("account_config")
    if (not isinstance(cfg, Mapping) or cfg.get("acctLv") != "2"
            or cfg.get("posMode") != "net_mode"):
        _refuse("LEDGER_ACCOUNT_SCOPE")
    uid = _text(cfg.get("uid"), "LEDGER_ACCOUNT_BINDING")
    if hashlib.sha256(("DEMO\0"+uid).encode()).hexdigest() != account:
        _refuse("LEDGER_ACCOUNT_BINDING")
    start = _integer(coverage.get("start_ms"))
    end = _integer(coverage.get("end_ms"))
    if end != now or start > min(day, week) or start > end:
        _refuse("LEDGER_COVERAGE_WINDOW")
    anchor = coverage.get("loss_anchor")
    if (not isinstance(anchor, Mapping)
            or anchor.get("source") != "completed-position-reset"
            or anchor.get("environment") != "DEMO" or anchor.get("account_hash") != account):
        _refuse("LEDGER_LOSS_ANCHOR_UNVERIFIED")
    record = anchor.get("record")
    if not isinstance(record, Mapping):
        _refuse("LEDGER_LOSS_ANCHOR_UNVERIFIED")
    opened = _integer(record.get("cTime"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    closed = _integer(record.get("uTime"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    authorization = _integer(coverage.get("authorization_at_ms"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    _text(record.get("posId"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    _text(record.get("instId"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    _text(record.get("ccy"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    maximum = _number(record.get("openMaxPos"), "LEDGER_LOSS_ANCHOR_UNVERIFIED", positive=True)
    net = _number(record.get("realizedPnl"), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
    reconciled = sum((_number(record.get(name), "LEDGER_LOSS_ANCHOR_UNVERIFIED")
                      for name in ("pnl", "fee", "fundingFee", "liqPenalty")), ZERO)
    if (not opened <= closed or closed + 1 != start or start > min(day, week)
            or not closed <= authorization <= now
            or _number(record.get("closeTotalPos"), "LEDGER_LOSS_ANCHOR_UNVERIFIED") != maximum
            or min(net, reconciled) < ZERO or abs(net - reconciled) > PNL_QUANTUM):
        _refuse("LEDGER_LOSS_ANCHOR_UNVERIFIED")
    if (coverage.get("anchor_lookback_complete") is not True
            or _integer(coverage.get("anchor_lookback_start_ms")) > closed - pause_ms):
        _refuse("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")
    return account, start, 0, 0, dict(record)


def _position_key(row):
    return _text(row.get("posId")), _integer(row.get("cTime"))


def _unsent_reservation(route, coverage):
    """A journal reservation is not an exchange order or a counted entry.

    Only the collector can supply account_flat_and_empty, after independently
    observing all-account positions, ordinary orders and algorithms before AND
    after collection. The flag does not replace strict inspection of this row.
    Router ownership/idempotency rules still decide who may execute the intent.
    """
    if (coverage.get("account_flat_and_empty") is not True
            or route.get("status") != "RESERVED"
            or route.get("submit_attempted") is not False
            or route.get("inst_id") != INST or route.get("side") not in {"long", "short"}):
        _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    client_id = _text(route.get("client_order_id"), "LEDGER_UNRESOLVED_OWNED_ROUTE")
    if len(client_id) > 32 or not client_id.isalnum():
        _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    for field in ("order_id", "entry_state", "protection_order_id", "close_order_id", "close_id"):
        if field not in route or route[field] != "":
            _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    for field in ("protected_qty", "confirmed_closed_qty"):
        if field not in route or _number(route[field]) != ZERO:
            _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    if (not isinstance(route.get("close_order_ids"), list) or route["close_order_ids"]
            or route.get("position_id", "") != ""):
        _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    if ("position_opened_at_ms" in route
            and _integer(route["position_opened_at_ms"]) != 0):
        _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    # These optional journal fields, if present, must also show no prior action.
    for field in ("protection_requested_qty", "emergency_status"):
        if field in route and route[field] != "":
            _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    if "cancel_requested" in route and route["cancel_requested"] is not False:
        _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
    for field in ("close_qty", "avg_price"):
        if field in route and _number(route[field]) != ZERO:
            _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")


def _validate(*, fills, bills, positions_history, routes, order_details, now_ms, day_start_ms,
              week_start_ms, coverage, consecutive_loss_limit, pause_hours):
    now, day, week = map(_integer, (now_ms, day_start_ms, week_start_ms))
    if _periods(now) != (day, week):
        _refuse("LEDGER_BOUNDARIES_INVALID")
    count_limit = _integer(consecutive_loss_limit, "LEDGER_POLICY_INVALID")
    hours = _integer(pause_hours, "LEDGER_POLICY_INVALID")
    if count_limit <= 0 or hours <= 0:
        _refuse("LEDGER_POLICY_INVALID")
    pause_ms = hours * 3_600_000
    account, start, losses, until, anchor_record = _coverage(coverage, now, day, week, count_limit, pause_ms)
    fill_rows = _deduplicate(fills, ("instId", "tradeId"), "LEDGER_FILL_DUPLICATE_CONFLICT")
    bill_rows = _deduplicate(bills, ("billId",), "LEDGER_BILL_DUPLICATE_CONFLICT")
    histories = _deduplicate(positions_history, ("posId", "cTime", "uTime"),
                             "LEDGER_POSITION_DUPLICATE_CONFLICT")
    if not any(row == anchor_record for row in histories):
        _refuse("LEDGER_LOSS_ANCHOR_RECORD_MISSING")
    histories = [row for row in histories if row != anchor_record]
    after_anchor = []
    anchor_closed = _integer(anchor_record["uTime"])
    lookback_start = _integer(coverage["anchor_lookback_start_ms"])
    for row in histories:
        closed = _integer(row.get("uTime"))
        if closed <= anchor_closed:
            if closed < lookback_start:
                _refuse("LEDGER_POSITION_OUTSIDE_COVERAGE")
            _position_key(row)
            _text(row.get("instId"))
            _text(row.get("ccy"))
            if _integer(row.get("cTime")) > closed:
                _refuse("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")
            maximum = _number(row.get("openMaxPos"), positive=True)
            if _number(row.get("closeTotalPos")) != maximum:
                _refuse("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")
            net = _number(row.get("realizedPnl"))
            total = sum((_number(row.get(field)) for field in
                         ("pnl", "fee", "fundingFee", "liqPenalty")), ZERO)
            if abs(net - total) > PNL_QUANTUM or min(net, total) < ZERO:
                # A win resets the streak but does not erase a previous active
                # cooldown. Without an earlier streak anchor, refuse any recent
                # loss rather than claiming that a cooldown never existed.
                _refuse("LEDGER_ANCHOR_COOLDOWN_UNPROVEN")
            continue
        after_anchor.append(row)
    histories = after_anchor
    if not isinstance(order_details, Mapping) or any(not isinstance(row, Mapping) for row in order_details.values()):
        _refuse("LEDGER_ORDER_DETAIL_INVALID")
    owned = {}
    route_names = set()
    all_order_ids = set()
    reserved_count = 0
    for route in _rows(routes):
        if route.get("account_hash") != account:
            _refuse("LEDGER_ACCOUNT_BINDING")
        name = _text(route.get("signal_id"))
        if name in route_names:
            _refuse("LEDGER_ROUTE_DUPLICATE")
        route_names.add(name)
        quantity = _number(route.get("filled_qty"), positive=False)
        if quantity < ZERO:
            _refuse("LEDGER_ROUTE_INVALID")
        if quantity == ZERO:
            if route.get("status") == "RESERVED":
                _unsent_reservation(route, coverage)
                reserved_count += 1
                if reserved_count > 1:
                    _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
                continue
            if route.get("status") not in {"CANCELED", "REJECTED"}:
                _refuse("LEDGER_UNRESOLVED_OWNED_ROUTE")
            if route.get("order_id"):
                canceled_id = _text(route["order_id"])
                if canceled_id in all_order_ids:
                    _refuse("LEDGER_OWNERSHIP_AMBIGUOUS")
                all_order_ids.add(canceled_id)
            continue
        if route.get("inst_id") != INST or route.get("side") not in {"long", "short"}:
            _refuse("LEDGER_ROUTE_INVALID")
        key = (_text(route.get("position_id")), _integer(route.get("position_opened_at_ms")))
        if key in owned or not start <= key[1] <= now:
            _refuse("LEDGER_POSITION_LINK_UNVERIFIED")
        entry = _text(route.get("order_id"))
        close_ids = route.get("close_order_ids", [])
        if not isinstance(close_ids, (tuple, list)):
            _refuse("LEDGER_ROUTE_INVALID")
        close_ids = {_text(value) for value in close_ids}
        if route.get("close_order_id"):
            close_ids.add(_text(route["close_order_id"]))
        protection = route.get("protection_order_id", "")
        if protection:
            _text(protection)
        if entry in close_ids or any(value in all_order_ids for value in close_ids | {entry}):
            _refuse("LEDGER_OWNERSHIP_AMBIGUOUS")
        all_order_ids.update(close_ids | {entry})
        owned[key] = {"route": route, "entry": entry, "closes": close_ids,
                      "protection": protection, "quantity": quantity, "fills": [], "funding": []}
    history_by_key = {}
    for history in histories:
        key = _position_key(history)
        if key in history_by_key:
            _refuse("LEDGER_POSITION_HISTORY_AMBIGUOUS")
        if key not in owned:
            _refuse("LEDGER_EXTERNAL_POSITION")
        if (history.get("instId") != INST or history.get("instType") != "SWAP"
                or history.get("posSide") != "net" or history.get("mgnMode") != "cross"
                or history.get("ccy") != "USDT"
                or history.get("direction") != owned[key]["route"]["side"]):
            _refuse("LEDGER_POSITION_SCOPE")
        closed = _integer(history.get("uTime"))
        if not start <= key[1] <= closed <= now:
            _refuse("LEDGER_PARTIAL_POSITION_HISTORY")
        quantity = owned[key]["quantity"]
        if (_number(history.get("openMaxPos"), positive=True) != quantity
                or _number(history.get("closeTotalPos"), positive=True) != quantity):
            _refuse("LEDGER_PARTIAL_POSITION_HISTORY")
        # Liquidation/settlement changes require their own complete accounting.
        # Scope above is already the fixed cross/net SOL SWAP. OKX documents
        # settledPnl for cross FUTURES only and returns "" when inapplicable.
        # Normalize this one optional field locally; keep the raw evidence and
        # all required numbers (including liquidation penalty) strict.
        settled_pnl = history.get("settledPnl", "0")
        if type(settled_pnl) is str and settled_pnl == "":
            settled_pnl = "0"
        if (_number(history.get("liqPenalty")) != ZERO
                or _number(settled_pnl) != ZERO):
            _refuse("LEDGER_UNSUPPORTED_POSITION_ADJUSTMENT")
        history_by_key[key] = history
    if set(owned) != set(history_by_key):
        _refuse("LEDGER_POSITION_HISTORY_MISSING")
    timed_fills = []
    for fill in fill_rows:
        stamp = _integer(fill.get("fillTime"))
        if not start <= stamp <= now:
            _refuse("LEDGER_FILL_OUTSIDE_COVERAGE")
        timed_fills.append((fill, stamp, _fill_record_time(fill, stamp, now)))
    entry_boundaries = {}
    for key, position in owned.items():
        candidates = [(stamp, recorded) for fill, stamp, recorded in timed_fills
                      if fill.get("ordId") == position["entry"]]
        if candidates:
            first = min(stamp for stamp, _ in candidates)
            # Tied executions must all support the boundary; another fill's
            # later record time cannot widen the first execution's evidence.
            entry_boundaries[key] = (first, min(recorded for stamp, recorded in candidates
                                                 if stamp == first))
    fill_index = {}
    for fill, stamp, recorded in timed_fills:
        if (fill.get("instId") != INST or fill.get("instType") != "SWAP"
                or fill.get("feeCcy") != "USDT" or fill.get("posSide") != "net"):
            _refuse("LEDGER_FILL_SCOPE")
        order = _text(fill.get("ordId"))
        trade = _text(fill.get("tradeId"))
        _text(fill.get("billId"))
        size = _number(fill.get("fillSz"), positive=True)
        pnl, fee = _number(fill.get("fillPnl")), _number(fill.get("fee"))
        detail = order_details.get(order)
        if (not isinstance(detail, Mapping) or detail.get("ordId") != order
                or detail.get("instId") != INST or detail.get("posSide") != "net"
                or detail.get("tdMode") != "cross"
                or detail.get("state") not in {"filled", "canceled", "mmp_canceled"}):
            _refuse("LEDGER_ORDER_DETAIL_UNVERIFIED")
        matches = []
        for key, position in owned.items():
            if order == position["entry"]:
                matches.append((key, "entry"))
            elif order in position["closes"] or (detail.get("algoId") and
                    position["protection"] and detail["algoId"] == position["protection"]):
                matches.append((key, "exit"))
        if len(matches) != 1:
            _refuse("LEDGER_FILL_OWNERSHIP_UNVERIFIED")
        key, role = matches[0]
        position, history = owned[key], history_by_key[key]
        if fill.get("posId") not in (None, "", key[0]):
            _refuse("LEDGER_FILL_OWNERSHIP_UNVERIFIED")
        first, first_recorded = entry_boundaries.get(key, (key[1], key[1]))
        before_open = stamp < key[1]
        if (stamp > _integer(history["uTime"])
                or (before_open and (role != "entry" or not first <= key[1] <= first_recorded))):
            _refuse("LEDGER_LIFECYCLE_TIME_MISMATCH")
        entry_side = "buy" if position["route"]["side"] == "long" else "sell"
        expected_side = entry_side if role == "entry" else ("sell" if entry_side == "buy" else "buy")
        if (fill.get("side") != expected_side or detail.get("side") != expected_side
                or (role == "entry" and (pnl != ZERO or detail.get("ordType") not in {"limit", "post_only"}
                    or detail.get("clOrdId") != position["route"].get("client_order_id")))
                or (role == "exit" and (str(detail.get("reduceOnly", "")).lower() != "true"
                    or detail.get("ordType") not in {"market", "limit"}))):
            _refuse("LEDGER_FILL_ROLE_MISMATCH")
        parsed = dict(raw=fill, stamp=stamp, record_stamp=recorded,
                      role=role, size=size, pnl=pnl, fee=fee, key=key)
        position["fills"].append(parsed)
        fill_index[(INST, trade, order, fill["billId"])] = parsed
    matched_fills = set()
    for bill in bill_rows:
        if bill.get("instId") != INST or bill.get("instType") != "SWAP" or bill.get("ccy") != "USDT":
            _refuse("LEDGER_BILL_SCOPE")
        stamp = _integer(bill.get("ts"))
        if not start <= stamp <= now:
            _refuse("LEDGER_BILL_OUTSIDE_COVERAGE")
        amount = _number(bill.get("balChg"))
        if bill.get("type") == "2":
            key = (INST, _text(bill.get("tradeId")), _text(bill.get("ordId")), _text(bill.get("billId")))
            if key not in fill_index or key in matched_fills:
                _refuse("LEDGER_TRADE_BILL_UNMATCHED")
            fill = fill_index[key]
            if (not fill["stamp"] <= stamp <= fill["record_stamp"]
                    or _number(bill.get("pnl")) != fill["pnl"]
                    or _number(bill.get("fee")) != fill["fee"]
                    or amount != fill["pnl"] + fill["fee"]):
                _refuse("LEDGER_TRADE_BILL_MISMATCH")
            matched_fills.add(key)
        elif bill.get("type") == "8":
            # Actual funding bills need not contain posId. In proved net-mode,
            # single-currency account level 2, a complete non-overlapping owned
            # SOL lifecycle interval can uniquely assign a SOL/USDT funding bill.
            # A supplied posId remains an additional constraint, never a default.
            pos_id = _text(bill["posId"], "LEDGER_FUNDING_LINK_UNVERIFIED") if "posId" in bill else None
            matches = [key for key in owned if (pos_id is None or key[0] == pos_id)
                       and key[1] < stamp <= _integer(history_by_key[key]["uTime"])]
            if len(matches) != 1:
                _refuse("LEDGER_FUNDING_LINK_UNVERIFIED")
            owned[matches[0]]["funding"].append(dict(stamp=stamp, amount=amount))
        else:
            _refuse("LEDGER_EXTERNAL_CASH_FLOW")
    if len(matched_fills) != len(fill_index):
        _refuse("LEDGER_TRADE_BILL_MISSING")
    used_order_ids = {fill["raw"]["ordId"] for fill in fill_index.values()}
    if set(order_details) != used_order_ids:
        _refuse("LEDGER_ORDER_DETAIL_UNMATCHED")
    for order in used_order_ids:
        order_fills = [fill for fill in fill_index.values() if fill["raw"]["ordId"] == order]
        total_size = sum((fill["size"] for fill in order_fills), ZERO)
        detail = order_details[order]
        ordered = _number(detail.get("sz"), positive=True)
        if (_number(detail.get("accFillSz")) != total_size or ordered < total_size
                or (detail["state"] == "filled" and ordered != total_size)):
            _refuse("LEDGER_ORDER_DETAIL_SIZE_MISMATCH")
    completed = []
    entries = set()
    pnl_day = pnl_week = ZERO
    for key, position in owned.items():
        history = history_by_key[key]
        entry_fills = [fill for fill in position["fills"] if fill["role"] == "entry"]
        exits = [fill for fill in position["fills"] if fill["role"] == "exit"]
        if (not entry_fills or not exits
                or sum((f["size"] for f in entry_fills), ZERO) != position["quantity"]
                or sum((f["size"] for f in exits), ZERO) != position["quantity"]):
            _refuse("LEDGER_PARTIAL_POSITION_HISTORY")
        first_entry, first_recorded = entry_boundaries[key]
        last_exit = max(f["stamp"] for f in exits)
        last_recorded = min(f["record_stamp"] for f in exits if f["stamp"] == last_exit)
        if (not first_entry <= key[1] <= first_recorded
                or not last_exit <= _integer(history["uTime"]) <= last_recorded):
            _refuse("LEDGER_PARTIAL_POSITION_HISTORY")
        if max(f["stamp"] for f in entry_fills) > min(f["stamp"] for f in exits):
            _refuse("LEDGER_LIFECYCLE_OVERLAP")
        if first_entry >= day:
            entries.add(position["entry"])
        gross = sum((fill["pnl"] for fill in position["fills"]), ZERO)
        fees = sum((fill["fee"] for fill in position["fills"]), ZERO)
        funding = sum((item["amount"] for item in position["funding"]), ZERO)
        net = gross + fees + funding
        reported_net = _number(history.get("realizedPnl"))
        if (_number(history.get("pnl")) != gross or _number(history.get("fee")) != fees
                or _number(history.get("fundingFee")) != funding
                or abs(reported_net - net) > PNL_QUANTUM):
            _refuse("LEDGER_POSITION_PNL_MISMATCH")
        for fill in position["fills"]:
            amount = fill["pnl"] + fill["fee"]
            if fill["stamp"] >= day:
                pnl_day += amount
            if fill["stamp"] >= week:
                pnl_week += amount
        for item in position["funding"]:
            if item["stamp"] >= day:
                pnl_day += item["amount"]
            if item["stamp"] >= week:
                pnl_week += item["amount"]
        completed.append(dict(position_id=key[0], opened_at_ms=key[1],
                              closed_at_ms=_integer(history["uTime"]),
                              entry_order_id=position["entry"], net_pnl=reported_net,
                              accounted_net_pnl=net,
                              loss_for_pause=min(reported_net, net) < ZERO))
    completed.sort(key=lambda row: row["closed_at_ms"])
    previous_close = None
    for position in completed:
        key = (position["position_id"], position["opened_at_ms"])
        # A later history cTime may reflect recording delay. It cannot hide an
        # execution that overlaps the previous completed lifecycle's uTime.
        if previous_close is not None and entry_boundaries[key][0] <= previous_close:
            _refuse("LEDGER_LIFECYCLE_OVERLAP")
        previous_close = position["closed_at_ms"]
        if position["loss_for_pause"]:
            losses += 1
            if losses >= count_limit:
                until = position["closed_at_ms"] + pause_ms
        else:
            losses = 0
            # A previously-triggered cooldown is not erased by a later win.
        # Activity during an already active cooldown is itself unverified policy
        # compliance, but the ledger still computes the conservative pause end.
    return dict(verified=True, refusals=[], realized_pnl_today=pnl_day,
                realized_pnl_week=pnl_week, entries_today=len(entries),
                completed_positions=completed, consecutive_losses=losses,
                paused_until_ms=until, paused=until > now,
                account_hash=account, complete_through_ms=now)


def validate_demo_ledger(*, fills, bills, positions_history, routes, now_ms,
                         day_start_ms, week_start_ms, coverage, order_details=None,
                         consecutive_loss_limit=3, pause_hours=24) -> dict:
    """Validate fully linked evidence, or return no trusted accounting values.

    PnL values are exact Decimal values. Trade bills corroborate fills and are
    never added twice; position realizedPnl is checked, never added to fill PnL.
    Coverage must have all COMPLETE_FLAGS, DEMO/account binding, exact start/end,
    and an explicit completed non-losing position reset anchor. A legacy foreign
    position can serve only as that pre-authorization, pre-period loss reset;
    every later position must belong to the SOL router with complete evidence.

    Required coverage metadata beyond COMPLETE_FLAGS::

        environment="DEMO", account_hash=sha256("DEMO\\0" + uid),
        account_config={"uid": uid, "acctLv": "2", "posMode": "net_mode"},
        start_ms=int(anchor_record["uTime"]) + 1, end_ms=now_ms,
        authorization_at_ms=<actual new automation authorization time>,
        anchor_lookback_complete=True,
        anchor_lookback_start_ms<=int(anchor_record["uTime"])-pause_hours*3600000,
        loss_anchor={"source": "completed-position-reset", "environment": "DEMO",
                     "account_hash": ..., "record": <raw complete position row>}

    ``positions_history`` includes the anchor row, complete prior cooldown-window
    rows, and every later completed position. fills/bills cover start_ms..now_ms,
    including every required owned lifecycle. Snapshot admission/exposure checks
    still belong to the risk provider; this verifier alone cannot release trading.
    One entirely unsent RESERVED journal row can be inspected without counting an
    entry only when coverage.account_flat_and_empty is True, backed by actual
    complete before/after exposure reads. Unknown/sent/pending rows still refuse.
    """
    try:
        with localcontext() as context:
            context.prec = 80
            return _validate(fills=fills, bills=bills, positions_history=positions_history,
                             routes=routes, order_details=order_details,
                             now_ms=now_ms, day_start_ms=day_start_ms,
                             week_start_ms=week_start_ms, coverage=coverage,
                             consecutive_loss_limit=consecutive_loss_limit, pause_hours=pause_hours)
    except LedgerRefused as error:
        code = error.code
    except Exception:
        code = "LEDGER_EVIDENCE_INVALID"
    return dict(verified=False, refusals=[code], realized_pnl_today=None,
                realized_pnl_week=None, entries_today=None, completed_positions=[],
                consecutive_losses=None, paused_until_ms=None, paused=True)
