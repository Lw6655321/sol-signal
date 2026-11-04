# -*- coding: utf-8 -*-
"""Pure, paper-only execution planning. No IO, account access, or order transport.

``build_intent(signal, state, config)`` accepts the existing engine signal shape.
The caller must supply ``equity_estimate``, ``day_start_equity``,
``realized_pnl_today``, ``entries_today``, ``open_positions``, and explicit boolean
``data_fresh`` / ``macro_blackout`` in state. Optional ``now_ms`` makes historical
replays deterministic. Equity provenance and accounting belong to the caller;
the fixed equity in config is deliberately never used as a fallback.

Quantities are CONTRACTS; base quantity is qty * contract_value. Only the project's
linear SOL USDT contract is supported. Fees and adverse stop slippage are included
in sizing and net RR. The result is a plan, never authorization for a live trade.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Mapping


ALLOWED_INSTRUMENT = "SOL-USDT-SWAP"
ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")
BPS = Decimal("10000")
MAX_NOTIONAL_USDT = Decimal("400")


class ExecutionRejected(ValueError):
    """Fail-closed rejection with a stable machine-readable reason code."""

    def __init__(self, code: str, reason: str):
        self.code = code
        self.reason = reason
        super().__init__(f"{code}: {reason}")


@dataclass(frozen=True)
class Intent:
    signal_id: str
    inst_id: str
    side: str
    entry: Decimal
    stop: Decimal
    tp1: Decimal
    tp2: Decimal
    qty: Decimal
    notional: Decimal
    contract_value: Decimal
    lot_size: Decimal
    tick_size: Decimal
    created_at_ms: int
    expires_at_ms: int
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    slippage_bps: Decimal
    require_maker: bool
    risk_budget: Decimal
    effective_risk: Decimal
    rr: Decimal
    net_rr: Decimal
    kind: str = ""

    @property
    def expiry(self) -> int:
        return self.expires_at_ms

    def to_dict(self) -> dict:
        """JSON/CSV-safe values without rounding monetary values to floats."""
        return {key: str(value) if isinstance(value, Decimal) else value
                for key, value in asdict(self).items()}


def _reject(code: str, message: str) -> None:
    raise ExecutionRejected(code, message)


def _number(value, name: str, *, positive: bool = False,
            nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        _reject("INVALID_INPUT", f"{name} must be a finite number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        _reject("INVALID_INPUT", f"{name} must be a finite number")
    if not result.is_finite():
        _reject("INVALID_INPUT", f"{name} must be finite")
    if positive and result <= ZERO:
        _reject("INVALID_INPUT", f"{name} must be positive")
    if nonnegative and result < ZERO:
        _reject("INVALID_INPUT", f"{name} cannot be negative")
    return result


def _required(data: Mapping, name: str):
    if name not in data:
        _reject("MISSING_INPUT", f"missing {name}")
    return data[name]


def _integer(value, name: str) -> int:
    number = _number(value, name, nonnegative=True)
    if number != number.to_integral_value():
        _reject("INVALID_INPUT", f"{name} must be an integer")
    return int(number)


def _timestamp(value, name: str) -> int:
    if isinstance(value, str) and ("T" in value or "t" in value):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                _reject("INVALID_INPUT", f"{name} must include a timezone")
            delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
            return _integer(delta.days * 86400000 + delta.seconds * 1000 +
                            delta.microseconds // 1000, name)
        except (ValueError, OverflowError, OSError):
            _reject("INVALID_INPUT", f"invalid {name}")
    return _integer(value, name)


def _step(value: Decimal, increment: Decimal, rounding=ROUND_FLOOR) -> Decimal:
    return (value / increment).to_integral_value(rounding=rounding) * increment


def _flag(state: Mapping, name: str, *, required: bool = False) -> bool:
    value = _required(state, name) if required else state.get(name, False)
    if not isinstance(value, bool):
        _reject("INVALID_INPUT", f"{name} must be a boolean")
    return value


def notional_limit(equity: object, config: Mapping) -> Decimal:
    """Pure shared cap: equity times leverage, the hard 400u ceiling, and config.

    Both ``max_notional`` and ``max_notional_usd`` are recognized at the root,
    risk, and account levels. Every present value must be finite and positive;
    an absent configured cap never removes the independent hard ceiling.
    """
    if not isinstance(config, Mapping):
        _reject("INVALID_INPUT", "config must be a mapping")
    risk = _required(config, "risk")
    account = _required(config, "account")
    if not isinstance(risk, Mapping) or not isinstance(account, Mapping):
        _reject("INVALID_INPUT", "risk and account must be mappings")
    balance = _number(equity, "equity_estimate", positive=True)
    leverage = _number(_required(account, "leverage_setting"),
                       "leverage_setting", positive=True)
    cap = min(balance * leverage, MAX_NOTIONAL_USDT)
    for scope_name, scope in (("config", config), ("risk", risk), ("account", account)):
        for field in ("max_notional", "max_notional_usd"):
            if field in scope:
                cap = min(cap, _number(scope[field], f"{scope_name}.{field}", positive=True))
    return cap


def build_intent(signal: Mapping, state: Mapping, config: Mapping) -> Intent:
    """Build a bounded paper limit intent or raise :class:`ExecutionRejected`.

    ``signal.qty`` is an optional upper bound, not trusted sizing. Caller-supplied
    ``slippage_bps`` (state first, then signal, otherwise 0) is an explicit paper
    scenario, not a measurement of real liquidity. Weekly PnL/weekly-loss flags,
    when supplied by the accounting caller, are also honored. This pure function
    neither reserves risk capacity nor persists state; the eventual execution
    coordinator must do that atomically and revalidate before submission.
    """
    if not all(isinstance(item, Mapping) for item in (signal, state, config)):
        _reject("INVALID_INPUT", "signal, state and config must be mappings")
    risk = _required(config, "risk")
    account = _required(config, "account")
    if not isinstance(risk, Mapping) or not isinstance(account, Mapping):
        _reject("INVALID_INPUT", "risk and account must be mappings")
    now = _timestamp(state.get("now_ms", int(datetime.now(timezone.utc).timestamp() * 1000)),
                     "now_ms")
    signal_id = signal.get("id")
    if not isinstance(signal_id, str) or not signal_id.strip():
        _reject("INVALID_INPUT", "signal id is required")
    inst_id = signal.get("instId", ALLOWED_INSTRUMENT)
    if inst_id != ALLOWED_INSTRUMENT:
        _reject("INSTRUMENT", "instrument is outside the hard whitelist")
    side = signal.get("side")
    if side not in ("long", "short"):
        _reject("INVALID_INPUT", "side must be long or short")
    created = _timestamp(signal.get("generated_at_ms", signal.get("generated_at_utc")),
                         "generated_at")
    expiry = _timestamp(signal.get("expires_at_ms", signal.get("expires_at_utc")),
                        "expires_at")
    if created > now or expiry <= now or expiry <= created:
        _reject("EXPIRED", "signal is expired or has inconsistent timestamps")
    if not _flag(state, "data_fresh", required=True):
        _reject("STALE_DATA", "caller has not confirmed fresh critical data")
    if _flag(state, "macro_blackout", required=True):
        _reject("MACRO_BLACKOUT", "macro blackout is active")
    if _flag(state, "halted") or _flag(state, "daily_loss_hit"):
        _reject("HALTED", "caller risk state blocks new exposure")
    if _flag(state, "weekly_loss_hit"):
        _reject("WEEKLY_LOSS", "weekly loss limit reached")
    if _flag(state, "paused") or _integer(state.get("paused_until_ms", 0), "paused_until_ms") > now:
        _reject("PAUSED", "consecutive-loss pause is active")
    if signal_id in state.get("seen_signal_ids", []):
        _reject("DUPLICATE", "signal was already handled")

    equity = _number(_required(state, "equity_estimate"), "equity_estimate", positive=True)
    day_start = _number(_required(state, "day_start_equity"), "day_start_equity", positive=True)
    pnl = _number(_required(state, "realized_pnl_today"), "realized_pnl_today")
    entries = _integer(_required(state, "entries_today"), "entries_today")
    entry_limit = _integer(_required(risk, "max_entries_per_day"), "max_entries_per_day")
    if entries >= entry_limit or ("entries_left" in state and
                                  _integer(state["entries_left"], "entries_left") == 0):
        _reject("DAILY_ENTRIES", "daily entry allowance exhausted")
    positions = _required(state, "open_positions")
    pending = state.get("pending_orders", [])
    if not isinstance(positions, (list, tuple)) or not isinstance(pending, (list, tuple)):
        _reject("INVALID_INPUT", "positions and pending_orders must be sequences")
    slots = _integer(_required(account, "max_concurrent_positions"), "max_concurrent_positions")
    # This project's one-position/no-averaging rule remains a hard ceiling.
    if len(positions) + len(pending) >= min(1, slots):
        _reject("POSITION_LIMIT", "a position or reserved entry already occupies the slot")

    daily_pct = _number(_required(risk, "daily_loss_limit_pct"), "daily_loss_limit_pct", positive=True)
    remaining_day = day_start * daily_pct / HUNDRED + pnl
    if remaining_day <= ZERO:
        _reject("DAILY_LOSS", "daily loss allowance exhausted")
    if "realized_pnl_week" in state:
        week_start = _number(_required(state, "week_start_equity"), "week_start_equity", positive=True)
        week_pct = _number(_required(risk, "weekly_loss_limit_pct"), "weekly_loss_limit_pct", positive=True)
        week_pnl = _number(state["realized_pnl_week"], "realized_pnl_week")
        remaining_week = week_start * week_pct / HUNDRED + week_pnl
        if remaining_week <= ZERO:
            _reject("WEEKLY_LOSS", "weekly loss allowance exhausted")
    else:
        remaining_week = equity  # T1 does not invent a weekly accounting ledger.

    specs = _required(config, "symbols")
    if not isinstance(specs, list):
        _reject("INVALID_INPUT", "symbols must be a list")
    matches = [spec for spec in specs if isinstance(spec, Mapping) and spec.get("instId") == inst_id]
    if len(matches) != 1:
        _reject("INSTRUMENT", "exactly one matching instrument specification is required")
    spec = matches[0]
    if spec.get("ctType", "linear") != "linear" or spec.get("ctValCcy", "SOL") != "SOL":
        _reject("INSTRUMENT", "paper sizing supports only linear SOL-denominated contracts")
    contract_value = _number(_required(spec, "ctVal"), "ctVal", positive=True)
    lot = _number(_required(spec, "lotSz"), "lotSz", positive=True)
    min_qty = _number(_required(spec, "minSz"), "minSz", positive=True)
    tick = _number(_required(spec, "tickSz"), "tickSz", positive=True)

    long = side == "long"
    entry = _step(_number(_required(signal, "entry"), "entry", positive=True), tick,
                  ROUND_FLOOR if long else ROUND_CEILING)
    # Never widen an existing signal's stop while fitting exchange precision.
    stop = _step(_number(_required(signal, "stop"), "stop", positive=True), tick,
                 ROUND_CEILING if long else ROUND_FLOOR)
    tp1 = _step(_number(_required(signal, "tp1"), "tp1", positive=True), tick,
                ROUND_FLOOR if long else ROUND_CEILING)
    tp2 = _step(_number(_required(signal, "tp2"), "tp2", positive=True), tick,
                ROUND_FLOOR if long else ROUND_CEILING)
    if not (ZERO < stop < entry < tp1 <= tp2 if long else ZERO < tp2 <= tp1 < entry < stop):
        _reject("PRICE_GEOMETRY", "entry, stop and targets have invalid direction or tick precision")

    playbooks = config.get("playbooks", {})
    if not isinstance(playbooks, Mapping):
        _reject("INVALID_INPUT", "playbooks must be a mapping")
    kind = signal.get("kind")
    if kind is not None and not isinstance(kind, str):
        _reject("INVALID_INPUT", "kind must be a string")
    if playbooks and kind not in playbooks:
        _reject("PLAYBOOK_UNKNOWN", "signal must name a configured playbook")
    playbook = playbooks.get(kind, {})
    if not isinstance(playbook, Mapping):
        _reject("INVALID_INPUT", "playbook must be a mapping")
    if kind in playbooks and playbook.get("enabled") is not True:
        _reject("PLAYBOOK_DISABLED", "signal playbook is disabled")
    scale = _number(playbook.get("risk_scale", 1), "risk_scale", positive=True)
    if scale > ONE:
        _reject("INVALID_INPUT", "risk_scale cannot raise the single-trade risk ceiling")
    stop_distance = abs(entry - stop)
    stop_pct = stop_distance / entry * HUNDRED
    for field, comparison in (("min_stop_pct", "min"), ("max_stop_pct", "max")):
        value = playbook.get(field, risk.get(field))
        if value is not None:
            bound = _number(value, field, positive=True)
            if (comparison == "min" and stop_pct < bound) or (comparison == "max" and stop_pct > bound):
                _reject("STOP_DISTANCE", "stop distance is outside configured bounds")

    fee = _required(risk, "fee")
    if not isinstance(fee, Mapping):
        _reject("INVALID_INPUT", "fee must be a mapping")
    maker = _number(_required(fee, "maker_pct"), "maker_pct", nonnegative=True) / HUNDRED
    taker = _number(_required(fee, "taker_pct"), "taker_pct", nonnegative=True) / HUNDRED
    max_fee_share = _number(_required(fee, "max_fee_share_of_risk"),
                            "max_fee_share_of_risk", nonnegative=True)
    require_maker = (playbook.get("entry_style") == "maker" or
                     "maker" in str(signal.get("entry_type", "")).lower() or
                     signal.get("require_maker") is True)
    entry_rate = maker if require_maker else max(maker, taker)
    slip_bps = _number(state.get("slippage_bps", signal.get("slippage_bps", 0)),
                       "slippage_bps", nonnegative=True)
    if slip_bps >= BPS:
        _reject("INVALID_INPUT", "slippage_bps must be below 10000")
    slip = slip_bps / BPS
    stop_fill = _step(stop * (ONE - slip if long else ONE + slip), tick,
                      ROUND_FLOOR if long else ROUND_CEILING)
    if stop_fill <= ZERO:
        _reject("INVALID_INPUT", "slipped stop must remain positive")
    entry_fee_unit = entry * contract_value * entry_rate
    stop_fee_unit = stop_fill * contract_value * taker
    price_risk_unit = stop_distance * contract_value
    # The transport may execute a TP as a market exit. The fee-share gate uses
    # three taker legs, even for maker-only entries; sizing below still budgets
    # the actual entry style and one stop exit, not two mutually exclusive exits.
    tp_fill = _step(tp1 * (ONE - slip if long else ONE + slip), tick,
                    ROUND_FLOOR if long else ROUND_CEILING)
    if tp_fill <= ZERO:
        _reject("INVALID_INPUT", "slipped target must remain positive")
    fee_share = ((entry + stop_fill + tp_fill) * contract_value * taker /
                 price_risk_unit)
    if fee_share > max_fee_share:
        _reject("FEE_SHARE", "entry, stop and target taker fees exceed configured share of price risk")
    loss_unit = abs(entry - stop_fill) * contract_value + entry_fee_unit + stop_fee_unit
    # Targets are conservatively modeled as taker exits too; no unearned maker rebate.
    direction = ONE if long else -ONE
    reward_unit = (tp_fill - entry) * direction * contract_value - entry_fee_unit - tp_fill * contract_value * taker
    rr = abs(tp1 - entry) / stop_distance
    net_rr = reward_unit / loss_unit
    min_rr = max(_number(_required(risk, "min_rr"), "min_rr", positive=True),
                 _number(playbook.get("min_rr", risk["min_rr"]), "playbook.min_rr", positive=True))
    min_net_rr = _number(_required(risk, "min_net_rr"), "min_net_rr", positive=True)
    if rr < min_rr or net_rr < min_net_rr:
        _reject("RR", "gross or cost-adjusted reward/risk is below configured threshold")

    risk_pct = _number(_required(risk, "risk_per_trade_pct"), "risk_per_trade_pct", positive=True)
    risk_budget = min(equity * risk_pct / HUNDRED * scale, remaining_day, remaining_week)
    notional_cap = notional_limit(equity, config)
    reserved = _number(state.get("reserved_notional", 0), "reserved_notional", nonnegative=True)
    notional_cap -= reserved
    if notional_cap <= ZERO:
        _reject("NOTIONAL", "no notional capacity remains")
    quantity_cap = min(risk_budget / loss_unit, notional_cap / (entry * contract_value))
    if "qty" in signal:
        quantity_cap = min(quantity_cap, _number(signal["qty"], "signal.qty", positive=True))
    qty = _step(quantity_cap, lot)
    if qty < min_qty:
        _reject("MIN_QTY", "risk-bounded quantity is below instrument minimum")
    notional = qty * entry * contract_value
    effective_risk = qty * loss_unit
    if effective_risk > risk_budget or notional > notional_cap:
        _reject("RISK", "rounded intent exceeds risk or notional capacity")
    return Intent(signal_id=signal_id, inst_id=inst_id, side=side, entry=entry,
                  stop=stop, tp1=tp1, tp2=tp2, qty=qty, notional=notional,
                  contract_value=contract_value, lot_size=lot, tick_size=tick,
                  created_at_ms=created, expires_at_ms=expiry, maker_fee_rate=maker,
                  taker_fee_rate=taker, slippage_bps=slip_bps, require_maker=require_maker,
                  risk_budget=risk_budget, effective_risk=effective_risk, rr=rr, net_rr=net_rr,
                  kind=kind or "")
