"""Local, deterministic paper execution; this module has no exchange connection.

Quantities are contract counts and ``Intent.contract_value`` is base coin per
contract. Quotes are point observations, not candles: neither queue priority nor
actual liquidity can be inferred from them. ``available_qty`` is an explicit
shared liquidity assumption for one event. TP1 closes the position; TP2 is kept
on the intent and in the audit log as a reference only.

An initial marketable order is conservatively treated as taker, including when
the first quote is received after submission. To model a maker order, establish
a non-marketable quote first. A maker-only order is rejected if initially
marketable. Resting fills and TP1 fills use their limit price, with maker fees;
aggressive fills use adverse slippage and taker fees. Stops can gap through the
stop price and remain triggered when an event has insufficient liquidity.

Each default broker creates ``logs/paper/run-UTC_DATE-UUID/`` for its paper CSVs.
An explicit ``log_dir`` selects exactly that directory. Logs are append-only.
Restart recovery is intentionally not
implemented: opening a directory containing previous records raises an error,
so an existing paper position cannot silently disappear on restart. Use a new
directory for a distinct run. ``clock`` returns integer epoch milliseconds.
"""

from __future__ import annotations

import csv
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Callable

from .execution import Intent


ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")
ALLOWED_INST = "SOL-USDT-SWAP"


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite decimal")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{name} must be a finite decimal") from None
    if not number.is_finite() or number < ZERO or (positive and number == ZERO):
        raise ValueError(f"{name} must be {'positive' if positive else 'non-negative'} and finite")
    return number


def _timestamp(value: object) -> int:
    number = _decimal(value, "timestamp_ms")
    if number != number.to_integral_value():
        raise ValueError("timestamp_ms must be an integer number of milliseconds")
    return int(number)


def _floor_qty(qty: Decimal, lot_size: Decimal) -> Decimal:
    return (qty / lot_size).to_integral_value(rounding=ROUND_FLOOR) * lot_size


@dataclass
class PaperOrder:
    """Mutable order snapshot returned by the broker; callers must not edit it.

    ``filled_qty`` is cumulative entry size, ``remaining_qty`` is still-live
    entry size, and ``open_qty`` is current exposure. ``realized_pnl`` includes
    all fees already charged, even while part of the position remains open.
    A close event's ``net_pnl`` instead allocates entry fees to that close only.
    ``status == 'filled'`` does not mean the position has been closed.
    """

    order_id: str
    intent: Intent
    submitted_at_ms: int
    status: str = "pending"
    filled_qty: Decimal = field(default_factory=lambda: ZERO)
    remaining_qty: Decimal = field(default_factory=lambda: ZERO)
    open_qty: Decimal = field(default_factory=lambda: ZERO)
    avg_entry_price: Decimal = field(default_factory=lambda: ZERO)
    entry_fee: Decimal = field(default_factory=lambda: ZERO)
    exit_fee: Decimal = field(default_factory=lambda: ZERO)
    gross_pnl: Decimal = field(default_factory=lambda: ZERO)
    cancelled_qty: Decimal = field(default_factory=lambda: ZERO)
    protection_active: bool = False
    last_update_ms: int = 0
    reason: str = ""
    _unallocated_entry_fee: Decimal = field(default_factory=lambda: ZERO, repr=False)

    @property
    def id(self) -> str:
        return self.order_id

    @property
    def realized_pnl(self) -> Decimal:
        return self.gross_pnl - self.entry_fee - self.exit_fee

    @property
    def net_pnl(self) -> Decimal:
        return self.realized_pnl

    @property
    def closed_qty(self) -> Decimal:
        return self.filled_qty - self.open_qty

    def to_dict(self) -> dict:
        result = {
            "mode": "paper", "environment": "paper",
            "order_id": self.order_id,
            "signal_id": self.intent.signal_id,
            "inst_id": self.intent.inst_id,
            "side": self.intent.side,
            "status": self.status,
            "submitted_at_ms": self.submitted_at_ms,
            "last_update_ms": self.last_update_ms,
            "protection_active": self.protection_active,
            "reason": self.reason,
        }
        for name in (
            "filled_qty", "remaining_qty", "open_qty", "avg_entry_price",
            "entry_fee", "exit_fee", "gross_pnl", "cancelled_qty",
        ):
            result[name] = str(getattr(self, name))
        result["net_pnl"] = str(self.realized_pnl)
        result["intent"] = self.intent.to_dict()
        return result


class PaperBroker:
    """Paper orders only; one broker instance and one run per log directory.

    A nonzero constructor ``slippage_bps`` overrides the intent's assumption;
    the default zero uses each intent's ``slippage_bps``. ``on_price`` processes
    existing protective orders before new entries, and never uses a new entry
    to manufacture an exit on the same quote. ``close`` is a caller-requested
    simulated market exit, so its supplied price is an assumed executable quote.
    """

    ORDER_FIELDS = (
        "mode", "environment", "timestamp_ms", "event", "order_id", "signal_id",
        "inst_id", "side", "status", "entry", "stop", "tp1", "tp2",
        "qty", "contract_value", "lot_size", "tick_size", "expires_at_ms",
        "maker_fee_rate", "taker_fee_rate", "slippage_bps", "require_maker",
        "filled_qty", "remaining_qty", "open_qty", "avg_entry_price",
        "entry_fee", "exit_fee", "gross_pnl", "net_pnl", "cancelled_qty",
        "protection_active", "reason",
    )
    TRADE_FIELDS = (
        "mode", "environment", "timestamp_ms", "trade_id", "order_id", "signal_id",
        "inst_id", "side", "reason", "qty", "contract_value", "entry_price",
        "exit_price", "gross_pnl", "entry_fee", "exit_fee", "total_fee",
        "net_pnl", "liquidity",
    )

    def __init__(
        self,
        log_dir: str | Path | None = None,
        *,
        slippage_bps: object = 0,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._slippage_bps = _decimal(slippage_bps, "slippage_bps")
        if self._slippage_bps >= BPS:
            raise ValueError("slippage_bps must be less than 10000")
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._orders: dict[str, PaperOrder] = {}
        self._signal_ids: set[str] = set()
        self._resting: dict[str, bool] = {}
        self._stop_triggered: set[str] = set()
        self._last_quote: tuple[Decimal, Decimal, int] | None = None
        if log_dir is None:
            run_time = datetime.fromtimestamp(self._now() / 1000, timezone.utc)
            run_name = f"run-{run_time:%Y%m%d-%H%M%S}-{uuid.uuid4().hex}"
            self.log_dir = Path("logs") / "paper" / run_name
        else:
            self.log_dir = Path(log_dir)
        self.orders_path = self.log_dir / "paper_orders.csv"
        self.trades_path = self.log_dir / "paper_trades.csv"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        # Inspect both files before modifying either one.
        for path, fields in (
            (self.orders_path, self.ORDER_FIELDS),
            (self.trades_path, self.TRADE_FIELDS),
        ):
            if path.exists() and path.stat().st_size:
                with path.open("r", newline="", encoding="utf-8") as handle:
                    rows = csv.DictReader(handle)
                    if tuple(rows.fieldnames or ()) != fields:
                        raise RuntimeError(f"Incompatible paper log: {path}")
                    if next(rows, None) is not None:
                        raise RuntimeError(
                            f"Previous paper records exist in {self.log_dir}; "
                            "recovery is not implemented. Use a new log directory "
                            "for a distinct paper run; do not overwrite old records."
                        )
        for path, fields in (
            (self.orders_path, self.ORDER_FIELDS),
            (self.trades_path, self.TRADE_FIELDS),
        ):
            if not path.exists() or not path.stat().st_size:
                with path.open("w", newline="", encoding="utf-8") as handle:
                    csv.DictWriter(handle, fieldnames=fields).writeheader()

    def _now(self) -> int:
        return _timestamp(self._clock())

    def _slippage(self, intent: Intent) -> Decimal:
        return self._slippage_bps or _decimal(intent.slippage_bps, "intent.slippage_bps")

    def _validate_intent(self, intent: Intent) -> None:
        if not isinstance(intent.signal_id, str) or not intent.signal_id.strip():
            raise ValueError("signal_id must be a non-empty string")
        if intent.inst_id != ALLOWED_INST:
            raise ValueError(f"Paper broker only accepts {ALLOWED_INST}")
        if intent.side not in ("long", "short"):
            raise ValueError("side must be long or short")
        for name in ("entry", "stop", "tp1", "tp2", "qty", "contract_value", "lot_size", "tick_size"):
            _decimal(getattr(intent, name), name, positive=True)
        for name in ("maker_fee_rate", "taker_fee_rate", "slippage_bps"):
            _decimal(getattr(intent, name), name)
        if self._slippage(intent) >= BPS:
            raise ValueError("slippage_bps must be less than 10000")
        if not isinstance(intent.require_maker, bool):
            raise ValueError("require_maker must be a boolean")
        if intent.qty % intent.lot_size:
            raise ValueError("intent qty must be a multiple of lot_size")
        for name in ("entry", "stop", "tp1", "tp2"):
            if getattr(intent, name) % intent.tick_size:
                raise ValueError(f"intent {name} must align with tick_size")
        if intent.side == "long" and not (intent.stop < intent.entry < intent.tp1 <= intent.tp2):
            raise ValueError("long prices must satisfy stop < entry < tp1 <= tp2")
        if intent.side == "short" and not (intent.stop > intent.entry > intent.tp1 >= intent.tp2):
            raise ValueError("short prices must satisfy stop > entry > tp1 >= tp2")
        if _timestamp(intent.expires_at_ms) <= _timestamp(intent.created_at_ms):
            raise ValueError("intent expiry must follow its creation time")

    @staticmethod
    def _marketable(intent: Intent, bid: Decimal, ask: Decimal) -> bool:
        return ask <= intent.entry if intent.side == "long" else bid >= intent.entry

    def place_limit(self, intent: Intent) -> PaperOrder:
        """Register a limit order without inventing liquidity or a fill.

        When a known quote makes a maker-only submission marketable, return a
        rejected paper order. Duplicate signal IDs raise ``ValueError``.
        """
        self._validate_intent(intent)
        if intent.signal_id in self._signal_ids:
            raise ValueError(f"Duplicate paper signal_id: {intent.signal_id}")
        # The project has one position slot and forbids averaging. Enforce
        # this again here so two plans built from the same empty state cannot
        # bypass the planner's capacity check when submitted sequentially.
        if any(order.remaining_qty > ZERO or order.open_qty > ZERO
               for order in self._orders.values()):
            raise ValueError("Paper position slot is occupied by an entry or position")
        now = self._now()
        order = PaperOrder(
            order_id="paper-" + uuid.uuid4().hex,
            intent=intent,
            submitted_at_ms=now,
            remaining_qty=intent.qty,
            last_update_ms=now,
        )
        self._orders[order.order_id] = order
        self._signal_ids.add(intent.signal_id)
        resting = False
        if self._last_quote is not None:
            bid, ask, _ = self._last_quote
            resting = not self._marketable(intent, bid, ask)
        self._resting[order.order_id] = resting
        if intent.expires_at_ms <= now:
            self._cancel_entry(order, "expired", now, status="expired")
        elif self._last_quote is not None and not resting and intent.require_maker:
            self._cancel_entry(order, "would_take_liquidity", now, status="rejected")
        else:
            self._record_order(order, "submitted", now)
        return order

    def get_order(self, order_id: str) -> PaperOrder:
        try:
            return self._orders[order_id]
        except KeyError:
            raise KeyError(f"Unknown paper order: {order_id}") from None

    @property
    def positions(self) -> tuple[PaperOrder, ...]:
        """Current paper positions, including positions with cancelled entries."""
        return tuple(order for order in self._orders.values() if order.open_qty > ZERO)

    @property
    def orders(self) -> tuple[PaperOrder, ...]:
        return tuple(self._orders.values())

    @staticmethod
    def _refresh_status(order: PaperOrder) -> None:
        if order.open_qty == ZERO and order.filled_qty > ZERO:
            order.status = "closed"
        elif order.remaining_qty > ZERO:
            order.status = "partially_filled" if order.filled_qty > ZERO else "pending"
        elif order.open_qty > ZERO:
            order.status = "filled"
        order.protection_active = order.open_qty > ZERO

    def _record_order(self, order: PaperOrder, event: str, timestamp_ms: int) -> None:
        intent = order.intent
        row = {
            "mode": "paper", "environment": "paper", "timestamp_ms": timestamp_ms,
            "event": event, "order_id": order.order_id,
            "signal_id": intent.signal_id, "inst_id": intent.inst_id,
            "side": intent.side, "status": order.status,
            "net_pnl": order.realized_pnl, "reason": order.reason,
        }
        for name in (
            "entry", "stop", "tp1", "tp2", "qty", "contract_value", "lot_size",
            "tick_size", "expires_at_ms", "maker_fee_rate", "taker_fee_rate",
            "require_maker",
        ):
            row[name] = getattr(intent, name)
        row["slippage_bps"] = self._slippage(intent)
        for name in (
            "filled_qty", "remaining_qty", "open_qty", "avg_entry_price",
            "entry_fee", "exit_fee", "gross_pnl", "cancelled_qty", "protection_active",
        ):
            row[name] = getattr(order, name)
        with self.orders_path.open("a", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=self.ORDER_FIELDS).writerow(row)

    def _cancel_entry(
        self, order: PaperOrder, reason: str, timestamp_ms: int, *, status: str = "cancelled"
    ) -> dict:
        qty = order.remaining_qty
        order.cancelled_qty += qty
        order.remaining_qty = ZERO
        order.reason = reason
        order.last_update_ms = timestamp_ms
        order.status = status
        self._refresh_status(order)
        self._record_order(order, status, timestamp_ms)
        return {
            "mode": "paper", "environment": "paper", "event": status, "type": "cancel",
            "order_id": order.order_id, "reason": reason, "qty": qty,
            "fee": ZERO, "timestamp_ms": timestamp_ms,
        }

    def cancel(self, order_id: str, *, reason: str = "manual", timestamp_ms: int | None = None) -> dict:
        """Cancel remaining entry size; filled size keeps its protective orders."""
        order = self.get_order(order_id)
        now = self._now() if timestamp_ms is None else _timestamp(timestamp_ms)
        if now < order.last_update_ms:
            raise ValueError("Cannot cancel using a timestamp older than this order")
        if order.remaining_qty == ZERO:
            raise ValueError("Paper order has no remaining entry quantity")
        return self._cancel_entry(order, reason, now)

    @staticmethod
    def _round_price(price: Decimal, tick: Decimal, *, buy: bool) -> Decimal:
        rounding = ROUND_CEILING if buy else ROUND_FLOOR
        rounded = (price / tick).to_integral_value(rounding=rounding) * tick
        if rounded <= ZERO:
            raise ValueError("Slippage produced a non-positive executable price")
        return rounded

    def _aggressive_price(self, intent: Intent, quote: Decimal, *, buy: bool) -> Decimal:
        factor = ONE + self._slippage(intent) / BPS if buy else ONE - self._slippage(intent) / BPS
        return self._round_price(quote * factor, intent.tick_size, buy=buy)

    def _fill_entry(
        self, order: PaperOrder, qty: Decimal, bid: Decimal, ask: Decimal,
        liquidity: str, timestamp_ms: int,
    ) -> dict:
        intent = order.intent
        if liquidity == "maker":
            price = intent.entry
            fee_rate = intent.maker_fee_rate
        elif intent.side == "long":
            price = min(intent.entry, self._aggressive_price(intent, ask, buy=True))
            fee_rate = intent.taker_fee_rate
        else:
            price = max(intent.entry, self._aggressive_price(intent, bid, buy=False))
            fee_rate = intent.taker_fee_rate
        notional = price * qty * intent.contract_value
        fee = notional * fee_rate
        order.avg_entry_price = (
            order.avg_entry_price * order.open_qty + price * qty
        ) / (order.open_qty + qty)
        order.filled_qty += qty
        order.remaining_qty -= qty
        order.open_qty += qty
        order.entry_fee += fee
        order._unallocated_entry_fee += fee
        order.last_update_ms = timestamp_ms
        self._refresh_status(order)
        self._record_order(order, "entry_fill", timestamp_ms)
        return {
            "mode": "paper", "environment": "paper", "event": "fill", "type": "entry",
            "order_id": order.order_id, "qty": qty, "price": price,
            "fee": fee, "notional": notional, "liquidity": liquidity,
            "filled_qty": order.filled_qty, "remaining_qty": order.remaining_qty,
            "open_qty": order.open_qty, "protection_active": True,
            "timestamp_ms": timestamp_ms,
        }

    def _fill_exit(
        self, order: PaperOrder, qty: Decimal, price: Decimal, reason: str,
        liquidity: str, timestamp_ms: int,
    ) -> dict:
        intent = order.intent
        # Allocate the exact residual to the final close, avoiding fee drift
        # after repeated partial closes under finite Decimal precision.
        entry_fee = (
            order._unallocated_entry_fee if qty == order.open_qty
            else order._unallocated_entry_fee * qty / order.open_qty
        )
        fee_rate = intent.maker_fee_rate if liquidity == "maker" else intent.taker_fee_rate
        exit_fee = qty * intent.contract_value * price * fee_rate
        direction = ONE if intent.side == "long" else -ONE
        gross_pnl = (price - order.avg_entry_price) * qty * intent.contract_value * direction
        net_pnl = gross_pnl - entry_fee - exit_fee
        order._unallocated_entry_fee -= entry_fee
        order.open_qty -= qty
        order.exit_fee += exit_fee
        order.gross_pnl += gross_pnl
        order.reason = reason
        order.last_update_ms = timestamp_ms
        self._refresh_status(order)
        if order.open_qty == ZERO:
            self._stop_triggered.discard(order.order_id)
        trade_id = "paper-trade-" + uuid.uuid4().hex
        row = {
            "mode": "paper", "environment": "paper", "timestamp_ms": timestamp_ms,
            "trade_id": trade_id, "order_id": order.order_id,
            "signal_id": intent.signal_id, "inst_id": intent.inst_id,
            "side": intent.side, "reason": reason, "qty": qty,
            "contract_value": intent.contract_value,
            "entry_price": order.avg_entry_price, "exit_price": price,
            "gross_pnl": gross_pnl, "entry_fee": entry_fee, "exit_fee": exit_fee,
            "total_fee": entry_fee + exit_fee, "net_pnl": net_pnl,
            "liquidity": liquidity,
        }
        with self.trades_path.open("a", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=self.TRADE_FIELDS).writerow(row)
        self._record_order(order, "exit_fill", timestamp_ms)
        return {
            "mode": "paper", "environment": "paper", "event": "close", "type": "exit",
            "trade_id": trade_id, "order_id": order.order_id,
            "reason": reason, "qty": qty, "price": price,
            "entry_price": order.avg_entry_price,
            "gross_pnl": gross_pnl, "entry_fee": entry_fee, "exit_fee": exit_fee,
            "fee": entry_fee + exit_fee, "net_pnl": net_pnl,
            "total_net_pnl": order.realized_pnl, "open_qty": order.open_qty,
            "liquidity": liquidity, "timestamp_ms": timestamp_ms,
        }

    def on_price(
        self, price: object | None = None, *, bid: object | None = None,
        ask: object | None = None, available_qty: object | None = None,
        timestamp_ms: int | None = None,
    ) -> list[dict]:
        """Apply one point quote, with shared contract liquidity for all fills.

        ``price`` supplies a zero-spread quote, or supplies either omitted side
        of a bid/ask quote. Without ``price`` both bid and ask are required.
        An omitted liquidity limit means unlimited assumed liquidity; zero
        means no fills. Out-of-order timestamps and crossed spreads are rejected.
        """
        fallback = _decimal(price, "price", positive=True) if price is not None else None
        if bid is None:
            bid = fallback
        if ask is None:
            ask = fallback
        if bid is None or ask is None:
            raise ValueError("Provide price, or both bid and ask")
        bid_d = _decimal(bid, "bid", positive=True)
        ask_d = _decimal(ask, "ask", positive=True)
        if bid_d > ask_d:
            raise ValueError("bid cannot exceed ask")
        remaining = None if available_qty is None else _decimal(available_qty, "available_qty")
        now = self._now() if timestamp_ms is None else _timestamp(timestamp_ms)
        if self._last_quote is not None and now < self._last_quote[2]:
            raise ValueError("Cannot process an out-of-order paper quote")
        if any(now < order.last_update_ms for order in self._orders.values()):
            raise ValueError("Quote timestamp precedes a submitted order or close")
        self._last_quote = bid_d, ask_d, now
        events: list[dict] = []

        def take_qty(needed: Decimal, lot: Decimal) -> Decimal:
            nonlocal remaining
            qty = _floor_qty(needed if remaining is None else min(needed, remaining), lot)
            if remaining is not None:
                remaining -= qty
            return qty

        # Expiry only cancels the unfilled entry, never a filled position's
        # synthetic bracket. Record cancellations even if liquidity is zero.
        for order in self._orders.values():
            if order.remaining_qty > ZERO and now >= order.intent.expires_at_ms:
                events.append(self._cancel_entry(order, "expired", now, status="expired"))

        # Positions present before this quote get first claim on event liquidity.
        for order in self._orders.values():
            if order.open_qty == ZERO:
                continue
            intent = order.intent
            stop_hit = (
                bid_d <= intent.stop if intent.side == "long" else ask_d >= intent.stop
            )
            tp_hit = bid_d >= intent.tp1 if intent.side == "long" else ask_d <= intent.tp1
            if stop_hit:
                self._stop_triggered.add(order.order_id)
            if order.order_id in self._stop_triggered:
                reason, liquidity = "stop_loss", "taker"
                quote = bid_d if intent.side == "long" else ask_d
                exit_price = self._aggressive_price(intent, quote, buy=intent.side == "short")
            elif tp_hit:
                reason, liquidity = "take_profit", "maker"
                exit_price = intent.tp1
            else:
                continue
            if order.remaining_qty > ZERO:
                events.append(self._cancel_entry(order, f"{reason}_triggered", now))
            qty = take_qty(order.open_qty, intent.lot_size)
            if qty > ZERO:
                events.append(self._fill_exit(order, qty, exit_price, reason, liquidity, now))

        # Only after old positions are protected may pending entries consume
        # the remaining event liquidity. No exit pass follows these fills.
        for order in self._orders.values():
            if order.remaining_qty == ZERO or now < order.intent.created_at_ms:
                continue
            intent = order.intent
            if order.open_qty == ZERO:
                through_stop = bid_d <= intent.stop if intent.side == "long" else ask_d >= intent.stop
                if through_stop:
                    events.append(self._cancel_entry(order, "stop_before_entry", now))
                    continue
            marketable = self._marketable(intent, bid_d, ask_d)
            liquidity = "maker" if self._resting[order.order_id] else "taker"
            if marketable and liquidity == "taker" and intent.require_maker:
                events.append(self._cancel_entry(order, "would_take_liquidity", now, status="rejected"))
                continue
            if marketable:
                qty = take_qty(order.remaining_qty, intent.lot_size)
                if qty > ZERO:
                    events.append(self._fill_entry(order, qty, bid_d, ask_d, liquidity, now))
            self._resting[order.order_id] = True
        return events

    def close(
        self, order_id: str, price: object, *, qty: object | None = None,
        reason: str = "manual", timestamp_ms: int | None = None,
    ) -> dict:
        """Simulate an adverse-slippage taker close and cancel entry leftovers."""
        order = self.get_order(order_id)
        quote = _decimal(price, "price", positive=True)
        if order.open_qty == ZERO:
            raise ValueError("Paper order has no open position")
        size = order.open_qty if qty is None else _decimal(qty, "qty", positive=True)
        if size > order.open_qty or size % order.intent.lot_size:
            raise ValueError("Close qty must align with lot_size and not exceed open_qty")
        now = self._now() if timestamp_ms is None else _timestamp(timestamp_ms)
        if now < order.last_update_ms or (self._last_quote is not None and now < self._last_quote[2]):
            raise ValueError("Cannot close using an out-of-order timestamp")
        fill_price = self._aggressive_price(order.intent, quote, buy=order.intent.side == "short")
        if order.remaining_qty > ZERO:
            self._cancel_entry(order, f"{reason}_close", now)
        return self._fill_exit(order, size, fill_price, reason, "taker", now)
