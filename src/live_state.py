# -*- coding: utf-8 -*-
"""实时风控状态提供器（隔离副本；只读、fail-closed）。

`live_state_provider(cfg, evidence=None) -> dict`

返回 canonical 字段：
    now_ms, equity_estimate, day_start_equity, realized_pnl_today,
    entries_today, open_positions, pending_orders, data_fresh,
    macro_blackout, halted, live_verified, observed_at_ms
附加：protection_orders、evidence、refusal_reasons、day_start_ms、clock_skew_ms。

版本 v2（Codex 复核后收紧）：
  - 默认 read_only_evidence 采集器标记 adapter_verified=False，provider 直接给
    ADAPTER_UNVERIFIED 拒绝；只有显式注入的离线 Evidence（fixture）才会 verified。
  - fills 盈亏字段用 `fillPnl`；固定 begin/end、after=billId 严格递减；
    按 tradeId/billId 去重，冲突重复或字段缺失拒绝，终端页证明窗口耗尽。
    默认采集完整 UTC 本周；adapter_verified 始终 False，不代表周亏已接通。
  - 成交归属必须来自 journal 订单映射（known_orders）；h/r 字符串样式不作为 bot 依据。
  - 基线必须是当日 UTC 00:00 附近、来源可信的快照。
  - 拒绝未来时间、NaN/Inf、非布尔宏观；macro 缺 blackout 不得当 False。
  - 资金费必须币种/品种归属且分页完整，否则拒绝。
"""
from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

INST = "SOL-USDT-SWAP"
ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "state" / "live_state_baseline.json"
LATEST_STATE_PATH = ROOT / "state" / "latest.json"
HALT_PATH = ROOT / "state" / "HALT"
JOURNAL_PATH = ROOT / "logs" / "router" / "journal.sqlite3"

TRUSTED_BASELINE_SOURCES = {"t4_reconcile", "reconcile_flat", "live_state_bootstrap"}
BASELINE_NEAR_START_MS = 15 * 60 * 1000       # 基线必须落在 00:00–00:15 UTC
ACCOUNT_STALE_MS = 60_000
FILLS_STALE_MS = 60_000
FUNDING_STALE_MS = 60_000
MACRO_STALE_MS = 1_800_000
MAX_CLOCK_SKEW_MS = 2_000
MAX_FUTURE_SKEW_MS = 2_000
ROLE_ENTRY = "bot_entry"
ROLE_EXIT = "bot_exit"
HISTORY_PAGE_LIMIT = 100
HISTORY_MAX_PAGES = 20


# --------------------------------------------------------------------------- 证据模型


@dataclass(frozen=True)
class Position:
    inst_id: str
    pos_side: str
    qty: float
    avg_px: float
    lever: float
    mgn_mode: str


@dataclass(frozen=True)
class OrderRef:
    inst_id: str
    side: str
    sz: float
    px: float
    state: str
    cl_ord_id: str
    ord_id: str
    ord_type: str
    pos_side: str


@dataclass(frozen=True)
class Fill:
    ts_ms: int
    trade_id: str
    cl_ord_id: str
    algo_cl_ord_id: str
    inst_id: str
    pnl: float
    fee: float


@dataclass(frozen=True)
class FundingBill:
    ts_ms: int
    bill_id: str
    amount: float
    ccy: str
    inst_id: str


@dataclass(frozen=True)
class AccountSnapshot:
    fetched_at_ms: int
    equity: float
    positions: tuple[Position, ...]
    pending_orders: tuple[OrderRef, ...]
    protection_orders: tuple[OrderRef, ...]


@dataclass(frozen=True)
class FillsEvidence:
    fetched_at_ms: int
    fills: tuple[Fill, ...]
    coverage_from_ms: int
    coverage_to_ms: int
    complete: bool
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class FundingEvidence:
    fetched_at_ms: int
    bills: tuple[FundingBill, ...]
    coverage_from_ms: int
    coverage_to_ms: int
    complete: bool
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class MacroState:
    observed_at_ms: int
    blackout: bool | None
    source: str


@dataclass(frozen=True)
class Baseline:
    date: str
    equity: float
    observed_at_ms: int
    source: str


@dataclass(frozen=True)
class Evidence:
    clock_ms: int
    server_time_ms: int
    fetched_at_ms: int
    account: AccountSnapshot
    fills: FillsEvidence
    funding: FundingEvidence
    macro: MacroState
    baseline: Baseline | None
    known_orders: dict[str, str]                    # journal: order id -> bot_entry/bot_exit
    halted: bool = False
    adapter_verified: bool = False                  # 采集器自证：未完成集成必须 False


# --------------------------------------------------------------------------- 工具


def _finite(value) -> float | None:
    if isinstance(value, bool):          # bool 是 int 子类，必须显式拒绝
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _day_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()


def _day_start_ms(ms: int) -> int:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)


def _week_start_ms(ms: int) -> int:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int((day - timedelta(days=day.weekday())).timestamp() * 1000)


def _attribution(fill: Fill, known_orders: dict[str, str]) -> str | None:
    key = fill.cl_ord_id or fill.algo_cl_ord_id
    if not key:
        return None
    role = known_orders.get(key)
    if role in (ROLE_ENTRY, ROLE_EXIT, "external"):
        return role
    if role is not None:
        return "invalid"
    return None


def _canonical_order(o: OrderRef) -> dict:
    return {"inst_id": o.inst_id, "side": o.side, "sz": o.sz, "px": o.px,
            "state": o.state, "cl_ord_id": o.cl_ord_id, "ord_id": o.ord_id,
            "ord_type": o.ord_type, "pos_side": o.pos_side}


def _canonical_position(p: Position) -> dict:
    return {"inst_id": p.inst_id, "pos_side": p.pos_side, "qty": p.qty,
            "avg_px": p.avg_px, "lever": p.lever, "mgn_mode": p.mgn_mode}


def _refusal(now_ms: int, reasons: list[str], evidence: dict | None = None) -> dict:
    return {
        "now_ms": int(now_ms), "observed_at_ms": int(now_ms),
        "equity_estimate": None, "day_start_equity": None,
        "realized_pnl_today": None, "entries_today": 0,
        "open_positions": [], "pending_orders": [], "protection_orders": [],
        "data_fresh": False, "macro_blackout": None, "halted": True,
        "live_verified": False, "refusal_reasons": reasons,
        "day_start_ms": _day_start_ms(int(now_ms)),
        "clock_skew_ms": None, "evidence": evidence or {},
    }


# --------------------------------------------------------------------------- 核心提供器


def live_state_provider(cfg: dict | None = None, evidence: Evidence | None = None) -> dict:
    if evidence is None:
        try:
            evidence = read_only_evidence(cfg)
        except Exception:                                       # noqa: BLE001
            return _refusal(0, ["EVIDENCE_UNAVAILABLE"])

    ev = evidence
    now = int(ev.clock_ms)
    day = _day_utc(now)
    day_start = _day_start_ms(now)
    reasons: list[str] = []
    skew = abs(int(ev.server_time_ms) - now)

    # 0) 采集器未验收：默认只读路径一律拒绝（必须是严格 True）
    if ev.adapter_verified is not True:
        reasons.append("ADAPTER_UNVERIFIED")

    # 1) 时钟与未来时间
    if skew > MAX_CLOCK_SKEW_MS:
        reasons.append("CLOCK_SKEW")
    for ts in (ev.server_time_ms, ev.fetched_at_ms, ev.account.fetched_at_ms,
               ev.fills.fetched_at_ms, ev.fills.coverage_to_ms,
               ev.funding.fetched_at_ms, ev.funding.coverage_to_ms,
               ev.macro.observed_at_ms):
        if int(ts) > now + MAX_FUTURE_SKEW_MS:
            reasons.append("FUTURE_TIMESTAMP")
            break
    if ev.baseline is not None and ev.baseline.observed_at_ms > now + MAX_FUTURE_SKEW_MS:
        reasons.append("FUTURE_TIMESTAMP")
    # 未来时间必须覆盖每一笔成交/账单的时间戳（不能只查聚合时间）
    future_limit = now + MAX_FUTURE_SKEW_MS
    if "FUTURE_TIMESTAMP" not in reasons:
        if any(int(f.ts_ms) > future_limit for f in ev.fills.fills) or any(
                int(b.ts_ms) > future_limit for b in ev.funding.bills):
            reasons.append("FUTURE_TIMESTAMP")

    # 2) 新鲜度
    if now - int(ev.account.fetched_at_ms) > ACCOUNT_STALE_MS:
        reasons.append("STALE_ACCOUNT")
    if now - int(ev.fills.fetched_at_ms) > FILLS_STALE_MS:
        reasons.append("STALE_FILLS")
    if now - int(ev.funding.fetched_at_ms) > FUNDING_STALE_MS:
        reasons.append("STALE_FUNDING")

    # 3) 权益（有限且为正）
    equity = _finite(ev.account.equity)
    if equity is None:
        reasons.append("NON_FINITE_VALUE")
        equity = 0.0
    elif equity <= 0:
        reasons.append("EQUITY_INVALID")

    # 4) 基线：当日 UTC 00:00 附近 + 可信来源
    baseline = ev.baseline
    day_start_equity = None
    if baseline is None:
        reasons.append("BASELINE_MISSING")
    else:
        if baseline.date != day:
            reasons.append("BASELINE_STALE")
        if not (day_start <= baseline.observed_at_ms <= day_start + BASELINE_NEAR_START_MS):
            reasons.append("BASELINE_NOT_NEAR_DAY_START")
        if baseline.source not in TRUSTED_BASELINE_SOURCES:
            reasons.append("BASELINE_UNTRUSTED")
        beq = _finite(baseline.equity)
        if beq is None:
            reasons.append("NON_FINITE_VALUE")
        elif beq <= 0:
            reasons.append("BASELINE_INVALID")
        else:
            day_start_equity = beq

    # 5) 成交：覆盖完整 + 去重 + 归属来自 journal 映射
    fills = ev.fills
    used = tuple(f for f in fills.fills if f.ts_ms >= day_start)
    if fills.complete is not True or fills.coverage_from_ms > day_start \
            or fills.coverage_to_ms < now - FILLS_STALE_MS:
        reasons.append("FILLS_INCOMPLETE")
    trade_ids = [f.trade_id for f in used if f.trade_id]
    if len(trade_ids) != len(set(trade_ids)):
        reasons.append("FILL_DUPLICATED")
    realized = 0.0
    entries: set[str] = set()
    for f in used:
        if f.inst_id != INST:
            reasons.append("FILL_INSTRUMENT_MISMATCH")
            continue
        pnl = _finite(f.pnl)
        fee = _finite(f.fee)
        if pnl is None or fee is None:
            reasons.append("NON_FINITE_VALUE")
            continue
        role = _attribution(f, ev.known_orders)
        if role == "external":
            reasons.append("UNKNOWN_EXTERNAL_TRADE")
        elif role == "invalid":
            reasons.append("ATTRIBUTION_INVALID")
        elif role is None:
            reasons.append("ATTRIBUTION_UNVERIFIED")
        else:
            realized += pnl + fee
            if role == ROLE_ENTRY and f.cl_ord_id:
                entries.add(f.cl_ord_id)

    # 6) 资金费：分页完整 + 币种/品种归属
    funding = ev.funding
    if (funding.complete is not True or funding.coverage_from_ms > day_start
            or funding.coverage_to_ms < now - FUNDING_STALE_MS):
        reasons.append("FUNDING_INCOMPLETE")
    bill_ids = [b.bill_id for b in funding.bills if b.ts_ms >= day_start and b.bill_id]
    if len(bill_ids) != len(set(bill_ids)):
        reasons.append("BILL_DUPLICATED")
    for b in funding.bills:
        if b.ts_ms < day_start:
            continue
        amt = _finite(b.amount)
        if amt is None:
            reasons.append("NON_FINITE_VALUE")
            continue
        if str(b.ccy or "") != "USDT" or str(b.inst_id or "") not in ("", INST):
            reasons.append("FUNDING_ATTRIBUTION_UNVERIFIED")
            continue
        realized += amt

    # 7) 宏观：必须是严格布尔，未知/非布尔拒绝（绝不当作 False）
    macro = ev.macro
    if not isinstance(macro.blackout, bool) or not macro.source:
        reasons.append("MACRO_UNAVAILABLE")
    elif now - int(macro.observed_at_ms) > MACRO_STALE_MS:
        reasons.append("STALE_MACRO")

    # 8) 熔断
    halted = bool(ev.halted)
    if halted:
        reasons.append("HALTED")

    data_fresh = not any(c.startswith("STALE_") for c in reasons) and "CLOCK_SKEW" not in reasons
    live_verified = not reasons

    return {
        "now_ms": now,
        "observed_at_ms": int(ev.fetched_at_ms),
        "day_start_ms": day_start,
        "equity_estimate": round(equity, 8),
        "day_start_equity": round(day_start_equity, 8) if day_start_equity is not None else None,
        "realized_pnl_today": round(realized, 8),
        "entries_today": len(entries),
        "open_positions": [_canonical_position(p) for p in ev.account.positions],
        "pending_orders": [_canonical_order(o) for o in ev.account.pending_orders],
        "protection_orders": [_canonical_order(o) for o in ev.account.protection_orders],
        "data_fresh": data_fresh,
        "macro_blackout": macro.blackout if isinstance(macro.blackout, bool) else None,
        "halted": halted,
        "live_verified": live_verified,
        "refusal_reasons": reasons,
        "clock_skew_ms": skew,
        "evidence": {
            "adapter_verified": bool(ev.adapter_verified),
            "account_fetched_at_ms": int(ev.account.fetched_at_ms),
            "fills_fetched_at_ms": int(ev.fills.fetched_at_ms),
            "funding_fetched_at_ms": int(ev.funding.fetched_at_ms),
            "macro_observed_at_ms": int(macro.observed_at_ms),
            "macro_source": macro.source,
            "baseline": ({"date": baseline.date, "equity": baseline.equity,
                          "source": baseline.source} if baseline else None),
            "fills_count": len(used),
            "known_orders": len(ev.known_orders),
            "coverage_ms": [int(fills.coverage_from_ms), int(fills.coverage_to_ms)],
            "fills_errors": list(fills.errors),
            "funding_errors": list(funding.errors),
        },
    }


# --------------------------------------------------------------------------- 只读采集器（未验收）


class _ReadOnlyGateway:
    def __init__(self, get=None):
        self._get = get

    def __call__(self, method: str, path: str, params: dict | None = None):
        if str(method).upper() != "GET":
            raise RuntimeError("live_state 只允许只读 GET")
        if self._get is None:
            from . import okx_private as P                  # 延迟导入
            return P.private("GET", path, params or {})
        return self._get(path, params or {})


def _required_text(row: dict, name: str) -> str:
    value = row.get(name)
    if value is None or isinstance(value, bool) or not str(value).strip():
        raise ValueError("MISSING_FIELD")
    return str(value)


def _required_timestamp(row: dict, *names: str) -> int:
    name = next((key for key in names if row.get(key) not in (None, "")), names[0])
    text = _required_text(row, name)
    if not text.isascii() or not text.isdecimal() or int(text) <= 0:
        raise ValueError("INVALID_TIMESTAMP")
    return int(text)


def _required_amount(row: dict, name: str) -> float:
    value = _finite(_required_text(row, name))
    if value is None:
        raise ValueError("INVALID_AMOUNT")
    return value


def _unique_rows(rows: list[dict], key_name: str) -> list[dict]:
    seen: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("INVALID_ROW")
        key = _required_text(row, key_name)
        if key in seen and seen[key] != row:
            raise ValueError("CONFLICTING_DUPLICATE")
        seen[key] = dict(row)
    return list(seen.values())


def _parse_fills(rows: list[dict]) -> tuple[Fill, ...]:
    """Strict accounting: missing PnL/fee/currency/instrument is never zero."""
    parsed = []
    for row in _unique_rows(rows, "tradeId"):
        if _required_text(row, "instId") != INST:
            raise ValueError("INSTRUMENT_MISMATCH")
        if _required_text(row, "feeCcy") != "USDT":
            raise ValueError("FEE_CURRENCY_UNVERIFIED")
        parsed.append(Fill(
            ts_ms=_required_timestamp(row, "fillTime", "ts"),
            trade_id=_required_text(row, "tradeId"),
            cl_ord_id=str(row.get("clOrdId") or ""),
            algo_cl_ord_id=str(row.get("algoClOrdId") or ""), inst_id=INST,
            pnl=_required_amount(row, "fillPnl"), fee=_required_amount(row, "fee")))
    return tuple(parsed)


def _parse_bills(rows: list[dict]) -> tuple[FundingBill, ...]:
    parsed = []
    for row in _unique_rows(rows, "billId"):
        if _required_text(row, "ccy") != "USDT" or _required_text(row, "instId") != INST:
            raise ValueError("FUNDING_ATTRIBUTION_UNVERIFIED")
        parsed.append(FundingBill(
            ts_ms=_required_timestamp(row, "ts"), bill_id=_required_text(row, "billId"),
            amount=_required_amount(row, "balChg"), ccy="USDT", inst_id=INST))
    return tuple(parsed)


def _collect_history(gateway, path: str, start: int, end: int, *, fills: bool):
    """Exhaust a fixed [start,end] query; a cursor must move to older billIds.

    An empty/short final page proves an empty remainder *within the fixed window*.
    Page caps, failures and conflicting duplicates preserve partial records but
    never claim complete evidence. No response bodies or exception text escape.
    """
    rows: dict[str, dict] = {}
    bill_rows: dict[str, dict] = {}
    cursor = None
    key_name = "tradeId" if fills else "billId"
    if not (isinstance(start, int) and isinstance(end, int) and 0 < start <= end):
        return [], False, ("INVALID_WINDOW",)
    for _ in range(HISTORY_MAX_PAGES):
        params = {"instType": "SWAP", "limit": str(HISTORY_PAGE_LIMIT),
                  "begin": str(start), "end": str(end)}
        if fills:
            params["instId"] = INST
        if cursor is not None:
            params["after"] = str(cursor)
        try:
            page = gateway("GET", path, params)
        except Exception:
            return list(rows.values()), False, ("HISTORY_READ_FAILED",)
        if not isinstance(page, list) or len(page) > HISTORY_PAGE_LIMIT:
            return list(rows.values()), False, ("INVALID_PAGE",)
        try:
            bills = []
            for row in page:
                if not isinstance(row, dict):
                    raise ValueError("INVALID_ROW")
                bill_text = _required_text(row, "billId")
                if not bill_text.isascii() or not bill_text.isdecimal() or int(bill_text) <= 0:
                    raise ValueError("INVALID_CURSOR")
                if cursor is not None and int(bill_text) >= cursor:
                    raise ValueError("CURSOR_BOUNDARY_VIOLATION")
                bills.append(int(bill_text))
                if bill_text in bill_rows and bill_rows[bill_text] != row:
                    raise ValueError("CONFLICTING_DUPLICATE")
                key = _required_text(row, key_name)
                ts = _required_timestamp(row, "fillTime", "ts") if fills else _required_timestamp(row, "ts")
                if not start <= ts <= end:
                    raise ValueError("OUTSIDE_QUERY_WINDOW")
                if key in rows and rows[key] != row:
                    raise ValueError("CONFLICTING_DUPLICATE")
                if fills:
                    _parse_fills([row])
                else:
                    _required_text(row, "type")
                    if str(row["type"]) == "8":
                        _parse_bills([row])
                rows[key] = dict(row)
                bill_rows[bill_text] = dict(row)
            if bills and cursor is not None and min(bills) >= cursor:
                raise ValueError("CURSOR_NO_PROGRESS")
        except (TypeError, ValueError, KeyError):
            return list(rows.values()), False, ("HISTORY_ROW_INVALID",)
        if len(page) < HISTORY_PAGE_LIMIT:
            return list(rows.values()), True, ()
        cursor = min(bills)
    return list(rows.values()), False, ("HISTORY_PAGE_CAP",)


def _collect_fills(gateway, day_start: int, now: int) -> FillsEvidence:
    rows, complete, errors = _collect_history(
        gateway, "/api/v5/trade/fills-history", day_start, now, fills=True)
    try:
        parsed = _parse_fills(rows)
    except (TypeError, ValueError):
        parsed, complete, errors = (), False, ("FILLS_PARSE_FAILED",)
    return FillsEvidence(now, parsed, day_start, now, complete, errors)


def _collect_funding(gateway, day_start: int, now: int) -> FundingEvidence:
    rows, complete, errors = _collect_history(
        gateway, "/api/v5/account/bills", day_start, now, fills=False)
    try:
        parsed = _parse_bills([row for row in rows if str(row.get("type")) == "8"])
    except (TypeError, ValueError):
        parsed, complete, errors = (), False, ("FUNDING_PARSE_FAILED",)
    return FundingEvidence(now, parsed, day_start, now, complete, errors)


def _macro_from_payload(payload: dict) -> MacroState:
    """Validate persisted snapshot schema and original generation time.

    Current trimmed snapshots keep features.macro and facts.macro_blackout;
    snapshot.macro is also supported, but conflicting copies are rejected.
    A recent file write cannot make an old snapshot current.
    """
    unknown = MacroState(observed_at_ms=0, blackout=None, source="")
    try:
        snap = payload["snapshot"]
        facts = snap["facts"]
        value = facts["macro_blackout"]
        candidates = []
        for raw in (snap.get("macro"), (snap.get("features") or {}).get("macro")):
            if raw is not None:
                candidates.append(raw["blackout"])
        if type(value) is not bool or not candidates or any(
                type(item) is not bool or item != value for item in candidates):
            return unknown
        generated = _required_timestamp(snap, "generated_at_ms")
        updated = _required_timestamp(payload, "updated_ms")
        if generated > updated:
            return unknown
        return MacroState(generated, value, "state/latest.json:snapshot/facts")
    except (TypeError, KeyError, ValueError, AttributeError):
        return unknown


def _load_known_orders(account: str, journal_path: Path | None = None, *,
                       immutable: bool = False) -> dict[str, str]:
    """从 router journal 读取订单归属映射。

    真实 schema：routes(account, signal, fingerprint, client, status, owner, data)
      - `client` 列 = wire 入场 ID → bot_entry
      - `data` JSON 的 `client_order_id` → bot_entry
      - `data` JSON 的 `protection_id` / `close_id` → bot_exit
    按 account 精确筛选；以 SQLite 只读模式打开；任何失败返回空映射（fail-closed）。
    """
    mapping: dict[str, str] = {}
    path = journal_path or JOURNAL_PATH
    if not account or not path.exists():
        return mapping
    # An explicit external context must never create SQLite WAL/shm sidecars.
    # Immutable reads ignore WAL, so refuse an active/recovery journal instead
    # of silently trusting an uncheckpointed main DB as complete attribution.
    if immutable and any(Path(str(path) + suffix).exists()
                         for suffix in ("-wal", "-shm", "-journal")):
        return mapping
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        if immutable:
            uri += "&immutable=1"
        con = sqlite3.connect(uri, uri=True, timeout=3)
        try:
            rows = con.execute(
                "SELECT client, data FROM routes WHERE account = ?", (account,)).fetchall()
        finally:
            con.close()
    except sqlite3.Error:
        return mapping
    for client, data_json in rows:
        client = str(client or "")
        if client:
            mapping[client] = ROLE_ENTRY
        try:
            data = json.loads(data_json or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        cid = data.get("client_order_id")
        if cid:
            mapping[str(cid)] = ROLE_ENTRY
        for key in ("protection_id", "close_id"):
            value = data.get(key)
            if value:
                mapping[str(value)] = ROLE_EXIT
    return mapping


def read_only_evidence(cfg: dict | None = None, gateway: _ReadOnlyGateway | None = None,
                       now_ms: int | None = None, *,
                       context_root: str | Path | None = None) -> Evidence:
    """Collect GET evidence and read an explicit existing local context, never write.

    None retains the legacy global paths. A context root only selects existing
    baseline/latest/HALT/journal inputs; it cannot approve the adapter or create
    files/directories in that context.
    """
    context = Path(context_root).resolve() if context_root is not None else None
    baseline_path = context / "state" / "live_state_baseline.json" if context else BASELINE_PATH
    latest_path = context / "state" / "latest.json" if context else LATEST_STATE_PATH
    halt_path = context / "state" / "HALT" if context else HALT_PATH
    journal_path = context / "logs" / "router" / "journal.sqlite3" if context else None
    gw = gateway or _ReadOnlyGateway()
    now = int(now_ms if now_ms is not None else datetime.now(tz=timezone.utc).timestamp() * 1000)
    server_ms = int(gw("GET", "/api/v5/public/time")[0]["ts"])
    bal = gw("GET", "/api/v5/account/balance", {"ccy": "USDT"})
    equity = float(bal[0].get("totalEq") or 0)

    raw_pos = gw("GET", "/api/v5/account/positions", {"instType": "SWAP", "instId": INST})
    positions = tuple(Position(inst_id=p.get("instId", INST), pos_side=(p.get("posSide") or "net"),
                               qty=float(p.get("pos") or 0), avg_px=float(p.get("avgPx") or 0),
                               lever=float(p.get("lever") or 0), mgn_mode=p.get("mgnMode") or "cross")
                      for p in raw_pos if abs(float(p.get("pos") or 0)) > 0)
    raw_pend = gw("GET", "/api/v5/trade/orders-pending", {"instType": "SWAP", "instId": INST})
    pending = tuple(OrderRef(inst_id=o.get("instId", INST), side=o.get("side", ""),
                             sz=float(o.get("sz") or 0), px=float(o.get("px") or 0),
                             state=o.get("state", ""), cl_ord_id=o.get("clOrdId", ""),
                             ord_id=o.get("ordId", ""), ord_type=o.get("ordType", ""),
                             pos_side=(o.get("posSide") or "net")) for o in raw_pend)
    protects = []
    for kind in ("oco", "conditional", "trigger"):
        for a in gw("GET", "/api/v5/trade/orders-algo-pending",
                    {"instType": "SWAP", "instId": INST, "ordType": kind}):
            protects.append(OrderRef(inst_id=a.get("instId", INST), side=a.get("side", ""),
                                     sz=float(a.get("sz") or 0), px=float(a.get("px") or 0),
                                     state=a.get("state", ""), cl_ord_id=a.get("algoClOrdId", ""),
                                     ord_id=a.get("algoId", ""), ord_type=a.get("ordType", ""),
                                     pos_side=(a.get("posSide") or "net")))

    # Fetch the whole current UTC week; daily and future weekly accounting use
    # the same fixed evidence window. This does not assert a weekly baseline.
    history_start = _week_start_ms(now)
    fills_ev = _collect_fills(gw, history_start, now)
    funding_ev = _collect_funding(gw, history_start, now)

    macro = MacroState(observed_at_ms=0, blackout=None, source="")
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
        macro = _macro_from_payload(payload)
    except (OSError, ValueError):
        pass

    baseline = None
    try:
        raw = json.loads(baseline_path.read_text(encoding="utf-8"))
        baseline = Baseline(date=str(raw["date"]), equity=float(raw["equity"]),
                            observed_at_ms=int(raw["observed_at_ms"]),
                            source=str(raw.get("source") or ""))
    except (OSError, ValueError, KeyError, TypeError):
        baseline = None

    journal_account = str(((cfg or {}).get("live_state") or {}).get("journal_account") or "T3-live")
    return Evidence(clock_ms=now, server_time_ms=server_ms, fetched_at_ms=now,
                    account=AccountSnapshot(fetched_at_ms=now, equity=equity,
                                            positions=positions, pending_orders=pending,
                                            protection_orders=tuple(protects)),
                    fills=fills_ev, funding=funding_ev, macro=macro, baseline=baseline,
                    known_orders=_load_known_orders(journal_account, journal_path,
                                                   immutable=context is not None),
                    halted=halt_path.exists(), adapter_verified=False)
