"""One controlled, GET-only activation of this prepared DEMO generation.

This does not start a service, create execution objects, admit an order intent,
or clear any predecessor state. A receipt followed by the final exact STOP
removal records activation; every interrupted intermediate state stays blocked.
The public entry has fixed production dependencies. Injectable orchestration is
private and exists for offline protocol tests, never as a snapshot-import API.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
import uuid

from . import demo_successor_gate as G


ROOT = Path(__file__).resolve().parents[1]
MAX_ACCOUNT_AGE_MS = 5000
MAX_SNAPSHOT_WINDOW_MS = 10000


class ActivationRefused(ValueError):
    """Fixed local refusal codes; never forward account or exception text."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _require(condition, code):
    if not condition:
        raise ActivationRefused(code)


def _now():
    return time.time_ns() // 1_000_000


def _integer(value):
    _require(type(value) is int and 0 < value < 2**63, "ACTIVATION_CLOCK_INVALID")
    return value


@dataclass(frozen=True, slots=True)
class ActivationResult:
    generation_id: str
    target_root: Path
    target_commit: str
    receipt_sha256: str
    activated_at_ms: int
    service_started: bool = field(default=False, init=False)
    orders_sent: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class _Collection:
    risk: dict = field(repr=False)
    started_ms: int
    completed_ms: int
    started_monotonic_ns: int
    completed_monotonic_ns: int


def _cache_storage(root, prepared, authorization, *, initial):
    """Require the preserved baseline in a plain, existing target namespace.

    This checks local storage provenance, never current account admission. No
    directory, binding or baseline is bootstrapped. A failed collection that
    changes this cache consequently needs separately reviewed continuation.
    """
    from . import demo_halted_resolution as R
    try:
        _require(G._hash(prepared.account_hash), "ACTIVATION_CACHE_INVALID")
        cache = G._plain(root / "state/demo-auto/verified-risk-v2", directory=True)
        inventory = G._inventory(cache)  # Every file and ancestor, including empty directories.
        if initial:
            archive = G._plain(root / G.RECOVERY / "source/state/demo-auto/verified-risk-v2", directory=True)
            _require(inventory == G._inventory(archive), "ACTIVATION_CACHE_CHANGED")
        directory = G._plain(cache / prepared.account_hash, directory=True)
        normalized = {key: authorization[key] for key in
                      ("environment", "account_hash", "authorization_recorded_at_ms", "source", "scope")}
        binding = R._json(G._read(directory / "authorization-binding.json"))
        baseline = R._json(G._read(directory / "verified.json"))
        _require(R.canonical_json(binding) == R.canonical_json(normalized)
                 and type(baseline) is dict and type(baseline.get("schema")) is int
                 and baseline["schema"] == 2 and baseline.get("environment") == "DEMO"
                 and baseline.get("account_hash") == prepared.account_hash
                 and baseline.get("authorization_digest") == G._sha(R.canonical_json(normalized))
                 and baseline.get("baseline_source") == "complete-ledger-flat-boundary-reconstruction"
                 and type(baseline.get("anchor")) is dict and bool(baseline["anchor"])
                 and all(type(baseline.get(key)) is int and baseline[key] > 0
                         for key in ("day_start_ms", "week_start_ms")),
                 "ACTIVATION_CACHE_BASELINE_REQUIRED")
        for key in ("day_start_equity", "week_start_equity"):
            R._number(baseline.get(key), positive=True)
    except ActivationRefused:
        raise
    except Exception:
        raise ActivationRefused("ACTIVATION_CACHE_INVALID") from None


class _ProductionCollector:
    """Fixed ordinary provider and immutable market evidence, no write client."""
    def __init__(self, root, prepared, proxy_url, clock, monotonic_ns):
        self.root, self.prepared = root, prepared
        self.proxy_url, self.clock, self.monotonic_ns = proxy_url, clock, monotonic_ns
        self.pool = None

    def __enter__(self):
        # Construction occurs only after local verification and both locks.
        from .demo_halted_resolution import _json
        from .demo_get_pool import DemoGetPool
        from .demo_transport import DemoReadOnlyClient, load_demo_credentials
        from .demo_verified_risk import VerifiedDemoRiskProvider
        from scripts.demo_auto_run import ServiceEntryEvidence, service_quote
        self.config = _json(G._read(self.root / "config.json"))
        _require(self.config.get("account", {}).get("signal_creates_position") is False,
                 "ACTIVATION_PAPER_POSITION_SETTING_INVALID")
        self.authorization = _json(G._read(self.root / "state/demo-auto/auto-authorization.json"))
        _require(self.authorization.get("account_hash") == self.prepared.account_hash,
                 "ACTIVATION_ACCOUNT_MISMATCH")
        _cache_storage(self.root, self.prepared, self.authorization, initial=True)
        credentials = load_demo_credentials()
        try:
            self.pool = DemoGetPool(proxy_url=self.proxy_url)
            self.client = DemoReadOnlyClient(credentials, http_get=self.pool.get, network_retries=0)
            self.provider = VerifiedDemoRiskProvider(self.client, self.config,
                self.root / "state/demo-auto/verified-risk-v2", authorization=self.authorization,
                router_journal_path=self.root / "state/demo-auto/router.sqlite3", clock=self.clock,
                diagnostic_halted_ledger=False)
            self.evidence = ServiceEntryEvidence(self.config,
                lambda: service_quote(self.client, clock=self.clock), clock=self.clock)
            return self
        except BaseException:
            if self.pool is not None:
                self.pool.close()
            raise

    def __exit__(self, *_):
        # Closing is a prerequisite for STOP removal, never a post-activation
        # operation that could leave an active generation after cleanup failed.
        if self.pool is not None:
            self.pool.close()

    def collect(self):
        from .collect import collect
        from scripts.demo_auto_run import DEMO_CALENDAR_REFRESH_SECONDS
        snapshot = collect(self.config, calendar_max_age_s=DEMO_CALENDAR_REFRESH_SECONDS)
        self.evidence.update_scan(snapshot)
        context = self.refresh_market()
        begin, begin_mono = self.clock(), self.monotonic_ns()
        risk = self.provider.snapshot(context)  # Exactly one ordinary collection.
        end_mono, end = self.monotonic_ns(), self.clock()
        return _Collection(deepcopy(risk), begin, end, begin_mono, end_mono)

    def refresh_market(self):
        context = self.evidence.fresh_context()
        _require(context.get("data_fresh") is True and context.get("macro_blackout") is False,
                 "ACTIVATION_MARKET_REFUSED")
        self.quote_stamp = _integer(context.get("observed_at_ms"))
        self.assert_local_current(self.clock())
        return context

    def assert_local_current(self, now):
        _require(0 <= _integer(now)-self.quote_stamp <= MAX_ACCOUNT_AGE_MS,
                 "ACTIVATION_QUOTE_STALE")
        self.evidence.assert_entry_current(now)


def _check_collection(collection, prepared, authorization_at_ms, now, monotonic_ns):
    _require(type(collection) is _Collection and type(collection.risk) is dict,
             "ACTIVATION_COLLECTION_INVALID")
    risk = collection.risk
    _require(risk.get("environment") == "DEMO" and risk.get("account_hash") == prepared.account_hash
             and risk.get("risk_adapter") == "verified-demo-ledger-v2"
             and risk.get("diagnostic_only", False) is False
             and risk.get("live_verified") is False
             and risk.get("halted") is False,
             "ACTIVATION_ORDINARY_PROVIDER_REQUIRED")
    required = ("demo_verified", "ledger_verified", "baseline_trusted", "history_complete",
                "pause_evidence_verified", "account_snapshot_consistent", "data_fresh")
    _require(all(risk.get(key) is True for key in required)
             and risk.get("refusal_reasons") == []
             and all(risk.get(key) is False for key in
                     ("macro_blackout", "daily_loss_hit", "weekly_loss_hit", "paused"))
             and all(type(risk.get(key)) is list and not risk[key] for key in
                     ("open_positions", "pending_orders", "protection_orders")),
             "ACTIVATION_ACCOUNT_OR_RISK_REFUSED")
    _require(risk.get("authorization_recorded_at_ms") == authorization_at_ms
             and type(risk.get("authorization_recorded_at_ms")) is int,
             "ACTIVATION_AUTHORIZATION_CHANGED")
    observed, completed = (_integer(risk.get(key)) for key in ("observed_at_ms", "collection_completed_ms"))
    window_start, window_end = (_integer(risk.get(key)) for key in
                               ("snapshot_window_start_ms", "snapshot_window_end_ms"))
    begin, end = _integer(collection.started_ms), _integer(collection.completed_ms)
    current = _integer(now)
    _require(begin <= completed <= end <= current and begin <= observed <= completed
             and 0 <= current-observed <= MAX_ACCOUNT_AGE_MS
             and 0 <= end-begin <= MAX_SNAPSHOT_WINDOW_MS
             and 0 <= window_end-window_start <= MAX_SNAPSHOT_WINDOW_MS,
             "ACTIVATION_ACCOUNT_STALE")
    first, last, current_mono = (collection.started_monotonic_ns,
                               collection.completed_monotonic_ns, monotonic_ns)
    _require(all(type(value) is int and value >= 0 for value in (first,last,current_mono))
             and first <= last <= current_mono
             and last-first <= MAX_SNAPSHOT_WINDOW_MS * 1_000_000
             # Provider invocation precedes its first account GET. This strict
             # upper bound also covers a wall-clock rollback during collection.
             and current_mono-first <= MAX_ACCOUNT_AGE_MS * 1_000_000
             and current_mono-last <= (MAX_ACCOUNT_AGE_MS-(completed-observed))*1_000_000,
             "ACTIVATION_MONOTONIC_STALE")
    from .demo_risk import _boundaries
    _require((risk.get("day_start_ms"),risk.get("week_start_ms")) == _boundaries(current),
             "ACTIVATION_BOUNDARY_CHANGED")


def _unactivated(root):
    receipt = root / G.RECOVERY / "activation.json"
    _require(not receipt.exists() and not receipt.is_symlink(), "ACTIVATION_ALREADY_RECORDED")
    stop = root / "state/demo-auto/STOP_NEW_ENTRIES"
    _require(G._read(stop) == G.PREPARATION_STOP, "ACTIVATION_PREPARATION_STOP_REQUIRED")


def _publish_receipt(root, raw):
    directory = G._plain(root / G.RECOVERY, directory=True)
    destination = directory / "activation.json"
    temporary = directory / (".activation-"+uuid.uuid4().hex+".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        # Same-directory link is no-overwrite and atomic. No replace fallback.
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _finish_stop(root, assert_current):
    """Commit by removing the exact preparation flag, preserving operator STOP.

The unexpected pending name itself blocks the common gate during finalization
and after a crash. Windows rename is atomic and refuses an existing destination.
A failed final check restores STOP without overwriting a concurrent operator
flag; if restoration fails, the pending name remains an execution barrier.
    """
    _require(os.name == "nt", "ACTIVATION_EXCLUSIVE_RENAME_REQUIRED")
    stop = root / "state/demo-auto/STOP_NEW_ENTRIES"
    pending = root / G.RECOVERY / "activation-stop.pending"
    _require(not pending.exists() and not pending.is_symlink(), "ACTIVATION_FINALIZATION_EXISTS")
    _require(G._read(stop) == G.PREPARATION_STOP, "ACTIVATION_PREPARATION_STOP_CHANGED")
    try:
        stop.rename(pending)
        _require(G._read(pending) == G.PREPARATION_STOP, "ACTIVATION_PREPARATION_STOP_CHANGED")
        assert_current()
        pending.unlink()
    except BaseException:
        if pending.exists() and not stop.exists():
            try:
                pending.rename(stop)
            except OSError:
                pass  # The pending name still makes common activation refuse.
        raise


def _activate(root, expected_commit, *, collector_factory, clock, monotonic_ns):
    root = Path(root)
    prepared = G.assert_prepared_generation(root, expected_commit)
    _unactivated(root)
    from .demo_halted_resolution import _json
    with G.owned_process_locks((prepared.source_root, root)):
        locked = G.assert_prepared_generation(root, expected_commit)
        _require(locked == prepared, "ACTIVATION_PREPARATION_CHANGED")
        _unactivated(root)
        authorization = _json(G._read(root / "state/demo-auto/auto-authorization.json"))
        authorization_at_ms = _integer(authorization.get("authorization_recorded_at_ms"))
        _cache_storage(root, prepared, authorization, initial=True)
        with collector_factory(root, prepared, clock, monotonic_ns) as collector:
            collection = collector.collect()
            _check_collection(collection, prepared, authorization_at_ms, clock(), monotonic_ns())
            collector.refresh_market()
            _check_collection(collection, prepared, authorization_at_ms, clock(), monotonic_ns())
        # All network and pool cleanup precede this last complete local gate.
        # Its real elapsed time counts against the unchanged five-second age;
        # no cached success or reduced inventory substitutes for the full gate.
        _require(G.assert_prepared_generation(root, expected_commit) == prepared,
                 "ACTIVATION_PREPARATION_CHANGED")
        _cache_storage(root, prepared, authorization, initial=False)
        collector.assert_local_current(clock())
        _check_collection(collection, prepared, authorization_at_ms, clock(), monotonic_ns())
        _unactivated(root)
        activated_at = _integer(clock())
        receipt = dict(schema=1, environment="DEMO", phase="ACTIVATED",
            generation_id=prepared.generation_id, target_root=str(root),
            target_commit=prepared.target_commit, prepared_sha256=prepared.prepared_sha256,
            activated_at_ms=activated_at, ordinary_provider_verified=True, diagnostic_only=False)
        _require(set(receipt) == G.ACTIVATION_KEYS, "ACTIVATION_RECEIPT_INVALID")
        raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("ascii")
        _publish_receipt(root, raw)
        _require(G._read(root / G.RECOVERY / "activation.json") == raw,
                 "ACTIVATION_RECEIPT_CHANGED")
        def assert_current():
            collector.assert_local_current(clock())
            _check_collection(collection, prepared, authorization_at_ms, clock(), monotonic_ns())
        # Last step: neither an operator STOP nor any predecessor flag is removed.
        _finish_stop(root, assert_current)
        return ActivationResult(prepared.generation_id, root, prepared.target_commit, G._sha(raw), activated_at)


def activate_generation(root, expected_commit, *, proxy_url=None):
    """Activate only this frozen deployment with one fresh ordinary collection.

No executable service is started and no order intent is approved. An operator
orchestrator must bound the process lifetime independently (maximum 120 s).
An interrupted receipt/STOP combination intentionally refuses automatic retry.
    """
    try:
        _require(Path(root) == ROOT, "ACTIVATION_OWN_DEPLOYMENT_REQUIRED")
        return _activate(root, expected_commit,
            collector_factory=lambda root, prepared, clock, mono:
                _ProductionCollector(root, prepared, proxy_url, clock, mono),
            clock=_now, monotonic_ns=time.monotonic_ns)
    except ActivationRefused:
        raise
    except G.SuccessorGateRefused as error:
        raise ActivationRefused(error.code) from None
    except Exception:
        raise ActivationRefused("ACTIVATION_UNVERIFIED") from None
