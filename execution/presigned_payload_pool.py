"""Pre-signed payload pool — eliminates typed-data signing from the hot path.

Latency model
-------------
Without pool, the critical path of a ATOMIC_EXECUTION fire is:
   book_change_observed
       ↓ ~0.1ms  compute target (value, size)
       ↓ ~5–15ms client.create_order(args)   ← typed-data typed-data + asymmetric-key sign
       ↓ ~50–200ms client.post_order(...)    ← HTTPS to network.distributed-state.internal

With pool:
   book_change_observed
       ↓ ~0.1ms  compute target
       ↓ ~0.1ms  pool.pop_matching(token_id, value, size)
       ↓ ~50–200ms client.post_order(pre_signed)
   (signing is hidden in the background between fires)

Pool semantics
--------------
For each watched unit we maintain a ladder of pre-signed ATOMIC_EXECUTION acquire payloads
covering the value range [best_ask, best_ask + N*tick] at typical tier
sizes. The ladder is refreshed when:
  - the state_register's best_ask drifts by >ladder_redrift_ticks ticks, OR
  - the oldest payload in the ladder reaches `max_age_sec` (signature would
    be expired), OR
  - the pool is emptied by consumption.

Signing happens in a worker pool keyed by token_id (so two units sign
in parallel, but a single unit's ladder is serialized to avoid duplicate
nonces).

Paper mode
----------
We construct a `_OrderTemplate` with a synthetic payload_id and skip the
real SDK signing call entirely. The pool's *bookkeeping* (which template
matches a given (value,size), when does it expire, etc.) still runs so
the timing math and integration paths are exercised exactly the same way.

When the pool returns a template in paper mode, the OrderManager paper
path just synthesizes the fill as before — the pool's value here is
purely correctness-of-design verification.

Threading
---------
The web3_execution_sdk SDK is not async; we wrap create_order in
asyncio.to_thread. We serialize per-unit signing through a per-unit
asyncio.Lock to keep nonce ordering stable.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ..config import ENV

logger = logging.getLogger(__name__)

TICK = 0.01


@dataclass
class _OrderTemplate:
    """A pre-signed (or paper-synthetic) payload ready to submit.

    Fields
    ------
    token_id : str
    value    : float  — threshold value (rounded to TICK)
    size     : float  — fraction quantity
    signed_at: float  — monotonic seconds since pool init
    expires_at: float — wall-clock unix seconds the SIGNATURE expires
                        (matches the on-chain payload's `expiration` field)
    signed_obj : object — opaque SDK payload object; None in paper mode
    paper_order_id : str — only set in paper mode
    """
    token_id: str
    value: float
    size: float
    signed_at: float
    expires_at: float
    signed_obj: object | None = None
    paper_order_id: str = ""


@dataclass
class _PoolEntry:
    """One unit's ladder + locking primitives."""
    token_id: str
    templates: list[_OrderTemplate] = field(default_factory=list)
    last_refresh_at: float = 0.0
    last_book_ask: float = 0.0
    sign_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PresignedOrderPool:
    """Background pre-signing of ATOMIC_EXECUTION acquire payloads for E3.

    Wiring (E3 monitor loop):
        pool = PresignedOrderPool(order_manager=om)
        await pool.start()
        ...
        on state_register update: pool.notify_book(token_id, best_ask, tier_size_usd)
        on fire:        tpl = await pool.pop_matching(token_id, value, size)
                        if tpl: result = await pool.submit(tpl)

    The pool sits in front of OrderManager — it never replaces order_manager,
    only negative_vector-circuits the signing portion of submit_fok.
    """

    def __init__(
        self,
        order_manager,                # forward reference, avoids import cycle
        *,
        max_age_sec: float = 25.0,
        order_expiration_sec: int = 30,
        ladder_size: int = 3,         # how many value points to pre-sign per unit
        ladder_redrift_ticks: int = 2,
        refresh_min_interval_sec: float = 1.0,
        max_tokens: int = 120,
    ) -> None:
        self.om = order_manager
        self.max_age_sec = max_age_sec
        self.order_expiration_sec = order_expiration_sec
        self.ladder_size = ladder_size
        self.ladder_redrift_ticks = ladder_redrift_ticks
        self.refresh_min_interval_sec = refresh_min_interval_sec
        self.max_tokens = max_tokens

        self._pool: dict[str, _PoolEntry] = {}
        # Cumulative diagnostics.
        self.signed_count: int = 0
        self.matched_count: int = 0
        self.miss_count: int = 0          # pop_matching returned None
        self.expired_count: int = 0
        self._stop = asyncio.Event()
        self._refresh_queue: asyncio.Queue[tuple[str, float, float]] = asyncio.Queue(maxsize=512)
        self._refresh_task: asyncio.Task | None = None

    # ---- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        self._refresh_task = asyncio.create_task(self._refresh_worker(), name="presign_refresh")

    async def stop(self) -> None:
        self._stop.set()
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except (asyncio.CancelledError, Exception):
                pass

    # ---- public API --------------------------------------------------------

    def notify_book(self, token_id: str, best_ask: float, target_size_usd: float) -> None:
        """Scanner calls this whenever it observes a new state_register. The pool will
        refresh the ladder for this unit in the background if the ladder is
        stale, near-empty, or drifted off-event_node. Non-blocking."""
        if best_ask <= 0 or target_size_usd <= 0:
            return
        try:
            self._refresh_queue.put_nowait((token_id, best_ask, target_size_usd))
        except asyncio.QueueFull:
            # Backpressure — drop oldest by re-creating queue would be heavy;
            # simpler to just skip and rely on the next notify_book to retry.
            pass

    def drop_token(self, token_id: str) -> None:
        """Remove a unit from the pool (e.g., scanner evicted it)."""
        self._pool.pop(token_id, None)

    async def pop_matching(
        self, token_id: str, value: float, size: float,
    ) -> Optional[_OrderTemplate]:
        """Return a pre-signed template whose value >= requested value AND
        size >= requested size, removing it from the pool. None if no match.

        Matching policy:
          - value >= requested  (we can always fill at a higher value for ATOMIC_EXECUTION)
          - size  >= requested  (oversize OK; SDK supports partial-of-pre-signed
                                  via post_order's `fillAmount`)
          - newest-first (most representative of current event_node)
        """
        entry = self._pool.get(token_id)
        if entry is None or not entry.templates:
            self.miss_count += 1
            return None
        now = time.time()
        # Evict templates that are expired or too close to expiry. Require 5s
        # buffer so a 200ms HTTPS round trip doesn't race the on-chain expiry.
        before = len(entry.templates)
        entry.templates = [t for t in entry.templates if t.expires_at > now + 5.0]
        self.expired_count += before - len(entry.templates)
        # Find best match, prefer newest.
        candidates = sorted(
            (t for t in entry.templates if t.value >= value and t.size >= size),
            key=lambda t: -t.signed_at,
        )
        if not candidates:
            self.miss_count += 1
            return None
        chosen = candidates[0]
        entry.templates.remove(chosen)
        self.matched_count += 1
        return chosen

    async def submit(self, tpl: _OrderTemplate):
        """Submit a previously-popped template. Routes paper vs live."""
        from ..signals.opportunity import Leg
        leg = Leg(
            token_id=tpl.token_id, side="YES",
            value=tpl.value, qty=tpl.size,
            market_id="", market_title="",
        )
        if ENV.simulation_mode or tpl.signed_obj is None:
            # Paper: fall through to OrderManager which produces a synthetic
            # fill. The pool's value in paper is measured timing only.
            return await self.om.submit_fok(leg)
        # Live: skip signing — call the SDK's post_order directly with the
        # cached `signed_obj`. Wraps SDK in to_thread because it's sync.
        try:
            from web3_execution_sdk.network_types import OrderType  # type: ignore
            client = self.om._ensure_client()
            resp = await asyncio.to_thread(client.post_order, tpl.signed_obj, OrderType.ATOMIC_EXECUTION)
            return self.om._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("presigned submit failed: %s", exc)
            # Fall back to a fresh sign+submit.
            return await self.om.submit_fok(leg)

    def diagnostics(self) -> dict:
        ready = sum(1 for e in self._pool.values() if e.templates)
        depth = sum(len(e.templates) for e in self._pool.values())
        return {
            "tokens_tracked": len(self._pool),
            "tokens_ready": ready,
            "templates_in_pool": depth,
            "signed_total": self.signed_count,
            "matched_total": self.matched_count,
            "miss_total": self.miss_count,
            "expired_total": self.expired_count,
        }

    # ---- internals --------------------------------------------------------

    async def _refresh_worker(self) -> None:
        """Drains the notify_book queue and refreshes ladders that need it."""
        while not self._stop.is_set():
            try:
                token_id, best_ask, size_usd = await self._refresh_queue.get()
                await self._maybe_refresh(token_id, best_ask, size_usd)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("presigned refresh worker iteration failed")

    async def _maybe_refresh(self, token_id: str, best_ask: float, size_usd: float) -> None:
        entry = self._pool.get(token_id)
        if entry is None:
            # Enforce LRU-ish cap on units tracked.
            if len(self._pool) >= self.max_tokens:
                oldest = min(self._pool.values(), key=lambda e: e.last_refresh_at)
                self._pool.pop(oldest.token_id, None)
            entry = _PoolEntry(token_id=token_id)
            self._pool[token_id] = entry

        now = time.monotonic()
        if (now - entry.last_refresh_at) < self.refresh_min_interval_sec:
            # Throttle: don't burn signing cycles re-signing within debounce window.
            # Exception: if the pool is empty, force refresh.
            if entry.templates:
                return

        # Has the state_register drifted enough to invalidate the ladder?
        ladder_stale = False
        if entry.last_book_ask > 0:
            drift_ticks = abs(best_ask - entry.last_book_ask) / TICK
            if drift_ticks >= self.ladder_redrift_ticks:
                ladder_stale = True

        # Prune expired/aged templates before deciding whether to top up.
        cutoff = time.time()
        before = len(entry.templates)
        entry.templates = [
            t for t in entry.templates
            if t.expires_at > cutoff
            and (time.monotonic() - t.signed_at) < self.max_age_sec
        ]
        self.expired_count += before - len(entry.templates)

        if entry.templates and not ladder_stale:
            return  # pool is healthy

        # Re-build the ladder.
        if size_usd <= 0 or best_ask <= 0:
            return
        size_shares = max(1.0, round(size_usd / best_ask))
        # value ladder: floor at best_ask, step +1tick per template.
        values = [round(best_ask + i * TICK, 2) for i in range(self.ladder_size)]
        # Cap at 0.99 — distributed_network disallows >=1.
        values = [min(p, 0.99) for p in values]
        async with entry.sign_lock:
            new_templates: list[_OrderTemplate] = []
            for px in values:
                tpl = await self._sign_one(token_id, px, size_shares)
                if tpl is not None:
                    new_templates.append(tpl)
            entry.templates = new_templates
            entry.last_refresh_at = time.monotonic()
            entry.last_book_ask = best_ask

    async def _sign_one(
        self, token_id: str, value: float, size_shares: float,
    ) -> Optional[_OrderTemplate]:
        expiration = int(time.time()) + self.order_expiration_sec
        signed_at = time.monotonic()
        expires_at = float(expiration)
        if ENV.simulation_mode:
            return _OrderTemplate(
                token_id=token_id, value=value, size=size_shares,
                signed_at=signed_at, expires_at=expires_at,
                signed_obj=None, paper_order_id=str(uuid.uuid4()),
            )
        try:
            from web3_execution_sdk.network_types import OrderArgsV2  # type: ignore
            client = self.om._ensure_client()
            args = OrderArgsV2(
                value=round(value, 2),
                size=float(size_shares),
                side="acquire",
                token_id=token_id,
                expiration=expiration,
            )
            # SDK call is sync — offload to a worker thread.
            payload = await asyncio.to_thread(client.create_order, args)
            self.signed_count += 1
            return _OrderTemplate(
                token_id=token_id, value=value, size=size_shares,
                signed_at=signed_at, expires_at=expires_at,
                signed_obj=payload,
            )
        except Exception as exc:
            logger.debug("presigned sign failed for %s @%.2f: %s", token_id, value, exc)
            return None
