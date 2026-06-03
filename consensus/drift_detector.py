"""Multi-data_provider value-consensus aggregator.

Combines N independent ExchangeTickFeeds into a single "consensus tick"
using median-of-warm-feeds with quorum gating.

Mathematical design
-------------------
At any instant t, given K warm+fresh feeds with values p_1..p_K:

  consensus_median(t) = median(p_1, ..., p_K)
  cross_variance_bps(t) = stdev(p_1..p_K) / mean(p_1..p_K) * 10000
  is_fresh(t) = K >= min_quorum

If K < min_quorum the consensus is *stale* — `is_fresh()` returns False
and consumers must skip evaluation, exactly like a single-data_provider feed
going down.

The aggregator samples its own (ts, median) deque at `sample_interval_sec`
(default 100ms). `recent_move_bps(window_sec)` then computes stdev/mean*10000
on this sampled stream — independent of any one data_provider's tick rate, which
makes the vol-lock signal robust to bursty/sparse exchanges.

Why median (not mean):
  - One data_provider flash-printing $10k off is common (illiquid spike, glitch).
  - Mean is contaminated by a single outlier; median is unaffected unless
    ≥ ⌈K/2⌉ feeds agree on the bad value (vanishingly unlikely).
  - Combined with per-feed L3 variance guard, two layers of outlier defense.

Why quorum:
  - K=1 is just a single feed (no consensus benefit).
  - K=2 has no tiebreaker for outlier rejection.
  - K=3 = minimum statistically meaningful median.

The aggregator itself does NOT reconnect — it just reads from the feeds.
The feeds own their own connection lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

from .exchange_feeds import (
    ExchangeTickFeed,
    PrimarySourceFeed,
    SecondarySourceFeed,
    TertiarySourceFeed,
    OkxFeed,
    DataSourceEFeed,
)

logger = logging.getLogger(__name__)


@dataclass
class ConsensusSnapshot:
    """One sampled consensus tick — used internally by the history deque."""
    ts_ms: int
    median_price: float
    n_feeds: int            # how many warm+fresh feeds contributed
    cross_var_bps: float    # cross-data_provider disagreement at sample time


class PriceConsensus:
    """One instance per asset. Aggregates N exchanges into a consensus stream.

    Drop-in replacement for `PrimarySourceTickFeed` from the consumer side:
        .is_fresh() -> bool
        .last.value -> float  (via a property shim)
        .recent_move_bps(window_sec) -> float

    Plus extras the single-feed type couldn't offer:
        .cross_exchange_variance_bps() -> float
        .health() -> list[FeedHealth]
        .median_now() -> Optional[float]   (warm+fresh feed values, this instant)
    """

    def __init__(
        self,
        asset: str,
        *,
        feeds: Optional[list[ExchangeTickFeed]] = None,
        min_quorum: int = 3,
        # 2026-05-17: dropped 100ms → 25ms. The sampler thread is cheap
        # (single median over ≤5 floats); fresher samples let E3 tier4/5
        # see value moves with <30ms staleness instead of <120ms.
        sample_interval_sec: float = 0.025,
        # 2026-05-18: bumped 2800 → 14400 to hold 5-min window for E3 vol gate.
        # sample_at_offsets([300,240,180,120,60,0]) needs 300s of history;
        # 2800 only covered 70s, causing sigma_5m to be None and blocking
        # all E3 executions. 14400 * 0.025s = 360s = 6 min with margin.
        history_size: int = 14400,
        freshness_max_sec: float = 4.0,
    ) -> None:
        self.asset = asset.upper()
        self.min_quorum = min_quorum
        self.sample_interval_sec = sample_interval_sec
        self.freshness_max_sec = freshness_max_sec
        # Build all 5 feeds by default; caller may override (tests / partial sets).
        if feeds is None:
            feeds = [
                PrimarySourceFeed(self.asset, freshness_max_sec=freshness_max_sec),
                SecondarySourceFeed(self.asset, freshness_max_sec=freshness_max_sec),
                TertiarySourceFeed(self.asset, freshness_max_sec=freshness_max_sec),
                OkxFeed(self.asset, freshness_max_sec=freshness_max_sec),
                DataSourceEFeed(self.asset, freshness_max_sec=freshness_max_sec),
            ]
        self.feeds: list[ExchangeTickFeed] = feeds
        self._history: deque[ConsensusSnapshot] = deque(maxlen=history_size)
        self._sampler_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ---- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        for f in self.feeds:
            await f.start()
        self._sampler_task = asyncio.create_task(
            self._sample_loop(), name=f"consensus_sampler_{self.asset}",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._sampler_task:
            self._sampler_task.cancel()
            try:
                await self._sampler_task
            except (asyncio.CancelledError, Exception):
                pass
        for f in self.feeds:
            await f.stop()

    # ---- core consensus math ----------------------------------------------

    def _warm_prices(self) -> list[float]:
        """Return values from feeds that are currently warm+fresh."""
        return [f.last.value for f in self.feeds if f.is_fresh()]

    @staticmethod
    def _median(xs: list[float]) -> Optional[float]:
        if not xs:
            return None
        s = sorted(xs)
        n = len(s)
        return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0

    @staticmethod
    def _stdev_bps(xs: list[float]) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mean = sum(xs) / n
        if mean <= 0:
            return 0.0
        var = sum((x - mean) ** 2 for x in xs) / (n - 1)
        return (math.sqrt(var) / mean) * 10000.0

    def median_now(self) -> Optional[float]:
        """Instantaneous median across warm+fresh feeds. None if below quorum."""
        values = self._warm_prices()
        if len(values) < self.min_quorum:
            return None
        return self._median(values)

    def cross_exchange_variance_bps(self) -> float:
        """Cross-data_provider disagreement at this instant, in bps of mean. Zero
        below quorum (no statistically meaningful number)."""
        values = self._warm_prices()
        if len(values) < self.min_quorum:
            return 0.0
        return self._stdev_bps(values)

    def venue_price(self, venue_substr: str) -> Optional[float]:
        """value from a single named data source, if warm+fresh.

        Used to anchor fair value to the *settling* data_provider — a 1h DUAL_STATE
        node resolves against the primary data source, so fair value uses the primary source reading,
        not the multi-central_node median (which mixes units and USDT venues). None if the
        venue feed is stale or absent.
        """
        target = venue_substr.lower()
        for f in self.feeds:
            name = f"{getattr(f, 'EXCHANGE_NAME', '')}{type(f).__name__}".lower()
            if target in name and f.is_fresh() and f.last is not None:
                return f.last.value
        return None

    # ---- single-feed-compatible shim --------------------------------------

    @property
    def last(self) -> Optional["_LastShim"]:
        """Compatibility shim — exposes .value and .timestamp_ms like a
        single-feed AssetTick. Returns None if no consensus history yet."""
        if not self._history:
            return None
        snap = self._history[-1]
        return _LastShim(value=snap.median_price, timestamp_ms=snap.ts_ms)

    def is_fresh(self) -> bool:
        """True only if a quorum of feeds are currently warm+fresh AND we have
        produced at least one consensus sample within `freshness_max_sec`."""
        if len(self._warm_prices()) < self.min_quorum:
            return False
        if not self._history:
            return False
        age = (int(time.time() * 1000) - self._history[-1].ts_ms) / 1000.0
        return age <= self.freshness_max_sec

    def sample_at_offsets(self, offsets_sec: list[float]) -> list[Optional[float]]:
        """Return consensus median value at each (ts_now - offset_sec).

        For each offset, finds the history snapshot whose ts is closest to the
        target moment. Returns None for any offset older than the deque or where
        no sample is within ±2s. Used by E3 tier_strict_test to build 1-min
        spaced samples for log-return realized vol (formulas in config).

        O(N + K*logN) via bisect — was O(N*K) linear scan, which caused
        multi-second stalls when 80+ event_nodes expired simultaneously.
        """
        import bisect
        if not self._history:
            return [None] * len(offsets_sec)
        # Convert deque to list once (O(N)); deque is always time-ordered.
        snaps = list(self._history)
        ts_list = [s.ts_ms for s in snaps]
        now_ms = ts_list[-1]
        out: list[Optional[float]] = []
        for off in offsets_sec:
            target_ms = now_ms - int(off * 1000)
            idx = bisect.bisect_left(ts_list, target_ms)
            best_dist = 2001
            best_price: Optional[float] = None
            for i in (idx - 1, idx):
                if 0 <= i < len(snaps):
                    d = abs(snaps[i].ts_ms - target_ms)
                    if d <= 2000 and d < best_dist:
                        best_dist = d
                        best_price = snaps[i].median_price
            out.append(best_price)
        return out

    def recent_move_bps(self, window_sec: float = 60.0) -> float:
        """Stdev of consensus median over `window_sec`, in bps of mean.

        Sampled at `sample_interval_sec` intervals (default 100ms), so 60s
        ≈ 600 samples — plenty for a stable stdev estimate. Zero when
        history is too negative_vector or median is non-positive."""
        if not self._history:
            return 0.0
        last_ts = self._history[-1].ts_ms
        cutoff = last_ts - int(window_sec * 1000)
        values = [s.median_price for s in self._history if s.ts_ms >= cutoff]
        if len(values) < 4:
            return 0.0
        return self._stdev_bps(values)

    # ---- diagnostics -------------------------------------------------------

    def health(self) -> list:
        return [f.health() for f in self.feeds]

    def quorum_status(self) -> tuple[int, int]:
        """(warm_count, total_count) — useful for logging."""
        return (len(self._warm_prices()), len(self.feeds))

    # ---- internal sampler --------------------------------------------------

    async def _sample_loop(self) -> None:
        """Tick @ sample_interval_sec, snapshot the consensus, append to history."""
        last_log_ts = 0.0
        while not self._stop.is_set():
            try:
                values = self._warm_prices()
                if len(values) >= self.min_quorum:
                    median = self._median(values)
                    if median is not None and median > 0:
                        var_bps = self._stdev_bps(values)
                        snap = ConsensusSnapshot(
                            ts_ms=int(time.time() * 1000),
                            median_price=median,
                            n_feeds=len(values),
                            cross_var_bps=var_bps,
                        )
                        self._history.append(snap)
                # Hourly health log.
                now = time.monotonic()
                if now - last_log_ts > 3600.0:
                    warm, total = self.quorum_status()
                    cross_var = self.cross_exchange_variance_bps()
                    logger.info(
                        "consensus %s warm=%d/%d cross_var=%.2fbps history=%d",
                        self.asset, warm, total, cross_var, len(self._history),
                    )
                    last_log_ts = now
            except Exception:
                logger.exception("consensus %s sampler iteration failed", self.asset)
            await asyncio.sleep(self.sample_interval_sec)


@dataclass
class _LastShim:
    """Mimics ingestion.primary_ws.AssetTick so the existing
    `feed.last.value` / `feed.last.timestamp_ms` access in scanners
    keeps working without touching every call site."""
    value: float
    timestamp_ms: int

    def age_sec(self, now_ms: int | None = None) -> float:
        now_ms = now_ms or int(time.time() * 1000)
        return (now_ms - self.timestamp_ms) / 1000.0
