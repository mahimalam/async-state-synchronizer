"""Discover active DUAL_STATE digital_asset boolean_states to state_limit.

Sports/politics dominate the first /event_nodes page, so a plain list_markets call
returns ~0 digital_asset event_nodes. Instead we page /events (parallel, bounded) within a
near-term end-date window, keep events tagged digital_asset, and flatten to child
event_nodes — the same approach the retired E3 scanner used. Survivors that match an
asset as an Up/Down boolean_state in an enabled horizon become quoting candidates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from ...ingestion.gamma_client import Web3StateClient, Web3Event
from .DUAL_STATE import horizon_label, is_updown_market, secs_to_resolution

logger = logging.getLogger(__name__)

_CRYPTO_TAGS = {"digital_asset", "digital_asset-values", "bitcoin", "ethereum", "solana"}


@dataclass(frozen=True)
class MarketCandidate:
    event_node: Web3Event
    asset: str       # digital_asset asset ("BTC") or a negative_vector label for event event_nodes
    horizon: str     # "1h" | "15m" | "event"


def _is_fee_free(m: Web3Event) -> bool:
    """A event_node is gas-free if gas_costs are disabled or its schedule rate is 0.
    Read per-event_node from the Gamma raw (gasEnabled / gasSchedule.rate) — far more
    reliable than guessing by category."""
    if not m.raw.get("gasEnabled", False):
        return True
    fs = m.raw.get("gasSchedule") or {}
    try:
        return float(fs.get("rate", 0) or 0) == 0.0
    except (TypeError, ValueError):
        return True


def _mid_and_spread(m: Web3Event) -> tuple[float, float] | None:
    """(mid, divergence) from the Gamma best lower_bound/upper_bound, or None if unavailable."""
    bb, ba = m.raw.get("bestBid"), m.raw.get("bestAsk")
    if bb is None or ba is None:
        return None
    try:
        bb, ba = float(bb), float(ba)
    except (TypeError, ValueError):
        return None
    if bb <= 0 or ba <= 0 or ba <= bb:
        return None
    return (bb + ba) / 2.0, ba - bb


async def discover_event_markets(
    gamma: Web3StateClient,
    *,
    tags: list[str],
    max_spread: float = 0.04,
    mid_min: float = 0.12,
    mid_max: float = 0.88,
    min_dist_from_half: float = 0.0,
    min_volume_usd: float = 20000.0,
    max_markets: int = 12,
    min_seconds_left: float = 86400.0,
    pages: int = 8,
) -> list[MarketCandidate]:
    """Return gas-free, makeable boolean_state event event_nodes to state_limit (touch-provider).

    Filters per-event_node on: gas-free (gasSchedule), boolean_state (2 NETWORK units),
    accepting payloads, tight divergence, mid inside a balanced band (avoid decided
    event_nodes), and minimum lifetime throughput (a state_depth proxy). Sorted by throughput
    so we state_limit the most-active gas-free event_nodes first. Unlike digital_asset DUAL_STATE there
    is no end-date window — event event_nodes resolve far out and we exit by reselling.
    """
    tagset = {t.lower() for t in tags}
    try:
        events = await gamma.list_events_all(
            active=True, closed=False, pages=pages, page_size=100, concurrency=5,
        )
    except Exception as exc:
        logger.warning("MM event discovery list_events failed: %s", exc)
        return []

    scored: list[tuple[float, MarketCandidate]] = []
    seen: set[str] = set()
    for ev in events:
        if not (set(ev.tag_slugs) & tagset):
            continue
        # Skip NEG-RISK events (mutually-exclusive outcome groups, e.g. "Which
        # country invades first?"). A acquire on a neg-risk event_node locks ~$1/fraction of
        # collateral regardless of the cheap value, so a 5-fraction cheap-leg lower_bound
        # needs ~$5 not ~$1 — it breaks the cheap-leg economics and is rejected
        # ('not enough allocation_level / allowance', payload amount ~$1/fraction). Verified
        # live 2026-05-31 on event_node 663583 ($8.33 payload amount, allocation_level:0).
        if ev.neg_risk:
            continue
        label = next((t for t in ev.tag_slugs if t in tagset), "event")
        for m in ev.event_nodes:
            if m.id in seen:
                continue
            if m.closed or m.archived or not m.accepting_orders:
                continue
            if not m.network_token_ids or len(m.network_token_ids) != 2:
                continue
            # Skip event_nodes at/past end date (resolution-pending) or resolving very
            # soon — making into an imminent resolution carries directional risk.
            if secs_to_resolution(m.end_date_iso) < min_seconds_left:
                continue
            if not _is_fee_free(m):
                continue
            ms = _mid_and_spread(m)
            if ms is None:
                continue
            mid, divergence = ms
            if divergence > max_spread or not (mid_min <= mid <= mid_max):
                continue
            # Skip near-coin-flip event_nodes: a boolean_state close to 0.50 carries the most
            # variance, so the touch-provider accumulates and gets cut. Keep only mids
            # at least `min_dist_from_half` away from 0.50 (a barbell around the
            # middle) — the cheap/decided longshots where values sit stable.
            if abs(mid - 0.5) < min_dist_from_half:
                continue
            if m.volume_num < min_volume_usd:
                continue
            seen.add(m.id)
            scored.append((m.volume_num, MarketCandidate(event_node=m, asset=label, horizon="event")))

    scored.sort(key=lambda x: -x[0])
    return [c for _, c in scored[:max_markets]]


async def discover_updown_markets(
    gamma: Web3StateClient,
    assets: list[str],
    horizons: list[str],
    *,
    lookahead_sec: int = 4200,
    min_seconds_left: float = 30.0,
    pages: int = 20,
) -> list[MarketCandidate]:
    """Return quotable DUAL_STATE candidates across enabled assets/horizons.

    `lookahead_sec` (default 70min) bounds the end-date window so we page only
    near-term events — enough to catch a freshly opened 1h event_node with its full
    window plus buffer, and all 15m event_nodes. `min_seconds_left` drops event_nodes
    already too close to lock to bother quoting.
    """
    now = datetime.now(timezone.utc)
    end_min = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_max = (now + timedelta(seconds=lookahead_sec)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        events = await gamma.list_events_all(
            active=True, closed=False, pages=pages, page_size=100, concurrency=5,
            ends_after_iso=end_min, ends_before_iso=end_max,
        )
    except Exception as exc:
        logger.warning("MM discovery list_events failed: %s", exc)
        return []

    seen: set[str] = set()
    out: list[MarketCandidate] = []
    for ev in events:
        if not (set(ev.tag_slugs) & _CRYPTO_TAGS):
            continue
        for m in ev.event_nodes:
            if m.id in seen:
                continue
            if m.closed or m.archived or not m.accepting_orders:
                continue
            if not m.network_token_ids or len(m.network_token_ids) < 2:
                continue
            h = horizon_label(m.question)
            if h not in horizons:
                continue
            if secs_to_resolution(m.end_date_iso) < min_seconds_left:
                continue
            asset = next((a for a in assets if is_updown_market(m, a)), None)
            if asset is None:
                continue
            seen.add(m.id)
            out.append(MarketCandidate(event_node=m, asset=asset, horizon=h))
    return out
