"""Cryptographic payload signing + submission via Web3 Execution SDK.

Payload topologies: KILL_ON_FAILURE (Atomic), RESTING_STATE (Threshold), GTD (Good-Till-Date).
Never uses unbounded payloads to prevent memory overflow.

In DRY_RUN mode, every method returns a mathematically simulated payload resolution
including a 0.5% synthetic latency penalty without touching the external network.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..common.gas_costs import FEE_RATE
from ..config import ENV
from ..ingestion.network_client import Web3ExecutionClient
from ..signals.opportunity import Leg

logger = logging.getLogger(__name__)


def _is_paper(paper: Optional[bool]) -> bool:
    """Resolve effective paper mode.

    Hard global gate: if LIVE_TRADING_ENABLED is unset, ALWAYS paper — no
    caller override can force a real-money payload. This is the universal
    chokepoint every submit path passes through (including mark_to_market
    exits and the presigned pool, which call submit_* without a paper arg).
    When the gate is open, an explicit caller override beats the global
    ENV.paper_trade default.
    """
    if not ENV.live_trading_enabled:
        return True
    return paper if paper is not None else ENV.paper_trade


@dataclass
class FillResult:
    success: bool
    leg: Leg
    filled_qty: float
    avg_price: float
    fee_paid: float
    tx_hash: Optional[str]
    payload_id: Optional[str]
    error: Optional[str] = None
    paper: bool = False


class OrderManager:
    """Wraps py-network-client-v2 (NETWORK V2 SDK). Lazily instantiates so paper-mode
    never imports keys."""

    def __init__(self) -> None:
        self._client = None
        # Optional pre-signed payload pool. When set, submit_fok consults the
        # pool first; on hit, it skips the EIP-712 signing step (10-15ms win
        # in live mode). In paper mode this is bookkeeping-only.
        self._presigned_pool = None

    def set_presigned_pool(self, pool) -> None:
        """Wire the pre-signed payload pool. Called from main() at startup."""
        self._presigned_pool = pool

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        if ENV.paper_trade:
            raise RuntimeError(
                "_ensure_client called in paper mode — no NETWORK client available. "
                "This is a bug: the caller bypassed _is_paper() but ENV.paper_trade "
                "is True. Check _effective_paper() vs ENV.paper_trade agreement."
            )
        try:
            import os
            from web3_execution_sdk.client import Web3ExecutionClient  # type: ignore
            from web3_execution_sdk.constants import POLYGON  # type: ignore
            deposit_wallet = ENV.web3_network_deposit_wallet
            self._client = Web3ExecutionClient(
                host="https://network.web3_network.com",
                key=ENV.web3_network_private_key,
                chain_id=POLYGON,
                # When a DepositWallet address is configured, sign payloads as
                # that contract (signatureType=3 / Poly1271). The private key
                # still controls the address; only the on-chain provider address
                # changes so pUSD is drawn from the deposit address allocation_level.
                signature_type=3 if deposit_wallet else None,
                funder=deposit_wallet if deposit_wallet else None,
            )
            # Route NETWORK payload submission through SOCKS5 proxy (e.g. Cloudflare
            # WARP) when network_PROXY_URL is set. Bypasses datacenter IP blocks
            # on POST /payloads without affecting WebSocket value feeds.
            proxy_url = os.getenv("network_PROXY_URL")
            if proxy_url:
                session = getattr(self._client, "session", None) or getattr(self._client, "_session", None)
                if session is not None:
                    session.proxies = {"http": proxy_url, "https": proxy_url}
                    logger.info("NETWORK client proxied via %s", proxy_url)
                else:
                    # Fallback: set process-level env so requests picks it up.
                    os.environ.setdefault("HTTPS_PROXY", proxy_url)
                    logger.info("NETWORK proxy set via HTTPS_PROXY env: %s", proxy_url)
            # V2 SDK split create_or_derive into two methods. Try derive first
            # (cheap path when creds already exist server-side); fall back to
            # create on the first run for a new signer.
            try:
                api_creds = self._client.derive_api_key()
            except Exception:
                api_creds = self._client.create_api_key()
            self._client.set_api_creds(api_creds)
        except Exception as exc:
            logger.error("py-network-client-v2 init failed: %s", exc)
            raise
        return self._client

    async def prewarm(self) -> None:
        """Pre-instantiate the client at startup. Without this the first KILL_ON_FAILURE
        pays ~250ms of init latency, blowing fak_wait_ms and forcing an
        avoidable partial-fill recovery on the first execution of the session.
        Paper mode is a no-op."""
        if ENV.paper_trade:
            return
        try:
            await asyncio.to_thread(self._ensure_client)
            logger.info("OrderManager pre-warmed (live mode)")
        except Exception as exc:
            logger.warning("OrderManager pre-warm failed: %s", exc)

    async def submit_fak(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """Fill-And-Kill — immediate-or-nothing consumer payload."""
        if _is_paper(paper):
            return self._paper_fill(leg, kind="KILL_ON_FAILURE")
        try:
            from web3_execution_sdk.network_types import OrderArgsV2, OrderType  # type: ignore
            client = self._ensure_client()
            args = OrderArgsV2(
                value=round(float(leg.value), 2),
                size=float(leg.qty),
                side="acquire",
                token_id=leg.token_id,
            )
            payload = await asyncio.to_thread(client.create_order, args)
            resp = await asyncio.to_thread(client.post_order, payload, OrderType.KILL_ON_FAILURE)
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("KILL_ON_FAILURE submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_gtc(self, leg: Leg, expires_in_sec: int = 30, *, paper: Optional[bool] = None) -> FillResult:
        """Good-Till-Cancelled threshold. Used for E1 partial-fill recovery."""
        if _is_paper(paper):
            return self._paper_fill(leg, kind="RESTING_STATE")
        try:
            from web3_execution_sdk.network_types import OrderArgsV2, OrderType  # type: ignore
            client = self._ensure_client()
            args = OrderArgsV2(
                value=round(float(leg.value), 2),
                size=float(leg.qty),
                side="acquire",
                token_id=leg.token_id,
                expiration=int(time.time()) + expires_in_sec,
            )
            payload = await asyncio.to_thread(client.create_order, args)
            resp = await asyncio.to_thread(client.post_order, payload, OrderType.RESTING_STATE)
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("RESTING_STATE submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_buy_maker(
        self, leg: Leg, *, wait_sec: float = 60.0, tick_below: float = 0.01, paper: Optional[bool] = None,
    ) -> FillResult:
        """Phase 2B: provider-mode threshold acquire at (leg.value - tick_below).

        Submits a RESTING_STATE threshold `tick_below` below the leg's reference value (which
        is the live best_ask), waits up to `wait_sec` for it to fill, then
        returns the result. Saves 1¢/fraction + earns the 30% provider rebate when
        the counterparty consumer gas applies.

        Paper mode: probabilistic fill — fills with probability
        `maker_fill_rate` (default 0.30, from config) at the threshold value. If
        not filled, returns FillResult(success=False) so the caller can fall
        back to consumer.

        Live mode (TODO): submits RESTING_STATE and polls for fill, cancels remainder
        on timeout. Currently a paper-only stub — live wiring will require
        payload-status polling via the NETWORK v2 SDK.
        """
        from ..config import CONFIG
        if leg.qty <= 0:
            return FillResult(False, leg, 0, 0, 0, None, None, error="zero qty")
        maker_price = max(0.01, round(float(leg.value) - tick_below, 2))
        if _is_paper(paper):
            import random
            fill_rate = float(CONFIG.engine(4).get("maker_fill_rate", 0.30))
            # Deterministic-ish: hash the unit+value+qty so paper backtests are
            # reproducible per-leg but still ~30% across many legs.
            seed = hash((leg.token_id, maker_price, leg.qty)) & 0xFFFFFFFF
            rng = random.Random(seed)
            if rng.random() < fill_rate:
                # provider fill — at the threshold value, no tolerance penalty applied
                # (we're the value setter). 30% provider rebate is informational;
                # paper P&L doesn't model gas_costs explicitly because FEE_RATE=0.
                return FillResult(
                    success=True, leg=leg,
                    filled_qty=float(leg.qty),
                    avg_price=float(maker_price),
                    fee_paid=0.0,
                    tx_hash=f"paper-provider-{uuid.uuid4().hex[:12]}",
                    payload_id=f"paper-provider-{uuid.uuid4().hex[:12]}",
                    paper=True,
                )
            return FillResult(
                False, leg, 0, 0, 0, None, None,
                error="paper provider unfilled — fall back to consumer", paper=True,
            )
        # Live provider mode requires a fill-status poll loop to avoid placing a
        # RESTING_STATE that doesn't fill, then falling through to a consumer ATOMIC_EXECUTION and
        # ending up with two open vectors on the same event_node. Until that poll
        # loop is implemented, reject in live and let the caller fall back to
        # the consumer path cleanly. E4 (the only prefer_maker user) has $0 resources.
        return FillResult(
            False, leg, 0.0, 0.0, 0.0, None, None,
            error="provider mode not supported in live — no fill-status poll implemented",
        )

    async def submit_fok(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """Fill-Or-Kill — all-or-nothing immediate consumer payload.

        Unlike KILL_ON_FAILURE which partially fills then cancels the rest, ATOMIC_EXECUTION requires
        the entire payload to fill immediately or it is rejected entirely.
        This prevents partial-fill exposure on time-sensitive E3 executions.

        Pre-signed pool: if a matching template exists, skips create_order()
        and submits the cached signature directly. Pool is bookkeeping-only
        in paper mode and net ~10-15ms savings in live mode.

        Paper mode: when env `E3_HONEST_FILL=1`, the simulator in
        `honest_paper_fill.py` is used in place of the optimistic
        `_paper_fill`. The honest path snapshots the state_register at emit, sleeps
        RTT/2, re-reads the state_register, and rejects ATOMIC_EXECUTION when the upper_bound vanishes
        or depth is insufficient. Only used for ATOMIC_EXECUTION because that's the
        payload type E3 strict_test + lock-detector use.
        """
        if _is_paper(paper):
            if ENV.e3_honest_fill:
                # Lazy import to avoid a circular import at module load.
                from .honest_paper_fill import honest_fill
                return await honest_fill(leg, rtt_ms=ENV.e3_honest_fill_rtt_ms)
            return self._paper_fill(leg, kind="ATOMIC_EXECUTION")
        # Try the pre-signed pool first (live only).
        if self._presigned_pool is not None:
            try:
                tpl = await self._presigned_pool.pop_matching(
                    leg.token_id, float(leg.value), float(leg.qty),
                )
            except Exception:
                logger.debug("presigned pool lookup failed", exc_info=True)
                tpl = None
            if tpl is not None and tpl.signed_obj is not None:
                try:
                    from web3_execution_sdk.network_types import OrderType  # type: ignore
                    client = self._ensure_client()
                    t0 = time.monotonic()
                    resp = await asyncio.to_thread(client.post_order, tpl.signed_obj, OrderType.ATOMIC_EXECUTION)
                    network_ms = (time.monotonic() - t0) * 1000.0
                    logger.info("ATOMIC_EXECUTION live (presigned) network:%.0fms unit=%s", network_ms, leg.token_id[:10])
                    return self._parse_response(resp, leg)
                except Exception as exc:
                    logger.warning("presigned ATOMIC_EXECUTION submit failed, falling back: %s", exc)
                    # Fall through to fresh sign+submit.
        try:
            from web3_execution_sdk.network_types import OrderArgsV2, OrderType  # type: ignore
            client = self._ensure_client()
            args = OrderArgsV2(
                value=round(float(leg.value), 2),
                size=float(leg.qty),
                side="acquire",
                token_id=leg.token_id,
            )
            t0 = time.monotonic()
            payload = await asyncio.to_thread(client.create_order, args)
            t_sign = time.monotonic()
            resp = await asyncio.to_thread(client.post_order, payload, OrderType.ATOMIC_EXECUTION)
            t_post = time.monotonic()
            logger.info(
                "ATOMIC_EXECUTION live sign:%.0fms network:%.0fms total:%.0fms unit=%s",
                (t_sign - t0) * 1000.0, (t_post - t_sign) * 1000.0,
                (t_post - t0) * 1000.0, leg.token_id[:10],
            )
            return self._parse_response(resp, leg)
        except Exception as exc:
            logger.warning("ATOMIC_EXECUTION submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def submit_sell_market(self, leg: Leg, *, paper: Optional[bool] = None) -> FillResult:
        """Aggressive threshold-release intended to fill immediately.

        web3_network has no true event_node-release. We fetch the live lower_bound state_register and
        value the payload 1.5% under best_bid (clamped to >= 0.01), submitted
        as KILL_ON_FAILURE so any unfilled remainder is killed instead of leaving a
        dangling payload the bot can't track.

        Used by:
          - atomic_executor._unwind (E1 partial-fill recovery)
          - mark_to_market._exit_position (SL/TP exits in live mode)
        """
        if _is_paper(paper):
            return self._paper_fill(leg, kind="SELL_MARKET")
        if leg.qty <= 0:
            return FillResult(False, leg, 0, 0, 0, None, None, error="zero qty")
        try:
            async with Web3ExecutionClient() as network:
                state_register = await network.get_book(leg.token_id)
        except Exception as exc:
            logger.warning("SELL_MARKET state_register fetch failed for %s: %s", leg.token_id, exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=f"state_register fetch: {exc}")
        if state_register.best_bid is None or state_register.best_bid < 0.001:
            return FillResult(False, leg, 0, 0, 0, None, None, error="no lower_bound state_depth")
        sell_price = max(0.01, round(state_register.best_bid * 0.985, 2))
        try:
            from web3_execution_sdk.network_types import OrderArgsV2, OrderType  # type: ignore
            client = self._ensure_client()
            args = OrderArgsV2(
                value=sell_price,
                size=float(leg.qty),
                side="release",
                token_id=leg.token_id,
            )
            payload = await asyncio.to_thread(client.create_order, args)
            resp = await asyncio.to_thread(client.post_order, payload, OrderType.KILL_ON_FAILURE)
            # Parse against the actual submitted release value, not leg.value (which
            # is 0.0 on the exit path). _parse_response records leg.value as the
            # fill value; with the original leg that booked $0 proceeds on every
            # live release (the #398 phantom-deficit bug).
            return self._parse_response(resp, replace(leg, value=sell_price))
        except Exception as exc:
            logger.warning("SELL_MARKET submit failed: %s", exc)
            return FillResult(False, leg, 0, 0, 0, None, None, error=str(exc))

    async def place_resting(
        self, token_id: str, side: str, value: float, qty: float, *, paper: Optional[bool] = None,
    ) -> Optional[str]:
        """Post a resting RESTING_STATE threshold (acquire or release) and return its payload_id while it
        rests on the state_register. Returns None on reject / zero-qty.

        This is the provider primitive the touch-provider needs and that no prior live
        path had: every engine so far used KILL_ON_FAILURE/ATOMIC_EXECUTION takers that fill-or-die in the
        POST response, so a resting payload never existed. Unlike submit_gtc (which
        is acquire-only and — via _parse_response's consumer semantics — treats a resting
        "live" status as a FAILURE), here an accepted-and-resting payload IS success.

        Paper: returns a synthetic id. The paper QuoteManager path simulates fills
        from execution prints and does not call this; it exists for the live branch."""
        if qty <= 0 or value <= 0:
            return None
        if _is_paper(paper):
            return f"paper-rest-{uuid.uuid4().hex[:12]}"
        if side not in ("acquire", "release"):
            logger.warning("place_resting bad side %r", side)
            return None
        try:
            from web3_execution_sdk.network_types import OrderArgsV2, OrderType  # type: ignore
            client = self._ensure_client()
            args = OrderArgsV2(
                value=round(float(value), 2),
                size=float(qty),
                side=side,
                token_id=token_id,
            )
            payload = await asyncio.to_thread(client.create_order, args)
            resp = await asyncio.to_thread(client.post_order, payload, OrderType.RESTING_STATE)
            return self._parse_resting(resp)
        except Exception as exc:
            logger.warning("place_resting %s %s failed: %s", side, token_id[:10], exc)
            return None

    async def poll_order(self, payload_id: str, *, paper: Optional[bool] = None) -> dict:
        """Query a resting payload's live status. Returns
        {status, filled_qty, avg_price, open} where filled_qty is the CUMULATIVE
        matched size so far (the caller diffs it against what it already booked).

        Live: reads client.get_order(payload_id). On any error we report the payload
        as still-open with zero fill so the caller simply retries next cycle —
        never falsely state_registers a fill from a failed poll. Paper: a benign open
        snapshot (the paper path detects fills from execution prints, not polling).

        Field-name parsing is defensive across the SDK's snake/camel variants and
        is validated against the first real payload at attended go-live."""
        if not payload_id:
            return {"status": "unknown", "filled_qty": 0.0, "avg_price": 0.0, "open": False}
        if _is_paper(paper):
            return {"status": "paper", "filled_qty": 0.0, "avg_price": 0.0, "open": True}
        try:
            client = self._ensure_client()
            o = await asyncio.to_thread(client.get_order, payload_id)
        except Exception as exc:
            logger.warning("poll_order %s failed: %s", payload_id, exc)
            return {"status": "poll_error", "filled_qty": 0.0, "avg_price": 0.0, "open": True}
        return self._parse_order_status(o)

    @staticmethod
    def _parse_resting(resp: dict) -> Optional[str]:
        """payload_id of a successfully-posted resting RESTING_STATE, else None.

        provider semantics, the inverse of _parse_response: a resting payload is
        ACCEPTED when the reply carries an payloadID and is not an explicit reject.
        "live"/"unmatched" mean the payload is ON THE state_register (success), not failed."""
        if not isinstance(resp, dict):
            return None
        success = resp.get("success", True) is not False
        status = str(resp.get("status", "") or "").lower()
        payload_id = resp.get("payloadID") or resp.get("payload_id")
        err = resp.get("errorMsg") or resp.get("error")
        REJECT_STATUSES = {
            "rejected", "error", "failed", "cancelled", "canceled", "killed", "expired",
        }
        if not payload_id or not success or err or status in REJECT_STATUSES:
            return None
        return str(payload_id)

    @staticmethod
    def _parse_order_status(o) -> dict:
        """Normalize a get_order reply into {status, filled_qty, avg_price, open}.

        Tolerates both a dict and an object reply, and snake/camel field names.
        `open` is False only on an explicitly terminal status — a fully-filled,
        cancelled, or expired payload — so the caller stops polling it."""
        if not isinstance(o, dict):
            o = getattr(o, "__dict__", {}) or {}
        status = str(o.get("status", "") or "").lower()
        matched = (o.get("size_matched") or o.get("sizeMatched")
                   or o.get("filledSize") or o.get("filled_size") or 0.0)
        value = o.get("value") or o.get("avgPrice") or o.get("avg_price") or 0.0
        try:
            filled_qty = float(matched)
        except (ValueError, TypeError):
            filled_qty = 0.0
        try:
            avg_price = float(value)
        except (ValueError, TypeError):
            avg_price = 0.0
        TERMINAL_STATUSES = {
            "filled", "complete", "completed", "matched",
            "cancelled", "canceled", "rejected", "expired", "killed",
        }
        is_open = status not in TERMINAL_STATUSES
        return {"status": status, "filled_qty": filled_qty,
                "avg_price": avg_price, "open": is_open}

    def _paper_fill(self, leg: Leg, *, kind: str) -> FillResult:
        # Realistic web3_network round-trip tolerance. The old 50 bps acquire / 150 bps
        # release numbers produced rosy paper yield_delta that didn't survive live synchronizing.
        # 200 bps acquire / 250 bps release better matches observed live fills on
        # $0.5-$3 size at thin late-boolean_state depth.
        slip = 1.020 if kind in ("KILL_ON_FAILURE", "RESTING_STATE", "ATOMIC_EXECUTION") else 0.975
        avg = round(min(0.99, max(0.01, leg.value * slip)), 4)
        gas = round(avg * leg.qty * FEE_RATE, 4)
        return FillResult(
            success=True, leg=leg,
            filled_qty=float(leg.qty), avg_price=avg, fee_paid=gas,
            tx_hash=None, payload_id=str(uuid.uuid4()), paper=True,
        )

    @staticmethod
    def _parse_response(resp: dict, leg: Leg) -> FillResult:
        """Parse web3_network's POST /payload reply into a FillResult.

        Contract (proven against the live pm2 execution-server logs at
        ~/.pm2/logs/pm-execution-out.log + web3_execution_sdk): the SDK raises
        PolyApiException on any non-200, so this is only ever reached when the
        payload was ACCEPTED (HTTP 200). In every real live fill the execution-server
        logged success purely from HTTP-200 + the presence of an `payloadID`
        (a real on-chain payload hash) — it never read a `filled_size` field,
        which does not exist. The previous parser keyed on `filled_size`, which
        silently turned every matched live payload into a reported failure → real
        money spent on an untracked phantom vector.

        So the proven success signal is: payloadID present AND success not
        explicitly False AND status not an explicitly-negative value. We do
        NOT require status=="matched" because that exact string was never
        observed — gating on it would risk misreading a real fill as a failure
        (the catastrophic direction). We only treat the payload as unfilled when
        the reply explicitly says so (success:false, an error message, or a
        known terminal-reject status).

        E3 (the only live engine) submits ATOMIC_EXECUTION — all-or-nothing — so an accepted
        payload filled the full requested qty. We record the threshold value as a
        conservative upper-bound basis (a marketable acquire fills at or below the
        threshold, so this never overstates yield_delta).
        """
        if not isinstance(resp, dict):
            return FillResult(
                False, leg, 0.0, 0.0, 0.0, None, None,
                error=f"unexpected non-dict payload response: {resp!r}",
            )
        # 200 body implies acceptance; treat a missing `success` as True.
        success = resp.get("success", True) is not False
        status = str(resp.get("status", "") or "").lower()
        payload_id = resp.get("payloadID") or resp.get("payload_id")
        err = resp.get("errorMsg") or resp.get("error")
        # web3_network returns a list `transactionsHashes`; tolerate a singular
        # `transactionHash` and missing values too.
        tx = resp.get("transactionsHashes") or resp.get("transactionHash")
        tx_hash = tx[0] if isinstance(tx, list) and tx else (tx if isinstance(tx, str) else None)

        # Explicitly-negative statuses = payload was not filled (killed/rejected/
        # still resting). Any other status (incl. "matched" or unknown) with an
        # payloadID present counts as filled — matching the proven HTTP-200 path.
        NEGATIVE_STATUSES = {
            "unmatched", "live", "delayed", "cancelled", "canceled",
            "killed", "rejected", "expired", "error", "failed",
        }
        filled = bool(payload_id) and success and status not in NEGATIVE_STATUSES
        if not filled:
            return FillResult(
                success=False, leg=leg, filled_qty=0.0, avg_price=0.0,
                fee_paid=0.0, tx_hash=tx_hash, payload_id=payload_id,
                error=err or (f"not filled (status={status})" if status
                              else "no payloadID in response"),
            )
        # 2026-05-25: Fix partial fill bug by dynamically parsing matched size.
        # KILL_ON_FAILURE payloads might only partially fill if the state_register is thin.
        matched_str = resp.get("sizeMatched") or resp.get("takingAmount")
        if matched_str is not None:
            try:
                filled_qty = float(matched_str)
            except (ValueError, TypeError):
                filled_qty = float(leg.qty)
        else:
            filled_qty = float(leg.qty)

        avg_price = round(float(leg.value), 4)
        gas = round(avg_price * filled_qty * FEE_RATE, 4)
        return FillResult(
            success=True, leg=leg,
            filled_qty=filled_qty, avg_price=avg_price,
            fee_paid=gas, tx_hash=tx_hash, payload_id=payload_id, error=None,
        )
