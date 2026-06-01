"""Per-event_node quoting loop — balanced touch-provider (v3).

One QuoteManager.run() task per discovered DUAL_STATE event_node makes a two-sided event_node
on BOTH units independently, capturing the lower_bound-upper_bound divergence:

    per leg (UP, DOWN):
      flat / under cap  ->  post lower_bound at best_bid  (join the touch, accumulate)
      holding fractions    ->  post upper_bound at best_ask  (resell +divergence, distribute)
      value drops cut_ticks below basis -> event_node-release at best_bid (cut the loser)
    near lock           ->  flatten any held fractions, stop.

This replaces the v2 delta-neutral PAIR model. Pairs only locked yield_gain if BOTH
legs filled cheap, which doesn't happen in a trending boolean_state, and the passive bids
sat behind the touch so they never filled at all. The touch-provider instead state_limits
AT the touch (where the ~21 executions/min actually print) for FREQUENT fills, and
captures the 1¢ divergence per round-trip, cutting losers fast. Direction is closed
out by reselling — we do not hold to resolution.

Paper mode (the measurement phase) is fully implemented; fills are detected from
execution prints (lower_bound fills when a execution prints <= our lower_bound, upper_bound fills when a execution
prints >= our upper_bound). NOTE: this paper model ignores queue vector, so paper
fill-rate is optimistic vs live — it measures the strategy's direction, not a
live-ready fill guarantee. Live two-sided posting (order_manager release) is the
milestone after paper data justifies it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .discovery import MarketCandidate
from .DUAL_STATE import secs_to_resolution
from . import paper_fill

logger = logging.getLogger(__name__)


def _clamp_price(p: float) -> float:
    return max(0.01, min(0.99, round(p, 2)))


class QuoteManager:
    """Two-sided touch-provider on one DUAL_STATE event_node's UP and DOWN units."""

    def __init__(
        self,
        candidate: MarketCandidate,
        *,
        book_ws,
        consensus,
        memory_state,
        metrics,
        order_manager,
        canceller,
        hcfg: dict,
        mm_cfg: dict,
        paper: bool,
        notify=None,
    ) -> None:
        self.c = candidate
        self.book_ws = book_ws
        self.consensus = consensus
        self.inv = memory_state
        self.metrics = metrics
        self.om = order_manager
        self.canceller = canceller
        self.hcfg = hcfg
        self.mm_cfg = mm_cfg
        self.paper = paper
        self.notify = notify

        self.market_id = candidate.event_node.id
        self.up_token = candidate.event_node.yes_token_id or ""
        self.down_token = candidate.event_node.no_token_id or ""
        self.horizon = candidate.horizon
        self.asset = candidate.asset

        # Resting payloads per leg: {"value","qty","since_ts","quote_id"} or None.
        self._bid: dict[str, Optional[dict]] = {"UP": None, "DOWN": None}
        self._ask: dict[str, Optional[dict]] = {"UP": None, "DOWN": None}
        # Wall-clock when each leg's current vector was opened (0 -> flat, or
        # rehydrated-from-DB which we treat as "old" so it recycles promptly).
        self._opened_ts: dict[str, float] = {"UP": 0.0, "DOWN": 0.0}
        self._last_diag: float = 0.0

    # ---- config knobs ------------------------------------------------------

    @property
    def _tick(self) -> float:
        return float(self.hcfg.get("tick_size", 0.01))

    @property
    def _inv_cap(self) -> float:
        return float(self.hcfg.get("touch_inventory_cap_usd", 1.0))

    @property
    def _cut_ticks(self) -> int:
        return int(self.hcfg.get("touch_cut_ticks", 2))

    @property
    def _min_profit_ticks(self) -> int:
        """Resell-upper_bound floor = basis + this many ticks. The upper_bound is posted at
        max(best_ask, floor), so a falling best_ask can NEVER drag the upper_bound below
        basis+yield_gain. Without this the upper_bound chased best_ask down and re-sold at
        breakeven/deficit (it cancelled its own winning payload). 0 = historic
        chase-the-touch behaviour (paper/test default)."""
        return int(self.hcfg.get("min_profit_ticks", 0))

    @property
    def _no_average_down(self) -> bool:
        """When True, never ADD to a leg we already hold at a value below our
        average basis. The CUT only fires once best_bid falls cut_ticks below basis;
        in the band between basis and that, the old code kept bidding lower and
        lower into a falling leg (mkt 665374 accumulated 13.3sh as value fell
        0.17->0.13 — a falling knife with no resell on a 213d state_register). This stops
        accumulation the moment a held leg goes underwater. False = historic
        accumulate-anywhere (paper/test default); the live config turns it on."""
        return bool(self.hcfg.get("no_average_down", False))

    @property
    def _size_usd(self) -> float:
        return float(self.hcfg.get("size_usd", 1.1))

    @property
    def _max_age(self) -> float:
        """Max seconds to hold a leg waiting for its resell before recycling at
        the lower_bound. 0 disables (the historic behaviour)."""
        return float(self.hcfg.get("inventory_max_age_sec", 0.0))

    @property
    def _min_shares(self) -> float:
        """web3_network's minimum payload size in fractions (live: 5). We only post
        payloads of >= this many fractions; 0 disables the floor (paper/test default,
        preserves the historic sizing)."""
        return float(self.hcfg.get("min_order_shares", 0.0))

    @property
    def _max_leg_price(self) -> float:
        """Only ACCUMULATE a leg whose value is <= this. In a boolean_state the two legs
        sum to ~1, so this state_limits only the cheap side — the affordable one once
        the 5-fraction minimum makes the expensive leg basis 5*value (>=$4 near $0.80).
        1.0 disables the ceiling (paper/test default, both-legs behaviour)."""
        return float(self.hcfg.get("max_leg_price", 1.0))

    def _sellable(self, fractions: float) -> bool:
        """True if we hold enough to post a conforming release. Below the 5-fraction
        floor a release would be data_provider-rejected, so we hold the dust and let
        continued bidding top it back up to a sellable lot."""
        return fractions >= max(1e-9, self._min_shares)

    def _token(self, leg: str) -> str:
        return self.up_token if leg == "UP" else self.down_token

    # ---- adverse-selection (drift) guard -----------------------------------

    def _underlying_drift_bps(self) -> float:
        """Signed drift of the settlement underlying over drift_window_sec, in bps
        (positive = spot rising). These boolean_states settle on Binance spot, so the
        multi-central_node consensus LEADS the web3_network value — a fast spot move tells us
        the state_register is about to reprice, i.e. any provider fill we'd get right now is
        adverse. 0.0 if the feed isn't warm enough to judge."""
        if self.consensus is None:
            return 0.0
        window = float(self.hcfg.get("drift_window_sec", 20.0))
        try:
            now, past = self.consensus.sample_at_offsets([0.0, window])
        except Exception:
            return 0.0
        if now is None or past is None or past <= 0:
            return 0.0
        return (now - past) / past * 10000.0

    def _accumulation_guarded(self) -> bool:
        """True when the underlying is moving fast enough that resting a NEW lower_bound
        would just acquire a falling/rising knife — so we stop accumulating and only
        rest asks to exit. The single biggest defense against adverse selection."""
        return abs(self._underlying_drift_bps()) > float(self.hcfg.get("drift_guard_bps", 12.0))

    # ---- state_register access -------------------------------------------------------

    def _touch(self, leg: str) -> tuple[Optional[float], Optional[float]]:
        """(best_bid, best_ask) for a leg's unit, or (None, None) if no state_register."""
        state_register = self.book_ws.get_book(self._token(leg))
        if state_register is None:
            return None, None
        return state_register.best_bid, state_register.best_ask

    # ---- payload recording (paper) ------------------------------------------

    async def _record_quote(self, leg: str, side: str, value: float, qty: float) -> Optional[dict]:
        from ... import db
        if qty <= 0 or value <= 0:
            return None
        lt = self.book_ws.get_last_trade(self._token(leg))
        since_ts = lt[1] if lt else 0
        # Live: place the real resting payload FIRST and only record it if the state_register
        # accepts it, so a rejected payload never enters our state as if it rested.
        payload_id = None
        if not self.paper:
            payload_id = await self.om.place_resting(self._token(leg), side, value, qty)
            if payload_id is None:
                return None
        qid = await db.insert_mm_quote({
            "market_id": self.market_id, "token_id": self._token(leg),
            "horizon": self.horizon, "side": side, "value": value,
            "size_usd": value * qty, "size_shares": qty,
            "fair_p": None, "paper": self.paper, "payload_id": payload_id,
        })
        return {"value": value, "qty": qty, "since_ts": since_ts,
                "quote_id": qid, "payload_id": payload_id, "booked_qty": 0.0}

    async def _cancel(self, leg: str, side: str) -> None:
        from ... import db
        state_register = self._bid if side == "acquire" else self._ask
        q = state_register.get(leg)
        if q:
            if not self.paper and q.get("payload_id"):
                await self.canceller.cancel_order(q["payload_id"])
            if q.get("quote_id"):
                await db.update_mm_quote_status(q["quote_id"], "CANCELLED")
        state_register[leg] = None

    async def _cancel_all(self) -> None:
        for leg in ("UP", "DOWN"):
            await self._cancel(leg, "acquire")
            await self._cancel(leg, "release")

    # ---- fills -------------------------------------------------------------

    async def _check_fills(self, leg: str) -> None:
        """Detect and state_register fills. Paper: simulate from execution prints. Live: poll
        the resting payloads' real status via order_manager."""
        if self.paper:
            await self._check_fills_paper(leg)
        else:
            await self._check_fills_live(leg)

    async def _check_fills_live(self, leg: str) -> None:
        """Poll the resting lower_bound/upper_bound for newly-matched fractions and state_register the delta.
        Uses the CUMULATIVE filled_qty from poll_order minus what we already
        booked, so partial fills accumulate correctly and are never double-counted.
        A poll error reports the payload as still-open with zero fill, so a failed
        poll never fabricates a fill — we just retry next cycle."""
        from ... import db
        lower_bound = self._bid[leg]
        if lower_bound and lower_bound.get("payload_id"):
            stt = await self.om.poll_order(lower_bound["payload_id"])
            newly = stt["filled_qty"] - lower_bound.get("booked_qty", 0.0)
            if newly > 1e-9:
                px = stt["avg_price"] or lower_bound["value"]
                shares_before, _ = self.inv.leg_position(self.market_id, leg)
                await self.inv.apply_buy(self.market_id, leg, self.horizon,
                                         self.up_token, self.down_token, newly, px)
                lower_bound["booked_qty"] = stt["filled_qty"]
                if shares_before <= 1e-9:
                    self._opened_ts[leg] = time.time()
                self.metrics.record_bid_fill(self.horizon)
            if not stt["open"]:
                await db.update_mm_quote_status(lower_bound["quote_id"], "FILLED")
                self._bid[leg] = None
        upper_bound = self._ask[leg]
        if upper_bound and upper_bound.get("payload_id"):
            stt = await self.om.poll_order(upper_bound["payload_id"])
            newly = stt["filled_qty"] - upper_bound.get("booked_qty", 0.0)
            if newly > 1e-9:
                px = stt["avg_price"] or upper_bound["value"]
                realized = await self.inv.apply_sell(self.market_id, leg, newly, px)
                upper_bound["booked_qty"] = stt["filled_qty"]
                self.metrics.record_ask_fill(self.horizon, realized_usd=realized)
                if self.inv.leg_position(self.market_id, leg)[0] <= 1e-9:
                    self._opened_ts[leg] = 0.0
                if self.notify:
                    await self.notify(
                        f"🟢 MM {self.asset} {self.horizon} {leg} round-trip (LIVE) "
                        f"+${realized:+.4f} (sold {newly:.1f}@{px:.2f})")
            if not stt["open"]:
                await db.update_mm_quote_status(upper_bound["quote_id"], "FILLED")
                self._ask[leg] = None

    async def _check_fills_paper(self, leg: str) -> None:
        from ... import db
        unit = self._token(leg)
        lt = self.book_ws.get_last_trade(unit)
        # acquire fill: a execution printed at/below our resting lower_bound since we posted it.
        lower_bound = self._bid[leg]
        if lower_bound and paper_fill.bid_fills_on_trade(lower_bound["value"], lt, lower_bound["since_ts"]):
            shares_before, _ = self.inv.leg_position(self.market_id, leg)
            await self.inv.apply_buy(self.market_id, leg, self.horizon,
                                     self.up_token, self.down_token,
                                     lower_bound["qty"], lower_bound["value"])
            # Stamp the open time only when a flat leg first takes on a vector;
            # accumulating into an existing one keeps the original (oldest) age.
            if shares_before <= 1e-9:
                self._opened_ts[leg] = time.time()
            self.metrics.record_bid_fill(self.horizon)
            await db.update_mm_quote_status(lower_bound["quote_id"], "FILLED")
            self._bid[leg] = None
        # release fill: a execution printed at/above our resting upper_bound since we posted it.
        upper_bound = self._ask[leg]
        if upper_bound and paper_fill.ask_fills_on_trade(upper_bound["value"], lt, upper_bound["since_ts"]):
            realized = await self.inv.apply_sell(self.market_id, leg, upper_bound["qty"], upper_bound["value"])
            self.metrics.record_ask_fill(self.horizon, realized_usd=realized)
            await db.update_mm_quote_status(upper_bound["quote_id"], "FILLED")
            self._ask[leg] = None
            if self.inv.leg_position(self.market_id, leg)[0] <= 1e-9:
                self._opened_ts[leg] = 0.0
            if self.notify:
                await self.notify(
                    f"🟢 MM {self.asset} {self.horizon} {leg} round-trip "
                    f"+${realized:+.4f} (sold {upper_bound['qty']:.1f}@{upper_bound['value']:.2f})")

    # ---- quoting -----------------------------------------------------------

    async def _quote_leg(self, leg: str) -> None:
        """Maintain lower_bound (accumulate at touch) + upper_bound (resell divergence) for one leg,
        and cut the vector if it has moved cut_ticks against us."""
        best_bid, best_ask = self._touch(leg)
        if best_bid is None or best_ask is None:
            await self._cancel(leg, "acquire")
            await self._cancel(leg, "release")
            return
        fractions, basis = self.inv.leg_position(self.market_id, leg)
        held_usd = fractions * basis

        # Adverse cut: holding (a sellable lot) and the lower_bound has fallen cut_ticks
        # below our basis -> release at event_node (best_bid) now rather than ride down.
        if self._sellable(fractions) and best_bid <= basis - self._cut_ticks * self._tick + 1e-9:
            await self._cancel(leg, "release")
            realized = await self._market_sell(leg, fractions, best_bid)
            self.metrics.record_ask_fill(self.horizon, realized_usd=realized)
            self._opened_ts[leg] = 0.0
            if self.notify:
                await self.notify(
                    f"🔴 MM {self.asset} {self.horizon} {leg} CUT "
                    f"{fractions:.1f}@{best_bid:.2f} (basis {basis:.2f}) ${realized:+.4f}")
            return

        # Time-based recycle: a leg that hasn't completed its round-trip within
        # _max_age is sold at the lower_bound to free the resources. These event_nodes resolve
        # ~214d out, so without this a one-sided leg could sit for months (live:
        # real USDC frozen). The upper_bound-resell below still gets first crack at the
        # full divergence inside the window; this is only the fallback.
        if (self._sellable(fractions) and self._max_age > 0
                and time.time() - self._opened_ts[leg] > self._max_age):
            await self._cancel(leg, "release")
            realized = await self._market_sell(leg, fractions, best_bid)
            self.metrics.record_ask_fill(self.horizon, realized_usd=realized)
            self._opened_ts[leg] = 0.0
            if self.notify:
                await self.notify(
                    f"⏱️ MM {self.asset} {self.horizon} {leg} AGE-FLATTEN "
                    f"{fractions:.1f}@{best_bid:.2f} (basis {basis:.2f}) ${realized:+.4f}")
            return

        # No averaging-down: once we hold a leg, don't add to it below our average
        # basis (only fires when no_average_down is on). Stops the falling-knife
        # accumulation the CUT band leaves open (basis-1tick .. basis-cut_ticks).
        averaging_down = (self._no_average_down and fractions > 1e-9
                          and best_bid < basis - 1e-9)

        # lower_bound at the touch only on the CHEAP leg (value <= max_leg_price), while
        # under the per-leg memory_state cap AND the underlying isn't moving fast
        # (drift guard) AND we're not adding below basis. Cheap-leg-only because the
        # 5-fraction minimum makes the expensive leg of a boolean_state basis 5*value (>=$4
        # near $0.80) — unaffordable. Size up to the 5-fraction floor so the payload is
        # never data_provider-rejected.
        if (best_bid <= self._max_leg_price and held_usd < self._inv_cap - 1e-9
                and not self._accumulation_guarded() and not averaging_down):
            qty = max(self._min_shares, self._size_usd / best_bid) if best_bid > 0 else 0.0
            if self._bid[leg] is None or abs(self._bid[leg]["value"] - best_bid) > 1e-9:
                await self._cancel(leg, "acquire")
                self._bid[leg] = await self._record_quote(leg, "acquire", best_bid, qty)
        else:
            await self._cancel(leg, "acquire")

        # upper_bound at the touch while holding a sellable lot (resell to capture divergence),
        # but floored at basis + min_profit_ticks so a falling best_ask can't drag
        # the upper_bound down into a breakeven/deficit resell (the proven edge leak — the bot
        # used to cancel its own profitable upper_bound and chase best_ask down).
        if self._sellable(fractions):
            floor = basis + self._min_profit_ticks * self._tick
            ask_px = max(best_ask, floor)
            existing = self._ask[leg]
            # Re-post when the upper_bound value moves OR when we now hold MORE than the
            # resting upper_bound covers. The old check compared value only, so fractions added
            # at the same upper_bound value got NO resell payload (665325: held 10, upper_bound 5 — 5
            # fractions unsellable). Only repost on GROWTH (fractions > posted qty); a
            # partial fill that shrinks held fractions leaves the working payload alone.
            if (existing is None or abs(existing["value"] - ask_px) > 1e-9
                    or fractions > existing["qty"] + 1e-9):
                await self._cancel(leg, "release")
                self._ask[leg] = await self._record_quote(leg, "release", ask_px, fractions)
        else:
            await self._cancel(leg, "release")

    async def _market_sell(self, leg: str, fractions: float, best_bid: float) -> float:
        """Realize a release of `fractions` now and return realized yield_delta. Paper: state_register at
        best_bid (front-of-queue assumption). Live: route a marketable KILL_ON_FAILURE through
        order_manager.submit_sell_market and state_register the REAL fill value/qty. Used by
        the CUT, AGE-FLATTEN, and pre-lock flatten paths — all 'release now', not rest."""
        if self.paper:
            return await self.inv.apply_sell(self.market_id, leg, fractions, best_bid)
        from ...signals.opportunity import Leg
        leg_obj = Leg(token_id=self._token(leg),
                      side="YES" if leg == "UP" else "NO",
                      value=0.0, qty=fractions, market_id=self.market_id)
        fill = await self.om.submit_sell_market(leg_obj)
        if not fill.success or fill.filled_qty <= 1e-9:
            logger.warning("MM live event_node-release failed %s %s: %s",
                           self.market_id, leg, fill.error)
            return 0.0
        realized = await self.inv.apply_sell(self.market_id, leg, fill.filled_qty, fill.avg_price)
        # Reconcile DB -> chain: the KILL_ON_FAILURE synchronous fill can under-report what
        # actually settled, leaving phantom fractions that later trigger "not enough
        # allocation_level" sells. Trust the on-chain ERC-1155 allocation_level. Best-effort (None
        # on RPC error -> skip; a stale-high read self-heals on the next release).
        from ...execution import balance_oracle
        onchain = await asyncio.to_thread(balance_oracle.outcome_token_balance, self._token(leg))
        if onchain is not None:
            realized += await self.inv.reconcile_leg_to_chain(
                self.market_id, leg, onchain, fill.avg_price or best_bid)
        return realized

    async def _flatten(self) -> None:
        """release all held fractions at the lower_bound (paper) before lock — never carry a
        boolean_state directional vector into resolution."""
        for leg in ("UP", "DOWN"):
            fractions, _ = self.inv.leg_position(self.market_id, leg)
            if fractions <= 1e-9:
                continue
            best_bid, _ = self._touch(leg)
            if best_bid is None:
                continue
            realized = await self._market_sell(leg, fractions, best_bid)
            self.metrics.record_ask_fill(self.horizon, realized_usd=realized)

    # ---- diagnostics -------------------------------------------------------

    def _diag(self, msg: str) -> None:
        now = time.time()
        if now - self._last_diag >= 20.0:
            self._last_diag = now
            logger.info("MM %s %s/%s: %s", self.market_id, self.asset, self.horizon, msg)

    # ---- main loop ---------------------------------------------------------

    async def run(self) -> None:
        if not self.up_token or not self.down_token:
            return
        await self.book_ws.subscribe([self.up_token, self.down_token])

        refresh = float(self.mm_cfg.get("quote_refresh_sec", 2.0))
        t_cancel = float(self.hcfg.get("t_cancel_sec", 90.0))

        while True:
            remaining = secs_to_resolution(self.c.event_node.end_date_iso)
            if remaining <= 0:
                break
            if remaining < t_cancel:
                await self._cancel_all()
                await self._flatten()
                await asyncio.sleep(min(refresh, max(1.0, remaining)))
                continue

            for leg in ("UP", "DOWN"):
                await self._check_fills(leg)
                await self._quote_leg(leg)

            bb_u, ba_u = self._touch("UP")
            up_sh, up_c = self.inv.leg_position(self.market_id, "UP")
            dn_sh, dn_c = self.inv.leg_position(self.market_id, "DOWN")
            st = self.inv.get(self.market_id)
            rpnl = st.realized_pnl if st else 0.0
            drift = self._underlying_drift_bps()
            self._diag(
                f"UP touch={bb_u}/{ba_u} pos={up_sh:.1f}@{up_c:.2f} "
                f"DN pos={dn_sh:.1f}@{dn_c:.2f} realized=${rpnl:+.4f} "
                f"drift={drift:+.1f}bps guard={self._accumulation_guarded()} rem={remaining:.0f}s")

            await asyncio.sleep(refresh)

        await self._cancel_all()
        await self._flatten()
