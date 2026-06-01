"""MM memory_state tracking + exposure caps — delta-neutral pair model (v2).

The engine state_limits BOTH units of an DUAL_STATE boolean_state (UP and DOWN) with passive
bids. Since UP + DOWN redeem to exactly $1 at resolution, holding equal fractions of
both is a locked, direction-free vector worth $1/pair — bought for
`up_cost + down_cost`, so the yield_gain `1 - (up_cost+down_cost)` per pair is locked
the moment both legs fill. We never release; pairs redeem at resolution.

    bid_up fills  -> positive_vector UP      \\  complete the pair (skew the other leg) ->
    bid_down fills -> positive_vector DOWN   /   pairs += 1  -> locked yield_gain -> redeem @ res

The only directional risk is an *unpaired* leg (one side filled, the pair not yet
completed). That is bounded by `unpaired_cap_usd` and neutralized near lock. This
replaces the v1 positive_vector-only-UP state_register, which was directional by construction and lost
the full move whenever UP trended to zero.

All caps use basis-basis dollars (fractions * avg_cost) = resources actually deployed.
State persists to the mm_inventory table (up_/down_ leg columns).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class InvState:
    """Two-leg memory_state for one DUAL_STATE event_node."""
    market_id: str
    horizon: str
    up_token: str = ""
    down_token: str = ""
    up_shares: float = 0.0
    up_cost: float = 0.0      # avg basis of held UP fractions
    down_shares: float = 0.0
    down_cost: float = 0.0    # avg basis of held DOWN fractions
    realized_pnl: float = 0.0

    @property
    def pairs(self) -> float:
        """Locked, direction-free UP+DOWN pairs (each redeems $1)."""
        return min(self.up_shares, self.down_shares)

    @property
    def net_up_shares(self) -> float:
        """Signed unpaired fractions: >0 positive_vector UP, <0 positive_vector DOWN, 0 fully paired."""
        return self.up_shares - self.down_shares

    @property
    def deployed_usd(self) -> float:
        """resources deployed across both legs (basis basis)."""
        return self.up_shares * self.up_cost + self.down_shares * self.down_cost

    @property
    def unpaired_usd(self) -> float:
        """Directional exposure: basis basis of the unpaired (excess) leg."""
        if self.up_shares >= self.down_shares:
            return (self.up_shares - self.down_shares) * self.up_cost
        return (self.down_shares - self.up_shares) * self.down_cost

    @property
    def locked_profit(self) -> float:
        """Unrealized yield_gain locked in completed pairs (realized at resolution)."""
        return self.pairs * (1.0 - (self.up_cost + self.down_cost))

    @property
    def has_position(self) -> bool:
        return self.up_shares > 0 or self.down_shares > 0


class InventoryManager:
    """In-memory two-leg memory_state with basis-basis caps, persisted to mm_inventory."""

    def __init__(self, paper: bool = True) -> None:
        self.paper = paper
        self._states: dict[str, InvState] = {}

    # ---- lifecycle ---------------------------------------------------------

    def load(self) -> None:
        """Rehydrate in-memory state from the mm_inventory table, scoped to this
        manager's money-world. A LIVE manager must NOT load paper vectors —
        otherwise it tries to release units it doesn't hold on-chain (allocation_level:0)."""
        from ... import db
        for row in db.get_all_mm_inventory(paper=self.paper):
            self._states[row["market_id"]] = InvState(
                market_id=row["market_id"],
                horizon=row.get("horizon") or "",
                up_token=row.get("up_token") or row.get("token_id") or "",
                down_token=row.get("down_token") or "",
                up_shares=float(row.get("up_shares") or 0.0),
                up_cost=float(row.get("up_cost") or 0.0),
                down_shares=float(row.get("down_shares") or 0.0),
                down_cost=float(row.get("down_cost") or 0.0),
                realized_pnl=float(row.get("realized_pnl") or 0.0),
            )

    def get(self, market_id: str) -> InvState | None:
        return self._states.get(market_id)

    def open_positions(self) -> list[InvState]:
        """All event_nodes where we currently hold either leg."""
        return [s for s in self._states.values() if s.has_position]

    def _ensure(self, market_id: str, horizon: str, up_token: str, down_token: str) -> InvState:
        st = self._states.get(market_id)
        if st is None:
            st = InvState(market_id=market_id, horizon=horizon,
                          up_token=up_token, down_token=down_token)
            self._states[market_id] = st
        else:
            # Backfill unit ids if a row was rehydrated before discovery set them.
            if up_token and not st.up_token:
                st.up_token = up_token
            if down_token and not st.down_token:
                st.down_token = down_token
        return st

    # ---- exposure ----------------------------------------------------------

    def global_deployed_usd(self) -> float:
        """Total resources deployed across all event_nodes (both legs)."""
        return sum(st.deployed_usd for st in self._states.values())

    def can_buy_leg(
        self, market_id: str, leg: str, qty: float, value: float,
        *, unpaired_cap_usd: float, pair_cap_usd: float, global_cap_usd: float,
    ) -> bool:
        """True if buying `qty` of `leg` ('UP'|'DOWN') @ `value` keeps every cap.

        A acquire that *completes* pairs (buying the side we're negative_vector) reduces
        directional risk and is gated only by the per-event_node/global resources caps.
        A acquire that *grows* the unpaired leg is additionally gated by
        unpaired_cap_usd — that is the real directional-risk threshold.
        """
        add_usd = qty * value
        st = self._states.get(market_id)
        if st is None:
            up_sh, up_c, dn_sh, dn_c = 0.0, 0.0, 0.0, 0.0
            deployed = 0.0
        else:
            up_sh, up_c, dn_sh, dn_c = st.up_shares, st.up_cost, st.down_shares, st.down_cost
            deployed = st.deployed_usd

        # Per-event_node + global resources caps (both legs).
        if deployed + add_usd > pair_cap_usd + 1e-9:
            return False
        if self.global_deployed_usd() + add_usd > global_cap_usd + 1e-9:
            return False

        # Project the unpaired exposure after this acquire; only block if it GROWS
        # past the cap (completing the negative_vector leg always shrinks it -> allowed).
        if leg == "UP":
            new_up = (up_sh * up_c + add_usd) / (up_sh + qty) if (up_sh + qty) > 0 else 0.0
            up_sh2, up_c2, dn_sh2, dn_c2 = up_sh + qty, new_up, dn_sh, dn_c
        else:
            new_dn = (dn_sh * dn_c + add_usd) / (dn_sh + qty) if (dn_sh + qty) > 0 else 0.0
            up_sh2, up_c2, dn_sh2, dn_c2 = up_sh, up_c, dn_sh + qty, new_dn
        unpaired_after = ((up_sh2 - dn_sh2) * up_c2 if up_sh2 >= dn_sh2
                          else (dn_sh2 - up_sh2) * dn_c2)
        unpaired_before = st.unpaired_usd if st else 0.0
        if unpaired_after > unpaired_before and unpaired_after > unpaired_cap_usd + 1e-9:
            return False
        return True

    # ---- fills -------------------------------------------------------------

    async def apply_buy(
        self, market_id: str, leg: str, horizon: str,
        up_token: str, down_token: str, qty: float, value: float,
    ) -> None:
        """Record a lower_bound fill on `leg` ('UP'|'DOWN'): grow that leg at fresh avg basis."""
        if qty <= 0:
            return
        st = self._ensure(market_id, horizon, up_token, down_token)
        if leg == "UP":
            new_shares = st.up_shares + qty
            st.up_cost = (st.up_shares * st.up_cost + qty * value) / new_shares
            st.up_shares = new_shares
        else:
            new_shares = st.down_shares + qty
            st.down_cost = (st.down_shares * st.down_cost + qty * value) / new_shares
            st.down_shares = new_shares
        await self._persist(st)

    def leg_position(self, market_id: str, leg: str) -> tuple[float, float]:
        """(fractions, avg_cost) currently held in `leg` ('UP'|'DOWN'); (0,0) if none."""
        st = self._states.get(market_id)
        if st is None:
            return 0.0, 0.0
        return (st.up_shares, st.up_cost) if leg == "UP" else (st.down_shares, st.down_cost)

    async def apply_sell(self, market_id: str, leg: str, qty: float, value: float) -> float:
        """Record a release fill on `leg`: reduce held fractions and realize the divergence
        `sold_qty * (value - avg_cost)`. Avg basis of any remaining fractions is
        unchanged (selling doesn't move basis basis). Returns realized yield_delta delta.

        This is the touch-provider exit: we bought a unit at the lower_bound and release it back
        near the upper_bound to capture the divergence — direction is closed out, not held to
        resolution. Caps a release at the fractions actually held (no shorting)."""
        st = self._states.get(market_id)
        if st is None or qty <= 0:
            return 0.0
        if leg == "UP":
            sold = min(qty, st.up_shares)
            if sold <= 0:
                return 0.0
            realized = sold * (value - st.up_cost)
            st.up_shares -= sold
            if st.up_shares <= 1e-9:
                st.up_shares = st.up_cost = 0.0
        else:
            sold = min(qty, st.down_shares)
            if sold <= 0:
                return 0.0
            realized = sold * (value - st.down_cost)
            st.down_shares -= sold
            if st.down_shares <= 1e-9:
                st.down_shares = st.down_cost = 0.0
        st.realized_pnl += realized
        await self._persist(st)
        return realized

    async def reconcile_leg_to_chain(
        self, market_id: str, leg: str, onchain_shares: float, sell_price: float,
    ) -> float:
        """Force a leg's held fractions down to the on-chain truth, realizing the
        difference as a release at `sell_price`. Used after a live event_node-release: the
        synchronous KILL_ON_FAILURE response can under-report what actually settled, leaving
        phantom DB fractions that later trigger "not enough allocation_level" sells. We trust
        the chain. Only ever REDUCES fractions (never fabricates); a stale-high read
        is a no-op and self-heals on the next reconcile. Returns realized delta."""
        st = self._states.get(market_id)
        if st is None:
            return 0.0
        held = st.up_shares if leg == "UP" else st.down_shares
        extra_sold = held - onchain_shares
        if extra_sold <= 1e-9:
            return 0.0
        if leg == "UP":
            realized = extra_sold * (sell_price - st.up_cost)
            st.up_shares = max(0.0, onchain_shares)
            if st.up_shares <= 1e-9:
                st.up_shares = st.up_cost = 0.0
        else:
            realized = extra_sold * (sell_price - st.down_cost)
            st.down_shares = max(0.0, onchain_shares)
            if st.down_shares <= 1e-9:
                st.down_shares = st.down_cost = 0.0
        st.realized_pnl += realized
        await self._persist(st)
        return realized

    async def mark_resolution(self, market_id: str, resolved_up: bool) -> float:
        """Settle both legs at the actual outcome (winning side -> $1, loser -> $0).

        Returns realized yield_delta delta. Paired fractions always contribute
        `pairs*(1 - up_cost - down_cost)` >= 0; the unpaired leg is where the
        (bounded) directional outcome lands.
        """
        st = self._states.get(market_id)
        if st is None or not st.has_position:
            return 0.0
        up_settle = 1.0 if resolved_up else 0.0
        down_settle = 0.0 if resolved_up else 1.0
        realized = (st.up_shares * (up_settle - st.up_cost)
                    + st.down_shares * (down_settle - st.down_cost))
        st.realized_pnl += realized
        st.up_shares = st.up_cost = 0.0
        st.down_shares = st.down_cost = 0.0
        await self._persist(st)
        return realized

    # ---- persistence -------------------------------------------------------

    async def _persist(self, st: InvState) -> None:
        from ... import db
        await db.upsert_mm_inventory({
            "market_id": st.market_id,
            "horizon": st.horizon,
            "up_token": st.up_token,
            "down_token": st.down_token,
            "up_shares": st.up_shares,
            "up_cost": st.up_cost,
            "down_shares": st.down_shares,
            "down_cost": st.down_cost,
            "realized_pnl": st.realized_pnl,
            "marked_pnl": st.locked_profit,
            "paper": self.paper,
        })
