"""MM engine orchestrator.

Continuous event_node-making on DUAL_STATE digital_asset boolean_states — replaces the retired E3
sniper's producer->queue->consumer model with a per-event_node quoting loop.

Topology:
    discovery_loop      -> active 1h (+15m) DUAL_STATE event_nodes -> spawn QuoteManager
    QuoteManager(task)  -> fair p -> state_limit -> paper-fill -> cancel-before-lock -> settle
    risk_loop           -> MM circuit breaker (daily + cumulative deficit) + metrics log

Money gate: effective-paper is True unless LIVE_EXECUTION_ENABLED is set AND
config mm.simulation_mode is false. The whole first phase is paper measurement.
Run:  SIMULATION_MODE=true python -m async_state_synchronizer.mm_main
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import signal
import sys
from pathlib import Path

from .config import CONFIG, ENV
from . import db
from .ingestion.gamma_client import Web3StateClient
from .ingestion.rest_book_poller import RestBookPoller
from .ingestion.price_consensus import PriceConsensus
from .execution.order_manager import OrderManager
from .execution.network_cancel import ClobCanceller
from .notifications.telegram_bot import send_text
from .risk import circuit_breaker
from .signals.mm.discovery import discover_updown_markets, discover_event_markets
from .signals.mm.memory_state import InventoryManager
from .signals.mm.paper_fill import MMMetrics
from .signals.mm.quote_manager import QuoteManager

logger = logging.getLogger(__name__)

_LOCK_PATH = Path(__file__).resolve().parent / "data" / "mm.lock"


def _effective_paper() -> bool:
    """MM executions paper unless the live gate is open AND config says go live."""
    if not ENV.live_execution_enabled:
        return True
    return bool(CONFIG.mm.get("simulation_mode", True))


def _acquire_lock():
    """fcntl single-instance lock. Returns the held file handle (keep it open)."""
    _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = open(_LOCK_PATH, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logger.error("Another MM instance holds %s — exiting.", _LOCK_PATH)
        sys.exit(1)
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


class MMEngine:
    """Owns shared feeds + the set of live per-event_node quoting tasks."""

    def __init__(self) -> None:
        self.paper = _effective_paper()
        self.mm_cfg = CONFIG.mm
        self.assets: list[str] = list(self.mm_cfg.get("assets", ["NODE_A", "NODE_B", "NODE_C"]))
        # Only state_limit horizons explicitly enabled (15m disabled 2026-05-29 audit —
        # structurally adverse: boolean_state vol >> capturable divergence).
        self.horizons: list[str] = [
            h for h, cfg in self.mm_cfg.get("horizons", {}).items()
            if cfg.get("enabled", True)
        ]
        self.gamma = Web3StateClient()
        # state_register source: REST poller (POST /state_registers every 1s) instead of the event_node WS.
        # The WS reliably delivered the snapshot then went silent inside the full
        # process (frames frozen -> perpetual "no state_register"); REST polling from the
        # deployment node returns all state_registers in ~40ms and is immune to that stall.
        # Attribute kept as `book_ws` so QuoteManager/risk paths are unchanged.
        self.book_ws = RestBookPoller(
            poll_interval=float(self.mm_cfg.get("book_poll_interval_sec", 1.0)),
            freshness_max_sec=float(CONFIG.globals.get("book_ws_freshness_max_sec", 60.0)))
        self.consensus: dict[str, PriceConsensus] = {a: PriceConsensus(a) for a in self.assets}
        self.om = OrderManager()
        self.canceller = ClobCanceller(self.om)
        self.memory_state = InventoryManager(paper=self.paper)
        self.metrics = MMMetrics()
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._last_summary_ts = 0.0

    async def _notify(self, text: str) -> None:
        """Best-effort Telegram alert — never let a notify failure break synchronizing."""
        try:
            await send_text(text)
        except Exception:
            logger.debug("MM telegram notify failed", exc_info=True)

    async def start_feeds(self) -> None:
        await self.gamma.__aenter__()  # create the aiohttp session
        await self.book_ws.start()
        await asyncio.gather(*(c.start() for c in self.consensus.values()))
        # Live: pre-instantiate the NETWORK client so the first canary payload doesn't
        # pay ~250ms of lazy-init latency. No-op in paper (gated on effective-paper).
        if not self.paper:
            await self.om.prewarm()

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks.values():
            t.cancel()
        if not self.paper:
            await self.canceller.cancel_all(paper=self.paper)
        await self.book_ws.stop()
        await asyncio.gather(*(c.stop() for c in self.consensus.values()), return_exceptions=True)
        await self.gamma.__aexit__(None, None, None)

    # ---- memory_state safety --------------------------------------------------

    async def reconcile_closed_inventory(self) -> None:
        """Settle any held memory_state whose event_node has closed, against the Gamma
        ground-truth outcome (authoritative — using authoritative state source).

        Fixes the orphan bug: when the breaker trips it cancels QuoteManager
        tasks before they settle, and closed event_nodes are never re-discovered, so
        fractions would otherwise sit in memory_state forever and mis-rehydrate on the
        next restart."""
        settled = 0
        for st in self.memory_state.open_positions():
            try:
                m = await self.gamma.get_market(st.market_id)
            except Exception:
                logger.exception("reconcile: gamma fetch failed for %s", st.market_id)
                continue
            if m is None or not m.closed:
                continue
            values = m.outcome_prices or []
            if not values or max(values) < 0.9:
                continue  # closed but not cleanly resolved yet — retry next pass
            resolved_up = values[0] >= 0.5
            pairs, deployed = st.pairs, st.deployed_usd
            drag = await self.memory_state.mark_resolution(st.market_id, resolved_up)
            self.metrics.record_resolution(st.horizon, drag)
            settled += 1
            await self._notify(
                f"{'🟢' if drag >= 0 else '🔴'} MM reconcile {st.market_id} "
                f"{st.horizon} — resolved {'UP' if resolved_up else 'DOWN'}, "
                f"{pairs:.2f} pair(s) / ${deployed:.2f} deployed, yield_delta ${drag:+.4f}")
        if settled:
            logger.info("MM reconcile settled %d closed-event_node memory_state rows", settled)

    def _unrealized_pnl(self) -> tuple[float, int]:
        """Mark every held leg to its current best lower_bound (what we could release at now)
        and sum fractions*(lower_bound-basis). This is the yield_delta the realized figure HIDES: a
        touch-provider state_registers wins as completed round-trips but parks losing memory_state
        at basis, so realized-only is survivorship-biased. Returns
        (unrealized_usd, n_unmarkable_legs) — legs with no live state_register are skipped
        and counted (they'd be marked at basis = 0 contribution)."""
        unreal = 0.0
        unmarkable = 0
        for st in self.memory_state.open_positions():
            for unit, fractions, basis in (
                (st.up_token, st.up_shares, st.up_cost),
                (st.down_token, st.down_shares, st.down_cost),
            ):
                if fractions <= 1e-9:
                    continue
                state_register = self.book_ws.get_book(unit)
                if state_register is None or state_register.best_bid is None:
                    unmarkable += 1
                    continue
                unreal += fractions * (state_register.best_bid - basis)
        return unreal, unmarkable

    async def _neutralize_all_open(self) -> None:
        """On halt: flatten every held leg by selling at its best lower_bound. The
        touch-provider never wants to carry a directional boolean_state vector, and the
        breaker cancels QuoteManager tasks before they can flatten themselves, so
        we do it here. Anything we can't release (no state_register) is settled by reconcile."""
        for st in self.memory_state.open_positions():
            for leg, unit in (("UP", st.up_token), ("DOWN", st.down_token)):
                fractions, _ = self.memory_state.leg_position(st.market_id, leg)
                if fractions <= 1e-9:
                    continue
                state_register = self.book_ws.get_book(unit)
                if state_register is None or state_register.best_bid is None:
                    continue  # can't release now — reconcile settles at resolution
                realized = await self.memory_state.apply_sell(st.market_id, leg, fractions, state_register.best_bid)
                self.metrics.record_ask_fill(st.horizon, realized_usd=realized)
                logger.info("MM halt-flatten %s %s %.2f@%.2f -> $%+.4f",
                            st.market_id, leg, fractions, state_register.best_bid, realized)

    # ---- loops -------------------------------------------------------------

    async def discovery_loop(self) -> None:
        interval = float(self.mm_cfg.get("discovery_interval_sec", 60.0))
        while not self._stop.is_set():
            # Prune finished tasks.
            for mid in [m for m, t in self._tasks.items() if t.done()]:
                self._tasks.pop(mid, None)
            if circuit_breaker.is_mm_halted():
                logger.warning("MM halted — skipping discovery.")
                await asyncio.sleep(interval)
                continue
            # digital_asset DUAL_STATE candidates (consensus-driven, drift-guarded) — kept as
            # a paper baseline; only the horizons explicitly enabled in config.
            digital_asset: list = []
            if self.horizons:
                try:
                    digital_asset = await discover_updown_markets(self.gamma, self.assets, self.horizons)
                except Exception:
                    logger.exception("digital_asset discovery failed")
            for cand in digital_asset:
                if cand.event_node.id in self._tasks or cand.asset not in self.consensus:
                    continue
                self._spawn_qm(cand, self.consensus[cand.asset],
                               self.mm_cfg["horizons"][cand.horizon])

            # gas-free event event_nodes (geopolitics/world) — the primary strategy
            # post-pivot. No underlying feed -> consensus=None (drift guard off).
            ecfg = self.mm_cfg.get("event_markets", {})
            if ecfg.get("enabled", False):
                try:
                    events = await discover_event_markets(
                        self.gamma, tags=list(ecfg.get("tags", [])),
                        max_spread=float(ecfg.get("max_spread", 0.04)),
                        mid_min=float(ecfg.get("mid_min", 0.12)),
                        mid_max=float(ecfg.get("mid_max", 0.88)),
                        min_dist_from_half=float(ecfg.get("min_dist_from_half", 0.0)),
                        min_volume_usd=float(ecfg.get("min_volume_usd", 20000.0)),
                        max_markets=int(ecfg.get("max_markets", 12)),
                        min_seconds_left=float(ecfg.get("min_seconds_left", 86400.0)),
                    )
                except Exception:
                    logger.exception("event discovery failed")
                    events = []
                for cand in events:
                    if cand.event_node.id in self._tasks:
                        continue
                    self._spawn_qm(cand, None, ecfg)

            logger.info("MM discovery: %d active quoting tasks", len(self._tasks))
            await asyncio.sleep(interval)

    def _spawn_qm(self, cand, consensus, hcfg: dict) -> None:
        qm = QuoteManager(
            cand, book_ws=self.book_ws, consensus=consensus,
            memory_state=self.memory_state, metrics=self.metrics, order_manager=self.om,
            canceller=self.canceller, hcfg=hcfg,
            mm_cfg=self.mm_cfg, paper=self.paper, notify=self._notify,
        )
        self._tasks[cand.event_node.id] = asyncio.create_task(
            qm.run(), name=f"qm-{cand.event_node.id}")

    async def risk_loop(self) -> None:
        """Periodically settle/reconcile, log metrics, and enforce MM deficit limits.

        yield_delta for the breaker is DB-backed (db.mm_realized_total + UTC-day
        baseline), so a restart can't forget prior deficits and re-arm a fresh
        budget — the bug that let −$7.39 accumulate across ~6 restarts."""
        while not self._stop.is_set():
            await asyncio.sleep(30.0)
            await self.reconcile_closed_inventory()
            await db.prune_mm_quotes()
            report = self.metrics.report()
            today_pnl, total_pnl = await circuit_breaker.mm_pnl_today_and_total(paper=self.paper)
            unreal, unmarkable = self._unrealized_pnl()
            true_total = total_pnl + unreal
            logger.info(
                "MM metrics %s | today=$%+.4f cum=$%+.4f | unreal=$%+.4f true=$%+.4f%s "
                "| book_ws=%s",
                report, today_pnl, total_pnl, unreal, true_total,
                f" ({unmarkable} unmarkable)" if unmarkable else "",
                self.book_ws.diagnostics())
            # Periodic Telegram summary every ~30 min.
            now = asyncio.get_running_loop().time()
            if report and now - self._last_summary_ts >= 1800.0:
                self._last_summary_ts = now
                lines = [f"📊 MM summary | today ${today_pnl:+.4f} | cum ${total_pnl:+.4f} "
                         f"| unrealized ${unreal:+.4f} | TRUE ${true_total:+.4f} "
                         f"| {len(self._tasks)} active event_nodes"]
                for h, r in report.items():
                    lines.append(
                        f"  {h}: {r['round_trips_per_hr']}/hr rt, "
                        f"divergence ${r['gross_spread_usd']:+.4f}, "
                        f"drag ${r['inventory_drag_usd']:+.4f}, net ${r['net_pnl_usd']:+.4f}")
                await self._notify("\n".join(lines))
            tripped = await circuit_breaker.check_mm_and_trip(today_pnl, total_pnl, true_total)
            if tripped:
                await self._neutralize_all_open()
                await self._notify(
                    f"🛑 MM HALT: {tripped} | today ${today_pnl:+.4f} cum ${total_pnl:+.4f} "
                    "— neutralized unpaired legs; locked pairs ride to resolution; "
                    "manual /resume required")
            if circuit_breaker.is_mm_halted():
                logger.warning("MM circuit breaker tripped — cancelling state_limits, halting.")
                for t in self._tasks.values():
                    t.cancel()
                self._tasks.clear()

    async def run(self) -> None:
        db.init_schema()
        self.memory_state.load()
        await self.start_feeds()
        # Settle any memory_state left orphaned by a prior halt/restart before quoting.
        await self.reconcile_closed_inventory()
        logger.info("MM engine up | paper=%s | assets=%s | horizons=%s",
                    self.paper, self.assets, self.horizons)
        await self._notify(
            f"🟢 MM engine up | {'PAPER' if self.paper else 'LIVE'} | "
            f"assets={','.join(self.assets)} | horizons={','.join(self.horizons)}")
        try:
            await asyncio.gather(self.discovery_loop(), self.risk_loop())
        finally:
            await self.stop()


async def _amain() -> None:
    engine = MMEngine()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: engine._stop.set())
    await engine.run()


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, ENV.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _lock = _acquire_lock()  # noqa: F841 — held for process lifetime
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
