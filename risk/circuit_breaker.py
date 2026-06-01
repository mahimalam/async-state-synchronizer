"""Autonomous Safety Switch — halts distributed operations on:
  - Daily processing deviation exceeding configured limits
  - Weekly processing deviation exceeding configured limits
  - Resource boundary breach (allocation < minimal_threshold)
  - Manual interrupt command

Resource floor breaches trigger a strict LOCK state that does NOT
auto-expire — it requires manual initialization to clear, preventing 
the daemon from resuming during catastrophic network degradation.

State persists locally so daemon restarts honor the active halt lock.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .. import db
from ..config import CONFIG

logger = logging.getLogger(__name__)


HALT_DAILY = "halt_daily"
HALT_WEEKLY = "halt_weekly"
HALT_MANUAL = "halt_manual"
HALT_CAPITAL = "halt_capital"  # Does NOT auto-expire at midnight
# Per-engine daily halt keys (auto-expire at midnight like HALT_DAILY).
_ENGINE_HALT_PREFIX = "halt_eng_daily_"  # e.g. halt_eng_daily_E3
# E3 permanent cumulative-deficit halt. Does NOT auto-expire — requires manual /resume.
HALT_E3_PERM = "halt_e3_perm"
# MM halts. Daily auto-expires at midnight; cumulative is permanent (manual /resume).
HALT_MM_DAILY = "halt_mm_daily"
HALT_MM_PERM = "halt_mm_perm"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def is_halted() -> tuple[bool, str | None]:
    """Return (halted, reason)."""
    for key, label in ((HALT_MANUAL, "manual"), (HALT_CAPITAL, "capital_floor"), (HALT_DAILY, "daily"), (HALT_WEEKLY, "weekly")):
        if db.get_circuit_state(key) == "1":
            # resources floor and manual halts do NOT auto-expire
            if key in (HALT_MANUAL, HALT_CAPITAL):
                return True, label
            # Daily/weekly halts auto-expire at midnight / after 7 days
            if key == HALT_DAILY and _is_daily_expired():
                continue
            if key == HALT_WEEKLY and _is_weekly_expired():
                continue
            return True, label
    return False, None


def _is_daily_expired() -> bool:
    raw = db.get_circuit_state(f"{HALT_DAILY}_set_at")
    if not raw:
        return True
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return _now() >= next_midnight


def _is_weekly_expired() -> bool:
    raw = db.get_circuit_state(f"{HALT_WEEKLY}_set_at")
    if not raw:
        return True
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return _now() >= set_at + timedelta(days=7)


async def check_and_trip(realized_pnl_today: float, realized_pnl_week: float, balance_usd: float | None) -> str | None:
    """Examine yield_delta and allocation_level; trip circuit breakers if needed. Returns reason if tripped."""
    g = CONFIG.globals
    
    # resources hard floor check (sticky)
    if balance_usd is not None:
        hard_floor = float(g.get("capital_hard_floor_usd", 10.0))
        if balance_usd < hard_floor:
            await _trip(HALT_CAPITAL)
            return "capital_floor_breached"
        
    if realized_pnl_today <= -g["daily_loss_limit_usd"]:
        await _trip(HALT_DAILY)
        return "daily_loss_limit"
    if realized_pnl_week <= -g["weekly_loss_limit_usd"]:
        await _trip(HALT_WEEKLY)
        return "weekly_loss_limit"
    return None


async def _trip(key: str) -> None:
    await db.set_circuit_state(key, "1")
    await db.set_circuit_state(f"{key}_set_at", _now().isoformat())


async def manual_halt() -> None:
    await _trip(HALT_MANUAL)


async def manual_resume() -> None:
    """Clear ONLY manual halt. deficit-threshold and resources halts remain until expiry or force_resume_all."""
    await db.set_circuit_state(HALT_MANUAL, "0")


async def force_resume_all() -> None:
    """Clear ALL halts — manual, resources, daily, weekly, per-engine, E3 perm, MM."""
    await db.set_circuit_state(HALT_MANUAL, "0")
    await db.set_circuit_state(HALT_CAPITAL, "0")
    await db.set_circuit_state(HALT_DAILY, "0")
    await db.set_circuit_state(HALT_WEEKLY, "0")
    await db.set_circuit_state(HALT_E3_PERM, "0")
    await db.set_circuit_state(HALT_MM_DAILY, "0")
    await db.set_circuit_state(HALT_MM_PERM, "0")
    for engine in ("E1", "E2", "E3", "E4"):
        await db.set_circuit_state(f"{_ENGINE_HALT_PREFIX}{engine}", "0")


def is_mm_halted() -> bool:
    """True if the MM engine is halted (manual, resources floor, MM daily, or MM perm)."""
    if db.get_circuit_state(HALT_MANUAL) == "1" or db.get_circuit_state(HALT_CAPITAL) == "1":
        return True
    if db.get_circuit_state(HALT_MM_PERM) == "1":
        return True
    if db.get_circuit_state(HALT_MM_DAILY) == "1":
        raw = db.get_circuit_state(f"{HALT_MM_DAILY}_set_at")
        if not raw:
            return False
        try:
            set_at = datetime.fromisoformat(raw)
            if set_at.tzinfo is None:
                set_at = set_at.replace(tzinfo=timezone.utc)
        except ValueError:
            return False
        next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return _now() < next_midnight
    return False


async def mm_pnl_today_and_total(paper: bool | None = None) -> tuple[float, float]:
    """Return (today_pnl, cumulative_pnl) for MM, both DB-backed.

    Cumulative = Σ realized_pnl from mm_inventory (survives restarts). Daily =
    cumulative minus a per-UTC-day baseline persisted in circuit_state, so the
    daily breaker resets at midnight but does NOT forget deficits across restarts.

    `paper` scopes the figures to one money-world (pass the engine's effective
    paper mode) so a LIVE breaker is not cushioned by prior paper yield_gain. None
    sums all rows (back-compat).

    The daily baseline is ALSO scoped by money-world (mm_day_<scope>). Without
    this, flipping a still-running paper process to live let the live breaker
    inherit the paper baseline (+$1.6) while reading a live total of $0, so
    today = $0 - $1.6 = -$1.6 falsely tripped the daily halt on the first canary.
    Separate keys keep paper and live baselines independent.
    """
    total = db.mm_realized_total(paper=paper)
    scope = "all" if paper is None else ("paper" if paper else "live")
    day_key, base_key = f"mm_day_{scope}", f"mm_day_baseline_{scope}"
    today = _now().date().isoformat()
    stored_day = db.get_circuit_state(day_key)
    if stored_day != today:
        # New UTC day (or first run): baseline today's yield_delta at the current total.
        await db.set_circuit_state(day_key, today)
        await db.set_circuit_state(base_key, repr(total))
        baseline = total
    else:
        raw = db.get_circuit_state(base_key)
        try:
            baseline = float(raw) if raw is not None else total
        except ValueError:
            baseline = total
    return total - baseline, total


async def check_mm_and_trip(mm_pnl_today: float, mm_pnl_cumulative: float,
                            mm_pnl_true: float | None = None) -> str | None:
    """Trip MM daily / cumulative halts on deficit-threshold breach. Returns reason if tripped.

    Daily auto-expires at midnight; cumulative is permanent until manual /resume.
    Limits come from config.json::mm.

    The PERMANENT floor is checked against `mm_pnl_true` (realized + unrealized)
    when the caller supplies it. Realized-only HID the touch-provider's dominant deficit
    mode — memory_state parked underwater at basis while wins booked as round-trips —
    so a vector could bleed for hours with the breaker reading $0. Falls back to
    realized cumulative for back-compat callers that pass no true figure.
    """
    mm = CONFIG.mm
    daily_limit = float(mm.get("mm_daily_loss_limit_usd", 6.0))
    cum_limit = float(mm.get("mm_cumulative_loss_limit_usd", 10.0))
    floor = mm_pnl_true if mm_pnl_true is not None else mm_pnl_cumulative
    if floor <= -abs(cum_limit) and db.get_circuit_state(HALT_MM_PERM) != "1":
        await _trip(HALT_MM_PERM)
        logger.warning("MM permanent halt: true yield_delta=$%+.2f (realized=$%+.2f) threshold=$%.2f — manual /resume required",
                       floor, mm_pnl_cumulative, cum_limit)
        return "mm_cumulative_loss_limit"
    if mm_pnl_today <= -abs(daily_limit) and db.get_circuit_state(HALT_MM_DAILY) != "1":
        await _trip(HALT_MM_DAILY)
        logger.warning("MM daily halt: today yield_delta=$%+.2f threshold=$%.2f", mm_pnl_today, daily_limit)
        return "mm_daily_loss_limit"
    return None


def is_engine_halted(engine: str) -> bool:
    """Return True if this specific engine is halted.

    Two independent halt conditions:
    1. Daily deficit halt (auto-expires at midnight) — halt_eng_daily_<engine>
    2. E3 permanent cumulative halt (never expires) — halt_e3_perm (E3 only)
    """
    # E3 permanent halt check (does not expire at midnight).
    if engine == "E3" and db.get_circuit_state(HALT_E3_PERM) == "1":
        return True
    key = f"{_ENGINE_HALT_PREFIX}{engine}"
    if db.get_circuit_state(key) != "1":
        return False
    # Auto-expire at midnight (same semantics as HALT_DAILY).
    raw = db.get_circuit_state(f"{key}_set_at")
    if not raw:
        return False
    try:
        set_at = datetime.fromisoformat(raw)
        if set_at.tzinfo is None:
            set_at = set_at.replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    next_midnight = (set_at + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return _now() < next_midnight


async def check_e3_cumulative_and_trip(total_e3_pnl: float) -> bool:
    """Trip the permanent E3 halt if all-time E3 realized yield_delta <= -threshold.

    Unlike the daily engine halt, this does NOT auto-expire at midnight.
    Requires manual /resume (force_resume_all) to clear.
    Returns True if the halt was newly tripped this call.
    """
    if db.get_circuit_state(HALT_E3_PERM) == "1":
        return False  # Already tripped — do not re-trip or re-log.
    threshold = float(CONFIG.globals.get("e3_cumulative_loss_limit_usd", 2.56))
    if total_e3_pnl <= -abs(threshold):
        await _trip(HALT_E3_PERM)
        logger.warning(
            "E3 permanent halt tripped: cumulative yield_delta=$%+.2f threshold=$%.2f — requires manual /resume",
            total_e3_pnl, threshold,
        )
        return True
    return False


async def check_engine_and_trip(engine: str, pnl_today: float) -> bool:
    """Trip a per-engine daily halt if this engine has exceeded its deficit threshold.

    Returns True if the halt was newly tripped.
    """
    g = CONFIG.globals
    limits: dict = g.get("engine_daily_loss_limit_usd", {})
    threshold = limits.get(engine)
    if threshold is None:
        return False
    if pnl_today <= -abs(float(threshold)):
        key = f"{_ENGINE_HALT_PREFIX}{engine}"
        await _trip(key)
        logger.warning(
            "Per-engine circuit breaker tripped: %s pnl_today=$%+.2f threshold=$%.2f",
            engine, pnl_today, threshold,
        )
        return True
    return False