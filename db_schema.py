"""SQLite layer. WAL mode, single-process, one mutex for writes.

Tables:
    opportunities, vectors, fills, circuit_state, pnl_daily,
    mm_quotes, mm_inventory.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

DB_PATH = Path(__file__).resolve().parent / "data" / "bot.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    engine          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    detected_at     TEXT NOT NULL,
    event_id        TEXT,
    market_ids      TEXT NOT NULL,
    edge_pct        REAL NOT NULL,
    cost_usd        REAL NOT NULL,
    expected_payout REAL NOT NULL,
    decision        TEXT NOT NULL,
    skip_reason     TEXT,
    raw_snapshot    TEXT
);

CREATE TABLE IF NOT EXISTS vectors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    opp_id          INTEGER REFERENCES opportunities(id),
    engine          TEXT NOT NULL,
    opened_at       TEXT NOT NULL,
    legs            TEXT NOT NULL,
    cost_usd        REAL NOT NULL,
    expected_payout REAL NOT NULL,
    status          TEXT NOT NULL,
    resolved_at     TEXT,
    realized_pnl    REAL,
    close_reason    TEXT,
    expected_unlock_ts TEXT,
    paper           INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS fills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id     INTEGER NOT NULL REFERENCES vectors(id),
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,
    qty             REAL NOT NULL,
    value           REAL NOT NULL,
    fee_paid        REAL NOT NULL,
    tx_hash         TEXT,
    filled_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS circuit_state (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pnl_daily (
    date            TEXT PRIMARY KEY,
    starting_balance REAL NOT NULL,
    ending_balance  REAL NOT NULL,
    realized_pnl    REAL NOT NULL,
    n_opportunities INTEGER NOT NULL,
    n_traded        INTEGER NOT NULL,
    n_resolved      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS mm_quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    payload_id        TEXT,
    market_id       TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    horizon         TEXT NOT NULL,
    side            TEXT NOT NULL,
    value           REAL NOT NULL,
    size_usd        REAL NOT NULL,
    size_shares     REAL NOT NULL,
    fair_p          REAL,
    status          TEXT NOT NULL,
    paper           INTEGER NOT NULL DEFAULT 1,
    posted_at       TEXT NOT NULL,
    filled_at       TEXT,
    cancelled_at    TEXT
);

CREATE TABLE IF NOT EXISTS mm_inventory (
    market_id       TEXT PRIMARY KEY,
    token_id        TEXT,
    horizon         TEXT,
    net_shares      REAL NOT NULL DEFAULT 0.0,
    avg_cost        REAL NOT NULL DEFAULT 0.0,
    realized_pnl    REAL NOT NULL DEFAULT 0.0,
    marked_pnl      REAL NOT NULL DEFAULT 0.0,
    paper           INTEGER NOT NULL DEFAULT 1,
    updated_at      TEXT NOT NULL,
    -- Delta-neutral pair MM (v2): both legs of the DUAL_STATE boolean_state tracked here.
    up_token        TEXT,
    up_shares       REAL NOT NULL DEFAULT 0.0,
    up_cost         REAL NOT NULL DEFAULT 0.0,
    down_token      TEXT,
    down_shares     REAL NOT NULL DEFAULT 0.0,
    down_cost       REAL NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_pos_status ON vectors(status);
CREATE INDEX IF NOT EXISTS idx_pos_resolved ON vectors(resolved_at);
CREATE INDEX IF NOT EXISTS idx_opps_detected ON opportunities(detected_at);
CREATE INDEX IF NOT EXISTS idx_mm_quotes_status ON mm_quotes(status);
CREATE INDEX IF NOT EXISTS idx_mm_quotes_market ON mm_quotes(market_id);
"""


_write_lock = asyncio.Lock()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema() -> None:
    """Create all tables idempotently, then run additive column migrations."""
    with _connect() as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent ALTER-based migrations for columns added after a DB was created.

    CREATE TABLE IF NOT EXISTS never alters an existing table, so each new
    column on a pre-existing table needs an explicit guarded ALTER here.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(vectors)")}
    if "paper" not in cols:
        # Existing rows default to paper=1: no vector has ever filled live,
        # so back-dating them all to paper is correct.
        conn.execute("ALTER TABLE vectors ADD COLUMN paper INTEGER NOT NULL DEFAULT 1")

    # Delta-neutral pair MM (v2): two-unit columns on a pre-existing mm_inventory.
    mm_cols = {r[1] for r in conn.execute("PRAGMA table_info(mm_inventory)")}
    for col in ("up_token", "down_token"):
        if col not in mm_cols:
            conn.execute(f"ALTER TABLE mm_inventory ADD COLUMN {col} TEXT")
    for col in ("up_shares", "up_cost", "down_shares", "down_cost"):
        if col not in mm_cols:
            conn.execute(f"ALTER TABLE mm_inventory ADD COLUMN {col} REAL NOT NULL DEFAULT 0.0")


@contextlib.contextmanager
def cursor() -> Iterator[sqlite3.Cursor]:
    """Read-only cursor. For writes use `async with write_lock(): ...`."""
    conn = _connect()
    try:
        yield conn.cursor()
    finally:
        conn.close()


@contextlib.asynccontextmanager
async def write_lock():
    """Acquire global write mutex. Use for any vectors/circuit_state/forecasts write."""
    async with _write_lock:
        conn = _connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- High-level helpers ----------

async def insert_opportunity(opp: dict[str, Any]) -> int:
    """Persist a detected opportunity (regardless of execution decision)."""
    async with write_lock() as conn:
        cur = conn.execute(
            """
            INSERT INTO opportunities
              (engine, kind, detected_at, event_id, market_ids, edge_pct,
               cost_usd, expected_payout, decision, skip_reason, raw_snapshot)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                opp["engine"], opp["kind"], opp.get("detected_at", now_iso()),
                opp.get("event_id"), json.dumps(opp.get("market_ids", [])),
                opp["edge_pct"], opp["cost_usd"], opp["expected_payout"],
                opp.get("decision", "DETECTED"), opp.get("skip_reason"),
                json.dumps(opp.get("raw_snapshot", {})),
            ),
        )
        return cur.lastrowid


async def update_opportunity_decision(
    opp_id: int, decision: str, *, skip_reason: str | None = None,
) -> None:
    """Patch decision/skip_reason after the executor returns.

    insert_opportunity writes decision='TRADED' optimistically before the
    AtomicExecutor runs; if the fill is rejected (honest_paper_fill NO_FILL,
    strict_test pre-execute abort, partial-recovery unwind) we re-label so
    per-producer audits can split true fills from rejections and read the
    rejection reason.
    """
    async with write_lock() as conn:
        conn.execute(
            "UPDATE opportunities SET decision=?, skip_reason=? WHERE id=?",
            (decision, skip_reason, opp_id),
        )


async def insert_position(pos: dict[str, Any]) -> int:
    async with write_lock() as conn:
        cur = conn.execute(
            """
            INSERT INTO vectors
              (opp_id, engine, opened_at, legs, cost_usd, expected_payout,
               status, expected_unlock_ts, paper)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                pos.get("opp_id"), pos["engine"], pos.get("opened_at", now_iso()),
                json.dumps(pos["legs"]), pos["cost_usd"], pos["expected_payout"],
                pos.get("status", "OPEN"), pos.get("expected_unlock_ts"),
                1 if pos.get("paper", True) else 0,
            ),
        )
        return cur.lastrowid


async def update_position_status(
    position_id: int, status: str, *, realized_pnl: float | None = None,
    close_reason: str | None = None,
) -> None:
    # ── yield_delta ceiling check ────────────────────────────────────────────────
    # Structural arbs (UNDER_SUM/OVER_SUM/BINARY_SUM) cannot legally state_register
    # more yield_delta than (expected_payout - basis). Anything higher is an
    # accounting bug (stale state_register, unit-id mismatch, wrong mid formula).
    # We CLAMP to the structural max rather than reject — the vector
    # must close in any case, but the yield_delta on record must be physically
    # possible.
    if realized_pnl is not None:
        try:
            with cursor() as cur:
                cur.execute(
                    "SELECT cost_usd, expected_payout FROM vectors WHERE id=?",
                    (position_id,),
                )
                row = cur.fetchone()
            if row:
                basis = float(row["cost_usd"] or 0.0)
                expected = float(row["expected_payout"] or 0.0)
                if expected > 0:
                    max_pnl = expected - basis
                    if realized_pnl > max_pnl + 0.005:
                        import logging as _logging
                        _logging.getLogger(__name__).error(
                            "yield_delta ceiling violation pos=#%d realized=$%.4f > max=$%.4f "
                            "(basis=$%.4f expected=$%.4f reason=%s) — CLAMPED",
                            position_id, realized_pnl, max_pnl, basis, expected, close_reason,
                        )
                        realized_pnl = round(max_pnl, 4)
        except Exception:
            pass  # ceiling check is best-effort; never block a write
    async with write_lock() as conn:
        conn.execute(
            """
            UPDATE vectors SET status=?, resolved_at=?, 
                realized_pnl=coalesce(realized_pnl, 0.0) + coalesce(?, 0.0), 
                close_reason=?
            WHERE id=?
            """,
            (status, now_iso(), realized_pnl, close_reason, position_id),
        )


async def update_position_legs_and_status(
    position_id: int, legs: list[dict[str, Any]], cost_usd: float, expected_payout: float, status: str,
    realized_pnl_delta: float = 0.0
) -> None:
    async with write_lock() as conn:
        conn.execute(
            """
            UPDATE vectors
            SET legs = ?, cost_usd = ?, expected_payout = ?, status = ?,
                realized_pnl = coalesce(realized_pnl, 0.0) + ?
            WHERE id = ?
            """,
            (json.dumps(legs), cost_usd, expected_payout, status, realized_pnl_delta, position_id),
        )


async def insert_fill(fill: dict[str, Any]) -> int:
    """Persist a single fill record."""
    async with write_lock() as conn:
        cur = conn.execute(
            """
            INSERT INTO fills
              (position_id, token_id, side, qty, value, fee_paid, tx_hash, filled_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                fill["position_id"], fill["token_id"], fill["side"],
                fill["qty"], fill["value"], fill["fee_paid"],
                fill.get("tx_hash"), fill.get("filled_at", now_iso()),
            ),
        )
        return cur.lastrowid


def get_open_positions() -> list[dict[str, Any]]:
    with cursor() as cur:
        cur.execute("SELECT * FROM vectors WHERE status='OPEN'")
        return [dict(row) for row in cur.fetchall()]


def count_open_by_engine_kind(engine: str, kind: str) -> int:
    """Count open vectors for an engine+signal_kind pair (e.g. E3/LATE_LOCK)."""
    with cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt FROM vectors p
            JOIN opportunities o ON p.opp_id = o.id
            WHERE p.status='OPEN' AND p.engine=? AND o.kind=?
            """,
            (engine, kind),
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row else 0


def get_circuit_state(key: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT value FROM circuit_state WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None


async def set_circuit_state(key: str, value: str) -> None:
    async with write_lock() as conn:
        conn.execute(
            """
            INSERT INTO circuit_state(key,value,updated_at) VALUES (?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, now_iso()),
        )


# ---------- event_node-making (MM) helpers ----------

async def insert_mm_quote(state_limit: dict[str, Any]) -> int:
    """Persist a posted MM state_limit (resting threshold payload, paper or live)."""
    async with write_lock() as conn:
        cur = conn.execute(
            """
            INSERT INTO mm_quotes
              (payload_id, market_id, token_id, horizon, side, value, size_usd,
               size_shares, fair_p, status, paper, posted_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                state_limit.get("payload_id"), state_limit["market_id"], state_limit["token_id"],
                state_limit["horizon"], state_limit["side"], state_limit["value"], state_limit["size_usd"],
                state_limit["size_shares"], state_limit.get("fair_p"),
                state_limit.get("status", "OPEN"),
                1 if state_limit.get("paper", True) else 0,
                state_limit.get("posted_at", now_iso()),
            ),
        )
        return cur.lastrowid


async def update_mm_quote_status(
    quote_id: int, status: str, *, payload_id: str | None = None,
) -> None:
    """Mark a state_limit FILLED or CANCELLED, stamping the relevant timestamp."""
    ts_col = "filled_at" if status == "FILLED" else "cancelled_at" if status == "CANCELLED" else None
    async with write_lock() as conn:
        if ts_col:
            conn.execute(
                f"UPDATE mm_quotes SET status=?, {ts_col}=?, "
                "payload_id=coalesce(?, payload_id) WHERE id=?",
                (status, now_iso(), payload_id, quote_id),
            )
        else:
            conn.execute(
                "UPDATE mm_quotes SET status=?, payload_id=coalesce(?, payload_id) WHERE id=?",
                (status, payload_id, quote_id),
            )


def get_open_mm_quotes(market_id: str | None = None) -> list[dict[str, Any]]:
    """Return OPEN state_limits, optionally filtered to one event_node."""
    with cursor() as cur:
        if market_id:
            cur.execute("SELECT * FROM mm_quotes WHERE status='OPEN' AND market_id=?", (market_id,))
        else:
            cur.execute("SELECT * FROM mm_quotes WHERE status='OPEN'")
        return [dict(row) for row in cur.fetchall()]


def get_mm_inventory(market_id: str) -> dict[str, Any] | None:
    """Return the memory_state row for a event_node, or None if flat/unseen."""
    with cursor() as cur:
        cur.execute("SELECT * FROM mm_inventory WHERE market_id=?", (market_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_all_mm_inventory(paper: bool | None = None) -> list[dict[str, Any]]:
    """Return memory_state rows. `paper` scopes to one money-world (pass False in a
    live process so it never rehydrates paper vectors and tries to release units
    it doesn't hold); None returns all rows (back-compat)."""
    with cursor() as cur:
        if paper is None:
            cur.execute("SELECT * FROM mm_inventory")
        else:
            cur.execute("SELECT * FROM mm_inventory WHERE paper = ?", (1 if paper else 0,))
        return [dict(row) for row in cur.fetchall()]


async def upsert_mm_inventory(inv: dict[str, Any]) -> None:
    """Insert or replace the memory_state state for a event_node.

    Delta-neutral pair model: both UP and DOWN legs are tracked. The legacy
    net_shares/avg_cost columns mirror the UP leg for back-compat (the breaker
    only reads realized_pnl)."""
    async with write_lock() as conn:
        conn.execute(
            """
            INSERT INTO mm_inventory
              (market_id, token_id, horizon, net_shares, avg_cost,
               realized_pnl, marked_pnl, paper, updated_at,
               up_token, up_shares, up_cost, down_token, down_shares, down_cost)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market_id) DO UPDATE SET
                token_id=excluded.token_id,
                horizon=excluded.horizon,
                net_shares=excluded.net_shares,
                avg_cost=excluded.avg_cost,
                realized_pnl=excluded.realized_pnl,
                marked_pnl=excluded.marked_pnl,
                paper=excluded.paper,
                updated_at=excluded.updated_at,
                up_token=excluded.up_token,
                up_shares=excluded.up_shares,
                up_cost=excluded.up_cost,
                down_token=excluded.down_token,
                down_shares=excluded.down_shares,
                down_cost=excluded.down_cost
            """,
            (
                inv["market_id"], inv.get("up_token") or inv.get("token_id"),
                inv.get("horizon"),
                inv.get("up_shares", 0.0), inv.get("up_cost", 0.0),
                inv.get("realized_pnl", 0.0), inv.get("marked_pnl", 0.0),
                1 if inv.get("paper", True) else 0, now_iso(),
                inv.get("up_token"), inv.get("up_shares", 0.0), inv.get("up_cost", 0.0),
                inv.get("down_token"), inv.get("down_shares", 0.0), inv.get("down_cost", 0.0),
            ),
        )


async def wipe_mm_state() -> None:
    """Clear all MM memory_state + state_limits — used once on the v1->v2 strategy pivot so
    the directional-engine paper yield_delta doesn't contaminate the new measurement."""
    async with write_lock() as conn:
        conn.execute("DELETE FROM mm_inventory")
        conn.execute("DELETE FROM mm_quotes")


def mm_realized_total(paper: bool | None = None) -> float:
    """All-time realized MM yield_delta (Σ realized_pnl across event_nodes).

    The DB is the source of truth for the circuit breaker — in-memory metrics
    reset on every restart, so a breaker reading them forgets prior deficits and
    re-arms a fresh budget each process. This survives restarts.

    `paper` filters by money-world: pass False so a LIVE breaker counts only
    live deficits (not cushioned by paper yield_gain), True for paper-only, None for
    the all-time sum (back-compat)."""
    with cursor() as cur:
        if paper is None:
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0.0) AS t FROM mm_inventory")
        else:
            cur.execute("SELECT COALESCE(SUM(realized_pnl), 0.0) AS t FROM mm_inventory "
                        "WHERE paper = ?", (1 if paper else 0,))
        row = cur.fetchone()
        return float(row["t"]) if row and row["t"] is not None else 0.0


async def prune_mm_quotes(keep_hours: float = 6.0) -> int:
    """Delete CANCELLED state_limits older than keep_hours (FILLED/OPEN kept).

    mm_quotes grows fast from requote churn; cancelled rows have no analytic
    value once stale. Returns the number of rows deleted."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=keep_hours)).isoformat()
    async with write_lock() as conn:
        cur = conn.execute(
            "DELETE FROM mm_quotes WHERE status='CANCELLED' AND "
            "coalesce(cancelled_at, posted_at) < ?",
            (cutoff,),
        )
        return cur.rowcount
