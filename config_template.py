"""Config + .env loader. One canonical entry point for all tunables."""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def _get(name: str, default: str | None = None, *, required: bool = False) -> str:
    """Fetch an env var; raise if required and unset."""
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val or ""


@dataclass(frozen=True)
class Env:
    node_signing_key: str
    node_address: str
    network_rpc_url: str
    network_ws_url: str
    indexer_api_key: str
    ALERT_BOT_TOKEN: str
    telegram_chat_id: str
    vertex_ai_project: str
    vertex_ai_region: str
    gemini_flash_model: str
    gemini_pro_model: str
    gemini_api_key: str
    news_api_key: str
    telegram_api_id: int
    telegram_api_hash: str
    simulation_mode: bool
    # Hard global gate for live execution. Defaults False so live payloads
    # are impossible unless explicitly enabled — independent of any per-engine
    # config.json simulation flag. Last line of defense against accidental
    # unintended live execution.
    live_execution_enabled: bool
    # distributed_network DepositWallet contract address. When set, the NETWORK client uses
    # signatureType=3 (NetworkSig) so payloads are signed on behalf of this contract
    # rather than the primary key. The allocation level is also read at this address.
    node_deposit_address: str
    # E3 measurement-mode flags. Default OFF so existing behavior is
    # preserved when env vars are missing; set to 1/true to activate.
    e3_local_resolution_fallback: bool
    e3_honest_fill: bool
    e3_honest_fill_rtt_ms: float
    log_level: str
    sentry_dsn: str
    dashboard_token: str

    @classmethod
    def load(cls) -> "Env":
        paper = _get("SIMULATION_MODE", "true").lower() in ("1", "true", "yes")
        return cls(
            node_signing_key=_get("NODE_SIGNING_KEY", required=not paper),
            node_address=_get("NODE_ADDRESS", required=not paper),
            network_rpc_url=_get("NETWORK_RPC_URL", "https://network-rpc.internal"),
            network_ws_url=_get("NETWORK_WS_URL", ""),
            indexer_api_key=_get("INDEXER_API_KEY", ""),
            ALERT_BOT_TOKEN=_get("ALERT_BOT_TOKEN", ""),
            telegram_chat_id=_get("TELEGRAM_CHAT_ID", ""),
            vertex_ai_project=_get("VERTEX_AI_PROJECT", "new-n8n-project-490407"),
            vertex_ai_region=_get("VERTEX_AI_REGION", "us-central1"),
            gemini_flash_model=_get("GEMINI_FLASH_MODEL", "gemini-2.5-flash"),
            gemini_pro_model=_get("GEMINI_PRO_MODEL", "gemini-2.5-pro"),
            gemini_api_key=_get("GEMINI_API_KEY", ""),
            news_api_key=_get("NEWS_API_KEY", ""),
            telegram_api_id=int(_get("TELEGRAM_API_ID", "0") or "0"),
            telegram_api_hash=_get("TELEGRAM_API_HASH", ""),
            simulation_mode=paper,
            live_execution_enabled=_get("LIVE_EXECUTION_ENABLED", "false").lower()
            in ("1", "true", "yes"),
            node_deposit_address=_get("NODE_DEPOSIT_ADDRESS", ""),
            e3_local_resolution_fallback=_get(
                "E3_LOCAL_RESOLUTION_FALLBACK", "false"
            ).lower() in ("1", "true", "yes"),
            e3_honest_fill=_get("E3_HONEST_FILL", "false").lower() in ("1", "true", "yes"),
            # Default RTT measured 2026-05-20 from GCP deployment node to distributed_network
            # NETWORK (Cloudflare-fronted at London edge): ~2ms median via Google Premium
            # network. Using 5ms as buffer; tune via E3_HONEST_FILL_RTT_MS in .env.
            e3_honest_fill_rtt_ms=float(_get("E3_HONEST_FILL_RTT_MS", "5") or "5"),
            log_level=_get("LOG_LEVEL", "INFO"),
            sentry_dsn=_get("SENTRY_DSN", ""),
            dashboard_token=_get("DASHBOARD_TOKEN", ""),
        )


@dataclass(frozen=True)
class Config:
    """Loaded from config.json. Tunable without code change."""

    raw: dict[str, Any] = field(repr=False)

    def section(self, key: str) -> dict[str, Any]:
        if key not in self.raw:
            raise KeyError(f"Config section missing: {key}")
        return self.raw[key]

    @property
    def globals(self) -> dict[str, Any]:
        return self.section("global")

    @property
    def mm(self) -> dict[str, Any]:
        return self.section("mm")

    def engine(self, n: int) -> dict[str, Any]:
        return self.section(f"engine_{n}")

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        path = path or (ROOT / "config.json")
        with path.open() as f:
            return cls(raw=json.load(f))


# Engine tag → config.json section key.
_ENGINE_SECTION: dict[str, str] = {
    "MM": "mm",
}

_CONFIG_WRITE_LOCK = threading.Lock()


def engine_section_key(engine: str) -> str:
    """Return the config.json section key for an engine tag (e.g. 'E2' → 'engine_2_new')."""
    key = _ENGINE_SECTION.get(engine.upper())
    if key is None:
        raise KeyError(f"Unknown engine: {engine!r}")
    return key


def write_config() -> None:
    """Atomically persist the current in-memory CONFIG.raw back to config.json.

    Uses a temp-file + rename so a crash mid-write never corrupts the file.
    Thread-safe via _CONFIG_WRITE_LOCK.
    """
    path = ROOT / "config.json"
    with _CONFIG_WRITE_LOCK:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(CONFIG.raw, indent=2, ensure_ascii=False))
        tmp.replace(path)


ENV = Env.load()
CONFIG = Config.load()
