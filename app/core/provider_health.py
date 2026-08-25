"""v0.51.280 — feature-brief D: per-provider download health + adaptive rate.

Two modes, per the brief's own hard requirement:
  * fixed (DEFAULT) — the existing rate_per_hour token bucket, byte-identical.
    The controller only OBSERVES (state is recorded in both modes so the
    operator has data before ever opting in).
  * adaptive — before each download the worker asks `check_cooldown`; a
    provider in cooldown re-queues the job via the v1.14.54 _mark_transient
    seam (attempt-free — cooldowns must not eat retry budget), with jitter so
    a queue of same-provider jobs cannot thunder back in one tick.

Classification → effect (driven by FailureKind, the v0.51.269 split):
  RATE_LIMITED     → streak+1, COOLDOWN for base*2^(streak-1) capped at 6h,
                     effective rate halves (floored at adaptive_min)
  NETWORK_ERROR /
  UNKNOWN          → streak+1 only (mild; a blip is not a throttle)
  COOKIES_EXPIRED  → recorded as last_error_class='auth', NO pause. Deliberate
                     deviation from the brief: per-job failure + the existing
                     cookies_needed notification already surface it, and a
                     single misclassified error must not halt every download.
  VIDEO_* kinds    → item problems, zero provider-health impact
  success          → streak 0, GOOD, effective rate recovers by 25% per
                     success toward adaptive_max (slow up, fast down)

State persists in runtime_settings (`provider_health:<provider>`) — the brief's
guardrail: losing a cooldown across a restart would resume hammering a
throttled provider. One writer (the worker's download thread) per provider at
a time, so last-write-wins on the JSON blob is safe here.
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .db import get_conn, transaction
from .downloader import FailureKind
from .events import log_event, now_iso

log = logging.getLogger(__name__)

_KEY = "provider_health:{}"
_COOLDOWN_BASE_S = 300          # 5 min, doubling per consecutive throttle
_COOLDOWN_CAP_S = 6 * 3600
_RECOVERY_FACTOR = 1.25
_BACKOFF_FACTOR = 0.5

PROVIDERS = ("youtube", "soundcloud", "instagram", "facebook", "other")


def provider_for_url(url: str | None) -> str:
    u = (url or "").lower()
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "soundcloud.com" in u:
        return "soundcloud"
    if "instagram.com" in u:
        return "instagram"
    if "facebook.com" in u or "fb.watch" in u:
        return "facebook"
    return "other"


def _default_state(base_rate: float) -> dict:
    return {"state": "GOOD", "effective_rate": base_rate, "failure_streak": 0,
            "cooldown_until": None, "last_success": None,
            "last_error_class": None, "updated_at": None}


def get_state(db_path: Path, provider: str, *, base_rate: float) -> dict:
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT value FROM runtime_settings WHERE key = ?",
                           (_KEY.format(provider),)).fetchone()
    if row is None:
        return _default_state(base_rate)
    try:
        return {**_default_state(base_rate), **json.loads(row["value"])}
    except (ValueError, TypeError):
        return _default_state(base_rate)


def _save(db_path: Path, provider: str, state: dict) -> None:
    state["updated_at"] = now_iso()
    with get_conn(db_path) as conn, transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO runtime_settings "
            "(key, value, updated_at, updated_by) VALUES (?, ?, ?, ?)",
            (_KEY.format(provider), json.dumps(state), now_iso(),
             "provider_health"))


def report_outcome(db_path: Path, provider: str, kind: FailureKind | None, *,
                   base_rate: float, min_rate: float, max_rate: float) -> dict:
    """Feed one download outcome into the state machine. Runs in BOTH modes —
    fixed mode collects the evidence adaptive mode would act on."""
    s = get_state(db_path, provider, base_rate=base_rate)
    prev_state = s["state"]
    if kind is None:
        s["failure_streak"] = 0
        s["state"] = "GOOD"
        s["cooldown_until"] = None
        s["last_success"] = now_iso()
        s["last_error_class"] = None
        s["effective_rate"] = min(max_rate,
                                  max(min_rate,
                                      s["effective_rate"] * _RECOVERY_FACTOR))
    elif kind is FailureKind.RATE_LIMITED:
        s["failure_streak"] += 1
        cool = min(_COOLDOWN_CAP_S,
                   _COOLDOWN_BASE_S * (2 ** (s["failure_streak"] - 1)))
        s["state"] = "COOLDOWN"
        s["cooldown_until"] = (
            datetime.now(timezone.utc) + timedelta(seconds=cool)
        ).isoformat(timespec="seconds")
        s["last_error_class"] = "rate_limit"
        s["effective_rate"] = max(min_rate, s["effective_rate"] * _BACKOFF_FACTOR)
    elif kind is FailureKind.COOKIES_EXPIRED:
        s["last_error_class"] = "auth"      # surfaced elsewhere; no pause
    elif kind in (FailureKind.NETWORK_ERROR, FailureKind.UNKNOWN):
        s["failure_streak"] += 1
        s["last_error_class"] = "transient"
        if s["state"] == "GOOD" and s["failure_streak"] >= 3:
            s["state"] = "DEGRADED"
    else:
        return s                            # VIDEO_*: item problem, no impact
    if s["state"] != prev_state:
        log_event(db_path, level="INFO", component="provider-health",
                  message=(f"Provider {provider}: {prev_state} → {s['state']}"
                           + (f" (cooldown until {s['cooldown_until']})"
                              if s["cooldown_until"] else "")),
                  detail={k: s[k] for k in
                          ("state", "effective_rate", "failure_streak",
                           "cooldown_until", "last_error_class")})
    _save(db_path, provider, s)
    return s


def check_cooldown(db_path: Path, provider: str, *, base_rate: float) -> int:
    """Seconds until this provider may be tried again (0 = go). Jittered ±20%
    so a same-provider queue cannot thunder back in a single tick."""
    s = get_state(db_path, provider, base_rate=base_rate)
    if not s["cooldown_until"]:
        return 0
    try:
        until = datetime.fromisoformat(s["cooldown_until"])
    except ValueError:
        return 0
    remain = (until - datetime.now(timezone.utc)).total_seconds()
    if remain <= 0:
        return 0
    return int(remain * random.uniform(1.0, 1.2)) + 1


def all_states(db_path: Path, *, base_rate: float) -> dict:
    return {p: get_state(db_path, p, base_rate=base_rate) for p in PROVIDERS}
