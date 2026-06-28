"""v1.19.88 — ops drawer + status-bar style audit.

A style review of the ops drawer (.op-card) + topbar status bars
(op-pill / op-mini) surfaced one cross-surface inconsistency + token
drifts. the user approved aligning the tones + the token cleanup.

## Tone realignment (the headline)

Pre-fix the drawer used tdb=cyan + plex=green, swapped vs the
source-letter palette used everywhere else (SRC=T green, SRC=P
amber, SRC=A cyan). So THEMERRDB SYNC rendered cyan (Adopted's
color) and PLEX REFRESH rendered green (ThemerrDB's color).

v1.19.88 realigns: TDB→green-bright, Plex→amber, and the
download/place/scan queues get a dedicated cyan `--tone-queue`
(distinct from Plex amber, per the user's "keep 3 distinct" pick).
`--tone-warn` stays amber for the genuine warnings that still use it
(the disk-low badge + the "+N ops" overflow pill).

The op-card threads its tone through ~8 sub-elements (inset,
breathe, counter, bar, indet bar, timeline, sparkline) — those were
centralized onto a single per-card `--ot` / `--ot-rgb` custom
property so the card's identity lives in one place.

## Token drifts (design-system compliance)

  M1 — `var(--text, #d8e0dc)` (--text is undefined → off-palette
       #d8e0dc hardcode) → `var(--fg)`.
  M2 — hardcoded `'JetBrains Mono', monospace` → `var(--font-mono)`.
  M3 — the fallback callout's split orange (`var(--orange)` border +
       `rgba(255,184,108,…)` bg = two different oranges) unified onto
       `--orange` / `rgba(var(--orange-rgb), …)`.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()


# ── Tone realignment ─────────────────────────────────────────


def test_root_tones_realigned():
    assert "--tone-tdb: var(--green-bright);" in OPS_CSS, (
        "v1.19.88: TDB ops tone must be green-bright (matches SRC=T)"
    )
    assert "--tone-plex: var(--amber);" in OPS_CSS, (
        "v1.19.88: Plex ops tone must be amber (matches SRC=P)"
    )
    assert "--tone-queue: var(--cyan);" in OPS_CSS, (
        "v1.19.88: queues get a dedicated cyan tone"
    )
    # warn stays amber (disk-low + overflow warnings still use it).
    assert "--tone-warn: var(--amber);" in OPS_CSS


def test_queue_kinds_use_queue_tone():
    """All 6 queue synth kinds must map to the new 'queue' tone, not
    the old 'warn' (which would now collide with realigned Plex)."""
    for kind in ("download_queue", "place_queue", "scan_queue",
                 "refresh_queue", "relink_queue", "adopt_queue"):
        # Column-aligned spacing varies per kind length, so match
        # whitespace-tolerantly.
        assert re.search(rf"{kind}:\s+'queue'", OPS_JS), (
            f"v1.19.88: {kind} must use the 'queue' tone"
        )
    # No queue kind left on 'warn'.
    assert not re.search(r"_queue:\s+'warn'", OPS_JS)


def test_op_card_tone_centralized_via_ot():
    """Each tone sets --ot/--ot-rgb once; sub-elements read them."""
    assert ".op-card.op-tone-tdb   { --ot: var(--green-bright); --ot-rgb: var(--green-rgb);" in OPS_CSS
    assert ".op-card.op-tone-plex  { --ot: var(--amber);        --ot-rgb: var(--amber-rgb);" in OPS_CSS
    assert ".op-card.op-tone-queue { --ot: var(--cyan);         --ot-rgb: var(--cyan-rgb);" in OPS_CSS
    # The duplicate plex breathe keyframe is gone (one parameterized
    # op-card-breathe reads --ot-rgb now).
    assert "op-card-breathe-plex" not in OPS_CSS
    # No sub-element re-hardcodes a per-tone cyan/green color.
    assert ".op-card.op-tone-tdb  .op-card-counter-current" not in OPS_CSS
    assert ".op-card.op-tone-plex .op-card-spark-bar" not in OPS_CSS


def test_download_badge_follows_queue_tone():
    """The topbar download +N badge must use the cyan queue tone to
    match the realigned download_queue card."""
    anchor = OPS_JS.index("dlQueueDepth > 0")
    block = OPS_JS[anchor:anchor + 1500]
    assert "op-pill op-tone-queue" in block


# ── Token compliance ─────────────────────────────────────────


def test_no_undefined_text_var_or_hardcoded_offwhite():
    assert "var(--text" not in OPS_CSS, (
        "v1.19.88: var(--text,#d8e0dc) (--text is undefined → "
        "off-palette hardcode) must be var(--fg)"
    )
    assert "#d8e0dc" not in OPS_CSS


def test_font_family_uses_token():
    assert "'JetBrains Mono'" not in OPS_CSS, (
        "v1.19.88: hardcoded font-family must be var(--font-mono)"
    )
    assert "var(--font-mono)" in OPS_CSS


def test_orange_callouts_unified_on_token():
    # The split #ffb86c / rgba(255,184,108,…) orange is gone.
    assert "#ffb86c" not in OPS_CSS
    assert "255,184,108" not in OPS_CSS
    # Fallback callouts use the --orange / --orange-rgb token.
    assert "rgba(var(--orange-rgb), 0.08)" in OPS_CSS


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_88_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
