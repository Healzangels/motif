"""v1.14.24 — api_manual_url + api_upload_theme per-section sfa write.

Wave 1.3 of the codebase audit. Server-side counterpart to v1.14.22's
client-side bulk ACK fix — both close the same cross-section bleed
class (CLAUDE.md class K), just on different write paths.

(Audit ref: AUDIT_API.md H2)

Pre-fix both endpoints did:

    UPDATE themes SET failure_acked_at = ?
    WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL

— the title-global form. With section_id in scope, the right shape
is the per-section sfa INSERT v1.13.54 introduced + v1.14.8
adopted in the worker.

Pre-fix net effect: SET URL or UPLOAD MP3 on a 4K row also
silently dismissed the FAIL glyph + N FAIL count for the
sibling standard MOVIES row (which still has a broken TDB URL,
no override).

v1.14.24 fix: replace both UPDATE-themes calls with the same
per-section sfa INSERT pattern api_clear_failure uses (api.py
~10038-10046). The worker write at worker.py:1201 (v1.14.8)
becomes belt-and-suspenders rather than the only correct path.

`acked_by` set to `'auto:set_url'` / `'auto:upload'` so the
audit trail distinguishes implicit acks (this fix) from
explicit user ACK (api_clear_failure) and from the worker's
post-download write (`'auto:user_override'`).

Choice point: the api_clear_failure pattern ALSO checks if all
sections are now acked → stamps title-global. v1.14.24 omits
that — the SET URL flow is fundamentally per-section (user only
provided override for ONE section), so cascading to title-
global on this path would be wrong. Only when the user
explicitly acks the LAST section via /clear-failure does the
title-global stamp make sense.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── api_manual_url ────────────────────────────────────────────


def test_manual_url_writes_per_section_sfa():
    """The api_manual_url handler must INSERT into
    section_failure_acks for the row's section_id, not UPDATE
    the title-global themes.failure_acked_at.

    Anchor on the v1.14.24 marker so the window covers the
    new code without false-matching the api_clear_failure
    handler (which legitimately uses the same SQL pattern)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Anchor on the v1.14.24 marker INSIDE api_manual_url.
    marker = "v1.14.24: per-section sfa write instead of title-global"
    # marker appears in BOTH api_manual_url and api_upload_theme;
    # find the second occurrence (api_manual_url is later in the file).
    first = src.index(marker)
    second = src.index(marker, first + len(marker))
    body = src[second:second + 2500]
    # The new per-section INSERT.
    assert 'section_id_for_ack = pi["section_id"]' in body
    assert "INSERT INTO section_failure_acks" in body
    assert "'auto:set_url'" in body
    assert "WHERE EXISTS" in body
    assert "t.failure_kind IS NOT NULL" in body
    assert "ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING" in body


def test_manual_url_no_longer_writes_title_global_acked_at():
    """Regression guard: the pre-fix title-global UPDATE must
    not survive in api_manual_url. Comment-stripped to dodge
    the rationale comment quoting the deleted shape."""
    src_raw = (REPO / "app" / "web" / "api.py").read_text()
    # Restrict to api_manual_url body so we don't false-match
    # api_clear_failure (which legitimately stamps title-global
    # when all sections are acked).
    fn_anchor = src_raw.index("async def api_manual_url(")
    end = src_raw.index('@app.', fn_anchor + 100)  # next route handler
    body_raw = src_raw[fn_anchor:end]
    body = "\n".join(
        line for line in body_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    pre_fix = ('UPDATE themes SET failure_acked_at = ? "\n'
               '                "WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL')
    assert pre_fix not in body, (
        "v1.14.24: pre-fix title-global UPDATE in api_manual_url "
        "must not survive — would re-instance the cross-section bleed"
    )


# ── api_upload_theme ──────────────────────────────────────────


def test_upload_theme_writes_per_section_sfa():
    """Same fix on the api_upload_theme handler — the upload
    path was the OTHER cross-section bleed site. Anchor on the
    FIRST occurrence of the v1.14.24 marker (api_upload_theme
    is earlier in the file than api_manual_url)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    marker = "v1.14.24: per-section sfa write instead of title-global"
    first = src.index(marker)
    body = src[first:first + 2500]
    assert "INSERT INTO section_failure_acks" in body
    assert "'auto:upload'" in body
    assert "WHERE EXISTS" in body
    assert "ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING" in body


def test_upload_theme_no_longer_writes_title_global_acked_at():
    """Regression guard: same pre-fix UPDATE guard for
    api_upload_theme."""
    src_raw = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src_raw.index("async def api_upload_theme(")
    end = src_raw.index('@app.', fn_anchor + 100)
    body_raw = src_raw[fn_anchor:end]
    body = "\n".join(
        line for line in body_raw.splitlines()
        if not line.lstrip().startswith("#")
    )
    pre_fix = ('UPDATE themes SET failure_acked_at = ? "\n'
               '                "WHERE media_type = ? AND tmdb_id = ? AND failure_kind IS NOT NULL')
    assert pre_fix not in body


# ── Audit trail: distinct acked_by values ─────────────────────


def test_distinct_acked_by_values_per_path():
    """The three implicit-ack write sites use distinct
    `acked_by` values so the audit log can distinguish:
      - 'auto:set_url'         — api_manual_url (v1.14.24)
      - 'auto:upload'          — api_upload_theme (v1.14.24)
      - 'auto:user_override'   — worker download success (v1.14.8)
      - 'auto:user_override:backfill'  — schema v44 migration (v1.14.8)

    Pin all four values so a refactor doesn't accidentally
    collapse them."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    db = (REPO / "app" / "core" / "db.py").read_text()
    assert "'auto:set_url'" in src, "api_manual_url should use 'auto:set_url'"
    assert "'auto:upload'" in src, "api_upload_theme should use 'auto:upload'"
    assert "'auto:user_override'" in worker, "worker download success should use 'auto:user_override'"
    assert "'auto:user_override:backfill'" in db, "v44 migration should use the backfill marker"


# ── api_clear_failure unchanged (regression guard) ───────────


def test_clear_failure_explicit_path_unchanged():
    """v1.14.24 doesn't touch api_clear_failure (the explicit
    user ACK path). Pin its v1.13.54 sfa-write + cascading
    title-global stamp logic so it stays the canonical model
    for cross-section ACK."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # The v1.13.54 marker is in the api_clear_failure handler.
    assert "v1.13.54: per-section ack" in src
    # The cascading-to-title-global pattern (when all sections
    # acked) should still exist in api_clear_failure.
    assert "owner_sections.issubset(acked_sections)" in src


# ── Mirror with worker.py belt-and-suspenders write ───────────


def test_worker_v1_14_8_sfa_write_still_present():
    """The worker's v1.14.8 sfa-on-download-success write at
    worker.py:~1201 stays in place — v1.14.24 makes the API
    write the immediate UX response (glyph clears for the
    section before the download lands), and the worker write
    at download-success time is the durable belt-and-suspenders
    confirmation. If both writes happen, ON CONFLICT DO NOTHING
    keeps the earlier (API) ack timestamp authoritative."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # The v1.14.8 marker.
    assert "v1.14.8: a successful user-override download" in src
    # The sfa write itself.
    assert "INSERT INTO section_failure_acks" in src
    assert "'auto:user_override'" in src
