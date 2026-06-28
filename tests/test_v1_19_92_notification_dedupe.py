"""v1.19.92 — notification title/body de-duplication + standard.

the user's 2026-05-29 report: the sync_completed notification said
the same thing twice — title "Motif sync — no changes (4581
checked)" + body "No changes detected (scanned 4581 upstream
items)." He asked to kill the duplicate and standardize ALL
notifications so they don't repeat themselves.

## The standard (now uniform across every motif notification)

  TITLE = emoji + outcome + identity (item title / count / status).
  BODY  = ONLY what the title can't hold — source/provenance, the
          theme URL, the action to take, error/breakdown detail,
          and the trailing ✅/⚠ status line.

A body must never repeat the item title, restate the title's
headline phrase, or repeat a count already shown in the title.
The piecemeal de-dup of v1.17.16 / v1.19.55 / v1.19.69 became a
uniform rule here.

This guard pins the de-dup at every touched dispatch site — the
inline ones (worker.py / api.py / scheduler.py) via source-text
pins, and the notify_content.py formatters behaviorally (call the
formatter with a distinctive title, assert it does NOT leak into
the body).
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SCHEDULER_PY = (REPO / "app" / "core" / "scheduler.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
NOTIFY_CONTENT_PY = (REPO / "app" / "core" / "notify_content.py").read_text()


def _inline_body(src: str, event_kind: str) -> str:
    """The EMITTED `body=(...)` argument for an inline dispatch —
    sliced from `body=` (which follows event_kind at these sites)
    so the preceding `# v1.19.92:` archaeology comment (which
    legitimately quotes the OLD strings) doesn't trip the
    'not in body' assertions."""
    idx = src.index(f'event_kind="{event_kind}"')
    bidx = src.index("body=", idx)
    return src[bidx:bidx + 900]


def _sync_emitted_body() -> str:
    """The sync_completed body-builder lines only — from
    `body_lines: list[str]` to `sync_summary =`. Excludes the
    v1.19.92 comment above it (which quotes the old duplicated
    body) and the title-text block below it."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    start = WORKER_PY.rindex("body_lines: list[str]", 0, idx)
    end = WORKER_PY.index("sync_summary =", start)
    return WORKER_PY[start:end]


def _sync_title_block() -> str:
    """The sync_completed title-text branch — from `sync_summary =`
    to the dispatch call."""
    idx = WORKER_PY.index('event_kind="sync_completed"')
    start = WORKER_PY.index("sync_summary =", 0)
    return WORKER_PY[start:idx]


# ── standard is documented ───────────────────────────────────


def test_copy_standard_documented_in_notify_content():
    """The standard lives in the notify_content module docstring
    so the next person adding a notification finds the rule."""
    assert "Copy standard (v1.19.92)" in NOTIFY_CONTENT_PY
    assert "never repeat the" in NOTIFY_CONTENT_PY


# ── sync_completed (the flagged one) ─────────────────────────


def test_sync_completed_body_has_no_outcome_restatement():
    """The emitted body must not restate the title's outcome —
    none of 'No changes', 'scanned', 'had upstream changes', or a
    '**N new**'-style count header."""
    body = _sync_emitted_body()
    assert "No changes detected" not in body
    assert "scanned" not in body
    assert "had upstream changes" not in body
    assert 'f"**{summary}**' not in body


def test_sync_completed_title_carries_the_outcome():
    """The outcome + count live in the title text, not the body."""
    block = _sync_title_block()
    # v1.24.16: "no CATALOG changes" — the headline scopes to the ThemerrDB
    # catalog diff (a post-sync enum can auto-theme an unthemed row afterward).
    assert '"Motif sync — no catalog changes"' in block
    # v1.23.28: the count is now `checked` (= total_seen, or the tracked
    # catalog size when an empty-diff cron sync processed 0 items).
    assert "no catalog changes ({checked} checked)" in block
    assert "' · '.join(parts)" in block


def test_sync_completed_body_keeps_updated_list_and_closer():
    """The body's genuine content survives: the 🔄 Updated list
    + the ✅ closer."""
    body = _sync_emitted_body()
    assert "🔄 Updated:" in body
    assert '"✅ Sync complete"' in body


# ── sync_failed ──────────────────────────────────────────────


def test_sync_failed_body_does_not_restate_title():
    """Body must not lead with 'ThemerrDB sync did not complete.'
    (it restated the '❌ Sync failed' title). It leads with the
    error + recovery path."""
    body = _inline_body(WORKER_PY, "sync_failed")
    assert "did not complete" not in body
    assert 'f"Error: {e}' in body


# ── disk_low ─────────────────────────────────────────────────


def test_disk_low_body_does_not_repeat_free_mb():
    """The title already shows '{free_mb}MB free'; the body must
    not repeat it (it now says 'is below the {min_mb}MB guard')."""
    body = _inline_body(WORKER_PY, "disk_low")
    assert "has {free_mb}MB" not in body
    assert "is below the" in body


# ── cookies_needed ───────────────────────────────────────────


def test_cookies_needed_body_does_not_restate_title():
    """Body must not lead with 'YouTube rejected the request with
    `cookies_expired`.' (the title already says cookies expired).
    It leads with the fix."""
    body = _inline_body(WORKER_PY, "cookies_needed")
    assert "YouTube rejected the request" not in body
    assert "Drop a fresh cookies.txt" in body


# ── release_available ────────────────────────────────────────


def test_release_available_body_does_not_restate_title():
    """Body must not restate 'A new motif release is available on
    GitHub: {tag}' (title already says '{tag} available'). It
    carries only the running version + notes link."""
    body = _inline_body(SCHEDULER_PY, "release_available")
    assert "is available on GitHub" not in body
    assert "You're running {running}." in body


# ── bulk_action_completed (PROBE + LPS) ──────────────────────


def test_bulk_probe_notification_body_drops_headline():
    """The PROBE notification body must not restate 'BULK PROBE
    TDB done: {done}/{total}' (the title carries done/total). It
    carries only the alive/dead/etc. breakdown. The verbose
    summary still goes to log_event."""
    idx = API_PY.index('title=f"✅ Bulk PROBE TDB done — {done}/{total}"')
    block = API_PY[idx:idx + 600]
    assert "body=_bulk_probe_summary" not in block, (
        "v1.19.92: notification body must not reuse the verbose "
        "log summary (which restates the title headline)"
    )
    assert "alive={n_alive}" in block


def test_bulk_lps_notification_body_drops_headline():
    """Symmetric to PROBE — the LPS notification body drops the
    'BULK LPS done: {n_targets} target(s)' headline."""
    idx = API_PY.index('title=f"✅ Bulk LPS done — {n_targets}"')
    block = API_PY[idx:idx + 600]
    assert "body=_bulk_lps_summary" not in block
    assert "probed={n_to_probe}" in block


def test_bulk_summaries_still_logged_verbose():
    """The full verbose summary lines stay as the log_event
    message — only the NOTIFICATION body was trimmed."""
    assert "BULK PROBE TDB done: {done}/{total} probed" in API_PY
    assert "BULK LPS done: {n_targets} target(s)" in API_PY


# ── notify_content formatters — title must not leak into body ─


_PROBE_TITLE = "Zardoz Reloaded (1974)"


def _ctx():
    return {
        "media_type": "movie",
        "tmdb_id": 999,
        "display_title": _PROBE_TITLE,
        "theme_url": "https://youtu.be/abcdefghijk",
        "thumb_url": None,
        "provenance": "themerrdb",
    }


def test_theme_deleted_body_omits_title_and_action_verb():
    """theme_deleted body must not repeat the display_title (the
    title has it) nor lead with the action verb (also in title).
    It leads with 'By {actor}'."""
    from app.core.notify_content import format_theme_deleted_body
    body = format_theme_deleted_body(
        _ctx(), action="unmanaged", actor="user",
        extra_note="3 placement(s) unlinked",
    )
    assert _PROBE_TITLE not in body
    assert "Unmanaged by" not in body
    assert body.startswith("By user")


def test_backup_ready_body_omits_title():
    """format_backup_ready_body must not repeat the display_title
    (title: '🎯 Backup ready — {title}')."""
    from app.core.notify_content import format_backup_ready_body
    body = format_backup_ready_body(_ctx())
    assert _PROBE_TITLE not in body
    assert "PROMOTE TO ACTIVE" in body


def test_plex_theme_lost_body_omits_title():
    """format_plex_theme_lost_body must not repeat the display
    title (title: '💔 Theme lost — {title}')."""
    from app.core.notify_content import format_plex_theme_lost_body
    body = format_plex_theme_lost_body(_ctx())
    assert _PROBE_TITLE not in body
    assert "no backup configured" in body


def test_theme_lost_backup_ready_body_omits_title():
    from app.core.notify_content import (
        format_theme_lost_backup_ready_body,
    )
    body = format_theme_lost_backup_ready_body(
        _ctx(), backup_source="user_url_backup",
    )
    assert _PROBE_TITLE not in body
    assert "no longer available" in body


def test_theme_lost_sidecar_available_body_omits_title():
    from app.core.notify_content import (
        format_theme_lost_sidecar_available_body,
    )
    body = format_theme_lost_sidecar_available_body(_ctx())
    assert _PROBE_TITLE not in body
    assert "// ADOPT" in body


# ── these formatters were already clean — confirm no regression ─


def test_theme_added_and_pushed_bodies_still_omit_title():
    """theme_added / theme_pushed bodies (de-duped at v1.17.16)
    must stay title-free — the standard didn't regress them."""
    from app.core.notify_content import (
        format_theme_added_body, format_theme_pushed_body,
    )
    assert _PROBE_TITLE not in format_theme_added_body(_ctx())
    assert _PROBE_TITLE not in format_theme_pushed_body(_ctx())


# ── version pin ──────────────────────────────────────────────


def test_v1_19_92_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
