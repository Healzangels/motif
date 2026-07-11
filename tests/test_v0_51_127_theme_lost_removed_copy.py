"""v0.51.127 — the no-backup 💔 Theme lost body reflects the reaper reality.

The plex_theme_lost (no_fallback) event fires from the v1.18.90 reaper — the
item is no longer in Plex's section listing (removed, or removed + re-added
under a new rating_key), so motif deleted the row. The pre-tag body told the
user to "open the row's // MOTIF INFO card to restore" — but there's no row to
open (the user searched for a reaped row and found nothing). Reframed around
the actual state (the item left Plex) + the conditional path if Plex re-added
it.
"""
from __future__ import annotations

from app.core.notify_content import format_plex_theme_lost_body


def _body():
    return format_plex_theme_lost_body({"display_title": "Some Show (2001)"})


def test_body_frames_the_item_as_removed_from_plex():
    body = _body()
    assert "no longer lists this item" in body
    assert "removed from your library" in body


def test_body_gives_conditional_reappear_path_not_a_dead_link():
    body = _body()
    # The recovery path is conditional on Plex re-adding the item + a refresh —
    # not the pre-tag "open the row's INFO card" (the row was reaped).
    assert "// REFRESH PLEX" in body
    assert "**To restore:** open the row's" not in body


def test_body_keeps_no_backup_phrasing_and_ctas():
    body = _body()
    assert "no backup configured" in body
    assert "// SET URL" in body
    assert "// UPLOAD MP3" in body
    # rate-limit footer preserved.
    assert "once per 24h per row" in body
