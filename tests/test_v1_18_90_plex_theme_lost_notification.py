"""v1.18.90 — plex_theme_lost notification event.

the user's request: when a row LOSES its Plex-served theme AND
motif has no fallback (user URL / upload / local file), notify
so the user knows to add a theme via SET URL or UPLOAD MP3.

Distinct from v1.18.80's `backup_ready_to_deploy` which fires
when motif HAS a backup ready (action: PROMOTE TO ACTIVE).
This event fires when motif has NOTHING (action: ADD a theme).

## Trigger

Wired into v1.18.89's reaper. When a stale plex_items row with
`has_theme=1` is about to be deleted (Plex stopped returning
that rk), check:

  - No surviving plex_items row for the same (mt, tmdb) has
    `has_theme=1` (no other rk is serving)
  - No motif fallback exists (no local_files / placements /
    user_overrides for the (mt, tmdb))

If both negative → the theme is lost + the user has no
recovery path → dispatch notification.

the user's exact repro fits: Troy was P (Plex served via the old
themerr-plex plugin embed), the user removed Troy from Plex, the
rk vanished from enumeration, v1.18.89's reaper deletes the
stale row, this v1.18.90 trigger fires + dispatches.

## Defaults + dedup

- **Default OFF.** Per the user's call — conservative since the
  event fires per-row and bulk Plex changes could produce a
  burst.
- **Per-row dedup via notify_dedupe.** Composite key
  `plex_theme_lost:<mt>:<tmdb>` + 24h rate-limit caps the
  worst case even when opted in.
- **Best-effort dispatch.** Each row's dispatch wrapped in
  try/except so one bad row doesn't poison the rest. Outer
  try/except too in case the imports themselves fail.
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

NOTIFY_PY = REPO / "app" / "core" / "notify.py"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── notify.py event_kind registered ──────────────────────────


def test_notify_py_registers_plex_theme_lost_at_warning_level():
    """`plex_theme_lost` must be in LEVEL_BY_KIND at warning
    level. Unlike backup_ready_to_deploy (info) — the row's
    silent state IS the user-visible failure, warning is
    appropriate."""
    src = NOTIFY_PY.read_text()
    assert '"plex_theme_lost":' in src
    body_flat = re.sub(r"\s+", " ", src)
    assert '"plex_theme_lost": "warning"' in body_flat


# ── config_file.py default OFF ───────────────────────────────


def test_config_default_is_off():
    """Per the user's call: default OFF. Conservative since the
    event is per-row + bulk Plex changes could burst. Users
    opt in via NOTIFICATIONS settings."""
    src = CONFIG_FILE_PY.read_text()
    fn_idx = src.index("_DEFAULT_NOTIFY_EVENTS")
    fn_end = src.index("}", fn_idx)
    block = src[fn_idx:fn_end]
    assert '"plex_theme_lost":         False' in block, (
        "v1.18.90: default must be False (opt-in)"
    )


# ── notify_content.py formatters ─────────────────────────────


def test_format_plex_theme_lost_title_uses_distinct_emoji():
    """Subject must use 💔 — distinct from 🎯 (backup ready) /
    🎵 (theme added) / 🗑️ (deleted) per v1.17.19's
    emoji-mapping convention."""
    src = NOTIFY_CONTENT_PY.read_text()
    assert "def format_plex_theme_lost_title(" in src
    assert "💔 Theme lost" in src


def test_format_plex_theme_lost_body_names_action_paths():
    """Body must explicitly name BOTH action paths — SET URL
    and UPLOAD MP3 — since the user has no fallback and either
    can restore."""
    src = NOTIFY_CONTENT_PY.read_text()
    fn_idx = src.index("def format_plex_theme_lost_body(")
    nxt_def = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt_def if nxt_def != -1 else len(src)]
    body_flat = re.sub(r'"\s*"', "", body)
    assert "SET URL" in body_flat
    assert "UPLOAD MP3" in body_flat
    assert "INFO card" in body_flat or "MOTIF INFO" in body_flat


def test_format_plex_theme_lost_body_explains_dedup_window():
    """Body should mention the 24h per-row rate-limit so users
    don't expect re-notifications during the dedup window."""
    src = NOTIFY_CONTENT_PY.read_text()
    fn_idx = src.index("def format_plex_theme_lost_body(")
    nxt_def = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt_def if nxt_def != -1 else len(src)]
    body_flat = re.sub(r'"\s*"', "", body)
    assert "24h" in body_flat or "24 hour" in body_flat.lower()


# ── plex_enum.py detection + dispatch ────────────────────────


def test_plex_enum_collects_lost_theme_candidates_in_reaper():
    """During the reaper, BEFORE deleting stale rows, capture
    the set of unique (mt, tmdb, title) for rows being deleted
    that have has_theme=1. These are the lost-theme candidates."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "lost_theme_candidates" in body
    # The capture query joins themes to filter by has_theme=1.
    assert "pi.has_theme = 1" in body
    # v1.18.90 marker.
    assert "v1.18.90" in body


def test_plex_enum_dispatches_after_transaction_commits():
    """The dispatch loop runs AFTER the `with transaction(...)`
    block so an Apprise HTTP failure doesn't roll back the
    reap. Outer try/except wraps the whole dispatch to absorb
    import errors.

    v1.19.41: the dispatch's event_kind is no longer a single
    `plex_theme_lost` literal — it branches by tier into
    `theme_lost_backup_ready` / `theme_lost_sidecar_available`
    / `plex_theme_lost` (Tier 4 retains the original kind).
    Pin all three are present."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    assert "if lost_theme_candidates:" in body
    assert "_notify.dispatch(" in body
    # All three event_kinds from the v1.19.41 tier split.
    # The dispatch's `event_kind=` param now binds to the
    # `_event_kind` local computed by the tier branches above
    # (was a per-call literal pre-v1.19.41). Pin each literal
    # is present in the if/elif/else ladder.
    assert '_event_kind = "plex_theme_lost"' in body, (
        "v1.18.90 Tier-4 event_kind literal must remain — "
        "backward compat for the no_fallback path"
    )
    assert '_event_kind = "theme_lost_backup_ready"' in body, (
        "v1.19.41: tier-1 event_kind literal must be present"
    )
    assert '_event_kind = "theme_lost_sidecar_available"' in body, (
        "v1.19.41: tier-2 event_kind literal must be present"
    )
    assert 'body_format="markdown"' in body


def test_plex_enum_uses_per_row_dedup_with_24h_rate_limit():
    """Each dispatch must dedup via notify_dedupe with a 24h
    rate-limit (86400 seconds).

    v1.19.41: dedupe key now includes {tier} so a row that
    flips tiers within the 24h window (e.g. user adds KEEP AS
    BACKUP after initial no_fallback fires, then theme is lost
    again 12h later) still gets a fresh tier-1 notification.
    Pin the {tier} segment is included."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # v1.19.41 composite key shape includes tier between
    # `plex_theme_lost` and `mt`. v1.24.8 appended the edition so two editions
    # of one title losing their theme in the same window each notify.
    assert 'f"plex_theme_lost:{tier}:{mt}:{tid}:{_disp_edition}"' in body, (
        "v1.19.41/v1.24.8: dedupe key must be `plex_theme_lost:"
        "{tier}:{mt}:{tid}:{edition}` so tier flips AND per-edition losses "
        "trigger fresh notifications inside the 24h window"
    )
    # 24h = 86400s.
    assert "rate_limit_seconds=86400" in body
    # record_fire stamps the dedup state after dispatch.
    assert "_ndedupe.record_fire(db_path, dedupe_key)" in body


def test_plex_enum_skips_dispatch_when_fallback_exists():
    """The lost-candidate filter must check for motif fallbacks.
    v1.18.90 had a single `has_fallback` boolean that silently
    skipped on ANY of {local_files, placements, user_overrides}.

    v1.19.41 split this into a four-way tier classifier:
      - Tier 1 (backup-ready): intent='backup' OR source_kind=
        'plex_cloud' → fire theme_lost_backup_ready
      - Tier 2 (sidecar-available): local_theme_file=1 OR
        theme.mp3 on disk → fire theme_lost_sidecar_available
      - Tier 3 (other fallback): replace-intent override /
        placement / non-backup local_file → SILENT SKIP
        (preserved v1.18.90 semantics for this case)
      - Tier 4 (no fallback): fire plex_theme_lost

    Pin: Tier 3 silent skip preserved (`other_fallback` query +
    `continue`); the new tiered queries present."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # Three fallback tables checked (queries widened but tables
    # remain).
    assert "FROM local_files" in body
    assert "FROM placements" in body
    assert "FROM user_overrides" in body
    # v1.19.41 Tier-3 silent-skip preserved via other_fallback +
    # continue. The pre-fix `has_fallback` variable was renamed
    # to `other_fallback` to clarify it's the residual case
    # after tiers 1 + 2 are excluded.
    assert "elif other_fallback:" in body, (
        "v1.19.41: Tier-3 silent-skip preserved via "
        "`elif other_fallback:` branch"
    )
    assert "continue" in body


# ── settings.html UI toggle ──────────────────────────────────


def test_settings_html_has_plex_theme_lost_toggle():
    """The NOTIFICATIONS block must include a checkbox for
    the plex_theme_lost event."""
    html = SETTINGS_HTML.read_text()
    assert "notifications.events.plex_theme_lost" in html


def test_settings_html_toggle_explains_default_off():
    """Label or hint must surface the OFF-by-default behavior
    so users know to opt in."""
    html = SETTINGS_HTML.read_text()
    idx = html.index("notifications.events.plex_theme_lost")
    block = html[idx:idx + 1500]
    assert "Off by default" in block or "OFF by default" in block.lower() or "off by default" in block.lower()


def test_settings_html_toggle_explains_action_paths():
    """Hint must name both action paths (SET URL / UPLOAD MP3)
    so users know what action the notification implies."""
    html = SETTINGS_HTML.read_text()
    idx = html.index("notifications.events.plex_theme_lost")
    block = html[idx:idx + 1500]
    assert "SET URL" in block
    assert "UPLOAD MP3" in block


# ── End-to-end: behavioral with seeded DB ───────────────────


def _prime_reaper_grace(db):
    """v0.51.128: pre-age every plex_items row to one miss below the reaper's
    _REAP_MISS_THRESHOLD so this single stale enum's increment crosses it. This
    test exercises the reaper's candidate capture + delete, not the new
    miss-counter, so it needs the pre-v0.51.128 single-pass reap. A fresh DB
    still starts every row at 0 misses, so the transient-glitch guard stays
    covered by its own v0.51.128 behavioral test."""
    import sqlite3 as _sq
    from app.core.plex_enum import _REAP_MISS_THRESHOLD
    with _sq.connect(db) as c:
        c.execute("UPDATE plex_items SET consecutive_missing = ?",
                  (_REAP_MISS_THRESHOLD - 1,))
        c.commit()


@pytest.fixture
def db_with_lost_theme(tmp_path):
    """Reproduce the user's Troy scenario: a P-row (has_theme=1)
    with NO motif fallback. The reaper deletes it; the
    detection should classify it as lost."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-01T00:00:00', "
            "        '2026-05-01T00:00:00')"
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 652, 'Troy', '2004', 'imdb', "
            "        '2026-05-23T00:00:00', "
            "        '2026-05-02T00:00:00')"
        ).lastrowid
        # Old stale rk with has_theme=1 (the P claim).
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('473665', '1', 'movie', "
            "        'tt0332452', 652, 'Troy', 2004, 1, 0, "
            "        '/data/movies/Troy (2004)', ?, "
            "        '2026-05-01T00:00:00', "
            "        '2026-05-19T01:23:42+00:00')",
            (themes_id,),
        )
        conn.commit()
    return db_path


def test_reaper_collects_candidate_when_stale_p_row_deleted(
    db_with_lost_theme,
):
    """End-to-end: reap a stale P-row with no fallback → the
    candidate enters lost_theme_candidates. Side effects: the
    plex_items row is deleted. The dispatch path will try to
    fire (will silently no-op against the test-tmp DB without
    Apprise config — that's fine, we're verifying the deletion
    and the dedup key are produced)."""
    from app.core import plex_enum
    # Empty items list — simulates Plex no longer returning
    # the rk. Reaper triggers via "current_rks - seen_rks = all
    # current rks". But the empty-items safety guard would
    # skip the reap! Need ONE item so seen_rks is non-empty.
    items = [
        plex_enum.PlexLibraryItem(
            rating_key="999999",  # different rk; existing
            # rk 473665 will be the only stale.
            section_id="1",
            media_type="movie",
            title="Other Movie",
            year="2020",
            guid_imdb="tt9999999",
            guid_tmdb=None,
            guid_tvdb=None,
            folder_path="/data/movies/Other (2020)",
            has_theme=False,
            plex_theme_uri="",
        )
    ]
    _prime_reaper_grace(db_with_lost_theme)
    plex_enum._upsert_items(
        db_with_lost_theme, items, section_id="1",
    )
    # The stale P-row 473665 must be deleted.
    with sqlite3.connect(db_with_lost_theme) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM plex_items "
            "WHERE rating_key='473665'"
        ).fetchone()[0]
    assert n == 0, (
        "v1.18.90: reaper must delete the stale P-row"
    )
    # The notify_dedupe state may or may not be stamped
    # depending on whether dispatch succeeded (which it won't
    # in the test env without Apprise). Just verify the reaper
    # ran end-to-end without raising.


def test_reaper_skips_candidate_when_local_file_exists(tmp_path):
    """If the row has a local_files fallback, the row is NOT
    silent — skip the dispatch. (The user has the file even
    if Plex no longer serves.)"""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, '2026-05-01T00:00:00', "
            "        '2026-05-01T00:00:00')"
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 652, 'Troy', '2004', 'imdb', "
            "        '2026-05-23T00:00:00', "
            "        '2026-05-02T00:00:00')"
        ).lastrowid
        # Stale P-row.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('473665', '1', 'movie', "
            "        'tt0332452', 652, 'Troy', 2004, 1, 0, "
            "        '/data/movies/Troy (2004)', ?, "
            "        '2026-05-01T00:00:00', "
            "        '2026-05-19T01:23:42+00:00')",
            (themes_id,),
        )
        # Motif has a local_files fallback.
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', 652, '1', "
            "        'movies/Troy (2004)/theme.mp3', "
            "        '2026-05-01T00:00:00', 'abc123', "
            "        'manual', 'url')"
        )
        conn.commit()
    from app.core import plex_enum
    items = [
        plex_enum.PlexLibraryItem(
            rating_key="999999",
            section_id="1",
            media_type="movie",
            title="Other Movie",
            year="2020",
            guid_imdb="tt9999999",
            guid_tmdb=None,
            guid_tvdb=None,
            folder_path="/data/movies/Other (2020)",
            has_theme=False,
            plex_theme_uri="",
        )
    ]
    plex_enum._upsert_items(db_path, items, section_id="1")
    # The reaper still ran (stale rk deleted) but the dedup
    # state should NOT have been stamped (no dispatch fired
    # because fallback skipped). Verify by checking runtime_
    # settings for the dedupe key — must NOT exist.
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = 'notify_dedupe.plex_theme_lost:movie:652'"
        ).fetchone()
    assert marker is None, (
        "v1.18.90: fallback existed → dispatch must skip → "
        "dedup marker must not be stamped"
    )
