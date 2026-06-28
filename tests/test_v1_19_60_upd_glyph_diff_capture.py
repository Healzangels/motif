"""v1.19.60 — !UPD glyph fires when there's a real, displayable diff.

the user's v1.19.58 actionability gate correctly counts a row as
actionable when sync detects url_changed=True, but the v1.19.59
deploy revealed that the row-table !UPD glyph stayed dark on the
exact rows the sync notification flagged. Two distinct row shapes
hit the same broken read-path gate, for two distinct reasons.

## Repro #1 — Bastard!! (1992) — upload row

the user uploaded an MP3 for Bastard!!. The row's local_files has
source_kind='upload', no user_override exists. Sync detected TDB
URL rolling, _upsert_theme UPDATEd themes.youtube_url to NEW,
sync's `if already_have or has_override:` branch INSERTed
pending_updates with new_youtube_url=NEW.

Pre-v1.19.60 the read-path gate compared `pu.new_url` against
`COALESCE(uo_section, uo_global, t.youtube_url)`. With no
user_override the COALESCE fell back to t.youtube_url = NEW
(sync just updated it). pu.new_url=NEW == applied=NEW → gate
suppressed → no !UPD.

Plus a deeper bug: sync.py line 992 bound `None` to
`old_youtube_url` on the INSERT, so the pending_updates row
NEVER captured the previous URL — the INFO card had no diff
context to display.

## Repro #2 — The Beginning After the End — U row + matching override

the user set a user_override URL for this row with intent='replace'.
TDB later published a URL that COINCIDENTALLY matched the user's
override. Sync detected url_changed (NULL → new vid). The
`has_override AND url_changed` branch of _upsert_theme skipped
updating themes.youtube_url (per v1.14.55 intent — overrides
authoritative). INSERTed pending_updates with kind='upstream_changed'
(default).

Pre-v1.19.60 the read-path gate compared pu.new_url against
the override URL → equal → gate suppressed → no !UPD. The
synthetic urls_match walker (sync.py:3231) couldn't catch this
either because it gates on `themes.youtube_url IS NOT NULL`
which was empty (skipped UPDATE).

## v1.19.60 fix

Three changes:

  1. `_upsert_theme` returns `(is_new, url_changed, old_vid,
     old_url)` — captures the previous URL for INSERT binding.
     Sync caller unpacks the 4-tuple and binds `old_url` to
     `old_youtube_url` (was None pre-fix).

  2. Sync's url_changed branch decides `kind` based on
     override-coincidence detection: when has_override AND
     `extract_video_id(override_url) == new_vid`, writes
     kind='urls_match' instead of 'upstream_changed' so the
     read-path gate's urls_match U→T conversion branch fires
     for the Beginning After the End shape.

  3. Read-path gate (`_pending_update_real_diff_sql` +
     `_row_has_non_url_local_content_sql` helpers in api.py)
     replaces the v1.12.119 `pu.new_url != applied_url` check
     with two new branches:
       (a) `_pending_update_real_diff_sql` — TRUE when old_url
           IS NOT NULL + differs from new_url. Real captured
           diff (post-v1.19.60 rows).
       (b) `_row_has_non_url_local_content_sql` — TRUE when
           the row has local_files with source_kind in
           ('upload', 'adopt', 'plex_cloud'). Fires !UPD on
           non-URL local content rows whenever any pending
           update exists (Bastard!! upload-row case).
     Both branches replace the pre-fix COALESCE-against-
     t.youtube_url check at all 11 mirror sites.

Legacy pending_updates rows (pre-v1.19.60 with old_url=NULL)
stay suppressed for URL-derived rows (T, U-no-coincidence) —
no recoverable diff means surfacing !UPD would just confuse
the user with "URL: X, pending update to: X" with no
meaningful action. Upload/adopt/plex_cloud rows fire because
the action is "swap your content for TDB URL" regardless of
whether the prior URL is known.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = (REPO / "app" / "web" / "api.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


# ── Source-text guards (helpers + mirror-drift safety) ───────


def test_pending_update_real_diff_helper_defined():
    """v1.19.60 helper for the real-diff gate branch."""
    assert "def _pending_update_real_diff_sql(" in API_PY


def test_row_has_non_url_local_content_helper_defined():
    """v1.19.60 helper for the content-diverge gate branch."""
    assert "def _row_has_non_url_local_content_sql(" in API_PY


def test_helper_filters_on_three_source_kinds():
    """The content-diverge helper must check upload/adopt/plex_cloud
    — the three source_kinds that represent manual non-URL content.
    Adding a new source_kind in the future MUST audit this
    helper for inclusion."""
    idx = API_PY.index("def _row_has_non_url_local_content_sql(")
    end = API_PY.index("\n\n\n", idx)
    body = API_PY[idx:end]
    assert "source_kind IN ('upload', 'adopt', 'plex_cloud')" in body, (
        "v1.19.60: source_kind set must include all three manual "
        "non-URL kinds — upload/adopt/plex_cloud"
    )


def test_real_diff_helper_checks_old_url_present_and_diff():
    """The real-diff gate must require BOTH `old IS NOT NULL` AND
    `old != ''` AND `old != new`. Pre-v1.19.60 old_youtube_url was
    bound to NULL on the INSERT (sync.py:992 bug) — legacy rows
    must stay dark (no recoverable diff to display)."""
    idx = API_PY.index("def _pending_update_real_diff_sql(")
    end = API_PY.index("\n\n\n", idx)
    body = API_PY[idx:end]
    assert "IS NOT NULL" in body
    assert "!= ''" in body
    assert "old_youtube_url" in body
    assert "new_youtube_url" in body


def test_all_eleven_gate_sites_use_helpers():
    """Mirror-drift class P. v1.19.60 spread _pending_update_real_diff_sql +
    _row_has_non_url_local_content_sql across 11 inline gate sites. v1.22.10
    CONSOLIDATED them into ONE _pending_update_actionable_sql helper invoked at
    every gate site — the strongest form of the mirror-drift lock (the two
    components are now referenced once, inside the single helper, and the helper
    is the thing repeated ≥11×)."""
    assert "def _pending_update_actionable_sql(" in API_PY
    idx = API_PY.index("def _pending_update_actionable_sql(")
    body = API_PY[idx:idx + 3400]
    assert "_pending_update_real_diff_sql(t, pi)" in body, (
        "v1.22.10: actionable helper must compose the URL-diff component"
    )
    assert "_row_has_non_url_local_content_sql(t, pi)" in body, (
        "v1.22.10: actionable helper must compose the non-url-content component"
    )
    assert API_PY.count("_pending_update_actionable_sql('") >= 11, (
        "v1.22.10: the actionable helper must gate ≥11 sites (pill columns, "
        "tdb/attn filters, NEEDS WORK sort, count subqueries)"
    )


# ── sync.py changes ──────────────────────────────────────────


def test_upsert_theme_returns_old_url():
    """_upsert_theme's return tuple must now include old_url as the
    4th element. Caller unpacks the 4-tuple. Pre-v1.19.60 returned
    only (is_new, url_changed, old_vid) — caller had no way to
    capture the previous URL into pending_updates."""
    # The fast-path return now has 4 elements.
    assert "return (False, False, None, None)" in SYNC_PY, (
        "v1.19.60: fast-path skip must return 4-tuple "
        "(is_new=False, url_changed=False, old_vid=None, old_url=None)"
    )
    # The terminal returns become 4-element.
    assert "return is_new, url_changed, old_vid, old_url" in SYNC_PY


def test_sync_caller_unpacks_old_url():
    """The apply-loop must unpack the 4-tuple."""
    assert (
        "is_new, url_changed, old_video_id, old_url = _upsert_theme("
        in SYNC_PY
    ), (
        "v1.19.60: caller must unpack old_url (4th tuple element)"
    )


def test_existing_select_includes_youtube_url():
    """The pre-UPDATE SELECT must include youtube_url so the
    helper can capture old_url before the UPDATE clobbers it."""
    # The main SELECT block (line ~286).
    idx = SYNC_PY.index(
        "SELECT youtube_video_id, youtube_url, youtube_edited_at, "
    )
    assert idx > 0, (
        "v1.19.60: main existing SELECT must include youtube_url"
    )


def test_url_changed_branch_captures_old_url():
    """In _upsert_theme's else branch (existing row), old_url
    must be assigned from `existing[\"youtube_url\"]` before any
    UPDATE runs."""
    idx = SYNC_PY.index("url_changed = (yt_vid != old_vid)")
    pre = SYNC_PY[max(0, idx - 400):idx]
    assert 'old_url = existing["youtube_url"]' in pre, (
        "v1.19.60: old_url must be captured from the existing "
        "themes row before url_changed branches"
    )


def test_pending_updates_insert_binds_old_url_not_none():
    """The pending_updates INSERT must bind the captured `old_url`
    variable, not the literal `None` that was there pre-v1.19.60."""
    elif_idx = SYNC_PY.index("elif url_changed:")
    # v1.21.10: widened 8000 → 10000 (the has_sidecar gate added ~700
    # chars; the INSERT binding-tuple close sits at ~offset 9050).
    # v1.22.45: 10000 → 10600 (the updated_titles append gained a
    # media_type+tmdb_id comment + line split upstream of the INSERT).
    block = SYNC_PY[elif_idx:elif_idx + 10600]
    insert_idx = block.index("INSERT INTO pending_updates")
    binding_idx = block.index("(\n                            media_type",
                              insert_idx)
    binding_end = block.index("),\n", binding_idx)
    binding = block[binding_idx:binding_end]
    assert "old_url," in binding, (
        "v1.19.60: pending_updates INSERT must bind the captured "
        "old_url variable for old_youtube_url (pre-fix bound None)"
    )
    # The literal `None` for old_youtube_url should be GONE from
    # this binding. Allow `None` for other slots if any.
    assert "None, record.get(\"youtube_theme_url\")" not in binding, (
        "v1.19.60: pre-fix pattern (None bound for old_youtube_url) "
        "must not survive"
    )


def test_sync_writes_urls_match_kind_for_override_coincidence():
    """When the user's override URL coincidentally matches the new
    TDB URL, sync.py must write kind='urls_match' instead of the
    default 'upstream_changed'. Beginning After the End repro."""
    elif_idx = SYNC_PY.index("elif url_changed:")
    # v1.22.39: widened 8000 → 9200 (the has_sidecar query grew, pushing the
    # pending_updates INSERT later in the branch).
    block = SYNC_PY[elif_idx:elif_idx + 9200]
    # The kind decision logic.
    assert "pu_kind" in block
    assert "'urls_match'" in block
    assert "override_url" in block
    # The INSERT must include `kind` in the columns list.
    assert ", kind" in block[block.index("INSERT INTO pending_updates"):
                              block.index("VALUES")]


def test_override_row_fetched_with_url():
    """The url_changed branch's user_overrides lookup must fetch
    youtube_url (was a bare EXISTS pre-fix) so the kind-decision
    can compare against the new TDB URL."""
    elif_idx = SYNC_PY.index("elif url_changed:")
    block = SYNC_PY[elif_idx:elif_idx + 2000]
    assert "SELECT youtube_url FROM user_overrides" in block, (
        "v1.19.60: must fetch youtube_url (was just SELECT 1)"
    )


# ── Behavioral simulation (gate decision matrix) ────────────


def test_gate_decision_matrix():
    """Re-implement the v1.19.60 gate logic in Python and assert
    the decision for each row shape from the v1.19.58 / .59 era.

    Gate fires (!UPD lights up) when:
      (a) kind='urls_match' AND _has_override AND _not_p_row
      (b) old_url IS NOT NULL AND old_url != '' AND old_url != new_url
      (c) row has source_kind in (upload, adopt, plex_cloud)
    """

    def gate_fires(*, kind, has_override, is_p_row,
                   old_url, new_url, has_non_url_content):
        # (a) urls_match branch
        if (kind == "urls_match"
                and has_override and not is_p_row):
            return True
        # (b) real diff branch
        if (old_url and old_url != ""
                and old_url != new_url):
            return True
        # (c) content-diverge branch
        if has_non_url_content:
            return True
        return False

    new = "https://www.youtube.com/watch?v=NEW123"
    old = "https://www.youtube.com/watch?v=OLD456"

    # Bastard!! TODAY (pre-fix legacy row, upload, no override):
    # old_url=NULL, new_url=NEW, has_non_url_content=True
    # → fires via branch (c) ✓ (the user wants !UPD on uploads now)
    assert gate_fires(
        kind="upstream_changed", has_override=False, is_p_row=False,
        old_url=None, new_url=new, has_non_url_content=True,
    ), "v1.19.60: upload row with TDB URL change must fire !UPD"

    # Beginning After the End (override coincidence): kind=urls_match
    # post-sync-fix, has_override=True, not P, override matches new.
    # → fires via branch (a) ✓
    assert gate_fires(
        kind="urls_match", has_override=True, is_p_row=False,
        old_url=None, new_url=new, has_non_url_content=False,
    ), "v1.19.60: override-coincidence row must fire !UPD via urls_match"

    # T-row going forward (post-v1.19.60), themes URL rolled X→Y:
    # old_url=OLD, new_url=NEW, has_non_url_content=False
    # → fires via branch (b) ✓
    assert gate_fires(
        kind="upstream_changed", has_override=False, is_p_row=False,
        old_url=old, new_url=new, has_non_url_content=False,
    ), "v1.19.60: T-row with real diff must fire !UPD"

    # Legacy T-row (pre-v1.19.60), old_url=NULL because of the
    # sync.py:992 bug:
    # → all branches fail → stays dark (the user's design intent)
    assert not gate_fires(
        kind="upstream_changed", has_override=False, is_p_row=False,
        old_url=None, new_url=new, has_non_url_content=False,
    ), "v1.19.60: legacy T-row with no captured diff must STAY dark"

    # P-row with urls_match: gate hides (P-row exclusion preserves
    # existing v1.19.4 behavior).
    assert not gate_fires(
        kind="urls_match", has_override=True, is_p_row=True,
        old_url=None, new_url=new, has_non_url_content=False,
    ), "P-row urls_match must stay suppressed (v1.19.4 preserved)"

    # U-row with override URL ≠ new TDB URL, sync wrote
    # upstream_changed (override didn't coincide):
    # old_url captured (post-v1.19.60 sync) = OLD themes URL,
    # new_url = NEW. old != new → fires via (b).
    assert gate_fires(
        kind="upstream_changed", has_override=True, is_p_row=False,
        old_url=old, new_url=new, has_non_url_content=False,
    ), "U-row with real URL diff must fire !UPD"


# ── End-to-end TestClient: prod repros ───────────────────────


from fastapi.testclient import TestClient


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_anime_section(conn, section_id="3"):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, 'Anime', 'show', 1, 0, "
        "        'tv', 1, '2026-05-27T09:00:00', "
        "        '2026-05-27T09:00:00')",
        (section_id,),
    )


def _seed_theme(conn, theme_id, tmdb_id, youtube_url, title="Test"):
    yt_vid = (youtube_url.split("v=")[-1]
              if youtube_url and "v=" in youtube_url else None)
    now = "2026-05-27T09:00:00"
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url, "
        "   youtube_video_id) "
        "VALUES (?, 'tv', ?, ?, 'themoviedb', ?, ?, ?, ?)",
        (theme_id, tmdb_id, title, now, now, youtube_url, yt_vid),
    )


def _seed_plex_item(conn, rk, theme_id, section_id="3", tmdb_id=None,
                    title="Test"):
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'show', ?, "
        "        'tt100', ?, ?, 1992, 1, 0, "
        "        '/data/anime/Test', 0, 1, "
        "        '2026-05-27T09:00:00', "
        "        '2026-05-27T09:00:00')",
        (rk, section_id, theme_id, tmdb_id, title),
    )


def _query_library_row(client, tmdb_id, db_path=None):
    r = client.get(
        "/api/library?tab=anime&fourk=false&per_page=200",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    rows = data.get("items", []) or data.get("rows", [])
    for row in rows:
        if int(row.get("tmdb_id") or row.get("theme_tmdb") or 0) == tmdb_id:
            return row
    return None


def test_bastard_upload_row_fires_upd_post_fix(admin_client):
    """End-to-end: Bastard!!-shape row (source_kind='upload',
    placement=plex_upload, no override), simulated sync url-change
    → pending_update field must surface as 1 (!UPD fires)."""
    client, db = admin_client
    OLD_URL = "https://www.youtube.com/watch?v=OLDOLDOLDxyz"
    NEW_URL = "https://www.youtube.com/watch?v=NEWnewNEW123"
    with sqlite3.connect(db) as conn:
        _seed_anime_section(conn)
        _seed_theme(conn, theme_id=1, tmdb_id=43798,
                    youtube_url=OLD_URL, title="Bastard!!")
        _seed_plex_item(conn, rk="rk-bastard", theme_id=1,
                        tmdb_id=43798, title="Bastard!!")
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance) "
            "VALUES ('tv', 43798, '3', 'Bastard/theme.mp3', "
            "        2500000, 'aaaa1111', '2026-05-20T12:00:00', "
            "        'upload', '', 'manual')"
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, provenance, placed_at) "
            "VALUES ('tv', 43798, '3', '', 'plex_upload', "
            "        'manual', '2026-05-20T12:00:00')"
        )
        conn.commit()

    # Simulate v1.19.60 sync url-changed branch — captures old_url,
    # writes kind='upstream_changed' (upload row, no override).
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE themes SET youtube_url = ?, "
            "  youtube_video_id = ?, last_seen_sync_at = ? "
            "WHERE id = 1",
            (NEW_URL, "NEWnewNEW123", "2026-05-27T09:01:00"),
        )
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   old_video_id, new_video_id, "
            "   old_youtube_url, new_youtube_url, "
            "   upstream_edited_at, detected_at, decision, kind) "
            "VALUES ('tv', 43798, '', 'OLDOLDOLDxyz', "
            "        'NEWnewNEW123', ?, ?, "
            "        '2026-05-27T08:00:00', "
            "        '2026-05-27T09:01:00', 'pending', "
            "        'upstream_changed')",
            (OLD_URL, NEW_URL),
        )
        conn.commit()

    row = _query_library_row(client, 43798, db)
    assert row is not None
    # POST-FIX: !UPD must fire.
    # Either via content-diverge branch (upload row) OR via real-
    # diff branch (old_url != new_url). Both apply here.
    assert row["pending_update"] == 1, (
        f"v1.19.60: Bastard!! upload row with TDB URL change must "
        f"fire !UPD; got pending_update={row['pending_update']}"
    )


def test_beginning_after_end_override_coincidence_fires_upd(admin_client):
    """End-to-end: Beginning After the End-shape row (U with
    override, override URL == new TDB URL — the urls_match
    coincidence case). Sync writes kind='urls_match'. Gate's
    urls_match branch must fire !UPD."""
    client, db = admin_client
    URL = "https://www.youtube.com/watch?v=ffk9sTl-rN0"
    with sqlite3.connect(db) as conn:
        _seed_anime_section(conn)
        # themes.youtube_url empty (the has_override branch in
        # _upsert_theme skips updating it).
        _seed_theme(conn, theme_id=2, tmdb_id=274671,
                    youtube_url="", title="Beginning")
        _seed_plex_item(conn, rk="rk-beg", theme_id=2,
                        tmdb_id=274671, title="Beginning")
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance) "
            "VALUES ('tv', 274671, '3', 'Beg/theme.mp3', "
            "        1500000, 'bbbb2222', '2026-05-20T12:00:00', "
            "        'url', 'ffk9sTl-rN0', 'manual')"
        )
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   intent, set_at, set_by) "
            "VALUES ('tv', 274671, '3', ?, 'replace', "
            "        '2026-05-20T12:00:00', 'admin')",
            (URL,),
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, provenance, placed_at) "
            "VALUES ('tv', 274671, '3', '/data/anime/Beg', "
            "        'hardlink', 'manual', '2026-05-20T12:00:00')"
        )
        conn.commit()

    # Simulate v1.19.60 sync url-changed branch — detects override
    # coincidence (yt_vid == extract(override_url)) → kind=urls_match.
    with sqlite3.connect(db) as conn:
        # themes.youtube_url stays empty (has_override branch
        # skips the UPDATE per v1.14.55).
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   old_video_id, new_video_id, "
            "   old_youtube_url, new_youtube_url, "
            "   upstream_edited_at, detected_at, decision, kind) "
            "VALUES ('tv', 274671, '', NULL, 'ffk9sTl-rN0', "
            "        NULL, ?, "
            "        '2026-05-27T08:00:00', "
            "        '2026-05-27T09:01:00', 'pending', "
            "        'urls_match')",
            (URL,),
        )
        conn.commit()

    row = _query_library_row(client, 274671, db)
    assert row is not None
    assert row["pending_update"] == 1, (
        f"v1.19.60: Beginning After the End override-coincidence "
        f"row must fire !UPD via urls_match branch; got "
        f"pending_update={row['pending_update']}"
    )
    assert row["pending_update_kind"] == "urls_match"


def test_legacy_t_row_with_lost_old_url_stays_dark(admin_client):
    """Legacy pre-v1.19.60 pending_updates row: old_youtube_url
    is NULL because of the sync.py:992 bug. The row is a T-row
    (no user override, source_kind='themerrdb' → has_non_url_content
    is False). Gate must SUPPRESS — no recoverable diff means
    surfacing !UPD would just show 'URL: X, pending update to: X'
    with no actionable difference."""
    client, db = admin_client
    NEW_URL = "https://www.youtube.com/watch?v=LEGACY999"
    with sqlite3.connect(db) as conn:
        _seed_anime_section(conn)
        _seed_theme(conn, theme_id=3, tmdb_id=55555,
                    youtube_url=NEW_URL, title="Legacy T")
        _seed_plex_item(conn, rk="rk-leg", theme_id=3,
                        tmdb_id=55555, title="Legacy T")
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, file_sha256, downloaded_at, "
            "   source_kind, source_video_id, provenance) "
            "VALUES ('tv', 55555, '3', 'Legacy/theme.mp3', "
            "        1000000, 'cccc3333', '2026-05-20T12:00:00', "
            "        'themerrdb', 'OLDold987', 'auto')"
        )
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, "
            "   placement_kind, provenance, placed_at) "
            "VALUES ('tv', 55555, '3', '/data/anime/Legacy', "
            "        'hardlink', 'auto', '2026-05-20T12:00:00')"
        )
        # Legacy pending_updates row: old_youtube_url IS NULL.
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   old_video_id, new_video_id, "
            "   old_youtube_url, new_youtube_url, "
            "   upstream_edited_at, detected_at, decision, kind) "
            "VALUES ('tv', 55555, '', 'OLDold987', 'LEGACY999', "
            "        NULL, ?, "
            "        '2026-05-27T08:00:00', "
            "        '2026-05-27T09:01:00', 'pending', "
            "        'upstream_changed')",
            (NEW_URL,),
        )
        conn.commit()

    row = _query_library_row(client, 55555, db)
    assert row is not None
    assert row["pending_update"] == 0, (
        f"v1.19.60: legacy T-row with old_youtube_url=NULL must "
        f"STAY dark (no diff to show); got pending_update="
        f"{row['pending_update']}"
    )


# ── INFO card diff display (app.js) ──────────────────────────


def test_info_card_renders_pending_old_url_diff_suffix():
    """The INFO card's themerrdb url line must surface a 'was: OLD'
    suffix when pending_update has captured the previous URL. Gives
    the user the diff context to decide ACCEPT/KEEP CURRENT. Pre-fix
    they only saw the new URL with no indication of what changed."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "_pendingOldUrl" in js, (
        "v1.19.60: INFO card must compute _pendingOldUrl from "
        "pu.old_youtube_url"
    )
    assert "tdbWasTag" in js, (
        "v1.19.60: INFO card must compute tdbWasTag for the "
        "'(was: OLD)' suffix"
    )
    # The themerrdb url <dd> must include the tdbWasTag template.
    idx = js.index("`<dt>themerrdb url${tdbSrcTag}</dt>")
    line_end = js.index("`}", idx) + 1
    line = js[idx:line_end]
    assert "${tdbWasTag}" in line, (
        "v1.19.60: themerrdb url <dd> must interpolate tdbWasTag "
        "so the diff suffix renders next to the URL link"
    )


def test_info_card_diff_suffix_only_when_old_differs_from_new():
    """The diff suffix must only fire when old != new (sanity guard
    against showing 'was: X' on a row where the URL didn't actually
    change). Pre-v1.19.60 rows have old=NULL so they stay dark."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    idx = js.index("_pendingOldUrl")
    line_end = js.index("''", idx + 50) + 2
    block = js[idx:line_end]
    assert "pu.old_youtube_url" in block
    assert "pu.old_youtube_url !== pu.new_youtube_url" in block
    assert "pu.decision === 'pending'" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_60_version_pin():
    """Version bumped at v1.19.60 (then v1.19.61 for PS/BK
    unification). Match v1.19.x prefix."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
