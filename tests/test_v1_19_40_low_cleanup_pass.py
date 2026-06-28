"""v1.19.40 — LOW cleanup pass closing the deferred audit findings.

Five small drift fixes from the original post-v1.19.32 deep audit:

  1. **UPLOAD MP3 hint stale** (library.html:663) — claimed
     "ThemerrDB doesn't track this title" but UPLOAD MP3 is
     reachable on every row via the SOURCE menu since v1.12.39.
     For T/A/U/M rows the upload OVERRIDES the current source;
     the orphan-only framing misleads. Rewritten to describe
     the any-row behavior + the KEEP AS BACKUP option for P-rows.

  2. **bk_override.youtube_url always None** (api.py:~16229) —
     the synthesized BK-state override read `row["youtube_url"]`
     with a graceful fallback that always fell back, because the
     themes SELECT at api.py:~15819 didn't include `youtube_url`.
     v1.19.40 widens the SELECT so the synthetic override carries
     the staged URL.

  3. **synthetic marker dead-doc obsolete** (api.py:~16234) — at
     audit time the `synthetic: True` marker had no JS consumer;
     v1.19.39 made it load-bearing via the PROMOTE TO ACTIVE
     tooltip branch. v1.19.40 adds a doc comment so the next
     reader doesn't think it's still dead.

  4. **CLAUDE.md SRC-axis line numbers stale** (200-400 off) +
     missing the sixth site added by v1.19.38. v1.19.40 updates
     the table.

  5. **JS↔SQL asymmetry doc comments** (api.py _SRC_LETTER_SQL +
     _is_p_row_for_section) — both have known asymmetries with
     their mirror-counterpart that converge in practice but
     could silently disagree if writers/contracts change. Doc
     comments inline note the constraints so future readers
     know what to preserve.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient

LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
CLAUDE_MD = (REPO / "CLAUDE.md").read_text()


# ── 1: UPLOAD MP3 hint rewritten ─────────────────────────────


def _upload_dlg_block_no_jinja_comments() -> str:
    """Slice the upload-dlg block AND strip Jinja `{# ... #}`
    comment blocks. The v1.19.40 doc comment preserves the
    pre-fix copy text inside a Jinja comment for archaeology;
    we don't want that to count as "still in the rendered HTML."
    """
    import re
    dlg_start = LIBRARY_HTML.index('<dialog class="dlg" id="upload-dlg">')
    dlg_end = LIBRARY_HTML.index("</dialog>", dlg_start)
    block = LIBRARY_HTML[dlg_start:dlg_end]
    return re.sub(r"{#.*?#}", "", block, flags=re.DOTALL)


def test_upload_dlg_hint_no_longer_claims_orphan_only():
    """The hint text under #upload-file must NOT claim 'ThemerrDB
    doesn't track this title' — the action is reachable on every
    row, not just orphans."""
    dlg_block = _upload_dlg_block_no_jinja_comments()
    assert "ThemerrDB doesn't track this title" not in dlg_block, (
        "v1.19.40: stale orphan-only framing must be gone — the "
        "action is reachable on every row via the SOURCE menu"
    )


def test_upload_dlg_hint_mentions_any_row_override_semantics():
    """The new hint must describe what happens on a row that
    HAS an existing source (SRC flips to U, overrides current)."""
    dlg_block = _upload_dlg_block_no_jinja_comments()
    assert "SRC flips to U" in dlg_block, (
        "v1.19.40: hint must describe the SRC=U flip so users know "
        "the upload changes the row's source classification"
    )
    # Collapse whitespace so the text wrap (`overrides
    # whatever's\n          currently set`) matches the
    # single-spaced assertion.
    flat = " ".join(dlg_block.split())
    assert "overrides whatever's currently set" in flat, (
        "v1.19.40: hint must mention the override behavior — the "
        "upload REPLACES whatever's there (ThemerrDB / sidecar / etc)"
    )


def test_upload_dlg_hint_cross_references_keep_as_backup_for_p_rows():
    """The hint must point users to the KEEP AS BACKUP checkbox
    for P-rows — the upload-dlg has both forms (replace vs
    backup-only) and the user needs guidance on which to pick."""
    dlg_block = _upload_dlg_block_no_jinja_comments()
    # KEEP AS BACKUP appears in the checkbox label AND now in the
    # v1.19.40 hint. Assert the hint references it specifically
    # (not just the checkbox label).
    # The hint paragraph follows the input element.
    hint_idx = dlg_block.index('id="upload-file"')
    hint_chunk = dlg_block[hint_idx:hint_idx + 800]
    assert "KEEP AS BACKUP" in hint_chunk, (
        "v1.19.40: hint paragraph (right after #upload-file) must "
        "cross-reference the KEEP AS BACKUP checkbox so P-row "
        "users know to use it"
    )


# ── 2: bk_override.youtube_url carries a real URL ───────────


def test_recovery_options_themes_select_includes_youtube_url():
    """`api_recovery_options`'s themes SELECT must include
    `youtube_url` so the v1.19.35 synthesized BK-state override
    can carry it (previously always None)."""
    fn_start = API_PY.index("async def api_recovery_options(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # The first SELECT in the body is the themes row lookup.
    select_idx = body.index("SELECT failure_kind")
    select_chunk = body[select_idx:select_idx + 300]
    assert "youtube_url" in select_chunk, (
        "v1.19.40: themes SELECT in api_recovery_options must "
        "fetch youtube_url so the v1.19.35 synthesized BK-state "
        "override carries a real URL (was always None)"
    )


# ── 3: synthetic marker documented ──────────────────────────


def test_synthetic_marker_consumer_documented():
    """The api.py block that synthesizes the BK-state override
    must document the JS consumer (the v1.19.39 PROMOTE TO
    ACTIVE tooltip branch) so future readers know the marker is
    load-bearing.

    Walk 200 chars BEFORE `bk_override = None` to capture the
    v1.19.40 doc comment (which sits in the preceding comment
    block, not inside the if-body)."""
    fn_start = API_PY.index("async def api_recovery_options(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    synth_idx = body.index("bk_override = None")
    # Window captures the preceding comment block + the if-body.
    chunk = body[max(0, synth_idx - 1200):synth_idx + 1500]
    assert "v1.19.40" in chunk, (
        "v1.19.40: marker required in the BK-state synthesize "
        "block so the dead-doc audit observation is closed"
    )
    assert "v1.19.39" in chunk and "tooltip" in chunk.lower(), (
        "v1.19.40: doc comment must reference the v1.19.39 "
        "PROMOTE TO ACTIVE tooltip consumer"
    )


# ── 4: CLAUDE.md SRC-axis table updated ─────────────────────


def test_claude_md_src_axis_table_has_six_sites():
    """CLAUDE.md SRC-axis table must now list 6 sites — the
    original 5 plus the v1.19.38 bulk-PUSH predicates added by
    the sixth-site drift fix.

    Slice on the next markdown section heading (the table sits
    between `| Site |` and the next paragraph)."""
    table_start = CLAUDE_MD.index("| Site | File:line | Purpose |")
    # Walk forward 3000 chars — the table + its trailing prose
    # is well under that.
    table = CLAUDE_MD[table_start:table_start + 3000]
    # The sixth site must be present.
    assert "Bulk PUSH predicates" in table, (
        "v1.19.40: CLAUDE.md SRC-axis table must include the "
        "v1.19.38 bulk-PUSH predicate sites — the sixth load-"
        "bearing surface for the v1.18.0 widening"
    )
    # All 5 original sites still present (backtick-wrapped
    # identifiers in markdown).
    assert "computeSrcLetter" in table
    assert "renderLibraryRow" in table and "inline-SRC" in table
    assert "isPlexAgentRow" in table
    assert "_SRC_LETTER_SQL" in table


def test_claude_md_src_axis_line_numbers_in_current_range():
    """The line numbers in the CLAUDE.md table must be within
    a few hundred lines of the actual current locations. Pre-
    v1.19.40 the table said ~7367 / ~7461 / ~9344 / ~12241 but
    reality was 7687 / 7790 / 9726 / 12663 (200-400 off)."""
    table_start = CLAUDE_MD.index("| Site | File:line | Purpose |")
    table_end = CLAUDE_MD.index("\n\n", table_start)
    table = CLAUDE_MD[table_start:table_end]
    # Spot-check each updated line number against the actual app.js.
    app_js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    actual_compute_line = (
        app_js[:app_js.index("function computeSrcLetter")]
              .count("\n") + 1
    )
    # Look for the documented ~Nxx for computeSrcLetter.
    import re
    match = re.search(
        r"computeSrcLetter`\s*\|\s*`app\.js:~(\d+)", table,
    )
    assert match, "no documented line number for computeSrcLetter"
    documented = int(match.group(1))
    drift = abs(actual_compute_line - documented)
    assert drift < 200, (
        f"v1.19.40: documented computeSrcLetter line ~{documented} "
        f"drifted {drift} lines from actual {actual_compute_line} — "
        f"update CLAUDE.md SRC-axis table to current values"
    )


# ── 5: JS↔SQL asymmetry doc comments ────────────────────────


def test_src_letter_sql_documents_schema_not_null_dependence():
    """`_SRC_LETTER_SQL` must carry a doc comment noting it
    depends on `placements.media_folder NOT NULL` to converge
    with the JS `placement_kind === 'plex_upload'` widening."""
    # v1.21.57: the SRC SQL is now built by _src_letter_sql(); the
    # v1.19.40 doc comment sits immediately above that def.
    sql_start = API_PY.index("def _src_letter_sql(")
    # Walk back to capture the preceding comment block.
    block_start = API_PY.rfind("# v1.", 0, sql_start)
    block = API_PY[block_start:sql_start + 100]
    assert "NOT NULL" in block, (
        "v1.19.40: _SRC_LETTER_SQL doc must mention the "
        "placements.media_folder NOT NULL schema dependence"
    )
    assert "v1.19.40" in block or "doc:" in block, (
        "v1.19.40: doc comment must carry a v1.19.40 marker so "
        "the future archaeology can find this rationale"
    )


def test_is_p_row_for_section_documents_wider_join_vs_sql_fragment():
    """`_is_p_row_for_section` must carry a doc comment noting
    the asymmetry with `_not_p_row_sql` — wider Python helper
    (matches via theme_id OR guid_tmdb) vs narrower SQL fragment
    (theme_id only, via outer JOIN).

    Walk back 2000 chars from the def to capture the full
    multi-paragraph comment block above it (single-line `rfind`
    only catches the LAST comment line)."""
    fn_start = API_PY.index("def _is_p_row_for_section(")
    block = API_PY[max(0, fn_start - 2000):fn_start + 200]
    assert "WIDER" in block.upper(), (
        "v1.19.40: _is_p_row_for_section doc must note the "
        "wider-than-SQL-fragment join shape"
    )
    assert "v1.19.40" in block, (
        "v1.19.40: doc comment must carry a v1.19.40 marker"
    )


# ── End-to-end behavioral (bk_override URL surfaces) ────────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def test_bk_no_override_response_carries_real_youtube_url(
    admin_client, tmp_path,
):
    """End-to-end: a BK-state row WITHOUT a real user_overrides
    row (post-v1.19.32 ACCEPT UPDATE on P-row shape) must return
    `data.override.youtube_url` populated with themes.youtube_url
    instead of always-None."""
    from app.config import Settings
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,"
            "        '2026-05-26T00:00:00','2026-05-26T00:00:00')"
        )
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (1, 'movie', 700, 'Test', 'imdb', "
            "        '2026-05-26', '2026-05-26', "
            "        'https://www.youtube.com/watch?v=STAGED')"
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-700', '1', 'movie', 1, "
            "        'tt700', 700, 'Test', 2020, 1, 0, "
            "        '/data/movies/Test', 0, 1, "
            "        '2026-05-26', '2026-05-26')"
        )
        # BK-state local_files (no user_overrides — the synthetic-
        # override path is the v1.19.32 ACCEPT UPDATE shape).
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   source_kind, source_video_id, downloaded_at, "
            "   provenance, last_place_attempt_reason) "
            "VALUES ('movie', 700, '1', 'x.mp3', 'themerrdb', "
            "        'STAGED', '2026-05-26', 'auto', 'backup_only')"
        )
        conn.commit()

    r = admin_client.get(
        "/api/items/movie/700/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("backup_state") is True
    ovr = body.get("override")
    assert ovr is not None
    assert ovr.get("synthetic") is True
    # The v1.19.40 fix: youtube_url must be populated.
    assert ovr.get("youtube_url") == (
        "https://www.youtube.com/watch?v=STAGED"
    ), (
        f"v1.19.40: synthesized override must carry "
        f"themes.youtube_url (was always None pre-v1.19.40); "
        f"got {ovr.get('youtube_url')}"
    )
