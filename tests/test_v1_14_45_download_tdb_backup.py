"""v1.14.45 — DOWNLOAD TDB BACKUP for pure-P + TDB rows.

the user's design intent: "have a ThemerDB backup file in
downloads but also allow plex to have a theme, only would be
for options that are also in ThemerrDB. This is purely optional
and just provides a way for motif to manage a theme but also
allow plex to have a theme."

Use case: pure-P row (Plex serves its own theme, motif owns
nothing) where TDB has the title. User wants a local backup of
TDB's theme without disturbing Plex's serving copy. End state:
DL=on (motif has the canonical), PL=gray (no placement),
LINK=PS (Plex still serves; the LPS state predicate matches).

## Scope

1. **New endpoint** `POST /api/items/{mt}/{tmdb_id}/download-backup`
   — sibling of `/redownload` but with `auto_place=False,
   force_place=False`. Worker downloads the canonical only;
   doesn't chain a place job. Same `_enqueue_download` helper.

2. **Recovery card option** for pure-P + TDB rows. Surfaces
   alongside the existing no-fail options. Action key
   `download-tdb-backup`, tone `tdb` (green), priority 1
   (after PUSH MOTIF'S THEME but before REVERT).

3. **SOURCE menu item** for pure-P + TDB rows. Same action key
   so the dispatcher branch handles both surfaces.

4. **JS dispatcher branches** for both surfaces — recovery card
   click handler + SOURCE menu top-level dispatcher.

## End state matches LPS exactly

After the download lands:
  • file_path set (motif has canonical)
  • media_folder NULL (no placement enqueued)
  • plex_independent_theme=1 (unchanged — Plex still serves)
  → LPS state predicate matches → LINK chip becomes PS
  → row drops out of NEEDS WORK
  → recovery card shows the standard LPS options (PUSH
    MOTIF'S THEME / REVERT TO USER URL / RE-DOWNLOAD FROM TDB)

So the post-state UX is FREE — already wired by v1.14.40 +
v1.14.43. The new feature just provides an alternative entry
path to the LPS state.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Server endpoint ─────────────────────────────────────────


def test_download_backup_endpoint_defined():
    """POST /api/items/{mt}/{tmdb}/download-backup must exist
    with the v1.14.45 marker comment explaining the
    auto_place=False intent."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert '@app.post("/api/items/{media_type}/{tmdb_id}/download-backup")' in src
    assert "async def api_download_backup(" in src
    # The marker explaining the design intent.
    assert "v1.14.45: DOWNLOAD TDB BACKUP" in src


def test_download_backup_endpoint_uses_no_auto_place():
    """The load-bearing pair: auto_place=False + force_place=
    False. Without these the worker would chain a place job and
    the row would NOT end up in LPS state — it'd just be a
    re-download with a placement. Defeats the entire purpose."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_download_backup(")
    body = src[fn_anchor:fn_anchor + 4000]
    assert "auto_place=False" in body
    assert "force_place=False" in body
    # Sanity: uses the same _enqueue_download helper as /redownload.
    assert "from ..core.sync import _enqueue_download" in body
    assert "_enqueue_download(" in body


def test_download_backup_endpoint_409s_for_plex_orphan():
    """plex_orphan rows have no TDB URL — the action is
    meaningless. 409 with a clear message."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_download_backup(")
    body = src[fn_anchor:fn_anchor + 4000]
    assert 'row["upstream_source"] == "plex_orphan"' in body
    assert "no TDB URL to back up" in body


def test_download_backup_endpoint_409s_for_no_youtube_url():
    """Sanity: row has upstream_source='imdb' but somehow no
    youtube_url (sync race / stale state) → 409 with clear msg."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_download_backup(")
    body = src[fn_anchor:fn_anchor + 4000]
    assert 'not row["youtube_url"]' in body
    assert "no TDB URL to back up" in body


# ── Recovery card option ────────────────────────────────────


def test_recovery_card_offers_download_tdb_backup_for_pure_p_plus_tdb():
    """The DOWNLOAD TDB BACKUP option must surface for pure-P
    rows with TDB coverage. Pin every gate condition so a
    future refactor that "loosens" the gate doesn't accidentally
    show the option for non-applicable rows.

    v1.14.47 reorg: the option moved from the api_recovery_options
    no-fail branch to the SOURCE-menu render in app.js (per
    the user's UX principle: TRY THIS NEXT is for error states
    only). The gate moved with it. Pin the SOURCE-menu render's
    JS-equivalent gate:
      isThemerrDb        ← non-orphan (TDB tracks the title)
      isPlexAgent        ← p_available (Plex serves its own)
      !downloaded        ← not local (no canonical yet)
      !tdbReplaceBlocked ← not in the dead-set / cookies-aware gate
      youtube_url        ← row[\"youtube_url\"] (TDB URL set)
    """
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The v1.14.45 marker on the SOURCE-menu render.
    assert "v1.14.45: DOWNLOAD TDB BACKUP" in js
    # The menuItemHtml call.
    anchor = js.index("'download-tdb-backup', 'DOWNLOAD TDB BACKUP'")
    # Walk back to the surrounding `if (` gate (~1500 chars to
    # cover the multiline gate + comment block).
    gate_start = js.rfind("if (", anchor - 2000, anchor)
    gate = js[gate_start:anchor]
    # All five gate predicates present.
    # v1.20.23: the canonical-check is `canSwapToTdbBackup`
    # (!downloaded || source_kind !== 'themerrdb') — symmetric with
    # DOWNLOAD PLEX BACKUP, supersedes v1.19.49's !hasNonCloudCanonical.
    assert "isThemerrDb" in gate
    assert "isPlexAgent" in gate
    assert "canSwapToTdbBackup" in gate
    assert "!tdbReplaceBlocked" in gate
    assert "it.youtube_url" in gate
    # Tone + label.
    item_block = js[anchor:anchor + 800]
    assert "tone: 'themerrdb'" in item_block


# ── SOURCE menu item ────────────────────────────────────────


def test_source_menu_emits_download_tdb_backup_button():
    """The SOURCE menu rendering must emit a `download-tdb-
    backup` button for pure-P + TDB rows. Pin the action key
    + label + the gate cluster (isThemerrDb + isPlexAgent +
    !downloaded + !tdbReplaceBlocked + !pending_update +
    !accepted_update + youtube_url)."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "v1.14.45: DOWNLOAD TDB BACKUP" in js
    # The menuItemHtml call.
    assert "'download-tdb-backup', 'DOWNLOAD TDB BACKUP'" in js
    # The gate (anchor on the SOURCE menu's emit site, not the
    # recovery dispatcher).
    src_block_anchor = js.index("'download-tdb-backup', 'DOWNLOAD TDB BACKUP'")
    gate_block = js[src_block_anchor - 1500:src_block_anchor]
    # v1.20.23: the canonical-check is now `canSwapToTdbBackup`
    # (= !downloaded || source_kind !== 'themerrdb'), symmetric with
    # DOWNLOAD PLEX BACKUP — any non-TDB backup (AB/UB/PB) or a pure-P
    # row can swap → TB. Supersedes v1.19.49's narrower
    # !hasNonCloudCanonical (which only allowed PB → TB).
    assert "isThemerrDb && isPlexAgent && canSwapToTdbBackup" in gate_block


# ── JS dispatchers ──────────────────────────────────────────


def test_recovery_card_dispatcher_handles_download_tdb_backup():
    """A click on a `download-tdb-backup` button must POST to the
    /download-backup endpoint.

    v1.14.47 reorg: the recovery-card dispatcher branch was
    removed (the option no longer surfaces in the no-fail TRY
    THIS NEXT). The SOURCE-menu dispatcher branch is now the
    single click path — it routes to the same /download-backup
    endpoint."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # The SOURCE-menu dispatcher branch is the surviving site.
    branch_anchor = js.index("} else if (act === 'download-tdb-backup') {")
    block = js[branch_anchor:branch_anchor + 2500]
    assert "/api/items/${mt}/${id}/download-backup" in block


def test_source_menu_dispatcher_handles_download_tdb_backup():
    """The SOURCE-menu top-level dispatcher (around line 8230)
    must ALSO have a branch — the recovery-card dispatcher
    only fires for clicks inside the recovery section, not the
    SOURCE menu."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Find the SOURCE-menu dispatcher (anchor on the redl
    # branch — the SOURCE-menu version, not the recovery one).
    redl_anchor = js.index(
        "        // v1.12.73: pass section_id from the menu button"
    )
    block = js[redl_anchor:redl_anchor + 3000]
    assert "} else if (act === 'download-tdb-backup') {" in block
    assert "v1.14.45: SOURCE-menu DOWNLOAD TDB BACKUP" in block


# ── Behavioral: enqueue produces a no-place download job ────


def test_download_backup_enqueues_with_auto_place_false(tmp_path):
    """End-to-end: extract the relevant SQL + helper call from
    the endpoint and run against a fixture. Verify the resulting
    jobs row has the right payload (`auto_place=false` in
    payload JSON)."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Seed: section + theme (with TDB URL) + plex_items entry.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (now, now),
    )
    # v1.15.142: seed an explicit themes.id so we can stamp
    # pi.theme_id for the linkage the v1.15.142 _enqueue_download
    # query relies on. Pre-v1.15.142 the test got away with theme_
    # id=NULL because the query matched purely on guid_tmdb.
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES (42, 'movie', 555, 'Pure-P TDB Row', 2020, 'imdb', "
        "        'https://youtube.com/watch?v=abc11111111', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, theme_id, "
        "  plex_independent_theme, first_seen_at, last_seen_at) "
        "VALUES ('555', '1', 'movie', '555', 'Pure-P TDB Row', "
        "        2020, 1, 0, '/data/movies/Pure-P', 42, 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    # Call _enqueue_download with the same args the endpoint uses.
    from app.core.sync import _enqueue_download
    n = _enqueue_download(
        conn, media_type="movie", tmdb_id=555,
        reason="manual_backup",
        auto_place=False,
        force_place=False,
        only_section_id="1",
    )
    conn.commit()
    assert n == 1, f"expected 1 enqueued job, got {n}"
    # Verify the job row has the right payload.
    job = conn.execute(
        "SELECT job_type, payload FROM jobs "
        "WHERE media_type = 'movie' AND tmdb_id = 555 "
        "  AND job_type = 'download'"
    ).fetchone()
    conn.close()
    assert job is not None
    assert job["job_type"] == "download"
    # The payload JSON should encode auto_place=False.
    import json
    payload = json.loads(job["payload"])
    assert payload.get("auto_place") is False, (
        f"Expected auto_place=False in payload; got {payload}. "
        "Without this gate the worker would chain a place job "
        "and the row wouldn't reach LPS state."
    )


def test_download_backup_lands_in_lps_state_predicate(tmp_path):
    """Sanity: simulate the post-download row state. After the
    backup download lands successfully, the row will have:
      file_path set
      media_folder NULL (no place job ran)
      plex_independent_theme=1 (unchanged from pre-action)
    → matches the v1.14.39 lpsState predicate (file_path +
    !media_folder + plex_independent_theme=1) → LINK chip
    becomes PS, row drops out of NEEDS WORK.

    This test pins the state convergence — the new feature
    inherits ALL the LPS-state UX wiring for free."""
    db_path = tmp_path / "motif.db"
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = now_iso()
    # Simulate the post-download state directly.
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', 666, 'Post-Backup Row', 2020, 'imdb', "
        "        'https://youtube.com/watch?v=def22222222', ?, ?)",
        (now, now),
    )
    # local_files = canonical landed (the backup download).
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance) "
        "VALUES ('movie', 666, '1', 'backup-666.mp3', 100, ?, "
        "        'def22222222', 'themerrdb', 'auto')",
        (now,),
    )
    # NO placement row (auto_place=False meant no place job ran).
    # plex_items: plex_independent_theme=1 (Plex serves).
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES ('666', '1', 'movie', '666', 'Post-Backup Row', "
        "        2020, 1, 0, '/data/movies/Post', 1, ?, ?)",
        (now, now),
    )
    conn.commit()
    # Run the v1.14.43 PS LINK predicate (same as lpsState):
    #   file_path set + media_folder NULL + plex_independent_theme=1
    sql = """
        SELECT t.tmdb_id
        FROM themes t
        JOIN plex_items pi
          ON pi.guid_tmdb = t.tmdb_id
         AND pi.media_type = (CASE t.media_type WHEN 'tv' THEN 'show' ELSE t.media_type END)
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        WHERE t.tmdb_id = 666
          AND lf.file_path IS NOT NULL
          AND p.media_folder IS NULL
          AND COALESCE(pi.plex_independent_theme, 0) = 1
    """
    matched = conn.execute(sql).fetchone()
    conn.close()
    assert matched is not None, (
        "Post-backup row didn't match the LPS / PS LINK predicate. "
        "The new DOWNLOAD TDB BACKUP feature is supposed to converge "
        "on the same end state as the LET PLEX SERVE flow — without "
        "this convergence the row wouldn't render the PS chip or "
        "drop out of NEEDS WORK."
    )
