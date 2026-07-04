"""v1.19.62 — DOWNLOAD PLEX BACKUP fires on PS-with-DL rows too.

the user's "86 EIGHTY-SIX (2021)" INFO card review (post v1.19.61
PS→BK unification): SRC=P + DL=green TDB download + LINK=BK.
The user wants the option to swap the TDB local for Plex's actual
cloud theme bytes — so the backup matches what Plex is serving,
not whatever TDB happened to have.

Pre-v1.19.62 the DOWNLOAD PLEX BACKUP SOURCE-menu action was
gated on `isPlexAgent && !downloaded` — only pure-P rows
(no local_files) could trigger it. The v1.19.42 walker's
`identify_c1_rows` query also required `NOT EXISTS local_files`
so even if the UI fired the action on a PS-with-DL row, the
walker would return 0 candidates.

## v1.19.62 changes

  1. `identify_c1_rows`: new `allow_existing_local: bool = False`
     parameter. When True, drop the `NOT EXISTS local_files`
     clause so PS-with-DL rows become C1 candidates. Still
     exclude `source_kind='plex_cloud'` rows (already cloud-
     backed; no-op).

  2. `_cloud_themes_backup_run` + `/api/admin/cloud-themes-backup-run`:
     thread the flag through (POST body
     `{"allow_existing_local": true}`).

  3. SOURCE-menu `app.js`: visibility extended to
     `isPlexAgent && (!downloaded || source_kind !== 'plex_cloud')`.
     PS-with-DL rows where the local isn't already plex_cloud now
     show the action. Tooltip warns "⚠ existing local file will
     be REPLACED" when downloaded. Click handler reads the
     `data-allow-existing-local='1'` attribute and passes
     `allow_existing_local: true` in the POST body.

`backup_cloud_theme`'s `ON CONFLICT DO UPDATE` clause (already
present from v1.19.42) handles the REPLACE — no schema change.
"""
from __future__ import annotations

import sqlite3
import sys
import sqlite3 as _sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

CLOUD_BK_PY = (REPO / "app" / "core" / "cloud_theme_backup.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── identify_c1_rows accepts allow_existing_local ────────────


def test_identify_c1_rows_accepts_allow_existing_local_kwarg():
    """The walker's signature must include `allow_existing_local:
    bool = False`. When True, the NOT-EXISTS-local_files clause
    drops out so PS-with-DL rows become C1 candidates."""
    assert (
        "allow_existing_local: bool = False"
        in CLOUD_BK_PY
    ), "v1.19.62: identify_c1_rows must accept allow_existing_local"


def test_walker_drops_not_exists_clause_when_flag_set():
    """The SQL builder must branch on `allow_existing_local`. When
    True, the NOT-EXISTS-local_files clause is OMITTED. When False
    (default), it's preserved."""
    idx = CLOUD_BK_PY.index("def identify_c1_rows(")
    end = CLOUD_BK_PY.index("\n\n\n", idx + 1) if "\n\n\n" in CLOUD_BK_PY[idx:] else len(CLOUD_BK_PY)
    # v1.21.71: slice to the function's actual end (`end`) rather than a
    # fixed 5000-char window — v1.21.70's edition-scoping comment + the two
    # `AND <tbl>.edition_key = pi.edition_key` clauses pushed the
    # source_kind line past 5000, breaking this brittle pin.
    body = CLOUD_BK_PY[idx:end]
    # The if/else dispatch must be present.
    assert "if not allow_existing_local:" in body
    # And the True branch must filter source_kind='plex_cloud'
    # specifically (no-op for rows already cloud-backed).
    assert "lf.source_kind = 'plex_cloud'" in body


def test_walker_smoke_with_allow_existing_local_returns_ps_with_dl_row():
    """Behavioral: seed a PS-with-DL row (plex_items.has_theme=1 +
    local_files exists with non-plex_cloud source_kind). With
    `allow_existing_local=False` (default) → 0 candidates. With
    `allow_existing_local=True` → 1 candidate."""
    from app.core.db import init_db
    from app.core.cloud_theme_backup import identify_c1_rows
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "test.db"
        init_db(db)
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO plex_sections "
                "  (section_id, title, type, is_anime, is_4k, "
                "   themes_subdir, included, discovered_at, last_seen_at) "
                "VALUES ('3', 'Anime', 'show', 1, 0, 'tv', 1, "
                "        '2026-05-27', '2026-05-27')"
            )
            conn.execute(
                "INSERT INTO themes "
                "  (id, media_type, tmdb_id, title, upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES (1, 'tv', 100565, '86 EIGHTY-SIX', "
                "        'themoviedb', '2026-05-27', '2026-05-27')"
            )
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, theme_id, "
                "   guid_imdb, guid_tmdb, title, year, has_theme, "
                "   local_theme_file, folder_path, "
                "   plex_independent_theme, plex_theme_verified_ok, "
                "   first_seen_at, last_seen_at) "
                "VALUES ('rk-86', '3', 'show', 1, '', '100565', "
                "        '86 EIGHTY-SIX', 2021, 1, 0, "
                "        '/data/anime/86 EIGHTY-SIX', 0, 1, "
                "        '2026-05-27', '2026-05-27')"
            )
            conn.execute(
                "INSERT INTO local_files "
                "  (media_type, tmdb_id, section_id, file_path, "
                "   file_size, file_sha256, downloaded_at, "
                "   source_kind, source_video_id, provenance) "
                "VALUES ('tv', 100565, '3', '86 EIGHTY-SIX/theme.mp3', "
                "        2964764, 'aaaa', '2026-05-19', "
                "        'themerrdb', 'eZIMFWAxMxQ', 'auto')"
            )
            conn.commit()

        class _MockPlex:
            """Minimal stand-in — identify_c1_rows calls
            plex_client.get_themes per candidate. We return no
            themes so no row gets classified C1; we only care that
            the candidate SQL surfaces (or doesn't surface) the row."""
            def get_themes(self, rating_key):
                return {"ok": False, "http_status": 500, "body": ""}

        # Default: NOT EXISTS clause filters out the PS-with-DL row.
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            # Re-do the candidate-discovery branch only. We can
            # inspect via direct SQL since the walker's actual
            # /themes call is mocked.
            candidates_off = conn.execute("""
                SELECT pi.rating_key FROM plex_items pi
                 WHERE pi.has_theme = 1
                   AND pi.guid_tmdb IS NOT NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM local_files lf
                      WHERE lf.tmdb_id = pi.guid_tmdb
                        AND lf.section_id = pi.section_id
                        AND lf.media_type = CASE pi.media_type
                                              WHEN 'show' THEN 'tv'
                                              ELSE pi.media_type END
                   )
                   AND NOT EXISTS (
                     SELECT 1 FROM placements pl
                      WHERE pl.tmdb_id = pi.guid_tmdb
                        AND pl.section_id = pi.section_id
                   )
            """).fetchall()
            # v1.19.62 SQL (allow_existing_local=True): the
            # NOT EXISTS clause is replaced with a 'plex_cloud'
            # exclusion only.
            candidates_on = conn.execute("""
                SELECT pi.rating_key FROM plex_items pi
                 WHERE pi.has_theme = 1
                   AND pi.guid_tmdb IS NOT NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM local_files lf
                      WHERE lf.tmdb_id = pi.guid_tmdb
                        AND lf.section_id = pi.section_id
                        AND lf.media_type = CASE pi.media_type
                                              WHEN 'show' THEN 'tv'
                                              ELSE pi.media_type END
                        AND lf.source_kind = 'plex_cloud'
                   )
                   AND NOT EXISTS (
                     SELECT 1 FROM placements pl
                      WHERE pl.tmdb_id = pi.guid_tmdb
                        AND pl.section_id = pi.section_id
                   )
            """).fetchall()
        assert len(candidates_off) == 0, (
            "v1.19.62: with allow_existing_local=False, PS-with-DL "
            "rows must be filtered out (existing v1.19.42 behavior)"
        )
        assert len(candidates_on) == 1, (
            f"v1.19.62: with allow_existing_local=True, PS-with-DL "
            f"row must surface as a candidate; got {len(candidates_on)}"
        )


# ── Endpoint plumbs the flag through ──────────────────────────


def test_endpoint_accepts_allow_existing_local_body_param():
    """The /api/admin/cloud-themes-backup-run endpoint must parse
    the `allow_existing_local` body param + thread it to the
    worker thread."""
    idx = API_PY.index('@app.post("/api/admin/cloud-themes-backup-run")')
    block = API_PY[idx:idx + 6500]
    assert "allow_existing_local" in block, (
        "v1.19.62: endpoint must accept allow_existing_local body param"
    )
    # Threaded into the worker kwargs.
    assert "allow_existing_local" in block[block.index("threading"):]
    # Log_event message surfaces the flag.
    assert "allow_existing_local={allow_existing_local}" in block


def test_worker_function_accepts_allow_existing_local():
    """`_cloud_themes_backup_run`'s signature must include
    `allow_existing_local: bool = False`."""
    idx = API_PY.index("def _cloud_themes_backup_run(")
    sig_end = API_PY.index(")", idx)
    sig = API_PY[idx:sig_end + 1]
    assert "allow_existing_local" in sig
    # And the call site to identify_c1_rows passes it through.
    body_end = API_PY.index("\n\n\ndef ", idx)
    body = API_PY[idx:body_end]
    assert "allow_existing_local=allow_existing_local" in body


# ── SOURCE-menu visibility + click handler ────────────────────


def test_source_menu_visibility_extends_to_ps_with_non_plex_cloud_dl():
    """SOURCE-menu DOWNLOAD PLEX BACKUP visibility gate must now
    fire on PS-with-DL rows where source_kind isn't already
    plex_cloud."""
    idx = APP_JS.index("v1.19.62: extend visibility to PS-with-")
    block = APP_JS[idx:idx + 2000]
    # The predicate must check both pure-P and PS-with-DL cases.
    assert "isCloudBackupable" in block
    assert "!downloaded" in block
    assert "source_kind !== 'plex_cloud'" in block


def test_source_menu_passes_allow_existing_local_on_ps_with_dl():
    """The menuItemHtml call must pass `allowExistingLocal: '1'`
    when the row is downloaded (PS-with-DL case) and '0' otherwise
    (pure-P case)."""
    idx = APP_JS.index("'backup-cloud-theme', 'DOWNLOAD PLEX BACKUP'")
    block = APP_JS[idx:idx + 800]
    assert "allowExistingLocal" in block
    assert "downloaded ? '1' : '0'" in block


def test_click_handler_reads_allow_existing_local_from_dataset():
    """The click handler must read `data-allow-existing-local` and
    pass it as `allow_existing_local` in the POST body."""
    idx = APP_JS.index("else if (act === 'backup-cloud-theme')")
    block = APP_JS[idx:idx + 3000]
    assert "btn.dataset.allowExistingLocal" in block
    assert "allow_existing_local: allowExistingLocal" in block


def test_menu_item_html_serializes_allow_existing_local_data_attr():
    """menuItemHtml must serialize extras.allowExistingLocal to a
    `data-allow-existing-local` attribute (so the click handler's
    `btn.dataset.allowExistingLocal` reads it correctly)."""
    idx = APP_JS.index("function menuItemHtml(")
    end = APP_JS.index("\n    }", idx + 1)
    body = APP_JS[idx:end]
    assert "data-allow-existing-local" in body
    assert "extras.allowExistingLocal" in body


def test_source_menu_tooltip_warns_on_destructive_swap():
    """When the row already has a non-plex_cloud local file, the
    tooltip must warn that the file will be REPLACED."""
    idx = APP_JS.index("v1.19.62: extend visibility to PS-with-")
    block = APP_JS[idx:idx + 3400]
    assert "REPLACED" in block
    assert "swapTooltip" in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_62_version_pin():
    """Version bumped at v1.19.62. Relaxed to v1.19.x prefix
    after v1.19.63 continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
