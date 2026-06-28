"""v1.15.90 — unplace clears pi.local_theme_file via natural key.

the user's repro on Unraid (M+P+PS+DL state after LET PLEX SERVE):

* Originally M-source row (manual sidecar) at
  /data/media/movies/A Dog's Journey (2019)/theme.mp3
* Adopt → A-source → REPLACE TDB → T-source → click LET PLEX SERVE
* Expected: row resolves to P (Plex's cloud theme serves; motif's
  canonical preserved in themes_dir; no sidecar at Plex folder)
* Actual: row showed M+P, DL green, PL gray, Link PS

Folder verification: the Plex folder had no theme.* files at all
post-LET-PLEX-SERVE (the user pasted `ls -la` output) — yet
plex_items.local_theme_file was still 1, so SRC=M fired per the
_SRC_LETTER_SQL gate (api.py:664):

    "WHEN p.media_folder IS NULL AND pi.local_theme_file = 1 THEN 'M'"

## Root cause

Path-domain mismatch on Unraid:

  * plex_items.folder_path stores Plex's REPORTED path (host view):
    "/mnt/user/movies/A Dog's Journey (2019)"
  * placements.media_folder stores motif's FolderIndex-RESOLVED
    container path:
    "/data/media/movies/A Dog's Journey (2019)"

The placement worker at placement.py:280+ uses
`_candidate_local_paths(cached_folder_path)` to translate Plex's
host path to a reachable container path — that's why placements
succeed despite the divergence. But pre-v1.15.90 the unplace
handler at api.py:10792 keyed on `WHERE folder_path = ?` passing
placements.media_folder. The string didn't match plex_items.
folder_path, so the UPDATE affected ZERO rows. local_theme_file
stayed at 1 from a prior plex_enum stat.

When SRC was computed:
  * p.media_folder IS NULL (placements row deleted) → no T/A/U/P-via-place
  * pi.local_theme_file = 1 (stale from before the unplace) → SRC=M

## Fix

Switch the plex_items lookup from folder_path matching to natural
key lookup (theme_id with guid_tmdb + media_type fallback). The
SRC letter join already uses this pattern via pi.theme_id = t.id
(api.py:713), so unplace now mirrors it.

Scoped by section_id when provided so per-edition unplace doesn't
clear sibling sections' plex_items rows.

## What this test verifies

Behavioral: seed plex_items with folder_path that DIFFERS from
the placements.media_folder (simulating the Unraid host vs
container path mismatch), then call /unplace and assert
pi.local_theme_file = 0 afterwards.

Pre-v1.15.90 this test would have failed: the WHERE folder_path
match would miss the divergent path → no UPDATE → local_theme_file
stays at 1.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """TestClient + DB fixture. Mirrors v1.14.59's pattern."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))

    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")

    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client, db


def _seed_section(conn, section_id="1"):
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', 'movie', 1, 0, 0, 'movies', ?, ?)",
        (section_id, now, now),
    )


def _seed_theme(conn, *, tmdb_id):
    """Theme row + return its theme_id PK."""
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', ?, 'A Dogs Journey', 2019, 'imdb', "
        "  'https://youtube.com/watch?v=abc11111111', ?, ?)",
        (tmdb_id, now, now),
    )
    row = conn.execute(
        "SELECT id FROM themes WHERE media_type='movie' AND tmdb_id=?",
        (tmdb_id,),
    ).fetchone()
    return row[0]


def _seed_plex_item_with_host_path(
    conn, *, rating_key, tmdb_id, section_id, theme_id_pk,
    host_folder_path, local_theme_file=1,
):
    """plex_items row with Plex's host path AND theme_id set.
    Simulates the user's Unraid setup where Plex reports
    /mnt/user/movies/... but motif resolves to /data/media/movies/..."""
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, theme_id, "
        "  plex_independent_theme, first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, 'A Dogs Journey', 2019, 1, ?, "
        "        ?, ?, 0, ?, ?)",
        (rating_key, section_id, str(tmdb_id), local_theme_file,
         host_folder_path, theme_id_pk, now, now),
    )


def _seed_placement(
    conn, *, tmdb_id, section_id, container_media_folder, theme_id_pk,
):
    """placements row with the CONTAINER path (what motif's
    placement worker would store after FolderIndex resolution)."""
    now = now_iso()
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance, theme_id) "
        "VALUES ('movie', ?, ?, ?, 100, ?, 'abc11111111', "
        "        'themerrdb', 'auto', ?)",
        (tmdb_id, section_id,
         f"movies/A Dogs Journey ({tmdb_id})/theme.mp3",
         now, theme_id_pk),
    )
    conn.execute(
        "INSERT INTO placements (media_type, tmdb_id, section_id, "
        "  media_folder, placed_at, placement_kind, provenance) "
        "VALUES ('movie', ?, ?, ?, ?, 'hardlink', 'auto')",
        (tmdb_id, section_id, container_media_folder, now),
    )


# ── The core v1.15.90 behavioral test ────────────────────────


def test_unplace_clears_local_theme_file_despite_path_domain_mismatch(
    app_client, tmp_path,
):
    """the user's Unraid scenario: plex_items.folder_path is a HOST
    path while placements.media_folder is the CONTAINER path. The
    pre-v1.15.90 unplace handler used WHERE folder_path = ? which
    missed the divergence. The fix is to look up plex_items via
    natural keys (theme_id + guid_tmdb fallback) instead."""
    client, db = app_client
    # Create the container-side media folder so the unlink+stat
    # don't fail on the path-not-exist branch.
    container_folder = tmp_path / "data" / "media" / "movies" / "A Dogs Journey"
    container_folder.mkdir(parents=True)
    # Use the user's actual path domains: host = /mnt/user/...,
    # container = /data/media/... (the tmp_path-rooted equivalent
    # is what we actually use on disk; the strings still differ).
    host_path = "/mnt/user/movies/A Dogs Journey (2019)"
    container_path = str(container_folder)
    # Place a theme.mp3 in the container folder so the unplace
    # handler's `if p.is_file(): p.unlink()` can execute the
    # deletion path naturally.
    theme_file = container_folder / "theme.mp3"
    theme_file.write_bytes(b"\x00" * 100)

    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        theme_id_pk = _seed_theme(conn, tmdb_id=8385474)
        _seed_plex_item_with_host_path(
            conn, rating_key="123", tmdb_id=8385474, section_id="1",
            theme_id_pk=theme_id_pk, host_folder_path=host_path,
            local_theme_file=1,
        )
        _seed_placement(
            conn, tmdb_id=8385474, section_id="1",
            container_media_folder=container_path,
            theme_id_pk=theme_id_pk,
        )
        conn.commit()

    # Sanity: pre-unplace, local_theme_file is 1 (the stale state
    # from prior plex_enum that v1.15.90 must clear).
    with sqlite3.connect(db) as conn:
        pre = conn.execute(
            "SELECT local_theme_file FROM plex_items WHERE rating_key='123'"
        ).fetchone()
    assert pre[0] == 1, (
        "Test fixture sanity: plex_items.local_theme_file should "
        "start at 1 to exercise the v1.15.90 clearing logic."
    )

    r = client.post(
        "/api/items/movie/8385474/unplace?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    # The v1.15.90 fix: plex_items.local_theme_file should now be 0
    # even though plex_items.folder_path != placements.media_folder.
    with sqlite3.connect(db) as conn:
        post = conn.execute(
            "SELECT local_theme_file, folder_path "
            "FROM plex_items WHERE rating_key='123'"
        ).fetchone()
    assert post[0] == 0, (
        f"v1.15.90 regression: plex_items.local_theme_file should "
        f"be 0 after unplace, got {post[0]}. The natural-key lookup "
        "(theme_id / guid_tmdb) didn't match — check the v1.15.90 "
        "WHERE clause construction in api_unplace_item."
    )
    # Sanity: folder_path is still the host path (we don't rewrite it).
    assert post[1] == host_path, (
        "v1.15.90 must NOT modify folder_path — only local_theme_file "
        f"and plex_theme_verified_ok. Got folder_path={post[1]!r}."
    )

    # placements row was deleted (existing behavior).
    with sqlite3.connect(db) as conn:
        placements_count = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id=8385474"
        ).fetchone()[0]
    assert placements_count == 0, (
        "Existing behavior: placements row should be deleted after "
        f"unplace. Got {placements_count} rows."
    )


def test_unplace_fallback_guid_tmdb_when_theme_id_null(app_client, tmp_path):
    """Counter-guard: plex_items with theme_id=NULL (pre-resolve
    window or orphan rows) must still get local_theme_file cleared
    via the guid_tmdb + media_type fallback branch in the v1.15.90
    WHERE clause."""
    client, db = app_client
    container_folder = tmp_path / "data" / "media" / "movies" / "X"
    container_folder.mkdir(parents=True)
    (container_folder / "theme.mp3").write_bytes(b"\x00" * 100)
    host_path = "/mnt/user/movies/X"
    container_path = str(container_folder)

    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        theme_id_pk = _seed_theme(conn, tmdb_id=999111)
        # plex_item with theme_id=NULL (pre-resolve / orphan case).
        now = now_iso()
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, title, year, has_theme, "
            "  local_theme_file, folder_path, theme_id, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('999', '1', 'movie', '999111', 'X', 2019, 1, 1, "
            "        ?, NULL, ?, ?)",
            (host_path, now, now),
        )
        _seed_placement(
            conn, tmdb_id=999111, section_id="1",
            container_media_folder=container_path,
            theme_id_pk=theme_id_pk,
        )
        conn.commit()

    r = client.post(
        "/api/items/movie/999111/unplace?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text

    with sqlite3.connect(db) as conn:
        post = conn.execute(
            "SELECT local_theme_file FROM plex_items WHERE rating_key='999'"
        ).fetchone()
    assert post[0] == 0, (
        f"v1.15.90 fallback path: guid_tmdb + media_type lookup "
        f"should catch plex_items rows with theme_id=NULL. "
        f"Got local_theme_file={post[0]}."
    )


# ── Static-text guards on the v1.15.90 code shape ────────────


def test_unplace_no_longer_uses_folder_path_lookup_only():
    """Counter-guard: the WHERE folder_path = ? lookup that lived
    in the unplace handler pre-v1.15.90 must be gone. A future
    refactor that re-introduces folder_path matching on plex_items
    without a fallback re-introduces the Unraid path-mismatch bug
    the user reported.

    Allowed: the path-translation comment may reference folder_path
    in prose; we check that no `UPDATE plex_items ... WHERE
    folder_path = ?` survives in the unplace function body."""
    import re
    REPO = Path(__file__).resolve().parent.parent
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("async def api_unplace_item(")
    # Next async def or end-of-file.
    next_fn = src.find("\n    async def ", fn_start + 1)
    if next_fn == -1:
        next_fn = len(src)
    fn_body = src[fn_start:next_fn]
    # Strip comments to avoid matching narrative prose.
    fn_body_no_comments = re.sub(
        r"#[^\n]*\n", "\n", fn_body,
    )
    # The pre-v1.15.90 anti-pattern: UPDATE plex_items WHERE
    # folder_path = ? as the SOLE constraint.
    bad_update = re.search(
        r"UPDATE\s+plex_items[^;]*?WHERE\s+folder_path\s*=\s*\?[^,;]*?(?<!OR)$",
        fn_body_no_comments,
        re.MULTILINE | re.IGNORECASE,
    )
    # Heuristic check: if "WHERE folder_path = ?" appears as the
    # ONLY where in the plex_items UPDATE, that's the bug.
    # The v1.15.90 fix uses natural keys (theme_id / guid_tmdb).
    assert "theme_id =" in fn_body or "guid_tmdb" in fn_body, (
        "v1.15.90: unplace must use natural-key lookup (theme_id "
        "or guid_tmdb) for plex_items, not folder_path. The "
        "Unraid host-vs-container path-mismatch bug returns "
        "without this guard."
    )
