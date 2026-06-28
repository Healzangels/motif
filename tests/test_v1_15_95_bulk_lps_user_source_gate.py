"""v1.15.95 — BULK LPS gate switches from TDB-alive to local_files-exists.

the user's repro:

> "Also I tried a let plex server on a group of U +P but nothing
>  happened these were rows with red pill tdb, in this case the
>  DL should be the user provided theme not the dead link
>  themerrdb"

Log output: `BULK LPS done: 4 target(s) — probed=0 (skipped-fresh=4),
alive=0, dead=0, unplaced=0, skipped-dead=4, unplace-failed=0`.

All 4 U+P rows had `failure_kind` set (TDB probe came back dead
within the 24h cooldown), so the pre-v1.15.95 unplace gate
(`WHERE failure_kind IS NULL`) rejected all 4 → nothing happened.

## Why the old gate was wrong

The gate intended "won't silently destroy the only recovery
path" — protecting against unplacing a T-source row where motif
couldn't re-download from a dead TDB URL.

But the analysis missed:
* For A/U-source rows, motif's canonical was downloaded from a
  NON-TDB source (adopt sidecar / user-provided URL). TDB health
  is irrelevant to whether motif has a working canonical.
* Even for T-source rows, the recovery path is PUSH TO PLEX
  (hardlink from themes_dir, no re-download needed). So a dead
  TDB URL doesn't prevent recovery as long as the canonical
  file exists in themes_dir.

The right invariant: motif must have a `local_files` row for
the section being unplaced. The file's source (themerrdb / url
/ upload / adopt) doesn't matter.

## Fix

`_bulk_lps_run`'s unplace stage replaces the pre-computed
`alive_keys` set (from `themes WHERE failure_kind IS NULL`)
with a per-row `SELECT 1 FROM local_files WHERE media_type=?
AND tmdb_id=? AND section_id=?` check. The counter
`n_dead_skipped` name is preserved for log-format
backwards-compat but its semantic is now "no motif canonical to
fall back to."

The probe stage is unchanged — it still updates
`themes.failure_kind` as a useful side effect, just doesn't
gate unplace anymore.

JS confirmation copy updated to reflect the new semantic:
probe + unplace are independent stages; a dead TDB URL no
longer blocks the unplace step.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def app_client(tmp_path, monkeypatch):
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
        "VALUES (?, 'Movies', 'movie', 1, 0, 0, ?, ?, ?)",
        (section_id, f"movies-{section_id}", now, now),
    )


def _seed_failed_tdb_row(conn, *, tmdb_id, section_id, with_local_files=True):
    """Theme row with failure_kind set (dead TDB) + a placements
    row + optionally a local_files row representing motif's
    canonical from a non-TDB source (user URL)."""
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "  upstream_source, youtube_url, "
        "  failure_kind, failure_at, last_probed_at, "
        "  first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', ?, 'TestMovie', 2020, 'imdb', "
        "  'https://yt.com/w?v=dead11111111', "
        "  'video_removed', ?, ?, ?, ?)",
        (tmdb_id, now, now, now, now),
    )
    if with_local_files:
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   file_size, downloaded_at, source_video_id, "
            "   source_kind, provenance) "
            "VALUES ('movie', ?, ?, ?, 100, ?, 'user11111111', "
            "        'url', 'manual')",
            (tmdb_id, section_id,
             f"movies/Test ({tmdb_id})/theme.mp3", now),
        )
    # Placement so LPS has something to unplace
    conn.execute(
        "INSERT INTO placements "
        "  (media_type, tmdb_id, section_id, media_folder, "
        "   placed_at, placement_kind, provenance) "
        "VALUES ('movie', ?, ?, ?, ?, 'hardlink', 'manual')",
        (tmdb_id, section_id, f"/data/media/movies/Test ({tmdb_id})", now),
    )


# ── #1 — gate switched to local_files existence ─────────────


def test_bulk_lps_unplaces_u_source_row_with_dead_tdb(app_client, tmp_path):
    """The the user-screenshot scenario. A U-source row with
    failure_kind set (dead TDB) but a local_files row from the
    user's URL. Pre-v1.15.95 the gate would reject (skipped-dead).
    Post-fix it should unplace + delete the placement."""
    client, db = app_client
    # Create the container folder so unlink doesn't fail on
    # missing path (matches v1.15.90's test pattern).
    folder = tmp_path / "data" / "media" / "movies" / "Test (100)"
    folder.mkdir(parents=True)
    (folder / "theme.mp3").write_bytes(b"\x00" * 100)
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        _seed_failed_tdb_row(conn, tmdb_id=100, section_id="1",
                             with_local_files=True)
        # Override placement.media_folder to point at the tmp
        # path the test created (override the default).
        conn.execute(
            "UPDATE placements SET media_folder = ? "
            "WHERE tmdb_id = 100",
            (str(folder),),
        )
        conn.commit()

    r = client.post(
        "/api/admin/bulk-let-plex-serve",
        json={"items": [{"media_type": "movie", "tmdb_id": 100,
                         "section_id": "1"}]},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    # The bulk runs in a thread — wait for it to complete by
    # polling the op_progress status. Most ops finish in <1s
    # in test conditions.
    import time
    for _ in range(50):
        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT status FROM op_progress "
                "WHERE op_id = 'bulk-lps' ORDER BY started_at DESC "
                "LIMIT 1"
            ).fetchone()
        if row and row[0] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.1)
    else:
        pytest.fail("BULK LPS didn't finish within 5s")

    # Post-condition: placements row gone (unplaced succeeded
    # despite the dead TDB).
    with sqlite3.connect(db) as conn:
        placements_count = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id = 100"
        ).fetchone()[0]
    assert placements_count == 0, (
        "v1.15.95: BULK LPS should unplace the U+P row even with "
        "a failed-TDB row (motif's canonical came from a user "
        f"URL, not TDB). Found {placements_count} surviving "
        "placements row(s)."
    )


def test_bulk_lps_skips_row_without_motif_canonical(app_client, tmp_path):
    """Counter-guard: rows where motif has NO local_files (e.g.,
    pure-P-cloud rows where motif never owned anything) should
    still be skipped — there's no canonical to fall back on."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, "1")
        # Failed TDB row WITHOUT local_files (motif owns nothing).
        _seed_failed_tdb_row(conn, tmdb_id=200, section_id="1",
                             with_local_files=False)
        conn.commit()

    r = client.post(
        "/api/admin/bulk-let-plex-serve",
        json={"items": [{"media_type": "movie", "tmdb_id": 200,
                         "section_id": "1"}]},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    # Wait for completion
    import time
    for _ in range(50):
        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT status FROM op_progress "
                "WHERE op_id = 'bulk-lps' ORDER BY started_at DESC "
                "LIMIT 1"
            ).fetchone()
        if row and row[0] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.1)
    else:
        pytest.fail("BULK LPS didn't finish within 5s")

    # Placements should still exist — gate rejected the row.
    with sqlite3.connect(db) as conn:
        placements_count = conn.execute(
            "SELECT COUNT(*) FROM placements WHERE tmdb_id = 200"
        ).fetchone()[0]
    assert placements_count == 1, (
        "v1.15.95: rows without a motif local_files canonical must "
        "still be skipped — no recovery path. Got "
        f"placements_count={placements_count}, expected 1."
    )


# ── Static guards on the code shape ─────────────────────────


def test_bulk_lps_no_longer_gates_on_failure_kind():
    """Counter-guard: the pre-v1.15.95 gate query
    `WHERE failure_kind IS NULL` must not survive as live SQL in
    `_bulk_lps_run`. A refactor that re-introduces it brings the
    the user-repro back. The string may still appear in narrative
    docstrings / comments referencing the old behavior; the test
    looks specifically for the SQL-shape with a WHERE prefix."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    # Strip both `#` line comments AND triple-quoted docstrings
    # before checking SQL — narrative prose mentioning the old
    # gate is allowed; only live SQL referencing it is forbidden.
    code = re.sub(r"#[^\n]*\n", "\n", fn_body)
    code = re.sub(r'"""[\s\S]*?"""', "", code)
    # The forbidden pattern is a `WHERE failure_kind IS NULL`
    # SQL clause (the actual gate that produced the bug).
    bad = re.search(
        r"WHERE[\s\S]{0,200}?failure_kind\s+IS\s+NULL",
        code,
    )
    assert bad is None, (
        "v1.15.95 regression: a `WHERE ... failure_kind IS NULL` "
        "SQL clause returned to _bulk_lps_run. The unplace gate "
        "must use local_files existence, not TDB URL alive-status."
    )


def test_bulk_lps_unplace_gate_uses_local_files():
    """The new gate must check local_files. Pin that the
    function body references the table in a SELECT."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("def _bulk_lps_run(")
    fn_end = src.index("\ndef ", fn_start + 1)
    fn_body = src[fn_start:fn_end]
    code = re.sub(r"#[^\n]*\n", "\n", fn_body)
    assert "FROM local_files" in code, (
        "v1.15.95: _bulk_lps_run must reference `FROM local_files` "
        "for the canonical-existence gate. If the SQL was "
        "refactored elsewhere, update this anchor."
    )


def test_bulk_lps_js_confirm_text_no_longer_promises_dead_skip():
    """The JS confirmation dialog's text used to promise 'If the
    probe shows the URL is dead: row is skipped' — that's no
    longer accurate post-v1.15.95. Pin the updated copy."""
    src = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Find the LPS confirm() call's text.
    anchor = src.index("LET PLEX SERVE on ${targets.length}")
    window = src[anchor:anchor + 2000]
    # New copy says "probe TDB URL" + the unplace gate is "motif
    # canonical exists" — pin that the misleading "probe shows
    # dead = skipped" sentence is gone.
    assert "probe shows the URL is dead: row is skipped" not in window, (
        "v1.15.95 regression: the JS confirm copy still promises "
        "'probe-dead = skipped' which is no longer how the unplace "
        "gate works. Update the dialog text to match the v1.15.95 "
        "semantic (gate is canonical-existence)."
    )
