"""v1.24.93 — finish the HAMA → TVDB rename (the user audit).

v1.16.2 renamed only the USER-FACING label to "TVDB BRIDGE"; the "HAMA bridge"
name was a misnomer (99.7% of stranded rows are TheTVDB-scraper TV, not the HAMA
anime agent — PROJECT_HISTORY §L) but the internal ids were kept "for stability".
v1.24.93 renames them everywhere they're active: op kind hama_bridge →
tvdb_bridge (schema v68 migration), op_id, routes, functions, the JS maps, and
comments. The only "hama" left is intentional archaeology: historical db.py
migrations + the v68 rename source, the changelog, and ABSENCE-asserting tests.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

ACTIVE_SOURCES = [
    REPO / "app" / "web" / "api.py",
    REPO / "app" / "core" / "plex_enum.py",
    REPO / "app" / "web" / "static" / "ops.js",
    REPO / "app" / "web" / "static" / "app.js",
]
HAMA_TOKENS = ("hama_bridge", "hama-bridge", "hama_gap", "hama-gap")


def test_no_hama_identifier_tokens_in_active_sources():
    """The renamed identifiers must not survive in the all-active source
    files (db.py is exempt — it holds immutable historical migrations + the
    v68 rename source that legitimately name the old value)."""
    for src in ACTIVE_SOURCES:
        text = src.read_text()
        for tok in HAMA_TOKENS:
            assert tok not in text, f"{src.name} still contains '{tok}'"
        assert "HAMA" not in text, f"{src.name} still contains 'HAMA' prose"


def test_routes_and_kind_are_tvdb():
    api = (REPO / "app" / "web" / "api.py").read_text()
    appjs = (REPO / "app" / "web" / "static" / "app.js").read_text()
    opsjs = (REPO / "app" / "web" / "static" / "ops.js").read_text()
    assert '/api/admin/tvdb-bridge/rebuild' in api and '/api/admin/tvdb-bridge/rebuild' in appjs
    assert '/api/admin/diagnostics/tvdb-gap' in api
    assert 'kind="tvdb_bridge"' in api
    assert "OP_ID = \"tvdb-bridge\"" in api
    assert "tvdb_bridge:" in opsjs  # KIND_LABEL/TONE/PRIORITY map keys


def test_schema_version_and_check_renamed():
    db = (REPO / "app" / "core" / "db.py").read_text()
    # v0.51.128: schema bumped to v70 (plex_items.consecutive_missing). The
    # v68 hama→tvdb rename migration + its renamed CHECK still stand.
    assert "CURRENT_SCHEMA_VERSION = 77" in db
    assert "def _migrate_v67_to_v68" in db
    # the CURRENT (live) op_progress CHECK clause lists tvdb_bridge, not
    # hama_bridge (the surrounding comment keeps a rename breadcrumb that
    # legitimately names the old value, so check the CHECK clause itself).
    schema_start = db.index("CREATE TABLE IF NOT EXISTS op_progress (")
    check_start = db.index("CHECK (kind IN (", schema_start)
    check_clause = db[check_start:check_start + 300]
    assert "'tvdb_bridge'" in check_clause
    assert "'hama_bridge'" not in check_clause


def test_migration_renames_kind_opid_and_runtime_key():
    """Behavioral: a v67 op_progress row keyed 'hama_bridge'/'hama-bridge' +
    a 'last_hama_bridge_at' runtime setting migrate to the tvdb names, and the
    rebuilt CHECK rejects the old kind."""
    from app.core import db as dbmod
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE op_progress (
              op_id TEXT PRIMARY KEY,
              kind TEXT NOT NULL CHECK (kind IN ('tdb_sync','plex_enum',
                'reprobe_plex_themes','bulk_probe_tdb','bulk_lps',
                'hama_bridge','cloud_themes_backup')),
              status TEXT NOT NULL DEFAULT 'running' CHECK (status IN
                ('pending','running','cancelling','done','failed','cancelled')),
              started_at TEXT NOT NULL, updated_at TEXT NOT NULL, finished_at TEXT,
              stage TEXT, stage_label TEXT, stage_current INTEGER NOT NULL DEFAULT 0,
              stage_total INTEGER NOT NULL DEFAULT 0,
              processed_total INTEGER NOT NULL DEFAULT 0,
              processed_est INTEGER NOT NULL DEFAULT 0,
              error_count INTEGER NOT NULL DEFAULT 0, detail_json TEXT
            );
            CREATE TABLE runtime_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL,
              updated_at TEXT NOT NULL, updated_by TEXT);
            INSERT INTO op_progress (op_id, kind, status, started_at, updated_at)
              VALUES ('hama-bridge','hama_bridge','done','t','t');
            INSERT INTO runtime_settings (key, value, updated_at)
              VALUES ('last_hama_bridge_at','2026-06-01','t');
        """)
        conn.commit()
        dbmod._migrate_v67_to_v68(conn)
        assert conn.execute("SELECT op_id, kind FROM op_progress").fetchone() \
            == ('tvdb-bridge', 'tvdb_bridge')
        assert conn.execute("SELECT key FROM runtime_settings").fetchone()[0] \
            == 'last_tvdb_bridge_at'
        # new CHECK rejects the old kind, accepts the new one.
        try:
            conn.execute("INSERT INTO op_progress (op_id,kind,status,started_at,"
                         "updated_at) VALUES ('x','hama_bridge','done','t','t')")
            assert False, "post-migration CHECK still accepts 'hama_bridge'"
        except sqlite3.IntegrityError:
            pass
        conn.execute("INSERT INTO op_progress (op_id,kind,status,started_at,"
                     "updated_at) VALUES ('y','tvdb_bridge','done','t','t')")
        conn.close()
    finally:
        os.unlink(path)
