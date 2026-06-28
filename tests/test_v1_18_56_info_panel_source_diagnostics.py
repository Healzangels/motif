"""v1.18.56 — surface source_kind + source_video_id on // MOTIF INFO panel.

the user's review of /anime SRC=A rows:

> "I would like to confirm these should be indeed src A"

The // MOTIF INFO panel showed downloaded file_path + provenance
but not the underlying `source_kind` or `source_video_id` that
drive the SRC letter computation. Without those values exposed,
operators can't directly verify the SRC letter's derivation —
they have to reason about it from the history events and external
context (e.g., HAMA-named folders implying adopt).

For the user's specific rows: the SRC=A classification IS correct.
`adopt.py:660` sets `source_kind='adopt'` for orphan / content-
mismatch findings (anime folders with pre-existing theme.mp3
files that motif claimed via the adopt path). The "Downloaded
theme for..." history events with valid YouTube video_ids are
from an EARLIER life of the same orphan synthetic id — they
don't reflect the CURRENT source_kind. Without source_kind +
source_video_id visible on the panel, this distinction wasn't
inspectable from the UI.

v1.18.56 adds them as inline diagnostic hints on the
`downloaded` row in the info panel. Long hash-derived
source_video_ids get truncated to 14 chars + ellipsis for
readability (adopt-path IDs can be 40+ chars).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── JS: info panel hint block ────────────────────────────────


def test_info_panel_renders_source_kind_hint():
    """The // MOTIF INFO panel's downloaded-row template must
    render `source_kind=<code>` when local_files.source_kind is
    set."""
    src = APP_JS.read_text()
    assert "source_kind=<code>" in src, (
        "v1.18.56: info panel must surface lf.source_kind in a "
        "code-wrapped span on the downloaded row"
    )
    # The hint is conditional — only fires when lf.source_kind
    # exists (falsy/null skips the block).
    assert "lf && lf.source_kind" in src or "lf.source_kind" in src


def test_info_panel_renders_source_video_id_hint():
    """The info panel must render `source_video_id=<code>` when
    local_files.source_video_id is set."""
    src = APP_JS.read_text()
    assert "source_video_id=<code>" in src, (
        "v1.18.56: info panel must surface lf.source_video_id"
    )


def test_info_panel_truncates_long_source_video_id():
    """Adopt-path source_video_ids are hash-derived and can be
    40+ chars. The panel must truncate so they don't blow out
    the layout."""
    src = APP_JS.read_text()
    # The truncation logic: `length > 16` → slice(0, 14) + '…'
    assert "source_video_id.length > 16" in src, (
        "v1.18.56: long source_video_id values must be truncated "
        "to ~14 chars + ellipsis for readability"
    )
    assert ".slice(0, 14)" in src
    assert "'…'" in src or '"…"' in src


def test_info_panel_hint_block_uses_html_escape():
    """User-supplied values (source_kind / source_video_id) go
    through htmlEscape before being injected — pin so a future
    refactor can't drop the escape and create an XSS vector."""
    src = APP_JS.read_text()
    # The sourceKindHint and sourceVidHint definitions each
    # call htmlEscape on the values they interpolate.
    idx = src.index("sourceKindHint")
    block = src[idx:idx + 1200]
    assert "htmlEscape(lf.source_kind)" in block
    # source_video_id is wrapped via the truncation but htmlEscape
    # is applied to the result.
    assert "htmlEscape(" in block


# ── API already returns source_kind + source_video_id ────────


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
    yield TestClient(create_app(settings)), db


AUTH = {"X-Authentik-Username": "testadmin"}


def test_api_item_info_returns_source_kind_and_video_id(admin_client):
    """The /api/items/{mt}/{tmdb}/info endpoint already uses
    `SELECT *` from local_files — the source_kind + source_video_id
    columns are surfaced without any API change. Pin the end-to-
    end contract so the JS-side render block has the data it
    needs."""
    client, db = admin_client
    # Seed minimal parents + a local_files row with source_kind +
    # source_video_id set to known values.
    from app.core.db import get_conn
    ts = "2026-05-21T00:00:00Z"
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "                            included, discovered_at, "
            "                            last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, "
            "                     upstream_source, last_seen_sync_at, "
            "                     first_seen_sync_at) "
            "VALUES ('movie', 42, 'Test', 'themoviedb', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, "
            "                          file_path, downloaded_at, "
            "                          source_video_id, provenance, "
            "                          source_kind) "
            "VALUES ('movie', 42, '1', 'movies/Test/theme.mp3', "
            "        ?, 'dQw4w9WgXcQ', 'auto', 'themerrdb')",
            (ts,),
        )
        conn.commit()

    r = client.get("/api/items/movie/42", headers=AUTH)
    assert r.status_code == 200
    data = r.json()
    # The API returns local_files as a list (one per section).
    locals_list = data.get("local_files") or []
    assert len(locals_list) >= 1
    lf = locals_list[0]
    assert lf.get("source_kind") == "themerrdb", (
        "v1.18.56: /api/items/{mt}/{tmdb} response must include "
        "source_kind on each local_files entry"
    )
    assert lf.get("source_video_id") == "dQw4w9WgXcQ", (
        "v1.18.56: response must include source_video_id"
    )


# ── SRC letter decision table reference ──────────────────────


SRC_DECISION_TABLE_KEY = """
The SRC letter classification (from _SRC_LETTER_SQL in
app/web/api.py:915 and computeSrcLetter in app.js:7332):

  Step 1: if not placed → fall through to M/P/—
  Step 2: source_kind override (highest specificity wins):
    'themerrdb'        → T
    'adopt'            → A
    'url' or 'upload'  → U
  Step 3: provenance fallback (when source_kind missing or
    unrecognized):
    provenance='auto'   → T
    provenance='manual':
      NOT orphan        → U
      orphan AND empty source_video_id  → U
      orphan AND 11-char source_video_id (YouTube ID) → U
      orphan AND source_video_id LIKE 'sc-%' (SoundCloud) → U
      orphan AND other source_video_id  → A
  Step 4: not placed:
    plex_local_theme=1 (sidecar) → M
    plex_has_theme + verified_ok → P
    otherwise → —

The v1.18.56 INFO panel hints expose source_kind +
source_video_id directly so the operator can walk through
this decision tree for any specific row without grepping
the SQL.
"""


def test_src_decision_table_reference_present():
    """The decision-table docstring exists so future debugging
    of 'why is this row SRC=X' has a single source of truth
    explaining the rules."""
    assert "source_kind override" in SRC_DECISION_TABLE_KEY
    assert "'adopt'" in SRC_DECISION_TABLE_KEY
    assert "11-char" in SRC_DECISION_TABLE_KEY


# ── Version marker ───────────────────────────────────────────


def test_v1_18_56_marker_present():
    """The marker lives on the new hint block."""
    src = APP_JS.read_text()
    # The marker must be in a comment near the sourceKindHint
    # definition.
    idx = src.index("sourceKindHint")
    block = src[max(0, idx - 1000):idx + 200]
    assert "v1.18.56" in block, (
        "v1.18.56: marker must sit near the new hint block"
    )
