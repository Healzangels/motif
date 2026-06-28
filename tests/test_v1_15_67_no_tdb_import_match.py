"""v1.15.67 — bug-hunt follow-up to v1.15.66 import.

the user tested v1.15.66 on a real anime CSV and reported:

  "not sure what the top check box it for exactly, also getting
   a lot of no matches which I know are matches since the export
   came from motif originally."

Two issues:

1. **NO TDB rows came back as NO MATCH.** v1.15.66 only matched
   IMDB → themes.imdb_id. But the library export emits one row
   per plex_items entry (LEFT JOIN themes), including rows where
   the JOIN finds nothing (NO TDB anime, TV shows TDB doesn't
   cover). Those rows ship a valid IMDB from pi.guid_imdb but
   have no themes row to match against, so the v1.15.66 matcher
   missed every NO TDB row in the user's CSV. The fix mirrors
   manual-url's two-stage lookup: existing theme first, then
   synthesize a plex_orphan theme on apply when only plex_items
   knows about the row.

2. **Header checkbox was unlabeled.** Visible "ALL" label +
   tooltip now make it clear it's a bulk-toggle for the per-row
   // Apply checkboxes.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import get_conn, init_db, transaction


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Header checkbox UX ──────────────────────────────────────


# v1.15.67 originally wrapped the header checkbox in a span with
# visible "ALL" text. the user's v1.15.67 test feedback:
# "currently it says STATUS TITLE IMDB CURRENT IMPORTED URL ACTION
# but it doesn't line up with anything below" — the inline label
# read as a phantom column name. v1.15.68 retired the "ALL" label,
# kept the checkbox flush in column 1, and moved the explanation
# to a hint paragraph above the table. New assertions live in
# tests/test_v1_15_68_*.


# ── Match path now considers plex_items ─────────────────────


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
    return client, settings


@pytest.fixture
def auth_headers():
    return {"X-Authentik-Username": "testadmin"}


def _seed_plex_items(db_path: Path, rows: list[dict]) -> None:
    """Seed plex_items rows for the test. Each row dict: {rating_key,
    section_id, media_type, title, year, guid_imdb, has_theme,
    local_theme_file, plex_theme_verified_ok}."""
    with get_conn(db_path) as conn, transaction(conn):
        # plex_sections FK on section_id, but the tests don't always
        # need a corresponding section row — schema permits orphan
        # section_ids in plex_items if FKs are off. init_db doesn't
        # enforce FKs by default so we can skip the section seed.
        for r in rows:
            conn.execute(
                """INSERT INTO plex_items
                     (rating_key, section_id, media_type, title, year,
                      guid_imdb, has_theme, local_theme_file,
                      plex_theme_verified_ok, first_seen_at, last_seen_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                           '2026-01-01T00:00:00', '2026-01-01T00:00:00')""",
                (r["rating_key"], r["section_id"], r["media_type"],
                 r["title"], r.get("year"), r.get("guid_imdb"),
                 r.get("has_theme", 0), r.get("local_theme_file", 0),
                 r.get("plex_theme_verified_ok")),
            )


def _make_csv(rows: list[tuple[str, str, str]]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    for title, imdb, yt in rows:
        w.writerow([title, imdb, yt])
    return buf.getvalue().encode("utf-8")


def test_preview_matches_no_tdb_row_via_plex_items(app_client, auth_headers):
    """the user's bug: NO TDB anime rows in the CSV came back as NO
    MATCH because v1.15.66 only walked themes. v1.15.67 falls
    through to plex_items.guid_imdb so the row matches via the
    orphan-pending path."""
    client, settings = app_client
    # Seed a plex_items row with NO matching themes row.
    _seed_plex_items(settings.db_path, [{
        "rating_key": "100001", "section_id": "1",
        "media_type": "show", "title": "A Wild Anime",
        "year": "2025", "guid_imdb": "tt99990001",
    }])
    csv_bytes = _make_csv([
        ("A Wild Anime (2025)", "tt99990001",
         "https://www.youtube.com/watch?v=anime000001"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"]["clean"] == 1, (
        f"v1.15.67: NO TDB row should now match via plex_items; "
        f"got counts={data['counts']}"
    )
    row = data["rows"][0]
    assert row["status"] == "clean"
    # Critical: imdb_id_for_orphan is the apply-side identifier for
    # the synthetic creation path, theme_tmdb_id is null.
    assert row["theme_tmdb_id"] is None
    assert row["imdb_id_for_orphan"] == "tt99990001"
    assert row["theme_media_type"] == "tv"  # 'show' → 'tv' translation
    assert row["current_src"] == "-"
    assert row["default_action"] == "replace"


def test_preview_orphan_pending_conflict_when_plex_serves_theme(
    app_client, auth_headers
):
    """A NO TDB row where Plex already serves a verified theme
    (has_theme=1 + verified=1) should be CONFLICT, not CLEAN — the
    user might not want to clobber a working Plex theme with their
    bulk import."""
    client, settings = app_client
    _seed_plex_items(settings.db_path, [{
        "rating_key": "100002", "section_id": "1",
        "media_type": "show", "title": "Anime With Plex Theme",
        "year": "2026", "guid_imdb": "tt99990002",
        "has_theme": 1, "plex_theme_verified_ok": 1,
    }])
    csv_bytes = _make_csv([
        ("Anime With Plex Theme (2026)", "tt99990002",
         "https://www.youtube.com/watch?v=anime000002"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["conflict"] == 1
    assert data["rows"][0]["current_src"] == "P"
    # v1.21.34: P-rows now default to "download as backup"
    # (download_only) instead of "keep current" — stage the import as a
    # UB backup while Plex keeps serving its own theme. the user's call:
    # only P-rows flip the default.
    assert data["rows"][0]["default_action"] == "download_only"


def test_preview_orphan_pending_conflict_when_local_sidecar_present(
    app_client, auth_headers
):
    """NO TDB row with a local sidecar (M) → CONFLICT. The user
    might not want to silently overwrite a manually-curated
    sidecar with a bulk-imported URL."""
    client, settings = app_client
    _seed_plex_items(settings.db_path, [{
        "rating_key": "100003", "section_id": "1",
        "media_type": "show", "title": "Anime With Sidecar",
        "year": "2027", "guid_imdb": "tt99990003",
        "local_theme_file": 1,
    }])
    csv_bytes = _make_csv([
        ("Anime With Sidecar (2027)", "tt99990003",
         "https://www.youtube.com/watch?v=anime000003"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["conflict"] == 1
    assert data["rows"][0]["current_src"] == "M"


def test_apply_synthesizes_orphan_theme_for_no_tdb_row(app_client, auth_headers):
    """End-to-end: a NO TDB row preview → apply → the apply
    endpoint synthesizes a plex_orphan theme + stamps pi.theme_id
    + writes the user_override. Mirrors manual-url's behavior on
    orphan rows (api.py:9018-9034)."""
    client, settings = app_client
    _seed_plex_items(settings.db_path, [{
        "rating_key": "100004", "section_id": "1",
        "media_type": "show", "title": "Apply Anime",
        "year": "2028", "guid_imdb": "tt99990004",
    }])
    new_url = "https://www.youtube.com/watch?v=applyorphan"
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "tv",
            "theme_tmdb_id": None,
            "imdb_id_for_orphan": "tt99990004",
            "imported_url": new_url,
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["applied"] == 1
    assert data["skipped"] == 0
    with get_conn(settings.db_path) as conn:
        # Synthetic theme created with negative tmdb_id +
        # upstream_source='plex_orphan'.
        theme = conn.execute(
            "SELECT * FROM themes WHERE imdb_id = ?",
            ("tt99990004",),
        ).fetchone()
        assert theme is not None, (
            "v1.15.67: apply must mint a synthetic theme for the "
            "orphan-pending case (no themes row pre-apply)"
        )
        assert theme["upstream_source"] == "plex_orphan"
        assert theme["tmdb_id"] < 0, (
            "v1.15.67: synthetic theme tmdb_id must be negative so it "
            "doesn't collide with real TDB ids"
        )
        # pi.theme_id stamped on the matching plex_items row so the
        # library JOIN picks it up (v1.11.64 fix mirror).
        pi = conn.execute(
            "SELECT theme_id FROM plex_items WHERE rating_key = '100004'"
        ).fetchone()
        assert pi["theme_id"] == theme["id"], (
            "v1.15.67: apply must stamp pi.theme_id on every matching "
            "plex_items row so the library JOIN finds the synthetic theme"
        )
        # user_overrides written at section_id=''.
        ovr = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE imdb_id_proxy IS NULL "  # noop; just verifying lookup shape
            "  AND tmdb_id = ?",
            (theme["tmdb_id"],),
        ).fetchone() if False else conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = 'tv' AND tmdb_id = ? AND section_id = ''",
            (theme["tmdb_id"],),
        ).fetchone()
        assert ovr is not None
        assert ovr["youtube_url"] == new_url


def test_apply_dedup_orphan_synth_across_decisions(app_client, auth_headers):
    """Two decisions for the same NO TDB IMDB in one batch must
    create ONE synthetic theme, not two. The second decision's
    theme lookup finds the freshly-minted theme from the first
    decision (within the same transaction)."""
    client, settings = app_client
    # Single plex_items row, two decisions targeting it via the
    # same IMDB (a duplicate in the CSV — pathological but possible).
    _seed_plex_items(settings.db_path, [{
        "rating_key": "100005", "section_id": "1",
        "media_type": "show", "title": "Dedup Anime",
        "year": "2029", "guid_imdb": "tt99990005",
    }])
    decisions = [
        {"theme_media_type": "tv", "theme_tmdb_id": None,
         "imdb_id_for_orphan": "tt99990005",
         "imported_url": "https://www.youtube.com/watch?v=firsturl001",
         "action": "replace"},
        {"theme_media_type": "tv", "theme_tmdb_id": None,
         "imdb_id_for_orphan": "tt99990005",
         "imported_url": "https://www.youtube.com/watch?v=secondurl1",
         "action": "replace"},
    ]
    resp = client.post(
        "/api/import/apply",
        json={"decisions": decisions},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM themes WHERE imdb_id = ?",
            ("tt99990005",),
        ).fetchone()
        assert rows["n"] == 1, (
            "v1.15.67: dup decisions on same IMDB must produce ONE "
            "synthetic theme (lookup-by-imdb must catch the just-"
            "created row before re-minting)"
        )


def test_apply_orphan_synth_missing_plex_items_errors(app_client, auth_headers):
    """If the decision references an IMDB with no plex_items row
    (caller submitted a stale preview), the apply must surface an
    error and skip — never crash or write garbage."""
    client, _ = app_client
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "tv",
            "theme_tmdb_id": None,
            "imdb_id_for_orphan": "tt00000000",  # nothing seeded
            "imported_url": "https://www.youtube.com/watch?v=ghostrow001",
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] == 0
    assert data["skipped"] == 1
    assert data["errors"], (
        "v1.15.67: orphan_synth with no plex_items must surface an error"
    )
    assert "no plex_items" in data["errors"][0]["error"]


def test_apply_missing_both_identifiers_errors(app_client, auth_headers):
    """Decision must carry theme_tmdb_id OR imdb_id_for_orphan;
    missing both → error (don't silently swallow malformed payloads)."""
    client, _ = app_client
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "tv",
            "imported_url": "https://www.youtube.com/watch?v=anything001",
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] == 0
    assert data["skipped"] == 1
    assert data["errors"]


def test_apply_existing_theme_path_still_works(app_client, auth_headers):
    """Counter-test: v1.15.66 happy-path (existing theme) must keep
    working — the orphan synth path is additive, not replacement."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source,
                  last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 555, 'tt00000555', 'Existing', 2020,
                       'themoviedb',
                       '2026-01-01T00:00:00', '2026-01-01T00:00:00')"""
        )
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "movie",
            "theme_tmdb_id": 555,
            "imdb_id_for_orphan": None,
            "imported_url": "https://www.youtube.com/watch?v=existing001",
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] == 1
    with get_conn(settings.db_path) as conn:
        ovr = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type = 'movie' AND tmdb_id = 555 AND section_id = ''"
        ).fetchone()
        assert ovr is not None


# ── Frontend passes the orphan identifier ───────────────────


def test_app_js_decision_payload_includes_imdb_id_for_orphan():
    """The JS apply handler must include imdb_id_for_orphan in
    each decision payload so the backend can take the synthetic
    creation path for NO TDB rows."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "imdb_id_for_orphan" in body, (
        "v1.15.67: bindImportPanel must pass imdb_id_for_orphan in "
        "each decision so NO TDB rows can take the orphan-synth "
        "path in /api/import/apply"
    )
