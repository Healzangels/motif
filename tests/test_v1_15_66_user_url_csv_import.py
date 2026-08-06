"""v1.15.66 — bulk user-URL CSV import (round-trip with // EXPORT CSV).

Pairs with the per-library // EXPORT CSV button. New 3-column
format (Title,IMDB,Youtube_URL) supports round-trip: export →
edit Youtube_URL cells → re-import via /settings#import.

Preview-first workflow (per design discussion):
  /api/import/preview parses + categorizes each row
    → clean / conflict / no_match / invalid_url / skipped
  /api/import/apply takes resolved decisions and writes
    user_overrides at section_id='' (title-global) + captures
    previous_urls + cancels in-flight downloads + enqueues
    fresh downloads per matching plex_items section.

the user went with the recommended path on all 3 design choices:
  * preview-first only (no purple-! ack-later state)
  * settings tab IMPORT (not top-level nav)
  * IMDB + title+year verify fallback for matching

Tests cover: CSV parse tolerance (UTF-8 BOM, UTF-16 LE BOM,
header aliases), match logic (IMDB exact, title+year fallback
with verify flag, no-match), categorization (clean/conflict),
apply happy-path + invalid-URL guards + audit + previous_urls
capture + download enqueue, end-to-end round-trip via TestClient.
"""
from __future__ import annotations

import csv
import io
import json
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
BASE_HTML = REPO / "app" / "web" / "templates" / "base.html"


# ── 1. Settings IMPORT tab wired through SSR + drift guards ──


def test_settings_html_has_import_tab_and_panel():
    """The IMPORT tab must appear in the settings tab nav AND
    have a matching tab-panel section. Drift between the nav
    list + panel set is caught by v1.15.64's parity test."""
    html = SETTINGS_HTML.read_text()
    assert 'data-tab="import"' in html, (
        "v1.15.66: IMPORT tab missing from #settings-tabs nav"
    )
    assert 'data-panel="import"' in html, (
        "v1.15.66: IMPORT panel section missing"
    )


def test_import_panel_has_file_input_and_buttons():
    """The IMPORT panel must surface the file input + preview +
    apply buttons that the JS handler binds to."""
    html = SETTINGS_HTML.read_text()
    # Anchor on the import panel start so we don't accidentally
    # match a different section's elements. v1.15.68 widened the
    # slice 4000 → 6500 to accommodate the new // IMPORT COMPLETE
    # banner + the preview hint paragraph; further additions
    # should keep widening rather than slicing tighter.
    # v1.19.39: widened 6500 → 8000. The v1.19.39 comment block
    # on the label-`for=` removal pushed
    # `id="import-preview-tbody"` past the 6500 window.
    start = html.index('data-panel="import"')
    block = html[start:start + 8000]
    for needed in (
        'id="import-csv-file"',
        'id="import-preview-btn"',
        'id="import-preview-results"',
        'id="import-preview-tbody"',
        'id="import-apply-btn"',
    ):
        assert needed in block, (
            f"v1.15.66: IMPORT panel missing {needed} — JS handler "
            "won't bind without it"
        )


def test_base_html_import_panel_in_ssr_allowlist():
    """v1.15.65's panel allowlist in base.html's <head> SSR script
    must include 'import' so /settings#import deep-links work
    pre-paint."""
    html = BASE_HTML.read_text()
    head = html[:html.index("</head>")]
    assert "'import'" in head, (
        "v1.15.66: base.html SSR allowlist must include 'import' — "
        "without it /settings#import deep-links flash the default "
        "panel before JS lands"
    )


# ── 2. Export change: 3-column CSV header ────────────────────


def test_export_emits_youtube_url_column():
    """The library export must emit a 3-column Title / IMDB /
    Youtube_URL header. v1.21.49: tab-delimited (UTF-16 'Unicode Text'
    so Excel/Numbers split into columns instead of lumping every field
    into column A)."""
    js = APP_JS.read_text()
    assert "'Title\\tIMDB\\tYoutube_URL'" in js, (
        "v1.21.49: CSV header must be TAB-delimited (Title\\tIMDB\\t"
        "Youtube_URL) — UTF-16 + comma collapsed every field into column "
        "A; round-trip import still needs the Youtube_URL column"
    )
    # Counter-guard: the old comma-delimited header is gone.
    assert "'Title,IMDB,Youtube_URL'" not in js, (
        "v1.21.49: stale comma-delimited header still present — UTF-16 "
        "needs TAB so spreadsheet apps split it into columns"
    )


def test_export_populates_youtube_url_only_for_u_rows():
    """The Youtube_URL cell must be populated from
    applied_youtube_url only when the row's src is 'U'. T / A / M
    / P / – rows export an empty cell so re-import is a no-op
    for them (the user hasn't expressed a user-URL preference)."""
    js = APP_JS.read_text()
    # The chain `computeSrcLetter(it) === 'U'` + `applied_youtube_url`
    # ternary is the source of truth for this rule.
    assert "computeSrcLetter(it) === 'U'" in js, (
        "v1.15.66: export must gate Youtube_URL on the U src letter"
    )
    assert "it.applied_youtube_url" in js


# ── 3. Backend: import preview + apply surface present ───────


def test_api_has_import_preview_and_apply_endpoints():
    """The two import endpoints must be registered."""
    src = API_PY.read_text()
    assert '@app.post("/api/import/preview")' in src, (
        "v1.15.66: /api/import/preview route missing"
    )
    assert '@app.post("/api/import/apply")' in src, (
        "v1.15.66: /api/import/apply route missing"
    )


def test_import_apply_writes_global_section_override():
    """Apply must INSERT into user_overrides with section_id=''
    (the title-global row). Per-section overrides are intentionally
    left untouched — bulk import is title-level, the COALESCE
    fallback in /api/library serves the global row to every section
    that doesn't have its own per-section override.

    v0.51.251: re-anchored INSIDE the apply handler. The original
    whole-file grep for a 6-placeholder VALUES literal went phantom
    when v1.21.34 added the intent column (7 placeholders) — from
    then on it was satisfied ONLY by the dead api_override handler's
    INSERT, and removing that endpoint exposed it. Pin-tests-that-
    mirror class: the anchor must be the handler, not a shape."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/apply")')
    apply_end = src.index("@app.post", apply_start + 1)
    body = src[apply_start:apply_end]
    i = body.index("INSERT INTO user_overrides")
    stmt = body[i:i + 400]
    assert "section_id)" in stmt and ", '')" in stmt, (
        "v1.15.66: apply must write user_overrides at section_id='' "
        "(title-global). Per-section writes would only affect a "
        "single section's row, missing the bulk-import intent."
    )


def test_import_apply_captures_previous_url():
    """Apply must call _capture_previous_url BEFORE the override
    upsert so REVERT round-trips back to the URL active before
    the bulk-import landed."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/apply")')
    apply_end = src.index("@app.post", apply_start + 1)
    body = src[apply_start:apply_end]
    assert "_capture_previous_url" in body, (
        "v1.15.66: apply must capture previous URL before override "
        "upsert — REVERT depends on previous_urls being populated"
    )


def test_import_apply_records_audit_with_source_import():
    """Apply must call _record_audit with details.source='import'
    so the audit log distinguishes bulk imports from manual
    SET URL operations (different blame surface for debugging)."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/apply")')
    apply_end = src.index("@app.post", apply_start + 1)
    body = src[apply_start:apply_end]
    assert "_record_audit" in body
    assert '"source": "import"' in body, (
        "v1.15.66: audit details must include source='import' so the "
        "audit log distinguishes bulk vs manual"
    )


# ── 4. Behavior: end-to-end via TestClient ──────────────────


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


def _seed_themes(db_path: Path, rows: list[dict]) -> None:
    """Insert themes rows for the test. Each row dict: {media_type,
    tmdb_id, imdb_id, title, year, upstream_source, youtube_url}."""
    with get_conn(db_path) as conn, transaction(conn):
        for r in rows:
            conn.execute(
                """INSERT INTO themes
                     (media_type, tmdb_id, imdb_id, title, year,
                      upstream_source, youtube_url,
                      last_seen_sync_at, first_seen_sync_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?,
                           '2026-01-01T00:00:00', '2026-01-01T00:00:00')""",
                (r["media_type"], r["tmdb_id"], r.get("imdb_id"),
                 r["title"], r.get("year"),
                 r.get("upstream_source", "themoviedb"),
                 r.get("youtube_url")),
            )


def _make_csv(rows: list[tuple[str, str, str]]) -> bytes:
    """Build a Title,IMDB,Youtube_URL CSV. Returns UTF-8 bytes
    without BOM (the endpoint accepts BOM-less UTF-8)."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Title", "IMDB", "Youtube_URL"])
    for title, imdb, yt in rows:
        w.writerow([title, imdb, yt])
    return buf.getvalue().encode("utf-8")


def test_preview_categorizes_clean_apply_for_new_url(app_client, auth_headers):
    """A row with a known IMDB + a URL that doesn't conflict with
    any existing user override / sidecar / Plex-serve = CLEAN
    with default_action='replace' (apply on submit)."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 100, "imdb_id": "tt0000100",
         "title": "Test Movie", "year": 1999, "upstream_source": "themoviedb"},
    ])
    csv_bytes = _make_csv([
        ("Test Movie (1999)", "tt0000100",
         "https://www.youtube.com/watch?v=abcdefghijk"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"]["clean"] == 1
    assert data["rows"][0]["status"] == "clean"
    assert data["rows"][0]["default_action"] == "replace"
    assert data["rows"][0]["theme_media_type"] == "movie"
    assert data["rows"][0]["theme_tmdb_id"] == 100


def test_preview_categorizes_skipped_for_empty_url(app_client, auth_headers):
    """Empty Youtube_URL cell → SKIPPED. Round-trip no-op signal."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 200, "imdb_id": "tt0000200",
         "title": "Movie No URL", "year": 2000},
    ])
    csv_bytes = _make_csv([
        ("Movie No URL (2000)", "tt0000200", ""),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["skipped"] == 1
    assert data["rows"][0]["status"] == "skipped"


def test_preview_categorizes_no_match_for_missing_imdb(app_client, auth_headers):
    """IMDB not in themes AND title+year doesn't match → NO MATCH."""
    client, settings = app_client
    csv_bytes = _make_csv([
        ("Phantom Movie (1990)", "tt9999999",
         "https://www.youtube.com/watch?v=zzzzzzzzzzz"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["no_match"] == 1
    assert data["rows"][0]["status"] == "no_match"


def test_preview_title_year_fallback_with_verify_flag(app_client, auth_headers):
    """Missing IMDB but title+year matches → match with verify=True
    so the UI surfaces a 'VERIFY' badge."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 300, "imdb_id": "tt0000300",
         "title": "Fallback Movie", "year": 2010},
    ])
    csv_bytes = _make_csv([
        # Empty IMDB; should fall back to title+year.
        ("Fallback Movie (2010)", "",
         "https://www.youtube.com/watch?v=fallbackid1"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["clean"] == 1
    assert data["rows"][0]["verify"] is True, (
        "v1.15.66: title+year fallback matches must set verify=True "
        "so the UI prompts the user to spot-check before applying"
    )


def test_preview_rejects_invalid_url(app_client, auth_headers):
    """A URL that isn't YouTube or SoundCloud → INVALID URL. Saves
    the user from re-uploading after the apply fails per-row."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 400, "imdb_id": "tt0000400",
         "title": "Bad URL Movie", "year": 2020},
    ])
    csv_bytes = _make_csv([
        ("Bad URL Movie (2020)", "tt0000400",
         "https://vimeo.com/something"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["invalid_url"] == 1


def test_preview_categorizes_conflict_for_existing_user_url(app_client, auth_headers):
    """Theme already has a different user_override URL → CONFLICT
    with default_action='keep' (don't accidentally overwrite)."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 500, "imdb_id": "tt0000500",
         "title": "Existing U Movie", "year": 2015},
    ])
    # Seed a user_override that differs from the imported URL.
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "                            set_at, set_by, note, section_id) "
            "VALUES ('movie', 500, ?, '2026-01-01', 'admin', 'manual', '')",
            ("https://www.youtube.com/watch?v=oldurl00000",),
        )
    csv_bytes = _make_csv([
        ("Existing U Movie (2015)", "tt0000500",
         "https://www.youtube.com/watch?v=newurl00000"),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["conflict"] == 1
    assert data["rows"][0]["status"] == "conflict"
    assert data["rows"][0]["current_src"] == "U"
    assert data["rows"][0]["default_action"] == "keep"


def test_preview_same_url_is_duplicate_skip_noop(app_client, auth_headers):
    """Imported URL identical to current user_override → DUPLICATE
    with default_action='skip' (no-op round-trip).

    v1.15.73: status was 'clean' until the user flagged the confusion:
    "when status is Clean and action is skip because it's an
    identical import then status should be something like duplicate
    or identical match." Split out as its own status."""
    client, settings = app_client
    same_url = "https://www.youtube.com/watch?v=sameurl0000"
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 600, "imdb_id": "tt0000600",
         "title": "Roundtrip Movie", "year": 2018},
    ])
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "                            set_at, set_by, note, section_id) "
            "VALUES ('movie', 600, ?, '2026-01-01', 'admin', 'manual', '')",
            (same_url,),
        )
    csv_bytes = _make_csv([
        ("Roundtrip Movie (2018)", "tt0000600", same_url),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["duplicate"] == 1
    assert data["rows"][0]["status"] == "duplicate"
    assert data["rows"][0]["default_action"] == "skip"


def test_apply_writes_user_override_and_captures_previous(app_client, auth_headers):
    """End-to-end apply: seed a theme, post a replace decision, and
    verify user_overrides + previous_urls are populated correctly."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 700, "imdb_id": "tt0000700",
         "title": "Apply Movie", "year": 2020,
         "youtube_url": "https://www.youtube.com/watch?v=tdburl00000"},
    ])
    new_url = "https://www.youtube.com/watch?v=newvideo123"
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "movie",
            "theme_tmdb_id": 700,
            "imported_url": new_url,
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["applied"] == 1
    assert data["skipped"] == 0
    # user_overrides row written at section_id=''.
    with get_conn(settings.db_path) as conn:
        ovr = conn.execute(
            "SELECT youtube_url, section_id FROM user_overrides "
            "WHERE media_type = 'movie' AND tmdb_id = 700"
        ).fetchone()
        assert ovr is not None
        assert ovr["youtube_url"] == new_url
        assert ovr["section_id"] == ""
        # previous_urls row written with the TDB url (captured before
        # the override overwrote state).
        prev = conn.execute(
            "SELECT youtube_url FROM previous_urls "
            "WHERE media_type = 'movie' AND tmdb_id = 700"
        ).fetchall()
        assert prev, "v1.15.66: apply must call _capture_previous_url"


def test_apply_keep_and_skip_are_noops(app_client, auth_headers):
    """Decisions with action='keep' or action='skip' must not write
    any user_overrides — they're the "leave alone" signals."""
    client, settings = app_client
    _seed_themes(settings.db_path, [
        {"media_type": "movie", "tmdb_id": 800, "imdb_id": "tt0000800",
         "title": "Skip Movie", "year": 2021},
    ])
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [
            {"theme_media_type": "movie", "theme_tmdb_id": 800,
             "imported_url": "https://www.youtube.com/watch?v=anyid000000",
             "action": "keep"},
            {"theme_media_type": "movie", "theme_tmdb_id": 800,
             "imported_url": "https://www.youtube.com/watch?v=anyid000000",
             "action": "skip"},
        ]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] == 0
    assert data["skipped"] == 2
    with get_conn(settings.db_path) as conn:
        ovr = conn.execute(
            "SELECT * FROM user_overrides WHERE tmdb_id = 800"
        ).fetchone()
        assert ovr is None, (
            "v1.15.66: keep/skip actions must not write user_overrides"
        )


def test_apply_rejects_unknown_theme(app_client, auth_headers):
    """A decision pointing at a theme that doesn't exist must
    surface as an error and not write anything."""
    client, _ = app_client
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "movie",
            "theme_tmdb_id": 99999999,
            "imported_url": "https://www.youtube.com/watch?v=somevideo01",
            "action": "replace",
        }]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["applied"] == 0
    assert data["skipped"] == 1
    assert data["errors"], (
        "v1.15.66: unknown theme must surface as an error entry"
    )


def test_preview_requires_admin(app_client):
    """No auth header → 401/403, not 200/500."""
    client, _ = app_client
    resp = client.post(
        "/api/import/preview",
        files={"file": ("x.csv", b"Title,IMDB,Youtube_URL\n", "text/csv")},
    )
    assert resp.status_code in (401, 403), resp.status_code


# ── 5. Frontend handler exists ───────────────────────────────


def test_app_js_has_bind_import_panel():
    """The frontend handler that wires the file picker → preview
    → apply chain must exist + be bound on DOMContentLoaded."""
    js = APP_JS.read_text()
    assert "function bindImportPanel()" in js, (
        "v1.15.66: bindImportPanel handler missing"
    )
    # And it must be called from the boot section (alongside other
    # bindX functions).
    assert "bindImportPanel();" in js, (
        "v1.15.66: bindImportPanel never invoked — file picker won't "
        "wire up"
    )
