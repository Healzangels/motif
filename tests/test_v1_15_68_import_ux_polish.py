"""v1.15.68 — UX polish to v1.15.67 import.

the user's feedback after testing v1.15.67:

  "getting more matches but we're not matching Übel Blatt (2024)
   also can we move import in setting to between schedule and runtime.
   Also still not sure what the top check box is actually doing,
   currently it says STATUS TITLE IMDB CURRENT IMPORTED URL ACTION
   but it doesn't line up with anything below.
   Also after accepting and having things applied I think the final
   import screen should change to a summary page or show it's done
   as currently its hard to tell the action happened and is
   completed successfully"

Four targeted fixes:

1. **Übel Blatt SKIPPED was correct** (empty Youtube_URL = no-op)
   but indistinguishable from NO MATCH visually. v1.15.68: the
   preview now still runs the match for empty-URL rows and
   surfaces current_src/current_url on SKIPPED matched rows.
   Unmatched-AND-empty rows fall through to NO MATCH instead.

2. **IMPORT tab moved** between SCHEDULE and RUNTIME (was last).

3. **Header "ALL" label was confusing** — read as a phantom
   column name. v1.15.68 strips the label, leaves the checkbox
   flush in column 1 (aligned with row checkboxes), and adds a
   hint paragraph above the table.

4. **Apply success was easy to miss** — inline "N applied" text
   only. v1.15.68 adds a green-bordered // ✓ IMPORT COMPLETE
   banner with counts + an // IMPORT ANOTHER reset button. The
   preview table hides on success for unambiguous "done" signal.
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


# ── 1. IMPORT tab moved to between SCHEDULE and RUNTIME ─────


def test_import_tab_sits_between_schedule_and_runtime():
    """the user: 'can we move import in setting to between schedule
    and runtime.' Tab nav order must reflect this — IMPORT after
    SCHEDULE, before RUNTIME."""
    html = SETTINGS_HTML.read_text()
    nav_start = html.index('id="settings-tabs"')
    nav_end = html.index("</nav>", nav_start)
    nav = html[nav_start:nav_end]
    sched = nav.index('data-tab="schedule"')
    imp = nav.index('data-tab="import"')
    runt = nav.index('data-tab="runtime"')
    assert sched < imp < runt, (
        "v1.15.68: IMPORT tab must sit between SCHEDULE and RUNTIME "
        "in the settings nav order (the user: groups data-operations "
        "adjacent)"
    )


# ── 2. Header checkbox UX: no in-column label, hint above table ──


def test_import_header_checkbox_has_no_inline_label():
    """v1.15.68 strips v1.15.67's in-cell 'ALL' label. The checkbox
    sits alone in the cell-tight column header so it aligns with
    the row checkboxes below — the user: 'doesn't line up with
    anything below.'"""
    html = SETTINGS_HTML.read_text()
    # Find the header <th> containing the master checkbox.
    cb_pos = html.index('id="import-row-select-all"')
    # Walk back to the enclosing <th> tag.
    th_start = html.rfind("<th", 0, cb_pos)
    th_end = html.index("</th>", cb_pos)
    th_block = html[th_start:th_end]
    assert "import-select-all-label" not in th_block, (
        "v1.15.68: header checkbox must NOT wrap in the v1.15.67 "
        "'ALL' label span — it reads as a phantom column name and "
        "throws off the column alignment"
    )
    # And the label text "ALL" must not appear inline anywhere
    # near the checkbox (would still cause the user's confusion).
    assert ">ALL<" not in th_block and " ALL " not in th_block, (
        "v1.15.68: 'ALL' text must not appear in the column 1 header"
    )


# v1.15.68 added a hint paragraph above the preview table to
# explain the master checkbox. v1.15.69 retired it — the user:
# "overly verbose and not overly helpful." With the v1.15.69
# table-rendering rebuild (.grid → .table, columns aligned),
# the bare checkbox + title tooltip is enough. The hint-paragraph
# assertion that lived here has been removed.


# ── 3. Apply-complete banner present + styled ───────────────


def test_import_apply_complete_banner_exists_in_template():
    """Apply-complete banner must be wired into the import panel
    so JS can reveal it on successful // APPLY IMPORT."""
    html = SETTINGS_HTML.read_text()
    assert 'id="import-apply-complete"' in html
    assert 'id="import-complete-applied"' in html
    assert 'id="import-complete-skipped"' in html
    assert 'id="import-complete-errors"' in html
    assert 'id="import-complete-reset"' in html, (
        "v1.15.68: must include // IMPORT ANOTHER reset button so the "
        "user can stage a follow-up CSV without a page reload"
    )


def test_import_apply_complete_banner_has_css_styling():
    """The .apply-complete-banner CSS rule must exist so the
    success state visually pops (green border, larger title) —
    plain text was too easy to miss in v1.15.67."""
    css = APP_CSS.read_text()
    assert ".apply-complete-banner {" in css, (
        "v1.15.68: .apply-complete-banner CSS rule must exist for "
        "the prominent success styling"
    )
    assert ".apply-complete-title {" in css


def test_app_js_show_complete_banner_function_exists():
    """JS must define a showCompleteBanner that fills in the counts
    + reveals the banner + hides the preview table. Without it the
    success surface is still the old inline-text fallback."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "function showCompleteBanner" in body, (
        "v1.15.68: bindImportPanel must define showCompleteBanner — "
        "it's the success-surface UX fix"
    )
    assert "function resetImportPanel" in body, (
        "v1.15.68: // IMPORT ANOTHER reset handler must exist so the "
        "user can start fresh without a page reload"
    )
    # The apply handler must CALL showCompleteBanner instead of the
    # old applyStatus.innerHTML write.
    assert "showCompleteBanner(data)" in body, (
        "v1.15.68: applyBtn handler must invoke showCompleteBanner "
        "on success to surface the prominent green banner"
    )


# ── 4. Empty-URL rows still match (no more silent NO MATCH) ──


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
    with get_conn(db_path) as conn, transaction(conn):
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


def test_preview_skipped_empty_url_row_shows_current_src_when_matched(
    app_client, auth_headers
):
    """the user's bug: 'we're not matching Übel Blatt (2024)' — the
    row was actually being SKIPPED (empty Youtube_URL = no-op)
    but the UI showed no current_src so it was indistinguishable
    from NO MATCH. v1.15.68: SKIPPED rows that DID match show
    their current state so the user can tell the row is in the
    library, just has nothing to import."""
    client, settings = app_client
    _seed_plex_items(settings.db_path, [{
        "rating_key": "200001", "section_id": "1",
        "media_type": "show", "title": "Übel Blatt",
        "year": "2024", "guid_imdb": "tt32581027",
    }])
    csv_bytes = _make_csv([
        # Empty Youtube_URL — exactly the v1.15.66 export shape
        # for a row that the user hasn't filled in.
        ("Übel Blatt (2024)", "tt32581027", ""),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["counts"]["skipped"] == 1, (
        f"v1.15.68: matched empty-URL row should be SKIPPED, got "
        f"counts={data['counts']}"
    )
    row = data["rows"][0]
    assert row["status"] == "skipped"
    assert row["current_src"] == "-", (
        f"v1.15.68: SKIPPED row must surface current_src so user "
        f"sees the row matched; got current_src={row['current_src']!r}"
    )
    assert row["default_action"] == "skip"
    # No NO MATCH count — that was the user's confusion.
    assert data["counts"].get("no_match", 0) == 0


def test_preview_skipped_empty_url_unmatched_falls_through_to_no_match(
    app_client, auth_headers
):
    """Counter-test: an empty-URL row whose IMDB isn't in the
    library should be NO MATCH (not SKIPPED). Visually distinct
    from the v1.15.68 matched-skip case so the user knows the
    row is foreign to their library."""
    client, _ = app_client
    csv_bytes = _make_csv([
        ("Ghost Show (2099)", "tt99999999", ""),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["no_match"] == 1
    assert data["counts"].get("skipped", 0) == 0


def test_preview_skipped_matched_row_shows_current_url_for_existing_u(
    app_client, auth_headers
):
    """A matched empty-URL row where the library row is currently
    'U' (existing user override) should SHOW the current URL in
    the preview so the user sees 'this row is already user-URL,
    no import needed'."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at, first_seen_sync_at)
               VALUES ('movie', 12345, 'tt12345000', 'Test', 2020,
                       'themoviedb', '2026-01-01', '2026-01-01')"""
        )
        existing_url = "https://www.youtube.com/watch?v=existing01"
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, "
            "                            set_at, set_by, note, section_id) "
            "VALUES ('movie', 12345, ?, '2026-01-01', 'admin', 'x', '')",
            (existing_url,),
        )
    csv_bytes = _make_csv([
        ("Test (2020)", "tt12345000", ""),
    ])
    resp = client.post(
        "/api/import/preview",
        files={"file": ("test.csv", csv_bytes, "text/csv")},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["counts"]["skipped"] == 1
    row = data["rows"][0]
    assert row["current_src"] == "U"
    assert row["current_url"] == existing_url, (
        "v1.15.68: SKIPPED rows with existing user URLs must surface "
        "the current URL so the user sees what's already there"
    )
