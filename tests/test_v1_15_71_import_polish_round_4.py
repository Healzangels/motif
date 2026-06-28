"""v1.15.71 — import polish round 4 (the user feedback on v1.15.70).

Five issues the user flagged after live-testing v1.15.70:

1. **SRC letter has no color** — v1.15.66 used `src-pill src-X`
   class names I'd invented; they were never in app.css so the
   letter rendered as plain text. Library uses `link-badge
   link-badge-{themerrdb|user|adopt|manual|cloud}`. v1.15.71
   swaps to the library palette so T=green, U=violet, A=cyan,
   M=red, P=amber — matching what the user already reads in
   /movies, /tv, /anime.

2. **Column alignment** — the user: "Status and Title should be
   left sort, imdb center, current center, import url center,
   actions centered." CSS aligns each column accordingly.

3. **Action column spilling off the right** + **CURRENT and
   IMPORTED URL overlapping** — both symptoms of the v1.15.70
   inline-flex .current-stack letting URLs overflow the cell.
   v1.15.71 caps URL columns at fixed pixel widths (260 / 280),
   makes .url-cell display:block so ellipsis-truncate works
   inside the column, and stacks the src-letter badge above the
   URL on its own line.

4. **No intro paragraph** — v1.15.69 retired the verbose
   v1.15.66 intro. the user: "lets provide some clear information
   ... use the same style/format as our other sections."
   v1.15.71 adds a concise 2-sentence intro in the standard
   .form-hint.block-intro pattern.

5. **Download-only action for P rows** — the user: "if it's a P
   row then let's offer an additional action for download only
   so that way we're downloading a backup copy but still
   allowing plex to serve the theme until motif one is pushed."
   P-row CONFLICT dropdowns gain a 'Download backup (Plex
   stays)' option. JS maps it to action=replace + force_place
   =false; backend reads force_place from the decision payload.
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
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. SRC letter uses library link-badge palette ───────────


def test_import_row_renders_src_letter_via_library_link_badge():
    """v1.15.66 src-pill src-X class never existed in app.css.
    v1.15.71 reuses the library .link-badge-{themerrdb,user,adopt,
    manual,cloud} palette so T/U/A/M/P match the row pill colors
    the user already reads on /movies, /tv, /anime."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # All 5 library badge classes are referenced.
    for css_class in (
        "link-badge-themerrdb",
        "link-badge-user",
        "link-badge-adopt",
        "link-badge-manual",
        "link-badge-cloud",
    ):
        assert css_class in body, (
            f"v1.15.71: import row rendering must reference {css_class} "
            "so the SRC letter matches the library palette"
        )


def test_import_row_no_longer_uses_phantom_src_pill_class():
    """Counter-guard: the v1.15.66 `src-pill src-X` class names
    (never defined in CSS) must not reappear in any rendered
    output. The narrative comment at the top of bindImportPanel
    mentions the name (referencing what was retired) — strip
    line comments before the assertion so we only check the
    actual rendered markup."""
    import re
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    # Strip JS // line comments + /* */ blocks so narrative
    # mentions of the retired class don't false-trigger.
    body = re.sub(r"//[^\n]*", "", body)
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.DOTALL)
    assert "src-pill" not in body, (
        "v1.15.71: the v1.15.66 src-pill class must not appear in "
        "rendered markup — library uses .link-badge-X"
    )


# ── 2. Column alignment matches the user's spec ───────────────


def test_import_css_column_alignment_matches_spec():
    """the user's spec: Status LEFT, Title LEFT, IMDB CENTER,
    Current CENTER, Imported URL CENTER, Action CENTER."""
    css = APP_CSS.read_text()
    expected = {
        ".col-import-status": "left",
        ".col-title": "left",
        ".col-imdb": "center",
        ".col-import-current": "center",
        ".col-import-url": "center",
        ".col-import-action": "center",
    }
    for cls, align in expected.items():
        # Look within the #import-preview-table scoped rules so we
        # don't false-match library table column rules.
        sel = f"#import-preview-table {cls}"
        rule_idx = css.find(sel)
        assert rule_idx >= 0, f"v1.15.71: missing {sel} rule"
        rule_end = css.index("}", rule_idx)
        rule = css[rule_idx:rule_end]
        assert f"text-align: {align}" in rule, (
            f"v1.15.71: {sel} must be text-align:{align} per the user's "
            "spec"
        )


# ── 3. URL columns don't overflow ───────────────────────────


def test_import_url_cells_are_block_level():
    """v1.15.70 used inline-flex for .current-stack which let
    long URLs grow past the cell and overlap into IMPORTED URL.
    v1.15.71: .url-cell is display:block so it takes the cell
    width and ellipsis-truncates inside the column boundary."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .url-cell")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "display: block" in rule, (
        "v1.15.71: .url-cell must be display:block so ellipsis "
        "truncates inside the column instead of overflowing"
    )
    assert "text-overflow: ellipsis" in rule
    assert "overflow: hidden" in rule


def test_import_current_stack_is_inline_row_not_column():
    """v1.15.71 stacked the src badge ABOVE the URL inside the
    CURRENT cell (column flex) so neither overflowed the column.
    v1.15.73 dropped the URL from the cell entirely — replaced
    with a small info button — so the cell is back to a single
    horizontal line (badge + info icon). Both CURRENT and
    IMPORTED URL cells are now single-line, vertically aligned
    across rows per the user's v1.15.71 alignment complaint."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .current-stack")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    assert "inline-flex" in rule or "display: flex" in rule
    # Single-line: must NOT use flex-direction:column.
    assert "flex-direction: column" not in rule, (
        "v1.15.73: .current-stack switched back to row layout "
        "(badge + info icon, no URL preview) — column flex would "
        "stack them vertically again and re-introduce the v1.15.70 "
        "alignment mismatch"
    )


def test_import_action_column_has_fixed_width():
    """The action column must have a fixed pixel width so its
    contents (the select picker) don't push the table past the
    container — the user's v1.15.70 'spilling off the edge' bug."""
    css = APP_CSS.read_text()
    rule_start = css.index("#import-preview-table .col-import-action")
    rule_end = css.index("}", rule_start)
    rule = css[rule_start:rule_end]
    # Width must be a pixel value (not a % which can compound past 100%).
    import re
    m = re.search(r"width:\s*(\d+)px", rule)
    assert m, (
        "v1.15.71: .col-import-action must use a pixel width — % "
        "widths compound past 100% when the URL columns also use %"
    )


# ── 4. Concise intro paragraph is back ──────────────────────


def test_import_panel_has_concise_block_intro():
    """v1.15.69 retired the verbose v1.15.66 intro. v1.15.71 adds
    a SHORT intro back per the user's request — same style as other
    settings sections (.form-hint.block-intro)."""
    html = SETTINGS_HTML.read_text()
    # Anchor on the IMPORT panel only — other panels also use
    # .form-hint.block-intro for their own intros.
    panel_start = html.index('data-panel="import"')
    block_body_start = html.index('class="block-body"', panel_start)
    # Look at the first ~1200 chars of the block-body for the intro.
    block_body = html[block_body_start:block_body_start + 1200]
    assert 'class="form-hint block-intro"' in block_body, (
        "v1.15.71: IMPORT panel must have a .form-hint.block-intro "
        "paragraph at the top — same pattern as other settings tabs"
    )
    # The intro must mention the round-trip with EXPORT CSV so the
    # user knows where the file should come from.
    assert "EXPORT CSV" in block_body, (
        "v1.15.71: the intro must reference // EXPORT CSV so the "
        "round-trip workflow is discoverable from the import panel"
    )


def test_import_intro_is_concise_not_verbose():
    """The new intro must be short. the user's v1.15.68 complaint
    about the previous intro ('overly verbose and not overly
    helpful') still applies — keep it to ~3 short sentences max."""
    html = SETTINGS_HTML.read_text()
    panel_start = html.index('data-panel="import"')
    intro_start = html.index('class="form-hint block-intro"',
                              panel_start)
    intro_end = html.index("</p>", intro_start)
    intro = html[intro_start:intro_end]
    # Strip HTML tags + whitespace then count sentences.
    import re
    text = re.sub(r"<[^>]+>", "", intro)
    text = re.sub(r"\s+", " ", text).strip()
    # Rough sentence count: split on period+space + colon+space.
    sentences = re.split(r"\.\s+|:\s+", text)
    sentences = [s for s in sentences if s.strip()]
    assert len(sentences) <= 4, (
        f"v1.15.71: intro should be at most 4 short sentences — "
        f"v1.15.66's verbose intro was the reason it got retired. "
        f"Got {len(sentences)}: {sentences!r}"
    )


# ── 5. Download-only action for P rows ──────────────────────


def test_app_js_offers_download_only_for_p_row_conflicts():
    """P-row CONFLICT dropdowns must include the 'Download
    backup (Plex stays)' option — the user: "if it's a P row
    then let's offer an additional action for download only"."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "download_only" in body, (
        "v1.15.71: P-row conflict picker must offer download_only "
        "as a 3rd action between Replace and Skip"
    )
    # Gating: only for current_src === 'P' (not for every conflict).
    assert "current_src === 'P'" in body or 'current_src=="P"' in body, (
        "v1.15.71: the download_only option must be gated on "
        "current_src === 'P' so non-P conflicts don't surface it"
    )


def test_app_js_maps_download_only_to_replace_plus_force_place_false():
    """JS converts the UI action='download_only' into the API
    payload action=replace + force_place=false. This keeps the
    API contract simple (single replace action) while the
    behavior diverges at the placement step."""
    js = APP_JS.read_text()
    fn_start = js.index("function bindImportPanel()")
    fn_end = js.index("// ---- Config form", fn_start)
    body = js[fn_start:fn_end]
    assert "force_place" in body, (
        "v1.15.71: JS must pass force_place in the decision payload "
        "so the apply endpoint knows whether to skip the place step"
    )
    # The mapping itself.
    assert "action === 'download_only'" in body \
        or "action == 'download_only'" in body, (
        "v1.15.71: JS must read action==='download_only' and map it "
        "to force_place=false before sending to the backend"
    )


def test_api_apply_reads_force_place_from_decision():
    """Backend apply must honor force_place from the decision
    payload — defaulting True (the standard bulk-import behavior)
    when omitted. Without this the JS mapping does nothing."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/apply")')
    apply_end = src.index('@app.post', apply_start + 1)
    body = src[apply_start:apply_end]
    assert '"force_place"' in body or '.get("force_place"' in body, (
        "v1.15.71: apply endpoint must read force_place from the "
        "decision payload to support download-only"
    )
    # Must default True when omitted (back-compat with v1.15.66-70
    # decisions which don't carry the field).
    assert "force_place = True" in body or "force_place is None" in body, (
        "v1.15.71: apply must default force_place to True when the "
        "field is missing — v1.15.66-70 decision payloads don't carry it"
    )


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


def test_apply_download_only_skips_place_step(app_client):
    """End-to-end: action=replace + force_place=false on a P row
    writes the user_override + enqueues a download job whose
    payload has force_place=false — so the worker downloads the
    file but doesn't push it to Plex. Plex keeps serving its
    theme until the user manually clicks PUSH."""
    client, settings = app_client
    # Seed a theme + a plex_items row marked as Plex-served.
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at,
                  first_seen_sync_at)
               VALUES ('movie', 71001, 'tt71001', 'P Row', 2020,
                       'themoviedb',
                       '2026-01-01', '2026-01-01')"""
        )
        # plex_items row so the apply endpoint has somewhere to
        # enqueue downloads against.
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, has_theme, theme_id,
                  first_seen_at, last_seen_at)
               VALUES ('rk-71001', '1', 'movie', 'P Row', '2020',
                       'tt71001', 1,
                       (SELECT id FROM themes WHERE tmdb_id = 71001),
                       '2026-01-01', '2026-01-01')"""
        )
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "movie",
            "theme_tmdb_id": 71001,
            "imdb_id_for_orphan": None,
            "imported_url": "https://www.youtube.com/watch?v=download001",
            "action": "replace",
            "force_place": False,
        }]},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["applied"] == 1
    # The enqueued download job's payload must have force_place=false.
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE job_type = 'download' "
            "  AND tmdb_id = 71001 AND status = 'pending'"
        ).fetchall()
        assert rows, (
            "v1.15.71: apply must enqueue a download even when "
            "force_place=false — that's the 'backup' part of "
            "download-only"
        )
        import json as _json
        payload = _json.loads(rows[0]["payload"])
        assert payload.get("force_place") is False, (
            "v1.15.71: download-only must enqueue with force_place "
            f"=false; got payload={payload!r}"
        )


def test_apply_default_force_place_is_true_backcompat(app_client):
    """Counter-test: v1.15.66-70 decision payloads don't carry
    force_place. The apply endpoint must default it to True so
    those payloads continue to work (push to Plex like before)."""
    client, settings = app_client
    with get_conn(settings.db_path) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO themes
                 (media_type, tmdb_id, imdb_id, title, year,
                  upstream_source, last_seen_sync_at,
                  first_seen_sync_at)
               VALUES ('movie', 71002, 'tt71002', 'Default', 2020,
                       'themoviedb',
                       '2026-01-01', '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO plex_items
                 (rating_key, section_id, media_type, title, year,
                  guid_imdb, theme_id, first_seen_at, last_seen_at)
               VALUES ('rk-71002', '1', 'movie', 'Default', '2020',
                       'tt71002',
                       (SELECT id FROM themes WHERE tmdb_id = 71002),
                       '2026-01-01', '2026-01-01')"""
        )
    resp = client.post(
        "/api/import/apply",
        json={"decisions": [{
            "theme_media_type": "movie",
            "theme_tmdb_id": 71002,
            "imported_url": "https://www.youtube.com/watch?v=defaultbck1",
            "action": "replace",
            # NO force_place field — back-compat with v1.15.66-70
        }]},
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert resp.status_code == 200
    with get_conn(settings.db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM jobs WHERE tmdb_id = 71002"
        ).fetchall()
        import json as _json
        payload = _json.loads(rows[0]["payload"])
        assert payload.get("force_place") is True, (
            "v1.15.71: when force_place is omitted from the decision, "
            "apply must default to True (the v1.15.66-70 behavior)"
        )
