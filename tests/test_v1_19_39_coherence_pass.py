"""v1.19.39 — MEDIUM coherence pass closing the post-v1.19.38 audit.

Four fixes shipped together — each is a small-but-real coherence
gap surfaced by the deep audit:

  1. **Bulk ACCEPT ALL per-row audit + HISTORY count** —
     api_accept_all_updates emitted only one summary log_event
     after the loop. Single-row api_accept_update emits
     _record_audit with `p_row_backup` per call. v1.19.39 makes
     the bulk path symmetric: per-row _record_audit + count
     of `bulk_is_p_row` rows surfaced in HISTORY + new
     `p_backup` field in the API response.

  2. **Bulk ACCEPT ALL confirm dialogs** (app.js) — claimed
     "replacing the current theme" for every row. v1.19.33's
     P-row branch means rows currently SRC=P download as a
     backup (Plex keeps serving). Rephrased for mixed selections.

  3. **PROMOTE TO ACTIVE tooltip** (app.js) — said "force-place
     your URL" but for the v1.19.35 BK-no-override path
     (synthesized override with no real user URL) there IS no
     user URL — motif force-places the TDB content it staged.
     Branches on `data.override.synthetic`.

  4. **settings.html `for="import-csv-file"` removed** — the
     v1.19.36 DESIGN_SYSTEM.md update said "no `for=`
     attribute" but settings.html still carried it; library's
     upload-dlg already complied. Doc-vs-code drift. v1.19.39
     fixes the code + flips the v1.19.25 test that actively
     pinned the violation.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient

API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS_HTML = (
    REPO / "app" / "web" / "templates" / "settings.html"
).read_text()


# ── Fix 1: bulk ACCEPT ALL audit + HISTORY count ────────────


def test_bulk_accept_all_counts_p_backup_rows():
    """The bulk-accept loop must increment p_backup_count on
    every bulk_is_p_row=True iteration so the summary message
    can report it."""
    fn_start = API_PY.index("async def api_accept_all_updates(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "p_backup_count = 0" in body, (
        "v1.19.39: bulk-accept must initialize p_backup_count "
        "before the per-section loop"
    )
    assert "p_backup_count += 1" in body, (
        "v1.19.39: bulk-accept must increment p_backup_count "
        "on each bulk_is_p_row branch"
    )


def test_bulk_accept_all_emits_per_row_audit():
    """Per-row _record_audit must fire inside the loop with
    p_row_backup + bulk=True details, matching the single-row
    api_accept_update's audit shape."""
    fn_start = API_PY.index("async def api_accept_all_updates(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "_record_audit(" in body, (
        "v1.19.39: bulk-accept must call _record_audit per row"
    )
    assert '"p_row_backup": bulk_is_p_row' in body, (
        "v1.19.39: bulk audit detail must carry the "
        "p_row_backup branch flag (matches the single-row "
        "audit shape at api_accept_update)"
    )
    assert '"bulk": True' in body, (
        "v1.19.39: bulk audit detail must mark `bulk: True` so "
        "consumers can filter bulk-only vs per-row audits"
    )


def test_bulk_accept_all_history_message_includes_p_backup_count():
    """The summary log_event must include the p_backup count
    when non-zero."""
    fn_start = API_PY.index("async def api_accept_all_updates(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # Conditional `as P-row backup` clause is the v1.19.39 marker.
    assert "as P-row backup" in body, (
        "v1.19.39: HISTORY summary must include `(N as P-row "
        "backup)` so readers can distinguish a fully force-"
        "placed bulk from one that routed P-rows to backup"
    )


def test_bulk_accept_all_response_includes_p_backup():
    """The endpoint's JSON response must include p_backup so
    the JS toast can surface it."""
    fn_start = API_PY.index("async def api_accept_all_updates(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert '"p_backup": p_backup_count' in body, (
        "v1.19.39: API response must include p_backup so the JS "
        "bulk-accept toast can surface it"
    )


# ── Fix 2: confirm dialogs rephrased ────────────────────────


def test_per_selection_bulk_confirm_acknowledges_p_row_backup():
    """The selection-mode confirm at #library-accept-all-updates-btn
    must mention SRC=P → backup behavior so users understand
    mixed-selection semantics."""
    # `library-accept-all-updates-btn` appears earlier in the
    # file at the visibility-gate site (updateLibrarySelectionUi)
    # — anchor on the addEventListener registration specifically.
    handler_idx = APP_JS.index(
        "getElementById('library-accept-all-updates-btn')?.addEventListener"
    )
    chunk = APP_JS[handler_idx:handler_idx + 4000]
    # Must reference SRC=P / Plex serving in the confirm copy.
    assert "SRC=P" in chunk or "Plex serving" in chunk, (
        "v1.19.39: per-selection bulk-accept confirm dialog "
        "must acknowledge the v1.19.33 P-row backup branch"
    )
    assert "backup" in chunk, (
        "v1.19.39: bulk-accept confirm copy must say `backup` "
        "so users understand SRC=P rows don't replace Plex"
    )


def test_global_bulk_confirm_acknowledges_p_row_backup():
    """The no-selection (global) confirm must also acknowledge
    the P-row backup branch. Has its own confirm call after the
    `// No selection — global accept-all path.` marker."""
    no_sel_marker_idx = APP_JS.index(
        "// No selection — global accept-all path."
    )
    chunk = APP_JS[no_sel_marker_idx:no_sel_marker_idx + 2000]
    assert "SRC=P" in chunk or "Plex serving" in chunk, (
        "v1.19.39: global bulk-accept confirm must acknowledge "
        "the v1.19.33 P-row backup branch"
    )


def test_bulk_accept_toast_surfaces_p_backup_count():
    """The toast after global accept-all must surface the
    res.p_backup count (v1.19.39 server response)."""
    no_sel_marker_idx = APP_JS.index(
        "// No selection — global accept-all path."
    )
    chunk = APP_JS[no_sel_marker_idx:no_sel_marker_idx + 3500]
    assert "res.p_backup" in chunk, (
        "v1.19.39: bulk-accept toast must read res.p_backup "
        "so the user sees `X ACCEPTED · N P-BACKUP`"
    )
    assert "P-BACKUP" in chunk, (
        "v1.19.39: toast label must include P-BACKUP so the "
        "count meaning is visible without context"
    )


# ── Fix 3: PROMOTE TO ACTIVE tooltip branch ─────────────────


def test_promote_tooltip_branches_on_synthetic_override():
    """The PROMOTE TO ACTIVE tooltip must branch on
    `data.override.synthetic` so the "your URL" copy doesn't
    fire on the v1.19.35 BK-no-override path."""
    # Find the promote-to-active button render block.
    idx = APP_JS.index("data-act=\"promote-to-active\"")
    # Walk back to find the encompassing if-branch.
    block_start = APP_JS.rfind(
        "if (overrideIntent === 'backup')", 0, idx,
    )
    block = APP_JS[block_start:idx + 800]
    assert "data.override.synthetic" in block, (
        "v1.19.39: PROMOTE TO ACTIVE tooltip must check "
        "data.override.synthetic"
    )
    assert "force-place its downloaded copy" in block, (
        "v1.19.39: synthetic-override tooltip must say `its "
        "downloaded copy` (not `your URL`) — there's no user "
        "URL when the override is synthesized"
    )
    # The non-synthetic branch (real override) keeps "your URL".
    assert "force-place your URL over" in block, (
        "v1.19.39: real-override branch must keep the v1.18.77 "
        "`your URL` wording — only the synthetic branch differs"
    )


# ── Fix 4: settings label `for=` removed ────────────────────


def test_settings_csv_label_drops_for_attribute():
    """The settings CSV-import <label> must NOT carry
    `for="import-csv-file"` — matches the v1.19.36
    DESIGN_SYSTEM.md update + the sibling library upload-dlg."""
    # The input itself must still have id=import-csv-file (the
    # JS picks it by id), but the label wrapping it must not
    # carry the for= attribute.
    idx = SETTINGS_HTML.index('id="import-csv-file"')
    backwards = SETTINGS_HTML[max(0, idx - 400):idx]
    assert 'for="import-csv-file"' not in backwards, (
        "v1.19.39: settings.html label must NOT carry "
        "`for=import-csv-file` — DESIGN_SYSTEM.md § 2 says "
        "input nested as direct child of label, no `for=`"
    )
    # The wrapping label still must be present (the wrap-inside
    # convention is the post-v1.19.25 shape). v1.22.55 settings
    # redesign moved the wrapper class form-label → field-row (hybrid
    # two-column field); accept either — the invariant is that the
    # input stays nested inside a <label>.
    assert ('<label class="field-row"' in backwards
            or '<label class="form-label"' in backwards), (
        "v1.19.39 / v1.22.55: regression — the wrapping <label> "
        "(field-row or form-label) must remain so the import-csv-file "
        "input is still nested inside it"
    )


# ── End-to-end behavioral (bulk ACCEPT ALL response shape) ──


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
    return TestClient(create_app(settings)), settings


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_p_with_pending(conn, *, tmdb_id=100):
    """P-row + U-backup + pending_update (mirrors the v1.19.33
    bulk-accept-protects-p-rows test seed)."""
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,"
        "        '2026-05-26T00:00:00','2026-05-26T00:00:00')"
    )
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (?, 'movie', ?, 'X', 'imdb', "
        "        '2026-05-26', '2026-05-26', 'https://yt/orig')",
        (tmdb_id, tmdb_id),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, 'tt100', ?, 'X', 2020, "
        "        1, 0, '/data/movies/X', 0, 1, "
        "        '2026-05-26', '2026-05-26')",
        (f"rk-{tmdb_id}", tmdb_id, tmdb_id),
    )
    conn.execute(
        "INSERT INTO user_overrides "
        "  (media_type, tmdb_id, section_id, youtube_url, "
        "   set_at, set_by) "
        "VALUES ('movie', ?, '1', 'https://yt/usr', "
        "        '2026-05-26', 'admin')",
        (tmdb_id,),
    )
    conn.execute(
        "INSERT INTO pending_updates "
        "  (media_type, tmdb_id, section_id, "
        "   new_video_id, new_youtube_url, old_youtube_url, "
        "   detected_at, decision, kind) "
        # v1.22.62: old_youtube_url added so the pending is a REAL
        # old→new diff — i.e. a row the UI actually shows. Pre-fix the
        # url-less seed only passed accept-all because the actionable
        # gate was missing there (the audit bug); with the gate
        # restored, an url-less upstream_changed is correctly hidden
        # everywhere, count included.
        "VALUES ('movie', ?, '1', 'NEW', "
        "        'https://yt/new', 'https://yt/orig', "
        "        '2026-05-26', 'pending', 'upstream_changed')",
        (tmdb_id,),
    )


def test_bulk_accept_response_carries_p_backup_count(
    admin_client, tmp_path,
):
    """End-to-end: POST /api/updates/accept-all on a P-row
    selection returns p_backup count + writes per-row audit
    rows with p_row_backup=True."""
    client, settings = admin_client
    db = settings.db_path
    with sqlite3.connect(db) as conn:
        _seed_p_with_pending(conn, tmdb_id=100)
        conn.commit()

    r = client.post("/api/updates/accept-all", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("accepted") == 1, body
    assert body.get("p_backup") == 1, (
        f"v1.19.39: response must include p_backup count; "
        f"got {body}"
    )

    # Verify per-row audit landed with p_row_backup=True.
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT details FROM audit_events "
            "WHERE action='accept_update' AND tmdb_id=100"
        ).fetchall()
    assert len(rows) == 1, (
        f"v1.19.39: bulk-accept must emit exactly one audit "
        f"row per accepted update; got {len(rows)}"
    )
    detail = json.loads(rows[0][0])
    assert detail.get("p_row_backup") is True, (
        f"v1.19.39: audit detail must record p_row_backup=True "
        f"for P-row branch; got {detail}"
    )
    assert detail.get("bulk") is True, (
        f"v1.19.39: bulk-accept audit detail must mark "
        f"`bulk: True`; got {detail}"
    )
