"""v0.50.11 — Fix B: CLEAN UP a stale dead-rk plex_upload placement.

the user's LotR Collection (collection/119, rk 579643): Plex removed + re-added
the collection under a new rating_key, so /themes 404s (PLEX_FETCH_FAILED) and
PROBE can only ever re-404. CLEAN UP drops the stale placement (motif tracking
only — no Plex change, no canonical deletion), after the server re-verifies the
rk is genuinely gone (refuses if it came back).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
ORPHANS = (REPO / "app" / "web" / "templates" / "orphans.html").read_text()


def _endpoint() -> str:
    i = API_PY.index("async def api_admin_orphan_cleanup_dead_rk(")
    return API_PY[i:API_PY.index("@app.", i + 1)]


def test_endpoint_exists_and_admin_gated():
    assert '@app.post("/api/admin/orphan-scan/cleanup-dead-rk")' in API_PY
    assert "_require_admin(request)" in _endpoint()


def test_reverifies_rk_is_dead_before_delete():
    b = _endpoint()
    # The gate is a LIVE /themes re-check — refuse only if Plex now serves it
    # (came back). v0.50.13: the plex_items-liveness refusal was removed (a
    # stale-but-unpruned plex_items row must NOT block cleanup of a row whose
    # /themes genuinely 404s — the user's LotR rk 579643).
    assert "plex.get_themes(rating_key=str(rating_key))" in b
    assert 'resp.get("ok")' in b
    assert "FROM plex_items WHERE rating_key" not in b
    # a failed re-verification surfaces as 409 (not a silent no-op).
    assert "status_code=409" in b


def test_deletes_only_the_placement_no_plex_or_file_change():
    b = _endpoint()
    assert "DELETE FROM placements WHERE media_type = ? AND tmdb_id = ?" in b
    assert "plex_rating_key = ?" in b
    # no Plex teardown, no canonical unlink — tracking-only cleanup.
    assert "unlink" not in b
    assert "delete_collection_theme" not in b


def test_runs_off_event_loop():
    # the get_themes re-check is a blocking Plex call → must be offloaded.
    assert "await run_in_threadpool(_cleanup)" in _endpoint()


def test_ui_cleanup_button_wired_for_dead_rk_rows():
    assert 'data-act="cleanup"' in ORPHANS
    assert "// CLEAN UP" in ORPHANS
    # the button only renders for plex_fetch_failed (the dead-rk drift type).
    i = ORPHANS.index('data-act="cleanup"')
    assert "plex_fetch_failed" in ORPHANS[i - 320:i]
    # the click handler posts to the cleanup endpoint.
    assert "/api/admin/orphan-scan/cleanup-dead-rk" in ORPHANS
