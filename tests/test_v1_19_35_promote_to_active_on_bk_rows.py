"""v1.19.35 — PROMOTE TO ACTIVE on BK rows + v1.18.77 phantom button fix.

Two findings from the post-v1.19.34 audit:

  1. **PROMOTE TO ACTIVE unreachable on post-v1.19.32/.33 BK rows**
     (audit finding #8). After ACCEPT UPDATE / REVERT on a P-row,
     the row sits in BK mode: local_files row with
     last_place_attempt_reason='backup_only', no placement,
     failure_kind=None. `api_recovery_options` hit its no-failure
     early-return → options=[] → the JS removed the recovery
     section entirely → the v1.18.77 BACKUP READY banner +
     PROMOTE TO ACTIVE button never rendered. User had to use the
     row's PLACE menu (PUSH TO PLEX) instead — works but
     inconsistent with the v1.18.77 banner UX promised for backup
     rows.

  2. **v1.18.77 PROMOTE/MARK buttons silently broken (phantom-fix
     sub-pattern strikes again).** The JS read
     `data.theme?.media_type` to populate the buttons' data-mt /
     data-id. But `api_recovery_options` NEVER returned a `theme`
     field — `data.theme` was always `undefined`, so the buttons
     rendered with `data-mt="" data-id=""`. Clicks hit
     `/api/items//<empty>/intent` and 422'd silently. v1.18.81
     fixed the adjacent `override` passthrough but missed this
     sibling silent bug because its tests checked override-shape
     not behavioral click → flip.

## Fix

  - `api_recovery_options`:
      • No-failure branch: detect BK state via
        `local_files.last_place_attempt_reason='backup_only'`
        + no placement; synthesize `override.intent='backup'`
        (or carry through a real one if present, overriding intent)
        + `backup_state=true` + `theme={mt, tmdb}`. Without this
        the JS branches for the v1.18.77 banner never fire.
      • Failure branch: also add `theme` field so the same
        phantom bug is closed for failure-bearing rows.
  - `api_set_override_intent`:
      • New no-override-but-BK branch: when new_intent='replace'
        + no user_overrides + a local_files row with
        last_place_attempt_reason='backup_only' exists, skip the
        intent-flip (nothing to flip) but still enqueue a
        force-place job. Audit row records the branch.
      • MARK AS BACKUP (new_intent='backup' without override)
        keeps the v1.18.77 409 — no override to mark.
  - `api_revert_to_themerrdb`: P-row branch's user_overrides
    re-INSERT now writes `intent='backup'` so the restored row's
    state matches reality (BK mode). Pre-fix the row's schema
    default ('replace') lied → MARK AS BACKUP button rendered
    instead of PROMOTE TO ACTIVE.
  - `app.js hydrateRecoveryOptions`:
      • Loosen the empty-options early-removal — keep the section
        visible when `data.backup_state` OR
        `override.intent==='backup'`.
      • Use function parameters `mediaType`/`tmdbId` as the
        canonical source for the PROMOTE/MARK buttons' data-mt /
        data-id (with the v1.19.35 `data.theme` passthrough as a
        redundant secondary source). Fixes the phantom-undefined
        bug that broke the v1.18.77 cascade.
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

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── Source-text guards ──────────────────────────────────────


def test_recovery_options_no_failure_branch_surfaces_theme_and_backup_state():
    """The `if not kind:` early return must include `theme`,
    `override`, and `backup_state` in the response shape."""
    fn_start = API_PY.index("async def api_recovery_options(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # Slice the no-failure branch (between `if not kind:` and the
    # next `return`).
    nofail_start = body.index("if not kind:")
    nofail_end = body.index("# Static recipe per FailureKind", nofail_start)
    chunk = body[nofail_start:nofail_end]
    assert '"backup_state": is_bk_state' in chunk, (
        "v1.19.35: no-failure branch must surface backup_state "
        "(true when last_place_attempt_reason='backup_only' + "
        "no placement)"
    )
    assert '"override": bk_override' in chunk, (
        "v1.19.35: no-failure branch must surface override "
        "(synthesized to {intent:'backup', ...} when BK state)"
    )
    assert '"theme": {' in chunk, (
        "v1.19.35: no-failure branch must include theme (mt, tmdb) "
        "so the JS PROMOTE button's data-mt / data-id work"
    )


def test_recovery_options_failure_branch_also_has_theme():
    """The failure-bearing branch must also surface `theme` so
    the v1.18.77 PROMOTE/MARK buttons work on failure-acked
    BACKUP rows too. Fixes the long-running v1.18.77 silent bug."""
    fn_start = API_PY.index("async def api_recovery_options(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    # The final `return {` at the bottom of the function.
    return_idx = body.rindex("return {")
    chunk = body[return_idx:]
    assert '"theme": {' in chunk, (
        "v1.19.35: failure-bearing recovery-options response must "
        "also include theme so the data.theme JS read works "
        "(fixes the v1.18.77 phantom-undefined bug surfaced by "
        "the v1.19.35 audit)"
    )


def test_intent_endpoint_handles_no_override_bk_state():
    """`api_set_override_intent` must accept new_intent='replace'
    on a BK-state row even without a user_overrides row.
    pre-v1.19.35 it 409'd unconditionally; now: BK + replace →
    force-place + audit."""
    fn_start = API_PY.index("async def api_set_override_intent(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "promote_bk_no_override" in body, (
        "v1.19.35: intent endpoint must enqueue a force-place job "
        "with reason='promote_bk_no_override' on the BK-state branch"
    )
    assert '"bk_no_override": True' in body, (
        "v1.19.35: intent endpoint response must signal the BK "
        "branch was taken (bk_no_override=True)"
    )
    # MARK AS BACKUP without override keeps the legacy 409. The
    # detail message is split across source lines (multi-line
    # string literal); check for the meaningful fragment + the
    # gate.
    assert 'new_intent != "replace"' in body, (
        "v1.19.35: BK branch must be replace-only — MARK AS BACKUP "
        "without override hits the 409 path"
    )
    assert "MARK AS BACKUP" in body, (
        "v1.19.35: 409 detail must mention MARK AS BACKUP so the "
        "user knows what to fix"
    )


def test_revert_p_row_restores_user_overrides_with_backup_intent():
    """REVERT P-row to user-kind must INSERT user_overrides with
    intent='backup'. Pre-fix the schema default 'replace' put the
    row in a state where MARK AS BACKUP shows on the recovery
    card instead of the correct PROMOTE TO ACTIVE."""
    fn_start = API_PY.index("async def api_revert_to_themerrdb(")
    fn_end = API_PY.index("@app.get", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "restored_intent" in body, (
        "v1.19.35: REVERT must compute restored_intent based on "
        "the row's current P-state"
    )
    assert "_is_p_row_for_section(" in body
    # The INSERT must reference the intent column.
    assert "intent" in body and "excluded.intent" in body, (
        "v1.19.35: REVERT INSERT must write user_overrides.intent "
        "AND propagate it through the ON CONFLICT UPDATE"
    )


def test_js_keeps_section_visible_on_backup_state():
    """`hydrateRecoveryOptions`'s empty-options early-removal
    must NOT fire when the row is in BK state — otherwise the
    v1.18.77 banner + PROMOTE TO ACTIVE button never render
    (the user's audit #8 symptom)."""
    fn_start = APP_JS.index("async function hydrateRecoveryOptions(")
    fn_end = APP_JS.index("\n  }\n", fn_start)
    body = APP_JS[fn_start:fn_end]
    # The guard must check backup_state OR override.intent='backup'.
    assert "data.backup_state" in body, (
        "v1.19.35: JS must read data.backup_state (the v1.19.35 "
        "synthesized BK-state hint)"
    )
    assert "isBkState" in body or "is_bk_state" in body, (
        "v1.19.35: JS must compute an isBkState predicate before "
        "the empty-options early-removal"
    )


def test_js_promote_button_uses_function_parameter_fallback():
    """The PROMOTE/MARK button data-mt/data-id must fall back to
    the `mediaType` / `tmdbId` function parameters (or the new
    `data.theme` passthrough), NOT just the bare `data.theme`
    chain that was always undefined pre-v1.19.35."""
    fn_start = APP_JS.index("async function hydrateRecoveryOptions(")
    fn_end = APP_JS.index("\n  }\n", fn_start)
    body = APP_JS[fn_start:fn_end]
    # The new shape uses `mediaType` / `tmdbId` as a fallback OR
    # the v1.19.35 `data.theme` passthrough.
    assert "mediaType ||" in body, (
        "v1.19.35: button data-mt fallback must include the "
        "function parameter `mediaType` (the canonical source — "
        "data.theme was the phantom-undefined chain pre-fix)"
    )
    assert "tmdbId" in body, (
        "v1.19.35: button data-id fallback must use the function "
        "parameter `tmdbId`"
    )


# ── End-to-end behavioral ────────────────────────────────────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def _seed_p_bk_row(conn, *, tmdb_id=100, with_override=False,
                   override_intent="replace"):
    """Seed a P-row in BK state: has_theme=1, verified=1, no
    placement, local_files row with backup_only stamp."""
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
        "        '2026-05-26T00:00:00', '2026-05-26T00:00:00', "
        "        'https://yt/orig')",
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
        "INSERT INTO local_files "
        "  (media_type, tmdb_id, section_id, file_path, "
        "   source_kind, source_video_id, downloaded_at, "
        "   provenance, last_place_attempt_reason) "
        "VALUES ('movie', ?, '1', 'x.mp3', 'themerrdb', "
        "        'newvid', '2026-05-26', 'auto', "
        "        'backup_only')",
        (tmdb_id,),
    )
    if with_override:
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by, intent) "
            "VALUES ('movie', ?, '1', 'https://yt/usr', "
            "        '2026-05-26', 'admin', ?)",
            (tmdb_id, override_intent),
        )


def test_recovery_options_surfaces_backup_state_on_post_accept_row(
    admin_client, tmp_path,
):
    """The v1.19.32 ACCEPT UPDATE P-row shape: failure cleared,
    no user_overrides, local_files with backup_only. The API
    must surface backup_state=true + override.intent='backup'
    (synthesized) + theme dict so the JS can render the
    v1.18.77 banner + PROMOTE TO ACTIVE button."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_p_bk_row(conn, tmdb_id=100, with_override=False)
        conn.commit()
    r = admin_client.get(
        "/api/items/movie/100/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    d = r.json()
    assert d.get("failure_kind") is None
    assert d.get("backup_state") is True, (
        f"v1.19.35: BK-state row must surface backup_state=true; "
        f"got {d}"
    )
    ovr = d.get("override")
    assert ovr is not None and ovr.get("intent") == "backup", (
        f"v1.19.35: override must be synthesized with "
        f"intent='backup'; got {ovr}"
    )
    assert ovr.get("synthetic") is True, (
        "v1.19.35: synthesized override must carry synthetic=True "
        "so consumers can tell it apart from a real one"
    )
    th = d.get("theme")
    assert th == {"media_type": "movie", "tmdb_id": 100}, (
        f"v1.19.35: theme passthrough must carry mt + tmdb; got {th}"
    )


def test_recovery_options_passes_through_real_override_with_intent_override(
    admin_client, tmp_path,
):
    """The v1.19.33 REVERT P-row shape: failure cleared, real
    user_overrides exists with intent='backup' (per v1.19.35
    REVERT INSERT fix), local_files with backup_only. API
    must surface the real override but flip intent to 'backup'
    in case it's stale (defense in depth)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Seed with override.intent='replace' to test the override.
        _seed_p_bk_row(
            conn, tmdb_id=200, with_override=True,
            override_intent="replace",
        )
        conn.commit()
    r = admin_client.get(
        "/api/items/movie/200/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200
    d = r.json()
    assert d.get("backup_state") is True
    ovr = d.get("override")
    # Real override fields carried through.
    assert ovr.get("youtube_url") == "https://yt/usr", (
        f"v1.19.35: real override URL must carry through; got {ovr}"
    )
    # Intent forced to 'backup' (the row is in BK state).
    assert ovr.get("intent") == "backup", (
        f"v1.19.35: BK-state must force intent='backup' on the "
        f"surfaced override even if the DB row says 'replace' — "
        f"the local_files signal is the canonical truth"
    )


def test_intent_endpoint_promotes_bk_no_override_row(
    admin_client, tmp_path,
):
    """POST /api/items/movie/100/intent {intent:'replace'} on a
    BK-state row with NO user_overrides must enqueue a
    force-place job + return bk_no_override=True (instead of
    the legacy 409)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_p_bk_row(conn, tmdb_id=300, with_override=False)
        conn.commit()
    r = admin_client.post(
        "/api/items/movie/300/intent?section_id=1",
        headers=AUTH,
        json={"intent": "replace"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("ok") is True
    assert body.get("bk_no_override") is True, (
        f"v1.19.35: BK + no-override + replace must return "
        f"bk_no_override=True; got {body}"
    )
    # A force-place job must have been enqueued.
    with sqlite3.connect(db) as conn:
        place_jobs = conn.execute(
            "SELECT payload FROM jobs "
            "WHERE job_type = 'place' AND media_type = 'movie' "
            "  AND tmdb_id = 300"
        ).fetchall()
    assert len(place_jobs) == 1, (
        f"v1.19.35: exactly one place job expected; got {place_jobs}"
    )
    payload = json.loads(place_jobs[0][0])
    assert payload.get("force_place") is True
    assert payload.get("reason") == "promote_bk_no_override"


def test_intent_endpoint_409s_mark_as_backup_without_override(
    admin_client, tmp_path,
):
    """Counter-guard: MARK AS BACKUP without an override row must
    still 409 — the v1.19.35 BK branch is replace-direction-only."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_p_bk_row(conn, tmdb_id=400, with_override=False)
        conn.commit()
    r = admin_client.post(
        "/api/items/movie/400/intent?section_id=1",
        headers=AUTH,
        json={"intent": "backup"},
    )
    assert r.status_code == 409, (
        f"v1.19.35: MARK AS BACKUP without override must 409; "
        f"got {r.status_code} {r.text}"
    )


def test_intent_endpoint_409s_no_override_no_bk_state(
    admin_client, tmp_path,
):
    """Counter-guard: no override AND no BK state → 409. The
    v1.19.35 BK branch only fires when there's something to
    promote."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Seed plex_items but NO local_files with backup_only.
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
            "VALUES (5, 'movie', 500, 'X', 'imdb', "
            "        '2026-05-26', '2026-05-26', 'https://yt/x')"
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-500', '1', 'movie', 5, 'tt500', 500, "
            "        'X', 2020, 1, 0, '/f', 0, 1, "
            "        '2026-05-26', '2026-05-26')"
        )
        conn.commit()
    r = admin_client.post(
        "/api/items/movie/500/intent?section_id=1",
        headers=AUTH,
        json={"intent": "replace"},
    )
    assert r.status_code == 409, (
        f"v1.19.35: no override AND no BK state must 409; "
        f"got {r.status_code} {r.text}"
    )


def test_revert_on_p_row_restores_override_with_backup_intent(
    admin_client, tmp_path,
):
    """End-to-end: REVERT on a P-row with prev_kind='user' must
    INSERT user_overrides with intent='backup' (matching the
    actual state — BK mode, Plex serving)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_p_bk_row(conn, tmdb_id=600, with_override=False)
        # Seed a previous_urls snapshot of kind='user' so REVERT
        # restores an override.
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('movie', 600, '1', 'https://yt/prev', "
            "        'user', '2026-05-26T00:00:00')"
        )
        conn.commit()
    r = admin_client.post(
        "/api/items/movie/600/revert?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    with sqlite3.connect(db) as conn:
        ovr = conn.execute(
            "SELECT youtube_url, intent FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=600 "
            "  AND section_id='1'"
        ).fetchone()
    assert ovr is not None, (
        "v1.19.35: REVERT user-kind must INSERT user_overrides"
    )
    assert ovr[0] == "https://yt/prev"
    assert ovr[1] == "backup", (
        f"v1.19.35: REVERT on a P-row must set intent='backup' "
        f"(NOT the schema default 'replace'); got intent={ovr[1]}"
    )
