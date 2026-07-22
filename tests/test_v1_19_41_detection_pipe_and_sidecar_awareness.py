"""v1.19.41 — detection-pipe + sidecar awareness (four-way notification split).

Ships the notification-pipe foundation for cloud-themes-backup
(v1.19.42). Without this tag, the cloud-backup feature is
decorative: when Plex eventually removes a row with a plex_cloud
backup, the v1.18.90 reaper's `has_fallback: continue` would
silently swallow the signal.

## Background

the user's 2026-05-26 American Psycho event proved the v1.18.90
reaper path is the real-world workhorse for theme-loss
notifications (v1.18.79 has never fired). But v1.18.90's gate
was a single boolean — `has_fallback` either skips silently
OR fires the "no fallback" notification. Three new motivating
cases:

  - Backup-ready (intent='backup' OR source_kind='plex_cloud'):
    the row HAS a fallback motif owns, but the user needs to
    deploy it. Today: silent skip. v1.19.41: fire
    `theme_lost_backup_ready` with PROMOTE TO ACTIVE CTA.
  - Sidecar-available (local_theme_file=1 OR theme.mp3 on disk):
    a sidecar exists at the Plex folder. Theme keeps playing as
    M even without action. Today: silent skip. v1.19.41: fire
    `theme_lost_sidecar_available` with ADOPT CTA.
  - Other fallback (replace-intent override / placement /
    non-backup local_file): row has working theme. Still silent
    (unchanged from v1.18.90).
  - No fallback: existing message, retuned to drop "no local
    file" line (cloud-backup rows WILL have one).

## Scope

  1. Four-way tier split in v1.18.90 reaper dispatch
     (`plex_enum.py:1518+`).
  2. Capture `folder_path` in lost-candidate payload for the
     filesystem fallback check.
  3. Filesystem check (Option B): `Path(folder_path)/'theme.mp3'`
     exists is a Tier-2 discriminator.
  4. New body formatters: `format_theme_lost_backup_ready_body`,
     `format_theme_lost_sidecar_available_body`.
  5. Retuned `format_plex_theme_lost_body` (Path 4 — drops the
     "no local file" misleading line).
  6. v1.18.79 Bug A (TVDB-bridge gap) — widen `guid_tmdb` gate to fall
     back to `theme_id` linkage when `guid_tmdb IS NULL`.
  7. v1.18.79 Bug C (transition INFO breadcrumb) — log BEFORE
     any gate so operators have signal for ALL has_theme: 1→0
     transitions.
  8. Silent-fail downgrade flags
     (`_THEME_LOST_NOTIFY_WARNED`, `_BACKUP_READY_NOTIFY_WARNED`)
     — first-occurrence WARN, subsequent debug. Class-9 hot-path
     sub-pattern.
  9. `POST /api/admin/test-trigger-theme-lost` endpoint with
     `force_state` query param (`backup` / `sidecar` / `none` /
     `auto`). Bypasses 24h dedupe for repeatable validation.
 10. Probe results copy-to-clipboard button.

## Precedence ladder (tiered alternatives)

| Tier | Signal | Primary CTA |
|---|---|---|
| 1 | intent='backup' OR source_kind='plex_cloud' | PROMOTE TO ACTIVE |
| 2 | local_theme_file=1 OR theme.mp3 on disk | ADOPT |
| 3 | other fallback | silent skip |
| 4 | none | SET URL / UPLOAD MP3 |

Highest present tier wins primary CTA. Sidecar-also-present
surfaces as an "Alternative:" line in the tier-1 body.
"""
from __future__ import annotations

from _slice_helpers import slice_to_next

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402

PLEX_ENUM_PY = (
    REPO / "app" / "core" / "plex_enum.py"
).read_text()
NOTIFY_CONTENT_PY = (
    REPO / "app" / "core" / "notify_content.py"
).read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
SETTINGS_HTML = (
    REPO / "app" / "web" / "templates" / "settings.html"
).read_text()


# ── Body formatters (notify_content.py) ──────────────────────


def test_format_theme_lost_backup_ready_body_user_url_variant():
    """Backup-source 'user_url_backup' must produce a body that
    references KEEP AS BACKUP. Phrasing branches on the source."""
    from app.core import notify_content as nc
    ctx = {"display_title": "American Psycho (2000)"}
    body = nc.format_theme_lost_backup_ready_body(
        ctx, backup_source="user_url_backup",
    )
    # v1.19.92: the title now carries the display_title; the body no
    # longer repeats it. Body leads with the lost-state line instead.
    assert "no longer available" in body
    assert "KEEP AS BACKUP" in body, (
        "v1.19.41: user_url_backup variant must reference the "
        "KEEP AS BACKUP user action so the operator recognizes "
        "what's staged"
    )
    assert "// PROMOTE TO ACTIVE" in body, (
        "v1.19.41: primary CTA must be PROMOTE TO ACTIVE"
    )
    # Without sidecar_present, the alternative line should NOT
    # appear.
    assert "// ADOPT" not in body


def test_format_theme_lost_backup_ready_body_plex_cloud_variant():
    """Backup-source 'plex_cloud_backup' must produce body that
    references the cloud-theme local copy (no "KEEP AS BACKUP"
    phrasing — that's a user action, not motif's automated
    cloud backup)."""
    from app.core import notify_content as nc
    ctx = {"display_title": "Willy Wonka (1971)"}
    body = nc.format_theme_lost_backup_ready_body(
        ctx, backup_source="plex_cloud_backup",
    )
    assert "Plex's cloud theme" in body, (
        "v1.19.41: plex_cloud_backup variant must reference "
        "Plex's cloud theme so the user understands the source"
    )
    assert "KEEP AS BACKUP" not in body, (
        "v1.19.41: plex_cloud variant must NOT mention KEEP AS "
        "BACKUP — that's a user-explicit action, not motif's "
        "automated backup"
    )


def test_format_theme_lost_backup_ready_body_sidecar_alternative():
    """When sidecar_present=True, the body must include an "
    "Alternative" line mentioning ADOPT."""
    from app.core import notify_content as nc
    ctx = {"display_title": "Test"}
    body = nc.format_theme_lost_backup_ready_body(
        ctx,
        backup_source="user_url_backup",
        sidecar_present=True,
    )
    assert "Alternative" in body
    assert "// ADOPT" in body, (
        "v1.19.41: sidecar_present=True must surface ADOPT as "
        "alternative — primary CTA stays PROMOTE TO ACTIVE"
    )
    # PROMOTE still primary, in earlier text position.
    promote_idx = body.index("// PROMOTE TO ACTIVE")
    adopt_idx = body.index("// ADOPT")
    assert promote_idx < adopt_idx, (
        "v1.19.41: PROMOTE TO ACTIVE must precede ADOPT (tier 1 "
        "primary, tier 2 alternative)"
    )


def test_format_theme_lost_sidecar_available_body():
    """Sidecar-available body must lead with ADOPT, mention
    SET URL / UPLOAD MP3 as alternatives, and call out the
    sidecar's unknown provenance."""
    from app.core import notify_content as nc
    ctx = {"display_title": "American Psycho (2000)"}
    body = nc.format_theme_lost_sidecar_available_body(ctx)
    assert "// ADOPT" in body, (
        "v1.19.41: sidecar-available body must surface ADOPT "
        "as primary CTA"
    )
    assert "// SET URL" in body and "// UPLOAD MP3" in body, (
        "v1.19.41: sidecar-available body must mention both "
        "alternatives so users have a replace path too"
    )
    assert "unverified" in body or "unknown" in body, (
        "v1.19.41: body must call out the sidecar's unknown "
        "provenance so users know what ADOPT actually does"
    )
    assert "no urgency" in body or "even without action" in body, (
        "v1.19.41: tonally less urgent than no-fallback — the "
        "theme keeps playing as M"
    )


def test_format_plex_theme_lost_body_drops_misleading_line():
    """v1.19.41 retunes the existing body to drop the "no
    user-provided URL, no uploaded MP3, no local file" line —
    that line becomes misleading when cloud-backup rows have a
    local file (v1.19.42)."""
    from app.core import notify_content as nc
    ctx = {"display_title": "Test"}
    body = nc.format_plex_theme_lost_body(ctx)
    assert "no user-provided URL" not in body, (
        "v1.19.41: pre-fix copy must be gone — `no local file` "
        "becomes misleading once cloud-backup ships"
    )
    assert "no backup configured" in body, (
        "v1.19.41: new copy says `no backup configured` which "
        "stays accurate across all backup-source types"
    )
    # Existing CTAs preserved.
    assert "// SET URL" in body
    assert "// UPLOAD MP3" in body


# ── Reaper dispatch loop tier classifier (plex_enum.py) ─────


def test_reaper_captures_folder_path_in_lost_candidates():
    """The reaper SELECT must capture `pi.folder_path` so the
    dispatch loop's filesystem fallback check (Tier 2) has the
    path to query."""
    # Slice the v1.18.90 candidate SELECT.
    idx = PLEX_ENUM_PY.index("lost_candidates_raw = conn.execute(")
    chunk = PLEX_ENUM_PY[idx:idx + 1500]
    assert "pi.folder_path" in chunk, (
        "v1.19.41: reaper must capture pi.folder_path for the "
        "filesystem fallback check"
    )
    # And include it in the candidate row alias.
    assert "AS folder_path" in chunk


def test_tier_classifier_signal_queries_present():
    """The tier classifier inside the reaper dispatch loop must
    issue THREE separate signal queries: backup_signal,
    sidecar_db, other_fallback."""
    # Anchor on the v1.19.41 four-way classifier comment block.
    idx = PLEX_ENUM_PY.index("Tier 1: backup-ready")
    # v1.19.61: widened from 6000→8000 to absorb the third
    # UNION ALL clause in backup_signal.
    # v1.22.39: widened 8000→9600 — sidecar_db grew to the LEFT-JOIN-themes +
    # theme_id linkage form (anime survivor fix), pushing other_fallback later.
    # v0.51.14: widened 9600→12000 — the bounded tier-2 fs check (audit #7)
    # sits between sidecar_db and other_fallback.
    chunk = PLEX_ENUM_PY[idx:idx + 12000]
    assert "backup_signal = conn.execute(" in chunk, (
        "v1.19.41: classifier must have a backup_signal query "
        "(user_overrides.intent='backup' OR source_kind='plex_cloud')"
    )
    assert "sidecar_db = conn.execute(" in chunk, (
        "v1.19.41: classifier must have a sidecar_db query "
        "(plex_items.local_theme_file=1 on same tmdb)"
    )
    assert "other_fallback = conn.execute(" in chunk, (
        "v1.19.41: classifier must have an other_fallback query "
        "(retained from pre-fix has_fallback semantics)"
    )


def test_tier_classifier_filesystem_check_present():
    """Tier-2 filesystem fallback (Option B): when the row's folder_path has a
    theme sidecar on disk, sidecar_fs is True. v1.22.15 routed the check
    through _candidate_local_paths (host→container); v1.22.72 routed it
    through find_theme_sidecar_path, which does that translation AND accepts
    the full SIDECAR_AUDIO_EXTS set (the hardcoded theme.mp3 mis-tiered a
    manual theme.flac to no_fallback)."""
    # v0.51.14 (audit #7): the check is now a BOUNDED executor submit (the call
    # ran inside the reap's BEGIN IMMEDIATE txn; a stalled /data mount held the
    # writer lock indefinitely) — the pin follows the submit form + deadline.
    chunk = slice_to_next(PLEX_ENUM_PY, "sidecar_fs = False",
                         "sidecar_present = bool(sidecar_db)")
    assert "find_theme_sidecar_path, _folder_path" in chunk, (
        "v1.22.15/v1.22.72: filesystem fallback must translate "
        "host→container + accept all sidecar extensions"
    )
    assert "timeout=_SIDECAR_STALL_TIMEOUT_S" in chunk, (
        "v0.51.14: the in-txn fs check must be deadline-bounded"
    )
    assert "_ex.shutdown(wait=False)" in chunk, (
        "v0.51.14: never join a possibly-hung fs thread (v1.22.65 rule)"
    )


def test_tier_classifier_other_fallback_excludes_double_counting():
    """The other_fallback query must EXCLUDE rows that would
    already trigger Tier 1 (intent='backup' overrides OR
    source_kind='plex_cloud' local_files). Without exclusion,
    a backup-ready row would also match Tier 3 and route through
    the wrong silent-skip path."""
    idx = PLEX_ENUM_PY.index("other_fallback = conn.execute(")
    chunk = PLEX_ENUM_PY[idx:idx + 1200]
    assert "!= 'plex_cloud'" in chunk, (
        "v1.19.41: other_fallback must exclude plex_cloud "
        "local_files (those belong to Tier 1, not Tier 3)"
    )
    assert "!= 'backup'" in chunk, (
        "v1.19.41: other_fallback must exclude intent='backup' "
        "overrides (those belong to Tier 1, not Tier 3)"
    )


def test_dispatch_loop_routes_by_tier():
    """The dispatch loop must branch on the tier field, calling
    the matching body formatter + event_kind for each path."""
    idx = PLEX_ENUM_PY.index('if tier == "backup_ready":')
    chunk = PLEX_ENUM_PY[idx:idx + 3000]
    assert "theme_lost_backup_ready" in chunk
    assert "theme_lost_sidecar_available" in chunk
    assert "plex_theme_lost" in chunk
    # All three event_kind values present in the dispatch
    # routing.
    assert "format_theme_lost_backup_ready_body" in chunk
    assert "format_theme_lost_sidecar_available_body" in chunk
    assert "format_plex_theme_lost_body" in chunk


def test_dispatch_dedupe_key_includes_tier():
    """Dedupe key must include the tier so a row that flips
    tiers within the 24h window still gets a notification (e.g.
    user adds KEEP AS BACKUP after initial no-fallback fire)."""
    idx = PLEX_ENUM_PY.index('dedupe_key = f"plex_theme_lost:')
    chunk = PLEX_ENUM_PY[idx:idx + 200]
    assert "{tier}" in chunk, (
        "v1.19.41: dedupe key must include {tier} so tier "
        "flips trigger fresh notifications inside the 24h window"
    )


# ── v1.18.79 detection: Bug A + Bug C ────────────────────────


def test_v1_18_79_tvdb_gap_fix():
    """Bug A: when guid_tmdb is NULL, fall back to theme_id
    linkage to resolve the row's tmdb_id. Mirror the
    v1.15.142 / v1.19.33 widening pattern."""
    idx = PLEX_ENUM_PY.index("v1.19.41 Bug A fix")
    chunk = PLEX_ENUM_PY[idx:idx + 1500]
    assert "existing[\"theme_id\"]" in chunk, (
        "v1.19.41: Bug A fix must read theme_id from the "
        "existing plex_items SELECT"
    )
    assert "SELECT tmdb_id FROM themes" in chunk, (
        "v1.19.41: fallback must resolve tmdb_id via "
        "themes table lookup using theme_id"
    )


def test_existing_select_includes_theme_id():
    """The plex_items SELECT in _upsert_items must include
    theme_id so the v1.18.79 Bug A fallback has data."""
    idx = PLEX_ENUM_PY.index(
        "SELECT plex_theme_uri, has_theme, guid_tmdb, "
    )
    chunk = PLEX_ENUM_PY[idx:idx + 300]
    assert "theme_id" in chunk, (
        "v1.19.41: existing SELECT must include theme_id "
        "for the Bug A fallback to work"
    )


def test_v1_18_79_transition_info_breadcrumb():
    """Bug C: log.info breadcrumb at the transition itself,
    BEFORE any gate (so ALL has_theme: 1→0 events surface in
    logs, not just the backup-intent subset)."""
    idx = PLEX_ENUM_PY.index("theme_transition:")
    chunk = PLEX_ENUM_PY[max(0, idx - 400):idx + 400]
    assert "log.info" in chunk, (
        "v1.19.41: transition breadcrumb must be log.info (not "
        "log.warning — warning is reserved for actionable events)"
    )
    # Must fire INSIDE the prior_has_theme AND not it.has_theme
    # block but BEFORE the if _row_tmdb is not None: gate.
    assert "has_theme 1→0 detected" in chunk


# ── Silent-fail downgrade flags ──────────────────────────────


def test_silent_fail_downgrade_flags_module_scope():
    """Two module-level flags govern first-occurrence WARN /
    subsequent DEBUG for the two dispatch paths."""
    assert (
        "_THEME_LOST_NOTIFY_WARNED: bool = False" in PLEX_ENUM_PY
    ), (
        "v1.19.41: _THEME_LOST_NOTIFY_WARNED flag missing"
    )
    assert (
        "_BACKUP_READY_NOTIFY_WARNED: bool = False"
        in PLEX_ENUM_PY
    ), (
        "v1.19.41: _BACKUP_READY_NOTIFY_WARNED flag missing"
    )


def test_silent_fail_downgrade_used_in_except_blocks():
    """Both flags must be referenced inside except blocks via
    `global _FLAG_NAME; if not _FLAG_NAME: log.warning(...)`."""
    # v1.18.90 (theme_lost) dispatch:
    assert "global _THEME_LOST_NOTIFY_WARNED" in PLEX_ENUM_PY
    # v1.18.79 (backup_ready) dispatch:
    assert "global _BACKUP_READY_NOTIFY_WARNED" in PLEX_ENUM_PY


# ── Test-trigger endpoint (api.py) ───────────────────────────


def test_test_trigger_endpoint_present():
    """The /api/admin/test-trigger-theme-lost endpoint must be
    defined + admin-gated + accept force_state values."""
    assert (
        '@app.post("/api/admin/test-trigger-theme-lost")'
        in API_PY
    ), (
        "v1.19.41: test-trigger endpoint missing"
    )
    fn_idx = API_PY.index("async def api_admin_test_trigger_theme_lost(")
    fn_end = API_PY.index("@app.", fn_idx + 1)
    body = API_PY[fn_idx:fn_end]
    assert "_require_admin(request)" in body
    assert '"auto", "backup", "sidecar", "none"' in body, (
        "v1.19.41: endpoint must accept the four force_state values"
    )


# ── Probe clipboard button (settings.html + app.js) ──────────


def test_probe_copy_button_present_in_settings():
    """A copy-to-clipboard button must be added to the probe
    section in settings.html."""
    assert (
        'id="probe-plex-themes-copy-btn"' in SETTINGS_HTML
    ), (
        "v1.19.41: probe copy button must be in settings.html"
    )
    # And disabled by default (enabled by JS after probe run).
    btn_idx = SETTINGS_HTML.index('id="probe-plex-themes-copy-btn"')
    chunk = SETTINGS_HTML[max(0, btn_idx - 200):btn_idx + 200]
    assert "disabled" in chunk, (
        "v1.19.41: copy button must be SSR-disabled (JS enables "
        "after probe run completes)"
    )


def test_probe_copy_button_handler_in_js():
    """The JS probe handler must enable the copy button after
    a successful probe run + wire a click handler that calls
    navigator.clipboard.writeText (with execCommand fallback)."""
    fn_idx = APP_JS.index("function bindProbePlexThemes()")
    fn_end = APP_JS.index("\n  function ", fn_idx + 1)
    body = APP_JS[fn_idx:fn_end]
    assert "probe-plex-themes-copy-btn" in body
    # Re-enable after successful run.
    assert "copyBtn.disabled = false" in body, (
        "v1.19.41: copy button must be enabled after successful "
        "probe run"
    )
    # navigator.clipboard primary path.
    assert "navigator.clipboard" in body
    assert "writeText" in body
    # execCommand fallback for ancient browsers.
    assert "execCommand('copy')" in body


# ── End-to-end behavioral via TestClient ─────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    # Need plex_enabled for the test-trigger endpoint not to 503.
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://fake:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "fake-token")
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


def _seed_test_trigger_row(
    conn, *, rk="rk-test", title="Test Movie",
    tmdb_id=99001, with_backup=False, with_sidecar=False,
):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,"
        "        '2026-05-27T00:00:00','2026-05-27T00:00:00')"
    )
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (1, 'movie', ?, ?, 'imdb', "
        "        '2026-05-27', '2026-05-27', "
        "        'https://yt/orig')",
        (tmdb_id, title),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', 1, "
        "        'tt99001', ?, ?, 2020, 1, ?, "
        "        '/data/movies/Test', 0, 1, "
        "        '2026-05-27', '2026-05-27')",
        (rk, tmdb_id, title, 1 if with_sidecar else 0),
    )
    if with_backup:
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by, intent) "
            "VALUES ('movie', ?, '1', "
            "        'https://yt/usr', "
            "        '2026-05-27', 'admin', 'backup')",
            (tmdb_id,),
        )


def test_test_trigger_endpoint_force_state_none_works(
    admin_client, tmp_path,
):
    """force_state='none' must always succeed — it forces tier 4
    (no_fallback) which doesn't depend on the row's actual
    fallback state."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        _seed_test_trigger_row(conn, rk="rk-none", tmdb_id=99100)
        conn.commit()
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "rk-none", "force_state": "none"},
    )
    # Dispatch may fail (no Apprise sinks configured in test
    # env) — accept 502 too. Only the gate logic matters.
    assert r.status_code in (200, 502), (
        f"v1.19.41: test-trigger force_state=none must reach "
        f"dispatch (200 OK or 502 dispatch-failed); got "
        f"{r.status_code} — {r.text}"
    )


def test_test_trigger_endpoint_force_state_backup_requires_backup_signal(
    admin_client, tmp_path,
):
    """force_state='backup' on a row WITHOUT backup-intent
    must 409."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        # No with_backup → no intent='backup' override.
        _seed_test_trigger_row(conn, rk="rk-no-bk", tmdb_id=99200)
        conn.commit()
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "rk-no-bk", "force_state": "backup"},
    )
    assert r.status_code == 409, (
        f"v1.19.41: force_state=backup without a backup signal "
        f"must 409; got {r.status_code} — {r.text}"
    )


def test_test_trigger_endpoint_force_state_sidecar_requires_sidecar(
    admin_client, tmp_path,
):
    """force_state='sidecar' on a row without local_theme_file=1
    AND without disk theme.mp3 must 409."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        _seed_test_trigger_row(
            conn, rk="rk-no-sc", tmdb_id=99300,
            with_sidecar=False,
        )
        conn.commit()
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "rk-no-sc", "force_state": "sidecar"},
    )
    assert r.status_code == 409, (
        f"v1.19.41: force_state=sidecar without a sidecar signal "
        f"must 409; got {r.status_code} — {r.text}"
    )


def test_test_trigger_endpoint_auto_picks_backup_when_available(
    admin_client, tmp_path,
):
    """force_state='auto' on a row WITH a backup-intent override
    must classify as tier 1 (backup_ready)."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        _seed_test_trigger_row(
            conn, rk="rk-bk", tmdb_id=99400, with_backup=True,
        )
        conn.commit()
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "rk-bk", "force_state": "auto"},
    )
    # Same as above — dispatch may 502 in test env. Check the
    # tier was correctly classified by inspecting the response
    # if dispatch succeeded, else verify the 502 is from dispatch
    # (not from the gate logic).
    assert r.status_code in (200, 502)
    if r.status_code == 200:
        body = r.json()
        assert body.get("tier") == "backup_ready", (
            f"v1.19.41: auto-classification on backup-intent row "
            f"must return tier=backup_ready; got {body}"
        )
        assert body.get("backup_source") == "user_url_backup"


def test_test_trigger_endpoint_rejects_bad_force_state(
    admin_client, tmp_path,
):
    """Bad force_state value must 400 with clear error."""
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "anything", "force_state": "bogus"},
    )
    assert r.status_code == 400


def test_test_trigger_endpoint_rejects_missing_rk(admin_client):
    """Empty / missing rk must 400."""
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"force_state": "none"},
    )
    assert r.status_code == 400


def test_test_trigger_endpoint_404s_unknown_rk(admin_client):
    """rk that doesn't exist in plex_items must 404."""
    r = admin_client.post(
        "/api/admin/test-trigger-theme-lost",
        headers=AUTH,
        json={"rk": "rk-does-not-exist", "force_state": "none"},
    )
    assert r.status_code == 404
