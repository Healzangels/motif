"""v1.19.43 — cloud-themes-backup UI surface (B badge + filter + bulk + per-row).

Ships the user-facing layer of cloud-themes-backup. v1.19.41 +
v1.19.42 shipped the detection pipe + the staging walker; v1.19.43
makes the feature reachable from the library page without the
operator needing to hit the admin endpoints by hand.

## Scope

  1. CSS — `.link-glyph-b` rule (lemon palette, mirrors BK shape).
  2. JS render — linkCell B branch BEFORE the BK check. Both
     stamps share `last_place_attempt_reason='backup_only'`, so
     flipping the order would render every B as BK and hide the
     cloud-backup classification.
  3. library.html — `// B` filter chip with `data-link-pill="b"`.
  4. api.py — `link_pills=b` SQL handler + `_pset` allow-list,
     filtering on `lf.source_kind = 'plex_cloud'`.
  5. Bulk-bar — `// BACKUP CLOUD THEMES` button. Visibility on
     ≥1 pure-P + !downloaded row; broader gate than DOWNLOAD TDB
     BACKUP (works without a TDB URL — anime cohort).
  6. SOURCE menu — `// BACKUP THIS THEME` per-row entry +
     single-rk POST to `/api/admin/cloud-themes-backup-run`.
  7. v1.19.39 PROMOTE TO ACTIVE tooltip — third variant for
     `source_kind='plex_cloud'` explaining the v1.18.36
     re-upload trick round-trip.
  8. api_recovery_options — source_kind passthrough on the
     synthesized override (both the carry-through and fabricated
     branches).

## Why a separate B badge instead of widening BK

the user + the cloud-backup arc design discussion: keeping BK
visually distinct surfaces the "automated cloud backup" surface
as its own filter axis. Pure-P rows currently render LINK=— so
a new badge color is the discovery signal users will look for.
Plumbing IS shared (backup_only stamp + v1.19.35 PROMOTE flow);
only the surface presentation diverges.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient  # noqa: E402

APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIBRARY_HTML = (
    REPO / "app" / "web" / "templates" / "library.html"
).read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── CSS ──────────────────────────────────────────────────────


def test_link_glyph_b_css_rule_defined():
    """The `.link-glyph-b` rule must exist + use design-system
    tokens (not raw hex). v1.19.48: switched lemon → amber-bright
    (the user's repro: lemon read as 'warning/cookies' because
    --lemon is reserved for the cookies-needed UX surfaces)."""
    assert ".link-glyph-b {" in APP_CSS, (
        "v1.19.43: .link-glyph-b CSS rule missing — B badge "
        "would render unstyled"
    )
    rule_idx = APP_CSS.index(".link-glyph-b {")
    rule_end = APP_CSS.index("}", rule_idx)
    rule = APP_CSS[rule_idx:rule_end]
    assert "var(--amber-bright)" in rule, (
        "v1.19.48: B badge color must use --amber-bright token "
        "(Plex-family but distinct from regular amber P/PS)"
    )
    assert "var(--amber-rgb)" in rule, (
        "v1.19.48: B badge border/bg must use --amber-rgb (the "
        "Plex-family connection — text-color amber-bright + "
        "border/bg amber keeps it amber-family but distinct "
        "from P/PS)"
    )
    # Lemon was the v1.19.43 choice but conflicts with the
    # cookies-needed UX (--lemon is reserved per v1.15.121).
    assert "var(--lemon)" not in rule, (
        "v1.19.48: B badge must NOT use --lemon — that token is "
        "reserved for the cookies-needed UX"
    )


def test_link_glyph_b_mirrors_bk_shape():
    """Shape (padding, font-size, font-weight, letter-spacing,
    text-transform) must mirror .link-glyph-bk so the LINK glyph
    row reads consistently across all the badges."""
    bk_idx = APP_CSS.index(".link-glyph-bk {")
    bk_rule = APP_CSS[bk_idx:APP_CSS.index("}", bk_idx)]
    b_idx = APP_CSS.index(".link-glyph-b {")
    b_rule = APP_CSS[b_idx:APP_CSS.index("}", b_idx)]
    # Shape properties pinned to confirm structural parity.
    for prop in ("display: inline-block",
                 "font-size: 9px", "font-weight: 700",
                 "letter-spacing: 0.1em", "text-transform: uppercase"):
        assert prop in bk_rule
        assert prop in b_rule, (
            f"v1.19.43: .link-glyph-b missing {prop!r} — mirror "
            f"drift from .link-glyph-bk"
        )


# ── JS render (linkCell B branch) ────────────────────────────


def test_b_branch_fires_before_bk_branch():
    """The B branch must check BEFORE the BK branch. Both
    require backup_only, so reversing the order hides every
    plex_cloud row behind the BK badge."""
    b_idx = APP_JS.index("isPlexCloudBackup")
    bk_idx = APP_JS.index("if (isBackupOnly) {")
    # The B branch declaration AND its `if` must precede the
    # standalone `if (isBackupOnly)` (which is now the `else if`
    # branch — the test pins the BK landmark string the v1.19.21
    # comment introduced).
    bk_else_idx = APP_JS.index("} else if (isBackupOnly) {")
    assert b_idx < bk_else_idx, (
        "v1.19.43: isPlexCloudBackup declaration must precede "
        "the BK branch — every B row also satisfies isBackupOnly, "
        "so the inverted order would render every B as BK"
    )


def test_b_predicate_requires_source_kind_plex_cloud():
    """The B predicate must include `source_kind === 'plex_cloud'`
    — without that, BK rows (user_url backup_only) would render
    as B since they ALSO satisfy isBackupOnly."""
    idx = APP_JS.index("isPlexCloudBackup")
    block = APP_JS[idx:idx + 400]
    assert "isBackupOnly" in block
    assert "source_kind === 'plex_cloud'" in block, (
        "v1.19.43: B predicate must AND in source_kind check — "
        "without it, every BK row would also classify as B"
    )


def test_b_glyph_uses_link_glyph_b_class():
    """The render call must emit the `link-glyph-b` class so
    the CSS rule fires. Tooltip mentions PROMOTE TO ACTIVE
    (the recovery action).

    v1.19.63: badge label B → BP (Backup Plex). CSS classname
    kept (.link-glyph-b). Test accepts B, BP, or PB.
    v1.19.75: BP → PB (Plex Backup, reordered)."""
    idx = APP_JS.index("if (isPlexCloudBackup)")
    block = APP_JS[idx:idx + 800]
    assert "link-glyph-b" in block
    assert (
        ">B<" in block
        or ">BP<" in block
        or ">PB<" in block
    ), "v1.19.43/63/75: glyph text must be literal 'B', 'BP', or 'PB'"
    assert "PROMOTE TO ACTIVE" in block, (
        "v1.19.43: B/BP tooltip must point at the recovery action "
        "(PROMOTE TO ACTIVE) so users know how to deploy the "
        "backup later"
    )


# ── Filter chip + SQL handler ────────────────────────────────


def test_library_html_has_b_filter_chip():
    """The `// B` chip must be in library.html with the
    canonical data-link-pill='b' attribute."""
    assert 'data-link-pill="b"' in LIBRARY_HTML, (
        "v1.19.43: B filter chip missing in library.html"
    )
    chip_idx = LIBRARY_HTML.index('data-link-pill="b"')
    # Window around the chip should show the link-glyph-b class
    # for visual parity with the row badge.
    chip_chunk = LIBRARY_HTML[
        max(0, chip_idx - 200):chip_idx + 500
    ]
    assert "link-glyph-b" in chip_chunk
    # v1.19.63: label renamed B → BP. v1.19.75: BP → PB.
    # Accept all three forms.
    assert (
        ">B</button>" in chip_chunk
        or ">BP</button>" in chip_chunk
        or ">PB</button>" in chip_chunk
    ), "v1.19.43/63/75: chip label must be 'B', 'BP', or 'PB'"


def test_pset_allow_list_includes_b():
    """`_pset` validator for link_pills must accept 'b' so the
    query param survives parsing. v1.19.50: relaxed pin to
    substring (the set grew again with 'bk')."""
    assert (
        '"hl", "c", "m", "none", "ps", "pu", "rp", "b"' in API_PY
    ), (
        "v1.19.43: link_pills _pset allow-list must include 'b'"
    )


def test_link_pills_b_sql_filter_handler():
    """The SQL handler for link_pills='b' must filter on
    source_kind='plex_cloud' AND backup_only stamp AND no
    placement (mirrors the JS predicate)."""
    # Locate the elif p == "b" branch.
    idx = API_PY.index('elif p == "b":')
    block = API_PY[idx:idx + 1700]
    assert "COALESCE(lf_e.source_kind, lf_g.source_kind) = 'plex_cloud'" in block, (
        "v1.19.43: link_pills=b SQL must filter on "
        "source_kind='plex_cloud'"
    )
    assert "backup_only" in block, (
        "v1.19.43: link_pills=b SQL must require the backup_only "
        "stamp — mirrors the JS isBackupOnly predicate"
    )
    assert "COALESCE(p_e.media_folder, p_g.media_folder) IS NULL" in block, (
        "v1.19.43: link_pills=b SQL must require no placement "
        "(otherwise a row that was promoted but kept the local "
        "file would still match)"
    )


# ── Bulk-bar button ──────────────────────────────────────────


def test_bulk_cloud_backup_btn_in_library_html():
    """library.html must declare the bulk button with the
    btn-plex tone class. v1.19.48: retoned from
    lib-source-themerrdb (green) → btn-plex (amber) so the
    user reads at-a-glance that this is a Plex-touching action.
    Mirrors LET PLEX SERVE / PUSH TO PLEX precedent."""
    assert 'id="library-cloud-backup-btn"' in LIBRARY_HTML
    btn_idx = LIBRARY_HTML.index('id="library-cloud-backup-btn"')
    # Walk back to the opening <button — class precedes id in HTML.
    open_idx = LIBRARY_HTML.rfind("<button", 0, btn_idx)
    btn_chunk = LIBRARY_HTML[open_idx:btn_idx + 400]
    assert "btn-plex" in btn_chunk, (
        "v1.19.48: cloud-backup bulk button must use btn-plex "
        "tone (amber, Plex-touching action — same family as "
        "PUSH TO PLEX / LET PLEX SERVE / RESTORE FROM PLEX)"
    )
    # Label renamed to mirror DOWNLOAD TDB BACKUP shape.
    assert "DOWNLOAD PLEX BACKUP" in btn_chunk, (
        "v1.19.48: label renamed BACKUP CLOUD THEMES → DOWNLOAD "
        "PLEX BACKUP to mirror DOWNLOAD TDB BACKUP convention "
        "(verb + SOURCE + intent)"
    )


def test_bulk_cloud_backup_count_computed():
    """The `cloudBackupCount` effectiveCount must exist + check
    !placed && !downloaded && plex_independent_theme."""
    assert "const cloudBackupCount = effectiveCount" in APP_JS
    idx = APP_JS.index("const cloudBackupCount = effectiveCount")
    block = APP_JS[idx:idx + 600]
    assert "plex_independent_theme" in block
    assert "!placed" in block
    assert "!downloaded" in block


def test_bulk_cloud_backup_btn_visibility_gate():
    """The bulk button must hide when cloudBackupCount === 0
    AND visibility-gated off on tdb-only-browse + attn-update
    filter (same as DOWNLOAD TDB BACKUP)."""
    idx = APP_JS.index("if (cloudBackupBtn)")
    block = APP_JS[idx:idx + 800]
    assert "cloudBackupCount > 0" in block
    assert "!onTdbOnly" in block
    assert "!onAttnUpdateFilter" in block


def test_bulk_cloud_backup_click_handler():
    """Click handler must POST to /api/admin/cloud-themes-backup-run
    with the selection's rks."""
    idx = APP_JS.index("library-cloud-backup-btn")
    # Second occurrence (the click handler — the first is the
    # const declaration).
    handler_idx = APP_JS.index(
        "library-cloud-backup-btn", idx + 1,
    )
    # v1.19.52: handler grew (waitForOp + count surface +
    # 409 alert + refresh timers); widen the window.
    block = APP_JS[handler_idx:handler_idx + 8000]
    assert "/api/admin/cloud-themes-backup-run" in block, (
        "v1.19.43: bulk handler must POST to the v1.19.42 "
        "admin endpoint"
    )
    assert "rks" in block
    # v1.19.45: endpoint is now async (acquire-then-spawn-thread).
    # Handler no longer surfaces downloaded_count synchronously
    # — progress shows up via op_progress drawer instead. Pin
    # the new contract: handler calls boostPoll to surface the
    # new op in the mini-bar within the next poll tick.
    assert "boostPoll" in block, (
        "v1.19.45: bulk handler must call motifOps.boostPoll() "
        "so the new background op surfaces in the mini-bar "
        "immediately rather than waiting for the next poll tick"
    )


# ── SOURCE-menu per-row entry ────────────────────────────────


def test_source_menu_backup_cloud_theme_item_exists():
    """SOURCE menu builder must add 'backup-cloud-theme' on
    isPlexAgent + !downloaded rows. v1.19.48: label renamed
    'BACKUP THIS THEME' → 'DOWNLOAD PLEX BACKUP' to mirror the
    DOWNLOAD TDB BACKUP convention (no demonstrative pronoun)."""
    assert "'backup-cloud-theme'" in APP_JS, (
        "v1.19.43: SOURCE menu must include 'backup-cloud-theme' "
        "action"
    )
    assert "'DOWNLOAD PLEX BACKUP'" in APP_JS, (
        "v1.19.48: label must be 'DOWNLOAD PLEX BACKUP' (mirrors "
        "DOWNLOAD TDB BACKUP shape)"
    )


def test_backup_cloud_theme_visibility_gate():
    """The per-row entry gate must check isPlexAgent (broader than
    DOWNLOAD TDB BACKUP — TDB-less rows ARE the cohort that
    benefits most). v1.19.62 extended the gate via
    `isCloudBackupable` to also cover PS-with-non-plex_cloud-DL
    rows."""
    idx = APP_JS.index("'backup-cloud-theme'")
    # v1.19.62: widened window 500→1500 to absorb the v1.19.62
    # tooltip-branching block between the gate and the menuItemHtml.
    pre = APP_JS[max(0, idx - 1500):idx]
    assert "isPlexAgent" in pre
    assert "!downloaded" in pre


def test_backup_cloud_theme_click_handler_present():
    """The dispatch must have an `act === 'backup-cloud-theme'`
    branch that calls /api/admin/cloud-themes-backup-run."""
    assert "act === 'backup-cloud-theme'" in APP_JS, (
        "v1.19.43: SOURCE-menu click dispatch needs the "
        "backup-cloud-theme branch"
    )
    idx = APP_JS.index("act === 'backup-cloud-theme'")
    block = APP_JS[idx:idx + 3400]
    assert "/api/admin/cloud-themes-backup-run" in block
    assert "rks" in block


def test_backup_cloud_theme_in_enqueueing_set():
    """The `_enqueueing` set drives the boostPoll trigger; the
    new action must be in it so the topbar mini-bar surfaces
    the queued op without waiting for the next regular tick."""
    idx = APP_JS.index("const _enqueueing = new Set([")
    block = APP_JS[idx:idx + 800]
    assert "'backup-cloud-theme'" in block, (
        "v1.19.43: backup-cloud-theme must be in the _enqueueing "
        "set so boostPoll fires + the mini-bar lights up"
    )


# ── PROMOTE TO ACTIVE tooltip third variant ──────────────────


def test_promote_tooltip_has_plex_cloud_variant():
    """The v1.19.39 promoteTip ternary must extend to a third
    variant for plex_cloud synthetic overrides."""
    idx = APP_JS.index("const promoteTip")
    block = APP_JS[idx:idx + 1200]
    assert "isPlexCloudSynthetic" in block, (
        "v1.19.43: tooltip ternary must branch on "
        "isPlexCloudSynthetic for the cloud-backup variant"
    )
    assert "re-upload trick" in block, (
        "v1.19.43: plex_cloud tooltip variant must reference the "
        "v1.18.36 re-upload trick so users understand the round-trip"
    )


def test_promote_tip_predicate_checks_source_kind():
    """The isPlexCloudSynthetic predicate must AND in
    source_kind='plex_cloud' AND isSynthetic — without isSynthetic
    a real user_overrides row with source_kind smuggled through
    would hijack the wording."""
    idx = APP_JS.index("isPlexCloudSynthetic")
    block = APP_JS[idx:idx + 400]
    assert "isSynthetic" in block
    assert "'plex_cloud'" in block


# ── api_recovery_options source_kind passthrough ─────────────


def test_recovery_options_synthesized_override_includes_source_kind():
    """The synthesized override (fabricated branch — no
    user_overrides row exists) must carry source_kind from
    bk_local so the JS tooltip can branch on it."""
    # Locate the synthesized override dict.
    fn_idx = API_PY.index("bk_override = {")
    fn_end = API_PY.index("}", fn_idx + 600)
    block = API_PY[fn_idx:fn_end + 100]
    assert '"source_kind"' in block, (
        "v1.19.43: synthesized bk_override must include "
        "source_kind so the JS PROMOTE tooltip can identify "
        "plex_cloud rows"
    )
    assert 'bk_local["source_kind"]' in block, (
        "v1.19.43: source_kind must come from bk_local, not "
        "hardcoded"
    )


def test_recovery_options_carry_through_branch_also_lifts_source_kind():
    """The carry-through branch (real user_overrides row exists)
    must ALSO lift source_kind from bk_local — user_overrides
    has no source_kind column, so the JS tooltip would otherwise
    miss the plex_cloud signal on REVERT-restored rows."""
    idx = API_PY.index('bk_override = dict(ovr)')
    block = API_PY[idx:idx + 800]
    assert "source_kind" in block, (
        "v1.19.43: carry-through branch must also set source_kind"
    )
    assert 'bk_local["source_kind"]' in block


# ── Behavioral via TestClient ────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_PLEX_URL", "http://fake:32400")
    monkeypatch.setenv("MOTIF_PLEX_TOKEN", "fake-token")
    (tmp_path / "themes").mkdir(exist_ok=True)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_plex_cloud_bk_row(conn, *, tmdb_id=88001, rk="rk-cloud"):
    """Seed a BK-state row backed by a plex_cloud local_files
    (mirrors v1.19.42 walker output shape)."""
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,"
        "        '2026-05-27','2026-05-27')"
    )
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (1, 'movie', ?, 'Cloud Test', 'imdb', "
        "        '2026-05-27', '2026-05-27', "
        "        'https://yt/orig')",
        (tmdb_id,),
    )
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_tmdb, title, year, has_theme, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', 1, ?, 'Cloud Test', 2020, "
        "        1, 1, 1, '2026-05-27', '2026-05-27')",
        (rk, tmdb_id),
    )
    conn.execute(
        "INSERT INTO local_files "
        "  (media_type, tmdb_id, section_id, file_path, "
        "   downloaded_at, source_video_id, source_kind, "
        "   provenance, last_place_attempt_reason, "
        "   last_place_attempt_at) "
        "VALUES ('movie', ?, '1', 'movies/Cloud Test (2020)/theme.mp3', "
        "        '2026-05-27', 'sha1deadbeef', 'plex_cloud', "
        "        'auto', 'backup_only', '2026-05-27')",
        (tmdb_id,),
    )


def test_recovery_options_returns_source_kind_for_plex_cloud(
    admin_client, tmp_path,
):
    """End-to-end: seed a plex_cloud BK row → call recovery
    options → assert override.source_kind === 'plex_cloud'.
    The JS tooltip branches on this; without it the PROMOTE
    wording stays generic."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        _seed_plex_cloud_bk_row(conn, tmdb_id=88001, rk="rk-cloud")
        conn.commit()
    r = admin_client.get(
        "/api/items/movie/88001/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["backup_state"] is True, (
        "v1.19.43: BK-state detection must fire for plex_cloud rows"
    )
    assert data["override"] is not None
    assert data["override"]["intent"] == "backup"
    assert data["override"]["source_kind"] == "plex_cloud", (
        "v1.19.43: recovery_options must pass source_kind through "
        "so the JS PROMOTE tooltip can branch on it"
    )


def test_recovery_options_source_kind_none_for_non_cloud(
    admin_client, tmp_path,
):
    """A BK row WITHOUT plex_cloud source_kind (e.g. user-url
    backup intent) should still carry source_kind through —
    just with a non-plex_cloud value so the JS doesn't
    misidentify it as cloud-backup."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        # Seed identical to _seed_plex_cloud_bk_row but with
        # source_kind='url' (mimics SET URL → KEEP AS BACKUP).
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1','Movies','movie',0,0,'movies',1,"
            "        '2026-05-27','2026-05-27')"
        )
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (1, 'movie', 88002, 'Url Test', 'imdb', "
            "        '2026-05-27', '2026-05-27', 'https://yt/u')"
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_tmdb, title, year, has_theme, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-url', '1', 'movie', 1, 88002, 'Url Test', "
            "        2020, 1, 1, 1, '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, source_kind, "
            "   provenance, last_place_attempt_reason, "
            "   last_place_attempt_at) "
            "VALUES ('movie', 88002, '1', "
            "        'movies/Url Test (2020)/theme.mp3', "
            "        '2026-05-27', 'yt-vid', 'url', 'auto', "
            "        'backup_only', '2026-05-27')"
        )
        conn.commit()
    r = admin_client.get(
        "/api/items/movie/88002/recovery-options?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["backup_state"] is True
    assert data["override"]["source_kind"] == "url", (
        "v1.19.43: source_kind for url-backup must be 'url' "
        "(NOT 'plex_cloud') — the JS tooltip variant must NOT "
        "fire on user-url backups"
    )


def test_link_pills_b_sql_filter_e2e(admin_client, tmp_path):
    """End-to-end SQL filter: link_pills=b returns ONLY the
    plex_cloud row in a mixed library."""
    from app.config import Settings
    settings = Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    )
    with sqlite3.connect(settings.db_path) as conn:
        _seed_plex_cloud_bk_row(conn, tmdb_id=88003, rk="rk-cloud2")
        # Seed a non-cloud BK row too (url backup).
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES (2, 'movie', 88004, 'Url Backup', 'imdb', "
            "        '2026-05-27', '2026-05-27', 'https://yt/u')"
        )
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_tmdb, title, year, has_theme, "
            "   plex_independent_theme, plex_theme_verified_ok, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-url2', '1', 'movie', 2, 88004, 'Url Backup', "
            "        2020, 1, 1, 1, '2026-05-27', '2026-05-27')"
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, source_kind, "
            "   provenance, last_place_attempt_reason, "
            "   last_place_attempt_at) "
            "VALUES ('movie', 88004, '1', "
            "        'movies/Url Backup (2020)/theme.mp3', "
            "        '2026-05-27', 'yt-id', 'url', 'auto', "
            "        'backup_only', '2026-05-27')"
        )
        conn.commit()
    r = admin_client.get(
        "/api/library?tab=movies&link_pills=b&page_size=100",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    items = data.get("items") or []
    tmdb_ids = {it.get("theme_tmdb") for it in items}
    assert 88003 in tmdb_ids, (
        "v1.19.43: link_pills=b must return the plex_cloud row"
    )
    assert 88004 not in tmdb_ids, (
        "v1.19.43: link_pills=b must NOT return url-backup rows "
        "(filter discriminates on source_kind='plex_cloud')"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_43_version_pin():
    """Version bumped at v1.19.43 (then again at v1.19.44 audit
    fixes). Match 1.19.x prefix so subsequent bumps don't break
    this guard. The v1.19.44 test file pins the exact value."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
