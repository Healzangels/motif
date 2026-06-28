"""v1.19.86 — // NEEDS WORK backup-row fix + PROMOTE TO ACTIVE tone.

## Bug — backup rows hoisted by the // NEEDS WORK sort

the user's repro (TV Shows page): clicking // NEEDS WORK floated a
wall of P-rows carrying PB/UB backup badges to the top, even though
they were in the intended "BACKUP READY — DEFERRING TO PLEX" state
(Plex serves its own theme, motif holds a backup_only canonical with
no placement) and needed zero action.

Root cause: the attention-SORT priority-3 ("awaiting placement":
`lf.file_path IS NOT NULL AND p.media_folder IS NULL`) lacked the
`plex_independent_theme=0` gate that the await ATTN *filter*
(v1.14.39, api.py ~2316) already had. So a Plex-serving backup row
matched priority-3 in the sort but was excluded from the await
filter — a mirror-drift. Verified against the user's prod DB: 77 TV
rows were wrongly hoisted; all are backup_only + plex_independent_
theme=1. Fix: add the same gate to priority-3 so sort == filter.

## Polish — PROMOTE TO ACTIVE tone-matches the backup badge

The deploy button was generic amber `.btn-warn` for every backup
row. Now it tone-matches the row's badge: violet (`.btn-promote-ub`,
--violet-bright) for UB (user backup, link-glyph-bk), amber-bright
(`.btn-promote-pb`) for PB (plex_cloud, link-glyph-b). Discriminator
is `data.override.source_kind === 'plex_cloud'` (carried on the
recovery-options override for both real + synthetic BK states).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


# ── Sort fix: source pin + mirror guard ──────────────────────


def test_attention_priority3_has_plex_independent_gate():
    # The "awaiting placement" branch (THEN 3) must now gate on
    # plex_independent_theme=0. The "THEN 3" suffix is unique to the
    # attention-sort priority ladder.
    assert (
        "COALESCE(pi.plex_independent_theme, 0) = 0 THEN 3" in API_PY
    ), (
        "v1.19.86: attention priority-3 (await) must exclude "
        "Plex-serving rows (plex_independent_theme=1), mirroring the "
        "await ATTN filter"
    )


def test_sort_and_filter_share_the_independent_gate():
    """Anti-drift: the attention SORT priority-3 and the await ATTN
    FILTER must both carry the plex_independent_theme exclusion so
    they can't diverge again."""
    gate = "COALESCE(pi.plex_independent_theme, 0) = 0"
    # Filter branch (v1.14.39): file_path + media_folder IS NULL + gate
    assert API_PY.count(gate) >= 2, (
        "v1.19.86: both the await sort-priority and the await filter "
        "must reference the plex_independent_theme gate"
    )


# ── Sort fix: behavioral ─────────────────────────────────────


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


def _db(tmp_path):
    from app.config import Settings
    return Settings(config_dir=tmp_path, data_dir=tmp_path / "data").db_path


def _seed_row(conn, *, theme_id, tmdb_id, title, pit, backup_only, rk):
    now = "2026-05-26T00:00:00"
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, "
        "  upstream_source, last_seen_sync_at, first_seen_sync_at, "
        "  youtube_url) "
        "VALUES (?, 'movie', ?, ?, 'imdb', ?, ?, "
        "        'https://www.youtube.com/watch?v=X')",
        (theme_id, tmdb_id, title, now, now),
    )
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, "
        "  theme_id, guid_imdb, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  plex_theme_verified_ok, first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, NULL, ?, ?, 2020, 1, 0, "
        "        '/data/movies/x', ?, 1, ?, ?)",
        (rk, theme_id, tmdb_id, title, pit, now, now),
    )
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  theme_id, file_path, downloaded_at, source_video_id, "
        "  provenance, source_kind, last_place_attempt_reason) "
        "VALUES ('movie', ?, '1', ?, ?, ?, 'X', 'auto', 'themerrdb', ?)",
        (tmdb_id, theme_id, f"movies/{title}/theme.mp3", now,
         "backup_only" if backup_only else None),
    )


def test_needs_work_sort_does_not_hoist_backup_rows(admin_client, tmp_path):
    """A genuine await row (downloaded, no placement, Plex NOT
    serving) must outrank a backup_ready row (downloaded, no
    placement, Plex serving) under sort=attention — even though the
    backup row sorts alphabetically FIRST. Pre-fix both were
    priority-3 so the alphabetically-first backup row led."""
    from app.core.db import init_db  # ensure schema present
    db = _db(tmp_path)
    init_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, "
            "  discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, "
            "        '2026-05-26T00:00:00', '2026-05-26T00:00:00')"
        )
        # Genuine await — Plex NOT serving (pit=0). Title sorts LAST.
        _seed_row(conn, theme_id=1, tmdb_id=101,
                  title="ZZZ Genuine Await", pit=0, backup_only=False,
                  rk="r1")
        # Backup ready — Plex serving (pit=1) + backup_only. Title
        # sorts FIRST.
        _seed_row(conn, theme_id=2, tmdb_id=102,
                  title="AAA Backup Ready", pit=1, backup_only=True,
                  rk="r2")
        conn.commit()

    r = admin_client.get(
        "/api/library?tab=movies&sort=attention&per_page=50",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    titles = [
        (it.get("plex_title") or it.get("theme_title"))
        for it in r.json()["items"]
    ]
    assert "ZZZ Genuine Await" in titles and "AAA Backup Ready" in titles
    assert titles.index("ZZZ Genuine Await") < titles.index("AAA Backup Ready"), (
        "v1.19.86: the backup_only + plex_independent_theme=1 row must "
        "NOT be ranked attention priority-3 (it is intentionally "
        f"unplaced). Got order: {titles}"
    )


# ── PROMOTE TO ACTIVE tone classes ───────────────────────────


def test_promote_tone_css_matches_badge_colors():
    assert ".btn-promote-ub { color: var(--violet-bright);" in APP_CSS, (
        "v1.19.86: UB promote button must use --violet-bright (matches "
        "link-glyph-bk)"
    )
    assert ".btn-promote-pb { color: var(--amber-bright);" in APP_CSS, (
        "v1.19.86: PB promote button must use --amber-bright (matches "
        "link-glyph-b)"
    )


def test_promote_button_uses_tone_class_from_source_kind():
    # The tone class is computed from the override's source_kind.
    assert "promoteSourceKind === 'plex_cloud'" in APP_JS
    assert "? 'btn-promote-pb'" in APP_JS
    assert ": 'btn-promote-ub'" in APP_JS
    # The button markup interpolates the computed tone class (no
    # longer hardcoded btn-warn).
    assert 'class="btn btn-tiny ${promoteToneClass}"' in APP_JS, (
        "v1.19.86: the PROMOTE TO ACTIVE button must use the computed "
        "tone class"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_86_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
