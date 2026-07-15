"""v1.21.9 — plex-refresh auto-download + accurate theme-available CTA.

Two asks from the user's Discord review of the v1.21.5 "theme available"
notification:

  1. CTA was inaccurate — it said "open the // MOTIF INFO card and
     click // DOWNLOAD"; the real action for a SRC=— row is the per-row
     // SOURCE menu → // DOWNLOAD TDB.

  2. Close the discovery asymmetry: the TDB-sync side already
     auto-downloads new SRC=— themes (behind
     sync.auto_download_new_themes_for_unthemed_rows); the plex-refresh
     side only NOTIFIED. Now the SAME toggle governs both — a Plex
     refresh that adds a new SRC=— item with an available TDB theme
     auto-acquires it (download → non-forced place) when the toggle is
     on, otherwise notifies.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

NOTIFY_CONTENT = (REPO / "app" / "core" / "notify_content.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


# ── CTA copy fix ─────────────────────────────────────────────

def test_theme_available_body_names_source_menu_download_tdb():
    idx = NOTIFY_CONTENT.index("def format_theme_available_body(")
    nxt = NOTIFY_CONTENT.find("\ndef ", idx + 1)
    body = NOTIFY_CONTENT[idx:nxt]
    assert "// SOURCE menu" in body
    assert "// DOWNLOAD TDB" in body
    # The old (inaccurate) wording is gone.
    assert "// MOTIF INFO card and click // DOWNLOAD to add" not in body


# ── source pins: the auto-acquire branch ─────────────────────

def test_plex_enum_has_auto_acquire_branch():
    fn_idx = PLEX_ENUM.index("def _maybe_notify_theme_available(")
    nxt = PLEX_ENUM.index("\ndef ", fn_idx + 1)
    body = PLEX_ENUM[fn_idx:nxt]
    # Reuses the SAME toggle as the sync side.
    assert "auto_download_new_themes_for_unthemed_rows" in body
    # Auto-acquire enqueues via _enqueue_download, gated on _auto_dl.
    assert "if _auto_dl:" in body
    assert "from .sync import _enqueue_download" in body
    assert "_enqueue_download(" in body
    # And it's wrapped in a transaction.
    assert "with get_conn(db_path) as conn, transaction(conn):" in body


def test_settings_toggle_mentions_both_discovery_paths():
    idx = SETTINGS_HTML.index(
        "auto_download_new_themes_for_unthemed_rows")
    block = SETTINGS_HTML[idx:idx + 1200]
    assert "Plex refresh" in block
    assert "both" in block.lower() or "BOTH" in block


# ── behavioral: auto-acquire vs notify ───────────────────────

class _FakeNotif:
    def __init__(self, enabled):
        self.events = {"new_tdb_theme_available": enabled}


class _FakeSync:
    def __init__(self, auto_dl):
        self.auto_download_new_themes_for_unthemed_rows = auto_dl


class _FakeCfg:
    def __init__(self, auto_dl, notify_on):
        self.sync = _FakeSync(auto_dl)
        self.notifications = _FakeNotif(notify_on)


def _patch(monkeypatch, *, auto_dl, notify_on):
    import app.config as _appconfig
    import app.core.notify as _notify

    class _FakeSettings:
        def __init__(self, *a, **k):
            self.cfg = _FakeCfg(auto_dl, notify_on)
    monkeypatch.setattr(_appconfig, "Settings", _FakeSettings)
    captured = []

    def _cap(db, notif, *, event_kind, title, body, body_format="text", **_kw):  # **_kw: absorb item_ctx (v0.51.151)
        captured.append(event_kind)
    monkeypatch.setattr(_notify, "dispatch", _cap)
    return captured


def _seed(conn, *, tmdb_id, title, rk):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, "
        "        '2026-05-01T00:00:00', '2026-05-01T00:00:00')"
    )
    tid = conn.execute(
        "INSERT INTO themes "
        "  (media_type, tmdb_id, title, year, youtube_url, "
        "   upstream_source, last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', ?, ?, '2024', 'https://youtu.be/abc', 'imdb', "
        "        '2026-05-23T00:00:00', '2026-05-02T00:00:00')",
        (tmdb_id, title),
    ).lastrowid
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, guid_tmdb, title, year, "
        "   has_theme, local_theme_file, folder_path, theme_id, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, ?, 2024, 0, 0, "
        "        '/data/movies/X (2024)', ?, "
        "        '2026-06-01T00:00:00', '2026-06-01T00:00:00')",
        (rk, tmdb_id, title, tid),
    )


def _download_jobs(db_path, tmdb_id):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM jobs "
            "WHERE job_type='download' AND tmdb_id=?",
            (tmdb_id,),
        ).fetchone()[0]


def test_auto_acquire_enqueues_download_when_toggle_on(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _seed(conn, tmdb_id=111, title="Alpha", rk="new1")
        conn.commit()
    captured = _patch(monkeypatch, auto_dl=True, notify_on=False)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert _download_jobs(db_path, 111) >= 1, (
        "v1.21.9: toggle ON → a Plex-added SRC=— item must auto-enqueue "
        "a download"
    )
    assert captured == [], "auto-acquire must NOT also notify"


def test_notifies_when_toggle_off(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _seed(conn, tmdb_id=111, title="Alpha", rk="new1")
        conn.commit()
    captured = _patch(monkeypatch, auto_dl=False, notify_on=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert _download_jobs(db_path, 111) == 0, (
        "v1.21.9: toggle OFF → must NOT auto-download"
    )
    assert captured == ["new_tdb_theme_available"], (
        "toggle OFF + notify ON → must dispatch the notification"
    )


def test_silent_when_both_off(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        _seed(conn, tmdb_id=111, title="Alpha", rk="new1")
        conn.commit()
    captured = _patch(monkeypatch, auto_dl=False, notify_on=False)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert _download_jobs(db_path, 111) == 0
    assert captured == []


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
