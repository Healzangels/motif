"""v1.21.5 — "theme available to add" push (new_tdb_theme_available).

the user's ask: "after a plex refresh if there is a new item in plex
that matches with a themerrdb theme that's available and doesn't have
a plex provided theme then it notifies that you could add a theme."

This ships the v1.17.0-deferred `new_tdb_theme_available` event. The
original deferral was about hooking the dispatch inside
`resolve_theme_ids`'s 500-row hot loop. v1.21.5 sidesteps that by
hooking the plex_enum refresh side: `_maybe_notify_theme_available`
runs AFTER resolve stamps theme_id, sweeps the rks inserted THIS enum
(the inherent "new item in Plex" gate), and notifies the SRC=— subset
(no Plex theme, no motif content, TDB theme available).

Design:
  - Bulk-shaped (one digest per refresh, not one ping per row), like
    themes_added_by_sync.
  - Per-(media_type, tmdb_id) dedupe so a Plex remove+re-add (new rk)
    doesn't re-ping.
  - Gated on `updated > 0` at the call site so the FIRST enum of a
    section (initial seed) stays silent — only genuine additions fire.
  - Default OFF / opt-in, like the other theme_* events.

Tests: source pins for the wiring + behavioral tests against a seeded
DB (the v1.18.81 lesson — exercise the real plex_enum → notify pipe,
not just the source text).
"""
from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
NOTIFY_PY = REPO / "app" / "core" / "notify.py"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Source pins: formatters ──────────────────────────────────

def test_formatters_exist():
    src = NOTIFY_CONTENT_PY.read_text()
    for fn in (
        "def format_theme_available_title(",
        "def format_theme_available_body(",
        "def format_theme_available_batch_title(",
        "def format_theme_available_batch_body(",
    ):
        assert fn in src, f"v1.21.5: missing formatter {fn}"


def test_single_body_names_the_download_action():
    """Body must point at the action — open the INFO card and
    DOWNLOAD — without repeating the title (v1.19.92 copy rule)."""
    src = NOTIFY_CONTENT_PY.read_text()
    fn_idx = src.index("def format_theme_available_body(")
    nxt = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt if nxt != -1 else len(src)]
    flat = re.sub(r'"\s*"', "", body)
    # v1.21.9: CTA points at the per-row SOURCE menu → DOWNLOAD TDB.
    assert "DOWNLOAD TDB" in flat
    assert "SOURCE menu" in flat


def test_titles_use_sparkle_emoji():
    # v1.23.82: ✨ (was 🎵, which collided with theme_added) — every
    # notification event now has its own distinct glyph.
    src = NOTIFY_CONTENT_PY.read_text()
    assert "✨ Theme available" in src
    assert "✨ " in src and "themes available to add" in src
    assert "🎵 Theme available" not in src


# ── Source pins: plex_enum wiring ────────────────────────────

def test_helper_and_callsite_present():
    src = PLEX_ENUM_PY.read_text()
    assert "def _maybe_notify_theme_available(" in src
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # New-item rks collected in the INSERT branch.
    assert "new_item_rks.append(it.rating_key)" in body
    # Call-site gate: only fire on an already-populated section
    # (updated>0) so the first-enum seed stays silent. v1.21.48: the
    # gate now keys on candidate_rks (new-item ∪ newly-linked) so a
    # pre-existing item linked late also fires.
    assert "if candidate_rks and updated > 0:" in body


def test_helper_uses_src_dash_predicate():
    """The sweep must restrict to SRC=— rows with a TDB theme: a
    themes JOIN (theme_id resolved), has_theme=0, local_theme_file=0,
    and NOT EXISTS across local_files / user_overrides / placements."""
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _maybe_notify_theme_available(")
    nxt = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt if nxt != -1 else len(src)]
    assert "JOIN themes t ON t.id = pi.theme_id" in body
    assert "pi.has_theme = 0" in body
    assert "pi.local_theme_file = 0" in body
    assert "NOT EXISTS (SELECT 1 FROM local_files" in body
    assert "NOT EXISTS (SELECT 1 FROM user_overrides" in body
    assert "NOT EXISTS (SELECT 1 FROM placements" in body


def test_helper_dispatch_and_dedupe_shape():
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _maybe_notify_theme_available(")
    nxt = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt if nxt != -1 else len(src)]
    assert 'event_kind="new_tdb_theme_available"' in body
    assert 'f"new_tdb_theme_available:{mt}:{tid}"' in body
    assert "_ndedupe.record_fire(db_path, key)" in body
    assert 'body_format="markdown"' in body


# ── Source pins: config + settings + tone ────────────────────

def test_config_default_off():
    src = CONFIG_FILE_PY.read_text()
    idx = src.index("_DEFAULT_NOTIFY_EVENTS")
    block = src[idx:src.index("}", idx)]
    assert '"new_tdb_theme_available": False' in block


def test_settings_toggle_surfaced():
    html = SETTINGS_HTML.read_text()
    assert ('data-cfg-field="notifications.events.new_tdb_theme_available"'
            in html)
    # The stale deferred note must be gone.
    assert "not yet UI-surfaced" not in html


def test_notify_tone_map_has_event():
    src = NOTIFY_PY.read_text()
    assert '"new_tdb_theme_available":' in src


# ── Behavioral: the real plex_enum → notify pipe ─────────────

class _FakeNotif:
    def __init__(self, enabled: bool):
        self.events = {"new_tdb_theme_available": enabled}


class _FakeSync:
    # v1.21.9: plex auto-download reuses this toggle — keep it OFF so
    # these tests exercise the notify path, not the auto-acquire path.
    auto_download_new_themes_for_unthemed_rows = False


class _FakeCfg:
    def __init__(self, enabled: bool):
        self.notifications = _FakeNotif(enabled)
        self.sync = _FakeSync()


def _make_fake_settings(enabled: bool):
    class _FakeSettings:
        def __init__(self, *a, **k):
            self.cfg = _FakeCfg(enabled)
    return _FakeSettings


def _patch(monkeypatch, *, enabled: bool):
    """Enable/disable the event + capture dispatch calls. Returns the
    capture list."""
    import app.config as _appconfig
    import app.core.notify as _notify
    monkeypatch.setattr(_appconfig, "Settings",
                        _make_fake_settings(enabled))
    captured: list[dict] = []

    def _cap(db, notif, *, event_kind, title, body, body_format="text", **_kw):  # **_kw: absorb item_ctx (v0.51.151)
        captured.append({"event_kind": event_kind, "title": title,
                         "body": body})

    monkeypatch.setattr(_notify, "dispatch", _cap)
    return captured


def _seed_section_and_theme(conn, *, tmdb_id: int, title: str):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, themes_subdir, "
        "   included, discovered_at, last_seen_at) "
        "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, "
        "        '2026-05-01T00:00:00', '2026-05-01T00:00:00')"
    )
    return conn.execute(
        "INSERT INTO themes "
        "  (media_type, tmdb_id, title, year, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at) "
        "VALUES ('movie', ?, ?, '2024', 'imdb', "
        "        '2026-05-23T00:00:00', '2026-05-02T00:00:00')",
        (tmdb_id, title),
    ).lastrowid


def _seed_new_plex_item(conn, *, rk: str, tmdb_id: int, title: str,
                        theme_id: int, has_theme: int = 0):
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, guid_tmdb, title, "
        "   year, has_theme, local_theme_file, folder_path, theme_id, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, '1', 'movie', ?, ?, 2024, ?, 0, "
        "        '/data/movies/X (2024)', ?, "
        "        '2026-06-01T00:00:00', '2026-06-01T00:00:00')",
        (rk, tmdb_id, title, has_theme, theme_id),
    )


def test_fires_for_new_src_dash_row(tmp_path, monkeypatch):
    """A genuinely-new Plex item with a resolved TDB theme and no
    theme anywhere → one dispatch with the right event_kind."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        tid = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=tid)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert len(captured) == 1
    assert captured[0]["event_kind"] == "new_tdb_theme_available"
    assert "Alpha" in captured[0]["title"]


def test_silent_when_event_disabled(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        tid = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=tid)
        conn.commit()
    captured = _patch(monkeypatch, enabled=False)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert captured == []


def test_dedupe_fires_once_per_title(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        tid = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=tid)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert len(captured) == 1, (
        "v1.21.5: per-(mt,tmdb) dedupe must suppress the second fire"
    )


def test_excluded_when_placement_exists(tmp_path, monkeypatch):
    """A row motif already placed a theme on is NOT SRC=— → skip."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        tid = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=tid)
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, media_folder, placed_at, "
            "   placement_kind) "
            "VALUES ('movie', 111, '1', '/data/movies/X (2024)', "
            "        '2026-06-01T00:00:00', 'hardlink')"
        )
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert captured == [], "placed row is not SRC=— — must not fire"


def test_excluded_when_plex_serves(tmp_path, monkeypatch):
    """has_theme=1 (Plex serving its own theme) → not SRC=— → skip."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        tid = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=tid, has_theme=1)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1"])
    assert captured == [], "Plex-served row is P, not — — must not fire"


def test_multiple_new_rows_collapse_into_one_digest(tmp_path, monkeypatch):
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        t1 = _seed_section_and_theme(conn, tmdb_id=111, title="Alpha")
        t2 = _seed_section_and_theme(conn, tmdb_id=222, title="Beta")
        _seed_new_plex_item(conn, rk="new1", tmdb_id=111,
                            title="Alpha", theme_id=t1)
        _seed_new_plex_item(conn, rk="new2", tmdb_id=222,
                            title="Beta", theme_id=t2)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_theme_available(db_path, ["new1", "new2"])
    assert len(captured) == 1, "two new rows → one digest, not two pings"
    assert "2 themes available" in captured[0]["title"]


# ── Version pin (loose; canonical pin in test_v1_13_79) ──────

def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
