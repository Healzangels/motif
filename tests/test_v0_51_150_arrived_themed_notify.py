"""v0.51.150 — "arrived already themed" FYI (plex_item_arrived_themed).

Notification-center Phase 3. Fires when a GENUINELY-NEW Plex item arrives ALREADY
themed by Plex (has_theme=1) that motif doesn't own (no placement, no local file
for the (media_type, tmdb_id)) — the inverse of new_tdb_theme_available (which
fires for new items with NO theme). A pure FYI: nothing to do.

Hooked in plex_enum._upsert_items after the theme-available push, over the rks
INSERTED this enum, gated on `updated > 0` so a section's first enum (all inserts)
stays silent instead of flooding the inbox. Per-(mt, tmdb) 30-day deduped. OFF for
Discord, but records to the in-app INBOX unconditionally of the toggle (the primary
surface) — notify.dispatch records INBOX_EVENT_KINDS before the Apprise gate.

Tests: source pins for the wiring + behavioral against a seeded DB, exercising the
real plex_enum → notify pipe (v1.18.81 discipline) including the real inbox record.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
NOTIFY_CONTENT_PY = REPO / "app" / "core" / "notify_content.py"
NOTIFY_PY = REPO / "app" / "core" / "notify.py"
CONFIG_FILE_PY = REPO / "app" / "core" / "config_file.py"
SETTINGS_HTML = REPO / "app" / "web" / "templates" / "settings.html"


# ── Source pins: formatters + wiring + config ────────────────────────────────


def test_formatters_exist():
    src = NOTIFY_CONTENT_PY.read_text()
    for fn in (
        "def format_arrived_themed_title(",
        "def format_arrived_themed_body(",
        "def format_arrived_themed_batch_title(",
        "def format_arrived_themed_batch_body(",
    ):
        assert fn in src, f"missing formatter {fn}"
    # 📺 glyph (distinct from ✨ theme-available), no CTA duplication of siblings.
    assert "📺 New in Plex, already themed" in src
    assert "📺 " in src and "new items arrived already themed" in src


def test_helper_and_baseline_gated_callsite():
    src = PLEX_ENUM_PY.read_text()
    assert "def _maybe_notify_arrived_themed(" in src
    fn_idx = src.index("def _upsert_items(")
    fn_end = src.index("\ndef ", fn_idx + 1)
    body = src[fn_idx:fn_end]
    # baseline gate: only fire on an already-populated section (updated>0), and
    # only for NEW inserts (an arrival is a fresh rk, not a late-link).
    assert "if new_item_rks and updated > 0:" in body
    assert "_maybe_notify_arrived_themed(db_path, new_item_rks)" in body


def test_helper_query_and_dispatch_shape():
    src = PLEX_ENUM_PY.read_text()
    fn_idx = src.index("def _maybe_notify_arrived_themed(")
    nxt = src.find("\ndef ", fn_idx + 1)
    body = src[fn_idx:nxt if nxt != -1 else len(src)]
    # Plex serves a theme, motif has no managed theme for the (mt, tmdb).
    assert "pi.has_theme = 1" in body
    assert "pi.guid_tmdb IS NOT NULL" in body
    assert "NOT EXISTS (SELECT 1 FROM local_files" in body
    assert "NOT EXISTS (SELECT 1 FROM placements" in body
    # dispatch + per-(mt, tmdb) dedupe.
    assert 'event_kind="plex_item_arrived_themed"' in body
    assert 'f"plex_item_arrived_themed:{mt}:{tid}"' in body
    assert "_ndedupe.record_fire(db_path, key)" in body


def test_config_default_off_and_settings_toggle_and_tone():
    cfg = CONFIG_FILE_PY.read_text()
    idx = cfg.index("_DEFAULT_NOTIFY_EVENTS")
    block = cfg[idx:cfg.index("}", idx)]
    assert '"plex_item_arrived_themed": False' in block
    html = SETTINGS_HTML.read_text()
    assert 'data-cfg-field="notifications.events.plex_item_arrived_themed"' in html
    assert "📺 ARRIVED ALREADY THEMED" in html
    assert '"plex_item_arrived_themed":' in NOTIFY_PY.read_text()


def test_kind_is_in_inbox_allowlist():
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    assert "plex_item_arrived_themed" in INBOX_EVENT_KINDS


# ── Behavioral: the real plex_enum → notify pipe ─────────────────────────────


class _FakeNotif:
    def __init__(self, enabled: bool):
        self.events = {"plex_item_arrived_themed": enabled}
        self.apprise_urls: list[str] = []
        self.apprise_external_url = ""


class _FakeCfg:
    def __init__(self, enabled: bool):
        self.notifications = _FakeNotif(enabled)


def _make_fake_settings(enabled: bool):
    class _FakeSettings:
        def __init__(self, *a, **k):
            self.cfg = _FakeCfg(enabled)
    return _FakeSettings


def _patch(monkeypatch, *, enabled: bool):
    import app.config as _appconfig
    import app.core.notify as _notify
    monkeypatch.setattr(_appconfig, "Settings", _make_fake_settings(enabled))
    captured: list[dict] = []

    def _cap(db, notif, *, event_kind, title, body, body_format="text"):
        captured.append({"event_kind": event_kind, "title": title, "body": body})

    monkeypatch.setattr(_notify, "dispatch", _cap)
    return captured


def _seed_section(conn):
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections (section_id, title, type, is_anime,"
        " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES ('1','Movies','movie',0,0,'movies',1,"
        "        '2026-05-01T00:00:00','2026-05-01T00:00:00')"
    )


def _seed_new_item(conn, *, rk, tmdb_id, title, has_theme=1):
    # guid_tmdb NULL when tmdb_id is None.
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, guid_tmdb,"
        " title, year, has_theme, local_theme_file, folder_path, "
        " first_seen_at, last_seen_at) "
        "VALUES (?, '1','movie',?,?,'2024',?,0,'/data/movies/X (2024)',"
        "        '2026-06-01T00:00:00','2026-06-01T00:00:00')",
        (rk, tmdb_id, title, has_theme),
    )


def _db(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def test_fires_for_new_plex_themed_row(tmp_path, monkeypatch):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=1)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    assert len(captured) == 1
    assert captured[0]["event_kind"] == "plex_item_arrived_themed"
    assert "Alpha" in captured[0]["title"]
    assert "already themed" in captured[0]["title"]


def test_unthemed_new_row_does_not_fire(tmp_path, monkeypatch):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=0)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    assert captured == []


def test_null_guid_does_not_fire(tmp_path, monkeypatch):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=None, title="Alpha", has_theme=1)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    assert captured == []


def test_motif_owned_row_does_not_fire(tmp_path, monkeypatch):
    """A new themed arrival that motif already owns (a placement for the (mt,tmdb))
    is not an external arrival — suppressed."""
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=1)
        conn.execute(
            "INSERT INTO placements (media_type, tmdb_id, section_id, media_folder,"
            " placed_at, placement_kind) "
            "VALUES ('movie', 111, '1', '/data/movies/Alpha (2024)',"
            "        '2026-06-01T00:00:00', 'hardlink')"
        )
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    assert captured == []


def test_batch_of_two_collapses_to_one_digest(tmp_path, monkeypatch):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=1)
        _seed_new_item(conn, rk="p2", tmdb_id=222, title="Beta", has_theme=1)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1", "p2"])
    assert len(captured) == 1
    assert "2 new items arrived already themed" in captured[0]["title"]


def test_per_tmdb_dedupe(tmp_path, monkeypatch):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=1)
        conn.commit()
    captured = _patch(monkeypatch, enabled=True)
    from app.core import plex_enum
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])  # same (mt, tmdb) again
    assert len(captured) == 1  # second call deduped


def test_records_inbox_even_when_discord_toggle_off(tmp_path, monkeypatch):
    """The whole point: the FYI lands in the in-app inbox even though the event
    defaults OFF for Discord. Uses the REAL dispatch (no mock) so the inbox
    record path is exercised."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_new_item(conn, rk="p1", tmdb_id=111, title="Alpha", has_theme=1)
        conn.commit()
    from app.core import plex_enum, notify_inbox
    # real Settings() (event OFF by default) + real notify.dispatch.
    plex_enum._maybe_notify_arrived_themed(db, ["p1"])
    rows = notify_inbox.list_notifications(db)
    assert len(rows) == 1
    assert rows[0]["event_kind"] == "plex_item_arrived_themed"
    assert notify_inbox.count_unread(db) == 1
