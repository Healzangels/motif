"""v0.51.220 — a notification carries the exact edition it fired for.

The last gap in the edition-exact click-through arc. v0.51.218 made the card ask which cut
to show; v0.51.219 made the per-row audit/health links carry theirs. Notifications were the
one surface that structurally could not: the `notifications` table had no edition_key
column, so every inbox click-through on a multi-edition title fell to the picker even when
the notice was about ONE specific cut (a theme placed/replaced on the extended edition).

The plumbing is: enrich_item already accepts edition_key (v1.21.76, for the display label)
— now it also keeps the RAW key in the ItemContext, notify.dispatch persists it via
record_notification, and the inbox row emits info_edition on the deep-link. Title-level
digests carry no edition (NULL) and correctly keep falling back to the picker.

Schema v78 adds the nullable column. This exercises the real dispatch → record → list path
(the v1.18.81 phantom-fix rule: a data-flow feature must be tested through the endpoint,
not by asserting the conditional appears in source).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.db import CURRENT_SCHEMA_VERSION, init_db

REPO = Path(__file__).resolve().parent.parent


# ── schema ───────────────────────────────────────────────────────────────────

def test_schema_bumped_to_78():
    assert CURRENT_SCHEMA_VERSION >= 78  # v0.51.277: floor, not mirror


def test_notifications_has_edition_key_column(tmp_path):
    db = tmp_path / "m.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(notifications)")}
    assert "edition_key" in cols


def test_migration_v77_to_v78_adds_the_column_idempotently(tmp_path):
    """A pre-v78 DB (column absent) must gain edition_key, and a re-run must NOT crash on
    the already-present column — the crash-loop the shared _add_column guard exists to
    prevent (a kill between the ADD COLUMN commit and the version stamp re-runs the step)."""
    from app.core.db import _migrate_v77_to_v78
    # a bare pre-v78 notifications table (no edition_key)
    conn = sqlite3.connect(tmp_path / "m.db")
    conn.execute("CREATE TABLE notifications (id INTEGER PRIMARY KEY, ts TEXT)")
    assert "edition_key" not in {r[1] for r in conn.execute("PRAGMA table_info(notifications)")}
    _migrate_v77_to_v78(conn)
    assert "edition_key" in {r[1] for r in conn.execute("PRAGMA table_info(notifications)")}
    _migrate_v77_to_v78(conn)  # idempotent — no "duplicate column name"
    conn.close()


# ── the dispatch → record → list round-trip ──────────────────────────────────

@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _list(db):
    from app.core import notify_inbox
    return notify_inbox.list_notifications(db)


def test_a_per_item_notice_records_its_edition(db):
    from app.core import notify_inbox
    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info", title="🎵 Theme added — X",
        media_type="movie", tmdb_id=120, section_id="1", edition_key="theatrical")
    rows = _list(db)
    assert len(rows) == 1
    assert rows[0]["edition_key"] == "theatrical"


def test_a_batch_digest_records_null_edition(db):
    """A title-level digest names no single cut — edition_key must be NULL so the drawer
    omits info_edition and the card falls back to the picker."""
    from app.core import notify_inbox
    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info", title="Sync added 5 themes")
    assert _list(db)[0]["edition_key"] is None


def test_the_standard_untagged_edition_is_preserved_not_nulled(db):
    """'' is the untagged standard edition, a REAL scope distinct from NULL 'unknown'.
    A theme placed on the standard cut must record '' so the click-through scopes to it."""
    from app.core import notify_inbox
    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info", title="🎵 Theme added — X",
        media_type="movie", tmdb_id=99, section_id="1", edition_key="")
    assert _list(db)[0]["edition_key"] == ""


def test_dispatch_threads_edition_from_item_ctx(db, monkeypatch):
    """The real wire: notify.dispatch reads edition_key off the item_ctx (which enrich_item
    now populates) and passes it to record_notification. Drive dispatch with a minimal ctx
    and a config that records to the inbox."""
    from types import SimpleNamespace
    from app.core import notify
    # events must contain the kind or dispatch early-returns before the inbox block
    # (the unknown-kind typo guard); the inbox record is independent of its True/False.
    cfg = SimpleNamespace(
        events={"theme_added": False}, apprise_urls=[], apprise_external_url="",
        inbox_events={"theme_added": True})
    notify.dispatch(
        db, cfg, event_kind="theme_added", title="🎵 Theme added — X", body=None,
        item_ctx={"media_type": "movie", "tmdb_id": 120, "section_id": "1",
                  "edition_key": "extended edition"})
    rows = _list(db)
    assert rows and rows[0]["edition_key"] == "extended edition"


def test_enrich_item_keeps_the_raw_edition_key_in_ctx(db):
    """The source of the value: enrich_item's ItemContext must carry the raw edition_key,
    not only the ctx['edition'] display label — dispatch persists the raw key."""
    from app.core import notify_content
    ctx = notify_content.enrich_item(
        db, media_type="movie", tmdb_id=120, section_id="1", edition_key="theatrical")
    assert ctx.get("edition_key") == "theatrical"


def test_edition_key_is_a_declared_itemcontext_field():
    """v0.51.221: the raw edition_key is persisted + read in two dispatch sites, so it must
    be a DECLARED field of the ItemContext contract, not an implied one set on a total=False
    dict (which mypy — report-only in CI — flags as 'no key edition_key')."""
    from app.core.notify_content import ItemContext
    assert "edition_key" in ItemContext.__annotations__


# ── the client end ───────────────────────────────────────────────────────────

def test_inbox_row_emits_info_edition_when_present():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    i = js.index("function rowHtml(")
    body = js[i:js.index("function renderEmpty(", i)]
    assert "n.edition_key != null" in body, "'' is a real edition; must be a null check"
    assert 'data-edn=' in body
    # openNotifRow forwards it as info_edition, keyed on presence not truthiness
    r = js.index("function openNotifRow(")
    fn = js[r:js.index("window.location.href", r)]
    assert "'edn' in row.dataset" in fn
    assert "params.set('info_edition'" in fn
