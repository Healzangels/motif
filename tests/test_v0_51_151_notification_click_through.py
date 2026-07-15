"""v0.51.151 — notification drawer click-through to the INFO card.

Phase 4 (enrichment) of the notification center. Per-item notifications now carry
the item's identity (media_type / tmdb_id / section_id) so the drawer row can
click through to that row's INFO card via the ?info_open= deep-link (the same
mechanism /queue's REPROBE OPEN ROW uses). Batch digests carry no identity and
render non-clickable.

The identity is threaded from the per-item dispatch hook sites: dispatch() gains
an `item_ctx` param and dispatch_coalesced() a `single_item_ctx` param (the
ItemContext dict, which already has media_type/tmdb_id/section_id); both pass the
ids to record_notification, which stores them on the (already-existing, nullable)
notifications columns.

Tests: behavioral against a seeded DB (store + list roundtrip; dispatch threading;
batch → NULL) + source pins for the JS click-through + the hook-site wiring.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
PLEX_ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()


class _FakeNotif:
    def __init__(self, enabled=False):
        self.events = {"theme_added": enabled, "plex_item_arrived_themed": enabled}
        self.apprise_urls: list[str] = []
        self.apprise_external_url = ""


def _db(tmp_path):
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    return db


# ── behavioral: storage + list roundtrip ─────────────────────────────────────


def test_record_and_list_carry_identity(tmp_path):
    from app.core import notify_inbox
    db = _db(tmp_path)
    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info",
        title="🎵 Theme added — Alpha (2024)", body="b",
        media_type="movie", tmdb_id=123, section_id="7",
    )
    rows = notify_inbox.list_notifications(db)
    assert len(rows) == 1
    assert rows[0]["media_type"] == "movie"
    assert rows[0]["tmdb_id"] == 123
    assert rows[0]["section_id"] == "7"


def test_record_without_identity_stores_null(tmp_path):
    from app.core import notify_inbox
    db = _db(tmp_path)
    notify_inbox.record_notification(
        db, event_kind="theme_added", severity="info", title="batch digest",
    )
    row = notify_inbox.list_notifications(db)[0]
    assert row["media_type"] is None
    assert row["tmdb_id"] is None


# ── behavioral: dispatch threads the ctx identity ────────────────────────────


def test_dispatch_item_ctx_threads_identity(tmp_path):
    from app.core import notify, notify_inbox
    db = _db(tmp_path)
    notify.dispatch(
        db, _FakeNotif(enabled=False),  # OFF for Apprise; inbox still records
        event_kind="plex_item_arrived_themed",
        title="📺 New in Plex — Beta", body="b", body_format="markdown",
        item_ctx={"media_type": "tv", "tmdb_id": 456, "section_id": "3"},
    )
    row = notify_inbox.list_notifications(db)[0]
    assert (row["media_type"], row["tmdb_id"], row["section_id"]) == ("tv", 456, "3")


def test_dispatch_coalesced_single_item_ctx_threads_identity(tmp_path):
    from app.core import notify, notify_inbox
    db = _db(tmp_path)
    notify.dispatch_coalesced(
        db, _FakeNotif(enabled=False),
        event_kind="theme_added",
        item_label="Alpha",
        single_title="🎵 Theme added — Alpha",
        single_body="b",
        batch_title_fn=lambda n: f"{n} added",
        batch_body_fn=lambda labels: "\n".join(labels),
        body_format="markdown",
        single_item_ctx={"media_type": "movie", "tmdb_id": 789, "section_id": "1"},
        bulk=False,
    )
    row = notify_inbox.list_notifications(db)[0]
    assert (row["media_type"], row["tmdb_id"]) == ("movie", 789)


def test_dispatch_without_ctx_stores_null(tmp_path):
    from app.core import notify, notify_inbox
    db = _db(tmp_path)
    notify.dispatch(
        db, _FakeNotif(enabled=False), event_kind="theme_added",
        title="no ctx", body="b",
    )
    row = notify_inbox.list_notifications(db)[0]
    assert row["media_type"] is None and row["tmdb_id"] is None


# ── source pins: JS click-through + CSS + hook-site wiring ────────────────────


def test_appjs_row_click_through_deeplink():
    i = APP_JS.index("function bindNotifInbox()")
    body = APP_JS[i:i + 6000]
    # per-item rows get identity data-attrs + the clickable class
    assert "notif-clickable" in body
    assert "data-mt=" in body and "data-tid=" in body
    # the click handler builds the info_open deep-link + still dismisses on ×
    assert "params.set('info_open'" in body
    assert "params.set('info_mt'" in body
    assert "window.location.href = `${tab}?${params.toString()}`" in body


def test_opscss_clickable_cursor():
    assert ".notif-row.notif-clickable" in OPS_CSS
    assert "cursor: pointer" in OPS_CSS[OPS_CSS.index(".notif-row.notif-clickable"):]


def test_dispatch_signatures_have_ctx_params():
    assert "item_ctx: dict | None = None," in NOTIFY_PY
    assert "single_item_ctx: dict | None = None," in NOTIFY_PY


def test_hook_sites_thread_ctx():
    # every per-item inbox dispatch site passes the item ctx (4 worker coalesced
    # + 4 plex_enum single); batch digests intentionally do not.
    assert WORKER_PY.count("single_item_ctx=ctx") == 4
    assert PLEX_ENUM_PY.count("item_ctx=_ctx") == 4
