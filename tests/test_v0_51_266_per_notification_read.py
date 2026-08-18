"""v0.51.266 — reading one notification no longer clears unread on all of them,
and Escape stops leaving a ring on the INBOX pill.

The operator: "clicking on one notification looks to be clearing the unread status
on all notifications." It wasn't the click — `mark_seen()` is unconditional
(`WHERE dismissed_at IS NULL AND seen_at IS NULL`, no id) and the drawer's load()
fired it on every OPEN. There was no per-row read state in the model at all: rows
kept an unread highlight for that one viewing and came back seen. So this adds the
missing axis — `mark_seen_one` + `POST /api/notifications/{id}/seen`, a click that
marks THIS row (clickable or not), and `// MARK ALL READ` for the old bulk gesture,
which is now a deliberate action rather than a side effect of looking.

Second fix: `.op-pill:focus-visible` (ops.css) painted an outline on Escape-close.
A mouse click focuses the pill without a ring, but the Escape keypress flips
Chrome's :focus-visible heuristic to "keyboard", so the ring appeared as the drawer
left and stuck — the stuck-highlight class v1.15.131 fixed globally, resurfacing
for the pills that later joined the focus-visible allow-list.
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


def _seed(db, n=3):
    from app.core import notify_inbox
    for i in range(n):
        notify_inbox.record_notification(
            db, event_kind="theme_added", severity="info",
            title=f"🎵 Theme added — Item {i}", body="",
            media_type="movie", tmdb_id=1000 + i, section_id="1")
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        return [r["id"] for r in c.execute(
            "SELECT id FROM notifications ORDER BY id")]


def _unread(db) -> int:
    from app.core import notify_inbox
    return notify_inbox.count_unread(db)


# ── the bug: one read must not clear the rest ────────────────


def test_marking_one_seen_leaves_the_others_unread(client):
    _, db = client
    ids = _seed(db, 3)
    from app.core import notify_inbox
    assert _unread(db) == 3
    assert notify_inbox.mark_seen_one(db, ids[1]) == 1
    assert _unread(db) == 2, (
        "v0.51.266: marking one notification seen must not touch the others")
    with sqlite3.connect(db) as c:
        c.row_factory = sqlite3.Row
        seen = {r["id"]: r["seen_at"] for r in c.execute(
            "SELECT id, seen_at FROM notifications")}
    assert seen[ids[1]] is not None
    assert seen[ids[0]] is None and seen[ids[2]] is None


def test_mark_seen_one_is_idempotent(client):
    _, db = client
    ids = _seed(db, 2)
    from app.core import notify_inbox
    assert notify_inbox.mark_seen_one(db, ids[0]) == 1
    assert notify_inbox.mark_seen_one(db, ids[0]) == 0
    assert _unread(db) == 1


def test_per_row_seen_endpoint(client):
    c, db = client
    ids = _seed(db, 3)
    r = c.post(f"/api/notifications/{ids[0]}/seen", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["marked"] == 1
    assert _unread(db) == 2


def test_seen_endpoint_requires_admin(client):
    c, db = client
    ids = _seed(db, 1)
    assert c.post(f"/api/notifications/{ids[0]}/seen").status_code in (401, 403)
    assert _unread(db) == 1


def test_bulk_seen_still_works_for_mark_all_read(client):
    """The old endpoint survives — it's what // MARK ALL READ calls now."""
    c, db = client
    _seed(db, 3)
    assert c.post("/api/notifications/seen", headers=AUTH).status_code == 200
    assert _unread(db) == 0


def test_marking_seen_does_not_dismiss(client):
    """seen and dismissed are separate axes — a read row stays in the drawer."""
    c, db = client
    ids = _seed(db, 2)
    c.post(f"/api/notifications/{ids[0]}/seen", headers=AUTH)
    from app.core import notify_inbox
    rows = notify_inbox.list_notifications(db)
    assert len(rows) == 2, "a read notification must remain until dismissed"


# ── the drawer no longer marks everything on open ────────────


def test_open_does_not_bulk_mark_seen():
    """THE regression lock. load() must not POST the bulk seen endpoint — that
    call is what made one read clear them all."""
    i = APP_JS.index("async function load() {", APP_JS.index("notif-clear-all"))
    end = APP_JS.index("function open() {", i)
    assert "'/api/notifications/seen'" not in APP_JS[i:end], (
        "v0.51.266: opening the drawer must not mark every row seen")


def test_row_click_marks_that_row_read():
    block = slice_to_next(
        APP_JS, "const anyRow = e.target.closest('.notif-row');", "});")
    assert "closest('.notif-row')" in block and "markRead(" in block, (
        "any row click marks that row read, not only .notif-clickable ones")
    assert "openNotifRow(row)" in block, "click-through (v0.51.151) must survive"


def test_mark_read_posts_the_per_row_endpoint():
    i = APP_JS.index("async function markRead(")
    block = APP_JS[i:APP_JS.index("function bumpUnreadBadge", i)]
    assert "/seen`" in block and "notifications/${" in block
    assert "classList.remove('unread')" in block


def test_mark_all_read_button_exists_and_is_bound():
    assert 'id="notif-mark-all-read"' in BASE_HTML
    assert "// MARK ALL READ" in BASE_HTML
    assert "readAllBtn.addEventListener('click', markAllRead)" in APP_JS
    assert "'/api/notifications/seen'" in slice_to_next(
        APP_JS, "async function markAllRead(", "async function clearAll(")


def test_mark_all_read_reuses_the_clear_all_primitive():
    """Design-system rule: put the existing class on the new surface, don't
    mirror it — only the non-destructive hover tone differs, via a token."""
    i = BASE_HTML.index('id="notif-mark-all-read"')
    open_tag = BASE_HTML.rindex("<button", 0, i)
    assert "notif-clear-all" in BASE_HTML[open_tag:i]
    assert ".notif-clear-all.notif-mark-read:hover" in OPS_CSS
    assert "var(--cyan)" in slice_to_next(
        OPS_CSS, ".notif-clear-all.notif-mark-read:hover", "}")


# ── Escape leaves no ring on the pill ────────────────────────


def test_escape_close_blurs_the_pill():
    block = slice_to_next(
        APP_JS, "if (e.key === 'Escape' && !drawer.hidden)",
        "function bindUploadDialog(")
    assert "pill.blur()" in block, (
        "v0.51.266: Escape flips Chrome's :focus-visible heuristic to keyboard, so "
        "the .op-pill:focus-visible ring paints on close and sticks")
    assert "document.activeElement === pill" in block, (
        "only blur when the pill actually holds focus")


def test_op_pill_focus_visible_rule_still_exists():
    """The blur is the fix, NOT deleting keyboard focus indication — a Tab user
    must still see the ring."""
    assert ".op-pill:focus-visible" in OPS_CSS


def test_v0_51_266_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
