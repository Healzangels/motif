"""v0.51.259 — notifications/inbox holistic review, findings 2 + 5.

**2 · MEDIUM — the drawer silently truncated at 50.** `load()` fetched with no
`limit`, taking the endpoint's 50-row default, and the group header rendered
`g.children.length` — the count of what was FETCHED, presented as the total.
After the operator's 77-row restore burst the drawer read "50 themes restored"
with 27 rows invisible and nothing indicating more existed. They were never
lost (they surface as you dismiss), but "showing 50 of 77" is the honest render.
This sharpened with .254/.255: Discord now coalesces a burst into one summary
while the inbox stays per-item on purpose, so large inbox bursts are the
expected case, not the exception.

Two halves, both needed. `total` (every undismissed row) is the denominator the
client had no way to compute; the drawer also asks for the endpoint's 200-row
ceiling, because the burst SIZE is set by Plex, not by us.

**5 · DESIGN — theme_pushed and theme_backed_up never reached the inbox.**
`theme_added` was an inbox kind and its two siblings weren't, so the operator's
72-item bulk PUSH left no in-app trace at all. Both join INBOX_EVENT_KINDS.
theme_pushed's Apprise default stays OFF while its inbox default is ON — the
registries answer different questions ("should this ping Discord" vs "should
this be findable in-app"), and since the inbox records BEFORE the Apprise gate,
the quiet local record lands either way.

The last test here is the drift guard that was missing: app.js's TIER and GROUP
maps are a fourth registry keyed on event_kind, and nothing checked them against
INBOX_EVENT_KINDS. A new kind without entries renders with no tier stripe and a
generic "• N notifications" group label — silently, exactly the contract-drift
sub-pattern CLAUDE.md class 9 describes.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()

AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
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
    return TestClient(create_app(settings)), db


def _seed(db, n, *, kind="theme_auto_restored", dismissed=False):
    with sqlite3.connect(db) as conn:
        for i in range(n):
            conn.execute(
                "INSERT INTO notifications (ts, event_kind, severity, title, "
                "  dismissed_at) VALUES (?, ?, 'info', ?, ?)",
                (f"2026-08-10T00:{i // 60:02d}:{i % 60:02d}", kind,
                 f"🛠 Theme restored — Item {i}",
                 "2026-08-10T01:00:00" if dismissed else None),
            )
        conn.commit()


# ── finding 2: the endpoint returns a denominator ────────────


def test_endpoint_reports_the_true_undismissed_total(client):
    """77 undismissed rows, a 50-row page → the response must say 77 so the
    client can tell a truncated list from a complete one."""
    c, db = client
    _seed(db, 77)
    r = c.get("/api/notifications?limit=50", headers=AUTH)
    assert r.status_code == 200, r.text
    data = r.json()
    assert len(data["notifications"]) == 50
    assert data["total"] == 77, (
        "v0.51.259: `total` is every UNDISMISSED row, not the page size"
    )


def test_total_excludes_dismissed_rows(client):
    """The drawer only ever renders undismissed rows, so the denominator must
    match that scope — a dismissed row inflating it would claim hidden rows
    that no amount of dismissing can reveal."""
    c, db = client
    _seed(db, 5)
    _seed(db, 9, dismissed=True)
    data = c.get("/api/notifications", headers=AUTH).json()
    assert data["total"] == 5
    assert len(data["notifications"]) == 5


def test_total_equals_page_when_nothing_is_truncated(client):
    c, db = client
    _seed(db, 3)
    data = c.get("/api/notifications", headers=AUTH).json()
    assert data["total"] == len(data["notifications"]) == 3


def test_count_undismissed_helper_is_scope_exact(client):
    from app.core import notify_inbox
    _c, db = client
    _seed(db, 4)
    _seed(db, 6, dismissed=True)
    assert notify_inbox.count_undismissed(db) == 4
    # unread is a STRICTER count (undismissed AND unseen) — the two must not
    # be conflated; the badge wants unread, the footer wants undismissed.
    notify_inbox.mark_seen(db)
    assert notify_inbox.count_unread(db) == 0
    assert notify_inbox.count_undismissed(db) == 4


# ── finding 2: the drawer renders it honestly ────────────────


def test_drawer_requests_the_endpoint_ceiling_not_the_default():
    i = APP_JS.index("async function load() {")
    body = APP_JS[i:APP_JS.index("\n    function open()", i)]
    assert "'/api/notifications?limit=200'" in body, (
        "v0.51.259: the bare URL took the 50-row default; a 77-item burst is "
        "the size Plex hands us, not a size we choose"
    )


def test_drawer_renders_showing_n_of_m_only_when_truncated():
    i = APP_JS.index("async function load() {")
    body = APP_JS[i:APP_JS.index("\n    function open()", i)]
    assert "data.total" in body
    assert "notif-more" in body
    # The footer is CONDITIONAL — a complete list must not carry a truncation
    # notice claiming rows are hidden when none are.
    assert "hidden" in body and "? `<li class=\"notif-more\">" in body
    assert "showing ${items.length} of ${total}" in body


def test_notif_more_is_styled():
    assert ".notif-more {" in OPS_CSS
    assert ".notif-more-sub" in OPS_CSS
    # design-system rule: tokens, never hardcoded values.
    block = OPS_CSS[OPS_CSS.index(".notif-more {"):]
    block = block[:block.index("}")]
    assert "var(--fg-mute)" in block and "var(--line)" in block


# ── finding 5: the two missing kinds ─────────────────────────


def test_pushed_and_backed_up_are_inbox_kinds():
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    assert "theme_pushed" in INBOX_EVENT_KINDS
    assert "theme_backed_up" in INBOX_EVENT_KINDS


def test_a_pushed_theme_lands_in_the_inbox_even_with_discord_off(client):
    """The behavioural point of the finding. theme_pushed defaults OFF for
    Apprise; the inbox record happens BEFORE that gate, so a muted-on-Discord
    operator still gets an in-app trace."""
    from app.core import notify, notify_inbox
    from app.core.config_file import NotificationsConfig
    _c, db = client
    cfg = NotificationsConfig(apprise_urls=[], apprise_external_url="")
    assert cfg.events["theme_pushed"] is False, "the fixture assumes the OFF default"
    assert cfg.inbox_events["theme_pushed"] is True
    notify.dispatch(
        db, cfg, event_kind="theme_pushed",
        title="📤 Theme pushed to Plex — Foo (2024)", body="b",
        item_ctx={"media_type": "movie", "tmdb_id": 42})
    rows = notify_inbox.list_notifications(db)
    assert [r["event_kind"] for r in rows] == ["theme_pushed"]
    assert rows[0]["tmdb_id"] == 42, "click-through identity must survive"


def test_backed_up_lands_in_the_inbox(client):
    from app.core import notify, notify_inbox
    from app.core.config_file import NotificationsConfig
    _c, db = client
    notify.dispatch(
        db, NotificationsConfig(), event_kind="theme_backed_up",
        title="💾 Theme backed up — Bar (2024)", body="b")
    assert [r["event_kind"] for r in notify_inbox.list_notifications(db)] \
        == ["theme_backed_up"]


def test_turning_the_inbox_toggle_off_suppresses_the_new_kinds(client):
    """The per-kind allowlist must actually govern the two additions — adding a
    kind without honoring its toggle would be a one-way door."""
    from app.core import notify, notify_inbox
    from app.core.config_file import NotificationsConfig
    _c, db = client
    cfg = NotificationsConfig()
    cfg.inbox_events["theme_pushed"] = False
    notify.dispatch(db, cfg, event_kind="theme_pushed", title="t", body="b")
    assert notify_inbox.list_notifications(db) == []


def test_settings_exposes_both_new_inbox_toggles():
    for kind in ("theme_pushed", "theme_backed_up"):
        assert f'data-cfg-field="notifications.inbox_events.{kind}"' in SETTINGS_HTML


# ── the drift guard that was missing ─────────────────────────


def _js_map_keys(name: str) -> set[str]:
    i = APP_JS.index(f"const {name} = {{")
    block = APP_JS[i:APP_JS.index("\n    };", i)]
    return {
        line.split(":")[0].strip()
        for line in block.splitlines()[1:]
        if ":" in line and not line.strip().startswith("//")
    }


def test_drawer_tier_and_group_maps_cover_every_inbox_kind():
    """app.js's TIER + GROUP maps are a FOURTH registry keyed on event_kind and
    nothing checked them. A kind missing from TIER renders with no tier stripe;
    missing from GROUP, a burst collapses to a generic '• N notifications'.
    Both fail silently — the contract-drift sub-pattern of CLAUDE.md class 9.
    v0.51.259 adds the guard, since it is the tag that grew the set."""
    from app.core.notify_inbox import INBOX_EVENT_KINDS
    kinds = set(INBOX_EVENT_KINDS)
    for name in ("TIER", "GROUP"):
        missing = sorted(kinds - _js_map_keys(name))
        assert not missing, (
            f"app.js {name} is missing inbox kinds: {missing} — a new "
            f"INBOX_EVENT_KINDS entry needs an entry here too")


def test_v0_51_259_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
