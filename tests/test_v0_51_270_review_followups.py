"""v0.51.270 — five findings from a review of v0.51.263–269, four of them in
code shipped earlier the same day.

  F1  renderEmpty() hid CLEAR ALL but not MARK ALL READ — the button postdates
      it (v0.51.266) — so an emptied inbox kept offering an action with nothing
      to act on.
  F2  markRead decremented the unread badge; dismiss did not. A dismissed row
      stops counting as unread server-side (count_unread is dismissed_at IS NULL
      AND seen_at IS NULL), so the topbar over-reported until the next
      /api/stats poll. A v0.51.266 regression: before it, opening the drawer
      zeroed the badge outright, so the two paths could not drift.
  F3  the topbar comment still claimed "opening the drawer marks the set seen,
      so the next poll lands here with 0" — untrue since v0.51.266. This is the
      class that caused v0.51.264 (a contract comment the next reader trusts),
      and v0.51.265 shipped specifically to fix an instance of it.
  F4  /readyz cached settings-derived answers on a wall-clock TTL only, so after
      the operator saved the setting that makes it ready it kept reporting
      not-ready for up to 30s. Settings.revision exists for exactly this and
      FolderIndex already uses it.
  F5  the cache wrote its validity keys before its payload, letting a concurrent
      reader pair a fresh timestamp with the previous snapshot.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _fn(name: str) -> str:
    """The named function's body, bounded by the next sibling declaration."""
    i = APP_JS.index(f"function {name}(")
    nxt = APP_JS.find("\n    function ", i + 1)
    nxt2 = APP_JS.find("\n    async function ", i + 1)
    ends = [x for x in (nxt, nxt2) if x != -1]
    # No char-count fallback: end-of-file is a structural bound too, and a
    # fixed window here would be the exact shape the v0.51.261 ratchet exists
    # to stop (it caught this line's first draft).
    return APP_JS[i:min(ends)] if ends else APP_JS[i:]


# ── F1 ───────────────────────────────────────────────────────


def test_render_empty_hides_both_head_actions():
    body = _fn("renderEmpty")
    assert "clearBtn.hidden = true" in body
    assert "readAllBtn.hidden = true" in body, (
        "v0.51.270: an emptied inbox must not keep offering MARK ALL READ")


# ── F2 ───────────────────────────────────────────────────────


def test_dismiss_decrements_the_unread_badge():
    body = _fn("dismiss")
    assert "bumpUnreadBadge(-1)" in body, (
        "dismissing an unread row drops the server-side unread count, so the "
        "badge must drop with it (v0.51.266 regression)")


def test_dismiss_reads_the_unread_class_before_removing_the_row():
    """Order matters: li.remove() first would make the check always false."""
    body = _fn("dismiss")
    assert body.index("classList.contains('unread')") < body.index("li.remove()")


def test_group_dismiss_decrements_once_per_unread_child():
    body = _fn("dismissGroup")
    assert "bumpUnreadBadge(-unread)" in body
    assert body.index("filter((li) => li.classList.contains('unread'))") < \
        body.index("groupLi.remove()"), "count before the group leaves the DOM"


def test_only_unread_rows_decrement():
    """A seen row already contributes 0 to the count — dismissing it must not
    push the badge below the truth."""
    assert "const wasUnread = !!li && li.classList.contains('unread');" in APP_JS
    assert "if (wasUnread) bumpUnreadBadge(-1);" in APP_JS


# ── F3 ───────────────────────────────────────────────────────


def test_the_topbar_comment_no_longer_claims_open_marks_all_seen():
    stale = ("Opening the drawer marks the\n      // set seen, so the next poll "
             "lands here with 0")
    assert stale not in APP_JS, (
        "v0.51.270: that stopped being true at v0.51.266 — a contract comment "
        "the next reader trusts is the v0.51.264 bug class")
    assert "opening the drawer no" in APP_JS and "longer marks anything seen" in APP_JS


# ── F4 / F5 ──────────────────────────────────────────────────


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True, exist_ok=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s


def test_a_settings_save_invalidates_the_readiness_cache(app_env, monkeypatch):
    """THE F4 behaviour: fix the condition, save, and /readyz must tell the
    truth immediately — not after the 30s TTL expires."""
    c, s = app_env
    import app.config as cfg
    broken = {"v": True}
    real = cfg.probe_dir_writable
    monkeypatch.setattr(
        cfg, "probe_dir_writable",
        lambda p: "PermissionError: denied" if broken["v"] else real(p))

    assert c.get("/readyz").status_code == 503        # warm the cache while broken
    broken["v"] = False                              # operator fixes it...
    assert c.get("/readyz").status_code == 503, (
        "sanity: within the TTL and with no settings change, the cache holds")
    object.__setattr__(s, "_revision", s.revision + 1)  # ...and saves settings
    assert c.get("/readyz").status_code == 200, (
        "v0.51.270: a settings save must invalidate the readiness cache — "
        "Settings.revision exists for this and FolderIndex already uses it")


def test_the_cache_publishes_its_payload_before_its_keys():
    """F5: a reader that sees a fresh key must see the matching snapshot."""
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    i = api_py.index("def _readyz_paths(")
    body = api_py[i:api_py.index('@app.get("/readyz")', i)]
    assert body.index('_readyz_cache["checks"] = dict(checks)') < \
        body.index('_readyz_cache["at"] = now'), (
        "publish the payload first; the timestamp is what validates it")


def test_readiness_still_caches_within_a_revision(app_env, monkeypatch):
    """The TTL still does its job — F4 must not turn every scrape into a probe."""
    c, _ = app_env
    import app.config as cfg
    calls = {"n": 0}
    real = cfg.probe_dir_writable
    monkeypatch.setattr(cfg, "probe_dir_writable",
                        lambda p: (calls.__setitem__("n", calls["n"] + 1), real(p))[1])
    for _ in range(5):
        c.get("/readyz")
    assert calls["n"] <= 2


def test_v0_51_270_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
