"""v1.19.97 — coalescer leading-edge: lone events instant, only real bursts collapse.

The v1.19.95 code review surfaced two real trade-offs in the pure-
debounce coalescer:
  #1 two UNRELATED lone events within the window collapsed into a
     misleading "2 themes pushed" summary that lost BOTH URL embeds,
  #2 every lone event was delayed the full window (and droppable on
     a restart inside it).

v1.19.97 reworks dispatch_coalesced to leading-edge + trailing tail:
  - the FIRST event of any quiet period sends IMMEDIATELY as the rich
    single (no delay, no drop, URL intact) and opens the window,
  - events landing while the window is open buffer into the tail,
  - on quiet, a tail of exactly one flushes as another rich single
    (two coincidental singles both keep their URLs — #1), a tail of
    ≥2 collapses to one summary (a genuine bulk place — the 33-flood).
"""
from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


def _cfg():
    return SimpleNamespace(
        events={"theme_pushed": True},
        apprise_urls=["discord://x/y"],
        apprise_external_url="",
    )


@pytest.fixture
def notify_mod(monkeypatch):
    from app.core import notify as n
    n._COALESCE_BUF.clear()
    n._COALESCE_TIMERS.clear()
    n._COALESCE_ACTIVE.clear()
    sent = []
    monkeypatch.setattr(
        n, "dispatch",
        lambda db, cfg, *, event_kind, title, body, body_format="text", **_kw:
            sent.append({"title": title, "body": body}),
    )
    return n, sent


def _push(n, db, label, bulk=False):
    from app.core import notify_content as nc
    n.dispatch_coalesced(
        db, _cfg(), event_kind="theme_pushed",
        item_label=label,
        single_title=f"📤 Theme pushed — {label}",
        single_body=f"Source: X\nhttps://y/{label}",
        batch_title_fn=nc.format_theme_pushed_batch_title,
        batch_body_fn=nc.format_theme_pushed_batch_body,
        body_format="markdown",
        window_seconds=3600,  # huge — the trailing timer won't fire mid-test
        bulk=bulk,
    )


# ── #2: lone event is instant, not window-delayed ────────────


def test_lone_event_is_instant_rich(notify_mod):
    n, sent = notify_mod
    _push(n, Path("/x"), "Solo (2024)")
    # Dispatched AT CALL TIME — not waiting on the 3600s window.
    assert [s["title"] for s in sent] == ["📤 Theme pushed — Solo (2024)"]
    assert "https://y/Solo (2024)" in sent[0]["body"]


# ── #1: two coincidental lone events BOTH stay rich ──────────


def test_two_coincidental_singles_both_stay_rich(notify_mod):
    """v1.23.46: two SINGLE actions (bulk=False) each send their OWN rich
    notification IMMEDIATELY — neither buffers, neither collapses to a summary.
    The v1.19.97 leading-edge dance is gone now that single actions never
    coalesce at all (the user: 'only batch an actual bulk action')."""
    n, sent = notify_mod
    db = Path("/x")
    _push(n, db, "First (2024)")
    _push(n, db, "Second (2024)")
    assert len(sent) == 2, "both singles sent immediately, no buffering"
    assert sent[0]["title"] == "📤 Theme pushed — First (2024)"
    assert sent[1]["title"] == "📤 Theme pushed — Second (2024)"
    assert "https://y/Second (2024)" in sent[1]["body"]
    assert all("themes pushed to Plex" not in s["title"] for s in sent)


# ── a genuine bulk action collapses to one list ──────────────


def test_confirmed_burst_collapses_tail_to_summary(notify_mod):
    """v1.23.46: an ACTUAL bulk action (bulk=True) buffers EVERY item — no
    leading edge — and flushes as ONE summary list (no previews)."""
    n, sent = notify_mod
    db = Path("/x")
    for i in range(4):
        _push(n, db, f"T{i}", bulk=True)
    assert sent == [], "bulk items buffer; nothing sends until the window closes"
    n._flush_coalesced(db, _cfg(), "theme_pushed")
    assert len(sent) == 1
    assert sent[0]["title"] == "📤 4 themes pushed to Plex"


# ── buffer clears after a bulk flush ─────────────────────────


def test_bulk_buffer_clears_after_flush(notify_mod):
    """v1.23.46: the bulk buffer empties on flush so the next burst starts
    fresh; a bulk-of-one flushes as the rich single (the user's choice) and a
    later SINGLE action still fires immediately."""
    n, sent = notify_mod
    db = Path("/x")
    _push(n, db, "A", bulk=True)
    n._flush_coalesced(db, _cfg(), "theme_pushed")
    assert not n._COALESCE_BUF.get("theme_pushed")
    assert sent[-1]["title"] == "📤 Theme pushed — A"   # bulk-of-1 → rich single
    _push(n, db, "B")  # single → immediate
    assert sent[-1]["title"] == "📤 Theme pushed — B"


def test_v1_19_97_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
