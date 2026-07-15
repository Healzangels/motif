"""v1.19.95 — coalesce bursty theme_pushed / theme_added notifications.

the user's 2026-05-29 bulk place into the anime section fired ONE
theme_pushed Discord notification PER item (33 of them) → Discord
429 rate-limits + 12s throttles flooding the channel + the LOGS
event stream.

notify.dispatch_coalesced() buffers per-item events for a short
debounce window:
  - a LONE event flushes as the normal rich single notification
    (URL embed intact),
  - ≥2 within the window collapse into ONE summary — a count
    headline ("📤 N themes pushed to Plex") + capped bulleted
    labels, NO URLs (15 URLs would render 15 Discord embed cards,
    the very spam we're collapsing).
"""
from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# ── fixtures: a fake config + dispatch-capture ───────────────


def _cfg(enabled=True, sinks=True):
    return SimpleNamespace(
        events={"theme_pushed": enabled, "theme_added": enabled},
        apprise_urls=(["discord://x/y"] if sinks else []),
        apprise_external_url="",
    )


@pytest.fixture
def notify_mod(monkeypatch):
    from app.core import notify as n
    # Reset coalesce state so tests don't bleed into each other.
    n._COALESCE_BUF.clear()
    n._COALESCE_TIMERS.clear()
    n._COALESCE_ACTIVE.clear()  # v1.19.97: leading-edge flag
    sent = []
    monkeypatch.setattr(
        n, "dispatch",
        lambda db, cfg, *, event_kind, title, body, body_format="text", **_kw:
            sent.append({"event_kind": event_kind, "title": title,
                         "body": body, "body_format": body_format}),
    )
    return n, sent


# ── _dispatch_batch: 1 → single, ≥2 → summary ────────────────


def test_single_item_sends_rich_notification(notify_mod):
    n, sent = notify_mod
    n._dispatch_batch(Path("/x"), _cfg(), "theme_pushed", [{
        "label": "Solo (2024)", "title": "📤 Theme pushed — Solo (2024)",
        "body": "Source: ThemerrDB · YouTube\nhttps://y/1",
        "body_format": "markdown",
        "batch_title_fn": lambda k: f"{k}", "batch_body_fn": lambda l: "",
    }])
    assert len(sent) == 1
    # Single → the rich payload verbatim (title + URL body).
    assert sent[0]["title"] == "📤 Theme pushed — Solo (2024)"
    assert "https://y/1" in sent[0]["body"]


def test_multi_item_collapses_to_one_summary(notify_mod):
    from app.core import notify_content as nc
    n, sent = notify_mod
    items = [{
        "label": f"Show {i} (2024) — via REPLACE TDB",
        "title": f"📤 Theme pushed — Show {i}",
        "body": f"Source: ThemerrDB · YouTube\nhttps://y/{i}",
        "body_format": "markdown",
        "batch_title_fn": nc.format_theme_pushed_batch_title,
        "batch_body_fn": nc.format_theme_pushed_batch_body,
    } for i in range(3)]
    n._dispatch_batch(Path("/x"), _cfg(), "theme_pushed", items)
    assert len(sent) == 1, "3 events must collapse to ONE notification"
    assert sent[0]["title"] == "📤 3 themes pushed to Plex"
    # Bulleted labels present; NO per-item URLs (would spawn embeds).
    assert "* Show 0 (2024) — via REPLACE TDB" in sent[0]["body"]
    assert "https://" not in sent[0]["body"]


# ── dispatch_coalesced: size-cap inline flush ────────────────


def _push(n, cfg, db, label, bulk=False):
    from app.core import notify_content as nc
    n.dispatch_coalesced(
        db, cfg, event_kind="theme_pushed",
        item_label=label,
        single_title=f"📤 Theme pushed — {label}",
        single_body=f"Source: X\nhttps://y/{label}",
        batch_title_fn=nc.format_theme_pushed_batch_title,
        batch_body_fn=nc.format_theme_pushed_batch_body,
        body_format="markdown",
        window_seconds=3600,  # huge — the debounce timer won't fire
        bulk=bulk,
    )


def test_tail_cap_flushes_without_losing_leading(notify_mod):
    """v1.23.46: a bulk burst caps at _COALESCE_MAX_TAIL — pushing MAX_TAIL+1
    bulk items flushes ONE summary of MAX_TAIL at the cap and keeps collecting
    the overflow (the buffer holds the +1th, the window re-arms)."""
    n, sent = notify_mod
    cfg, db = _cfg(), Path("/x")
    for i in range(n._COALESCE_MAX_TAIL + 1):
        _push(n, cfg, db, f"Title {i}", bulk=True)
    assert len(sent) == 1, f"expected one cap summary, got {sent}"
    assert sent[0]["title"] == f"📤 {n._COALESCE_MAX_TAIL} themes pushed to Plex"
    # the overflow item keeps buffering (window stays armed).
    assert n._COALESCE_BUF.get("theme_pushed"), "overflow item still buffered"


def test_burst_is_leading_rich_plus_tail_summary(notify_mod):
    """v1.23.46: a bulk burst of 3 buffers EVERY item (no leading edge) and
    flushes as ONE summary of all 3 when the window closes."""
    n, sent = notify_mod
    cfg, db = _cfg(), Path("/x")
    for i in range(3):
        _push(n, cfg, db, f"Title {i}", bulk=True)
    assert sent == [], "bulk items buffer; nothing sends during the burst"
    n._flush_coalesced(db, cfg, "theme_pushed")
    assert len(sent) == 1
    assert sent[0]["title"] == "📤 3 themes pushed to Plex"


def test_lone_event_sends_immediately(notify_mod):
    """v1.19.97: a lone event dispatches AT CALL TIME as a rich single
    — no window wait, no shutdown-drop. The trailing flush finds an
    empty tail and sends nothing more."""
    n, sent = notify_mod
    cfg, db = _cfg(), Path("/x")
    _push(n, cfg, db, "Only One (2024)")
    assert len(sent) == 1, "lone event must dispatch at call time"
    assert sent[0]["title"] == "📤 Theme pushed — Only One (2024)"
    assert "https://" in sent[0]["body"]
    n._flush_coalesced(db, cfg, "theme_pushed")
    assert len(sent) == 1, "trailing flush of an empty tail sends nothing"


# ── gate: disabled / no-sink events never buffer ─────────────


def test_disabled_event_does_not_buffer(notify_mod):
    n, sent = notify_mod
    _push(n, _cfg(enabled=False), Path("/x"), "Off (2024)")
    assert not n._COALESCE_BUF.get("theme_pushed")
    assert not n._COALESCE_ACTIVE.get("theme_pushed")
    assert sent == [], "disabled event must not send (even the leading edge)"


def test_no_sinks_does_not_buffer(notify_mod):
    n, sent = notify_mod
    _push(n, _cfg(sinks=False), Path("/x"), "NoSink (2024)")
    assert not n._COALESCE_BUF.get("theme_pushed")
    assert not n._COALESCE_ACTIVE.get("theme_pushed")
    assert sent == [], "no-sink event must not send"


# ── notify_content batch + item formatters ───────────────────


def test_batch_title_and_body_formatters():
    from app.core import notify_content as nc
    assert nc.format_theme_pushed_batch_title(5) == "📤 5 themes pushed to Plex"
    assert nc.format_theme_added_batch_title(2) == "🎵 2 themes added"
    body = nc.format_theme_pushed_batch_body([f"T{i}" for i in range(20)])
    # Capped at 15 + overflow line.
    assert "* T0" in body and "* T14" in body
    assert "* T15" not in body
    assert "…and 5 more" in body


def test_item_label_includes_reason_suffix():
    from app.core import notify_content as nc
    ctx = {"display_title": "Blue Box (2024)"}
    assert nc.format_theme_pushed_item(
        ctx, reason="replace_with_themerrdb"
    ) == "Blue Box (2024) — via REPLACE TDB"
    # No reason → bare title.
    assert nc.format_theme_pushed_item(ctx) == "Blue Box (2024)"
    assert nc.format_theme_added_item(ctx) == "Blue Box (2024)"


# ── worker wires the coalescer (source pin) ──────────────────


def test_worker_uses_dispatch_coalesced_for_place():
    worker = (REPO / "app" / "core" / "worker.py").read_text()
    assert worker.count("_notify.dispatch_coalesced(") >= 4, (
        "v1.19.95: both _do_place and _do_place_collection must "
        "route theme_pushed + theme_added through dispatch_coalesced"
    )


def test_v1_19_95_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
