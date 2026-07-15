"""v1.20.44 — widen the place-notification coalesce window for
place-as-you-go.

the user's 2026-05-30 report: "One problem with the push after download
in the bulk download action is that after every push it sends a
notification where the bulk place would have been in batches."

Root cause: v1.20.40 introduced place-as-you-go — a completed download
chains a SEPARATE place job, so theme_added / theme_pushed dispatches
now interleave with downloads ~15-60s apart instead of bursting back-
to-back at the end of a bulk place. notify.dispatch_coalesced re-arms
its trailing-window timer on every event (notify.py:_arm_coalesce_timer),
so two events coalesce ONLY if the second lands within window_seconds
of the first. With the old 8s window every place landed AFTER the
window closed → each re-led as a fresh rich single = one Discord
message per placed theme.

Fix: raise _COALESCE_WINDOW_S 8.0 → 120.0 so consecutive place-as-you-go
dispatches fall inside the window and coalesce into batches again, the
same way the pre-v1.20.40 end-of-bulk burst did. The leading edge still
fires immediately, so lone placements are unaffected.

Behavioral coverage drives the real coalescer with monkeypatched
dispatch + a deterministic clock so no wall-time sleeps are needed.
"""
from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()


# ── Source pins ──────────────────────────────────────────────


def test_window_widened_to_span_inter_place_gap():
    """The default coalesce window must exceed the place-as-you-go
    inter-place interval (~15-60s). 8.0 can never batch them."""
    from app.core import notify as n
    assert n._COALESCE_WINDOW_S >= 90.0, (
        "v1.20.44: the place-notification coalesce window must span the "
        "place-as-you-go inter-place gap (~15-60s) so a bulk download's "
        "placements batch instead of firing one notification each"
    )


def test_window_comment_references_place_as_you_go():
    """Breadcrumb: the constant's comment must explain WHY it's no
    longer 8s, so a future reader doesn't 'tidy' it back down and
    resurrect the per-place spam."""
    anchor = NOTIFY_PY.index("_COALESCE_WINDOW_S =")
    preamble = NOTIFY_PY[anchor - 1200:anchor]
    assert "v1.20.44" in preamble
    assert "place-as-you-go" in preamble.lower() or "v1.20.40" in preamble


# ── Behavioral: place-as-you-go cadence coalesces ────────────


@pytest.fixture
def notify_harness(monkeypatch):
    """Drive the real coalescer with a deterministic clock so timer
    callbacks fire only when WE advance time — no wall-clock sleeps,
    no flaky races. Returns (module, sent_list, advance_fn)."""
    from app.core import notify as n

    sent: list[dict] = []
    sent_lock = threading.Lock()

    def _capture(db, cfg, *, event_kind, title, body, body_format="text", **_kw):
        # **_kw tolerates dispatch()'s real kwargs (attach_url, _sync, _record_inbox
        # v0.51.147) so the mock stays faithful as the signature grows.
        with sent_lock:
            sent.append({"kind": event_kind, "title": title, "batch": False})

    def _capture_batch(db, cfg, event_kind, tail):
        with sent_lock:
            sent.append({"kind": event_kind, "title": None,
                         "batch": True, "n": len(tail)})

    # Replace the two terminal sinks so we observe leading singles vs
    # batch flushes without hitting Apprise.
    monkeypatch.setattr(n, "dispatch", _capture)
    monkeypatch.setattr(n, "_dispatch_batch", _capture_batch)

    # Swap threading.Timer for a manual one: arming records the
    # callback; advance() fires whatever is currently armed. This
    # models "the window elapsed with no new event."
    armed: dict[str, object] = {}

    class _ManualTimer:
        def __init__(self, interval, fn, args=()):
            self._fn = fn
            self._args = args

        def start(self):
            armed["pending"] = (self._fn, self._args)

        def cancel(self):
            armed.pop("pending", None)

    monkeypatch.setattr(n.threading, "Timer", _ManualTimer)

    def advance():
        """Fire the currently-armed trailing-window timer (the window
        elapsed). Mirrors _flush_coalesced being called by the timer."""
        pending = armed.pop("pending", None)
        if pending is not None:
            fn, a = pending
            fn(*a)

    # Reset coalescer state so prior tests don't leak an open window.
    n._COALESCE_BUF.clear()
    n._COALESCE_ACTIVE.clear()
    n._COALESCE_TIMERS.clear()

    return n, sent, advance


def _fire(n, cfg, db, i, bulk=False):
    from app.core import notify_content as nc
    n.dispatch_coalesced(
        db, cfg, event_kind="theme_added",
        item_label=f"Movie {i}",
        single_title=f"🎬 Theme added — Movie {i}",
        single_body="b",
        batch_title_fn=nc.format_theme_added_batch_title,
        batch_body_fn=nc.format_theme_added_batch_body,
        body_format="markdown",
        bulk=bulk,
    )


def test_place_as_you_go_burst_coalesces_to_one_leading_plus_batch(notify_harness):
    """v1.23.46: a BULK place-as-you-go burst buffers EVERY item (no leading
    edge) and flushes ONE batch of all 10 when the window finally elapses —
    NOT 10 individual notifications. (Pre-v1.23.46 the first led as a rich
    single; that's gone now that bulk is fully list-shaped.)"""
    n, sent, advance = notify_harness
    cfg = SimpleNamespace(
        events={"theme_added": True}, apprise_urls=["discord://x/y"],
        apprise_external_url="")
    db = Path("/x")

    for i in range(10):
        _fire(n, cfg, db, i, bulk=True)

    # Nothing sent during the burst — every bulk item buffered.
    assert sent == [], "bulk items buffer; no leading edge"

    # Window elapses (bulk ended) → flushes as ONE batch of all 10.
    advance()
    batches = [s for s in sent if s["batch"]]
    assert len(batches) == 1
    assert batches[0]["n"] == 10, "the 10 buffered places flush as one batch"


def test_lone_place_still_fires_immediately(notify_harness):
    """A single place (not part of a bulk) must still send its rich
    single IMMEDIATELY — the wider window only delays the trailing
    batch summary, never the leading edge."""
    n, sent, advance = notify_harness
    cfg = SimpleNamespace(
        events={"theme_added": True}, apprise_urls=["discord://x/y"],
        apprise_external_url="")
    db = Path("/x")

    _fire(n, cfg, db, 0)
    # Leading single is out the door before any window elapses.
    assert len(sent) == 1
    assert sent[0]["batch"] is False

    # Window elapses with nothing buffered → no spurious batch.
    advance()
    assert len([s for s in sent if s["batch"]]) == 0


def test_max_tail_still_caps_a_huge_bulk(notify_harness):
    """The wider window must not let the buffer grow unbounded — the
    MAX_TAIL cap still flushes a batch mid-burst so a 1000-item bulk
    yields periodic summaries, not one 999-deep tail."""
    n, sent, advance = notify_harness
    cfg = SimpleNamespace(
        events={"theme_added": True}, apprise_urls=["discord://x/y"],
        apprise_external_url="")
    db = Path("/x")

    # v1.23.46: MAX_TAIL buffered bulk items triggers the early cap-flush.
    for i in range(n._COALESCE_MAX_TAIL + 1):
        _fire(n, cfg, db, i, bulk=True)

    batches = [s for s in sent if s["batch"]]
    assert len(batches) == 1, "MAX_TAIL cap must flush a batch mid-burst"
    assert batches[0]["n"] == n._COALESCE_MAX_TAIL


def test_v1_20_44_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
