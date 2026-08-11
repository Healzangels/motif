"""v0.51.257 — notifications/inbox holistic review, findings 1 + 3.

1. **flush_all_coalesced drained by TIMER, not by BUFFER (MEDIUM).**
   The shutdown drain iterated `_COALESCE_TIMERS` and recovered
   `(db_path, notifications)` from `timer.args`. A kind holding buffered
   items with NO timer entry was therefore invisible to it — and that is
   exactly the state an `_arm_coalesce_timer` failure leaves behind, the
   case v1.20.0's except handler exists for. The items vanished at exit
   with no breadcrumb, inside the function whose entire job is preventing
   that. The drain now walks `set(_COALESCE_TIMERS) | set(_COALESCE_BUF)`
   and reads config from `_COALESCE_CFG`, written beside the buffer.

2. **_COALESCE_ACTIVE was write-only dead state (LOW).** Set to False in
   two places, read nowhere since v1.23.46 replaced leading-edge
   inference with the explicit `bulk=` flag. Removed. The v1.20.0 guard
   that kept it alive was itself a phantom — see
   `test_v1_20_0_rollover_audit::test_dispatch_coalesced_arm_failure_is_recoverable`.

The finding-1 tests are behavioural: they drive a real arm failure and
assert the batch still lands. Reverting notify.py sends them red.
"""
from __future__ import annotations

import re
import types
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()


@pytest.fixture(autouse=True)
def _clean_coalescer():
    from app.core import notify
    def _reset():
        for t in list(notify._COALESCE_TIMERS.values()):
            try:
                t.cancel()
            except Exception:
                pass
        notify._COALESCE_TIMERS.clear()
        notify._COALESCE_BUF.clear()
        notify._COALESCE_CFG.clear()
    _reset()
    yield
    _reset()


def _cfg():
    return types.SimpleNamespace(
        events={"theme_pushed": True},
        apprise_urls=["json://localhost/"],
        apprise_external_url="",
    )


def _push(notify, db, label, **over):
    kw = dict(
        event_kind="theme_pushed",
        item_label=label,
        single_title=f"t{label}",
        single_body=f"b{label}",
        batch_title_fn=lambda n: f"{n} pushed",
        batch_body_fn=lambda labels, buckets=None: "\n".join(labels),
        bulk=True,
    )
    kw.update(over)
    notify.dispatch_coalesced(db, _cfg(), **kw)


# ── finding 1: the drain follows the BUFFER ──────────────────


def test_arm_failure_leaves_the_batch_drainable(monkeypatch):
    """The discriminator. `_arm_coalesce_timer` raises → the item is in
    `_COALESCE_BUF` with NO `_COALESCE_TIMERS` entry. Pre-fix the shutdown
    drain iterated the timers, so this batch was unreachable and silently
    lost. It must now flush."""
    from app.core import notify
    calls = []
    monkeypatch.setattr(
        notify, "dispatch",
        lambda db, cfg, *, event_kind, title, body, body_format="text",
        _sync=False, **_kw: calls.append({"title": title, "_sync": _sync}))

    def _boom(*_a, **_kw):
        raise RuntimeError("can't start new thread")
    monkeypatch.setattr(notify, "_arm_coalesce_timer", _boom)

    db = Path("/tmp/x.db")
    _push(notify, db, "A")
    _push(notify, db, "B")

    # Precondition: buffered, but NO timer — the un-drainable pre-fix state.
    assert len(notify._COALESCE_BUF.get("theme_pushed") or []) == 2
    assert "theme_pushed" not in notify._COALESCE_TIMERS

    notify.flush_all_coalesced()

    assert not notify._COALESCE_BUF.get("theme_pushed"), "buffer must drain"
    sync = [c for c in calls if c["_sync"]]
    assert sync and sync[0]["title"] == "2 pushed", (
        f"v0.51.257: a timer-less buffer must still flush at shutdown; "
        f"calls={calls}"
    )


def test_config_is_stashed_before_the_append(monkeypatch):
    """The config must be recorded BEFORE the buffer append, otherwise an
    arm failure could still leave items with nowhere to send them."""
    from app.core import notify
    monkeypatch.setattr(
        notify, "dispatch",
        lambda *a, **k: None)
    monkeypatch.setattr(notify, "_arm_coalesce_timer",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    db = Path("/tmp/x.db")
    _push(notify, db, "A")
    assert notify._COALESCE_CFG["theme_pushed"][0] == db


def test_gated_events_never_write_coalescer_state():
    """A disabled event / sink-less config returns before touching any
    coalescer state — so the new _COALESCE_CFG can't accumulate entries
    for kinds that never buffer."""
    from app.core import notify
    off = types.SimpleNamespace(
        events={"theme_pushed": False}, apprise_urls=["json://localhost/"],
        apprise_external_url="")
    nosink = types.SimpleNamespace(
        events={"theme_pushed": True}, apprise_urls=[], apprise_external_url="")
    for cfg in (off, nosink):
        notify.dispatch_coalesced(
            Path("/tmp/x.db"), cfg, event_kind="theme_pushed", item_label="A",
            single_title="t", single_body="b",
            batch_title_fn=lambda n: f"{n} pushed",
            batch_body_fn=lambda labels, buckets=None: "", bulk=True)
    assert notify._COALESCE_CFG == {}
    assert notify._COALESCE_BUF == {}


def test_flush_still_drains_the_normal_timer_path(monkeypatch):
    """Regression lock on the v1.20.63 behaviour the rewrite must keep: a
    kind WITH an armed timer still cancels + drains."""
    from app.core import notify
    calls = []
    monkeypatch.setattr(
        notify, "dispatch",
        lambda db, cfg, *, event_kind, title, body, body_format="text",
        _sync=False, **_kw: calls.append({"title": title, "_sync": _sync}))
    db = Path("/tmp/x.db")
    _push(notify, db, "A")
    _push(notify, db, "B")
    timer = notify._COALESCE_TIMERS["theme_pushed"]
    assert not timer.finished.is_set()

    notify.flush_all_coalesced()

    # `finished` is Timer.cancel()'s signal — is_alive() lags until the
    # thread actually wakes and exits, so it's the wrong thing to read.
    assert timer.finished.is_set(), "the armed timer must be cancelled"
    assert "theme_pushed" not in notify._COALESCE_TIMERS
    assert [c["title"] for c in calls if c["_sync"]] == ["2 pushed"]


def test_missing_config_drop_is_loud():
    """The no-config path is now structurally unreachable, so if it ever
    fires something broke — it must WARN, not vanish (class-9)."""
    idx = NOTIFY_PY.index("def flush_all_coalesced(")
    body = NOTIFY_PY[idx:NOTIFY_PY.index("\ndef ", idx + 1)]
    assert 'log.warning("notify.flush: no config for %s — DROPPING' in body


# ── finding 3: the dead flag is gone ─────────────────────────


def test_coalesce_active_flag_is_removed():
    """Write-only since v1.23.46 dropped leading-edge inference. Nothing
    reads it; nothing may re-add it as a load-bearing signal."""
    from app.core import notify
    assert not hasattr(notify, "_COALESCE_ACTIVE"), (
        "v0.51.257 removed _COALESCE_ACTIVE — it was set in two places and "
        "read in none"
    )
    assert "_COALESCE_ACTIVE" not in NOTIFY_PY


def test_no_test_still_pins_the_dead_flag():
    """The v1.20.0 guard kept the dead assignment alive across 37 tags.
    Fail loud if a future tag re-introduces a source-text pin on it."""
    # Match only LIVE references — an attribute access (`n._COALESCE_ACTIVE`)
    # or a source-text pin (`"_COALESCE_ACTIVE…`). Prose naming the retired
    # flag in a docstring is the archaeology this repo wants kept, and a bare
    # substring check would flag that too (the comment-trap class).
    live = re.compile(r"""[.'"]_COALESCE_ACTIVE""")
    offenders = [
        p.name for p in sorted((REPO / "tests").glob("test_*.py"))
        if p.name != Path(__file__).name and live.search(p.read_text())
    ]
    assert offenders == [], (
        f"v0.51.257: these still reference the removed flag: {offenders}"
    )
