"""v1.20.63 — class-9 silent-fail hardening bundle.

Four breadcrumb/flush gaps surfaced by the deferred-audit verification:

1. **notify shutdown-flush** (notify.py): the coalescer holds pending
   tail notifications in DAEMON threading.Timers, killed mid-batch at
   interpreter exit. With the v1.20.44 window at 120s, a bulk place
   finishing < 2 min before a redeploy silently lost its batch summary.
   New `flush_all_coalesced()` drains them synchronously; wired into the
   main.py SIGTERM/SIGINT handler.
2. **scanner.py stat()** (~217): a theme file present on the walk but
   un-stattable was dropped from findings with no breadcrumb.
3. **api.py _annotate_canonical_state** (~3296): per-row is_file() OSError
   swallowed → row painted *_missing=True with no log. Hot path → the
   `_FOO_WARNED` first-warn-then-debug pattern.
4. **recovery_v55 json.loads ×3**: cold-path walkers silently skipped
   events/audit rows with unparseable JSON detail → un-recoverable data
   with no signal.
"""
from __future__ import annotations

import types
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# ── 1. notify shutdown-flush (behavioral — the discriminator) ────


@pytest.fixture(autouse=True)
def _clean_coalescer():
    from app.core import notify
    for t in list(notify._COALESCE_TIMERS.values()):
        try:
            t.cancel()
        except Exception:
            pass
    notify._COALESCE_TIMERS.clear()
    notify._COALESCE_BUF.clear()
    notify._COALESCE_ACTIVE.clear()
    yield
    for t in list(notify._COALESCE_TIMERS.values()):
        try:
            t.cancel()
        except Exception:
            pass
    notify._COALESCE_TIMERS.clear()
    notify._COALESCE_BUF.clear()
    notify._COALESCE_ACTIVE.clear()


def _stub_cfg():
    return types.SimpleNamespace(
        events={"theme_pushed": True},
        apprise_urls=["json://localhost/"],
        apprise_external_url="",
    )


def _arm_two(notify, calls):
    cfg = _stub_cfg()
    db = Path("/tmp/x.db")
    common = dict(
        event_kind="theme_pushed",
        batch_title_fn=lambda n: f"{n} pushed",
        batch_body_fn=lambda labels, buckets=None: "\n".join(labels),
        bulk=True,  # v1.23.46: only bulk events buffer for shutdown-flush
    )
    # Two bulk items → both buffer + arm the trailing timer (no leading edge).
    notify.dispatch_coalesced(db, cfg, item_label="A",
                              single_title="tA", single_body="bA", **common)
    notify.dispatch_coalesced(db, cfg, item_label="B",
                              single_title="tB", single_body="bB", **common)


def test_flush_all_coalesced_drains_pending_tail(monkeypatch):
    from app.core import notify
    calls = []

    def _mock_dispatch(db_path, notifications, *, event_kind, title, body,
                       body_format="text", _sync=False, **_kw):
        calls.append({"title": title, "_sync": _sync})

    monkeypatch.setattr(notify, "dispatch", _mock_dispatch)
    _arm_two(notify, calls)

    # Precondition: a tail buffered + a timer armed (NOT yet fired).
    assert notify._COALESCE_BUF.get("theme_pushed"), "tail should buffer"
    assert "theme_pushed" in notify._COALESCE_TIMERS

    notify.flush_all_coalesced()

    # The timer is cancelled, the buffer drained, and the tail was
    # dispatched SYNCHRONOUSLY (_sync=True) so it leaves before exit.
    assert not notify._COALESCE_BUF.get("theme_pushed")
    assert "theme_pushed" not in notify._COALESCE_TIMERS
    sync = [c for c in calls if c["_sync"]]
    assert sync and sync[0]["title"] == "2 pushed", (
        f"flush must synchronously dispatch the buffered bulk list; calls={calls}"
    )


def test_flush_all_coalesced_noop_when_empty(monkeypatch):
    from app.core import notify
    calls = []
    monkeypatch.setattr(
        notify, "dispatch",
        lambda *a, _sync=False, **k: calls.append(_sync))
    # Nothing armed → no dispatch, no raise.
    notify.flush_all_coalesced()
    assert calls == []


def test_dispatch_sync_flag_sends_inline(monkeypatch):
    """dispatch(_sync=True) must call _dispatch_inline directly, NOT
    pool-submit (the shutdown path can't rely on the daemon pool)."""
    from app.core import notify
    inline, pooled = [], []
    monkeypatch.setattr(notify, "_dispatch_inline",
                        lambda *a, **k: inline.append(k.get("title")))
    monkeypatch.setattr(notify, "_get_pool",
                        lambda: (_ for _ in ()).throw(AssertionError("pool used")))
    cfg = _stub_cfg()
    notify.dispatch(Path("/tmp/x.db"), cfg, event_kind="theme_pushed",
                    title="T", body="B", _sync=True)
    assert inline == ["T"], "sync dispatch must run _dispatch_inline inline"
    assert pooled == []


def test_main_shutdown_wires_flush():
    src = (REPO / "app" / "main.py").read_text()
    anchor = src.index("def shutdown(signum")
    body = src[anchor:anchor + 1200]
    assert "flush_all_coalesced()" in body, (
        "the SIGTERM/SIGINT handler must drain coalesced notifications"
    )
    assert "from .core import notify" in body


# ── 2/3/4. breadcrumb source pins ────────────────────────────────


def test_scanner_stat_breadcrumb():
    src = (REPO / "app" / "core" / "scanner.py").read_text()
    anchor = src.index("def _classify_and_record(")
    body = src[anchor:anchor + 700]
    assert "scanner: stat() failed on" in body, (
        "the entry-point stat() OSError must log a breadcrumb"
    )
    # The bare `except OSError: return False` must be gone.
    assert "except OSError as e:" in body


def test_api_canon_fs_breadcrumb_behavioral(monkeypatch, caplog):
    """Force is_file() to raise OSError and confirm the row is still
    annotated *_missing=True AND a breadcrumb is logged (not silent)."""
    import logging
    from app.web import api

    # Reset the first-warn flag so this test sees the warning.
    monkeypatch.setattr(api, "_CANON_FS_OSERROR_WARNED", False)

    def _boom(self):
        raise OSError("simulated dead mount")

    monkeypatch.setattr(Path, "is_file", _boom)
    items = [{"file_path": "x.mp3", "media_folder": "/data/x"}]
    with caplog.at_level(logging.WARNING, logger="app.web.api"):
        out = api._annotate_canonical_state(items, themes_dir=Path("/themes"))
    assert out[0]["canonical_missing"] is True
    assert out[0]["placement_missing"] is True
    assert any("OSError" in r.message for r in caplog.records), (
        "OSError on is_file() must leave a breadcrumb, not paint "
        "*_missing silently"
    )


def test_recovery_json_breadcrumbs():
    src = (REPO / "app" / "core" / "recovery_v55.py").read_text()
    # All three walkers now log on unparseable JSON instead of a silent
    # `continue`.
    assert "recovery (lost user_overrides): skipping event" in src
    assert "recovery (lost bulk imports): skipping audit row" in src
    assert "recovery (lost adopts): skipping event" in src
    # The bare silent skip must be gone (no `except (TypeError, ValueError):`
    # immediately followed by a bare continue in these walkers).
    assert src.count("except (TypeError, ValueError) as e:") >= 3


def test_v1_20_63_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
