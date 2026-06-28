"""v1.23.46 — coalesce notifications ONLY for actual bulk actions.

the user (Discord screenshot): three themes he'd fixed one-at-a-time (manual SET
URL / UPLOAD MP3) showed up as a batched, bulk-looking notification. The
coalescer inferred "bulk" purely from a 120s time window, so single actions in
that window collapsed together.

Fix: a `bulk` flag is stamped on the job by the bulk ENDPOINT (via
_enqueue_download / the CSV import) and propagated through the download→place
chain, then read at the worker's notify dispatch. dispatch_coalesced(bulk=False)
— a single action — fires the rich single IMMEDIATELY with its preview;
bulk=True buffers every item into ONE list (no previews).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


REPO = Path(__file__).resolve().parent.parent
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _cfg():
    return SimpleNamespace(
        events={"theme_added": True},
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
        lambda db, cfg, *, event_kind, title, body, body_format="text",
        **kw: sent.append({"title": title, "attach": kw.get("attach_url")}),
    )
    return n, sent


def _added(n, db, label, bulk):
    from app.core import notify_content as nc
    n.dispatch_coalesced(
        db, _cfg(), event_kind="theme_added", item_label=label,
        single_title=f"🎵 Theme added — {label}", single_body="b",
        batch_title_fn=nc.format_theme_added_batch_title,
        batch_body_fn=nc.format_theme_added_batch_body,
        body_format="markdown", window_seconds=3600,
        single_attach_url=f"https://thumb/{label}", bulk=bulk,
    )


# ── behavioral: single immediate (with preview), bulk → one list ──


def test_single_actions_each_send_immediately_with_preview(notify_mod):
    """Three single actions (bulk=False) → three immediate rich notifications,
    each keeping its thumbnail. None are buffered, none collapse."""
    n, sent = notify_mod
    db = Path("/x")
    for label in ("Lady Ballers", "Reality", "The Abyss"):
        _added(n, db, label, bulk=False)
    assert [s["title"] for s in sent] == [
        "🎵 Theme added — Lady Ballers",
        "🎵 Theme added — Reality",
        "🎵 Theme added — The Abyss",
    ]
    # each carries its preview thumbnail.
    assert all(s["attach"] for s in sent)
    # nothing buffered.
    assert not n._COALESCE_BUF.get("theme_added")


def test_bulk_action_collapses_to_one_list_no_preview(notify_mod):
    """A real bulk action (bulk=True) buffers every item — nothing during the
    burst — and flushes ONE summary list with no per-item preview."""
    n, sent = notify_mod
    db = Path("/x")
    for label in ("A", "B", "C"):
        _added(n, db, label, bulk=True)
    assert sent == [], "bulk items buffer; nothing sends during the burst"
    n._flush_coalesced(db, _cfg(), "theme_added")
    assert len(sent) == 1
    assert "3" in sent[0]["title"]            # a count headline
    assert sent[0]["attach"] is None          # no thumbnail on the list


def test_bulk_of_one_flushes_as_rich_single(notify_mod):
    """the user's choice: a bulk action that yields exactly one item flushes as
    the rich single (with preview), not a one-item list."""
    n, sent = notify_mod
    db = Path("/x")
    _added(n, db, "Solo", bulk=True)
    n._flush_coalesced(db, _cfg(), "theme_added")
    assert len(sent) == 1
    assert sent[0]["title"] == "🎵 Theme added — Solo"
    assert sent[0]["attach"] == "https://thumb/Solo"


# ── source pins: the bulk flag is threaded end-to-end ──


def test_enqueue_download_has_bulk_param_and_stamps_payload():
    assert "bulk: bool = False," in SYNC_PY
    i = SYNC_PY.index('payload: dict = {"reason": reason}')
    assert 'payload["bulk"] = True' in SYNC_PY[i:i + 400]


def test_bulk_endpoints_pass_bulk_true():
    # every download-driven bulk endpoint stamps the flag.
    assert API_PY.count("bulk=True,  # v1.23.46") >= 5
    # the CSV import (direct INSERT) stamps it in the payload dict.
    assert '"bulk": True,  # v1.23.46: CSV import' in API_PY


def test_worker_propagates_bulk_download_to_place_and_reads_it():
    # download→place chain carries the flag.
    assert 'if payload.get("bulk"):' in WORKER_PY
    assert '_place_obj["bulk"] = True' in WORKER_PY
    # all three coalesced dispatch sites read it.
    assert WORKER_PY.count('bulk=bool(_p.get("bulk"))') >= 3
    assert 'bulk=bool(payload.get("bulk"))' in WORKER_PY  # theme_backed_up


def test_single_action_path_dispatches_immediately_in_source():
    # the not-bulk branch returns after an immediate dispatch (no buffering).
    i = NOTIFY_PY.index("if not bulk:")
    block = NOTIFY_PY[i:i + 400]
    assert "dispatch(db_path, notifications" in block
    assert "return" in block
