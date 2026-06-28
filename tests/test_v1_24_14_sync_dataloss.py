"""v1.24.14 — holistic-review sync data-loss batch (wave 2/4).

#3 (MED-HIGH): git-differential sync advanced the MOTIF_LAST_SYNC baseline even
   when a changed item's read_json() returned None — a `continue` + errors+=1,
   NOT a raise, so it slipped the v1.22.74 "advance only on success" guard.
   Advancing consumed the changeset → that add/modify was permanently dropped
   (next run diffs from the new HEAD). Now `commit_sync_ok` is also gated on
   stats.errors == 0 (clean in a pure-git run — no index walk), with a
   breadcrumb when read failures hold the baseline back.

#5 (MED): remote-tier _fetch_index trusted a 200-with-bad-body pages.json
   (`total_pages = int(meta.get("pages", 0))` → ([], 0)) — a silent green
   zero-theme sync of a whole media type. Now raises on a non-dict body and
   breadcrumbs a zero-page catalog (the snapshot tier got this in v1.23.67).
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYNC_PY = (REPO / "app" / "core" / "sync.py").read_text()


# ── #3 git baseline advance gated on read errors ─────────────

def test_git_baseline_advance_gated_on_no_read_errors():
    assert "if ran_git_diff and detection_ok and stats.errors == 0:" in SYNC_PY
    # and the pre-fix unconditional-on-detection gate is gone.
    assert "if ran_git_diff and detection_ok:\n" not in SYNC_PY
    # breadcrumb when read failures hold the baseline back (substring is within
    # one source literal — the log string is split across two).
    assert "advancing the git baseline so the next run re-diffs" in SYNC_PY


# ── #5 remote index bad-body / zero-page handling ────────────

class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, payload):
        self._payload = payload

    def get(self, url):
        return _FakeResp(self._payload)


def test_remote_index_raises_on_non_dict_body():
    from app.core.sync import _fetch_index
    # HTML error page that still JSON-decodes to a string:
    with pytest.raises(ValueError):
        _fetch_index(_FakeClient("<html>error</html>"), "http://x", "movies")
    # a list body:
    with pytest.raises(ValueError):
        _fetch_index(_FakeClient([1, 2, 3]), "http://x", "movies")


def test_remote_index_zero_pages_returns_empty_with_breadcrumb():
    from app.core.sync import _fetch_index
    items, failed = _fetch_index(_FakeClient({"pages": 0}), "http://x", "movies")
    assert items == [] and failed == 0
    # the source carries the suspicious-zero-catalog warning.
    assert "pages=0 — syncing ZERO items for this media" in SYNC_PY
