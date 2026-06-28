"""v1.24.19 — plex_enum (Plex section refresh) status-bar accuracy.

Two LIVE-OPS accuracy improvements from the status-bar review:

1. SCOPE label in the drawer card head. The card title was the generic
   "// PLEX REFRESH" for every refresh — a full all-libraries scan, a single
   /movies refresh, and a collections-only refresh all read identically.
   run_plex_enum now stamps `detail.scope_label` ("All libraries" / "Movies" /
   "Movies (items only)" / "Movies collections") and ops.js appends it to the
   card kind so the head says WHAT is refreshing. The per-section stage_label
   still names the section actively walked.

2. Indeterminate fetch reads as working. When Plex returns no totalSize the
   fetch bar stays indeterminate (stage_total=0) AND the N/N counter is hidden
   (the drawer shows it only when total>0), so the climbing `received` count
   was invisible and the fetch looked stuck. The fetch progress callback now
   pushes the running count onto the activity feed when total is unknown.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

# Reuse the v1.14.74 fake-PlexClient harness (seeds section "1" = "Movies").
from test_v1_14_74_content_changed_at_delta_gate import (  # noqa: F401
    _FakeClient, _seed_db)

from app.core import plex_enum
from app.core.plex import PlexConfig

REPO = Path(__file__).resolve().parent.parent
ENUM_PY = (REPO / "app" / "core" / "plex_enum.py").read_text()
OPS_JS = (REPO / "app" / "web" / "static" / "ops.js").read_text()

CFG = PlexConfig(url="http://x", token="t", movie_section="1", tv_section="2")


def _op_detail(db_file: Path) -> dict:
    with sqlite3.connect(db_file) as conn:
        row = conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id = 'plex_enum'"
        ).fetchone()
    return json.loads(row[0] or "{}") if row and row[0] else {}


def _run(db_file, monkeypatch, fake, **kwargs):
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    plex_enum.run_plex_enum(db_file, CFG, **kwargs)


# ── #1 scope_label stamped per refresh scope ─────────────────────

def test_scope_label_all_libraries(tmp_path, monkeypatch):
    db = _seed_db(tmp_path, stored_cca=None)
    _run(db, monkeypatch, _FakeClient(live_cca="1700000005"))
    assert _op_detail(db).get("scope_label") == "All libraries"


def test_scope_label_single_section_uses_title(tmp_path, monkeypatch):
    db = _seed_db(tmp_path, stored_cca=None)
    _run(db, monkeypatch, _FakeClient(live_cca="1700000005"),
         only_section_id="1")
    assert _op_detail(db).get("scope_label") == "Movies"


def test_scope_label_items_only(tmp_path, monkeypatch):
    db = _seed_db(tmp_path, stored_cca=None)
    _run(db, monkeypatch, _FakeClient(live_cca="1700000005"),
         only_section_id="1", skip_collections=True)
    assert _op_detail(db).get("scope_label") == "Movies (items only)"


def test_scope_label_collections_only(tmp_path, monkeypatch):
    db = _seed_db(tmp_path, stored_cca=None)
    _run(db, monkeypatch, _FakeClient(live_cca="1700000005"),
         only_section_id="1", collections_only=True)
    assert _op_detail(db).get("scope_label") == "Movies collections"


# ── #2 indeterminate fetch surfaces the running count ────────────

class _IndetFetchClient(_FakeClient):
    """enumerate_section_items invokes the progress callback with an UNKNOWN
    total (None) — the no-totalSize case where the bar goes indeterminate."""

    def enumerate_section_items(self, **kwargs):
        cb = kwargs.get("progress_callback")
        if cb:
            # Clear the 0.3s emit throttle so the single callback emits.
            time.sleep(0.31)
            cb(250, None)  # total unknown
        return super().enumerate_section_items(**kwargs)


def test_indeterminate_fetch_emits_running_count(tmp_path, monkeypatch):
    """When the fetch total is unknown, the running received count must land on
    the activity feed so the indeterminate bar reads as actively working.

    Spied at the update_progress layer (not the finished row) because the
    activity deque is only 5 deep and the post-fetch upsert/verify/done lines
    would evict the fetch entry."""
    db = _seed_db(tmp_path, stored_cca=None)
    fake = _IndetFetchClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)

    recorded: list[str] = []
    real_update = plex_enum.op_progress.update_progress

    def _spy(db_path, op_id, **kw):
        if kw.get("activity"):
            recorded.append(kw["activity"])
        return real_update(db_path, op_id, **kw)

    monkeypatch.setattr(plex_enum.op_progress, "update_progress", _spy)
    plex_enum.run_plex_enum(db, CFG)

    assert any("250 items so far" in a for a in recorded), recorded


def test_known_total_fetch_does_not_spam_activity(tmp_path, monkeypatch):
    """When Plex DOES return a totalSize the counter/real-bar already convey
    progress — the fetch callback must NOT push a redundant '... items so far'
    activity line (activity=None on the known-total branch)."""
    db = _seed_db(tmp_path, stored_cca=None)

    class _KnownTotalClient(_FakeClient):
        def enumerate_section_items(self, **kwargs):
            cb = kwargs.get("progress_callback")
            if cb:
                time.sleep(0.31)
                cb(250, 500)  # total KNOWN
            return super().enumerate_section_items(**kwargs)

    fake = _KnownTotalClient(live_cca="1700000005")
    monkeypatch.setattr(plex_enum, "PlexClient", lambda cfg: fake)
    recorded: list[str] = []
    real_update = plex_enum.op_progress.update_progress

    def _spy(db_path, op_id, **kw):
        if kw.get("activity"):
            recorded.append(kw["activity"])
        return real_update(db_path, op_id, **kw)

    monkeypatch.setattr(plex_enum.op_progress, "update_progress", _spy)
    plex_enum.run_plex_enum(db, CFG)

    assert not any("items so far" in a for a in recorded), recorded


# ── source pins ──────────────────────────────────────────────────

def test_scope_label_stamped_in_enum_source():
    assert '"scope_label", _scope_summary' in ENUM_PY
    assert '_scope_summary = "All libraries"' in ENUM_PY
    assert '_scope_summary = f"{_sec_title} (items only)"' in ENUM_PY
    assert '_scope_summary = f"{_sec_title} collections"' in ENUM_PY


def test_fetch_activity_gated_on_unknown_total_in_source():
    # the activity is None when total is known, the count string when not.
    assert "items so far" in ENUM_PY
    i = ENUM_PY.index("items so far")
    window = ENUM_PY[i - 200:i]
    assert "None if total else" in window


def test_ops_card_head_appends_scope_label():
    assert "op.detail && op.detail.scope_label" in OPS_JS
    assert "String(op.detail.scope_label).toUpperCase()" in OPS_JS
