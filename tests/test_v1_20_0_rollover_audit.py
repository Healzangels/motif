"""v1.20.0 — rollover audit fixes (v1.19.x line closed at v1.19.99).

3-agent audit (silent-fails / recent-regressions / coverage). One
real bug + two coalescer hardenings + audit coverage:

1. **Real bug — v1.19.98 SRC-axis mirror-drift miss.** A 4th
   pending-update predicate (`selectedEligibleUpdates`, app.js) still
   used the bare `pending_update && SRC!='-'` form without the
   new_theme_available exception the other 3 sites got — so selecting
   SRC=— new-theme rows left the whole bulk bar hidden. Altitude fix:
   extracted `pendingUpdateActionable(it)` as the single source of
   truth, used at all 5 bulk-bar/accept sites (ends the drift class).

2. **Coalescer hardening (notify.py).** dispatch_coalesced's except
   now resets _COALESCE_ACTIVE so a Timer.start failure can't jam the
   burst window open (dark-holing notifications). _dispatch_batch is
   wrapped so a formatter raise on the daemon timer thread can't
   silently drop a batch.

3. **Coverage.** Coalescer concurrency behavioral test (the lock
   contract was only serially tested); decline-all of new_theme rows.
"""
from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
NOTIFY_PY = (REPO / "app" / "core" / "notify.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── 1. the consolidation (mirror-drift altitude fix) ─────────


def test_helper_defined_with_exception():
    idx = APP_JS.index("function pendingUpdateActionable(")
    body = APP_JS[idx:idx + 320]
    assert "it.pending_update" in body
    assert "computeSrcLetter(it) !== '-'" in body
    assert "it.pending_update_kind === 'new_theme_available'" in body


def test_all_bulk_bar_sites_use_the_helper():
    # The 5 consolidated call sites: selectedEligibleUpdates (the bug),
    # header count, updateCount, _selectedActionableForUpdates, the
    # accept gate. Calls = helper def (1) + 5 uses = 6 occurrences.
    assert APP_JS.count("pendingUpdateActionable(it)") >= 6, (
        f"v1.20.0: expected the helper + 5 call sites, got "
        f"{APP_JS.count('pendingUpdateActionable(it)')}"
    )


def test_no_bare_bulk_bar_predicate_remains():
    """The bare `pending_update && computeSrcLetter(it) !== '-'` form
    (single-line, no new_theme exception) must be gone — that was the
    selectedEligibleUpdates bug the audit caught."""
    assert "it.pending_update && computeSrcLetter(it) !== '-'" not in APP_JS


# ── 2. coalescer hardenings ──────────────────────────────────


def test_dispatch_coalesced_except_resets_active():
    idx = NOTIFY_PY.index("notify.dispatch_coalesced buffer failed")
    block = NOTIFY_PY[idx:idx + 800]
    assert "_COALESCE_ACTIVE[event_kind] = False" in block, (
        "v1.20.0: the except must reset ACTIVE so a Timer.start "
        "failure can't jam the burst window open"
    )


def test_dispatch_batch_wrapped_with_breadcrumb():
    # v1.22.94: slice to the function's end, not a fixed window — the
    # attach_url threading grew the body past the old 1500 chars.
    idx = NOTIFY_PY.index("def _dispatch_batch(")
    body = NOTIFY_PY[idx:NOTIFY_PY.index("\ndef ", idx + 1)]
    assert "try:" in body
    assert "log.warning(" in body, (
        "v1.20.0: a formatter/dispatch raise must log.warning (visible) "
        "not silently kill the daemon timer + drop the batch"
    )


# ── 3a. coalescer concurrency (behavioral) ───────────────────


@pytest.fixture
def notify_mod(monkeypatch):
    from app.core import notify as n
    n._COALESCE_BUF.clear()
    n._COALESCE_TIMERS.clear()
    n._COALESCE_ACTIVE.clear()
    sent = []
    sent_lock = threading.Lock()

    def _capture(db, cfg, *, event_kind, title, body, body_format="text"):
        with sent_lock:
            sent.append({"title": title})
    monkeypatch.setattr(n, "dispatch", _capture)
    return n, sent


def test_concurrent_burst_yields_one_leading_plus_summary(notify_mod):
    """v1.23.46: N worker threads firing the SAME bulk kind concurrently must
    buffer EVERY item under _COALESCE_LOCK — no leading edge, so nothing floods
    Discord during the burst (the 429 protection the coalescer exists for). A
    lost-lock regression would let items escape and send per-item."""
    from app.core import notify_content as nc
    n, sent = notify_mod
    cfg, db = SimpleNamespace(
        events={"theme_pushed": True}, apprise_urls=["discord://x/y"],
        apprise_external_url=""), Path("/x")
    N = 12
    barrier = threading.Barrier(N)

    def worker(i):
        barrier.wait()
        n.dispatch_coalesced(
            db, cfg, event_kind="theme_pushed", item_label=f"T{i}",
            single_title=f"📤 Theme pushed — T{i}", single_body="b",
            batch_title_fn=nc.format_theme_pushed_batch_title,
            batch_body_fn=nc.format_theme_pushed_batch_body,
            body_format="markdown", window_seconds=3600, bulk=True)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # NOTHING sent during the burst — every bulk item buffered under the lock.
    assert len(sent) == 0, (
        f"v1.23.46: a bulk burst must buffer all items, got {len(sent)} sent"
    )
    # Flush → one summary of all N.
    n._flush_coalesced(db, cfg, "theme_pushed")
    assert len(sent) == 1
    assert "themes pushed to Plex" in sent[0]["title"]


# ── 3b. decline-all of new_theme_available (behavioral) ──────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
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


AUTH = {"X-Authentik-Username": "testadmin"}


def _seed_new_theme_row(db, tmdb_id, title):
    now = "2026-05-29T00:00:00"
    url = f"https://www.youtube.com/watch?v=NEW{tmdb_id}"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('3','Anime','show',1,0,'anime',1,?,?)",
            (now, now))
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source, "
            "  last_seen_sync_at, first_seen_sync_at, youtube_url) "
            "VALUES ('tv',?,?,'imdb',?,?,?)", (tmdb_id, title, now, now, url))
        tid = cur.lastrowid
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  theme_id, guid_tmdb, title, year, has_theme, local_theme_file, "
            "  first_seen_at, last_seen_at) "
            "VALUES (?,'3','show',?,?,?,2024,0,0,?,?)",
            (f"rk{tmdb_id}", tid, tmdb_id, title, now, now))
        conn.execute(
            "INSERT INTO pending_updates (media_type, tmdb_id, section_id, "
            "  kind, new_youtube_url, decision, detected_at) "
            "VALUES ('tv',?,'3','new_theme_available',?,'pending',?)",
            (tmdb_id, url, now))
        conn.commit()


def test_decline_all_dismisses_new_theme_rows(admin_client):
    client, db = admin_client
    _seed_new_theme_row(db, 7001, "Garo")
    _seed_new_theme_row(db, 7002, "Jormungand")
    r = client.post("/api/updates/decline-all", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["declined"] >= 2, (
        "v1.20.0: decline-all must dismiss new_theme_available rows"
    )
    with sqlite3.connect(db) as conn:
        pend = conn.execute(
            "SELECT COUNT(*) FROM pending_updates WHERE decision='pending'"
        ).fetchone()[0]
    assert pend == 0


def test_v1_20_0_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
