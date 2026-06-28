"""v1.19.8 — bulk download honors selected row's section_id.

the user's 2026-05-24 repro: selected 4 items on /4K tab,
clicked DOWNLOAD TDB BACKUP, saw "+7 QUEUED" in topbar.
Docker logs showed 8 jobs created (4 std-section downloads
+ 4 4K-section sibling-hardlinks). The std-section
downloads were unintended — the user selected on 4K, his
std-section library state shouldn't have been touched.

## Root cause

POST /api/library/download-batch payload was
`{items: [{media_type, tmdb_id}, ...]}` — no section_id.
The backend dropped section context entirely and called
`_enqueue_download(mt, tid)` with no `only_section_id`.
`_enqueue_download` fans out to every section containing
(mt, tid), so multi-section titles (std + 4K) got 2 jobs
per requested item.

## Fix

Frontend includes `section_id: it.section_id` in each item
payload. Backend reads it + passes
`only_section_id=section_id` to `_enqueue_download` if
present. Legacy callers without section_id fall through to
old fan-out behavior (safety net — frontend bumps in same
deploy).

## What's pinned

- Frontend both bulk handlers (DOWNLOAD + DOWNLOAD TDB
  BACKUP) include section_id in items
- Backend reads section_id per item + threads through to
  _enqueue_download
- Markers reference the user's 2026-05-24 repro
- End-to-end: posting items with section_id only enqueues
  jobs for THAT section (not every section the item lives in)
- Backward-compat: posting items WITHOUT section_id falls
  through to old fan-out behavior (safety net)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
APP_JS = REPO / "app" / "web" / "static" / "app.js"
API_PY = REPO / "app" / "web" / "api.py"


# ── Source pins ──────────────────────────────────────────────


def test_frontend_download_handler_includes_section_id():
    """The DOWNLOAD bulk handler must push `section_id` into
    each item payload — without it the backend fans out to
    every section the item lives in."""
    js = APP_JS.read_text()
    # Find the DOWNLOAD-bulk handler's items.push call.
    # Anchor on the comment that explains the fix.
    idx = js.index("v1.19.8: include section_id so the backend")
    block = js[idx:idx + 800]
    assert "section_id: it.section_id" in block, (
        "v1.19.8: DOWNLOAD bulk handler must include "
        "section_id in each item payload"
    )


def test_frontend_tdb_backup_handler_includes_section_id():
    """Same fix for the TDB BACKUP bulk handler."""
    js = APP_JS.read_text()
    # The handler has its own short marker referencing the
    # DOWNLOAD handler's fix.
    backup_idx = js.index("v1.19.8: see DOWNLOAD handler")
    block = js[backup_idx:backup_idx + 800]
    assert "section_id: it.section_id" in block, (
        "v1.19.8: DOWNLOAD TDB BACKUP handler must include "
        "section_id in each item payload"
    )


def test_backend_reads_section_id_from_item():
    """Bulk download-batch endpoint must read section_id from
    each item dict and pass it as only_section_id."""
    src = API_PY.read_text()
    # Anchor on the v1.19.8 marker block.
    idx = src.index("v1.19.8: honor per-item section_id")
    # v1.21.85: widened from 3000 — the per-item rating_key->edition resolve
    # comment block pushed the _enqueue_download call further down.
    # v1.23.63: widened to 4200 — the audit-#16 per-item transaction wrap +
    # its breadcrumb comment pushed the call down again.
    block = src[idx:idx + 4200]
    assert 'it.get("section_id")' in block, (
        "v1.19.8: endpoint must read section_id per item"
    )
    assert "only_section_id=only_section_id" in block, (
        "v1.19.8: endpoint must thread section_id to "
        "_enqueue_download via only_section_id"
    )


def test_backend_calls_enqueue_with_section_id_in_both_branches():
    """Both branches (place=True / place=False) of the bulk
    endpoint must pass only_section_id to _enqueue_download.
    Missing it on either branch would silently re-introduce
    the fan-out bug for that mode."""
    src = API_PY.read_text()
    # Find the bulk endpoint function body.
    fn_idx = src.index("Bulk download-batch by")
    fn_end = src.index('return {"ok": True, "enqueued"', fn_idx)
    fn_body = src[max(0, fn_idx - 5000):fn_end]
    # Both _enqueue_download calls must include only_section_id.
    import re
    calls = re.findall(
        r"_enqueue_download\([^)]+\)",
        fn_body, flags=re.DOTALL,
    )
    assert len(calls) >= 2, (
        f"v1.19.8: expected 2 _enqueue_download calls (place + "
        f"no-place branches); found {len(calls)}"
    )
    for call in calls:
        assert "only_section_id=" in call, (
            f"v1.19.8: every _enqueue_download call in the "
            f"bulk endpoint must include only_section_id; "
            f"missing in: {call[:200]}..."
        )


# ── End-to-end behavioral ────────────────────────────────────


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
    return TestClient(create_app(settings))


AUTH = {"X-Authentik-Username": "testadmin"}


def _settings_db(tmp_path):
    from app.config import Settings
    return Settings(
        config_dir=tmp_path, data_dir=tmp_path / "data",
    ).db_path


def _seed_multi_section_item(conn, *, theme_id=1, tmdb_id=100):
    """Theme + plex_items rows in TWO sections (std=1 + 4K=18) —
    the multi-section shape that triggers fan-out."""
    now = "2026-05-24T00:00:00"
    # Two sections: std + 4K. themes_subdir has a UNIQUE
    # constraint so each needs a distinct value.
    for sid, title, is_4k, subdir in [
        ("1", "Movies", 0, "movies"),
        ("18", "4K Movies", 1, "4kmovies"),
    ]:
        conn.execute(
            "INSERT OR IGNORE INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES (?, ?, 'movie', 0, ?, ?, 1, ?, ?)",
            (sid, title, is_4k, subdir, now, now),
        )
    # Theme + a TDB URL so _enqueue_download has something to
    # enqueue.
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, "
        "   youtube_url, youtube_video_id) "
        "VALUES (?, 'movie', ?, 'Multi-Sec Item', 'imdb', "
        "        ?, ?, "
        "        'https://www.youtube.com/watch?v=ABC123', "
        "        'ABC123')",
        (theme_id, tmdb_id, now, now),
    )
    # Item lives in BOTH sections — multi-section.
    # rk values must be unique across the test DB; suffix with
    # tmdb_id so multiple _seed calls in one test don't collide
    # on the plex_items.rating_key UNIQUE constraint.
    for sid, rk in [("1", f"rk-std-{tmdb_id}"),
                     ("18", f"rk-4k-{tmdb_id}")]:
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, theme_id, "
            "   guid_imdb, guid_tmdb, title, year, has_theme, "
            "   local_theme_file, folder_path, "
            "   plex_independent_theme, first_seen_at, "
            "   last_seen_at) "
            "VALUES (?, ?, 'movie', ?, ?, ?, 'Multi', "
            "        2020, 0, 0, ?, 0, ?, ?)",
            (rk, sid, theme_id, f"tt{tmdb_id}", tmdb_id,
             f"/data/movies/Multi", now, now),
        )


def test_bulk_with_section_id_only_enqueues_that_section(
    admin_client, tmp_path,
):
    """the user's exact repro inverted: post bulk with
    section_id=18 (4K only). Verify only 1 job lands (for
    section 18), NOT 2 (the pre-fix fan-out would have
    enqueued both std + 4K)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_multi_section_item(conn)
        conn.commit()

    r = admin_client.post(
        "/api/library/download-batch",
        headers=AUTH,
        json={
            "items": [{
                "media_type": "movie",
                "tmdb_id": 100,
                "section_id": "18",
            }],
            "place": False,
        },
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("enqueued") == 1, (
        f"v1.19.8: with section_id='18', exactly 1 job must "
        f"be enqueued (only the 4K section); got "
        f"enqueued={data.get('enqueued')}. Pre-fix this "
        f"would have been 2 (std + 4K fan-out)."
    )

    # Verify the actual job's payload carries section_id=18.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        jobs = conn.execute(
            "SELECT section_id, payload FROM jobs "
            "WHERE job_type = 'download' "
            "  AND tmdb_id = 100"
        ).fetchall()
    assert len(jobs) == 1, (
        f"v1.19.8: exactly 1 download job must exist; got "
        f"{len(jobs)}: {[dict(j) for j in jobs]}"
    )
    assert jobs[0]["section_id"] == "18", (
        f"v1.19.8: the one job must target section 18; got "
        f"{jobs[0]['section_id']}"
    )


def test_bulk_without_section_id_falls_back_to_fan_out(
    admin_client, tmp_path,
):
    """Backward-compat safety net: legacy callers (no
    section_id in items) get the old fan-out behavior. The
    frontend bumps to v1.19.8 in the same deploy, so this
    path is the SAFETY NET, not the expected behavior."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_multi_section_item(conn)
        conn.commit()

    r = admin_client.post(
        "/api/library/download-batch",
        headers=AUTH,
        json={
            "items": [{
                "media_type": "movie",
                "tmdb_id": 100,
                # No section_id — legacy shape.
            }],
            "place": False,
        },
    )
    assert r.status_code == 200, r.text
    data = r.json()
    # Pre-fix fan-out: 2 jobs (std + 4K).
    assert data.get("enqueued") == 2, (
        f"v1.19.8: legacy items without section_id should "
        f"fall through to fan-out (2 jobs for std + 4K); "
        f"got enqueued={data.get('enqueued')}"
    )


def test_bulk_with_place_branch_also_honors_section_id(
    admin_client, tmp_path,
):
    """The place=True branch (regular DOWNLOAD bulk, not
    DOWNLOAD TDB BACKUP) must ALSO honor section_id.
    Otherwise fixing one branch would leave the other
    silently broken."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_multi_section_item(conn, theme_id=2, tmdb_id=200)
        conn.commit()

    r = admin_client.post(
        "/api/library/download-batch",
        headers=AUTH,
        json={
            "items": [{
                "media_type": "movie",
                "tmdb_id": 200,
                "section_id": "1",  # std-only
            }],
            "place": True,
        },
    )
    assert r.status_code == 200, r.text
    assert r.json().get("enqueued") == 1, (
        f"v1.19.8: place=True branch must also honor "
        f"section_id; got enqueued={r.json().get('enqueued')}"
    )


# ── v1.19.9: symmetric pin — std → only std (mirror of 4K) ───


def test_bulk_with_std_section_does_not_touch_4k(
    admin_client, tmp_path,
):
    """v1.19.9: explicit symmetric pin. v1.19.8's primary
    test covered 4K → only-4K. This locks the inverse:
    std → only-std (NOT also 4K). Same semantic as the
    4K case, just inverted — but worth pinning explicitly
    so a future patch to _enqueue_download (e.g., a 'auto-
    sibling' optimization) can't asymmetrically break one
    direction without failing this test.

    the user's ask: 'lets make sure that goes for bulk
    actions on standard not impacting 4k as well.'"""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_multi_section_item(conn, theme_id=3, tmdb_id=300)
        conn.commit()

    r = admin_client.post(
        "/api/library/download-batch",
        headers=AUTH,
        json={
            "items": [{
                "media_type": "movie",
                "tmdb_id": 300,
                "section_id": "1",  # standard only
            }],
            "place": False,
        },
    )
    assert r.status_code == 200, r.text
    assert r.json().get("enqueued") == 1, (
        f"v1.19.9 symmetric: with section_id='1' (std), "
        f"exactly 1 job must be enqueued — NOT 2. The 4K "
        f"section must NOT be touched. Got enqueued="
        f"{r.json().get('enqueued')}"
    )
    # Verify the actual job's section_id.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        jobs = conn.execute(
            "SELECT section_id FROM jobs "
            "WHERE job_type = 'download' AND tmdb_id = 300"
        ).fetchall()
    assert len(jobs) == 1
    assert jobs[0]["section_id"] == "1", (
        f"v1.19.9 symmetric: std-scoped bulk must produce "
        f"a section_id='1' job, NOT '18' (4K). Got "
        f"{jobs[0]['section_id']}"
    )


def test_bulk_scope_symmetric_across_sections(
    admin_client, tmp_path,
):
    """v1.19.9 belt-and-suspenders: parameterized check
    across BOTH sections in one run. Asserts the
    enqueued-count + actual-section-id contract holds for
    each section independently. Pins the symmetry as a
    cross-cutting property — if a future patch makes
    _enqueue_download asymmetric (e.g., 'always pair std
    with 4K' optimization that ignores only_section_id
    half the time), this fires loud."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Two test items so the parametric runs don't
        # contaminate each other.
        _seed_multi_section_item(conn, theme_id=4, tmdb_id=400)
        _seed_multi_section_item(conn, theme_id=5, tmdb_id=500)
        conn.commit()

    # Test (tmdb_id, section_id) pairs.
    cases = [
        (400, "1"),    # std
        (500, "18"),   # 4K
    ]
    for tmdb_id, sid in cases:
        r = admin_client.post(
            "/api/library/download-batch",
            headers=AUTH,
            json={
                "items": [{
                    "media_type": "movie",
                    "tmdb_id": tmdb_id,
                    "section_id": sid,
                }],
                "place": False,
            },
        )
        assert r.status_code == 200, (
            f"v1.19.9 (tmdb={tmdb_id}, sid={sid}): {r.text}"
        )
        assert r.json().get("enqueued") == 1, (
            f"v1.19.9 symmetric (tmdb={tmdb_id}, sid={sid}): "
            f"expected 1 job, got {r.json().get('enqueued')}"
        )

    # And final cross-check: jobs table has exactly 2 jobs
    # total (1 per cases entry), each in the expected section.
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        all_jobs = conn.execute(
            "SELECT tmdb_id, section_id FROM jobs "
            "WHERE job_type = 'download' "
            "  AND tmdb_id IN (400, 500) "
            "ORDER BY tmdb_id"
        ).fetchall()
    assert len(all_jobs) == 2
    assert all_jobs[0]["tmdb_id"] == 400 and all_jobs[0]["section_id"] == "1"
    assert all_jobs[1]["tmdb_id"] == 500 and all_jobs[1]["section_id"] == "18"
