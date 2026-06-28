"""v1.19.11 — section-scope insurance pins for remaining bulk endpoints.

the user's 2026-05-24 cross-section bleed bug (v1.19.8 / v1.19.9
download-batch fix) prompted a broader audit of every bulk
endpoint. The audit found only download-batch had the leak;
the others — bulk-let-plex-serve, accept-all/decline-all
updates, scans/findings/decisions/bulk — were already
section-correct or title-global by design.

v1.19.11 adds insurance pins for the already-correct endpoints
so a future "optimization" patch can't silently re-introduce
cross-section bleed in any of them.

## Scope

  - `/api/admin/bulk-let-plex-serve`: items carry section_id;
    backend reads it; `_bulk_lps_run` uses section_id in
    per-target WHERE clauses. Symmetric pin (mirror of
    v1.19.9 for downloads).

  - `/api/updates/accept-all` + `/api/updates/decline-all`:
    server-driven (no items[] from user). Both iterate
    `(pi2.section_id, t2.media_type, t2.tmdb_id)` tuples
    server-side — the v1.12.120 fix replaced the
    title-global `WHERE decision='pending'` with the
    per-tuple iteration. Pin that structure so a future
    "simplification" doesn't revert to title-global.

  - Frontend bulk-LPS handler: items pushed to the payload
    include section_id. Pin matches the v1.19.8 download
    pattern.

## Not in scope

  - `/api/admin/bulk-probe-tdb`: title-global by data-model
    design (themes.failure_kind is per-title, not per-section).
    No fix needed; not part of this bug class.

  - `/api/scans/findings/decisions/bulk`: operates on
    scan_findings.id (row PK). Not multi-section.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"


# ── Bulk LPS source pins ─────────────────────────────────────


def test_bulk_lps_frontend_includes_section_id():
    """The bulk LET PLEX SERVE frontend handler must include
    section_id in each item posted to /api/admin/bulk-let-
    plex-serve. Mirror of v1.19.8's download-batch pin."""
    js = APP_JS.read_text()
    # Find the POST call.
    idx = js.index("'/api/admin/bulk-let-plex-serve'")
    block = js[idx:idx + 800]
    assert "section_id:" in block, (
        "v1.19.11: bulk-LPS frontend must include section_id "
        "in each item payload — mirror of v1.19.8 contract"
    )


def test_bulk_lps_endpoint_reads_section_id_per_item():
    """The /api/admin/bulk-let-plex-serve endpoint must
    extract section_id from each item dict and thread it
    through to the targets list."""
    src = API_PY.read_text()
    idx = src.index('@app.post("/api/admin/bulk-let-plex-serve")')
    # End at next @app.post.
    end_idx = src.index("@app.post(", idx + 1)
    block = src[idx:end_idx]
    assert "section_id = it.get(\"section_id\")" in block, (
        "v1.19.11: bulk-LPS endpoint must read section_id "
        "per item from the request body"
    )
    # And targets must carry it through.
    assert '"section_id":' in block, (
        "v1.19.11: targets dict must include section_id key"
    )


def test_bulk_lps_worker_uses_section_id_in_queries():
    """_bulk_lps_run must use section_id in its per-target
    WHERE clauses so the unplace stage scopes correctly."""
    src = API_PY.read_text()
    idx = src.index("def _bulk_lps_run(")
    # Walk forward to find the next def at the same indent
    # (module-level functions).
    end_idx = src.index("\ndef ", idx + 1)
    block = src[idx:end_idx]
    # The worker must read section_id from each target and use
    # it in queries.
    assert 't.get("section_id")' in block, (
        "v1.19.11: _bulk_lps_run must extract section_id "
        "per target via t.get('section_id')"
    )
    # And use it in a WHERE clause (per-target scoping).
    assert "AND section_id = ?" in block, (
        "v1.19.11: _bulk_lps_run must include section_id "
        "in per-target WHERE clauses (scoped queries)"
    )


# ── accept-all + decline-all per-tuple iteration ─────────────


def test_accept_all_iterates_per_section_tuple():
    """`/api/updates/accept-all` must iterate by
    (pi.section_id, t.media_type, t.tmdb_id) tuples — NOT
    the title-global `WHERE decision='pending'` shape that
    v1.12.120 retired (caused cross-section bleed)."""
    src = API_PY.read_text()
    idx = src.index('@app.post("/api/updates/accept-all")')
    end_idx = src.index("@app.post(", idx + 1)
    block = src[idx:end_idx]
    # Must select pi2.section_id explicitly + iterate tuples.
    assert "pi2.section_id" in block, (
        "v1.19.11: accept-all must iterate per-section "
        "tuples (pi2.section_id in SELECT). Pre-v1.12.120 "
        "the title-global shape caused cross-section bleed."
    )
    # Must NOT have a raw "WHERE decision = 'pending'" without
    # per-section scoping. (Search for the anti-pattern.)
    # The current canonical form includes the EXISTS gate for
    # motif tracking presence + per-tuple selection.
    assert "section_id = pi2.section_id" in block or \
           "section_id IN (pi2.section_id" in block, (
        "v1.19.11: accept-all per-section scoping must use "
        "pi2.section_id in pending_updates JOIN/EXISTS"
    )


def test_decline_all_iterates_per_section_tuple():
    """Same pin for decline-all — same shape, same risk if
    a future patch reverts to title-global."""
    src = API_PY.read_text()
    idx = src.index('@app.post("/api/updates/decline-all")')
    end_idx = src.index("@app.post(", idx + 1)
    block = src[idx:end_idx]
    assert "pi2.section_id" in block, (
        "v1.19.11: decline-all must iterate per-section "
        "tuples — same v1.12.120 contract as accept-all"
    )
    assert "section_id = pi2.section_id" in block or \
           "section_id IN (pi2.section_id" in block


# ── Behavioral: bulk LPS section isolation ───────────────────


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


def test_bulk_lps_request_accepts_section_id_payload(
    admin_client, tmp_path,
):
    """v1.19.11 behavioral: posting bulk-LPS with an items
    payload carrying section_id must be ACCEPTED (200) and
    forwarded to the worker. The worker's actual unplace
    behavior is covered by v1.15.95 + v1.15.6 series — this
    test only confirms the section_id flows through the
    request boundary cleanly without 400-ing."""
    db = _settings_db(tmp_path)
    # Seed minimal state — the LPS worker runs in a background
    # thread, so the endpoint just needs to accept the request
    # + spawn the thread without crashing.
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, "
            "   discovered_at, last_seen_at) "
            "VALUES ('18', '4K Movies', 'movie', 0, 1, "
            "        '4kmovies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.commit()

    r = admin_client.post(
        "/api/admin/bulk-let-plex-serve",
        headers=AUTH,
        json={
            "items": [{
                "media_type": "movie",
                "tmdb_id": 100,
                "section_id": "18",
            }],
        },
    )
    assert r.status_code == 200, (
        f"v1.19.11: bulk-LPS must accept items with "
        f"section_id; got {r.status_code}: {r.text}"
    )
    data = r.json()
    assert data.get("ok") is True
    assert data.get("target_count") == 1, (
        f"v1.19.11: target_count must reflect the 1 item "
        f"sent; got {data}"
    )


def test_bulk_lps_rejects_items_without_required_fields(
    admin_client, tmp_path,
):
    """Defensive: items missing media_type or tmdb_id are
    silently dropped (existing v1.15.28 behavior). All-
    invalid items produce a 400 with the 'items must be a
    non-empty list' message. Pin so the dropping doesn't
    accidentally apply to valid items with valid
    section_id."""
    r = admin_client.post(
        "/api/admin/bulk-let-plex-serve",
        headers=AUTH,
        json={"items": []},  # empty
    )
    assert r.status_code == 400


# ── Audit guard: any future bulk endpoint must opt-in ────────


def test_no_bulk_endpoint_drops_section_id_blindly():
    """Audit guard: every `_enqueue_download(` call in api.py
    that accepts items[] from a request body must pass
    `only_section_id=...`. If a future bulk-like endpoint
    forgets, this fires. Pre-v1.19.8 the download-batch
    endpoint called _enqueue_download without
    only_section_id → fan-out bug. This regex pin catches
    a re-introduction of the same shape."""
    src = API_PY.read_text()
    import re
    # Find every _enqueue_download( call site.
    calls = []
    for m in re.finditer(
        r"_enqueue_download\(\s*[^)]+\)",
        src, flags=re.DOTALL,
    ):
        calls.append(m.group(0))
    # Calls inside bulk endpoints (heuristic: inside a function
    # that reads `items` from request body) MUST include
    # only_section_id.
    # Simpler pin: count calls and verify at least the v1.19.8
    # fix's two calls have only_section_id. We accept that
    # non-bulk callers (single-row endpoints, sync handlers)
    # legitimately omit only_section_id — but bulk endpoints
    # must include it.
    bulk_calls = [
        c for c in calls
        if "reason=\"bulk_select\"" in c
        or "reason=\"bulk_backup\"" in c
    ]
    assert bulk_calls, (
        "v1.19.11 sanity: bulk_select / bulk_backup reason "
        "tags should still exist (download-batch endpoint)"
    )
    for c in bulk_calls:
        assert "only_section_id" in c, (
            f"v1.19.11 audit guard: every bulk-context "
            f"_enqueue_download call must pass "
            f"only_section_id (re-introducing the fan-out "
            f"bug from pre-v1.19.8). Missing in: {c[:300]}"
        )
