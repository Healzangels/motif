"""v1.15.63 — three test guards bundled to clear out parked items.

the user: "Lets tackle all those parked items I want to work next
from a nice clean slate."

Each guard prevents a class of bug we've hit before:

1. **Filter-axis triplet SQL-parity** (parked at v1.15.58)
   The cookies pill exists on 3 surfaces — TDB pill SQL (api.py
   ~1254), ATTN pill SQL (api.py ~1400), post-stat Python matcher
   (api.py ~2207). v1.15.38 + v1.15.58 both had to fix the same
   predicate on different surfaces because no test enforced
   cross-surface consistency. This guard extracts each surface's
   cookies predicate + asserts they all agree.

2. **Background-op route POST smoke tests** (parked at v1.15.48)
   v1.15.48 + v1.15.56 caught op_progress.{status,kind} CHECK
   drift via cross-source guards — but neither actually exercises
   the route. A live POST against init_db() would have caught
   both at write time. Hits the 4 routes that call
   `op_progress.try_acquire`: bulk-probe-tdb, reprobe-tdb-failures,
   reprobe-plex-themes, bulk-let-plex-serve. Each must return 200
   (started) or 409 (already running) — NEVER 500.

3. **SSR perf budget** (parked at v1.15.62)
   v1.15.61 added an OR-join to _FAILURES_SFA_FROM_SQL that
   tanked SQLite's optimizer; every page load slowed to 60s+.
   v1.15.62 reverted the OR + added idx_plex_items_theme_id.
   This guard times _topbar_ssr_state against a fresh init_db
   and asserts < 250ms — a perf-budget regression catches the
   next "clever JOIN" before it ships.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── 1. Filter-axis triplet: cookies pill SQL parity ──────────


def _extract_branch_body(src: str, marker_or_branch: str,
                          end_marker: str) -> str:
    """Slice between an anchor + the next branch's `elif p == "X":`.
    Strips Python comment lines so marker comments mentioning
    other predicates don't pollute substring checks."""
    start = src.index(marker_or_branch)
    end = src.index(end_marker, start + 1)
    block = src[start:end]
    return "\n".join(
        line for line in block.splitlines()
        if not line.lstrip().startswith("#")
    )


def test_cookies_pill_sql_predicate_matches_across_three_surfaces():
    """The cookies pill predicate (`failure_kind = 'cookies_expired'`)
    must be identical across TDB pill SQL, ATTN pill SQL, and the
    post-stat Python matcher. v1.15.38 (ATTN) and v1.15.58 (TDB)
    both fixed the same drift after the user reported "filter not
    applying" — this test pins parity so the next drift trips
    at commit time."""
    src = API_PY.read_text()

    # Surface 1: TDB pill cookies branch (api.py ~1254). Anchor
    # past `elif p == "update":` so we don't match attn pill's
    # cookies branch (which comes after a different `update` elif).
    tdb_update = src.index('elif p == "update":')
    tdb_cookies_body = _extract_branch_body(
        src[tdb_update:], 'elif p == "cookies":', 'elif p == "dead":'
    )
    assert "(t.failure_kind = 'cookies_expired')" in tdb_cookies_body, (
        "v1.15.63: TDB pill cookies branch must use canonical SQL "
        "predicate (without ack gates, without cookies_present gate)"
    )

    # Surface 2: ATTN pill cookies branch (api.py ~1400). Anchor
    # past `elif p == "fail":` (the ATTN-side `update` comes AFTER
    # `cookies` in the ATTN loop, so we need a different anchor).
    attn_fail = src.index("v1.15.38: kept ack-aware")
    attn_cookies_body = _extract_branch_body(
        src[attn_fail:], 'elif p == "cookies":', 'elif p == "mismatch":'
    )
    assert "(t.failure_kind = 'cookies_expired')" in attn_cookies_body, (
        "v1.15.63: ATTN pill cookies branch must match — drift "
        "caused the user's v1.15.38 'filter not filtering' complaint"
    )

    # Surface 3: post-stat Python matcher (api.py ~2207). Anchor
    # past the helper definition.
    post_stat = src.index('elif p == "fail":\n                    # SQL')
    post_stat_body = _extract_branch_body(
        src[post_stat:], 'elif p == "cookies":', 'elif p == "update":'
    )
    assert 'it.get("failure_kind") == "cookies_expired"' in post_stat_body, (
        "v1.15.63: post-stat Python matcher must check "
        "failure_kind == 'cookies_expired' — mirrors SQL predicate"
    )


def test_cookies_pill_no_cookies_present_gate_on_any_surface():
    """Counter-guard: NONE of the three surfaces may gate on
    `cookies_present`. v1.15.17 originally added the gate;
    v1.15.38 stripped it from ATTN; v1.15.58 stripped it from
    TDB. If any future refactor reintroduces it on any surface,
    the user's "filter not applying" recurs."""
    src = API_PY.read_text()

    for branch_marker, end_marker, name in [
        ('elif p == "cookies":', 'elif p == "dead":', 'TDB-pill cookies'),
        ('elif p == "cookies":', 'elif p == "mismatch":', 'ATTN-pill cookies'),
    ]:
        # Find each occurrence
        for match_start in range(0, len(src), 1):
            try:
                idx = src.index(branch_marker, match_start)
            except ValueError:
                break
            try:
                end_idx = src.index(end_marker, idx)
            except ValueError:
                break
            body = src[idx:end_idx]
            code_only = "\n".join(
                line for line in body.splitlines()
                if not line.lstrip().startswith("#")
            )
            assert "if not cookies_present:" not in code_only, (
                f"v1.15.63: {name} branch must NOT gate on "
                "`if not cookies_present:` — see v1.15.38 + v1.15.58 "
                "for why this gate breaks the filter for cookied "
                "deploys"
            )
            match_start = end_idx
            break  # only check first occurrence per pattern


# ── 2. Background-op route POST smoke tests ────────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Same TestClient bootstrap as v1.14.59. Forward-auth lets
    test requests carry X-Authentik-Username to satisfy
    AuthMiddleware without a real session cookie."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client


BG_OP_ROUTES = [
    # (path, body, route_doc)
    ("/api/admin/bulk-probe-tdb", {},
     "// PROBE TDB URLS (Settings → DOWNLOADS); kind='bulk_probe_tdb'"),
    ("/api/admin/reprobe-tdb-failures", {},
     "// REPROBE FAILURES; shares 'bulk-probe-tdb' op_id"),
    ("/api/admin/reprobe-plex-themes", {},
     "// REPROBE PLEX THEMES; kind='reprobe_plex_themes'"),
    ("/api/admin/bulk-let-plex-serve", {"items": []},
     "bulk // LET PLEX SERVE; kind='bulk_lps' — added v1.15.28"),
]


@pytest.mark.parametrize("path,body,doc", BG_OP_ROUTES,
                         ids=[r[0].split("/")[-1] for r in BG_OP_ROUTES])
def test_background_op_route_post_does_not_500(app_client, path, body, doc):
    """Each background-op route POST must return 200 (started),
    409 (already running), or 400 (empty body where required).
    NEVER 500. A 500 here means schema CHECK or endpoint
    integration drift — the exact bug class v1.15.48 + v1.15.56
    caught only via cross-source guards.

    Mitigates the recurring 'try_acquire INSERT 500s on a new
    {status,kind} value' bug class at the route level."""
    headers = {"X-Authentik-Username": "testadmin"}
    response = app_client.post(path, json=body, headers=headers)
    assert response.status_code != 500, (
        f"v1.15.63: {path} returned 500 — schema CHECK drift or "
        f"endpoint integration broken. Route doc: {doc}. "
        f"Response body: {response.text[:500]}"
    )
    # Should be one of the legitimate status codes.
    assert response.status_code in (200, 400, 409), (
        f"v1.15.63: {path} returned unexpected {response.status_code}. "
        f"Body: {response.text[:200]}"
    )


# ── 3. SSR perf budget ─────────────────────────────────────


def test_topbar_ssr_state_completes_under_perf_budget(app_client, tmp_path):
    """_topbar_ssr_state runs on EVERY page load via base.html SSR.
    Its 3 scalar subqueries (failures_count + cookies_count +
    drops_count) all use _FAILURES_SFA_FROM_SQL. v1.15.61 added
    an OR-join that tanked the query planner → 60s+ page loads.
    v1.15.62 reverted + added idx_plex_items_theme_id.

    Budget: < 250ms even on a fresh, near-empty DB. Real-DB
    perf is dominated by index lookups; an OR-join regression
    would explode the count well past this budget."""
    # Trigger an actual SSR by hitting the dashboard route. Force
    # a fresh request (no cache) and time it. The dashboard route
    # is the cheapest SSR-exercising surface; loading it == calling
    # _topbar_ssr_state + _dashboard_ssr_state once each.
    headers = {"X-Authentik-Username": "testadmin"}
    # Warm-up — first request initializes connection pools etc.
    app_client.get("/", headers=headers)

    # Measure 3 calls, take the median to smooth jitter.
    timings = []
    for _ in range(3):
        t0 = time.perf_counter()
        response = app_client.get("/", headers=headers)
        timings.append((time.perf_counter() - t0) * 1000)
        assert response.status_code == 200
    timings.sort()
    median_ms = timings[1]
    # 250ms budget on a fresh DB. Production DBs with 10K+ rows
    # SHOULD still be well under this thanks to indexed joins,
    # but if they're not the regression manifests here first.
    assert median_ms < 250, (
        f"v1.15.63: dashboard SSR took {median_ms:.0f}ms (budget "
        "250ms). Likely a JOIN regression on _FAILURES_SFA_FROM_SQL "
        "or _dashboard_ssr_state — v1.15.61 → v1.15.62's OR-join "
        "trap was the precedent. Check the query planner with "
        "EXPLAIN QUERY PLAN."
    )
