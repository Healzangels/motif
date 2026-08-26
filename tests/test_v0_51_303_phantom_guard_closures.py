"""v0.51.303 — holistic round 2, wave 12: phantom-guard closures.

Three confirmed test-health findings (the v1.18.81 class — guards that
never exercised their pipe):
  1. api_release_latest's corrupt-cache WARNING was pinned by source text
     only. Now driven: invalid JSON in the cache → 200 with latest=None +
     the warning fires; a valid cache round-trips.
  2. The v1.19.38 SRC-axis lint only matched a subset of predicate
     shapes. Generalized here: every CODE occurrence of a bare
     `!it.media_folder` (comments stripped — all 8 current matches are
     historical-fix narration) must carry a plex_upload widening in its
     enclosing statement. Vacuity-guarded on the widened-site count.
  3. POST /api/dry-run had zero behavioral coverage. Now driven: the
     flip round-trips through is_dry_run, the truthiness parse holds,
     and non-admins are rejected.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
AUTH = {"X-Authentik-Username": "testadmin"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir()
    (tmp_path / "motif.yaml").write_text("paths: {}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    return TestClient(create_app(s)), s.db_path


# ── 1. release cache: corrupt + happy paths ──────────────────


def test_corrupt_release_cache_warns_and_degrades(client, caplog):
    import logging
    c, db = client
    cache = db.parent / "cache"
    cache.mkdir(exist_ok=True)
    (cache / "release.json").write_text("{not json")
    with caplog.at_level(logging.WARNING):
        r = c.get("/api/release/latest", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body.get("latest") is None
    assert not body.get("update_available")
    assert any("release" in rec.message.lower() and "cache" in
               rec.message.lower() for rec in caplog.records), (
        "the class-9 fix: a corrupt cache must WARN, not silently no-op")


def test_valid_release_cache_flows_through(client):
    c, db = client
    cache = db.parent / "cache"
    cache.mkdir(exist_ok=True)
    (cache / "release.json").write_text(json.dumps(
        {"tag_name": "v9.9.9", "html_url": "https://x", "published_at": "t"}))
    r = c.get("/api/release/latest", headers=AUTH)
    assert r.status_code == 200
    assert r.json().get("latest") == "v9.9.9"  # tag_name flows verbatim


# ── 2. the generalized SRC-axis placement lint ───────────────

_WIDENINGS = ("placement_kind !== 'plex_upload'",
              "placement_kind === 'plex_upload'",
              "!placed", "isPlexUpload")


def _code_lines():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    out = []
    for n, line in enumerate(js.split("\n"), 1):
        code = line.split("//", 1)[0]   # strip line comments
        out.append((n, code))
    return out


def test_every_bare_media_folder_predicate_is_widened():
    lines = _code_lines()
    bare = re.compile(r"(?<!!)!it\.media_folder")
    violations = []
    for n, code in lines:
        if not bare.search(code):
            continue
        # the widening must appear within the enclosing statement — take a
        # ±6 code-line neighborhood (statements here span at most a few).
        lo = max(0, n - 7)
        hood = "\n".join(c for _, c in lines[lo:n + 6])
        if not any(w in hood for w in _WIDENINGS):
            violations.append(n)
    assert violations == [], (
        f"bare !it.media_folder without a plex_upload widening at lines "
        f"{violations} — the v1.19.38 SRC-axis class: media_folder='' is "
        f"the plex_upload SENTINEL, not 'unplaced'")


def test_widened_site_census_is_not_vacuous():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    n = sum(js.count(w) for w in ("placement_kind === 'plex_upload'",
                                  "placement_kind !== 'plex_upload'"))
    assert n >= 6, (
        f"only {n} widened sites found — the lint's detector may be blind "
        f"(CLAUDE.md documents 7+ SRC-axis placement sites)")


# ── 3. the dry-run endpoint, behaviorally ────────────────────


def test_dry_run_flip_round_trips(client):
    from app.core.db import get_conn
    c, db = client
    r = c.post("/api/dry-run", headers=AUTH, data={"enabled": "true"})
    assert r.status_code == 200 and r.json()["dry_run"] is True
    assert c.get("/api/dry-run", headers=AUTH).json()["dry_run"] is True
    r = c.post("/api/dry-run", headers=AUTH, data={"enabled": "no"})
    assert r.status_code == 200 and r.json()["dry_run"] is False
    assert c.get("/api/dry-run", headers=AUTH).json()["dry_run"] is False


def test_dry_run_truthiness_parse(client):
    c, _db = client
    for raw, want in (("TRUE", True), ("on", True), ("1", True),
                      ("0", False), ("off", False), ("banana", False)):
        r = c.post("/api/dry-run", headers=AUTH, data={"enabled": raw})
        assert r.json()["dry_run"] is want, (raw, want)


def test_dry_run_requires_admin(client):
    c, _db = client
    r = c.post("/api/dry-run", data={"enabled": "true"})   # no auth header
    assert r.status_code in (401, 403)


def test_v0_51_303_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.303: " in init_py
