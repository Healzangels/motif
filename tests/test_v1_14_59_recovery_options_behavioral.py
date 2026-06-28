"""v1.14.59 — behavioral test for api_recovery_options.

the user's "Tier 1.2" follow-up from the v1.14.57 audit-close. The
audit's static-text-only guards have shown their limits twice:

  • v1.14.42 HOTFIX — closed-DB crash on every INFO-card open.
    Static guards pinned the SQL strings perfectly but never
    executed the handler. A behavioral test would have caught
    the post-conn-close NameError at write-time.

  • v1.14.47 reorg — emptying the no-fail TRY THIS NEXT branch
    broke 20 prior tests pinning the now-removed option set.
    Behavioral coverage on the response shape would have been
    the single source of truth, not 20 scattered string pins.

This test bootstraps a real TestClient + seeds DB fixtures
covering the contract surfaces:

  1. **No-failure row** (post-v1.14.47): GET returns
     `{"failure_kind": None, "options": [], ...}`. Any future
     reintroduction of options on this branch fails.

  2. **Failure recipe** (e.g. video_private): GET returns the
     static recipe with the right action keys + tones, gated on
     ack state.

  3. **Locally-resolved row** (TDB failed but user override
     succeeded): `resolved=True, resolved_via="user_url"`.

  4. **404 on missing theme**: row not in DB → HTTP 404.

  5. **No-crash sanity** (v1.14.42 hotfix invariant): every
     fixture shape resolves to a 200 response, not a 500.

The auth scaffolding (forward-auth + admin gate) is sized for
this single endpoint — keeps the test self-contained without
forcing a project-wide TestClient fixture refactor.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


# ── Fixtures: per-test isolated DB + TestClient ──────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Bootstrap a Settings-driven app pointing at a fresh tmp DB,
    with forward-auth enabled so we can satisfy AuthMiddleware
    via the X-Authentik-Username header. Reload the api module's
    create_app closure on every fixture call so settings + db_path
    don't leak across tests."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))

    # Force a fresh Settings (the env vars above must be set BEFORE
    # the Settings constructor reads MOTIF_CONFIG_DIR).
    from app.config import Settings
    settings = Settings(config_dir=tmp_path,
                        data_dir=tmp_path / "data")

    db = settings.db_path
    init_db(db)
    init_auth_schema(db)  # creates `admin` / `api_tokens` / `sessions`
    create_admin(db, username="testadmin", password="testpassword")

    from app.web.api import create_app
    app = create_app(settings)
    client = TestClient(app)
    return client, db


def _seed_section(conn: sqlite3.Connection, section_id: str = "1",
                  title: str = "Movies", section_type: str = "movie",
                  themes_subdir: str | None = None) -> None:
    now = now_iso()
    # themes_subdir is UNIQUE — derive from section_id when caller
    # doesn't pass one so multi-section tests don't UNIQUE-collide.
    sub = themes_subdir or f"{section_type}s-{section_id}"
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, 1, 0, 0, ?, ?, ?)",
        (section_id, title, section_type, sub, now, now),
    )


def _seed_theme(conn: sqlite3.Connection, *, media_type: str = "movie",
                tmdb_id: int = 100, title: str = "Test Title",
                upstream: str = "imdb",
                youtube_url: str = "https://youtube.com/watch?v=abc11111111",
                failure_kind: str | None = None,
                failure_message: str | None = None,
                failure_acked_at: str | None = None) -> None:
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    failure_kind, failure_message, "
        "                    failure_at, failure_acked_at, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES (?, ?, ?, 2020, ?, ?, ?, ?, ?, ?, ?, ?)",
        (media_type, tmdb_id, title, upstream, youtube_url,
         failure_kind, failure_message,
         now if failure_kind else None, failure_acked_at,
         now, now),
    )


def _seed_plex_item(conn: sqlite3.Connection, *, rating_key: str = "100",
                    section_id: str = "1", media_type: str = "movie",
                    tmdb_id: int = 100, title: str = "Test Title",
                    has_theme: int = 1, local_theme_file: int = 0,
                    plex_independent_theme: int = 0) -> None:
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, 2020, ?, ?, ?, ?, ?, ?)",
        (rating_key, section_id, media_type, str(tmdb_id), title,
         has_theme, local_theme_file, f"/data/movies/{title}",
         plex_independent_theme, now, now),
    )


def _seed_local_file(conn: sqlite3.Connection, *, media_type: str = "movie",
                     tmdb_id: int = 100, section_id: str = "1",
                     source_kind: str = "url",
                     provenance: str = "manual") -> None:
    now = now_iso()
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, "
        "  file_path, file_size, downloaded_at, source_video_id, "
        "  source_kind, provenance) "
        "VALUES (?, ?, ?, ?, 100, ?, 'aaa11111111', ?, ?)",
        (media_type, tmdb_id, section_id,
         f"theme-{tmdb_id}.mp3", now, source_kind, provenance),
    )


# ── 1. No-failure row → empty options (v1.14.47 contract) ────


def test_no_failure_row_returns_empty_options(app_client):
    """v1.14.47 emptied the `if not kind:` branch — non-failure
    rows now return options=[]. Any future reintroduction of
    options on this branch (a regression of the v1.14.47 reorg)
    fails this test."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, tmdb_id=100, failure_kind=None)
        _seed_plex_item(conn, tmdb_id=100)
        conn.commit()
    r = client.get(
        "/api/items/movie/100/recovery-options",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["failure_kind"] is None
    assert body["options"] == [], (
        f"v1.14.47: no-fail branch must return empty options. "
        f"Got: {body['options']}"
    )
    assert body["acked"] is False
    # rating_key still resolves so PUSH TO PLEX (PLACE menu) can wire up.
    assert body["rating_key"] == "100"


def test_no_failure_row_with_section_id_also_empty_options(app_client):
    """Sanity: passing section_id doesn't change the no-fail
    branch's empty-options contract. The pre-v1.14.42 closed-DB
    bug specifically lived on the section_id branch."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_section(conn, section_id="2", title="4K Movies")
        _seed_theme(conn, tmdb_id=200, failure_kind=None)
        _seed_plex_item(conn, rating_key="200a", section_id="1", tmdb_id=200)
        _seed_plex_item(conn, rating_key="200b", section_id="2", tmdb_id=200)
        conn.commit()
    r = client.get(
        "/api/items/movie/200/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["options"] == []


# ── 2. Failure recipe surfaces per failure_kind ──────────────


def test_video_private_failure_returns_static_recipe(app_client):
    """A row with failure_kind=video_private returns the static
    recipe (SET URL / UPLOAD MP3 / ACK FAILURE). Pin the recipe
    shape so future edits don't silently drop options."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, tmdb_id=300,
                    failure_kind="video_private",
                    failure_message="probe: This video is private")
        _seed_plex_item(conn, rating_key="300", tmdb_id=300, has_theme=0)
        conn.commit()
    r = client.get(
        "/api/items/movie/300/recovery-options",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["failure_kind"] == "video_private"
    actions = {opt["action"] for opt in body["options"]}
    # The video_private recipe (api.py:10395+) ships these 3 actions.
    assert {"manual-url", "upload-theme", "clear-failure"}.issubset(actions)
    # Tones are wired through to the JS TONE_CLASS map. v1.15.17
    # added "cookies" (yellow) for the COOKIES recovery family +
    # v1.15.24 reused it for MARK ALIVE; v1.15.34 split MARK
    # ALIVE off to its own "magenta" tone so operator-override
    # CTAs don't share visual identity with config-repair
    # (cookies) CTAs.
    for opt in body["options"]:
        assert opt["tone"] in {
            "themerrdb", "user", "adopt", "info", "danger", "plex",
            "cookies", "magenta",
        }, f"unexpected tone: {opt['tone']}"


def test_cookies_expired_failure_includes_cookies_action(app_client):
    """The cookies_expired recipe must surface a cookies-related
    primary action.

    v1.13.83 had this as a non-interactive "DROP cookies.txt" info
    tile. v1.15.17 promoted it to an interactive button (FIX
    COOKIES, action='fix-cookies-link', tone='cookies') that
    navigates to /settings#paths where the cookies.txt field +
    // TEST COOKIES button live. The motivation: an info tile is
    visually styled but not clickable — operators had to manually
    navigate. The action button gives a single-click recovery."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, tmdb_id=400,
                    failure_kind="cookies_expired",
                    failure_message="probe: cookies needed")
        _seed_plex_item(conn, rating_key="400", tmdb_id=400, has_theme=0)
        conn.commit()
    r = client.get(
        "/api/items/movie/400/recovery-options",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    options = r.json()["options"]
    # v1.15.17: there must be a FIX COOKIES interactive button
    # tagged with the cookies tone.
    cookies_action = [opt for opt in options
                      if opt.get("action") == "fix-cookies-link"]
    assert cookies_action, (
        "v1.15.17: cookies_expired must include a fix-cookies-link "
        "action button"
    )
    fix = cookies_action[0]
    assert fix["interactive"] is True, (
        "v1.15.17: FIX COOKIES is interactive (was non-interactive "
        "info tile pre-v1.15.17)"
    )
    assert fix["tone"] == "cookies", (
        "v1.15.17: FIX COOKIES uses the cookies tone (yellow)"
    )
    # Label / tooltip still reference cookies for clarity.
    assert ("cookies" in fix["label"].lower()
            or "cookies" in fix["tooltip"].lower())


# ── 3. Locally-resolved row (v1.13.33 contract) ──────────────


def test_locally_resolved_row_marks_resolved(app_client):
    """v1.13.33 (refined v1.13.34/35): when a row has a healthy
    user-override canonical (source_kind in 'url'/'upload'/'adopt'),
    the response carries `resolved=True, resolved_via=...` so the
    INFO card renders "✓ RESOLVED — TDB UNAVAILABLE" instead of
    pushing more failure-recovery actions."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, tmdb_id=500,
                    failure_kind="video_removed",
                    failure_message="probe: video removed")
        _seed_plex_item(conn, rating_key="500", tmdb_id=500)
        # User-override canonical exists — locally resolved.
        _seed_local_file(conn, tmdb_id=500, source_kind="url")
        conn.commit()
    r = client.get(
        "/api/items/movie/500/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["resolved"] is True
    assert body["resolved_via"] in ("url", "upload", "adopt")


# ── 4. 404 on missing row ────────────────────────────────────


def test_missing_theme_returns_404(app_client):
    """Sanity: non-existent (mt, tmdb_id) returns 404, not 500."""
    client, _db = app_client
    r = client.get(
        "/api/items/movie/99999/recovery-options",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 404, r.text


# ── 5. No-crash invariant (v1.14.42 hotfix) ──────────────────


@pytest.mark.parametrize(
    "fixture_name",
    ["plain_no_failure", "video_private", "cookies_expired",
     "network_error", "unknown", "video_removed",
     "geo_blocked", "video_age_restricted",
     "no_plex_item", "no_local_files"],
)
def test_no_crash_across_all_fixture_shapes(app_client, fixture_name):
    """v1.14.42 hotfix invariant: api_recovery_options must NOT
    raise a 500 on any reasonable row shape. Pre-fix the LPS-
    detection query lived OUTSIDE the with get_conn block →
    sqlite3.ProgrammingError on every INFO-card open. Static-
    text guards passed; runtime broke. This parametrized test
    sweeps the failure_kind enum + a few state shapes to lock
    down "no 500s, regardless of row shape" as a contract."""
    client, db = app_client
    tmdb_id = 1000 + hash(fixture_name) % 1000
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        kind_map = {
            "plain_no_failure": (None, None),
            "video_private": ("video_private", "private"),
            "cookies_expired": ("cookies_expired", "cookies"),
            "network_error": ("network_error", "network"),
            "unknown": ("unknown", "unknown"),
            "video_removed": ("video_removed", "removed"),
            "geo_blocked": ("geo_blocked", "geo"),
            "video_age_restricted": ("video_age_restricted", "age"),
            "no_plex_item": (None, None),
            "no_local_files": ("video_private", "private"),
        }
        kind, msg = kind_map[fixture_name]
        _seed_theme(conn, tmdb_id=tmdb_id, failure_kind=kind,
                    failure_message=msg)
        if fixture_name != "no_plex_item":
            _seed_plex_item(conn, rating_key=str(tmdb_id),
                            tmdb_id=tmdb_id, has_theme=0)
        if fixture_name not in ("no_plex_item", "no_local_files",
                                "plain_no_failure"):
            _seed_local_file(conn, tmdb_id=tmdb_id, source_kind="url")
        conn.commit()
    # Both unscoped and section-scoped calls must succeed.
    for url in (
        f"/api/items/movie/{tmdb_id}/recovery-options",
        f"/api/items/movie/{tmdb_id}/recovery-options?section_id=1",
    ):
        r = client.get(url, headers={"X-Authentik-Username": "testadmin"})
        assert r.status_code == 200, (
            f"500 on {fixture_name} ({url}): {r.text[:300]}"
        )
        body = r.json()
        # Response shape stays consistent across all branches.
        for key in ("failure_kind", "failure_message", "options",
                    "rating_key", "acked", "resolved", "resolved_via"):
            assert key in body, f"missing key {key!r} in {fixture_name}"


# ── 6. Section-failure-acks (v1.13.54) flow ──────────────────


def test_per_section_ack_sets_acked_true(app_client):
    """When a per-section sfa row exists for the requested
    section_id, the response's `acked` field must be True even
    if the title-global failure_acked_at is still NULL."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme(conn, tmdb_id=600,
                    failure_kind="video_private",
                    failure_message="private")
        _seed_plex_item(conn, rating_key="600", tmdb_id=600)
        conn.execute(
            """INSERT INTO section_failure_acks
                 (media_type, tmdb_id, section_id, acked_at, acked_by)
                 VALUES ('movie', 600, '1', ?, 'test')""",
            (now_iso(),),
        )
        conn.commit()
    r = client.get(
        "/api/items/movie/600/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["acked"] is True
    # Calling without section_id falls back to the title-global
    # failure_acked_at (NULL) → not acked.
    r2 = client.get(
        "/api/items/movie/600/recovery-options",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r2.json()["acked"] is False
