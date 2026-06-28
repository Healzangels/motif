"""v1.19.7 — fix UnboundLocalError in api_recovery_options.

the user's 2026-05-24 docker logs showed a 500 crash:

  File "/app/app/web/api.py", line 16033, in api_recovery_options
      if state:
         ^^^^^
  UnboundLocalError: cannot access local variable 'state'
  where it is not associated with a value

## Root cause

The function assigned `state` inside `if rating_key:` only.
When the theme has no plex_items row (rating_key is None),
the assignment is skipped. The later `if state:` check at
the motif_has_content block raised UnboundLocalError.

Common trigger: a theme whose Plex section was removed (or
that's not yet enumerated). The /api/recovery-options call
returns 500 instead of the recovery checklist.

## Fix

Initialize `state = None` BEFORE the `if rating_key:` block.
Downstream `if state:` resolves cleanly to False when
rating_key was None.

## What's pinned

- `state = None` initialization at module level inside the
  function, before the `if rating_key:` guard
- Marker references v1.19.7 + the user's 2026-05-24 repro
- End-to-end: hit /api/recovery-options for a theme that
  has NO plex_items row → 200 response (not 500)
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"


# ── Source pin ───────────────────────────────────────────────


def test_state_initialized_before_rating_key_guard():
    """`state` must be initialized to None BEFORE the
    `if rating_key:` block so the later `if state:` check
    doesn't UnboundLocalError when rating_key is None.
    Locates by line offsets, not text matching (comment
    blocks contain the literal "if rating_key:" too)."""
    src = API_PY.read_text()
    lines = src.splitlines()
    # Find function start.
    fn_line = next(
        i for i, line in enumerate(lines)
        if "async def api_recovery_options(" in line
    )
    # Find function end (next `async def` at the same indent).
    fn_end = next(
        (i for i, line in enumerate(lines[fn_line + 1:], fn_line + 1)
         if line.startswith("    async def ")),
        len(lines),
    )
    fn_lines = lines[fn_line:fn_end]
    # `state = None` line — must be a CODE line (12-space
    # indent), not inside a comment block.
    state_init_idx = next(
        (i for i, line in enumerate(fn_lines)
         if line.strip() == "state = None"),
        None,
    )
    assert state_init_idx is not None, (
        "v1.19.7: function must contain `state = None` "
        "initializer line"
    )
    # `if rating_key:` CODE line (not comment) — look for the
    # exact indent pattern (no leading `#`).
    # v1.22.71: the function gained an EARLIER `if rating_key:` (the
    # clicked-rk validation) that never touches `state` — anchor the
    # guard that opens the state-ASSIGNING block (the pi.local_theme_
    # file SELECT sits within it).
    rk_guard_idx = next(
        (i for i, line in enumerate(fn_lines)
         if line.strip() == "if rating_key:"
         and not line.lstrip().startswith("#")
         and any("SELECT pi.local_theme_file" in l
                 for l in fn_lines[i:i + 6])),
        None,
    )
    assert rk_guard_idx is not None, (
        "v1.19.7: function must contain `if rating_key:` "
        "code line"
    )
    assert state_init_idx < rk_guard_idx, (
        f"v1.19.7: `state = None` (line {state_init_idx}) "
        f"must come BEFORE `if rating_key:` (line "
        f"{rk_guard_idx}) so the downstream `if state:` "
        f"check doesn't UnboundLocalError"
    )


def test_marker_references_v1_19_7_repro():
    """The fix must carry a v1.19.7 marker explaining WHY —
    so future maintainers see the rationale (not just an
    arbitrary `state = None` init)."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_recovery_options(")
    next_fn = src.find("\n    async def ", fn_idx + 1)
    fn_body = src[fn_idx:next_fn if next_fn > 0 else fn_idx + 50000]
    # The marker must appear near the state init.
    idx = fn_body.index("state = None")
    block = fn_body[max(0, idx - 1500):idx + 200]
    assert "v1.19.7" in block, (
        "v1.19.7: state init must carry the version marker"
    )
    assert "UnboundLocalError" in block or "rating_key is None" in block, (
        "v1.19.7: marker should reference the crash mode "
        "(UnboundLocalError) or the trigger (rating_key None)"
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


def test_recovery_options_returns_200_when_no_plex_items(
    admin_client, tmp_path,
):
    """the user's exact repro: a theme that has no plex_items
    row (post-section-removal or un-discovered). Pre-fix
    /api/recovery-options returned 500 with UnboundLocalError.
    Post-fix returns 200 with the recovery option list (P/M
    options just don't appear since no Plex state is known)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        # Theme exists with a failure but NO plex_items row
        # exists for it (the bug condition: rating_key
        # resolves to None at line 15519).
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, "
            "  first_seen_sync_at, failure_kind) "
            "VALUES (99, 'movie', 12345, 'Orphan Failure', "
            "        'imdb', '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00', 'video_removed')"
        )
        conn.commit()

    r = admin_client.get(
        "/api/items/movie/12345/recovery-options",
        headers=AUTH,
    )
    assert r.status_code == 200, (
        f"v1.19.7: recovery-options must return 200 for "
        f"theme with no plex_items row (rating_key=None); "
        f"got {r.status_code}: {r.text}"
    )
    # Response shape must include the options list.
    data = r.json()
    assert "options" in data, (
        f"v1.19.7: response must include options list; "
        f"got: {data}"
    )
    # Sanity: at least one option present (the always-available
    # fallbacks like clear-failure / manual-url / upload-theme).
    options = data.get("options", [])
    assert options, (
        f"v1.19.7: even without plex_items, response must "
        f"include the always-available fallback options "
        f"(clear-failure / manual-url / upload-theme); "
        f"got: {data}"
    )


def test_recovery_options_with_plex_items_still_works(
    admin_client, tmp_path,
):
    """Sanity: theme WITH a plex_items row still resolves
    correctly. The v1.19.7 fix is additive — it doesn't change
    the with-rk path."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, "
            "   last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.execute(
            "INSERT INTO themes (id, media_type, tmdb_id, title, "
            "  upstream_source, last_seen_sync_at, "
            "  first_seen_sync_at, failure_kind) "
            "VALUES (99, 'movie', 12345, 'With RK', "
            "        'imdb', '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00', 'video_removed')"
        )
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, theme_id, guid_imdb, guid_tmdb, "
            "  title, year, has_theme, local_theme_file, "
            "  folder_path, plex_independent_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('100', '1', 'movie', 99, 'tt12345', "
            "        12345, 'With RK', 2020, 0, 0, "
            "        '/data/movies/X', 0, "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.commit()

    r = admin_client.get(
        "/api/items/movie/12345/recovery-options",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text
    assert "options" in r.json()
