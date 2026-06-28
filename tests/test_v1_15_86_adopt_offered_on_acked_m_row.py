"""v1.15.86 — ADOPT EXISTING THEME surfaces on M+failed-TDB+acked rows.

the user's screenshot of v1.15.83 "Antichrist (2009)" info card:

> "in the case of a movie that is a M row but also a failed TDB
>  red pill the options in the info card should be to adopt as
>  well, let's have the be the first option and the normal
>  adopt blue"

The row state was:
  * M-source (manual sidecar at Plex folder, motif doesn't own)
  * failure_kind=video_removed
  * failure_acked_at set (✓ ACKED — TDB UNAVAILABLE)
  * options shown: SET URL, UPLOAD MP3, MARK URL ALIVE

ADOPT EXISTING THEME was missing despite m_available being true.

## Root cause

`api_recovery_options` injected the smart-extras block (REVERT
TO USER URL / ADOPT EXISTING THEME / LET PLEX SERVE) inside a
gate:

    if kind in SMART_KINDS and not effective_acked_at:
        extra = []
        if revert_to_user_available: extra.append({REVERT})
        if m_available: extra.append({ADOPT})
        if p_available and motif_has_content: extra.append({LPS})
        recipes[kind] = extra + recipes.get(kind, recipes["unknown"])

Once the user ACKed the failure, the entire smart-extras block
was skipped — the recovery section then showed only the static
recipe (SET URL / UPLOAD MP3 / MARK URL ALIVE).

## Fix

ADOPT is a purely additive recovery: it claims an existing
sidecar (M-row → A-row). It doesn't depend on the failure
alarm being live; the sidecar at the Plex folder doesn't
disappear when the user clicks ACK. v1.15.86 lifts ADOPT out
of the not-acked gate:

    if kind in SMART_KINDS and m_available:
        adopt_opt = {...}
    else:
        adopt_opt = None

    if kind in SMART_KINDS and not effective_acked_at:
        extra = []
        # REVERT (still gated on not-acked)
        if adopt_opt is not None:
            extra.append(adopt_opt)
            adopt_opt = None
        # LET PLEX SERVE (still gated on not-acked)
        recipes[kind] = extra + recipes.get(kind, ...)

    # ADOPT injection for the acked branch:
    if adopt_opt is not None:
        recipes[kind] = [adopt_opt] + recipes.get(kind, ...)

REVERT and LET PLEX SERVE stay gated on not-acked because they
actively transition the row's state (REVERT switches to the
saved user URL; LET PLEX SERVE purges motif's tracking + acks
the failure as one action). ADOPT is the only purely additive
smart action and the only one safe to surface always.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.auth import create_admin, init_auth_schema
from app.core.db import init_db
from app.core.events import now_iso


REPO = Path(__file__).resolve().parent.parent


# ── Fixtures: TestClient + DB seeders ────────────────────────


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Bootstrap a Settings-driven app + fresh DB. Mirrors the
    v1.14.59 behavioral-test fixture."""
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
    return client, db


def _seed_section(conn, section_id="1", title="Movies",
                  section_type="movie", themes_subdir=None):
    now = now_iso()
    sub = themes_subdir or f"{section_type}s-{section_id}"
    conn.execute(
        "INSERT INTO plex_sections (section_id, title, type, "
        "  included, is_anime, is_4k, themes_subdir, "
        "  discovered_at, last_seen_at) "
        "VALUES (?, ?, ?, 1, 0, 0, ?, ?, ?)",
        (section_id, title, section_type, sub, now, now),
    )


def _seed_theme_acked_video_removed(conn, *, tmdb_id):
    """Theme row with failure_kind=video_removed already acked
    via failure_acked_at (title-global ack). Mirrors the
    the user-screenshot state."""
    now = now_iso()
    conn.execute(
        "INSERT INTO themes (media_type, tmdb_id, title, year, "
        "                    upstream_source, youtube_url, "
        "                    failure_kind, failure_message, "
        "                    failure_at, failure_acked_at, "
        "                    first_seen_sync_at, last_seen_sync_at) "
        "VALUES ('movie', ?, 'Antichrist', 2009, 'imdb', "
        "  'https://www.youtube.com/watch?v=_bv45yjPRAE', "
        "  'video_removed', 'probe: Track or video was removed', "
        "  ?, ?, ?, ?)",
        (tmdb_id, now, now, now, now),
    )


def _seed_plex_item_with_sidecar(conn, *, rating_key, tmdb_id,
                                 section_id="1"):
    """plex_item with local_theme_file=1 (sidecar exists at
    Plex folder) and NO matching placements row → m_available
    will be True for this row."""
    now = now_iso()
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, "
        "  media_type, guid_tmdb, title, year, has_theme, "
        "  local_theme_file, folder_path, plex_independent_theme, "
        "  first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, 'Antichrist', 2009, 1, 1, "
        "        ?, 0, ?, ?)",
        (rating_key, section_id, str(tmdb_id),
         f"/data/movies/Antichrist", now, now),
    )


# ── Behavioral: the core v1.15.86 contract ───────────────────


def test_adopt_offered_on_m_row_with_failed_tdb_and_acked(app_client):
    """The the user-screenshot scenario. m_available=True + failure
    acked → ADOPT EXISTING THEME must be in options, with
    action='adopt-and-ack', priority=0 (first), tone='adopt'."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme_acked_video_removed(conn, tmdb_id=870984)
        _seed_plex_item_with_sidecar(
            conn, rating_key="870984", tmdb_id=870984, section_id="1",
        )
        conn.commit()
    r = client.get(
        "/api/items/movie/870984/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Sanity: this is the acked-but-failure shape we set up.
    assert body["acked"] is True
    assert body["failure_kind"] == "video_removed"
    # The v1.15.86 fix: ADOPT must appear in the options.
    adopt = next(
        (o for o in body["options"]
         if o.get("action") == "adopt-and-ack"),
        None,
    )
    assert adopt is not None, (
        "v1.15.86: ADOPT EXISTING THEME must surface on M+failed-TDB+"
        "acked rows. Options returned: "
        f"{[o['action'] for o in body['options']]}"
    )
    assert adopt["priority"] == 0, (
        "v1.15.86: ADOPT must be priority 0 (first slot) per the user's "
        f"'let's have [it] be the first option'. Got {adopt['priority']}."
    )
    assert adopt["tone"] == "adopt", (
        "v1.15.86: ADOPT must use tone='adopt' (blue) per the user's "
        f"'the normal adopt blue'. Got {adopt['tone']!r}."
    )
    assert adopt["interactive"] is True
    # And it should sort first in the actual rendered list (the
    # sort uses priority, so priority=0 lands before priority>=1).
    assert body["options"][0]["action"] == "adopt-and-ack", (
        "v1.15.86: ADOPT must render as the FIRST option in the list. "
        f"Got order: {[o['action'] for o in body['options']]}"
    )


def test_adopt_still_offered_on_unacked_m_row(app_client):
    """Regression-guard: the pre-v1.15.86 behavior on a non-acked
    M+failed-TDB row was ADOPT visible. The v1.15.86 refactor must
    not break that path."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        # Same theme but WITHOUT failure_acked_at.
        now = now_iso()
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, "
            "                    upstream_source, youtube_url, "
            "                    failure_kind, failure_message, "
            "                    failure_at, "
            "                    first_seen_sync_at, last_seen_sync_at) "
            "VALUES ('movie', 870985, 'Antichrist2', 2009, 'imdb', "
            "  'https://youtube.com/watch?v=abc11111111', "
            "  'video_removed', 'probe: removed', ?, ?, ?)",
            (now, now, now),
        )
        _seed_plex_item_with_sidecar(
            conn, rating_key="870985", tmdb_id=870985, section_id="1",
        )
        conn.commit()
    r = client.get(
        "/api/items/movie/870985/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["acked"] is False
    actions = [o["action"] for o in body["options"]]
    assert "adopt-and-ack" in actions, (
        "Regression: ADOPT must remain visible on un-acked M+failed "
        f"rows (pre-v1.15.86 behavior). Got actions: {actions}"
    )


def test_no_adopt_when_no_sidecar_even_with_failed_tdb_acked(app_client):
    """Negative case: failure acked but plex_item has no sidecar
    (local_theme_file=0). m_available=False → ADOPT must NOT
    appear (nothing to adopt)."""
    client, db = app_client
    with sqlite3.connect(db) as conn:
        _seed_section(conn, section_id="1")
        _seed_theme_acked_video_removed(conn, tmdb_id=870986)
        # plex_item but without local_theme_file
        now = now_iso()
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, "
            "  media_type, guid_tmdb, title, year, has_theme, "
            "  local_theme_file, folder_path, plex_independent_theme, "
            "  first_seen_at, last_seen_at) "
            "VALUES ('870986', '1', 'movie', '870986', "
            "  'NoSidecar', 2009, 0, 0, '/data/movies/NoSidecar', "
            "  0, ?, ?)",
            (now, now),
        )
        conn.commit()
    r = client.get(
        "/api/items/movie/870986/recovery-options?section_id=1",
        headers={"X-Authentik-Username": "testadmin"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    actions = [o["action"] for o in body["options"]]
    assert "adopt-and-ack" not in actions, (
        "ADOPT must NOT appear when m_available is False (no sidecar "
        f"at Plex folder). Got actions: {actions}"
    )


# ── Static-text guards: pin the v1.15.86 code shape ──────────


def _fn_body() -> str:
    """Slice api.py from the api_recovery_options definition through
    the start of the next async def (or the file end). Captures the
    full handler body regardless of its current line count."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_start = src.index("async def api_recovery_options(")
    # Find the next async def after this one — handler ends just
    # before it. Falls back to end-of-file if this is the last one.
    next_def = src.find("\n    async def ", fn_start + 1)
    if next_def == -1:
        next_def = len(src)
    return src[fn_start:next_def]


def test_adopt_opt_computed_outside_not_acked_gate():
    """The v1.15.86 refactor builds `adopt_opt` BEFORE the
    `if kind in SMART_KINDS and not effective_acked_at:` gate
    so it survives into the acked path. Pin this structural
    change so a future refactor can't fold ADOPT back into the
    gated branch (the exact regression the v1.15.86 fix
    targets)."""
    fn_body = _fn_body()
    adopt_setup = fn_body.index("adopt_opt = {")
    not_acked_gate = fn_body.index(
        "if kind in SMART_KINDS and not effective_acked_at:")
    assert adopt_setup < not_acked_gate, (
        "v1.15.86: adopt_opt must be assembled BEFORE the not-acked "
        "gate so the acked path can still inject it. Current order has "
        "adopt_opt setup AFTER the gate — ADOPT is back to being "
        "hidden on acked rows."
    )


def test_acked_path_adopt_prepend_exists():
    """v1.15.86: after the not-acked gate exits, if adopt_opt is
    still set, it gets prepended to the recipe list. Pin the
    presence of this post-gate prepend."""
    fn_body = _fn_body()
    assert "[adopt_opt] +" in fn_body, (
        "v1.15.86: missing post-gate ADOPT prepend "
        "(`recipes[kind] = [adopt_opt] + ...`). Without it, the "
        "acked path won't surface ADOPT."
    )


def test_adopt_opt_preserves_v1_13_47_tone_and_priority():
    """Pin that the v1.15.86 refactor preserves the v1.13.47
    label / action / tone / priority for the ADOPT button.
    Static guard against a refactor accidentally re-toning it
    or shifting priority."""
    fn_body = _fn_body()
    adopt_block_start = fn_body.index("adopt_opt = {")
    # Read 1500 chars forward — enough to capture the full dict
    # literal including the close brace.
    adopt_block = fn_body[adopt_block_start:adopt_block_start + 1500]
    assert '"action": "adopt-and-ack"' in adopt_block, (
        f"v1.15.86: adopt-and-ack action missing from adopt_opt. "
        f"Block: {adopt_block[:500]}"
    )
    assert '"label": "ADOPT EXISTING THEME"' in adopt_block
    assert '"priority": 0' in adopt_block
    assert '"tone": "adopt"' in adopt_block
    assert '"interactive": True' in adopt_block
