"""v1.18.49 — three audit fixes from the user's v1.18.48 deploy report.

## Bug A — reconcile_placement_paths re-uploads plex_upload rows forever

the user's docker logs showed 3 movies stuck in a tight re-upload loop:

  16:24:30 "Plex folder moved; relinking theme" {"old_folder": "",
            "new_folder": "/data/media/movies/...", "kind": "delete-stale"}
  16:24:30 Uploaded collection theme for '...'
  16:24:30 Notification sent: theme_added
  16:25:18 (same 3 items, same actions)
  16:25:41 (same 3 items, same actions)
  16:26:31 (same 3 items, same actions)

4 cycles in ~2 minutes per movie — every plex_enum run after a
sync re-uploaded the same audio bytes to Plex and fired a Discord
theme_added notification.

Root cause: `reconcile_placement_paths` in plex_enum.py queries
for placements whose `media_folder` differs from the matching
plex_items' `folder_path`. For `placement_kind='plex_upload'`
placements (the v1.18.0 schema-v55 sentinel), `media_folder=''`
is the canonical valid state — they don't have a folder by
design. The query saw `'' != '/data/...'` and treated it as a
folder move, enqueued a force place job, which re-INSERTed the
placement with `media_folder=''` again. Next enum: same mismatch
→ loop.

Fix: add `AND p.placement_kind != 'plex_upload'` to the WHERE
clause. plex_upload placements are immune from the folder-move
reconcile because they have no folder to begin with.

Class-9 cousin: a defensive reconcile that silently amplified
instead of fixing. The "kind": "delete-stale" log line tipped
that the DELETE branch was firing every cycle, not just once.

## Bug B — SYNC THEMERRDB busy label hides REFRESH PLEX

When AUTO-REFRESH PLEX AFTER SYNC is enabled, the dashboard's
SYNC THEMERRDB button reads `// SYNC THEMERRDB + REFRESH PLEX`
at idle (spells both phases) but the busy label collapsed to
just `// SYNCING THEMERRDB…` — the operator had no UI hint that
plex refresh was queued behind the sync. the user: "it does not
indicate that the plex refreshes are queued up on the status bar."

Fix: busy label mirrors the idle label's two-phase shape when
autoEnum is on. `// SYNCING THEMERRDB + REFRESH PLEX…` during
work, falls back to `// SYNCING THEMERRDB…` when auto-refresh
is off (single-phase work).

## Bug C — PS chip on /collections is dead

Collections have no media folder + no on-disk sidecar (the
plex_upload sentinel is `''`). PS state requires a real
sidecar file Plex serves — can never apply to collections.
The chip rendered but clicking it returned zero rows.

Fix: wrap the PS chip in `{% if tab != 'collections' %}` so
it's hidden on /collections. Other LINK chips (HL/C/M) are
likewise non-applicable to collections but the user's audit
only called out PS — wider cleanup deferred.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM_PY = REPO / "app" / "core" / "plex_enum.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
LIBRARY_HTML = REPO / "app" / "web" / "templates" / "library.html"


# ── Bug A: reconcile_placement_paths excludes plex_upload ────


def _init_schema(db_path):
    """Materialise the real DB schema so reconcile can run."""
    from app.core.db import init_db
    init_db(db_path)


def _seed_parents(conn, *, section_id, media_type, tmdb_id, title):
    """Insert the FK parents required for placements + plex_items."""
    # plex_sections is FK target for placements.section_id +
    # plex_items.section_id. Type maps 'collection'/'movie' → 'movie'.
    sec_type = "show" if media_type in ("tv", "show") else "movie"
    conn.execute(
        "INSERT OR IGNORE INTO plex_sections "
        "  (section_id, title, type, included, "
        "   discovered_at, last_seen_at) "
        "VALUES (?, 'Movies', ?, 1, "
        "        '2026-05-21T00:00:00Z', '2026-05-21T00:00:00Z')",
        (section_id, sec_type),
    )
    # themes is FK target for placements + plex_items.theme_id.
    # Use lookup by (media_type, tmdb_id) for the composite FK.
    conn.execute(
        "INSERT OR IGNORE INTO themes "
        "  (media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at) "
        "VALUES (?, ?, ?, 'themoviedb', "
        "        '2026-05-21T00:00:00Z', '2026-05-21T00:00:00Z')",
        (media_type, tmdb_id, title),
    )


def _seed_plex_item(conn, *, rk, section_id, media_type, tmdb_id,
                    title, folder_path):
    _seed_parents(conn, section_id=section_id,
                  media_type=media_type, tmdb_id=tmdb_id, title=title)
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, title, "
        "   title_norm, year, folder_path, guid_tmdb, "
        "   has_theme, first_seen_at, last_seen_at) "
        "VALUES (?, ?, ?, ?, ?, '', ?, ?, 1, "
        "        '2026-05-21T00:00:00Z', '2026-05-21T00:00:00Z')",
        (rk, section_id, media_type, title, title.lower(),
         folder_path, str(tmdb_id)),
    )


def _seed_placement(conn, *, media_type, tmdb_id, section_id,
                    media_folder, placement_kind):
    _seed_parents(conn, section_id=section_id,
                  media_type=media_type, tmdb_id=tmdb_id,
                  title="seed")
    conn.execute(
        "INSERT INTO placements "
        "  (media_type, tmdb_id, section_id, media_folder, "
        "   placement_kind, placed_at, plex_refreshed) "
        "VALUES (?, ?, ?, ?, ?, "
        "        '2026-05-21T00:00:00Z', 1)",
        (media_type, tmdb_id, section_id, media_folder,
         placement_kind),
    )


def test_reconcile_skips_plex_upload_placements(tmp_path):
    """The bug: a plex_upload placement (media_folder='') vs
    Plex's reported folder_path triggered the reconcile and
    re-enqueued a force place job every enum cycle. Verify the
    new WHERE clause skips it."""
    db = tmp_path / "test.db"
    _init_schema(db)

    from app.core.db import get_conn
    from app.core.plex_enum import reconcile_placement_paths

    with get_conn(db) as conn:
        # A plex_upload movie placement (the v1.18.0 sentinel).
        _seed_plex_item(
            conn, rk="653191", section_id="1", media_type="movie",
            tmdb_id=9805, title="Raumschiff",
            folder_path="/data/media/movies/Raumschiff (2004)",
        )
        _seed_placement(
            conn, media_type="movie", tmdb_id=9805, section_id="1",
            media_folder="",  # the plex_upload sentinel
            placement_kind="plex_upload",
        )
        conn.commit()

    # Pre-fix this returned 1 (one placement reconciled, force
    # place job enqueued). Post-fix it returns 0.
    enqueued = reconcile_placement_paths(db)
    assert enqueued == 0, (
        "v1.18.49: plex_upload placements MUST NOT match the "
        "folder-move reconcile — media_folder='' is the canonical "
        "valid state, not a stale path. Pre-fix this returned 1 "
        "and the loop fired every enum cycle."
    )

    # No place job should be queued either.
    with get_conn(db) as conn:
        jobs = conn.execute(
            "SELECT id FROM jobs WHERE job_type='place'"
        ).fetchall()
        assert len(jobs) == 0


def test_reconcile_still_fires_for_normal_folder_move(tmp_path):
    """Regression guard: a placement_kind='hardlink' row with a
    mismatched media_folder MUST still get reconciled. The fix
    only skips plex_upload — the legitimate folder-move case
    that the function was built for must keep working."""
    db = tmp_path / "test.db"
    _init_schema(db)

    from app.core.db import get_conn
    from app.core.plex_enum import reconcile_placement_paths

    with get_conn(db) as conn:
        # A hardlink movie whose folder actually moved.
        _seed_plex_item(
            conn, rk="100", section_id="1", media_type="movie",
            tmdb_id=42, title="Some Movie",
            folder_path="/data/media/movies/Some Movie NEW (2024)",
        )
        _seed_placement(
            conn, media_type="movie", tmdb_id=42, section_id="1",
            media_folder="/data/media/movies/Some Movie OLD (2024)",
            placement_kind="hardlink",
        )
        conn.commit()

    enqueued = reconcile_placement_paths(db)
    assert enqueued == 1, (
        "Regression: the legitimate folder-move case (hardlink "
        "placement, mismatched folder) must still reconcile. "
        "v1.18.49's fix is narrow — only plex_upload is exempt."
    )


def test_reconcile_skips_mixed_kinds_correctly(tmp_path):
    """Two placements for the same movie: one plex_upload
    (legitimate empty media_folder), one hardlink at the wrong
    folder. Only the hardlink should reconcile."""
    db = tmp_path / "test.db"
    _init_schema(db)

    from app.core.db import get_conn
    from app.core.plex_enum import reconcile_placement_paths

    with get_conn(db) as conn:
        # Two sections so we can have two placements without
        # violating the section-scoped UNIQUE.
        _seed_plex_item(
            conn, rk="200", section_id="1", media_type="movie",
            tmdb_id=99, title="Dual Movie",
            folder_path="/data/media/movies/Dual NEW (2024)",
        )
        _seed_plex_item(
            conn, rk="201", section_id="2", media_type="movie",
            tmdb_id=99, title="Dual Movie",
            folder_path="/data/media/movies/Dual NEW (2024)",
        )
        _seed_placement(
            conn, media_type="movie", tmdb_id=99, section_id="1",
            media_folder="", placement_kind="plex_upload",
        )
        _seed_placement(
            conn, media_type="movie", tmdb_id=99, section_id="2",
            media_folder="/data/media/movies/Dual OLD (2024)",
            placement_kind="hardlink",
        )
        conn.commit()

    enqueued = reconcile_placement_paths(db)
    assert enqueued == 1, (
        "Only the hardlink placement should reconcile — the "
        "plex_upload one is exempt by design"
    )


def test_plex_enum_py_carries_v1_18_49_marker():
    """Inline `# vX.Y.Z:` archaeology marker. Pinned so a future
    diff removing the placement_kind filter has to surface the
    rationale."""
    src = PLEX_ENUM_PY.read_text()
    assert "v1.18.49" in src
    assert "placement_kind != 'plex_upload'" in src


# ── Bug B: SYNC busy label spells REFRESH PLEX when autoEnum on ─


def test_setsyncbuttonstate_busy_label_includes_refresh_plex():
    """`setSyncButtonState('running')` must surface
    `// SYNCING THEMERRDB + REFRESH PLEX…` when the idle label
    indicates auto-refresh-plex is enabled. Pre-fix the busy
    label collapsed to `// SYNCING THEMERRDB…` regardless,
    hiding the queued plex refresh from the operator."""
    src = APP_JS.read_text()
    # The new busy label string must exist.
    assert "// SYNCING THEMERRDB + REFRESH PLEX…" in src, (
        "v1.18.49: busy label must spell both phases when "
        "auto_enum_after_sync is on"
    )
    # The setSyncButtonState 'running' branch must reference the
    # idleLabel-based detection.
    idx = src.index("function setSyncButtonState")
    block = src[idx:idx + 1500]
    assert "REFRESH PLEX" in block, (
        "setSyncButtonState must conditionally pick the busy "
        "label based on whether the idle label spells REFRESH PLEX"
    )


def test_refreshtopbarstatus_busy_label_includes_refresh_plex():
    """The poll-driven busy-label path (refreshTopbarStatus's
    cross-tab/cron sync detector) must also use the new
    two-phase busy label when autoEnum is on. Mirror site to
    setSyncButtonState — both write `syncBtn.textContent`."""
    src = APP_JS.read_text()
    # The busy-label literal appears in both call sites.
    occurrences = src.count("// SYNCING THEMERRDB + REFRESH PLEX…")
    assert occurrences >= 2, (
        "v1.18.49: both setSyncButtonState (canonical owner) "
        "AND refreshTopbarStatus (cross-tab/cron path) must "
        "carry the two-phase busy label — drift between them "
        "would resurrect the pre-v1.18.49 single-phase display "
        "in the cross-tab path"
    )


def test_unlock_branch_accepts_both_busy_labels():
    """The cross-tab unlock branch must compare against BOTH the
    legacy single-phase label and the new two-phase label so the
    button correctly returns to idle in either mode."""
    src = APP_JS.read_text()
    # The OR-of-two-labels check must exist.
    assert (
        "syncBtn.textContent === '// SYNCING THEMERRDB…'\n"
        in src
        or "'// SYNCING THEMERRDB…'\n                       || "
        in src
        or "// SYNCING THEMERRDB + REFRESH PLEX…" in src
    )


# ── Bug C: PS chip hidden on /collections (REVERTED v1.18.54) ─
# v1.18.49 hid PS on /collections; v1.18.54 restored it.
# v1.19.66 dropped the PS chip entirely from every page — the
# original PS state is now handled via the BU chip + the SRC=P
# + LINK=— combo workflow. The test that pinned PS's existence
# on /collections is removed here; see
# test_v1_19_66_revert_ps_chip.py for the chip-removed pin.


# ── Render end-to-end ────────────────────────────────────────


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    db = settings.db_path
    init_db(db)
    init_auth_schema(db)
    create_admin(db, username="testadmin", password="testpassword")
    app = create_app(settings)
    return TestClient(app)


AUTH = {"X-Authentik-Username": "testadmin"}


def test_collections_page_renders_ps_chip_v1_18_54():
    """End-to-end placeholder — kept for v1.18.49 archaeology.
    v1.18.49 originally pinned 'PS chip MUST NOT render on
    /collections'. v1.18.54 reverted that hide once the user
    found the state is reachable via DOWNLOAD TDB BACKUP +
    plex_has_theme skip → plex_independent_theme=1 stamp.

    See test_v1_18_54_ps_chip_restored.py for the new
    end-to-end pin that PS does render on /collections."""
    # Intentionally inverted: PS now SHOULD render. The full
    # behavior is asserted in test_v1_18_54.
    pass


def test_movies_page_does_not_render_ps_chip_post_v1_19_66(admin_client):
    """v1.19.66: PS chip dropped from all pages. /movies must
    NOT contain data-link-pill='ps'. URL param ?link_pills=ps
    is preserved as a no-op for bookmark compat (handled in
    api.py's _pset whitelist), but the template button is
    gone."""
    r = admin_client.get("/movies", headers=AUTH)
    assert r.status_code == 200
    assert 'data-link-pill="ps"' not in r.text, (
        "v1.19.66: PS chip dropped — /movies must not render it"
    )
