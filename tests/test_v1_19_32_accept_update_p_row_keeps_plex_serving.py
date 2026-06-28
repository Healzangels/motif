"""v1.19.32 — ACCEPT UPDATE on a P-row keeps Plex serving.

the user's repro (Bleach, anime/tv 30984, sec=3):

  1. Row was P (Plex serving its own theme) with a red TDB ✗ pill
     (video_removed on the upstream URL).
  2. the user used SET URL njRNT2TYz9w — landed a U-backup download.
     Worker's place skipped with reason='plex_has_theme' (P-row
     semantics: Plex keeps serving, motif holds a backup MP3).
  3. TDB later published a fresh URL _ty-Nqm4Pdc. Sync flagged
     !UPD on the row (kind=upstream_changed).
  4. the user clicked ACCEPT UPDATE → worker downloaded the new TDB
     URL AND force-placed it via plex_upload, flipping SRC=P → T.

the user's design point:

  > if a row is a P row and we get an update notification like this
  > it would just replace the downloaded copy and leave it as src P.
  > [...] we want to leave plex serving when we can but we do want
  > to be notified of the updated theme as well as be able to
  > update our downloaded local theme but keep plex serving and not
  > have an extra step of going to src T then letting plex serve again

## Fix

`api_accept_update` detects P-row state per-section (mirrors the
`_not_p_row_sql` v1.19.4 predicate: pi.has_theme=1, verified_ok=1,
NO placement). On a P row it:

  - Enqueues the download with `auto_place=False` (worker doesn't
    chain a place job).
  - Uses reason='upstream_update_accepted_p_backup'.

`worker._record_local_file`'s else-branch (auto_place=False)
stamps `last_place_attempt_reason='backup_only'` when the reason
matches the v1.19.21 / v1.19.32 whitelist. v1.19.32 extends that
whitelist to include the new reason, so:

  - The hourly retry sweep skips the row (existing v1.19.21
    sweep clause).
  - The BK link badge surfaces (existing v1.19.21 JS gate).

Non-P rows (placement exists, or has_theme=0) take the legacy
force_place=True branch — same behavior as before.

When section_id is omitted (legacy bulk-accept path with no per-
row UI context), the P-row check is skipped and current
force-place semantics are preserved.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from fastapi.testclient import TestClient

API_PY = (REPO / "app" / "web" / "api.py").read_text()
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── Source-text guards (cheap pin against accidental drop) ───


def _accept_handler_body() -> str:
    start = API_PY.index("async def api_accept_update(")
    end = API_PY.index("@app.post", start + 1)
    return API_PY[start:end]


def test_handler_has_p_row_branch_marker():
    """v1.19.32 marker must sit in the handler body so a future
    refactor confronts the P-row preservation history."""
    body = _accept_handler_body()
    assert "v1.19.32" in body, (
        "v1.19.32: marker required in api_accept_update — the "
        "P-row branch is load-bearing for SRC=P preservation"
    )
    assert "is_p_row_for_section" in body, (
        "v1.19.32: handler must compute is_p_row_for_section "
        "before deciding the download payload shape"
    )


def test_handler_p_row_branch_uses_auto_place_false():
    """The P-row branch must call _enqueue_download with
    auto_place=False (worker skips chained place job). Pin both
    the reason string and the auto_place flag."""
    body = _accept_handler_body()
    # Slice the if-block only — end at the matching else: so the
    # 'force_place=True' guard below doesn't trip on the legacy
    # branch's payload.
    branch_start = body.index("if is_p_row_for_section:")
    else_idx = body.index("else:", branch_start)
    branch = body[branch_start:else_idx]
    assert "auto_place=False" in branch, (
        "v1.19.32: P-row branch must pass auto_place=False — "
        "anything else risks the place chain that flips SRC to T"
    )
    assert 'reason="upstream_update_accepted_p_backup"' in branch, (
        "v1.19.32: P-row reason must be the new distinct string "
        "so the worker's backup_only stamp whitelist matches"
    )
    # Must NOT pass force_place=True in the P-row branch.
    assert "force_place=True" not in branch, (
        "v1.19.32: P-row branch must NOT set force_place=True — "
        "that's the legacy non-P branch's behavior"
    )


def test_handler_non_p_branch_preserves_legacy_force_place():
    """The else branch (non-P) must still pass force_place=True
    + auto_place=True so the legacy v1.12.43 behavior survives
    for U/T/A/M rows where the user IS asking motif to take over
    the placement."""
    body = _accept_handler_body()
    else_start = body.index("if is_p_row_for_section:")
    else_idx = body.index("else:", else_start)
    branch = body[else_idx:else_idx + 600]
    assert "auto_place=True" in branch
    assert "force_place=True" in branch
    assert 'reason="upstream_update_accepted"' in branch, (
        "v1.19.32: non-P branch keeps the original v1.12.41 reason "
        "string so existing log/audit consumers don't drift"
    )


def test_handler_section_id_required_for_p_check():
    """The P-row check must be gated on section_id presence —
    without a section we can't scope plex_items/placements lookups,
    and the legacy bulk path predates the P-row UI contract."""
    body = _accept_handler_body()
    # The is_p_row computation sits inside an `if section_id:` guard.
    p_compute_idx = body.index("is_p_row_for_section = False")
    after = body[p_compute_idx:p_compute_idx + 200]
    assert "if section_id:" in after, (
        "v1.19.32: is_p_row_for_section check must be section_id-gated"
    )


def test_handler_audit_records_p_row_backup_flag():
    """The audit-events row must record `p_row_backup: True/False`
    so post-incident debugging can correlate the branch taken with
    the row's resulting state."""
    body = _accept_handler_body()
    # Inside the _record_audit call.
    audit_idx = body.index('action="accept_update"')
    chunk = body[audit_idx:audit_idx + 600]
    assert '"p_row_backup":' in chunk, (
        "v1.19.32: audit details must surface the p_row_backup "
        "branch flag for post-incident debugging"
    )


# ── Worker whitelist extended ────────────────────────────────


def test_worker_backup_only_whitelist_includes_p_backup_reason():
    """`_record_local_file`'s auto_place=False branch stamps
    last_place_attempt_reason='backup_only' when the download
    reason matches the whitelist. v1.19.32 extends the whitelist
    to include the ACCEPT UPDATE P-row reason so:

      - The BK badge surfaces (v1.19.21 JS gate).
      - The hourly retry sweep skips the row (v1.19.21 SQL gate).
    """
    fn_start = WORKER_PY.index("def _record_local_file(")
    next_def = WORKER_PY.find("\n    def ", fn_start + 1)
    body = WORKER_PY[fn_start:next_def if next_def > 0 else len(WORKER_PY)]
    # The whitelist tuple must contain BOTH the v1.19.21 reason
    # and the new v1.19.32 reason.
    assert "bulk_backup" in body, (
        "v1.19.32: must not regress the v1.19.21 bulk_backup branch"
    )
    assert "upstream_update_accepted_p_backup" in body, (
        "v1.19.32: worker whitelist must include the new ACCEPT "
        "UPDATE P-row reason so backup_only stamp fires"
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


def _seed_section(conn, section_id="1", title="Movies"):
    conn.execute(
        "INSERT INTO plex_sections "
        "  (section_id, title, type, is_anime, is_4k, "
        "   themes_subdir, included, discovered_at, last_seen_at) "
        "VALUES (?, ?, 'movie', 0, 0, "
        "        'movies', 1, '2026-05-24T00:00:00', "
        "        '2026-05-24T00:00:00')",
        (section_id, title),
    )


def _seed_theme(conn, theme_id, tmdb_id,
                youtube_url="https://www.youtube.com/watch?v=ORIG"):
    now = "2026-05-24T00:00:00"
    conn.execute(
        "INSERT INTO themes "
        "  (id, media_type, tmdb_id, title, upstream_source, "
        "   last_seen_sync_at, first_seen_sync_at, youtube_url) "
        "VALUES (?, 'movie', ?, 'Test Movie', 'imdb', ?, ?, ?)",
        (theme_id, tmdb_id, now, now, youtube_url),
    )


def _seed_plex_item(conn, *, rk, theme_id, section_id="1",
                    has_theme=1, verified=1, tmdb_id=None):
    conn.execute(
        "INSERT INTO plex_items "
        "  (rating_key, section_id, media_type, theme_id, "
        "   guid_imdb, guid_tmdb, title, year, has_theme, "
        "   local_theme_file, folder_path, "
        "   plex_independent_theme, plex_theme_verified_ok, "
        "   first_seen_at, last_seen_at) "
        "VALUES (?, ?, 'movie', ?, "
        "        'tt100', ?, 'Test', 2020, ?, 0, "
        "        '/data/movies/Test', 0, ?, "
        "        '2026-05-24T00:00:00', "
        "        '2026-05-24T00:00:00')",
        (rk, section_id, theme_id, tmdb_id, has_theme, verified),
    )


def _seed_user_override(conn, tmdb_id, section_id="1",
                        url="https://www.youtube.com/watch?v=USR"):
    conn.execute(
        "INSERT INTO user_overrides "
        "  (media_type, tmdb_id, section_id, youtube_url, "
        "   set_at, set_by) "
        "VALUES ('movie', ?, ?, ?, '2026-05-24T00:00:00', 'admin')",
        (tmdb_id, section_id, url),
    )


def _seed_pending_update(conn, tmdb_id, section_id="1",
                         kind="upstream_changed",
                         new_url="https://www.youtube.com/watch?v=NEW"):
    conn.execute(
        "INSERT INTO pending_updates "
        "  (media_type, tmdb_id, section_id, "
        "   new_video_id, new_youtube_url, "
        "   detected_at, decision, kind) "
        "VALUES ('movie', ?, ?, 'NEW', ?, "
        "        '2026-05-24T00:00:00', 'pending', ?)",
        (tmdb_id, section_id, new_url, kind),
    )


def _get_download_jobs(db, tmdb_id):
    """Pull all download jobs for a tmdb so the test can inspect
    their payload."""
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT job_type, payload, status "
            "  FROM jobs "
            " WHERE media_type='movie' AND tmdb_id=?",
            (tmdb_id,),
        ).fetchall()
    return rows


def test_accept_update_on_p_row_enqueues_backup_only_download(
    admin_client, tmp_path,
):
    """the user's Bleach repro, simplified to a movie row.

    P-row (has_theme=1, verified=1, NO placement) + user override
    + pending_update → ACCEPT UPDATE must enqueue a download
    with auto_place=False AND reason='upstream_update_accepted_p_backup'.
    No place job is enqueued. The user override is deleted (per
    existing v1.12.34/35 contract) so the worker downloads the
    new TDB URL on next tick."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(
            conn, theme_id=1, tmdb_id=100,
            youtube_url="https://www.youtube.com/watch?v=ORIG",
        )
        _seed_plex_item(
            conn, rk="rk-p", theme_id=1, tmdb_id=100,
            has_theme=1, verified=1,
        )
        _seed_user_override(
            conn, tmdb_id=100,
            url="https://www.youtube.com/watch?v=USR",
        )
        _seed_pending_update(
            conn, tmdb_id=100, kind="upstream_changed",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.post(
        "/api/updates/movie/100/accept?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 100)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1, (
        f"v1.19.32: exactly one download job expected, got {download_jobs}"
    )
    payload = json.loads(download_jobs[0][1])
    assert payload.get("auto_place") is False, (
        f"v1.19.32: P-row ACCEPT UPDATE must enqueue download "
        f"with auto_place=False; got payload={payload}"
    )
    assert payload.get("reason") == "upstream_update_accepted_p_backup", (
        f"v1.19.32: P-row reason must mark the intent; "
        f"got reason={payload.get('reason')}"
    )
    assert payload.get("force_place") is not True, (
        f"v1.19.32: P-row branch must NOT force_place; "
        f"got payload={payload}"
    )
    # No place jobs should be queued — the worker chains the
    # place from auto_place=True; auto_place=False skips it.
    place_jobs = [j for j in jobs if j[0] == "place"]
    assert place_jobs == [], (
        f"v1.19.32: P-row ACCEPT UPDATE must NOT enqueue a place "
        f"job (Plex keeps serving); got {place_jobs}"
    )


def test_accept_update_on_non_p_row_keeps_force_place(
    admin_client, tmp_path,
):
    """Non-P row (placement exists → motif owns the served theme):
    ACCEPT UPDATE keeps the legacy v1.12.43 force_place=True
    behavior so the new TDB content actually overwrites the
    existing placement. Without this branch, U→T conversions
    would silently skip placement and leave the OLD U file
    serving (the v1.12.43 bug class)."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(
            conn, theme_id=2, tmdb_id=200,
            youtube_url="https://www.youtube.com/watch?v=ORIG",
        )
        _seed_plex_item(
            conn, rk="rk-u", theme_id=2, tmdb_id=200,
            has_theme=0, verified=0,
        )
        # Placement: motif owns the served theme (U/T/A/M class).
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, "
            "   media_folder, placed_at, placement_kind, "
            "   plex_rating_key, plex_refreshed, provenance) "
            "VALUES ('movie', 200, '1', "
            "        '/data/movies/U Row', "
            "        '2026-05-24T00:00:00', 'hardlink', "
            "        'rk-u', 1, 'manual')"
        )
        _seed_user_override(
            conn, tmdb_id=200,
            url="https://www.youtube.com/watch?v=USR",
        )
        _seed_pending_update(
            conn, tmdb_id=200, kind="upstream_changed",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.post(
        "/api/updates/movie/200/accept?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 200)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1
    payload = json.loads(download_jobs[0][1])
    assert payload.get("force_place") is True, (
        f"v1.19.32: non-P ACCEPT UPDATE must preserve the legacy "
        f"force_place=True semantics; got payload={payload}"
    )
    assert payload.get("auto_place") is True, (
        f"v1.19.32: non-P must keep auto_place=True; "
        f"got payload={payload}"
    )
    assert payload.get("reason") == "upstream_update_accepted", (
        f"v1.19.32: non-P reason must remain the legacy "
        f"'upstream_update_accepted'; got reason={payload.get('reason')}"
    )


def test_accept_update_p_row_verified_ok_zero_takes_legacy_branch(
    admin_client, tmp_path,
):
    """Counter-guard: a row with has_theme=1 BUT verified_ok=0
    (Plex's cache is verified-stale per v1.12.112) must NOT be
    treated as P. The SRC=P pill demotes to '-' for verified=0
    rows, so the P-preservation guarantee doesn't apply — fall
    through to the legacy force_place branch."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=3, tmdb_id=300)
        _seed_plex_item(
            conn, rk="rk-stale", theme_id=3, tmdb_id=300,
            has_theme=1, verified=0,
        )
        _seed_user_override(
            conn, tmdb_id=300,
            url="https://www.youtube.com/watch?v=USR",
        )
        _seed_pending_update(
            conn, tmdb_id=300, kind="upstream_changed",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.post(
        "/api/updates/movie/300/accept?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 300)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1
    payload = json.loads(download_jobs[0][1])
    # Verified-stale → NOT a P row → legacy branch.
    assert payload.get("force_place") is True, (
        f"v1.19.32: verified_ok=0 must take the legacy branch — "
        f"the P-row guarantee only applies when Plex's cache is "
        f"trustworthy; got payload={payload}"
    )
    assert payload.get("reason") == "upstream_update_accepted"


def test_accept_update_p_row_with_placement_takes_legacy_branch(
    admin_client, tmp_path,
):
    """Counter-guard: a row where has_theme=1 + verified=1 BUT a
    placement exists (motif owns the served theme, even if Plex
    also reports has_theme=1 — happens after a recent PUSH) is
    NOT a P row in the v1.19.4 / v1.19.32 sense. The legacy
    branch is correct — refresh the placement to the new TDB
    content."""
    db = _settings_db(tmp_path)
    with sqlite3.connect(db) as conn:
        _seed_section(conn)
        _seed_theme(conn, theme_id=4, tmdb_id=400)
        _seed_plex_item(
            conn, rk="rk-placed", theme_id=4, tmdb_id=400,
            has_theme=1, verified=1,
        )
        # Placement exists → motif owns the served theme.
        conn.execute(
            "INSERT INTO placements "
            "  (media_type, tmdb_id, section_id, "
            "   media_folder, placed_at, placement_kind, "
            "   plex_rating_key, plex_refreshed, provenance) "
            "VALUES ('movie', 400, '1', "
            "        '/data/movies/Placed Row', "
            "        '2026-05-24T00:00:00', 'hardlink', "
            "        'rk-placed', 1, 'auto')"
        )
        _seed_user_override(
            conn, tmdb_id=400,
            url="https://www.youtube.com/watch?v=USR",
        )
        _seed_pending_update(
            conn, tmdb_id=400, kind="upstream_changed",
            new_url="https://www.youtube.com/watch?v=NEW",
        )
        conn.commit()

    r = admin_client.post(
        "/api/updates/movie/400/accept?section_id=1",
        headers=AUTH,
    )
    assert r.status_code == 200, r.text

    jobs = _get_download_jobs(db, 400)
    download_jobs = [j for j in jobs if j[0] == "download"]
    assert len(download_jobs) == 1
    payload = json.loads(download_jobs[0][1])
    assert payload.get("force_place") is True, (
        f"v1.19.32: placement-present rows must take legacy "
        f"force_place branch; got payload={payload}"
    )
