"""v1.18.83 — recover user_overrides lost from bulk CSV imports.

Sibling to v1.18.10's `maybe_recover_lost_user_overrides`.
v1.18.10 only scans the `events` table for messages matching
`'Manual URL set by admin:%'` — the per-row SET URL endpoint
emits exactly that string via `log_event`, so v1.18.10
recovers those. The bulk CSV-import endpoint
(`/api/import/apply`) wrote ONLY to `audit_events` (with
details `{"source":"import"}`) — never to `events`. So when
v1.18.0's cascade + v1.12.60's orphan sweep wiped user_
overrides, bulk-imported URLs were left unrecoverable.

the user's repro on the 2026-05-22 deploy of v1.18.82:
  - bulk-imported 17 anime URLs on 2026-05-17
  - v1.18.0 cascade hit shortly after
  - v1.18.5 walker recreated local_files as adopt+recovered
  - v1.18.10 walker scanned events, found 0 import entries,
    skipped
  - rows persisted as A (adopt) instead of U
  - direct DB query: 17 import audit_events present, 0
    user_overrides for the orphan IDs

## What's pinned

- `maybe_recover_lost_bulk_imports(db_path)` exists and
  scans `audit_events` (not `events`) WHERE action='set_url'
  AND details contains `"source":"import"`.
- Walker INSERTs user_overrides at section_id='' (bulk
  import's canonical write location).
- Reclassifies `local_files.source_kind='adopt' → 'url'` for
  matching rows that ALSO have `source_video_id='recovered'`
  (narrow filter — v1.18.5 walker's distinct signature).
- Backfills `source_video_id` from the URL.
- Has independent marker `recovery_bulk_imports_done_at`.
- Idempotent via INSERT OR IGNORE.
- main.py wires the walker into boot sequence with try/except.
- api_import_apply also emits the canonical
  `'Manual URL set by admin'` log_event so future bulk imports
  are recoverable via the v1.18.10 events-based walker too.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
MAIN_PY = REPO / "app" / "main.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── Source-level pins ────────────────────────────────────────


def test_bulk_import_recovery_function_exists():
    """`maybe_recover_lost_bulk_imports(db_path)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_recover_lost_bulk_imports(" in src


def test_bulk_import_recovery_scans_audit_events_not_events():
    """The walker must scan `audit_events` (where bulk imports
    write) — NOT the `events` table (where they don't). This
    is the v1.18.83 distinction from v1.18.10."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_bulk_imports(")
    body = src[fn_start:]
    assert "FROM audit_events" in body, (
        "v1.18.83: walker must read from audit_events — that's "
        "where bulk imports write. Reading 'events' (v1.18.10's "
        "source) would re-introduce the same blind spot."
    )
    assert "action = 'set_url'" in body
    assert '"source":"import"' in body or "'source':'import'" in body, (
        "v1.18.83: must filter on the bulk-import signature in "
        "details JSON to avoid touching per-row SET URL audits"
    )


def test_bulk_import_recovery_has_independent_marker():
    """Independent marker so the walker runs even on installs
    where v1.18.10's marker is already set."""
    src = RECOVERY_PY.read_text()
    assert "recovery_bulk_imports_done_at" in src


def test_bulk_import_recovery_reclassifies_with_narrow_filter():
    """The walker must reclassify local_files.source_kind from
    'adopt' → 'url' for the recovered rows. Filter must include
    BOTH source_kind='adopt' AND source_video_id='recovered' so
    legitimate (non-recovery) adopt rows are left alone."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_bulk_imports(")
    body = src[fn_start:]
    assert "UPDATE local_files" in body
    assert "SET source_kind = 'url'" in body
    assert "source_kind = 'adopt'" in body
    assert "source_video_id = 'recovered'" in body, (
        "v1.18.83: reclassification filter must include source_"
        "video_id='recovered' (v1.18.5 walker signature) so "
        "legitimate non-recovery adopt rows aren't touched"
    )


def test_bulk_import_recovery_is_idempotent():
    """INSERT OR IGNORE on user_overrides + marker stamp
    ensures re-runs are no-ops."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_bulk_imports(")
    body = src[fn_start:]
    assert "INSERT OR IGNORE INTO user_overrides" in body


def test_bulk_import_recovery_skips_when_override_exists():
    """The walker must skip rows where user_overrides already
    exists — don't overwrite a later manual SET URL the user
    made by hand."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_bulk_imports(")
    body = src[fn_start:]
    # Look for the existing-row guard.
    assert "existing is not None" in body or "if existing" in body
    assert "continue" in body  # the early-exit


def test_bulk_import_recovery_backfills_video_id():
    """The walker must backfill source_video_id from the URL
    (parsed via extract_video_id) so the info-card thumbnail
    loads instead of 404-ing on the 'recovered' sentinel."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_lost_bulk_imports(")
    body = src[fn_start:]
    assert "extract_video_id" in body
    assert "source_video_id = ?" in body or "source_video_id=?" in body


# ── Forward-fix: api_import_apply now emits log_event ────────


def test_api_import_apply_emits_canonical_log_event():
    """v1.18.83 belt-and-suspenders: api_import_apply must
    ALSO emit the canonical "Manual URL set by admin" log_event
    so future bulk imports are recoverable via v1.18.10's
    events-based walker. Pre-v1.18.83 the bulk path wrote
    ONLY to audit_events."""
    src = API_PY.read_text()
    apply_start = src.index('@app.post("/api/import/apply")')
    # Find end of function — look for the next decorator.
    apply_end = src.index('@app.post', apply_start + 1)
    body = src[apply_start:apply_end]
    assert "log_event(" in body, (
        "v1.18.83: api_import_apply must call log_event for "
        "each applied row so future cascade-recovery via the "
        "v1.18.10 walker works"
    )
    assert "Manual URL set by" in body, (
        "v1.18.83: log_event message must match the canonical "
        "format v1.18.10 walker greps for"
    )


# ── End-to-end: behavioral recovery ──────────────────────────


@pytest.fixture
def lost_bulk_import_fixture(tmp_path: Path):
    """Reproduce the user's Apocalypse Hotel scenario: orphan
    themes row, plex_items row, local_files row classified as
    ('manual', 'adopt') with source_video_id='recovered',
    audit_events entry showing bulk-import SET URL — but NO
    user_overrides row (nuked by the v1.18.0/.10 cascade)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-17T19:30:30"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, 0, "
            "        'anime', 1, ?, ?)",
            (ts, ts),
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, imdb_id, title, title_norm, "
            "   year, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', -62, 'tt35636155', "
            "        'Apocalypse Hotel', 'apocalypse hotel', "
            "        '2025', 'plex_orphan', ?, ?)",
            (ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, "
            "   guid_imdb, title, title_norm, year, "
            "   folder_path, local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('461824', '3', 'show', "
            "        'tt35636155', 'Apocalypse Hotel', "
            "        'apocalypse hotel', '2025', "
            "        '/data/media/anime/Apocalypse Hotel (2025)', "
            "        1, ?, ?, ?)",
            (themes_id, ts, ts),
        )
        # v1.18.5 walker shape: source_kind='adopt' +
        # source_video_id='recovered' (the v1.18.83 reclassification
        # target).
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('tv', -62, '3', "
            "        'anime/Apocalypse Hotel (2025)/theme.mp3', "
            "        ?, 'recovered', 'manual', 'adopt')",
            (ts,),
        )
        # The bulk-import audit survived in audit_events. Use the
        # same compact JSON serializer (separators=(",",":")) that
        # production _record_audit uses — otherwise the LIKE
        # pattern in the walker SQL won't match.
        conn.execute(
            "INSERT INTO audit_events "
            "  (occurred_at, actor, action, media_type, "
            "   tmdb_id, section_id, details) "
            "VALUES (?, 'admin', 'set_url', 'tv', -62, '', ?)",
            (ts, json.dumps({
                "old_url": None,
                "new_url": "https://www.youtube.com/watch?v=0Pn6FQXbqhw",
                "source": "import",
            }, separators=(",", ":"))),
        )
        conn.commit()
    return db_path


def test_walker_recovers_user_override_from_audit_events(
    lost_bulk_import_fixture,
):
    """End-to-end: the walker reads the bulk-import audit,
    finds the matching orphan row, INSERTs user_overrides,
    reclassifies local_files.source_kind 'adopt' → 'url',
    and backfills source_video_id from the URL."""
    db_path = lost_bulk_import_fixture
    from app.core.recovery_v55 import maybe_recover_lost_bulk_imports
    stats = maybe_recover_lost_bulk_imports(db_path)
    assert stats["audits_scanned"] == 1
    assert stats["user_overrides_inserted"] == 1
    assert stats["local_files_reclassified"] == 1
    assert stats["video_ids_backfilled"] == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        uo = conn.execute(
            "SELECT youtube_url, section_id, set_by "
            "FROM user_overrides "
            "WHERE media_type='tv' AND tmdb_id=-62"
        ).fetchone()
        lf = conn.execute(
            "SELECT source_kind, source_video_id "
            "FROM local_files "
            "WHERE media_type='tv' AND tmdb_id=-62"
        ).fetchone()
    assert uo is not None, (
        "v1.18.83: walker must INSERT user_overrides for the "
        "bulk-imported orphan row"
    )
    assert uo["youtube_url"] == (
        "https://www.youtube.com/watch?v=0Pn6FQXbqhw"
    )
    assert uo["section_id"] == "", (
        "v1.18.83: must INSERT at section_id='' (bulk import's "
        "canonical write location — api.py:11437-11447)"
    )
    assert uo["set_by"] == "recovery_v1.18.83"
    assert lf["source_kind"] == "url", (
        "v1.18.83: local_files must be reclassified from "
        "'adopt' to 'url' so SRC renders U not A"
    )
    assert lf["source_video_id"] == "0Pn6FQXbqhw", (
        "v1.18.83: source_video_id must be backfilled from "
        "the URL so info-card thumbnails load instead of "
        "404-ing on the 'recovered' sentinel"
    )


def test_walker_marker_stops_re_run(lost_bulk_import_fixture):
    """Second invocation after marker stamp must be a no-op."""
    db_path = lost_bulk_import_fixture
    from app.core.recovery_v55 import maybe_recover_lost_bulk_imports
    stats1 = maybe_recover_lost_bulk_imports(db_path)
    assert stats1["user_overrides_inserted"] == 1
    stats2 = maybe_recover_lost_bulk_imports(db_path)
    assert stats2["user_overrides_inserted"] == 0, (
        "v1.18.83: marker must prevent re-run from re-inserting"
    )


def test_walker_skips_when_override_already_exists(
    lost_bulk_import_fixture,
):
    """If user_overrides already has a row (e.g. user manually
    re-SET URL after the cascade), the walker must NOT clobber
    it with the older bulk-import URL."""
    db_path = lost_bulk_import_fixture
    # Pre-seed a manual user_overrides row (later choice).
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, youtube_url, set_at, "
            "   set_by, note, section_id) "
            "VALUES ('tv', -62, "
            "        'https://www.youtube.com/watch?v=MANUAL_xx', "
            "        '2026-05-22T18:00:00', 'admin', "
            "        'manual override', '')"
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_lost_bulk_imports
    stats = maybe_recover_lost_bulk_imports(db_path)
    assert stats["user_overrides_inserted"] == 0, (
        "v1.18.83: walker must SKIP rows where a user_overrides "
        "row already exists — don't overwrite a later choice"
    )
    # Verify the manual choice survived.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        uo = conn.execute(
            "SELECT youtube_url FROM user_overrides "
            "WHERE media_type='tv' AND tmdb_id=-62"
        ).fetchone()
    assert uo["youtube_url"] == (
        "https://www.youtube.com/watch?v=MANUAL_xx"
    )


def test_walker_no_op_when_no_import_audits(tmp_path):
    """Fresh install with no import audits — walker stamps no
    marker (so future imports + cascade can still recover)
    and returns 0 stats."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    from app.core.recovery_v55 import maybe_recover_lost_bulk_imports
    stats = maybe_recover_lost_bulk_imports(db_path)
    assert stats["audits_scanned"] == 0
    assert stats["user_overrides_inserted"] == 0
    # Marker NOT stamped — re-run can still process future imports.
    with sqlite3.connect(db_path) as conn:
        marker = conn.execute(
            "SELECT 1 FROM runtime_settings "
            "WHERE key = 'recovery_bulk_imports_done_at'"
        ).fetchone()
    assert marker is None, (
        "v1.18.83: walker must NOT stamp the marker when there "
        "are no audits — otherwise a fresh install's first bulk "
        "import after the v1.18.83 deploy would be permanently "
        "locked out from recovery"
    )
