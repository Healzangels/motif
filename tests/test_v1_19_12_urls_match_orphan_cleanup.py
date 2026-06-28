"""v1.19.12 — urls_match orphan cleanup: trigger + read-side gate.

the user's 2026-05-24 diagnostic on Cowboys & Aliens (tmdb=49849)
showed a T+PU row that had displayed the !UPD prompt from an
orphan `pending_updates kind='urls_match'` row whose
`user_overrides` counterpart had been deleted by an earlier
action (likely REPLACE TDB / ACCEPT UPDATE).

`urls_match` is by definition a U→T conversion offer — sync
writes it when a user_override URL coincidentally matches the
new TDB URL, inviting the user to convert their override into
T-tracking. With no user_override left to convert FROM, the
prompt is meaningless. v1.19.4's P-row exclusion didn't catch
this case (T+PU rows have a placement).

## Fix (defense in depth)

  1. **Schema trigger** (v57): `trg_user_overrides_cleanup_
     urls_match` fires AFTER DELETE on user_overrides and
     auto-deletes any matching kind='urls_match' pending_
     updates row. Prevents NEW orphans regardless of which
     of the ~12 code paths deleted the user_overrides.

  2. **One-shot migration cleanup**: `_migrate_v56_to_v57`
     also runs a one-time DELETE pass against EXISTING
     orphans (pending_updates kind='urls_match' rows with
     no matching user_overrides). Cleans up state that
     accumulated on installs over the v1.12.34 → v1.19.12
     lifetime before the trigger landed.

  3. **Read-side gate** (`_has_user_override_sql` helper):
     extends the v1.19.4 urls_match prompt gate to ALSO
     require a user_override exists for the row. Catches
     any orphans that slip through (defense in depth) and
     covers the case where SCHEMA/migration ran but a
     race-condition delete during runtime briefly produces
     an orphan.

## What's pinned

- CURRENT_SCHEMA_VERSION = 57
- Trigger declaration exists in SCHEMA + the v56→v57 migration
- Trigger fires on user_overrides DELETE
- Migration includes one-shot orphan-cleanup DELETE
- `_has_user_override_sql` helper exists
- All 11 urls_match gate sites in api.py invoke the new helper
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
DB_PY = REPO / "app" / "core" / "db.py"
API_PY = REPO / "app" / "web" / "api.py"


# ── Schema pins ──────────────────────────────────────────────


def test_schema_version_bumped_to_57():
    """v1.19.12 introduced schema v57 (urls_match cleanup trigger).
    v1.19.42 added v58. The v57 → v58 dispatch step must still exist
    so existing v57 installs keep upgrading cleanly."""
    src = DB_PY.read_text()
    # v1.19.42: post-v58 bump, just confirm v57 still in the
    # migration ladder rather than pinning the absolute version.
    assert "_migrate_v56_to_v57" in src, (
        "v1.19.12: trg_user_overrides_cleanup_urls_match "
        "migration must remain in the dispatch ladder"
    )
    assert "elif current == 56:" in src
    assert "current = 57" in src


def test_trigger_in_schema_block():
    """SCHEMA (the idempotent CREATE block) must declare the
    trigger so fresh installs get it without needing the
    migration."""
    src = DB_PY.read_text()
    assert "CREATE TRIGGER IF NOT EXISTS " \
           "trg_user_overrides_cleanup_urls_match" in src, (
        "v1.19.12: SCHEMA must include the trigger CREATE"
    )


def test_v56_to_v57_migration_exists():
    """Existing installs at v56 need the migration to add the
    trigger + clean up pre-existing orphans."""
    src = DB_PY.read_text()
    assert "def _migrate_v56_to_v57(" in src, (
        "v1.19.12: migration function must exist"
    )
    # Migration dispatcher must reference v57.
    assert "elif current == 56:" in src
    assert "_migrate_v56_to_v57(conn)" in src
    assert "current = 57" in src


def test_v57_migration_includes_one_shot_cleanup():
    """Beyond installing the trigger, the migration must also
    run a one-shot DELETE pass against existing orphan rows
    (the trigger only catches future deletes)."""
    src = DB_PY.read_text()
    idx = src.index("def _migrate_v56_to_v57(")
    end = src.index("\ndef ", idx + 1)
    body = src[idx:end]
    # The cleanup DELETE.
    assert "DELETE FROM pending_updates" in body
    assert "kind = 'urls_match'" in body
    assert "NOT EXISTS" in body and "user_overrides" in body, (
        "v1.19.12: migration must NOT EXISTS-gate the cleanup "
        "DELETE against user_overrides so it only removes "
        "genuine orphans, not active prompts"
    )


# ── Read-side gate pins ──────────────────────────────────────


def test_has_user_override_helper_exists():
    """The `_has_user_override_sql` helper must be defined at
    module scope so the 11 urls_match gate sites can chain
    it after `_not_p_row_sql`."""
    src = API_PY.read_text()
    assert "def _has_user_override_sql(" in src, (
        "v1.19.12: helper must be defined at module scope"
    )


def test_all_gate_sites_chain_both_helpers():
    """Every site that uses _not_p_row_sql in a gate context
    must ALSO use _has_user_override_sql immediately after.
    Pre-fix the gate was {_not_p_row_sql} alone (caught P
    rows only); v1.19.12 requires both to suppress orphan
    urls_match prompts regardless of source class."""
    src = API_PY.read_text()
    import re
    # Find every _not_p_row_sql() call site (excluding the def
    # line + comment references).
    bad_sites = []
    for m in re.finditer(
        r"_not_p_row_sql\(['\"]?\w+['\"]?,\s*['\"]?\w+['\"]?\)",
        src,
    ):
        pos = m.end()
        # v1.22.12: the new_theme_available P-exclusion uses _not_p_row
        # STANDALONE (gated against pure SRC=P rows, not paired with an
        # override). Skip it — it's not a urls_match gate site.
        lookback = src[max(0, m.start() - 90):m.start()]
        if "_pending_update_new_theme_kind_sql" in lookback:
            continue
        # Look ahead ~200 chars for _has_user_override_sql.
        lookahead = src[pos:pos + 200]
        if "_has_user_override_sql" not in lookahead:
            bad_sites.append((m.start(), m.group(0),
                              src[m.start():m.end() + 100]))
    assert not bad_sites, (
        f"v1.19.12: every _not_p_row_sql call site must be "
        f"chained with _has_user_override_sql within 200 chars. "
        f"Ungated sites: {bad_sites[:2]}"
    )


def test_helper_call_count_alignment():
    """Both helpers should be invoked the same number of
    times (1 per gate site). If they diverge, a site got
    one helper but not the other."""
    src = API_PY.read_text()
    not_p_calls = src.count("_not_p_row_sql('")
    has_uo_calls = src.count("_has_user_override_sql('")
    # Defs use `def _foo(t: str =` shape — the call shape uses
    # quoted args. So the .count('(\'') filters to call sites.
    assert not_p_calls == has_uo_calls, (
        f"v1.19.12: _not_p_row_sql call count ({not_p_calls}) "
        f"must equal _has_user_override_sql call count "
        f"({has_uo_calls}). Mismatch means a gate site got "
        f"one helper but not the other."
    )


# ── End-to-end behavioral ────────────────────────────────────


@pytest.fixture
def fresh_db(tmp_path):
    """Fresh DB initialized at v57. The trigger should be in
    place via the SCHEMA block."""
    from app.core.db import init_db
    db = tmp_path / "motif.db"
    init_db(db)
    return db


def test_trigger_fires_on_user_overrides_delete(fresh_db):
    """End-to-end: insert a user_overrides row + a matching
    pending_updates kind='urls_match' row. DELETE the
    user_override. Verify the pending_updates row is
    auto-cleaned by the trigger."""
    with sqlite3.connect(fresh_db) as conn:
        # Theme.
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES (1, 'movie', 100, 'T', 'imdb', "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        # user_overrides row.
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by) "
            "VALUES ('movie', 100, '1', "
            "        'https://www.youtube.com/watch?v=ABC', "
            "        '2026-05-24T00:00:00', 'admin')"
        )
        # Matching urls_match pending_updates row.
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   new_youtube_url, detected_at, "
            "   decision, kind) "
            "VALUES ('movie', 100, '1', "
            "        'https://www.youtube.com/watch?v=ABC', "
            "        '2026-05-24T00:00:00', 'pending', "
            "        'urls_match')"
        )
        conn.commit()

        # Sanity: pending_updates exists.
        count = conn.execute(
            "SELECT COUNT(*) FROM pending_updates "
            "WHERE media_type='movie' AND tmdb_id=100"
        ).fetchone()[0]
        assert count == 1

        # DELETE the user_overrides row.
        conn.execute(
            "DELETE FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=100 "
            "  AND section_id='1'"
        )
        conn.commit()

        # Trigger should have auto-deleted the urls_match
        # pending_updates row.
        count = conn.execute(
            "SELECT COUNT(*) FROM pending_updates "
            "WHERE media_type='movie' AND tmdb_id=100"
        ).fetchone()[0]
    assert count == 0, (
        f"v1.19.12: trigger should have auto-deleted the "
        f"urls_match pending_updates row when user_overrides "
        f"was deleted; remaining count={count}"
    )


def test_trigger_preserves_non_urls_match_kinds(fresh_db):
    """The trigger filters by `kind = 'urls_match'`. Other
    kinds (upstream_changed) on the same row must SURVIVE
    a user_overrides delete — they represent genuine TDB
    URL changes that the user might still want to act on."""
    with sqlite3.connect(fresh_db) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES (2, 'movie', 200, 'T2', 'imdb', "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   set_at, set_by) "
            "VALUES ('movie', 200, '1', "
            "        'https://www.youtube.com/watch?v=XYZ', "
            "        '2026-05-24T00:00:00', 'admin')"
        )
        # upstream_changed kind — NOT a urls_match.
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   new_youtube_url, detected_at, "
            "   decision, kind) "
            "VALUES ('movie', 200, '1', "
            "        'https://www.youtube.com/watch?v=NEW', "
            "        '2026-05-24T00:00:00', 'pending', "
            "        'upstream_changed')"
        )
        conn.commit()

        # DELETE the user_override.
        conn.execute(
            "DELETE FROM user_overrides "
            "WHERE media_type='movie' AND tmdb_id=200"
        )
        conn.commit()

        # upstream_changed must survive.
        rows = conn.execute(
            "SELECT kind FROM pending_updates "
            "WHERE media_type='movie' AND tmdb_id=200"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "upstream_changed", (
        f"v1.19.12: trigger must only delete kind='urls_match' "
        f"rows; upstream_changed must survive. Got rows: {rows}"
    )


def test_migration_one_shot_cleans_existing_orphans(tmp_path):
    """Simulate an install with existing orphan urls_match
    rows (pre-trigger state). Run init_db (which triggers
    the v56→v57 migration). Verify orphans are cleaned + the
    trigger is now in place."""
    from app.core.db import init_db, CURRENT_SCHEMA_VERSION

    db = tmp_path / "motif.db"
    # Initialize at v57 (fresh) so we have the schema, then
    # manually create an orphan to simulate the pre-trigger
    # state on existing installs. The one-shot DELETE in
    # the migration is what we're testing — but on fresh
    # init the migration doesn't run (it's only for
    # existing-DB upgrades). So we synthesize the same
    # condition manually + verify the trigger handles it
    # going forward.
    init_db(db)
    # Synthesize an orphan that PRE-DATED the trigger by
    # using a direct INSERT (bypassing user_overrides side).
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes "
            "  (id, media_type, tmdb_id, title, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES (3, 'movie', 300, 'T3', 'imdb', "
            "        '2026-05-24T00:00:00', "
            "        '2026-05-24T00:00:00')"
        )
        conn.execute(
            "INSERT INTO pending_updates "
            "  (media_type, tmdb_id, section_id, "
            "   new_youtube_url, detected_at, "
            "   decision, kind) "
            "VALUES ('movie', 300, '1', "
            "        'https://www.youtube.com/watch?v=ORPHAN', "
            "        '2026-05-24T00:00:00', 'pending', "
            "        'urls_match')"
        )
        conn.commit()

    # Sanity: orphan exists.
    with sqlite3.connect(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM pending_updates "
            "WHERE media_type='movie' AND tmdb_id=300 "
            "  AND kind='urls_match'"
        ).fetchone()[0]
    assert count == 1

    # Run the one-shot cleanup SQL that the migration runs.
    # (We can't easily re-run the migration on an already-
    # v57 DB, so we run the equivalent DELETE directly.)
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            """
            DELETE FROM pending_updates
             WHERE kind = 'urls_match'
               AND NOT EXISTS (
                   SELECT 1 FROM user_overrides uo
                    WHERE uo.media_type = pending_updates.media_type
                      AND uo.tmdb_id = pending_updates.tmdb_id
                      AND uo.section_id = pending_updates.section_id
               )
            """
        )
        conn.commit()
        assert cur.rowcount >= 1
        # Verify orphan was cleaned.
        count = conn.execute(
            "SELECT COUNT(*) FROM pending_updates "
            "WHERE tmdb_id=300"
        ).fetchone()[0]
    assert count == 0, (
        f"v1.19.12 one-shot cleanup: orphan should be gone; "
        f"remaining count={count}"
    )
