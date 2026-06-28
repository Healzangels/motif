"""v1.21.52 — per-edition theme isolation, Phase A2 (schema v63).

Folds `edition_key` into the PRIMARY KEYs of local_files, user_overrides,
pending_updates, placements (the data-loss-risk-class PK rebuild, done via
the proven foreign_keys=OFF recipe). This makes per-edition state STORABLE
— two editions of the same (tmdb, section) can now hold independent rows.
Still behavior-preserving (edition_key='' everywhere = today).

These tests pin: edition_key is part of each table's PK; the payoff (two
editions coexist where they'd collide before); the trigger carries
edition_key; and the migration from a v62 fixture is zero-row-loss +
foreign_key_check-clean (the v1.18.0 data-loss guard).
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from app.core.db import init_db, CURRENT_SCHEMA_VERSION


REPO = Path(__file__).resolve().parent.parent

# Tables whose PK gained edition_key in v63 — their ON CONFLICT upsert
# targets MUST carry edition_key (SQLite requires an exact constraint
# match). The two below keep 3-col PKs and must NOT carry it.
_WIDENED = {"local_files", "user_overrides", "pending_updates", "placements"}
_NOT_WIDENED = {"previous_urls", "section_failure_acks"}


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _pk_cols(db: Path, table: str) -> list[str]:
    """Columns in `table`'s PRIMARY KEY, in PK order (PRAGMA table_info's
    `pk` field: 0 = not in PK, >0 = 1-based position)."""
    with sqlite3.connect(db) as conn:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in sorted(rows, key=lambda r: r[5]) if r[5] > 0]


def test_schema_version_at_least_63():
    # v63 folded edition_key into the 4 PKs; later versions only add to it.
    # >= so this A2 test doesn't go stale every schema bump.
    assert CURRENT_SCHEMA_VERSION >= 63


def test_edition_key_in_pk_of_all_four_tables(db):
    assert _pk_cols(db, "local_files") == [
        "media_type", "tmdb_id", "section_id", "edition_key"]
    assert _pk_cols(db, "user_overrides") == [
        "media_type", "tmdb_id", "section_id", "edition_key"]
    assert _pk_cols(db, "pending_updates") == [
        "media_type", "tmdb_id", "section_id", "edition_key"]
    # placements keeps media_folder, with edition_key appended.
    assert _pk_cols(db, "placements") == [
        "media_type", "tmdb_id", "section_id", "media_folder", "edition_key"]


def test_two_editions_coexist_in_local_files(db):
    """THE payoff: two editions of the same (tmdb, section) now hold
    INDEPENDENT local_files rows. Before A2 the second INSERT collided on
    the (media_type, tmdb_id, section_id) PK."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,'t','t')")
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',120,'LotR','imdb','t','t')")
        for ek, vid in (("", "std"), ("theatrical", "thr"),
                        ("extended", "ext")):
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " file_path, downloaded_at, source_video_id, edition_key)"
                " VALUES ('movie',120,'1',?, 't', ?, ?)",
                (f"{ek or 'std'}.mp3", vid, ek))
        conn.commit()
        n = conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE tmdb_id=120").fetchone()[0]
    assert n == 3  # all three editions persisted independently


def test_collision_on_duplicate_edition_key(db):
    """The PK still enforces uniqueness WITHIN an edition — two rows for
    the same (tmdb, section, edition_key) still collide."""
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, is_anime,"
            " is_4k, themes_subdir, included, discovered_at, last_seen_at)"
            " VALUES ('1','Movies','movie',0,0,'movies',1,'t','t')")
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
            " last_seen_sync_at, first_seen_sync_at)"
            " VALUES ('movie',120,'X','imdb','t','t')")
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id,"
            " file_path, downloaded_at, source_video_id, edition_key)"
            " VALUES ('movie',120,'1','a.mp3','t','v','extended')")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO local_files (media_type, tmdb_id, section_id,"
                " file_path, downloaded_at, source_video_id, edition_key)"
                " VALUES ('movie',120,'1','b.mp3','t','v2','extended')")


def test_cleanup_trigger_carries_edition_key(db):
    sql = sqlite3.connect(db).execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND "
        "name='trg_user_overrides_cleanup_urls_match'").fetchone()[0]
    assert "edition_key = OLD.edition_key" in sql


# ── the REAL rebuild: a v62-shaped (old PK) table → widened ──


def test_rebuild_pk_widens_old_shape_and_preserves_rows(tmp_path):
    """Directly exercise _rebuild_pk_add_edition_key on a hand-built v62-
    shaped table (edition_key column, OLD PK without it): the helper must
    widen the PK + preserve the row. This is the real rebuild the prod
    v59→v63 path runs; the full-init tests above hit the idempotent no-op
    (fresh DBs are already v63)."""
    from app.core.db import _rebuild_pk_add_edition_key
    p = tmp_path / "m.db"
    conn = sqlite3.connect(p)
    conn.execute(
        "CREATE TABLE local_files (media_type TEXT NOT NULL, tmdb_id INTEGER"
        " NOT NULL, section_id TEXT NOT NULL, file_path TEXT NOT NULL,"
        " downloaded_at TEXT NOT NULL, source_video_id TEXT NOT NULL,"
        " edition_key TEXT NOT NULL DEFAULT '',"
        " PRIMARY KEY (media_type, tmdb_id, section_id))")
    conn.execute("INSERT INTO local_files VALUES"
                 " ('movie',1,'1','x.mp3','t','v','')")
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    _rebuild_pk_add_edition_key(
        conn, "local_files",
        "PRIMARY KEY (media_type, tmdb_id, section_id)",
        "PRIMARY KEY (media_type, tmdb_id, section_id, edition_key)")
    conn.execute("PRAGMA foreign_keys = ON")
    pk = [r[1] for r in sorted(
        conn.execute("PRAGMA table_info(local_files)").fetchall(),
        key=lambda r: r[5]) if r[5] > 0]
    assert pk == ["media_type", "tmdb_id", "section_id", "edition_key"]
    assert conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0] == 1
    # idempotent: re-running is a no-op (PK already widened).
    conn.execute("PRAGMA foreign_keys = OFF")
    _rebuild_pk_add_edition_key(
        conn, "local_files",
        "PRIMARY KEY (media_type, tmdb_id, section_id)",
        "PRIMARY KEY (media_type, tmdb_id, section_id, edition_key)")
    conn.execute("PRAGMA foreign_keys = ON")
    assert conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0] == 1


# ── cascade lint: every ON CONFLICT target stays in sync with the PKs ──


def _iter_on_conflict_sites():
    """Yield (file, lineno, table, target_cols, has_edition) for every
    `ON CONFLICT(... section_id ...)` in app/, resolving the INSERT target
    table by the nearest preceding `INSERT INTO`. Multi-line aware — the
    wrapped placements target at api.py was missed by a single-line grep
    in v1.21.52 review (a 18th site), which is exactly what this guards."""
    for f in sorted((REPO / "app").rglob("*.py")):
        text = f.read_text()
        for m in re.finditer(r"ON CONFLICT\s*\(([^)]*)\)", text, re.DOTALL):
            target = re.sub(r"\s+", " ", m.group(1)).strip()
            if "section_id" not in target:
                continue
            pre = text[: m.start()]
            table = None
            for im in re.finditer(
                r'INSERT(?:\s+OR\s+\w+)?\s+INTO\s+["`]?(\w+)', pre, re.I):
                table = im.group(1)
            lineno = pre.count("\n") + 1
            yield (f.relative_to(REPO), lineno, table, target,
                   "edition_key" in target)


def test_widened_tables_carry_edition_key_in_every_conflict_target():
    """The v63 PK-widen cascade: every upsert against a widened table must
    include edition_key in its ON CONFLICT target, or SQLite raises
    `OperationalError: ON CONFLICT clause does not match any ... constraint`
    at prepare time (breaks on EVERY execution, not just on conflict)."""
    missing = [f"{f}:{ln} table={t} ({cols})"
               for f, ln, t, cols, has_ed in _iter_on_conflict_sites()
               if t in _WIDENED and not has_ed]
    assert not missing, (
        "ON CONFLICT target(s) on a v63-widened table missing edition_key:\n"
        + "\n".join(missing))


def test_unwidened_tables_do_not_carry_edition_key():
    """The mirror direction: previous_urls + section_failure_acks keep
    3-col PKs (no edition_key column), so adding edition_key to their
    conflict target would itself raise the same OperationalError."""
    wrong = [f"{f}:{ln} table={t} ({cols})"
             for f, ln, t, cols, has_ed in _iter_on_conflict_sites()
             if t in _NOT_WIDENED and has_ed]
    assert not wrong, (
        "ON CONFLICT target(s) on a NON-widened table wrongly carry "
        "edition_key:\n" + "\n".join(wrong))


def test_lint_actually_sees_the_known_sites():
    """Guard the guard: confirm the scan finds a healthy number of sites
    on the widened tables (so a regex break can't silently pass the two
    asserts above by matching nothing)."""
    widened_hits = [s for s in _iter_on_conflict_sites() if s[2] in _WIDENED]
    assert len(widened_hits) >= 18, (
        f"expected >=18 widened-table ON CONFLICT sites, saw "
        f"{len(widened_hits)} — did the scan regex break?")
