"""v1.20.21 — sync + Plex-refresh audit follow-ups (git-path).

Parallel-agent audit of the ThemerrDB sync + Plex refresh pipelines.
the user is on the GIT transport, so the full-walk speed findings are
near-free for him; the items that actually help his git+Plex setup
(all LOW-risk) ship here. The phantom finding (refresh fan-out "dedup"
— rks are primary-key-distinct, nothing to dedup) and the race-prone
one (immediate HEAD verify after API upload — fights the class-1 cache
lag, deferred to a probe) were dropped/deferred deliberately.

A3   plex_enum._section_enum_overdue swallowed its timestamp-parse
     failure at log.debug → silently disabled the 24h overdue bypass
     that lets the reaper run on quiet sections (class-9). Warn-once.
A1   sync._detect_and_stamp_drops_git silently `continue`d when an
     imdb-keyed removal couldn't resolve a tmdb_id → a real drop could
     be missed on the git path with no breadcrumb. Logged.
S4   sync._GitMirror.list_changes() ran the full dulwich tree_changes()
     diff TWICE per git sync (upsert pass + drop-detection pass) on the
     same per-run instance. Memoized on the instance.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()
SYNC = (REPO / "app" / "core" / "sync.py").read_text()


# ── A3: overdue-check warn-once ──────────────────────────────


def test_section_enum_overdue_warn_once_pattern():
    fn = PLEX_ENUM[PLEX_ENUM.index("def _section_enum_overdue("):]
    fn = fn[:fn.index("\ndef ", 1)]
    # redundant (ValueError, TypeError, Exception) collapsed to Exception
    assert "except (ValueError, TypeError, Exception)" not in fn
    assert "global _SECTION_ENUM_OVERDUE_WARNED" in fn
    assert "_SECTION_ENUM_OVERDUE_WARNED = True" in fn


def test_section_enum_overdue_survives_malformed_timestamp(tmp_path):
    """Behavioral: a malformed last_seen_at must NOT raise — the catch
    returns False (bypass disabled) instead of exploding the enum."""
    from app.core.db import init_db
    from app.core.plex_enum import _section_enum_overdue
    db = tmp_path / "t.db"
    init_db(db)
    with sqlite3.connect(db) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id, title, type, "
            "  is_anime, is_4k, themes_subdir, included, discovered_at, "
            "  last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,"
            "  '2026-05-29','2026-05-29')")
        c.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type, "
            "  guid_tmdb, title, year, has_theme, first_seen_at, "
            "  last_seen_at) VALUES ('r1','1','movie',1,'X',2020,1,"
            "  '2026-05-29','not-a-real-timestamp')")
        c.commit()
    # Must return False, not raise.
    assert _section_enum_overdue(db, "1") is False


# ── A1: git drop-miss breadcrumb ─────────────────────────────


def test_git_drop_unresolved_imdb_logs():
    fn = SYNC[SYNC.index("def _detect_and_stamp_drops_git("):]
    fn = fn[:fn.index("\ndef ", 1)]
    idx = fn.index("if row is None:")
    block = fn[idx:idx + 1200]
    assert "log.info(" in block, (
        "v1.20.21 A1: the unresolved-imdb continue must leave a "
        "breadcrumb (class-9 cold-path rule), not silently drop the "
        "candidate"
    )
    assert "no resolvable themes row" in block


# ── S4: list_changes() memoized on the mirror ────────────────


def test_gitmirror_init_has_changeset_cache():
    from app.core.sync import _GitMirror
    m = _GitMirror(Path("/tmp/x"), "op", "url", "database",
                   lambda: False)
    assert m._changeset_cache is None, (
        "v1.20.21 S4: a fresh mirror starts with no cached changeset"
    )


def test_list_changes_returns_cached_changeset():
    """The second caller (drop-detection) must get the memoized diff
    without re-walking the object store."""
    from app.core.sync import _GitMirror, _ChangeSet
    m = _GitMirror(Path("/tmp/x"), "op", "url", "database",
                   lambda: False)
    # Heads must be non-None to pass the acquire-guard; the cache check
    # sits right after it, BEFORE any tree_changes() walk.
    m._repo = object()
    m._new_head = b"deadbeef"
    sentinel = _ChangeSet(added=["a"], modified=[], removed=["b"])
    m._changeset_cache = sentinel
    result = m.list_changes()
    assert result is sentinel, (
        "v1.20.21 S4: list_changes() must return the memoized changeset "
        "on the second call instead of recomputing the dulwich diff"
    )


def test_v1_20_21_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
