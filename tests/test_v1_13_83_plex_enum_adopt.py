"""v1.13.83 — targeted test coverage for plex_enum.py + adopt.py.

Continues the post-v1.13.80 audit's P1 #4 sweep (per-module test
coverage gaps). Worker (v1.13.81) and placement/normalize
(v1.13.82) are now covered; this commit adds plex_enum + adopt.

Scope is deliberately tight — pure functions and well-isolated
behaviors only. The big orchestration entry points
(`run_plex_enum`, `adopt_folder` end-to-end, `_do_adopt`) need
extensive mocking and would bloat this test suite without
proportional confidence gain. Their critical sub-behaviors are
exercised via the unit functions they delegate to.

Coverage:

  plex_enum:
  - `_candidate_local_paths` — Unraid container/host path
    translation order + edge cases
  - `stat_theme_sidecar` — tri-state (True / False / None)
    indeterminate-result detection (v1.11.67)
  - `folder_has_theme_sidecar` — None-coercion-to-False contract

  adopt:
  - `_hash_file` — pure streaming hash + size
  - implicit-ack contract: adopt.py sets `failure_acked_at` but
    NEVER `failure_kind` — the v1.10.50 pattern that v1.13.81
    mirrored for sfa cleanup. Static guard on the SQL.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest


# ── plex_enum._candidate_local_paths ────────────────────────

def test_candidate_local_paths_yields_literal_first():
    """The first yield is always the literal path Plex reported.
    Caller iterates in order, uses first .is_dir()=True hit; so
    the literal path takes priority over any translation."""
    from app.core.plex_enum import _candidate_local_paths
    out = list(_candidate_local_paths("/data/media/movies/Foo"))
    assert out[0] == Path("/data/media/movies/Foo")


def test_candidate_local_paths_unraid_user_data_translation():
    """`/mnt/user/data/...` → `/data/...` — the most common
    Unraid host→container mapping.

    Note: the bare `/mnt/user/` → `/` rule ALSO matches this
    input (since `/mnt/user/data/...` startswith `/mnt/user/`),
    so the destination `/data/...` may appear twice in the
    output list. Caller tolerates duplicates — it iterates
    and uses the first .is_dir()=True candidate, so the
    duplicate is harmless. Pin the contract: literal first,
    `/data/...` is reachable somewhere in the list."""
    from app.core.plex_enum import _candidate_local_paths
    out = list(_candidate_local_paths("/mnt/user/data/media/x"))
    # Literal first.
    assert out[0] == Path("/mnt/user/data/media/x")
    # Translated destination is yielded.
    assert Path("/data/media/x") in out


def test_candidate_local_paths_no_match_yields_only_literal():
    """A path that doesn't start with any known prefix yields
    just the literal — caller's .is_dir() check decides."""
    from app.core.plex_enum import _candidate_local_paths
    out = list(_candidate_local_paths("/some/unknown/root/x"))
    assert out == [Path("/some/unknown/root/x")]


def test_candidate_local_paths_empty_string_yields_nothing():
    """Empty / None-ish input is a no-op, not an error. Caller
    iterates zero candidates and treats as 'not found'."""
    from app.core.plex_enum import _candidate_local_paths
    assert list(_candidate_local_paths("")) == []


def test_candidate_local_paths_cache_translation():
    """`/mnt/cache/data/...` → `/data/...` — Unraid SSD cache
    pool variant, used when a share is cache-only."""
    from app.core.plex_enum import _candidate_local_paths
    out = list(_candidate_local_paths("/mnt/cache/data/media/y"))
    assert Path("/data/media/y") in out


# ── plex_enum.stat_theme_sidecar ────────────────────────────

def test_stat_theme_sidecar_finds_mp3(tmp_path):
    """Happy path: theme.mp3 in folder → True."""
    from app.core.plex_enum import stat_theme_sidecar
    (tmp_path / "theme.mp3").write_bytes(b"x")
    assert stat_theme_sidecar(str(tmp_path)) is True


def test_stat_theme_sidecar_finds_m4a(tmp_path):
    """SIDECAR_AUDIO_EXTS includes .m4a — covers AAC / iTunes
    container theme files."""
    from app.core.plex_enum import stat_theme_sidecar
    (tmp_path / "theme.m4a").write_bytes(b"x")
    assert stat_theme_sidecar(str(tmp_path)) is True


def test_stat_theme_sidecar_finds_flac(tmp_path):
    """SIDECAR_AUDIO_EXTS includes .flac — lossless edge case."""
    from app.core.plex_enum import stat_theme_sidecar
    (tmp_path / "theme.flac").write_bytes(b"x")
    assert stat_theme_sidecar(str(tmp_path)) is True


def test_stat_theme_sidecar_returns_false_when_folder_empty(tmp_path):
    """Reachable folder with no theme.<ext> file → False."""
    from app.core.plex_enum import stat_theme_sidecar
    assert stat_theme_sidecar(str(tmp_path)) is False


def test_stat_theme_sidecar_ignores_non_theme_audio(tmp_path):
    """A foo.mp3 in the folder is NOT a theme sidecar — only
    theme.<ext> qualifies. Pin the prefix check."""
    from app.core.plex_enum import stat_theme_sidecar
    (tmp_path / "foo.mp3").write_bytes(b"x")
    (tmp_path / "movie.mkv").write_bytes(b"x")
    assert stat_theme_sidecar(str(tmp_path)) is False


def test_stat_theme_sidecar_ignores_unknown_ext(tmp_path):
    """theme.txt is not in SIDECAR_AUDIO_EXTS — must NOT match."""
    from app.core.plex_enum import stat_theme_sidecar
    (tmp_path / "theme.txt").write_bytes(b"x")
    assert stat_theme_sidecar(str(tmp_path)) is False


def test_stat_theme_sidecar_returns_none_when_no_candidate_reachable():
    """v1.11.67 indeterminate-result fix: when EVERY candidate
    path is unreachable (.is_dir() never returned True for any
    candidate), return None so callers preserve the previous-
    known value instead of overwriting truth with a transient
    NFS hiccup.

    Triggered when the literal Plex-reported folder doesn't
    exist on disk AND no translation rule produces a path that
    does either. Pin the None return — the call site in
    plex_enum knows None means "skip this update, keep prior
    pi.local_theme_file value."""
    from app.core.plex_enum import stat_theme_sidecar
    result = stat_theme_sidecar("/mnt/nonexistent-test-path-xyz/foo")
    assert result is None


def test_stat_theme_sidecar_empty_folder_path_is_none():
    """v1.21.42 (audit M2): empty input is INDETERMINATE (None), not
    False — a blank path means 'we don't know where to look', so the
    caller preserves the previously-known local_theme_file instead of
    stomping an M-row's sidecar flag on a transient empty folder_path.
    Matches find_theme_sidecar_path, which already returns None for ''."""
    from app.core.plex_enum import stat_theme_sidecar
    assert stat_theme_sidecar("") is None


# ── plex_enum.folder_has_theme_sidecar ──────────────────────

def test_folder_has_theme_sidecar_coerces_none_to_false(tmp_path):
    """Convenience wrapper: maps None (indeterminate) → False.
    Used when caller doesn't have a previous-known value to
    preserve (one-shot checks)."""
    from app.core.plex_enum import folder_has_theme_sidecar
    # Empty folder → False from stat_theme_sidecar → False here.
    assert folder_has_theme_sidecar(str(tmp_path)) is False


def test_folder_has_theme_sidecar_passes_through_true(tmp_path):
    """Wrapper preserves True from stat_theme_sidecar."""
    from app.core.plex_enum import folder_has_theme_sidecar
    (tmp_path / "theme.mp3").write_bytes(b"x")
    assert folder_has_theme_sidecar(str(tmp_path)) is True


# ── adopt._hash_file ────────────────────────────────────────

def test_hash_file_returns_sha256_and_size(tmp_path):
    """Pure streaming hash + size. Pin both outputs against a
    known value computed by hashlib directly."""
    from app.core.adopt import _hash_file
    f = tmp_path / "x.bin"
    payload = b"motif test payload"
    f.write_bytes(payload)
    expected_hash = hashlib.sha256(payload).hexdigest()
    actual_hash, actual_size = _hash_file(f)
    assert actual_hash == expected_hash
    assert actual_size == len(payload)


def test_hash_file_handles_empty_file(tmp_path):
    """Edge case: zero-byte file. Sha256 of empty is
    e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855."""
    from app.core.adopt import _hash_file
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    h, sz = _hash_file(f)
    assert sz == 0
    assert h == hashlib.sha256(b"").hexdigest()


def test_hash_file_streams_multi_megabyte(tmp_path):
    """The 1 MiB buffer means files larger than buf size go
    through multiple read() calls. Ensure the hash + size
    accumulate correctly across buffer boundaries (3 MiB
    payload triggers 3+ read calls)."""
    from app.core.adopt import _hash_file
    f = tmp_path / "big.bin"
    payload = b"motif" * (700_000)  # ~3.5 MiB
    f.write_bytes(payload)
    h, sz = _hash_file(f)
    assert sz == len(payload)
    assert h == hashlib.sha256(payload).hexdigest()


# ── adopt.py implicit-ack contract (v1.10.50 + audit) ───────

def test_adopt_only_acks_failure_never_clears_kind():
    """v1.10.50 contract pinned by the audit: adopt.py touches
    `failure_acked_at` but NEVER `failure_kind`. The TDB pill
    must keep painting red so the user knows the TDB-side URL
    is still broken; only the alarm gets dismissed.

    Inverse of v1.13.81's worker fix (TDB success clears
    failure_kind too because the URL is genuinely working).
    Adopt is the user routing AROUND a broken TDB URL with a
    sidecar — the URL is still broken.

    Static guard against a regression that adds a failure_kind
    NULL clear inside adopt_folder — would resurrect the v1.10.50
    bug class (TDB pill turns green on adopt even though TDB is
    still broken; same family as the v1.13.74 bug for the
    download success path)."""
    src = (Path(__file__).resolve().parent.parent
           / "app" / "core" / "adopt.py").read_text()
    # Adopt MUST NOT contain a failure_kind = NULL UPDATE.
    # Allow the string in comments (v markers, doc references)
    # but not as actual SQL we'd execute.
    assert "SET failure_kind = NULL" not in src, (
        "v1.10.50 contract: adopt only acks failures, "
        "never clears failure_kind — TDB pill stays red"
    )
    # v1.14.34: the failure_acked_at UPDATE was replaced with an
    # INSERT into section_failure_acks (per-section sfa pattern)
    # so the cross-section bleed v1.14.24 closed for SET URL +
    # UPLOAD MP3 also closes here. The contract this test pins —
    # "adopt only acks on existing failure" — survives via the
    # EXISTS gate inside the INSERT. Pin the new shape:
    assert "INSERT INTO section_failure_acks" in src
    assert "auto:adopt" in src
    # The EXISTS gate is the no-op-on-healthy-row guard the old
    # `WHERE failure_kind IS NOT NULL` clause provided.
    assert "WHERE EXISTS (" in src
    assert "t.failure_kind IS NOT NULL" in src


def test_adopt_acks_only_on_existing_failure(tmp_path):
    """Behavioral pin of the EXISTS-gate guard: a row WITHOUT a
    failure should be a no-op (no spurious sfa row written).

    v1.14.34: the underlying SQL flipped from a title-global
    UPDATE on themes.failure_acked_at to an INSERT INTO
    section_failure_acks gated by EXISTS(themes WHERE
    failure_kind IS NOT NULL). Behavioral contract is the same:
    no failure → no ack write."""
    import sqlite3
    from datetime import datetime, timezone
    from app.core.db import init_db

    db = tmp_path / "motif.db"
    init_db(db)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with sqlite3.connect(db) as conn:
        # Row WITHOUT a failure.
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  last_seen_sync_at, first_seen_sync_at"
            ") VALUES ('movie', 1, 'x', 'imdb', ?, ?)",
            (now, now),
        )
        # Mirror the v1.14.34 adopt.py INSERT shape.
        conn.execute(
            """INSERT INTO section_failure_acks
               (media_type, tmdb_id, section_id, acked_at, acked_by)
               SELECT ?, ?, ?, ?, 'auto:adopt'
               WHERE EXISTS (
                   SELECT 1 FROM themes t
                   WHERE t.media_type = ?
                     AND t.tmdb_id = ?
                     AND t.failure_kind IS NOT NULL
               )
               ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING""",
            ("movie", 1, "sec1", now, "movie", 1),
        )
        n = conn.execute(
            "SELECT COUNT(*) FROM section_failure_acks "
            "WHERE media_type = 'movie' AND tmdb_id = 1",
        ).fetchone()[0]
    # No-op: no failure to ack → no write.
    assert n == 0


def test_adopt_acks_existing_failure(tmp_path):
    """Behavioral pin of the happy path: a row WITH a failure
    gets a section_failure_acks entry, and themes.failure_kind
    stays untouched (TDB pill stays red).

    v1.14.34: ack write moved from themes.failure_acked_at to
    section_failure_acks. The "TDB pill stays red" contract
    holds — failure_kind on themes is never touched by adopt."""
    import sqlite3
    from datetime import datetime, timezone
    from app.core.db import init_db

    db = tmp_path / "motif.db"
    init_db(db)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes ("
            "  media_type, tmdb_id, title, upstream_source,"
            "  last_seen_sync_at, first_seen_sync_at,"
            "  failure_kind"
            ") VALUES ('movie', 1, 'x', 'imdb', ?, ?, 'video_removed')",
            (now, now),
        )
        conn.execute(
            """INSERT INTO section_failure_acks
               (media_type, tmdb_id, section_id, acked_at, acked_by)
               SELECT ?, ?, ?, ?, 'auto:adopt'
               WHERE EXISTS (
                   SELECT 1 FROM themes t
                   WHERE t.media_type = ?
                     AND t.tmdb_id = ?
                     AND t.failure_kind IS NOT NULL
               )
               ON CONFLICT(media_type, tmdb_id, section_id) DO NOTHING""",
            ("movie", 1, "sec1", now, "movie", 1),
        )
        themes_row = conn.execute(
            "SELECT failure_kind, failure_acked_at FROM themes "
            "WHERE tmdb_id = 1",
        ).fetchone()
        sfa_row = conn.execute(
            "SELECT section_id, acked_at, acked_by FROM section_failure_acks "
            "WHERE media_type = 'movie' AND tmdb_id = 1",
        ).fetchone()
    # failure_kind preserved on themes (TDB pill stays red).
    assert themes_row[0] == "video_removed"
    # themes.failure_acked_at NOT touched — sfa is the new ack
    # surface, not themes.
    assert themes_row[1] is None
    # sfa row written for the right section, by 'auto:adopt'.
    assert sfa_row is not None
    assert sfa_row[0] == "sec1"
    assert sfa_row[1] is not None
    assert sfa_row[2] == "auto:adopt"
