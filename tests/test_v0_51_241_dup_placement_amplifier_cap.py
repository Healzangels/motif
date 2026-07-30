"""v0.51.241 — an unreachable media folder is not evidence of a stale placement.

maybe_cleanup_duplicate_placements deletes a hardlink/copy placement when the
same item also has a plex_upload AND the sidecar file is MISSING. That
missing-file test is the entire safety argument — and "missing" is exactly what
an unmounted /data looks like from inside the container (on Unraid the array may
not be up when the container starts). With the mount gone every candidate reads
stale in one pass and the sweep would drop the hardlink half of every dual
placement at once: the v1.18.10 amplifier-sweep class.

A COUNT cap can't separate those cases, because the legitimate case is also
"every candidate's sidecar is missing" — the operator's real v1.19.17 run had
10 duplicates and all 10 were genuinely stale. The FOLDER is the signal that
distinguishes them: a genuinely stale row has its media folder present with
theme.mp3 gone, while an unreachable path has no folder either.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.core.db import get_conn, init_db
from app.core.recovery_v55 import maybe_cleanup_duplicate_placements

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
MARKER = "recovery_duplicate_placements_cleanup_done_at_v1_19_17"


@pytest.fixture
def db(tmp_path):
    d = tmp_path / "m.db"
    init_db(d)
    with get_conn(d) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, included,"
                  " discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',1,?,?)", (NOW, NOW))
        c.commit()
    return d


def _dual(conn, *, tmdb, folder):
    """A hardlink placement paired with a newer plex_upload of the same edition
    — the shape the sweep targets."""
    conn.execute("INSERT INTO themes (media_type, tmdb_id, title, upstream_source,"
                 " last_seen_sync_at, first_seen_sync_at) "
                 "VALUES ('movie', ?, ?, 'themoviedb', ?, ?)",
                 (tmdb, f"T{tmdb}", NOW, NOW))
    conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id,"
                 " edition_key, media_folder, placed_at, placement_kind) "
                 "VALUES ('movie', ?, '1', '', ?, ?, 'hardlink')",
                 (tmdb, folder, NOW))
    conn.execute("INSERT INTO placements (media_type, tmdb_id, section_id,"
                 " edition_key, media_folder, placed_at, placement_kind) "
                 "VALUES ('movie', ?, '1', '', '', ?, 'plex_upload')",
                 (tmdb, NOW))


def _stale_folder(tmp_path, i):
    """Genuinely stale: the movie folder EXISTS, theme.mp3 was removed."""
    d = tmp_path / f"movie{i}"
    d.mkdir()
    return str(d)


def _marker(db):
    with get_conn(db) as c:
        r = c.execute("SELECT value FROM runtime_settings WHERE key = ?",
                      (MARKER,)).fetchone()
    return r["value"] if r else None


def _placements(db):
    with get_conn(db) as c:
        return c.execute("SELECT COUNT(*) FROM placements").fetchone()[0]


# ── the amplifier case ───────────────────────────────────────────────────

def test_unreachable_folders_delete_nothing(db, tmp_path):
    """THE guard: /data is unmounted, so no candidate's folder resolves."""
    with get_conn(db) as c:
        for i in range(40):
            _dual(c, tmdb=1000 + i, folder=str(tmp_path / "unmounted" / f"m{i}"))
        c.commit()
    before = _placements(db)
    stats = maybe_cleanup_duplicate_placements(db)
    assert stats.get("deleted", 0) == 0, "an unmounted /data must delete nothing"
    assert stats.get("skipped_folder_missing", 0) == 40
    assert _placements(db) == before


def test_a_mixed_run_deletes_only_the_reachable_stale_rows(db, tmp_path):
    """Per-row, not all-or-nothing: a genuinely stale row next to unreachable
    ones is still cleaned. A count cap would have skipped the whole batch."""
    with get_conn(db) as c:
        _dual(c, tmdb=1, folder=_stale_folder(tmp_path, 1))          # real stale
        _dual(c, tmdb=2, folder=str(tmp_path / "gone" / "a"))        # unreachable
        _dual(c, tmdb=3, folder=str(tmp_path / "gone" / "b"))        # unreachable
        c.commit()
    stats = maybe_cleanup_duplicate_placements(db)
    assert stats.get("deleted", 0) == 1
    assert stats.get("skipped_folder_missing", 0) == 2


# ── the legitimate case still works ──────────────────────────────────────

def test_genuinely_stale_rows_are_still_cleaned(db, tmp_path):
    """The operator's real v1.19.17 run: every candidate's sidecar missing, all
    genuinely stale. A count cap would have blocked exactly this."""
    with get_conn(db) as c:
        for i in range(10):
            _dual(c, tmdb=2000 + i, folder=_stale_folder(tmp_path, 100 + i))
        c.commit()
    before = _placements(db)
    stats = maybe_cleanup_duplicate_placements(db)
    assert stats.get("deleted", 0) == 10
    assert stats.get("skipped_folder_missing", 0) == 0
    assert _placements(db) == before - 10
    assert _marker(db) is not None, "a completed run stamps the one-shot"


def test_a_present_sidecar_is_never_deleted(db, tmp_path):
    """Pre-existing behaviour, pinned: a real dual state (file present) is
    legitimate and must survive."""
    live = tmp_path / "live"
    live.mkdir()
    (live / "theme.mp3").write_bytes(b"ID3")
    with get_conn(db) as c:
        _dual(c, tmdb=3000, folder=str(live))
        c.commit()
    before = _placements(db)
    stats = maybe_cleanup_duplicate_placements(db)
    assert stats.get("deleted", 0) == 0
    assert stats.get("skipped_file_present", 0) == 1
    assert _placements(db) == before


def test_the_folder_check_is_the_discriminator(db):
    """Mechanism guard: deleting on a bare `sidecar missing` reading is what
    made a mount outage catastrophic."""
    import inspect
    src = inspect.getsource(maybe_cleanup_duplicate_placements)
    code = "\n".join(ln for ln in src.splitlines()
                     if not ln.lstrip().startswith("#"))
    assert "folder.is_dir()" in code, (
        "a missing sidecar must only count as stale when the folder resolves")


def test_an_unreachable_skip_leaves_the_one_shot_unstamped(db, tmp_path):
    """If we could not decide a row, the walker must NOT close itself out —
    stamping would make the guess permanent and the row would never be
    revisited. Costs one cheap re-run per boot; a mounted /data finishes it."""
    with get_conn(db) as c:
        _dual(c, tmdb=1, folder=_stale_folder(tmp_path, 1))       # decidable
        _dual(c, tmdb=2, folder=str(tmp_path / "gone" / "a"))     # not decidable
        c.commit()
    maybe_cleanup_duplicate_placements(db)
    assert _marker(db) is None, (
        "an undecidable candidate must leave the one-shot open for a retry")


def test_a_fully_decidable_run_does_stamp(db, tmp_path):
    """Contrast: nothing unreachable -> the one-shot closes as before, so this
    does not turn a one-shot into a walker that runs forever."""
    with get_conn(db) as c:
        _dual(c, tmdb=1, folder=_stale_folder(tmp_path, 1))
        c.commit()
    maybe_cleanup_duplicate_placements(db)
    assert _marker(db) is not None
