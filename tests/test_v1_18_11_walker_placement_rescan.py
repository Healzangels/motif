"""v1.18.11 — recovery walker uses theme_id linkage + disk-check.

the user's v1.18.10 deploy report: the recovery walkers ran and
restored most rows, but a subset of orphans (07-Ghost, M*A*S*H
30th Anniversary etc.) still rendered as M instead of U after a
library refresh. Investigation: those orphans had local_files
re-inserted by v1.18.5's walker but NO placements row.

## Why v1.18.5 missed those placements

The v1.18.5 walker matched placements via:
  - `pi.guid_tmdb = themes.tmdb_id` (orphans have negative tmdb_id
    that never appears in pi.guid_tmdb — always fails for them)
  - OR `pi.title_norm = themes.title_norm AND pi.year = themes.year`
    (fragile: special characters in titles, year-vs-no-year)
  - AND trusted `pi.local_theme_file = 1` as the sidecar signal

For the user's 07-Ghost the title/year match likely failed AND
pi.local_theme_file was 0 at walker time (Plex's stat was stale)
even though the on-disk sidecar at
`/data/media/anime/07-Ghost (2009) {imdb-tt1424033}/theme.mp3`
was right there hardlinked from motif's canonical.

Symptom: after `// REPROBE PLEX THEMES` re-enumerated and flipped
`pi.local_theme_file` to 1, the row's SRC dropped from P to M
(because `p.media_folder IS NULL AND pi.local_theme_file = 1` is
the M predicate) — with PL=amber because the placements row
still didn't exist.

## v1.18.11 walker improvements

  1. **Primary lookup: pi.theme_id linkage.** v1.18.5's terminal
     `resolve_theme_ids()` call repopulates `plex_items.theme_id`
     via the standard 3-pass match (tmdb / imdb / title+year).
     For orphans with imdb_id, the imdb pass catches them even
     when tmdb/title fail. The walker now uses `theme_id` as
     the primary placement-target lookup, falling back to
     title/tmdb for rows where theme_id is still NULL.

  2. **Disk-check instead of pi.local_theme_file trust.** The
     walker now stats `pi.folder_path/theme.mp3` directly before
     inserting a placement. If the file actually exists on disk,
     the placement is valid regardless of what pi.local_theme_
     file says. Stale plex_enum state no longer blocks recovery.

  3. **Last-resort: scan in-section pi rows by file size.** For
     orphans whose title_norm completely diverged from Plex's
     (user-renamed titles, special-character normalize drift),
     walk the section's pi rows and claim the placement if a
     theme.mp3 exists at pi.folder_path AND its size matches
     the canonical (proving it's a hardlink of motif's file).

  4. **Marker key bump: `recovery_v55_done_at_v1_18_11`.** The
     v1.18.5 marker stays for audit trail, but the new key
     means v1.18.5-recovered installs (like the user's) re-run
     the improved walker once. Idempotent via INSERT OR IGNORE
     so re-runs are safe.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"


# ── Marker key bump ───────────────────────────────────────────


def test_walker_marker_key_bumped_to_v1_18_11():
    """The walker must check + stamp
    `recovery_v55_done_at_v1_18_11`, not the v1.18.5 key. This
    forces existing v1.18.5-recovered installs to re-run the
    improved placement-scan logic. The walker is idempotent
    (INSERT OR IGNORE) so re-runs don't double-insert."""
    src = RECOVERY_PY.read_text()
    assert "recovery_v55_done_at_v1_18_11" in src, (
        "v1.18.11: marker key must bump so existing recovered "
        "installs re-run the improved walker once"
    )
    # The _already_recovered check + the stamp must BOTH use the
    # new key.
    fn_start = src.index("def _already_recovered(")
    fn_end = src.index("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "recovery_v55_done_at_v1_18_11" in body


# ── Three-tier placement lookup ───────────────────────────────


def test_walker_builds_theme_id_index():
    """The walker must build a `plex_by_theme_id` lookup keyed
    on `plex_items.theme_id` — the primary placement-source path
    for orphans whose tmdb/title matching is unreliable."""
    src = RECOVERY_PY.read_text()
    assert "plex_by_theme_id" in src, (
        "v1.18.11: walker must index plex_items by theme_id "
        "as the primary placement-target lookup"
    )
    # Pin that the index is populated from a SELECT that pulls
    # theme_id.
    assert "SELECT rating_key, section_id, media_type, guid_tmdb" in src
    assert "theme_id" in src


def test_walker_indexes_all_plex_items_not_just_sidecar_marked():
    """The walker must NOT pre-filter on `local_theme_file = 1`
    when building the plex index — v1.18.5's filter missed rows
    where pi.local_theme_file was stale. v1.18.11 indexes all
    rows with a folder_path and checks the on-disk file
    directly per-candidate."""
    src = RECOVERY_PY.read_text()
    # The plex index SELECT must NOT filter on local_theme_file.
    # Walk all plex_items WHERE folder_path != '' instead.
    assert (
        "FROM plex_items WHERE folder_path != ''"
    ) in src, (
        "v1.18.11: walker must index all plex_items with a "
        "folder_path; per-candidate disk-check replaces the "
        "stale local_theme_file pre-filter"
    )


def test_walker_disk_checks_plex_folder_before_inserting():
    """For each placement candidate, the walker must stat
    `pi.folder_path/theme.mp3` to confirm the file actually
    exists. v1.18.5 trusted pi.local_theme_file; v1.18.11
    trusts the filesystem."""
    src = RECOVERY_PY.read_text()
    # The disk-check must appear in the placement loop.
    assert "plex_path = Path(pi[\"folder_path\"]) / \"theme.mp3\"" in src
    # And must call .is_file() (or similar) before inserting.
    assert "plex_path.is_file()" in src, (
        "v1.18.11: walker must call is_file() on the candidate "
        "Plex sidecar path before INSERT — stops trusting "
        "pi.local_theme_file as gospel"
    )


def test_walker_drops_last_resort_size_match_scan():
    """v1.18.12: the v1.18.11 "last-resort file-size match"
    scan was O(N²) — for each themes row that hit canonical-on-
    disk, it walked the entire section's plex_by_section list
    and stat()'d each candidate. On the user's populated install
    (5,334 themes × thousands of candidates) this caused a 7+
    minute hang with no progress signal. The tier was dropped
    in v1.18.12; theme_id (primary) + title/tmdb (fallback)
    tiers catch the common case. Edge case (orphans whose Plex
    title was user-renamed past title_norm match) is rare
    enough to leave to manual SET URL."""
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_post_v55_data_loss(")
    fn_end = src.index("\ndef ", fn_start + 1) if (
        "\ndef " in src[fn_start + 1:]
    ) else len(src)
    body = src[fn_start:fn_end]
    # The size-match scan must be GONE from executable code.
    # (Narrative comments documenting the historical tier are
    # fine — we explicitly mention "Last-resort" in a comment
    # explaining why it's removed.)
    import re
    # Strip Python comments + docstrings before the assertion.
    body_no_strings = re.sub(r'""".*?"""', '', body, flags=re.DOTALL)
    body_no_strings = re.sub(r'#.*$', '', body_no_strings, flags=re.MULTILINE)
    assert "plex_path.stat().st_size" not in body_no_strings, (
        "v1.18.12: the O(N²) last-resort size-match scan must "
        "be removed from executable code"
    )


# ── End-to-end: 07-Ghost-style orphan (theme_id, no title match) ──


@pytest.fixture
def theme_id_orphan_fixture(tmp_path: Path, monkeypatch):
    """Reproduce the user's 07-Ghost shape: orphan themes row,
    pi.theme_id linked (via v1.18.5's resolve_theme_ids call),
    pi.local_theme_file=0 (stale state), pi.title_norm differs
    from themes.title_norm (special-char normalization drift),
    pi.year differs from themes.year, AND the on-disk theme.mp3
    exists at pi.folder_path. v1.18.5 walker would miss the
    placement; v1.18.11 should catch it via the theme_id path."""
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-06T02:38:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, 0, 'anime', 1, ?, ?)",
            (ts, ts),
        )
        # Seed a non-orphan themes row to satisfy the detector's
        # "themes_n > 0 (non-orphan)" gate. The actual recovery
        # target below is the orphan; this is just so detection
        # fires.
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 999, 'Filler', 'filler', "
            "        'themoviedb', ?, ?)",
            (ts, ts),
        )
        # Orphan themes row at synthetic tmdb=-21.
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   imdb_id, upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', -21, '07-Ghost', 'simplified-title', "
            "        '2009', 'tt1424033', 'plex_orphan', ?, ?)",
            (ts, ts),
        ).lastrowid
        # Sidecar exists in the Plex folder.
        plex_folder = tmp_path / "anime_plex" / "07-Ghost (2009) {imdb-tt1424033}"
        plex_folder.mkdir(parents=True)
        sidecar = plex_folder / "theme.mp3"
        sidecar.write_bytes(b"\xff\xfb\x90" + b"\x00" * 4096)
        # Canonical exists at themes_dir.
        canonical_folder = themes_dir / "anime" / "07-Ghost (2009)"
        canonical_folder.mkdir(parents=True)
        canonical = canonical_folder / "theme.mp3"
        canonical.write_bytes(b"\xff\xfb\x90" + b"\x00" * 4096)
        # plex_items: pi.theme_id LINKED to the orphan (set by a
        # prior resolve_theme_ids via the imdb path), title_norm
        # DIFFERENT from themes.title_norm (Plex's title was
        # normalized differently — the v1.18.5 walker's title/
        # tmdb match would miss this). local_theme_file=1 so the
        # detector fires; the placement-scan is what v1.18.11
        # actually improves.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, guid_imdb, folder_path, "
            "   local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('546596', '3', 'show', '07-Ghost', "
            "        'totally different normalize', '2009', "
            "        'tt1424033', ?, 1, ?, ?, ?)",
            (str(plex_folder), themes_id, ts, ts),
        )
        conn.commit()
    return db_path, themes_dir, tmp_path


def test_walker_finds_placement_via_theme_id_when_title_mismatches(
    theme_id_orphan_fixture,
):
    """End-to-end: the orphan with pi.theme_id linked but
    title_norm + tmdb both failing — v1.18.5's title/tmdb-only
    matcher missed this. v1.18.11's primary theme_id lookup
    catches it AND the disk-check confirms the sidecar exists."""
    db_path, themes_dir, _ = theme_id_orphan_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    assert stats["detected"] is True
    assert stats["local_files_inserted"] >= 1
    assert stats["placements_inserted"] >= 1, (
        "v1.18.11: walker must find the placement via "
        "pi.theme_id linkage even when title/tmdb matching "
        "fails and pi.local_theme_file is stale"
    )
    # Verify the placement row.
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pl = conn.execute(
            "SELECT * FROM placements WHERE media_type='tv' "
            "  AND tmdb_id=-21 AND section_id='3'"
        ).fetchone()
    assert pl is not None
    assert pl["plex_rating_key"] == "546596"
    assert "07-Ghost" in pl["media_folder"]


def test_walker_disk_check_blocks_placement_when_file_missing(
    tmp_path, monkeypatch,
):
    """Counter-pin: if the file isn't actually on disk at
    pi.folder_path, the walker must NOT insert a stale
    placement row. v1.18.5 would have happily inserted based on
    pi.local_theme_file=1 alone."""
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-06T02:38:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, 'movies', 1, ?, ?)",
            (ts, ts),
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', 100, 'X', 'x', '2020', "
            "        'themoviedb', ?, ?)",
            (ts, ts),
        ).lastrowid
        # Canonical exists.
        cf = themes_dir / "movies" / "X (2020)"
        cf.mkdir(parents=True)
        (cf / "theme.mp3").write_bytes(b"X" * 4096)
        # Plex folder path POINTS at a directory but no sidecar
        # file inside.
        plex_folder = tmp_path / "movies_plex" / "X (2020)"
        plex_folder.mkdir(parents=True)
        # NOTE: no theme.mp3 written here.
        conn.execute(
            "INSERT INTO plex_items "
            "  (rating_key, section_id, media_type, title, "
            "   title_norm, year, folder_path, "
            "   local_theme_file, theme_id, "
            "   first_seen_at, last_seen_at) "
            "VALUES ('rk-X', '1', 'movie', 'X', 'x', '2020', "
            "        ?, 1, ?, ?, ?)",
            (str(plex_folder), themes_id, ts, ts),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    # local_files inserted (canonical exists), but NO placement.
    assert stats["local_files_inserted"] >= 1
    assert stats["placements_inserted"] == 0, (
        "v1.18.11: walker MUST NOT insert a placement when the "
        "on-disk Plex sidecar doesn't actually exist — even if "
        "pi.local_theme_file=1 (stale signal)"
    )
