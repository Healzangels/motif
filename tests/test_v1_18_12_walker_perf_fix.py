"""v1.18.12 — recovery walker: drop O(N²) last-resort + cache disk-checks.

the user's v1.18.11 deploy hung mid-walk past 7 minutes with no
progress signal:

  2026-05-20 12:23:48 WARNING v1.18.5 recovery: LOSS PATTERN DETECTED
  2026-05-20 12:23:48 WARNING ... Walking themes_dir + Plex folders ...
  <-- no further output after 7 minutes -->

Root cause: the v1.18.11 walker's three-tier placement-scan had
a fatal O(N²) tier:

```python
if not placed_in_section:
    # LAST-RESORT: walk plex_by_section by file size match
    for pi in plex_by_section.get((mt, section_id), []):
        if not Path(pi.folder_path / "theme.mp3").is_file():
            continue
        if plex_path.stat().st_size != stat.st_size:
            continue
        # claim placement
```

For the user's install: 5,334 themes × Movies section has 10,381
items → ~50 million `stat()` calls in the worst case. NAS-backed
I/O at ~50ms/stat = days of wall-clock time. The walk was
effectively hung.

## v1.18.12 fixes

  1. **Drop the O(N²) last-resort scan.** The theme_id (primary)
     + title/tmdb (fallback) tiers catch the common case. Edge
     case (orphans whose Plex title was user-renamed past
     title_norm match) is rare enough to leave to manual SET URL.

  2. **Cache disk-check per pi.folder_path.** Pre-compute
     `pi_on_disk: dict[rating_key] → bool` ONCE in a single pass
     over the plex_by_section union (max ~16K stats, bounded
     regardless of themes count). Placement-scan then does O(1)
     dict lookups instead of per-candidate stat() calls.

  3. **Progress logging every 250 themes.** "Black box past 7
     minutes" was a class-9 cold-path-needs-MORE-logging
     violation (CLAUDE.md). v1.18.12 emits a heartbeat at fixed
     intervals so the operator sees forward progress.

Expected walk time on the user's install post-fix:
  - Cache build: ~16K stats × ~50ms = 10-15 minutes WORST case
    (but typical disks are 1-5ms/stat → 30-90s)
  - Walk + INSERT: O(themes × sections) memory-resident dict
    lookups, completes in seconds
  - Total: under 2 minutes on most installs.

If the cache build itself is the bottleneck, we'd see the
"disk-state cache built — N pi rows checked" line firing
mid-walk. If even THAT hangs, the disk genuinely is the
bottleneck and we need per-pi stat() timeouts via threading
— but the same hang would affect every other motif file
operation, so it's an environmental issue not a motif bug.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"


# ── Last-resort scan removed ──────────────────────────────────


def test_walker_last_resort_size_match_scan_removed():
    """The O(N²) size-match scan must be GONE from executable
    code. It can stay in narrative comments documenting the
    historical tier."""
    import re
    src = RECOVERY_PY.read_text()
    fn_start = src.index("def maybe_recover_post_v55_data_loss(")
    # Strip docstrings + comments before assertion.
    body = src[fn_start:fn_start + 30000]
    body_no_strings = re.sub(r'""".*?"""', '', body, flags=re.DOTALL)
    body_no_strings = re.sub(r'#.*$', '', body_no_strings,
                             flags=re.MULTILINE)
    assert "plex_path.stat().st_size" not in body_no_strings, (
        "v1.18.12: size-match scan must be removed from "
        "executable code"
    )


# ── Disk-check cache ──────────────────────────────────────────


def test_walker_precomputes_disk_check_cache():
    """The walker must pre-compute `pi_on_disk: dict[rating_key]
    → bool` ONCE before the themes loop. Pre-fix the disk-check
    fired per (themes_row × candidate) which on the user's install
    meant millions of stat() calls."""
    src = RECOVERY_PY.read_text()
    # v0.51.15 (audit #21): the cache is now bool | None — None = the stat
    # raised (indeterminate), no longer coerced to "absent".
    assert "pi_on_disk: dict[str, bool | None] = {}" in src, (
        "v1.18.12: cache must be declared as a typed dict"
    )
    # The cache must be populated BEFORE the themes loop (which
    # starts with `for t_idx, t in enumerate(themes_rows):`).
    cache_idx = src.index("pi_on_disk: dict[str, bool | None]")
    themes_loop_idx = src.index("for t_idx, t in enumerate(themes_rows):")
    assert cache_idx < themes_loop_idx, (
        "v1.18.12: disk-check cache must be built BEFORE the "
        "themes loop, not per-iteration"
    )


def test_walker_cache_dedupes_by_rating_key():
    """A pi can appear in multiple index buckets (plex_by_section
    + plex_by_theme_id). The cache must dedupe on rating_key so
    we don't re-stat the same file."""
    src = RECOVERY_PY.read_text()
    cache_idx = src.index("pi_on_disk: dict[str, bool | None]")
    block = src[cache_idx:cache_idx + 1500]
    assert "seen_rks" in block or "seen_rks: set" in block, (
        "v1.18.12: cache build must dedupe via a seen-set"
    )


def test_walker_uses_cache_in_placement_scan():
    """The placement-scan loop must look up the cache instead
    of calling is_file() per-candidate. Pre-fix each candidate
    triggered a fresh stat() — the perf bottleneck."""
    src = RECOVERY_PY.read_text()
    # The cache lookup line must be present.
    assert "pi_on_disk.get(pi[\"rating_key\"], False)" in src, (
        "v1.18.12: candidate disk-check must use the pre-built "
        "cache, not per-candidate stat()"
    )


# ── Progress logging ──────────────────────────────────────────


def test_walker_emits_progress_every_n_themes():
    """The walker must emit a progress log line every
    PROGRESS_EVERY themes so the operator sees forward motion.
    the user's 7-minute hang on v1.18.11 was a black-box failure
    mode — class-9 cold-path-needs-MORE-logging."""
    src = RECOVERY_PY.read_text()
    assert "PROGRESS_EVERY = 250" in src
    assert "walk progress" in src, (
        "v1.18.12: walker must log progress periodically"
    )


def test_walker_cache_build_logs_summary():
    """The cache-build pass must log a summary line so the
    operator can see (a) the build completed, (b) how many pi
    rows were checked, (c) how many had a sidecar on disk."""
    src = RECOVERY_PY.read_text()
    assert "disk-state cache built" in src
    # Python concatenates adjacent string literals at parse time
    # ("foo " "bar" → "foo bar"). The source has the format
    # string split across two literals; check each fragment.
    assert "pi rows " in src
    assert "checked" in src
    assert "with on-disk sidecar" in src


# ── End-to-end: walker completes on a populated fixture ───────


@pytest.fixture
def perf_fixture(tmp_path: Path, monkeypatch):
    """Seed a fixture with enough plex_items to make the v1.18.11
    O(N²) walker measurably slow but still test-fast (~100
    plex_items × ~10 themes). The v1.18.12 walker should
    complete near-instantly thanks to the cache."""
    monkeypatch.setattr(
        "app.core.recovery_v55._SIDECAR_EVIDENCE_FLOOR", 1,
    )
    db_path = tmp_path / "motif.db"
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T12:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        # Seed 10 themes + 100 plex_items (representative of
        # the user's "many candidates per section" shape but
        # bounded for test speed).
        for tmdb_id in range(100, 110):
            conn.execute(
                "INSERT INTO themes "
                "  (media_type, tmdb_id, title, title_norm, year, "
                "   upstream_source, "
                "   last_seen_sync_at, first_seen_sync_at) "
                "VALUES ('movie', ?, ?, ?, '2020', 'themoviedb', "
                "        ?, ?)",
                (tmdb_id, f"Movie {tmdb_id}",
                 f"movie {tmdb_id}", ts, ts),
            )
        # Seed canonicals so local_files inserts fire.
        for tmdb_id in range(100, 110):
            mdir = themes_dir / "movies" / f"Movie {tmdb_id} (2020)"
            mdir.mkdir(parents=True)
            (mdir / "theme.mp3").write_bytes(b"X" * 1024)
        # Seed 100 plex_items, all with folder_path, 50 with
        # on-disk theme.mp3 (the v1.18.11 last-resort would
        # stat() all 100 per matching theme — 10 × 100 = 1000
        # stats). v1.18.12 caches this so it's one pass.
        for rk in range(500, 600):
            folder = tmp_path / "plex_movies" / f"P{rk}"
            folder.mkdir(parents=True)
            if rk < 550:
                (folder / "theme.mp3").write_bytes(b"X" * 1024)
            conn.execute(
                "INSERT INTO plex_items "
                "  (rating_key, section_id, media_type, title, "
                "   title_norm, year, folder_path, "
                "   local_theme_file, first_seen_at, last_seen_at) "
                "VALUES (?, '1', 'movie', ?, ?, '2020', ?, "
                "        1, ?, ?)",
                (str(rk), f"P{rk}", f"p{rk}", str(folder), ts, ts),
            )
        conn.commit()
    return db_path, themes_dir


def test_walker_completes_quickly_with_many_candidates(perf_fixture):
    """End-to-end perf pin: the v1.18.12 walker with 10 themes ×
    100 plex_items must complete in well under 1 second. The
    v1.18.11 implementation would do up to 1000 stat() calls;
    the v1.18.12 caches to 100 total."""
    import time
    db_path, themes_dir = perf_fixture
    from app.core.recovery_v55 import maybe_recover_post_v55_data_loss
    t0 = time.monotonic()
    stats = maybe_recover_post_v55_data_loss(db_path, themes_dir)
    elapsed = time.monotonic() - t0
    assert stats["detected"] is True
    # Tight bound: 100 stat()s on local fs should complete in
    # well under 1s. 5s is generous headroom.
    assert elapsed < 5.0, (
        f"v1.18.12: walker took {elapsed:.2f}s on a 10-themes × "
        f"100-pi fixture — should be sub-second. The O(N²) "
        f"last-resort scan may have crept back in."
    )
