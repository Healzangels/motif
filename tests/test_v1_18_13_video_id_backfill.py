"""v1.18.13 — info-card thumbnail fix + video_id backfill.

After the user's v1.18.12 deploy completed the walker, 07-Ghost +
M*A*S*H finally showed SRC=U as expected — but their info cards
rendered with the broken-image YouTube thumbnail icon:

  [grey cassette placeholder]
  ▸ click to watch on YouTube

Root cause: the v1.18.5 recovery walker stamps
`local_files.source_video_id = themes.youtube_video_id or
'recovered'`. For orphans (themes.youtube_video_id IS NULL),
this writes the literal string 'recovered' as a sentinel.
The info card builds the thumbnail URL as
`https://img.youtube.com/vi/{source_video_id}/hqdefault.jpg`
— and `img.youtube.com/vi/recovered/hqdefault.jpg` 404s,
yielding the broken-image icon in the UI.

The SRC letter SQL doesn't suffer from this because once the
v1.18.10 user_overrides walker reclassifies source_kind='adopt'
→ 'url', the FIRST branch (`lf.source_kind IN ('url','upload')
THEN 'U'`) fires before the length-of-video-id check is reached.
But the thumbnail render code looks at source_video_id directly.

## v1.18.13 fixes

  1. **Frontend (app.js)**: treat 'recovered' as falsy in the
     ytId derivation chain. The fallback that extracts video_id
     from currentUrl already exists — we just need to skip
     'recovered' so it fires. Immediate visual fix without DB
     changes; takes effect on next page load.

  2. **Backend walker (recovery_v55.py)**: the v1.18.10
     user_overrides recovery walker now ALSO overwrites
     source_video_id from the override URL via extract_video_id.
     Going-forward correctness for installs where the walker
     runs fresh.

  3. **One-shot backfill (recovery_v55.py)**: new
     `maybe_backfill_recovered_video_ids` function scans
     local_files WHERE source_video_id='recovered', joins to
     user_overrides for the URL, and parses + writes the real
     video_id. Idempotent + one-shot via marker
     `recovery_video_ids_done_at` — covers existing installs
     where v1.18.10 already stamped its marker (the user's case).

  4. **Wired into main.py startup** as the third recovery step
     after the v1.18.5 walker + v1.18.10 user_overrides walker.

Rows with no matching user_overrides (orphans the user adopted
from sidecar without ever setting a URL) keep their 'recovered'
sentinel — there's no source to parse from. Those rows render
correctly as SRC=A anyway, and they have no YouTube thumbnail
to display (the source-aware thumbnail block only fires for
youtube/soundcloud URL sources).
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
MAIN_PY = REPO / "app" / "main.py"


# ── Frontend: ytId derivation skips 'recovered' ───────────────


def test_frontend_skips_recovered_sentinel_in_ytid():
    """app.js's info-card ytId derivation must treat the
    'recovered' sentinel as falsy so the URL-extraction fallback
    can populate ytId from the user_overrides URL. Pre-fix the
    truthy non-empty string short-circuited the fallback."""
    js = APP_JS.read_text()
    assert "if (ytId === 'recovered') ytId = '';" in js, (
        "v1.18.13: ytId derivation must skip the 'recovered' "
        "sentinel value (truthy but unusable as a YouTube id)"
    )
    # v1.18.13: explicit marker comment so future grep finds it.
    assert "v1.18.13:" in js


# ── Thumbnail isolation regression guards ─────────────────────
# the user's v1.18.13 audit ask: "be extra sure the preview
# thumbnail is showing what the actual current set theme is and
# not grabbing a previous url or something similar." These tests
# pin the contract: the YouTube thumbnail block + the ytId
# derivation that feeds it must NEVER reference previousUrl /
# previous_url. The thumbnail is driven solely by currentUrl
# (= ovr.youtube_url ?? t.youtube_url) and lf.source_video_id.
#
# Future-proofing: if a refactor accidentally pulls previousUrl
# into the thumbnail path (e.g. by merging the diff-section
# fallback into the thumbnail block), these guards fail loudly
# before the misleading thumbnail ships.


def _extract_thumbnail_block() -> str:
    """Locate the YouTube info-card thumbnail block in app.js.

    Anchored on the v1.15.129 marker comment + bounded by the
    closing `</div>` of the youtube branch (immediately before
    the SoundCloud branch). Returns the source text inside.
    """
    js = APP_JS.read_text()
    start_marker = "v1.15.129: source-aware thumbnail block"
    block_start = js.index(start_marker)
    # Walk forward to the SoundCloud/Instagram branch which closes the
    # youtube branch's scope. v1.20.26 merged Instagram into this branch
    # (`if ((tUrlSrc === 'soundcloud' || tUrlSrc === 'instagram')`).
    sc_marker = "tUrlSrc === 'soundcloud' || tUrlSrc === 'instagram'"
    block_end = js.index(sc_marker, block_start)
    return js[block_start:block_end]


def _extract_ytid_derivation_block() -> str:
    """Locate the ytId derivation block in openInfoDialog.

    Anchored on the v1.14.20 H1 marker comment + bounded by the
    closing `}` of the `if (currentUrl) {` scope.
    """
    js = APP_JS.read_text()
    start_marker = "v1.14.20 (H1): rewritten to derive from currentUrl"
    block_start = js.index(start_marker)
    # The block ends at the `const imdb = ...` line that comes
    # immediately after the if-block's closing brace (stable
    # anchor since v1.14.20).
    end_marker = "const imdb = t.imdb_id"
    block_end = js.index(end_marker, block_start)
    return js[block_start:block_end]


def test_thumbnail_block_does_not_reference_previous_url():
    """The YouTube thumbnail block must not touch previousUrl in
    any form. Pre-emptive guard: a refactor that accidentally
    pulls previousUrl into the img src / href would show a stale
    thumbnail for the prior URL on rows the user just updated."""
    block = _extract_thumbnail_block()
    assert "previousUrl" not in block, (
        "Thumbnail block referenced previousUrl — "
        "should be driven by currentUrl/ytUrl only"
    )
    assert "previous_url" not in block, (
        "Thumbnail block referenced previous_url payload field"
    )
    assert "previousUrlObj" not in block


def test_thumbnail_src_uses_ytid_only():
    """The thumbnail <img> src must build the img.youtube.com URL
    from `ytId` (the derived video id of what's currently
    playing) — never from a previous-url-derived id."""
    block = _extract_thumbnail_block()
    assert "img.youtube.com/vi/${htmlEscape(ytId)}/hqdefault.jpg" in block, (
        "Thumbnail src must template against ytId — the var "
        "fed by lf.source_video_id + currentUrl extraction"
    )


def test_thumbnail_anchor_links_current_url_not_previous():
    """The 'click to watch on YouTube' link wrapping the
    thumbnail must href to ytUrl (= currentUrl). Linking the
    user away to a previous video would be a worse failure
    mode than the broken-image icon."""
    block = _extract_thumbnail_block()
    assert 'href="${htmlEscape(ytUrl)}"' in block, (
        "Thumbnail anchor must href to ytUrl (the currently "
        "applied URL), not previousUrl"
    )


def test_ytid_derivation_does_not_reference_previous_url():
    """The ytId derivation chain feeds the thumbnail src. It
    must read from lf.source_video_id and currentUrl only —
    never previousUrl. Catches a regression where someone adds
    a 'fallback to previous_url' branch that would silently
    poison the thumbnail."""
    block = _extract_ytid_derivation_block()
    assert "previousUrl" not in block, (
        "ytId derivation referenced previousUrl — should be "
        "currentUrl + lf.source_video_id only"
    )
    assert "previous_url" not in block, (
        "ytId derivation referenced previous_url payload field"
    )


def test_ytid_derivation_reads_currentUrl_and_lf_only():
    """Positive guard mirror of the negative one above: the
    derivation must reference the two canonical sources
    (currentUrl + lf.source_video_id). If a refactor renames
    these, the negative guards lose their teeth — this one
    catches that case so the contract stays explicit."""
    block = _extract_ytid_derivation_block()
    assert "lf.source_video_id" in block
    assert "currentUrl.match" in block, (
        "Fallback URL-extraction must parse currentUrl directly"
    )


def test_thumbnail_block_is_gated_on_currentUrl():
    """The thumbnail block must only render when ytId is set,
    which requires currentUrl to be non-empty (the `if
    (currentUrl)` gate in the derivation block above). M/A/P
    rows where currentUrl='' must NOT produce a thumbnail —
    they have no canonical "currently playing URL" to preview."""
    deriv = _extract_ytid_derivation_block()
    # The if (currentUrl) gate must wrap the ytId assignment.
    assert "if (currentUrl) {" in deriv
    # The thumbnail block itself gates on ytId being truthy.
    block = _extract_thumbnail_block()
    assert "tUrlSrc === 'youtube' && ytId" in block, (
        "Thumbnail render must gate on both URL source AND a "
        "non-empty ytId — without the latter M/A/P rows would "
        "render a broken-image icon"
    )


# ── Backend: backfill function exists and wires in ────────────


def test_backfill_function_exists():
    """`maybe_backfill_recovered_video_ids(db_path)` must be
    importable from app.core.recovery_v55."""
    src = RECOVERY_PY.read_text()
    assert "def maybe_backfill_recovered_video_ids(" in src


def test_backfill_uses_extract_video_id():
    """The backfill must delegate URL parsing to the canonical
    sync.extract_video_id helper — that handles both YouTube
    11-char ids and SoundCloud 'sc-' sentinels uniformly."""
    src = RECOVERY_PY.read_text()
    bf_idx = src.index("def maybe_backfill_recovered_video_ids(")
    body = src[bf_idx:bf_idx + 4000]
    assert "from .sync import extract_video_id" in body
    assert "extract_video_id(" in body


def test_backfill_uses_independent_marker():
    """The backfill must use its own runtime_settings marker
    (recovery_video_ids_done_at) independent of the v1.18.5 +
    v1.18.10 markers — so it runs even on installs where the
    earlier walkers already completed."""
    src = RECOVERY_PY.read_text()
    assert "recovery_video_ids_done_at" in src


# ── Backend: v1.18.10 walker also stamps source_video_id ──────


def test_v18_10_walker_overwrites_recovered_video_id():
    """The v1.18.10 user_overrides walker must, on successful
    INSERT, ALSO overwrite local_files.source_video_id from
    'recovered' to the real id parsed from the override URL.
    Going-forward correctness for installs where the v1.18.10
    walker runs fresh."""
    src = RECOVERY_PY.read_text()
    v10_idx = src.index("def maybe_recover_lost_user_overrides(")
    # Bound the body by the next `def ` so the assertion can't
    # leak into the maybe_backfill_recovered_video_ids body.
    next_def = src.index("\ndef ", v10_idx + 1)
    body = src[v10_idx:next_def]
    # v1.18.13 marker comment in the v1.18.10 walker body.
    assert "v1.18.13" in body
    # The UPDATE statement that overwrites the sentinel.
    assert "source_video_id = 'recovered'" in body
    assert "SET source_video_id = ?" in body


# ── End-to-end: backfill fixture ──────────────────────────────


@pytest.fixture
def recovered_video_id_fixture(tmp_path: Path):
    """Seed a fixture mirroring the user's post-v1.18.12 state:
    orphan with user_overrides row + local_files row where
    source_video_id='recovered'. After backfill runs, source_
    video_id must hold the real YouTube id."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T13:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('2', 'Anime', 'show', 1, 0, "
            "        'anime', 1, ?, ?)",
            (ts, ts),
        )
        # 07-Ghost orphan with user URL set.
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', -33, '07-Ghost', "
            "        '07 ghost', '2009', 'plex_orphan', ?, ?)",
            (ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, theme_id, youtube_url, "
            "   set_at, set_by, section_id) "
            "VALUES ('tv', -33, ?, "
            "        'https://www.youtube.com/watch?v=7yOut8TgdU8', "
            "        ?, 'admin', '2')",
            (themes_id, ts),
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('tv', -33, '2', "
            "        'anime/07-Ghost (2009)/theme.mp3', "
            "        ?, 'recovered', 'manual', 'url')",
            (ts,),
        )
        conn.commit()
    return db_path


def test_backfill_overwrites_recovered_sentinel(
    recovered_video_id_fixture,
):
    """End-to-end: backfill scans 'recovered' rows, parses the
    user_overrides URL, and writes the real 11-char video id."""
    db_path = recovered_video_id_fixture
    from app.core.recovery_v55 import maybe_backfill_recovered_video_ids
    stats = maybe_backfill_recovered_video_ids(db_path)
    assert stats["scanned"] >= 1, (
        "v1.18.13: backfill must find the 'recovered' row joined "
        "to user_overrides"
    )
    assert stats["updated"] >= 1, (
        "v1.18.13: backfill must UPDATE the row with the real "
        "video id parsed from the override URL"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT source_video_id FROM local_files "
            "WHERE media_type='tv' AND tmdb_id=-33"
        ).fetchone()
    assert row["source_video_id"] == "7yOut8TgdU8", (
        f"v1.18.13: expected '7yOut8TgdU8', got "
        f"{row['source_video_id']!r}"
    )


def test_backfill_is_idempotent(recovered_video_id_fixture):
    """Second invocation must short-circuit on the marker."""
    db_path = recovered_video_id_fixture
    from app.core.recovery_v55 import maybe_backfill_recovered_video_ids
    maybe_backfill_recovered_video_ids(db_path)
    stats2 = maybe_backfill_recovered_video_ids(db_path)
    assert stats2["skipped_reason"] == "marker_set"
    assert stats2["scanned"] == 0
    assert stats2["updated"] == 0


def test_backfill_skips_rows_without_user_override(tmp_path: Path):
    """Rows with source_video_id='recovered' but NO matching
    user_overrides (orphans adopted via PROBE without a URL)
    must be left alone — there's no URL to parse a video id from
    and those rows correctly render as SRC=A (no thumbnail
    expected)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T13:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -99, 'Lonely Adopt', "
            "        'lonely adopt', '2005', 'plex_orphan', ?, ?)",
            (ts, ts),
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', -99, '1', "
            "        'movies/Lonely Adopt (2005)/theme.mp3', "
            "        ?, 'recovered', 'manual', 'adopt')",
            (ts,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_backfill_recovered_video_ids
    stats = maybe_backfill_recovered_video_ids(db_path)
    # JOIN requires user_overrides row → row never reaches the
    # UPDATE → updated=0.
    assert stats["updated"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT source_video_id FROM local_files "
            "WHERE media_type='movie' AND tmdb_id=-99"
        ).fetchone()
    assert row["source_video_id"] == "recovered", (
        "Adopt-only orphan with no user URL must keep its "
        "'recovered' sentinel untouched"
    )


def test_backfill_handles_soundcloud_urls(tmp_path: Path):
    """SoundCloud overrides yield 'sc-<artist>-<slug>' sentinels
    via extract_video_id. The backfill must store those too —
    the frontend recognizes them via the LIKE 'sc-%' branch."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    init_db(db_path)
    ts = "2026-05-20T13:00:00"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO plex_sections "
            "  (section_id, title, type, is_anime, is_4k, "
            "   themes_subdir, included, discovered_at, last_seen_at) "
            "VALUES ('1', 'Movies', 'movie', 0, 0, "
            "        'movies', 1, ?, ?)",
            (ts, ts),
        )
        themes_id = conn.execute(
            "INSERT INTO themes "
            "  (media_type, tmdb_id, title, title_norm, year, "
            "   upstream_source, "
            "   last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('movie', -77, 'SC Orphan', "
            "        'sc orphan', '2010', 'plex_orphan', ?, ?)",
            (ts, ts),
        ).lastrowid
        conn.execute(
            "INSERT INTO user_overrides "
            "  (media_type, tmdb_id, theme_id, youtube_url, "
            "   set_at, set_by, section_id) "
            "VALUES ('movie', -77, ?, "
            "        'https://soundcloud.com/artist/awesome-track', "
            "        ?, 'admin', '1')",
            (themes_id, ts),
        )
        conn.execute(
            "INSERT INTO local_files "
            "  (media_type, tmdb_id, section_id, file_path, "
            "   downloaded_at, source_video_id, "
            "   provenance, source_kind) "
            "VALUES ('movie', -77, '1', "
            "        'movies/SC Orphan (2010)/theme.mp3', "
            "        ?, 'recovered', 'manual', 'url')",
            (ts,),
        )
        conn.commit()
    from app.core.recovery_v55 import maybe_backfill_recovered_video_ids
    stats = maybe_backfill_recovered_video_ids(db_path)
    assert stats["updated"] >= 1
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT source_video_id FROM local_files "
            "WHERE media_type='movie' AND tmdb_id=-77"
        ).fetchone()
    assert row["source_video_id"] == "sc-artist-awesome-track", (
        f"SoundCloud sentinel expected, got "
        f"{row['source_video_id']!r}"
    )
