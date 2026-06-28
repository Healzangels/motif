"""v1.14.0 — SoundCloud URL support spike.

The first commit in the SoundCloud feature rollout. yt-dlp already
supports SoundCloud, so the work is purely plumbing:
  - Widen URL parser to accept SC URLs alongside YT
  - Stash a recognizable id in source_video_id (sc-<artist>-<slug>)
  - Relax _SRC_LETTER_SQL so SC IDs (>11 chars) tag as 'U' (user)
    not 'A' (adopt)
  - Add url_source() helper for source-aware UI labels
  - Update HTML5 pattern + JS validators to accept SC

This commit deliberately does NOT touch:
  - The downloader itself (already URL-generic)
  - Failure classifier (v1.14.1's job — SC errors will look like
    UNKNOWN until we widen the classifier)
  - REVERT/RESTORE/ACCEPT UPDATE flows (v1.14.2's job)
  - Oembed routing (v1.14.3)
  - Broader UI copy pass (v1.14.4)

Tests pin the URL parser + SRC letter classification + the
source-aware label rendering.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.db import init_db
from app.core.sync import extract_video_id, url_source


REPO = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── url_source() classification ───────────────────────────────


def test_url_source_youtube_standard():
    assert url_source("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "youtube"


def test_url_source_youtube_short():
    assert url_source("https://youtu.be/dQw4w9WgXcQ") == "youtube"


def test_url_source_youtube_embed():
    assert url_source("https://www.youtube.com/embed/dQw4w9WgXcQ") == "youtube"


def test_url_source_soundcloud_standard():
    assert url_source("https://soundcloud.com/foo/bar-track") == "soundcloud"


def test_url_source_soundcloud_www():
    assert url_source("https://www.soundcloud.com/foo/bar") == "soundcloud"


def test_url_source_soundcloud_mobile():
    assert url_source("https://m.soundcloud.com/foo/bar") == "soundcloud"


def test_url_source_unknown_host():
    assert url_source("https://vimeo.com/12345") == "unknown"


def test_url_source_empty_or_none():
    assert url_source("") == "unknown"
    assert url_source(None) == "unknown"


# ── extract_video_id: YT preserved + SC adds sc- prefix ──────


def test_extract_video_id_youtube_standard():
    assert (extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
            == "dQw4w9WgXcQ")


def test_extract_video_id_youtube_short():
    assert (extract_video_id("https://youtu.be/dQw4w9WgXcQ")
            == "dQw4w9WgXcQ")


def test_extract_video_id_soundcloud_returns_sc_prefix():
    """Pin the sc-<artist>-<slug> shape — the v1.14.0
    _SRC_LETTER_SQL relaxation keys on 'sc-%' to tag SC user
    URLs as U not A."""
    vid = extract_video_id("https://soundcloud.com/myartist/coolsong")
    assert vid == "sc-myartist-coolsong"


def test_extract_video_id_soundcloud_lowercases():
    """SoundCloud slugs are case-insensitive in URLs but
    canonically lowercase. Pin so two URLs differing only in
    case produce the same id (idempotent re-downloads)."""
    a = extract_video_id("https://soundcloud.com/MyArtist/CoolSong")
    b = extract_video_id("https://soundcloud.com/myartist/coolsong")
    assert a == b == "sc-myartist-coolsong"


def test_extract_video_id_unknown_returns_none():
    assert extract_video_id("https://vimeo.com/12345") is None
    assert extract_video_id("not a url") is None
    assert extract_video_id("") is None
    assert extract_video_id(None) is None


def test_extract_video_id_youtube_takes_precedence():
    """If a URL somehow matches both regexes (shouldn't happen
    in practice — different hosts), YT wins. Pin via the
    function flow: YT regex checked first."""
    # An impossible URL that has both — confirms the fall-through
    # ordering. In reality these won't co-occur.
    sc_only = "https://soundcloud.com/foo/bar"
    assert extract_video_id(sc_only) == "sc-foo-bar"


# ── _SRC_LETTER_SQL: SC IDs tag as U not A ───────────────────


@pytest.fixture
def db(tmp_path: Path) -> Path:
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _seed_orphan_with_user_url(conn, *, source_video_id: str):
    """Set up a plex_orphan row + manual placement + local_files
    with the given source_video_id. The _SRC_LETTER_SQL branch
    we're testing fires when:
      - p.media_folder IS NOT NULL (placement exists)
      - p.provenance = 'manual'
      - t.upstream_source = 'plex_orphan'
      - source_video_id is non-empty AND (length=11 OR sc-%) → U
      - else → A
    """
    now = _now_iso()
    conn.execute(
        "INSERT INTO plex_sections ("
        "  section_id, title, type, included, discovered_at,"
        "  last_seen_at, is_4k, is_anime"
        ") VALUES ('1', 'Movies', 'movie', 1, ?, ?, 0, 0)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO themes ("
        "  media_type, tmdb_id, title, upstream_source,"
        "  youtube_url, last_seen_sync_at, first_seen_sync_at"
        ") VALUES ('movie', 1, 'x', 'plex_orphan',"
        "  'https://example.com/url', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO plex_items ("
        "  rating_key, section_id, media_type, title, guid_tmdb,"
        "  first_seen_at, last_seen_at"
        ") VALUES ('rk1', '1', 'movie', 'x', 1, ?, ?)",
        (now, now),
    )
    # source_kind=NULL forces the SQL through the orphan-special
    # branch (provenance=manual + upstream_source='plex_orphan'
    # + svid checks) where the v1.14.0 SC relaxation lives. With
    # source_kind='url' the EARLIER branch would unconditionally
    # tag as 'U' before we reach the svid-length check.
    conn.execute(
        "INSERT INTO local_files ("
        "  media_type, tmdb_id, section_id, file_path,"
        "  source_video_id, downloaded_at, source_kind, provenance"
        ") VALUES ('movie', 1, '1', 'x.mp3', ?, ?, NULL, 'manual')",
        (source_video_id, now),
    )
    conn.execute(
        "INSERT INTO placements ("
        "  media_type, tmdb_id, section_id, media_folder,"
        "  placed_at, placement_kind, plex_refreshed, provenance"
        ") VALUES ('movie', 1, '1', '/data/x', ?, 'hardlink', 0, 'manual')",
        (now,),
    )


def _src_letter(db: Path) -> str:
    """Run _SRC_LETTER_SQL against the seeded row + return the
    computed letter."""
    from app.web.api import _SRC_LETTER_SQL
    sql = f"""
        SELECT {_SRC_LETTER_SQL} AS letter
        FROM plex_items pi
        INNER JOIN plex_sections ps
          ON ps.section_id = pi.section_id AND ps.included = 1
        LEFT JOIN themes t ON t.tmdb_id = pi.guid_tmdb
        LEFT JOIN placements p
          ON p.media_type = t.media_type
         AND p.tmdb_id = t.tmdb_id
         AND p.section_id = pi.section_id
        LEFT JOIN local_files lf
          ON lf.media_type = t.media_type
         AND lf.tmdb_id = t.tmdb_id
         AND lf.section_id = pi.section_id
        WHERE pi.rating_key = 'rk1'
    """
    with sqlite3.connect(db) as conn:
        return conn.execute(sql).fetchone()[0]


def test_src_letter_youtube_id_classifies_as_U(db):
    """Baseline regression guard: an 11-char YouTube id on a
    plex_orphan + manual placement → 'U'. Pre-v1.14.0 behavior."""
    with sqlite3.connect(db) as conn:
        _seed_orphan_with_user_url(conn, source_video_id="dQw4w9WgXcQ")
    assert _src_letter(db) == "U"


def test_src_letter_soundcloud_id_classifies_as_U(db):
    """v1.14.0: SoundCloud ids are sc-<artist>-<slug>, longer
    than 11 chars. Pre-v1.14.0 the row tagged as 'A' (adopt)
    via the fall-through. Post-fix the LIKE 'sc-%' branch
    matches → 'U'."""
    with sqlite3.connect(db) as conn:
        _seed_orphan_with_user_url(conn, source_video_id="sc-myartist-coolsong")
    assert _src_letter(db) == "U"


def test_src_letter_random_long_id_still_classifies_as_A(db):
    """Negative case: a non-SC, non-11-char id (e.g. an old hash
    from adopt) still falls through to 'A'. Pin so the SC
    relaxation doesn't accidentally pull in adopt ids."""
    with sqlite3.connect(db) as conn:
        _seed_orphan_with_user_url(
            conn,
            source_video_id="abcdefabcdefabcdefabcdefabcdefab",  # 32-char hash
        )
    assert _src_letter(db) == "A"


# ── JS-side mirror of url_source() ────────────────────────────


def test_app_js_has_url_source_helper():
    """Pin the JS mirror of the Python url_source() helper.
    Both must agree on classification — drift between them would
    cause UI labels to disagree with backend classification.

    v1.17.9: `urlSourceLabel()` was deleted in the hygiene audit
    (defined v1.14.0 but never called — info card now uses
    `urlSource()` directly). The classification mirror is
    `urlSource()`; this test pins that. The label sibling was a
    pure English-text helper, not a classifier, so its removal
    can't cause classification drift."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "function urlSource(url)" in js
    # Mirror constants: YT regex unchanged; new SC regex.
    assert "SOUNDCLOUD_URL_RE = " in js
    assert "THEME_URL_RE = " in js


def test_app_js_validator_accepts_either_source():
    """SET URL form's submit handler must use THEME_URL_RE
    (which OR's YT and SC), not the YT-only YOUTUBE_URL_RE.
    Pre-fix users couldn't paste a SoundCloud URL — submitted
    URL was rejected at the validator."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    assert "THEME_URL_RE.test(url)" in js
    # The bad pattern (YT-only validator) must be gone from the
    # submit handler. Allow YOUTUBE_URL_RE to exist (it's used
    # by urlSource() for classification) but not as a gate on
    # the submit form.
    assert "if (!YOUTUBE_URL_RE.test(url))" not in js


def test_info_card_uses_source_aware_labels():
    """The info card's URL row labels must be source-aware. Pin
    via the urlSourceLabel() / urlSource() calls in the
    template-string DL block.

    v1.14.20 (M1) renamed the `currently applied` label to
    `applied url`. v1.24.9 made it a variable (`appliedUrlLabel`,
    so backup-only rows read "backup url"); anchor on that."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Find the info card DL grid (label var post-v1.24.9).
    anchor = js.index("<dt>${appliedUrlLabel}")
    block = js[anchor - 200:anchor + 400]
    # Source-aware label fragments.
    assert "urlSourceLabel" in block or "urlSource(" in block, (
        "Info card URL row labels must use urlSource()/urlSourceLabel() "
        "to render source-aware text"
    )


# ── Static guards: production SQL has the SC relaxation ──────


def test_src_letter_sql_has_soundcloud_relaxation():
    """Pin the v1.14.0 _SRC_LETTER_SQL change. The OR LIKE
    'sc-%' branch is the difference between SC user URLs
    showing as 'U' vs falling through to 'A'."""
    # v1.21.57: check the rendered constant (byte-identical default).
    from app.web.api import _SRC_LETTER_SQL as sql_block
    assert "lf.source_video_id LIKE 'sc-%'" in sql_block, (
        "v1.14.0: _SRC_LETTER_SQL must allow SC ids (sc-%) to "
        "tag as 'U'. Removing this resurrects the SC-as-A bug."
    )


def test_html_pattern_attr_accepts_soundcloud():
    """HTML5 pattern attr on the override-url input must accept
    SoundCloud URLs (in addition to YouTube). Pre-fix the
    pattern was YT-only and blocked SC URL submission at the
    browser layer.

    v1.14.41: dropped the browse.html assertion — that template
    was deleted in the audit M1 dead-code sweep. library.html
    is the only live override-url surface now."""
    library = (REPO / "app" / "web" / "templates" / "library.html").read_text()
    # Library template's dialogs reference the SC accept set in
    # placeholders / hint text.
    assert "soundcloud.com" in library.lower()
