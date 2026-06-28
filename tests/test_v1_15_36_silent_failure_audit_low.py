"""v1.15.36 — silent-failure audit: LOW-severity tail.

Closes out the v1.15.34 (HIGH) + v1.15.35 (MED) sweep. Triaged
the original LOW list down to 7 real findings worth shipping
(several were false positives on closer read — placement.py
cleanup leaks self-heal on next call, unlink errors already
include the OSError string in the reason field).

## Findings

### Real silent-failure (functional fix)

1. **plex.py item_has_theme JSON-era fallback** — the v1.14.78
   XML→JSON migration silently broke the substring fallback.
   JSON serializes the attr as `"theme":"..."` (no leading
   space, with colon) but the original ` theme="` substring
   check never matched, making the fallback a silent always-
   False return path. Fix: accept both shapes.

### UX visibility (logging upgrades)

2. **plex.py discover_sections** — silent `[]` on HTTP / JSON
   parse failure looked like a legit empty Plex install. Fix:
   log.warning on both paths so first-run users can diagnose
   "no sections detected" reports.
3. **sync.py git diff threshold** — "produced > N changed
   paths" was misleading (iterator break-out means we hit the
   threshold but actual total is unknown). Fix: rephrase as
   "AT LEAST N (threshold reached, actual total unknown)" so
   operator can tell safety-bail from measured-count.
4. **sync.py _do_fetch error tuple** — `("error", tmdb_id)`
   returned with no logging. Fix: log.info with media_path +
   tmdb_id + imdb_id so 100 transient timeouts can be
   distinguished from 100 upstream-deleted items.
5. **downloader.py ffmpeg fallback error** — "no MP3 produced"
   gave zero diagnostic hint. Fix: include the actual
   surviving extensions in output_dir so operator can tell
   `.m4a` produced (ffmpeg conversion failed) from `(none)`
   (yt-dlp wrote nothing — broken container ffmpeg).

### Frontend UX

6. **app.js redownload error path** — optimistic placeholder
   lingered after RE-DOWNLOAD failed (mirrors v1.15.35 fix on
   download-backup). Fix: clear placeholder via
   motifOps.clearOptimisticPlaceholder.
7. **ops.js fetchProgress persistent failure** — silent
   `return null` on every poll left the drawer stuck without
   any signal. Fix: track consecutive failures; log.warn
   after 5 in a row.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
PLEX_PY = REPO / "app" / "core" / "plex.py"
SYNC_PY = REPO / "app" / "core" / "sync.py"
DOWNLOADER_PY = REPO / "app" / "core" / "downloader.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
OPS_JS = REPO / "app" / "web" / "static" / "ops.js"


# ── 1. plex.py item_has_theme JSON-era fallback ──────────────


def test_item_has_theme_fallback_accepts_json_shape():
    """The v1.14.78 XML→JSON migration silently broke the
    fallback substring check. v1.15.36 accepts both the XML
    `' theme="'` and the JSON `'"theme":"'` shapes so the
    fallback works regardless of the response encoding."""
    src = PLEX_PY.read_text()
    fn_anchor = src.index("def item_has_theme(")
    fn_end = src.index("\n    def ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    # Both shapes accepted.
    assert "' theme=\"' in r.text" in fn_body
    assert "'\"theme\":\"' in r.text" in fn_body
    # Marker comment explains the v1.14.78 history.
    assert "v1.15.36" in fn_body
    assert "v1.14.78" in fn_body


# ── 2. plex.py discover_sections logging ─────────────────────


def test_discover_sections_logs_http_failure():
    """HTTP failure from /library/sections must log at WARNING
    so first-run users can tell "no sections" from "Plex
    unreachable / token wrong". Pre-fix returned [] silently."""
    src = PLEX_PY.read_text()
    fn_anchor = src.index("def discover_sections(")
    fn_end = src.index("\n    def ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "log.warning(" in fn_body
    # The HTTP-failure branch references "check Plex URL/token"
    # so the operator gets an actionable hint.
    assert "check Plex URL/token" in fn_body


def test_discover_sections_logs_json_parse_failure():
    """JSON parse failure must also log so a corrupted Plex
    response is distinguishable from an empty install."""
    src = PLEX_PY.read_text()
    fn_anchor = src.index("def discover_sections(")
    fn_end = src.index("\n    def ", fn_anchor + 1)
    fn_body = src[fn_anchor:fn_end]
    assert "JSON parse failed" in fn_body
    assert "treating as no sections" in fn_body


# ── 3. sync.py git diff threshold message ────────────────────


def test_git_diff_threshold_message_says_at_least():
    """The threshold-reached error must phrase the count as
    "AT LEAST N" so the operator knows it's a safety bail at
    exactly the threshold, not a measured total. Pre-fix the
    `> N` wording read like a measured count past the
    threshold."""
    src = SYNC_PY.read_text()
    # Anchor on the v1.15.36 marker.
    anchor = src.index("v1.15.36: clarify \"AT LEAST N\" wording")
    block = src[anchor:anchor + 1500]
    assert "AT LEAST" in block
    assert "actual total unknown" in block


# ── 4. sync.py _do_fetch error context ───────────────────────


def test_do_fetch_logs_failed_tmdb_id_with_context():
    """sync `_do_fetch` returning `("error", tmdb_id)` must
    log.info with media_path + tmdb_id + imdb_id so transient
    timeouts can be distinguished from upstream-deleted items
    in the docker logs."""
    src = SYNC_PY.read_text()
    fn_anchor = src.index("def _do_fetch(entry):")
    # _do_fetch is a small nested function — slice a generous
    # window past it.
    fn_body = src[fn_anchor:fn_anchor + 2500]
    assert "log.info(" in fn_body
    assert "sync fetch:" in fn_body
    assert "media_path" in fn_body
    assert "tmdb_id" in fn_body
    assert "imdb_id" in fn_body


# ── 5. downloader.py ffmpeg fallback error ───────────────────


def test_no_mp3_produced_error_lists_surviving_files():
    """The "no MP3 produced" error must include the surviving
    extensions in output_dir so the operator can diagnose
    ffmpeg conversion failures (`.m4a` produced) vs broken
    yt-dlp / container build (`(none)`)."""
    src = DOWNLOADER_PY.read_text()
    # Anchor on the v1.15.36 marker.
    anchor = src.index("v1.15.36: include the actual files found")
    block = src[anchor:anchor + 1200]
    # Lists surviving extensions in the error message.
    assert "surviving" in block
    assert "found extensions in output_dir:" in block
    # Hint when nothing was produced.
    assert "ffmpeg" in block.lower()
    assert "container build" in block


# ── 6. app.js redownload optimistic placeholder cleanup ──────


def test_redownload_clears_optimistic_placeholder_on_failure():
    """The redownload error path must clear the optimistic
    placeholder via motifOps.clearOptimisticPlaceholder
    (mirrors the v1.15.35 fix on download-backup)."""
    src = APP_JS.read_text()
    fn_anchor = src.index("async function redownload(")
    fn_end = src.index("\n  ", fn_anchor + 200)
    # Walk forward past the catch block.
    fn_body = src[fn_anchor:fn_anchor + 3500]
    # v1.15.36 marker inside the catch.
    assert "v1.15.36: clear the optimistic placeholder on failure" in fn_body
    assert "clearOptimisticPlaceholder('download_queue')" in fn_body


# ── 7. ops.js fetchProgress persistent-failure tracking ──────


def test_fetch_progress_tracks_consecutive_failures():
    """fetchProgress must track consecutive failures + log at
    console.warn after the threshold (5) so a persistent
    network outage isn't a silent stuck-drawer. Reset on first
    successful response."""
    src = OPS_JS.read_text()
    # Anchor on the v1.15.36 marker.
    anchor = src.index(
        "v1.15.36: track consecutive fetchProgress failures"
    )
    block = src[anchor:anchor + 2500]
    # Module-level streak counter + threshold constant.
    assert "_fetchProgressFailStreak" in block
    assert "_FETCH_PROGRESS_FAIL_THRESHOLD" in block
    # Logs at WARN after threshold.
    assert "console.warn(" in block
    # Reset on successful response.
    assert "_fetchProgressFailStreak = 0" in block
    # Both the non-OK branch AND the network-error branch
    # increment the streak.
    assert block.count("_fetchProgressFailStreak++") >= 2
