"""v1.18.21 — Audit follow-ups + RESTORE status-bar fix.

the user's audit ask + direct bug report on v1.18.20:

  1. Audit recent features (v1.18.0+) for silent failures.
  2. "While doing a Restore after purge I see the amber DL and PL
     but never any status bar in the corner indicating the
     download and place."

The audit (general-purpose agent + manual trace) turned up 6
items worth fixing in one tag. All ship together because they're
small, surface-adjacent, and benefit from one verify pass.

## v1.18.21 fix bundle

  1. **`api_clear_url_override` transaction wrap (HIGH)** — the
     two destructive operations (`_clear_previous_url` DELETE
     and `_maybe_evict_empty_orphan` DELETE) ran under autocommit
     (`get_conn` is isolation_level=None). If the themes DELETE
     raised mid-flight, the previous_urls row was already gone
     → no REVERT/RESTORE recovery + corrupt half-cleared state.
     Wrapping both in `transaction(conn)` makes them atomic.

  2. **`/manual-url` normalize_title fallback log (HIGH)** — the
     v1.18.19 fix added title_norm to the orphan INSERT but
     the `except Exception: tn = .lower()` fallback swallowed
     normalize_title errors silently. A row that hit the
     fallback would silently miss the `sql_collection_title`
     JOIN and render SRC=P with no breadcrumb. Now logs at warn.

  3. **TDB Collections card bar (MEDIUM)** — v1.18.20 added
     `<span data-bar-fill="collections">` to the template but
     missed the matching JS branch. Bar rendered permanently
     empty. New JS reads stats.collections.downloaded /
     stats.collections.total.

  4. **`maybe_repair_collection_linkage` polish (MEDIUM)**:
     a. log a per-batch summary of normalize_title fallbacks
        (same root cause as #2 above — silent .lower() drift).
     b. only stamp the v1.18.19 marker on a successful
        resolve_theme_ids call. Pre-fix the marker was stamped
        unconditionally → a resolve failure blocked any
        retry on subsequent boots.

  5. **JS info-card thumbnail sc- guard (LOW)** — `'sc-foo'`
     source_video_id combined with a YouTube currentUrl
     (post-SET-URL switch from SC to YT before lf row caught
     up) would yield img.youtube.com/vi/sc-foo/hqdefault.jpg →
     404. Mirrors the v1.18.13 'recovered' sentinel handling.

  6. **RESTORE optimistic placeholder (MEDIUM UX)** —
     `revertToThemerrDb` (REVERT/RESTORE) didn't paint the
     topbar `// QUEUING DOWNLOAD` placeholder. the user's report:
     amber DL+PL pills on the row but nothing in the topbar
     corner. Now mirrors the redownload pattern with paint-
     before-POST + clear-on-failure.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

API_PY = REPO / "app" / "web" / "api.py"
APP_JS = REPO / "app" / "web" / "static" / "app.js"
RECOVERY_PY = REPO / "app" / "core" / "recovery_v55.py"


# ── Fix 1: api_clear_url_override transaction ─────────────────


def test_clear_url_endpoint_wraps_in_transaction():
    """The `with get_conn(db) as conn:` block must escalate to
    `with get_conn(db) as conn, transaction(conn):` so the
    previous_urls DELETE and the optional themes DELETE
    (via _maybe_evict_empty_orphan) commit atomically."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_clear_url_override(")
    body = src[fn_idx:fn_idx + 4000]
    assert "with get_conn(db) as conn, transaction(conn):" in body, (
        "v1.18.21: api_clear_url_override must wrap the body in "
        "an explicit transaction so the _clear_previous_url and "
        "_maybe_evict_empty_orphan DELETEs are atomic"
    )


def test_clear_url_atomic_rollback_on_evict_failure(tmp_path: Path):
    """End-to-end: if eviction raises mid-clear, the
    previous_urls row stays put (transaction rolls back)."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db, get_conn, transaction
    from app.web.api import _clear_previous_url
    init_db(db_path)
    ts = "2026-05-20T19:00:00"
    # Seed a previous_urls row. Simulate the transaction: clear
    # the URL, then deliberately raise inside the with-block.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO previous_urls "
            "  (media_type, tmdb_id, section_id, youtube_url, "
            "   kind, captured_at) "
            "VALUES ('collection', -1, '1', "
            "        'https://youtube.com/watch?v=AAAAAAAAAAA', "
            "        'user', ?)",
            (ts,),
        )
        conn.commit()
    # Mirror the api_clear_url_override flow: open conn with
    # explicit transaction, clear the URL, raise. The transaction
    # rollback should restore the row.
    try:
        with get_conn(db_path) as conn, transaction(conn):
            _clear_previous_url(conn, "collection", -1, section_id="1")
            raise RuntimeError("simulated evict failure")
    except RuntimeError:
        pass
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT youtube_url FROM previous_urls "
            "WHERE media_type='collection' AND tmdb_id=-1"
        ).fetchone()
    assert row is not None, (
        "v1.18.21: transaction rollback must restore the "
        "previous_urls row when the evict step raises"
    )


# ── Fix 2: /manual-url normalize_title log ───────────────────


def test_manual_url_logs_normalize_title_fallback():
    """The orphan-creation INSERT in api_manual_url must log a
    warning when normalize_title raises and the code falls back
    to `.lower()`. Pre-fix the fallback was silent → SRC could
    stick at P with no breadcrumb."""
    src = API_PY.read_text()
    fn_idx = src.index("async def api_manual_url(")
    body = src[fn_idx:fn_idx + 8000]
    assert "manual_url: normalize_title(" in body
    # The log line must explicitly mention the fallback path so
    # operators searching for "fall back" / "JOIN miss" find it.
    assert "falling back to .lower()" in body
    # The except must NOT be bare — must capture the exception.
    assert "except Exception as _norm_err" in body


# ── Fix 3: TDB Collections card bar animation ─────────────────


def test_collections_bar_fill_wired_in_js():
    """The dashboard JS must compute + set the collections bar fill.
    v1.23.34: the collections card was reworked to library coverage
    like its siblings — setCov('collections', ...) sets the bar via
    the shared `[data-bar-fill="${key}"]` selector, filling with
    themed/total instead of the old downloaded/catalogue ratio."""
    js = APP_JS.read_text()
    assert "setCov('collections'" in js, (
        "v1.23.34: collections coverage card must be wired via setCov"
    )
    assert '[data-bar-fill="${key}"]' in js
    # The old downloaded/total bar formula is gone.
    assert "collectionsBucket.downloaded / collectionsBucket.total" not in js


# ── Fix 4: maybe_repair_collection_linkage polish ─────────────


def test_repair_logs_normalize_title_fallback_summary():
    """The repair helper must aggregate + log the count of rows
    that fell back to .lower() during the title_norm backfill.
    Per-row logging would spam on large libraries; one summary
    line is enough."""
    src = RECOVERY_PY.read_text()
    fn_idx = src.index("def maybe_repair_collection_linkage(")
    next_def = src.index("\ndef ", fn_idx + 1) \
        if "\ndef " in src[fn_idx + 1:] else len(src)
    body = src[fn_idx:next_def]
    assert "normalize_fallbacks = 0" in body
    assert "normalize_fallbacks += 1" in body
    assert "fell back to .lower()" in body


def test_repair_only_stamps_marker_on_resolve_success():
    """The v1.18.19 marker must only be written when
    resolve_theme_ids succeeded. Pre-fix the marker was stamped
    unconditionally — a resolve raise left the marker in place
    blocking any retry."""
    src = RECOVERY_PY.read_text()
    fn_idx = src.index("def maybe_repair_collection_linkage(")
    next_def = src.index("\ndef ", fn_idx + 1) \
        if "\ndef " in src[fn_idx + 1:] else len(src)
    body = src[fn_idx:next_def]
    # The marker write must be inside a `if stats["resolve_called"]:`
    # guard.
    assert 'if stats["resolve_called"]:' in body
    # And there must be an else-branch that warns about not
    # stamping.
    assert "marker NOT" in body, (
        "v1.18.21: must warn when marker is skipped so "
        "operators know to expect a retry on next boot"
    )


# ── Fix 5: JS info-card thumbnail sc- guard ───────────────────


def test_info_card_drops_sc_prefix_for_youtube_url():
    """The ytId derivation must drop a sc- prefix when the
    currentUrl is a YouTube URL — `sc-<artist>-<slug>` is the
    SoundCloud sentinel and will 404 against img.youtube.com."""
    js = APP_JS.read_text()
    # Pin the guard.
    assert "ytId.startsWith('sc-')" in js
    # And the gating on YouTube URL source.
    idx = js.index("ytId.startsWith('sc-')")
    block = js[idx:idx + 400]
    assert "urlSource(currentUrl) === 'youtube'" in block


# ── Fix 6: RESTORE optimistic placeholder ─────────────────────


def test_revert_paints_optimistic_placeholder_before_post():
    """revertToThemerrDb must call setOptimisticPlaceholder with
    'download_queue' BEFORE the API POST. Mirrors the redownload
    flow at app.js:~3021 so the topbar lights up the same frame
    the user clicks RESTORE/REVERT."""
    js = APP_JS.read_text()
    fn_idx = js.index("async function revertToThemerrDb(")
    fn_body = js[fn_idx:fn_idx + 3000]
    assert "setOptimisticPlaceholder(" in fn_body, (
        "v1.18.21: revertToThemerrDb must paint the optimistic "
        "placeholder so the topbar shows the queued state"
    )
    # The label must be the canonical `// QUEUING DOWNLOAD` so
    # the placeholder dedupes with other download-queue paints.
    assert "// QUEUING DOWNLOAD" in fn_body
    # Paint must precede the POST (the placeholder needs to be
    # up before the round-trip latency kicks in).
    placeholder_idx = fn_body.index("setOptimisticPlaceholder")
    post_idx = fn_body.index("api('POST'")
    assert placeholder_idx < post_idx, (
        "v1.18.21: placeholder must paint BEFORE the API POST"
    )


def test_revert_clears_placeholder_on_failure():
    """If the API POST fails, revertToThemerrDb must clear the
    optimistic placeholder so the topbar doesn't show
    `// QUEUING DOWNLOAD` for a row that didn't queue. Mirrors
    the v1.15.36 redownload-failure cleanup."""
    js = APP_JS.read_text()
    fn_idx = js.index("async function revertToThemerrDb(")
    fn_body = js[fn_idx:fn_idx + 3000]
    assert "clearOptimisticPlaceholder(" in fn_body
    # The clear call must be in the catch block.
    catch_idx = fn_body.index("catch (e)")
    clear_idx = fn_body.index("clearOptimisticPlaceholder")
    assert catch_idx < clear_idx, (
        "v1.18.21: placeholder clear must live in the catch arm"
    )


# ── Combined: marker-versioning regression ────────────────────


def test_collection_linkage_marker_not_stamped_on_resolve_failure(
    tmp_path: Path,
):
    """End-to-end: when resolve_theme_ids raises, the v1.18.19
    marker stays absent so the next boot retries the repair."""
    db_path = tmp_path / "motif.db"
    from app.core.db import init_db
    from app.core import recovery_v55
    init_db(db_path)
    # Monkeypatch resolve_theme_ids to raise.
    import app.core.plex_enum as plex_enum_mod
    orig = plex_enum_mod.resolve_theme_ids
    plex_enum_mod.resolve_theme_ids = lambda *args, **kwargs: (
        (_ for _ in ()).throw(RuntimeError("simulated resolve failure"))
    )
    try:
        stats = recovery_v55.maybe_repair_collection_linkage(db_path)
    finally:
        plex_enum_mod.resolve_theme_ids = orig
    assert stats["resolve_called"] is False
    # Marker absent → next call will retry.
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_settings "
            "WHERE key = 'recovery_collection_linkage_done_at_v1_18_19'"
        ).fetchone()
    assert row is None, (
        "v1.18.21: marker must NOT be stamped when "
        "resolve_theme_ids raises"
    )
