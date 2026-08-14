"""v1.21.6 — notification cleanup.

Two fixes from the user's Discord review:

  Q2 — "Theme replaced via TDB" on a brand-new theme.
    REPLACE TDB hard-coded "replaced" in the theme_pushed title
    regardless of whether anything pre-existed. On a row with no
    prior theme (the user's The Last Starfighter — a brand-new TDB
    theme) that read as "replaced something" when nothing was
    there. Fix: format_theme_pushed_title gains `had_prior_theme`;
    REPLACE TDB says "set" when the row had no theme. worker.py
    threads the pre-place `cached_has_theme` signal.

  Q1 — duplicate sync notifications.
    A changed sync fired up to THREE messages — sync_completed
    (which embedded the Updated list) PLUS standalone
    themes_added_by_sync + themes_updated_by_sync messages
    repeating the same lists. Fix: fold the New/Updated title
    sections INTO the single sync_completed message, each gated by
    its existing per-section toggle; remove the two standalone
    dispatches.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()
SETTINGS_HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
CONFIG_FILE_PY = (REPO / "app" / "core" / "config_file.py").read_text()


# ── Q2: replaced-verb accuracy (real unit test) ──────────────

def test_replace_tdb_says_replaced_when_prior_theme_existed():
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "Some Movie (2020)"}
    title = format_theme_pushed_title(
        ctx, reason="replace_with_themerrdb", had_prior_theme=True)
    assert title == "📤 Theme replaced via TDB — Some Movie (2020)"


def test_replace_tdb_says_set_when_no_prior_theme():
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "The Last Starfighter (1984)"}
    title = format_theme_pushed_title(
        ctx, reason="replace_with_themerrdb", had_prior_theme=False)
    assert title == "📤 Theme set via TDB — The Last Starfighter (1984)", (
        "v1.21.6: REPLACE TDB on a row with no prior theme must say "
        "'set', not 'replaced' — nothing was replaced"
    )


def test_replace_tdb_defaults_to_replaced():
    """Default had_prior_theme=True keeps the historic wording for
    any caller that doesn't thread the signal."""
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "X (2000)"}
    assert "replaced" in format_theme_pushed_title(
        ctx, reason="replace_with_themerrdb")


def test_other_reasons_unaffected_by_prior_theme_flag():
    from app.core.notify_content import format_theme_pushed_title
    ctx = {"display_title": "X (2000)"}
    for reason, frag in (
        ("promote_bk_no_override", "promoted from backup"),
        ("plex_cloud_reupload", "Cloud backup re-uploaded"),
        (None, "pushed to Plex"),
    ):
        for prior in (True, False):
            assert frag in format_theme_pushed_title(
                ctx, reason=reason, had_prior_theme=prior)


def test_worker_threads_cached_has_theme_into_pushed_title():
    """BOTH theme_pushed dispatch sites — _do_place (movie/show) AND
    _do_place_collection (the plex_upload path, v1.21.13 fix) — must
    pass the pre-place cached_has_theme as had_prior_theme so the verb
    is accurate. v1.21.6 fixed only the first; a brand-new theme placed
    via plex_upload (a movie with no on-disk folder, or a collection)
    still said 'replaced' until v1.21.13."""
    assert WORKER_PY.count("had_prior_theme=cached_has_theme") == 2, (
        "both _do_place and _do_place_collection theme_pushed dispatches "
        "must thread had_prior_theme"
    )


# ── Q1: one consolidated sync message ────────────────────────

def test_no_standalone_sync_detail_dispatches():
    """The separate themes_added_by_sync / themes_updated_by_sync
    notifications are gone — folded into sync_completed."""
    assert 'event_kind="themes_added_by_sync"' not in WORKER_PY, (
        "v1.21.6: themes_added_by_sync must not fire its own message"
    )
    assert 'event_kind="themes_updated_by_sync"' not in WORKER_PY, (
        "v1.21.6: themes_updated_by_sync must not fire its own message"
    )
    # Only ONE sync_completed dispatch survives in the sync path.
    assert WORKER_PY.count('event_kind="sync_completed"') == 1


def test_sync_summary_has_gated_new_and_updated_sections():
    start = WORKER_PY.index("v1.18.64: body reworked")
    block = WORKER_PY[start:WORKER_PY.index("# v1.12.126", start)]
    # New section, gated by themes_added_by_sync, with 🎵 header.
    # v1.22.45: rendered via the section-grouping formatter (grouped by Plex
    # library) instead of an inline body_lines.append.
    assert '_events.get("themes_added_by_sync", False)' in block
    assert 'format_section_grouped_lines(' in block
    assert '"🎵 New:"' in block
    # Updated section, gated by themes_updated_by_sync, with 🔄.
    assert '_events.get("themes_updated_by_sync", False)' in block
    assert '"🔄 Updated:"' in block
    # The single dispatch still ships the ✅ closer.
    assert '"✅ Sync complete"' in block


def test_detail_events_still_registered_in_config():
    """Kept as distinct keys (closed-set contract / independent
    section toggles), default OFF."""
    idx = CONFIG_FILE_PY.index("_DEFAULT_NOTIFY_EVENTS")
    block = CONFIG_FILE_PY[idx:CONFIG_FILE_PY.index("}", idx)]
    assert '"themes_added_by_sync":    False' in block
    assert '"themes_updated_by_sync":  False' in block


def test_settings_toggles_reworded_as_sections():
    assert "SYNC: LIST NEW TITLES" in SETTINGS_HTML
    assert "SYNC: LIST UPDATED TITLES" in SETTINGS_HTML
    # The v1.21.6 invariant: these two FOLD into the sync summary rather than
    # firing their own notification. v0.51.263 moved that statement from the
    # SYNC COMPLETED hint into the sub-toggles' own hints + the
    # .form-checkbox-sub nesting, so pin the folding language where it lives
    # now (the old exact-sentence pin was a presentation mirror).
    assert SETTINGS_HTML.count("into the sync summary") >= 2


# ── Version pin (loose) ──────────────────────────────────────

def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
