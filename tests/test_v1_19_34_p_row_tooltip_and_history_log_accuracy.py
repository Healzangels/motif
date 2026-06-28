"""v1.19.34 — tooltip + HISTORY log accuracy for P-row backup paths.

Session audit (post-v1.19.33) found three user-facing texts that
lie about behavior after the v1.19.32/.33 P-row preservation work:

  1. ACCEPT UPDATE tooltip (app.js:~8519) — "Download the new
     ThemerrDB URL and replace the current theme in this section
     only." On a SRC=P row, v1.19.32 deliberately does NOT replace
     (auto_place=False, no plex_upload). Tooltip teaches the wrong
     mental model.

  2. REVERT tooltip (app.js:~8877) — "Revert to the previously-
     active URL and re-download in this section." On a SRC=P row,
     v1.19.33 routes REVERT through the same backup-only path.
     Same mental-model lie.

  3. BK badge tooltip (app.js:~8035) — "User intent: keep a TDB
     backup file without touching Plex." v1.19.32/.33 added two
     non-user-intent paths to the BK badge (ACCEPT UPDATE on a
     P-row + REVERT on a P-row are upstream-driven). The tooltip
     should enumerate the reach paths so the user knows why BK
     appeared without them having clicked KEEP AS BACKUP.

  4. HISTORY event message for ACCEPT UPDATE (api.py:~9463) and
     REVERT (api.py:~15419) — neither mentions the P-row branch.
     Reading the row's HISTORY tab after a P-row accept, the
     user sees "Update accepted by admin" with no hint that the
     download was scoped to a backup. The PROVENANCE audit row
     records `p_row_backup` (v1.19.33) but HISTORY is what users
     scroll first.

## Fix shape

- ACCEPT UPDATE + REVERT tooltips: prepend a P-row branch.
- BK badge tooltip: rewrite to enumerate the 5 reach paths (bulk
  DOWNLOAD TDB BACKUP / per-row DOWNLOAD TDB BACKUP / SET URL
  with KEEP AS BACKUP / UPLOAD MP3 with KEEP AS BACKUP /
  ACCEPT UPDATE or REVERT on a P-row).
- HISTORY messages: append `(P-row — downloaded as backup;
  Plex keeps serving)` when the v1.19.32/.33 branch fires.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── ACCEPT UPDATE tooltip ────────────────────────────────────


def test_accept_update_tooltip_branches_on_p_row():
    """The acceptTip ternary must branch on srcLetter='P' BEFORE
    the urls_match / upstream_changed split. Pin both the gate
    and the new copy."""
    # The new branch must reference srcLetter === 'P'.
    assert "(srcLetter === 'P')" in APP_JS, (
        "v1.19.34: ACCEPT UPDATE tooltip must branch on srcLetter "
        "to surface the v1.19.32 P-row backup path"
    )
    # The new tooltip body must say "BACKUP" and "Plex keeps serving"
    # so the user understands why no place is happening.
    assert "BACKUP. Plex keeps serving" in APP_JS, (
        "v1.19.34: P-row ACCEPT UPDATE tooltip must explicitly say "
        "Plex keeps serving — the prior copy claimed 'replace the "
        "current theme' which v1.19.32 doesn't do on P-rows"
    )
    # Must reference the resulting backup badge so the user knows
    # what to expect. v1.19.90: ACCEPT UPDATE downloads the ThemerrDB
    # URL → source_kind='themerrdb' + backup_only → TB badge (split
    # out of the old generic BK/UB).
    assert "row stays SRC=P with a TB badge" in APP_JS, (
        "v1.19.34 + v1.19.90: tooltip must name the TB badge — the "
        "post-action signal for a ThemerrDB-sourced P-row backup"
    )


def test_accept_update_tooltip_preserves_legacy_branches():
    """The urls_match copy + the upstream_changed copy must
    remain unchanged for non-P rows."""
    # urls_match copy.
    assert "Convert this row from U to T." in APP_JS, (
        "v1.19.34: must not regress the urls_match tooltip"
    )
    # upstream_changed copy.
    assert ("Download the new ThemerrDB URL and replace the "
            "current theme in this section only.") in APP_JS, (
        "v1.19.34: must not regress the non-P upstream_changed tooltip"
    )


# ── REVERT tooltip ───────────────────────────────────────────


def test_revert_tooltip_branches_on_p_row():
    """The restoreTip ternary (the non-isRestore branch) must
    add a P-row clause."""
    # Locate the restoreTip block in app.js.
    idx = APP_JS.index("const restoreTip = isRestore")
    block = APP_JS[idx:idx + 1200]
    # P-row branch must use srcLetter check + match the v1.19.34
    # copy + reference the BK badge.
    assert "srcLetter === 'P'" in block, (
        "v1.19.34: REVERT tooltip must branch on srcLetter='P' to "
        "surface v1.19.33's backup-only branch"
    )
    assert "Revert to the previously-active URL as a BACKUP" in block, (
        "v1.19.34: P-row REVERT tooltip must say BACKUP so the user "
        "understands Plex keeps serving"
    )


def test_revert_tooltip_preserves_legacy_branches():
    """isRestore (srcLetter='-' or 'M') branch unchanged; legacy
    revert (non-P, non-restore) branch unchanged."""
    # Restore-themerrdb copy.
    assert ("Restore the ThemerrDB URL captured before "
            "PURGE/UNMANAGE") in APP_JS
    # Restore-user copy.
    assert ("Restore the user URL captured before "
            "PURGE/UNMANAGE") in APP_JS
    # Non-P REVERT copy.
    assert ("Revert to the previously-active URL and re-download "
            "in this section.") in APP_JS, (
        "v1.19.34: non-P REVERT tooltip must remain the legacy "
        "string for the U/T/A row case"
    )


# ── BK badge tooltip ─────────────────────────────────────────


def test_bk_badge_tooltip_describes_backup_state():
    """v1.19.34 originally pinned a full enumeration of every
    reach path that lands a row on BK. v1.19.75 retired that
    enumeration — the user explicitly asked to trim the BU/UB
    tooltip to the HL/C/M/PU one-sentence style. The row's
    INFO card is now the right place for full provenance. This
    test now just verifies the tooltip describes the state
    accurately + uses the canonical 'User Backup —' lead-in."""
    idx = APP_JS.index("link-glyph-bk")
    block = APP_JS[idx:idx + 600]
    assert "User Backup —" in block, (
        "v1.19.34/.75: BK/UB tooltip lead-in must be "
        "'User Backup —'"
    )
    has_state = (
        "not placed in the Plex folder" in block
        or "ready but not placed" in block
    )
    assert has_state, (
        "v1.19.34/.75: tooltip must describe the row state "
        "(file ready but not placed)"
    )


def test_bk_badge_tooltip_keeps_push_to_plex_cta():
    """The PUSH TO PLEX call-to-action must survive the v1.19.75
    rewrite — it's the canonical recovery path."""
    idx = APP_JS.index("link-glyph-bk")
    block = APP_JS[idx:idx + 600]
    assert "PUSH TO PLEX" in block, (
        "v1.19.34/.75: BK/UB tooltip must keep the PUSH TO PLEX "
        "CTA so the user knows the promote-to-active path"
    )


# ── HISTORY log message for ACCEPT UPDATE ────────────────────


def test_accept_update_history_log_has_p_row_branch():
    """The log_event message after ACCEPT UPDATE must append a
    P-row branch note when is_p_row_for_section is True."""
    fn_start = API_PY.index("async def api_accept_update(")
    fn_end = API_PY.index("@app.post", fn_start + 1)
    body = API_PY[fn_start:fn_end]
    assert "P-row — downloaded as backup; Plex keeps serving" in body, (
        "v1.19.34: ACCEPT UPDATE HISTORY message must include the "
        "P-row branch note so the timeline correctly explains why "
        "no place job ran"
    )
    # The branch must guard on the v1.19.32 helper variable.
    assert "is_p_row_for_section" in body
    # The legacy "user URL captured for revert" + "user URL matched
    # TDB; no revert" branches must remain reachable for non-P rows.
    assert "user URL captured for revert" in body
    assert "user URL matched TDB; no revert" in body


# ── HISTORY log message for REVERT ───────────────────────────


def test_revert_history_log_has_p_row_branch():
    """REVERT's log_event message must append the P-row branch
    note when revert_is_p_row is True."""
    fn_start = API_PY.index("async def api_revert_to_themerrdb(")
    # REVERT is followed by @app.get (audit endpoint), not @app.post.
    fn_end_candidates = [
        API_PY.find("@app.get", fn_start + 1),
        API_PY.find("@app.post", fn_start + 1),
    ]
    fn_end = min(x for x in fn_end_candidates if x > 0)
    body = API_PY[fn_start:fn_end]
    assert "P-row — downloaded as backup; Plex keeps serving" in body, (
        "v1.19.34: REVERT HISTORY message must include the P-row "
        "branch note"
    )
    assert "revert_is_p_row" in body
    # Legacy message shape preserved.
    assert "REVERT to" in body and "URL by" in body
