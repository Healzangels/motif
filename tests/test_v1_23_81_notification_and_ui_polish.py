"""v1.23.81 — notification + UI coherence polish (4 items).

Verified survivors of a notification + UI sweep (most of the sweep's
"coherence gaps" were deliberate-and-documented and left as-is):

  1. The three "theme lost" tiers had byte-identical subjects
     ("💔 Theme lost — {title}") despite very different urgency —
     backup-ready (deploy now), sidecar-available (still playing, no
     urgency), and no-fallback (broken). The 💔 stays as the family
     marker but each tier now adds a qualifier so a Discord list is
     triageable at a glance.
  2. The sync-summary error line used the informal "error(s)"; now
     proper plural ("1 error" / "2 errors"), matching the rest of the
     codebase. Same fix on the cloud-backup summary sibling.
  3. The help `?` glyph (v1.23.76, replaced the "// HELP" text) had no
     aria-label — a screen reader announced "question mark". Added one.
  4. The per-row failure button read "// ACK" while the bulk bar reads
     "// ACK FAILURES"; aligned the per-row label to "// ACK FAILURE".
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


# ── 1. lost-theme tier titles are distinct (behavioral) ──────

def test_lost_theme_tiers_have_distinct_titles():
    from app.core.notify_content import (
        format_plex_theme_lost_title,
        format_theme_lost_backup_ready_title,
        format_theme_lost_sidecar_available_title,
    )
    ctx = {"display_title": "Watchmen"}
    backup = format_theme_lost_backup_ready_title(ctx)
    sidecar = format_theme_lost_sidecar_available_title(ctx)
    nofallback = format_plex_theme_lost_title(ctx)

    # exact tier copy
    assert backup == "💔 Theme lost — backup ready — Watchmen"
    assert sidecar == "💔 Theme lost — still playing — Watchmen"
    # the no-fallback (broken) tier keeps the bare, unqualified subject
    assert nofallback == "💔 Theme lost — Watchmen"

    # the whole point: a Discord list can tell them apart now
    titles = {backup, sidecar, nofallback}
    assert len(titles) == 3, (
        "all three lost-theme tiers must have distinct titles "
        "(were byte-identical before v1.23.81)"
    )
    # …while all keeping the 💔 Theme lost family marker
    assert all(t.startswith("💔 Theme lost") for t in titles)


# ── 2. proper plural on the error lines (source pins) ────────

def test_sync_summary_error_line_pluralizes():
    src = (REPO / "app" / "core" / "worker.py").read_text()
    assert '"error" if sync_stats.errors == 1 else "errors"' in src, (
        "sync-summary error line must pick error/errors by count"
    )


def test_cloud_backup_summary_error_count_pluralizes():
    src = (REPO / "app" / "web" / "api.py").read_text()
    assert "'error' if len(errors) == 1 else 'errors'" in src, (
        "cloud-backup summary must pluralize its error count too "
        "(sibling of the sync-summary fix)"
    )


# ── 3. help-toggle has an accessible name ────────────────────

def test_help_toggle_has_aria_label():
    html = (REPO / "app" / "web" / "templates" / "base.html").read_text()
    i = html.index('id="help-toggle"')
    btn = html[html.rfind("<button", 0, i):html.index("</button>", i)]
    assert 'aria-label="Toggle help mode"' in btn, (
        "the bare `?` glyph (v1.23.76) needs an aria-label so a "
        "screen reader announces the action, not 'question mark'"
    )
    # the visible glyph + tooltip are unchanged
    assert ">?" in btn and 'title="Toggle help mode' in btn


# ── 4. per-row ACK label aligns with the bulk bar ────────────

def test_per_row_ack_label_is_ack_failure():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # the per-row job-failure button
    i = js.index('data-act="ack-job-failure"')
    btn = js[i:js.index("</button>", i)]
    assert "// ACK FAILURE" in btn, (
        "per-row label aligned to '// ACK FAILURE' (was bare '// ACK', "
        "while the bulk bar reads '// ACK FAILURES')"
    )
    # the bulk bar's plural label is untouched
    assert "// ACK FAILURES" in js


# ── version pin ──────────────────────────────────────────────

def test_v1_23_81_version_pin():
    # v1.23.82: durable prefix (was an exact-version pin that broke on the
    # next bump — the established pattern, and the exact pin in
    # test_v1_13_79, already cover this).
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
