"""v1.23.82 — notification family uniformity pass.

A holistic style/look-and-feel sweep of the whole notification family
(the formatters in notify_content.py + the inline sync titles). The
family already followed the v1.19.92 copy standard; this closes the
remaining outliers:

  1. theme_added and theme_available both used 🎵 — the only emoji
     collision in the set. theme_available → ✨ ("actionable suggestion",
     distinct from 🎵 "a theme started playing"). Every event now owns a
     distinct glyph.
  2. theme_available's CTA was an inline clause; its siblings open the
     action with a bold lead-in (**To deploy:** / **To restore:**). Now
     **To add:**.
  3. theme_lost_sidecar_available's footer omitted the 24h rate-limit
     note its two reaper siblings carry — all three loss tiers are
     rate-limited identically (86400s). Footer now reads uniformly.
  4. The sync pair didn't match: sync_completed "Motif sync — {state}"
     vs sync_failed "❌ Sync failed". sync_failed → "❌ Motif sync —
     failed" (same stem; ❌ kept for alarm).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


# ── 1. theme_available emoji is ✨, distinct from the family ──

def test_theme_available_uses_sparkle():
    from app.core.notify_content import (
        format_theme_available_title,
        format_theme_available_batch_title,
    )
    ctx = {"display_title": "Foo (2020)"}
    assert format_theme_available_title(ctx) == "✨ Theme available — Foo (2020)"
    assert format_theme_available_batch_title(3) == "✨ 3 themes available to add"


def test_per_item_title_emojis_are_all_distinct():
    """The whole point of the v1.17.19 emoji set is at-a-glance
    scannability — so no two per-item events may share a glyph."""
    from app.core import notify_content as nc
    ctx = {"display_title": "Foo (2020)"}
    titles = [
        nc.format_theme_added_title(ctx),
        nc.format_theme_pushed_title(ctx),
        nc.format_theme_backed_up_title(ctx),
        nc.format_theme_available_title(ctx),
        nc.format_theme_deleted_title(ctx, action="deleted"),
        nc.format_backup_ready_title(ctx),
        nc.format_plex_theme_lost_title(ctx),
    ]
    emojis = [t.split(" ", 1)[0] for t in titles]
    assert len(set(emojis)) == len(emojis), (
        f"emoji collision in the title family: {emojis}"
    )
    # ✨ in particular must not be 🎵 (theme_added) nor 🆕 (release_available)
    assert "✨" in emojis and "🎵" in emojis
    assert emojis.count("🎵") == 1  # only theme_added keeps 🎵


# ── 2. theme_available CTA has the bold lead-in ──────────────

def test_theme_available_cta_has_bold_lead_in():
    from app.core.notify_content import format_theme_available_body
    body = format_theme_available_body({"display_title": "Foo"})
    assert "**To add:**" in body, (
        "theme_available CTA must use the bold lead-in like its siblings "
        "(**To deploy:** / **To restore:**)"
    )
    # the action targets are unchanged
    assert "// SOURCE menu" in body and "// DOWNLOAD TDB" in body


# ── 3. sidecar-lost footer carries the 24h rate-limit note ───

def test_sidecar_lost_footer_mentions_rate_limit():
    from app.core.notify_content import (
        format_theme_lost_sidecar_available_body,
        format_plex_theme_lost_body,
    )
    sidecar = format_theme_lost_sidecar_available_body({"display_title": "Foo"})
    nofallback = format_plex_theme_lost_body({"display_title": "Foo"})
    note = "rate-limits this notification to once per 24h per row"
    # all three reaper loss tiers are rate-limited identically — the
    # footer should say so uniformly (sidecar was the lone omission).
    assert note in sidecar, "sidecar-lost footer must carry the rate-limit note"
    assert note in nofallback  # control: the no-fallback tier already does
    # the sidecar body keeps its useful 'common cause' context too
    assert "Common cause" in sidecar


# ── 4. sync_failed unified onto the 'Motif sync — …' stem ────

def test_sync_failed_title_matches_sync_pair():
    assert 'title="❌ Motif sync — failed"' in WORKER_PY, (
        "sync_failed adopts the 'Motif sync — {state}' stem to match "
        "sync_completed; ❌ kept for alarm"
    )
    assert 'title="❌ Sync failed"' not in WORKER_PY  # old form gone


# ── version pin ──────────────────────────────────────────────

def test_v1_23_82_version_pin():
    # durable prefix (matches the established per-tag pin pattern); the
    # exact-version pin lives in test_v1_13_79 + is bumped each tag.
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
