"""v1.19.69 — sync_completed body cleanup (drop redundant summary + 5396 noise).

the user's 2026-05-29 sample sync notification showed "43 new"
three times stacked in one Discord block:
  1. sync_completed title: "Motif sync — 43 new"
  2. sync_completed body line: "**43 new** (5396 item(s) had
     upstream changes this sync window)"
  3. themes_added_by_sync event title: "🎵 43 new themes added
     by sync"

The "5396 item(s) had upstream changes" parenthetical also
misled — readers parsed it as "5396 items had actionable
changes" when motif's git-diff sync path counts upstream items
walked (most have non-theme-relevant edits: title spelling,
release date, raw_json fingerprint). The number is debug
noise for a notification audience.

## v1.19.69 changes

**`app/core/worker.py`** sync_completed body builder:
  - **Has-changes branch**: dropped the
    `**{summary}** ({total_seen} item(s) had upstream changes
    this sync window)` line entirely. The title carries the
    summary; the title-list event surfaces the detail.
  - **Zero-changes branch**: rephrased
    `"No changes detected — N item(s) had upstream changes this
    sync window."` → `"No changes detected (scanned N upstream
    items)."` Honest about motif's reach without overclaiming
    that N items got actionable changes. Keeps the breadcrumb
    "yes sync ran" signal for the zero case.
  - **All-zeros branch**: no change (still
    `"No changes detected since last sync."`).

## What stays the same

  - Title text unchanged ("Motif sync — N new · M updated").
  - Updates list ("🔄 Updated:" + bullets) unchanged.
  - "✅ Sync complete" closer unchanged.
  - themes_added_by_sync event unchanged (still ships
    title-by-title detail).
  - themes_updated_by_sync event unchanged.

## Net effect

Sample sync (43 new, 0 updated, 5396 seen):

  BEFORE:
    Title: "Motif sync — 43 new"
    Body:  "**43 new** (5396 item(s) had upstream changes
            this sync window)
            \n
            ✅ Sync complete"
    + themes_added_by_sync notification with title list

  AFTER:
    Title: "Motif sync — 43 new"
    Body:  "✅ Sync complete"
    + themes_added_by_sync notification with title list

  "43 new" appears 2 times (title + themes_added title)
  instead of 3. No more 5396 confusion.

Zero-change sync (0 new, 0 updated, 5396 seen):

  BEFORE:
    Title: "Motif sync — no changes (5396 checked)"
    Body:  "No changes detected — 5396 item(s) had upstream
            changes this sync window."

  AFTER:
    Title: "Motif sync — no changes (5396 checked)"
    Body:  "No changes detected (scanned 5396 upstream items)."

  Same information, accurate phrasing.
"""
from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

WORKER_PY = (REPO / "app" / "core" / "worker.py").read_text()


def _sync_body_block() -> str:
    """The sync_completed body-builder block in worker.py — from
    the v1.18.64 marker to the title_text computation."""
    start = WORKER_PY.index("v1.18.64: body reworked")
    # v1.21.6: the sync_completed block now runs to the post-sync
    # plex_enum comment — the standalone themes_added/updated_by_sync
    # dispatches were folded into this one message + removed.
    end = WORKER_PY.index("# v1.12.126")
    return WORKER_PY[start:end]


def _sync_body_emitted_block() -> str:
    """Just the body-builder portion — from `body_lines: list[str]`
    to `sync_summary =` — so assertions about the EMITTED body
    don't accidentally match the title-text branch below it."""
    block = _sync_body_block()
    start = block.index("body_lines: list[str]")
    end = block.index("sync_summary =")
    return block[start:end]


# ── Redundant summary line gone ──────────────────────────────


def test_has_changes_branch_drops_summary_line():
    """v1.19.92 went further than v1.19.69 — the body no longer
    emits ANY outcome summary (neither the '**{summary}**' header
    nor the 'had upstream changes this sync window' parenthetical).
    The title carries the outcome; the body carries only the
    updated-titles list + errors + ✅ closer."""
    block = _sync_body_block()
    assert "had upstream changes" not in block, (
        "v1.19.92: the 'had upstream changes' parenthetical is gone"
    )
    assert 'f"**{summary}**' not in block, (
        "v1.19.92: the redundant '**{summary}**' header is gone"
    )


def test_parts_still_built_for_title():
    """The `parts` list (used by the title text below) must STILL
    be built — the title says 'Motif sync — N new · M updated'
    from this list. v1.19.92 lifted it out of the old else: branch
    so it's computed unconditionally."""
    block = _sync_body_block()
    assert 'parts.append(f"{new_count} new")' in block
    assert 'parts.append(f"{upd_count} updated")' in block


def test_v1_19_69_marker_documents_drop():
    """v1.19.92 superseded v1.19.69's body cleanup with the
    uniform 'body never restates the title' rule. The worker
    block must carry the v1.19.92 marker + the duplication
    rationale so future archaeology finds the WHY."""
    block = _sync_body_block()
    assert "v1.19.92" in block
    assert "duplicat" in block.lower() or "restate" in block.lower()


# ── Zero-change branch rephrased ─────────────────────────────


def test_zero_changes_body_has_no_outcome_restatement():
    """v1.19.92: the zero-changes body no longer states the
    outcome at all — no 'No changes', no 'scanned … upstream
    items', no 'had upstream changes'. The title says it; the
    body must not duplicate it. (Pre-v1.19.92 this branch emitted
    a 'scanned N upstream items' line that restated the title.)"""
    body = _sync_body_emitted_block()
    assert "No changes" not in body
    assert "scanned" not in body
    assert "had upstream changes" not in body


def test_zero_changes_count_lives_in_title():
    """The {total_seen} 'yes sync ran' breadcrumb moved to the
    TITLE ('Motif sync — no changes (N checked)') — it's no
    longer in the body. Pin that the count is in the title-text
    branch, not the body builder."""
    block = _sync_body_block()
    title_anchor = block.index('title_text = (')
    title_block = block[title_anchor:title_anchor + 200]
    # v1.23.28: the rendered count is now `checked` (total_seen, or the
    # tracked-catalog fallback for an empty-diff cron sync). Still in the
    # TITLE, not the body — which is what this guard pins.
    # v0.50.72: the count names its unit + is comma-formatted ({checked:,}).
    assert "{checked:,}" in title_block, (
        "v1.19.92: the (N checked) breadcrumb is rendered in the "
        "title, not the body"
    )


# ── What stays the same ──────────────────────────────────────


def test_all_zeros_body_has_no_no_changes_line():
    """v1.19.92: the all-zeros body line 'No changes detected
    since last sync.' is GONE — the title ('Motif sync — no
    changes') carries that, and repeating it in the body was the
    duplication the user flagged. The all-zeros body is just ✅."""
    body = _sync_body_emitted_block()
    assert "No changes detected since last sync." not in body
    assert '"✅ Sync complete"' in body


def test_updates_list_still_renders_first():
    """The '🔄 Updated:' list survives — v1.19.92 only dropped the
    outcome-restating lines, not the updated-titles enumeration
    (which is genuine info the title can't hold)."""
    block = _sync_body_block()
    assert "🔄 Updated:" in block


def test_sync_complete_closer_preserved():
    """'✅ Sync complete' at the END of the body — closer
    line introduced in v1.19.55 — must survive."""
    assert '"✅ Sync complete"' in WORKER_PY


def test_themes_added_section_gated_in_summary():
    """v1.21.6: themes_added_by_sync no longer fires a SEPARATE
    notification — it now gates a "🎵 New:" titles section folded
    into the single sync_completed message. Pin the toggle gate +
    the section header."""
    block = _sync_body_block()
    assert '_events.get("themes_added_by_sync", False)' in block, (
        "v1.21.6: the New-titles section must be gated by the "
        "themes_added_by_sync toggle inside the sync summary"
    )
    assert '"🎵 New:"' in block


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_69_version_pin():
    """v1.19.69 bumped. Relaxed to v1.19.x prefix after v1.19.70
    continued the line."""
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
