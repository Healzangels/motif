"""v1.18.64 — sync_completed notification clarity.

the user's feedback on v1.18.63 sync notifications:

  Top:    "✅ Sync complete / 1 items processed, 0 new, 1 updated"
  Middle: "🎵 2 new themes added by sync / • The Book of Boba Fett
           (2021) / • Star Wars: Maul - Shadow Lord (2026) /
           ✅ Sync complete / 5195 items processed, 2 new, 0 updated"
  Bottom: "✅ Sync complete / 0 items processed, 0 new, 0 updated"

Concerns (verbatim):
  - "could we improve our sync notifications, as its a bit confusing
     the only 1 processed 1 updated and if possible to list what was
     updated that would be helpful too"
  - "the 2 new themes added by sync is great since it lists what was
     added the items processed still feels a bit confusing though as
     you'd like it would process all of the entries everytime but
     then show how many new or updated there were"
  - "same with the notification below processed 0, new 0, updated 0
     makes it seem like the sync didn't actually do or check anything,
     which might be true but that should be clearer"

## Scope

1. SyncStats gains updated_titles: list[(title, year)] capturing
   the titles of rows whose upstream URL changed this run (mirrors
   the themes_added_by_sync rendering of new titles).
2. sync.py _process_batch_for_db appends to stats.updated_titles
   whenever url_changed (cap 50 stored).
3. worker.py sync_completed body reworked:
   - all-zeros case → "No changes detected since last sync."
   - non-zero new/updated case → "**N new · M updated**" header
     + bulleted Updated: list (mirrors themes_added_by_sync) +
     "Checked X upstream item(s) in this sync window." footer.
   - body_format='markdown' so Discord/Slack render bullets.
"""
from __future__ import annotations

from dataclasses import fields
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── SyncStats schema ────────────────────────────────────────


def test_sync_stats_has_updated_titles_field():
    """SyncStats must carry an updated_titles list so the worker
    can render it into the Apprise body. Pin so a refactor that
    drops the field has to confront the v1.18.64 contract."""
    from app.core.sync import SyncStats
    s = SyncStats()
    assert hasattr(s, "updated_titles"), (
        "v1.18.64: SyncStats.updated_titles required"
    )
    assert isinstance(s.updated_titles, list)
    assert s.updated_titles == []
    # Each instance gets its own list (default_factory, not
    # the dataclass-mutable-default footgun).
    s2 = SyncStats()
    s.updated_titles.append(("X", "2024"))
    assert s2.updated_titles == [], (
        "shared mutable default — use field(default_factory=list)"
    )


def test_sync_stats_updated_titles_field_uses_default_factory():
    """Pin that the field uses default_factory rather than a
    shared `= []` default."""
    from app.core.sync import SyncStats
    fld = next(f for f in fields(SyncStats) if f.name == "updated_titles")
    assert fld.default_factory is list, (
        "v1.18.64: updated_titles must use default_factory=list"
    )


# ── sync.py captures titles on url_changed ──────────────────


def test_sync_appends_title_year_on_url_changed():
    """The batch processor must append (title, year) to
    stats.updated_titles every time it ticks stats.updated_count.
    Pin the literal in sync.py — the capture lives next to the
    `stats.updated_count += 1` line per v1.18.64."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    # The capture block immediately follows the updated_count
    # increment so the two stay in lockstep.
    idx = src.index("stats.updated_count += 1")
    window = src[idx:idx + 1200]
    assert "stats.updated_titles" in window, (
        "v1.18.64: updated_titles capture must sit beside the "
        "stats.updated_count increment in _process_batch_for_db"
    )
    assert "v1.18.64" in window, (
        "v1.18.64: marker required so a future refactor knows WHY"
    )
    # Memory cap so big initial syncs don't balloon.
    assert "len(stats.updated_titles) < 50" in window, (
        "v1.18.64: cap stored titles to 50 to bound memory"
    )


def test_sync_capture_uses_movie_and_tv_field_aliases():
    """The capture must accept either title/release_date (movies)
    or name/first_air_date (TV) shapes — same coalesce pattern
    _upsert_theme uses on line 268-271."""
    src = (REPO / "app" / "core" / "sync.py").read_text()
    idx = src.index("stats.updated_count += 1")
    window = src[idx:idx + 1200]
    assert 'record.get("title")' in window
    assert 'record.get("name")' in window
    assert 'record.get("release_date")' in window
    assert 'record.get("first_air_date")' in window


# ── worker.py renders the new body ──────────────────────────


def test_worker_sync_completed_body_drops_items_processed_phrase():
    """The old confusing 'N items processed' phrase must be gone
    from the sync_completed dispatch body. the user flagged it as
    misleading in the git-differential case where N = diff size."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    # Find the sync_completed dispatch block.
    idx = src.index('event_kind="sync_completed"')
    # Scope to the EMITTED body builder (excludes the v1.18.64 archaeology
    # comment above it, which quotes the old "items processed" phrasing).
    block_start = src.rindex("body_lines: list[str]", 0, idx)
    block_end = src.index("except Exception", idx)
    block = src[block_start:block_end]
    assert "items processed" not in block, (
        "v1.18.64: 'items processed' phrasing was ambiguous "
        "(git-differential diff size, not full library walk). "
        "the user's repro: '1 items processed, 0 new, 1 updated' "
        "with no hint which title changed."
    )


def test_worker_sync_completed_body_lists_updated_titles():
    """The new body must include the bullet-list rendering of
    stats.updated_titles when any rows were updated — mirroring
    themes_added_by_sync's `* Title (Year)` shape.
    v1.19.55: 'Updated:' section header gained the 🔄 prefix
    for parity with the new themes_updated_by_sync event icon."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    block_start = src.index("v1.18.64: body reworked")
    block_end = src.index("except Exception", idx)
    block = src[block_start:block_end]
    assert "sync_stats.updated_titles" in block, (
        "v1.18.64: body must reference updated_titles"
    )
    # Bulleted list shape (matches themes_added_by_sync).
    # v1.19.55: section header is now "🔄 Updated:" for visual
    # parity with the new themes_updated_by_sync event icon.
    assert '"🔄 Updated:"' in block, (
        "v1.19.55: '🔄 Updated:' section header expected"
    )
    assert '"* "' not in block, (
        "use f-strings, not bare '* '"
    )
    # Sample cap at 10 so the message stays readable.
    assert "updated_titles[:10]" in block, (
        "v1.18.64: sample at most 10 titles for display"
    )
    # v1.22.45: the bullet rendering moved into the section-grouping
    # formatter (notify_content.format_section_grouped_lines) so New/Updated
    # render under Plex-section sub-headers. The worker block now builds the
    # section buckets + calls the formatter instead of inlining bullets.
    assert "_build_sync_section_buckets" in block, (
        "v1.22.45: Updated list must be grouped by Plex section"
    )
    assert "format_section_grouped_lines(" in block


def test_worker_sync_completed_body_zero_zero_zero_case_is_explicit():
    """v1.19.92: the 0/0/0 outcome is stated in the TITLE
    ('Motif sync — no changes'); the body no longer carries a
    'No changes detected …' line (it restated the title — the
    duplication the user flagged 2026-05-29). Pin that the
    duplicated body line is gone."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    # Scope to the EMITTED body builder (excludes the v1.19.92
    # archaeology comment above it, which quotes the old line).
    block_start = src.rindex("body_lines: list[str]", 0, idx)
    block_end = src.index("sync_summary =", block_start)
    block = src[block_start:block_end]
    assert "No changes detected" not in block, (
        "v1.19.92: the body must not restate the title's outcome — "
        "'No changes detected …' moved entirely to the title"
    )


def test_worker_sync_completed_body_uses_checked_phrasing():
    """v1.19.92 superseded the v1.19.55 'had upstream changes'
    phrasing — the body no longer states the outcome at all (the
    title carries '(N checked)'). Pin that both the misleading
    'had upstream changes' wording AND the 'scanned … upstream
    items' rephrase are gone from the body builder."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    # Scope to the EMITTED body builder (excludes the v1.19.92
    # archaeology comment above it, which quotes the old phrasings).
    block_start = src.rindex("body_lines: list[str]", 0, idx)
    block_end = src.index("sync_summary =", block_start)
    block = src[block_start:block_end]
    assert "had upstream changes" not in block
    assert "scanned" not in block


def test_worker_sync_completed_body_format_is_markdown():
    """The dispatch must pass body_format='markdown' so bullets
    render on Discord/Slack/Telegram — same treatment as
    themes_added_by_sync per v1.17.12 / v1.17.14."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    # The dispatch call spans multiple lines — walk forward to
    # the body= argument and the matching close-paren by finding
    # the next `except Exception` (the post-dispatch boundary).
    block_end = src.index("except Exception", idx)
    block = src[idx:block_end]
    assert 'body_format="markdown"' in block, (
        "v1.18.64: sync_completed must dispatch as markdown so "
        "bullets render correctly on Discord/Slack."
    )


def test_no_change_count_is_catalog_not_total_seen():
    """v1.23.79 superseded the v1.18.64 total_seen count (movies + tv +
    collections_seen) for the no-change sync title. The displayed "(N checked)"
    is now the ThemerrDB catalog size (COUNT(*) FROM themes) — stable run-to-run
    and, by design, themes-only (collections aren't ThemerrDB themes). The
    diff-size total_seen computation is gone from the sync_completed block."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    block_start = src.index("v1.18.64: body reworked")
    block_end = src.index('event_kind="sync_completed"', block_start)
    block = src[block_start:block_end]
    assert "total_seen = (" not in block, (
        "v1.23.79 removed the total_seen diff-size count from the no-change "
        "title — it jittered run-to-run (the user's 6193-vs-5618 confusion)"
    )
    assert "SELECT COUNT(*) FROM themes" in block, (
        "the no-change count is now the catalog size"
    )


def test_worker_sync_completed_body_uses_emoji_warning_on_errors():
    """If sync_stats.errors > 0, append a '⚠ N errors during
    sync.' line so the operator sees the partial-failure signal
    without it getting buried. v1.23.81: proper plural (was the
    informal 'error(s)')."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    block_start = src.index("v1.18.64: body reworked")
    block_end = src.index("except Exception", idx)
    block = src[block_start:block_end]
    assert "if sync_stats.errors:" in block
    assert '"error" if sync_stats.errors == 1 else "errors"' in block
    assert "during sync." in block


def test_worker_sync_completed_uses_dot_separator_between_counts():
    """The new header line is '**N new · M updated**' (middot
    separator) — pin so the format doesn't drift back to a
    comma."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    idx = src.index('event_kind="sync_completed"')
    block_start = src.index("v1.18.64: body reworked")
    block_end = src.index("except Exception", idx)
    block = src[block_start:block_end]
    assert "' · '.join(parts)" in block, (
        "v1.18.64: counts header uses middot separator"
    )


# ── End-to-end body render (string-level integration) ───────


def _render_sync_body(stats):
    """Replicate the worker.py v1.19.92 body logic in-process so we
    can assert the rendered output for fixture inputs. Keeps the
    test from booting the full worker harness.

    v1.19.92: the body no longer states the sync outcome — the
    title does ("no changes (N checked)" / "N new · M updated").
    The body lists the actual UPDATED titles (info the title can't
    hold) + any error count, then closes with ✅. A no-change sync
    is just the ✅ closer."""
    new_count = stats.new_count
    upd_count = stats.updated_count
    body_lines = []
    has_changes = new_count != 0 or upd_count != 0
    if has_changes and stats.updated_titles:
        body_lines.append("🔄 Updated:")
        sample = stats.updated_titles[:10]
        for _t, _y in sample:
            if _y:
                body_lines.append(f"* {_t} ({_y})")
            else:
                body_lines.append(f"* {_t}")
        extra = max(0, upd_count - len(sample))
        if extra > 0:
            body_lines.append(f"…and {extra} more")
    if stats.errors:
        _err_word = "error" if stats.errors == 1 else "errors"
        body_lines.append(
            f"⚠ {stats.errors} {_err_word} during sync.")
    if body_lines:
        body_lines.append("")
    body_lines.append("✅ Sync complete")
    return "\n".join(body_lines)


def test_rendered_body_all_zero_case():
    """v1.19.92: 0/0/0 → body is just the ✅ closer. The 'no
    changes' outcome lives in the TITLE now, not the body (it was
    the duplication the user flagged in his 2026-05-29 screenshot)."""
    from app.core.sync import SyncStats
    body = _render_sync_body(SyncStats())
    assert body == "✅ Sync complete"


def test_rendered_body_walked_but_no_changes():
    """v1.19.92: items seen but none new/updated → body is still
    just the ✅ closer. The '(N checked)' count is in the title,
    not duplicated in the body."""
    from app.core.sync import SyncStats
    s = SyncStats(movies_seen=100, tv_seen=50)
    body = _render_sync_body(s)
    assert body == "✅ Sync complete"


def test_rendered_body_with_updates_and_titles():
    """The the user-flagged case: 1 updated. Body names the title
    under the '🔄 Updated:' header + closes with ✅. v1.19.92
    dropped the '**1 updated**' header (it duplicated the title)."""
    from app.core.sync import SyncStats
    s = SyncStats(
        movies_seen=1, updated_count=1,
        updated_titles=[("13 Assassins", "2010")],
    )
    body = _render_sync_body(s)
    assert "**1 updated**" not in body
    assert body.startswith("🔄 Updated:")
    assert "* 13 Assassins (2010)" in body
    assert body.rstrip().endswith("✅ Sync complete")


def test_rendered_body_with_news_and_updates():
    """Mixed new + updated. v1.19.92: no '**2 new · 1 updated**'
    header (the title carries the counts); the body just lists the
    updated subset under the 🔄 header."""
    from app.core.sync import SyncStats
    s = SyncStats(
        movies_seen=2, tv_seen=3, new_count=2, updated_count=1,
        updated_titles=[("Foo Bar", "2024")],
    )
    body = _render_sync_body(s)
    assert "**2 new · 1 updated**" not in body
    assert "🔄 Updated:" in body
    assert "* Foo Bar (2024)" in body


def test_rendered_body_overflows_at_10_titles():
    """When updated_count > 10, sample first 10 + render an
    '…and N more' footer to keep the message bounded."""
    from app.core.sync import SyncStats
    titles = [(f"Title {i}", "2024") for i in range(15)]
    s = SyncStats(
        movies_seen=15, updated_count=15, updated_titles=titles,
    )
    body = _render_sync_body(s)
    # First 10 rendered.
    for i in range(10):
        assert f"* Title {i} (2024)" in body
    # Title 10..14 NOT rendered.
    assert "* Title 10 (2024)" not in body
    # Overflow footer.
    assert "…and 5 more" in body


def test_rendered_body_handles_missing_year():
    """If a record has no release_date/first_air_date, the
    capture stores year=None — body must still render the
    title without the parenthetical year suffix."""
    from app.core.sync import SyncStats
    s = SyncStats(
        movies_seen=1, updated_count=1,
        updated_titles=[("Some Untyped Theme", None)],
    )
    body = _render_sync_body(s)
    assert "* Some Untyped Theme\n" in body or body.endswith(
        "* Some Untyped Theme")
    assert "* Some Untyped Theme (None)" not in body
    assert "* Some Untyped Theme ()" not in body


def test_rendered_body_appends_error_line_when_errors_nonzero():
    """⚠ N errors during sync. footer surfaces partial-fail
    signal without burying it. v1.23.81: proper plural (errors=2
    → 'errors')."""
    from app.core.sync import SyncStats
    s = SyncStats(movies_seen=5, new_count=1, errors=2)
    body = _render_sync_body(s)
    assert "⚠ 2 errors during sync." in body


def test_rendered_body_omits_total_seen_count():
    """v1.19.92: total_seen (incl. collections) feeds the TITLE
    only — the body must not restate the count. A collections-only
    no-change sync renders just the ✅ closer. (Pre-fix this pinned
    'checked 42 upstream item(s)' in the body — exactly the
    title/body duplication v1.19.92 removed.)"""
    from app.core.sync import SyncStats
    s = SyncStats(collections_seen=42)
    body = _render_sync_body(s)
    assert "42" not in body
    assert body == "✅ Sync complete"


# ── Version pin ─────────────────────────────────────────────


# v1.18.65: the canonical version pin lives in
# tests/test_v1_13_79_link_fixes.py::test_version_string_matches_current_release
# and gets bumped with each tag. The previous tag-local pin here
# blocked v1.18.65's ship because it asserted "1.18.64" against
# the bumped init. Drop the tag-local pin so each tag-ship only
# touches the canonical pin (single source of truth).
