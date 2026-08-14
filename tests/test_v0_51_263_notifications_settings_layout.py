"""v0.51.263 — NOTIFICATIONS settings relayout: grouped, one-line hints,
compact inbox grid.

The operator's complaint: "one long list … plus there is a lot of text."
Measured before the change: 34 hint paragraphs, 1,319 words (avg 38, max 132)
across ~30 stacked checkboxes in one column. After: 22 hints, 440 words
(max 45), the 20 EVENTS toggles grouped under five `.form-subhead` family
headers, the two sync fold-in toggles nested with `.form-checkbox-sub`
(v1.21.19 — they depend on SYNC COMPLETED, so the rail SHOWS the dependency
instead of stating it), and the IN-APP INBOX rendered as a hint-less
two-column grid (its ten kinds are named identically to their EVENTS twins,
which v1.23.83 pins, so per-kind hints were pure duplication).

Everything load-bearing survived: all data-cfg-field attrs, label parity,
and the pinned hint phrases — the 237 pre-existing tests over this panel
pass unmodified.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()

_start = HTML.index('data-panel="notifications"')
PANEL = HTML[_start:HTML.index("</section>", _start)]


# ── grouping structure ───────────────────────────────────────


def test_events_are_grouped_under_family_subheads():
    for head in ("// SYNC &amp; BULK", "// THEME LIFECYCLE",
                 "// AVAILABLE &amp; ARRIVED", "// LOSS &amp; RECOVERY",
                 "// SYSTEM HEALTH"):
        assert f'<p class="form-subhead">{head}</p>' in PANEL, (
            f"missing family subhead {head!r} — the flat-list wall is back")


def test_sync_fold_in_toggles_are_nested_sub_toggles():
    """The two list toggles fold INTO the SYNC COMPLETED message (v1.21.6).
    That dependency now renders as .form-checkbox-sub nesting instead of a
    'Needs SYNC COMPLETED on' sentence in each hint."""
    for kind in ("themes_added_by_sync", "themes_updated_by_sync"):
        i = PANEL.index(f'data-cfg-field="notifications.events.{kind}"')
        label_open = PANEL.rindex("<label", 0, i)
        assert "form-checkbox-sub" in PANEL[label_open:i], (
            f"{kind} must nest under SYNC COMPLETED via .form-checkbox-sub")
    # and the parent itself must NOT be nested
    i = PANEL.index('data-cfg-field="notifications.events.sync_completed"')
    label_open = PANEL.rindex("<label", 0, i)
    assert "form-checkbox-sub" not in PANEL[label_open:i]


def test_group_ordering_sync_first_system_last():
    """Scanning order mirrors operator priority: the aggregate sync family
    first, ambient system health last."""
    order = [PANEL.index(h) for h in (
        "// SYNC &amp; BULK", "// THEME LIFECYCLE", "// AVAILABLE &amp; ARRIVED",
        "// LOSS &amp; RECOVERY", "// SYSTEM HEALTH")]
    assert order == sorted(order)


# ── the prose budget (the actual complaint) ──────────────────


def test_hint_word_budget():
    """Pre-relayout the panel carried 1,319 words of hint prose, the longest
    a 132-word paragraph. Budget: no hint over 50 words, total under 600.
    The long-form WHY lives in PROJECT_HISTORY / tag comments — a settings
    page is a control surface, not documentation. (Floor guard too: pinned
    phrases mean hints can't collapse to nothing.)"""
    hints = re.findall(r'<p class="form-hint">(.*?)</p>', PANEL, re.S)
    words = [len(re.sub(r"<[^>]+>", "", h).split()) for h in hints]
    total = sum(words)
    assert max(words) <= 50, (
        f"a hint regrew to {max(words)} words — trim it or move the WHY to "
        f"PROJECT_HISTORY")
    assert total <= 600, f"panel hint prose regrew to {total} words (budget 600)"
    assert total >= 250, (
        f"only {total} hint words — the pinned phrases (reason-branch titles, "
        f"default rationales) should keep this above 250; did hints get "
        f"stripped wholesale?")


# ── the compact inbox grid ───────────────────────────────────


def test_inbox_grid_is_two_column_and_hintless():
    i = PANEL.index("// IN-APP INBOX")
    inbox = PANEL[i:]
    grid_i = inbox.index("form-grid-cols2")
    grid = inbox[grid_i:inbox.index("</div>", grid_i)]
    assert 'class="form-hint"' not in grid, (
        "inbox toggles carry no per-kind hints — each kind is described by "
        "its EVENTS twin above (v1.23.83 pins the names identical)")
    assert grid.count("form-checkbox") == 10, (
        "all ten INBOX_EVENT_KINDS render inside the compact grid")


def test_form_grid_cols2_css_exists_and_collapses_on_mobile():
    assert ".form-grid.form-grid-cols2" in CSS
    i = CSS.index(".form-grid.form-grid-cols2")
    around = CSS[i - 600:i + 600]
    assert "repeat(2, minmax(0, 1fr))" in around
    # the mobile collapse — same breakpoint family as .field-row (760px).
    # Slice to the media block's closing brace, not a byte count (the v0.51.261
    # ratchet — which correctly flagged this line's first draft).
    m = CSS.index("@media (max-width: 760px)", i - 600)
    assert "form-grid-cols2" in CSS[m:CSS.index("}", CSS.index("}", m) + 1)]


# ── nothing load-bearing was lost ────────────────────────────


def test_every_event_and_inbox_toggle_survived_the_relayout():
    import sys
    sys.path.insert(0, str(REPO))
    from app.core.config_file import _DEFAULT_NOTIFY_EVENTS, _DEFAULT_INBOX_EVENTS
    for k in _DEFAULT_NOTIFY_EVENTS:
        assert f'data-cfg-field="notifications.events.{k}"' in PANEL, k
    for k in _DEFAULT_INBOX_EVENTS:
        assert f'data-cfg-field="notifications.inbox_events.{k}"' in PANEL, k


def test_v0_51_263_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
