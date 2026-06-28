"""v1.14.52 — audit Bundle B: 3 HIGH bugs across app.js + worker.py + api.py.

From the v1.14.50 holistic audit, the four HIGHs not from my
recent code:
  • H4 (shipped here): LINK pill // ALL button missing 'ps' token
  • H5 (shipped here): _do_refresh silently reports success when
    payload lacks rating_key
  • H7 (NOT shipped — investigated, already fixed since v1.13.54):
    audit + audit-doc both wrong; the gate `if owner_sections and
    owner_sections.issubset(...)` has been there from the original
    v1.13.54 commit. No code change.
  • H8 (shipped here): update_tab_row (UPD pill static fallback)
    SQL was the v1.13.83-and-earlier permissive shape; the
    breakdown query right below has v1.13.84/86/88 strict
    predicates. Pre-cycle clicks on UPD could deep-link to a tab
    where the strict library filter renders 0 matches.

Each finding is independent — three different surfaces (frontend
click handler, worker job handler, server SQL fallback) — but
they bundle cleanly because the audit surfaced them together
and each is small.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── H4: LINK pill ALL set includes 'ps' ──────────────────────


def test_link_pill_all_set_includes_ps_token():
    """The pillAxes definition for linkPill must include 'ps' in
    the values array. Pre-fix the array was 4 tokens (hl, c, m,
    none) — // ALL on the LINK row left the amber ●ps chip
    inactive, leaking LPS rows on the inverse-select pattern."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    # Anchor on the linkPill entry in pillAxes.
    anchor = js.index("attr: 'linkPill', allAttr: 'linkPillAll'")
    # v1.19.50: window widened 500 → 1500. Accreted rationale
    # comments above the `values:` line crossed the 500-char
    # boundary by v1.19.50's 'bk' add.
    block = js[anchor:anchor + 1500]
    # The values array. v1.18.22 extended with 'pu' for the
    # Pushed (plex_upload) chip — // ALL must include it too.
    # v1.19.43: substring-style pin (no trailing `]`) so future
    # LINK-axis widenings (v1.19.43 added 'b') don't break this
    # guard. Mirror-principle is what matters — ps + pu must
    # remain in the values array.
    assert "values: ['hl', 'c', 'm', 'none', 'ps', 'pu'" in block
    # v1.14.52 marker explains the parity rationale.
    block_with_comment = js[anchor - 1500:anchor + 500]
    assert "v1.14.52: 'ps' added to LINK ALL set" in block_with_comment


def test_link_pill_all_set_no_pre_fix_4_token_form():
    """Regression guard: the pre-fix 4-token form must not survive."""
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    js_no_comments = "\n".join(
        line for line in js.splitlines()
        if not line.lstrip().startswith("//")
    )
    # The exact pre-fix array shape (with closing bracket).
    assert "values: ['hl', 'c', 'm', 'none']" not in js_no_comments


# ── H5: _do_refresh raises on missing rating_key ─────────────


def test_do_refresh_raises_permanent_failure_on_missing_rating_key():
    """The _do_refresh handler must raise _JobPermanentFailure
    when the payload doesn't carry a rating_key. Pre-fix the
    `if rk:` gate silently returned, dispatch then called
    _mark_done — recording success without any plex.refresh call."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_refresh(self, job:")
    body = src[fn_anchor:fn_anchor + 2500]
    # The new explicit raise.
    assert 'raise _JobPermanentFailure(\n                    "refresh job missing rating_key in payload")' in body
    # Marker rationale.
    assert "v1.14.52:" in body


def test_do_refresh_raises_permanent_failure_on_unparseable_payload():
    """The payload-parse exception must also raise
    _JobPermanentFailure (a malformed payload won't fix itself
    on retry). Pre-fix the bare `pass` silently treated parse
    failures as success."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_refresh(self, job:")
    body = src[fn_anchor:fn_anchor + 2500]
    # The exception caught + re-raised as _JobPermanentFailure.
    assert "except (json.JSONDecodeError, TypeError) as e:" in body
    assert "raise _JobPermanentFailure(" in body
    assert "refresh job payload unparseable" in body


def test_do_refresh_no_longer_has_silent_pass():
    """Regression guard: the bare `except (json.JSONDecodeError,
    TypeError): pass` must not survive — it was the silent-failure
    shape this fix exists to close."""
    src = (REPO / "app" / "core" / "worker.py").read_text()
    fn_anchor = src.index("def _do_refresh(self, job:")
    body = src[fn_anchor:fn_anchor + 2500]
    # The bare pass on the parse exception is gone.
    assert "except (json.JSONDecodeError, TypeError):\n            pass" not in body


# ── H8: update_tab_row uses strict library-filter predicates ─


def test_update_tab_row_includes_src_letter_exclusion():
    """The static-fallback `update_tab_row` lookup must apply the
    same SRC != '-' exclusion the breakdown applies. Pre-fix a
    row with src='-' could win the LIMIT 1 race and hand the
    static href a tab where the library filter excludes the row
    → 0 matches on click."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    # Find the update_tab_row block specifically (not the breakdown
    # — the breakdown's body is right below).
    anchor = src.index("update_tab_row = conn.execute(")
    end = src.index("update_tab_breakdown_rows = conn.execute(", anchor)
    block = src[anchor:end]
    # SRC letter macro reference.
    assert "_SRC_LETTER_SQL" in block
    assert ") != '-'" in block


def test_update_tab_row_includes_has_something_predicate():
    """Mirror of v1.13.86 library filter has-something check
    (excludes pure-P rows where ACCEPT is a no-op)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("update_tab_row = conn.execute(")
    end = src.index("update_tab_breakdown_rows = conn.execute(", anchor)
    block = src[anchor:end]
    # The 4-way OR: local_files OR user_overrides OR placement OR
    # local_theme_file.
    assert "EXISTS (SELECT 1 FROM local_files lf2" in block
    assert "EXISTS (SELECT 1 FROM user_overrides uo2" in block
    # v1.22.83: the bare p alias became the edition-aware two-tier pair.
    assert ("COALESCE(p_e.media_folder, p_g.media_folder) IS NOT NULL"
            in block)
    assert "pi.local_theme_file = 1" in block


def test_update_tab_row_includes_url_diff_predicate():
    """Mirror of v1.13.86 library filter URL-diff check (excludes
    legacy urls_match leftovers where new_url already matches the
    currently-applied URL)."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("update_tab_row = conn.execute(")
    end = src.index("update_tab_breakdown_rows = conn.execute(", anchor)
    block = src[anchor:end]
    # v1.22.10: the urls_match short-circuit + per-section/global override
    # COALESCE chain now live in the shared _pending_update_actionable_sql
    # helper (which this row SQL invokes) — consolidated from the inline form.
    assert "_pending_update_actionable_sql" in block


def test_update_tab_row_drops_pre_fix_pending_updates_root():
    """Regression guard: the pre-fix shape was rooted on
    `FROM pending_updates pu` — replaced with the plex_items-
    rooted shape that mirrors the breakdown."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("update_tab_row = conn.execute(")
    end = src.index("update_tab_breakdown_rows = conn.execute(", anchor)
    block = src[anchor:end]
    # The new shape roots on themes (so the SRC macro can resolve).
    assert "FROM themes t" in block
    # Pre-fix shape gone.
    assert "FROM pending_updates pu\n                JOIN plex_items pi" not in block


def test_update_tab_row_uses_consistent_order_by_with_breakdown():
    """ORDER BY must match the breakdown so the LIMIT 1 winner is
    the same tab the breakdown ranks first."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    anchor = src.index("update_tab_row = conn.execute(")
    end = src.index("update_tab_breakdown_rows = conn.execute(", anchor)
    block = src[anchor:end]
    assert "ORDER BY ps.is_anime, ps.type, ps.is_4k" in block
    assert "LIMIT 1" in block
