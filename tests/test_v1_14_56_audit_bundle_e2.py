"""v1.14.56 — audit Bundle E2: dead-code sweep + 2 schema/security nits.

From the v1.14.50 holistic audit. Pure cleanup pass — deletes
known dead code + tightens 2 small safety / hygiene gaps.

  • L1: deleted `app/web/templates/pending.html` (no route renders
    it; v1.12.41 removed /pending).
  • L2: deleted 18 unused CSS class declarations:
    `.dot-amber`, `.dot-red`, `.topbar-status-clickable:hover .dot-red`,
    `.chip-group`, `.chip-warn`, `.chip-warn.chip-active`,
    `.chip-sep`, `.chips-secondary`, `.chips-secondary .chips-label`,
    `.chip-tiny`, `.fail-glyph`, `.fail-glyph-bad`, `.fail-glyph-warn`,
    `.fail-glyph-info`, `.coverage-bar-seg-available`,
    `.coverage-bar-seg-no-tdb`, `.coverage-row-legend`,
    `.coverage-legend-swatch-themed`, `.coverage-legend-swatch-available`,
    `.coverage-legend-swatch-no-tdb`, `.info-scope-chip-fourk`,
    `.info-scope-chip-standard`. All zero JS / template refs.
    Same shape as the v1.14.50 / v1.14.48 cleanup. The
    `@keyframes fail-pulse` they referenced stayed (5 OTHER
    rules still consume it).
  • L8: `api_events` now requires admin auth. Pre-fix the endpoint
    was unauthenticated; raw events table was readable by any
    forward-auth-disabled session. AUDIT_API.md L5.
  • L13: v44 migration backfill rewrites `datetime('now')` →
    `strftime('%Y-%m-%dT%H:%M:%S+00:00','now')` so the row's
    timestamp matches the runtime now_iso() format.
  • L14: dormant `probe` job_type asymmetry (allowed in v14-
    migrated DBs, rejected in fresh installs) documented inline
    so the next contributor isn't surprised.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = REPO / "app" / "web" / "static" / "app.css"
TEMPLATES = REPO / "app" / "web" / "templates"


# ── L1: dead pending.html removed ────────────────────────────


def test_pending_template_deleted():
    """app/web/templates/pending.html had no route renderer
    (v1.12.41 removed /pending). Deleted."""
    assert not (TEMPLATES / "pending.html").exists()


# ── L2: 18 dead CSS class declarations removed ───────────────


def _strip_css_comments(text: str) -> str:
    """Remove `/* ... */` comment blocks (single- and multi-line)
    from a CSS source. The dead-class scan must skip comments so
    rationale markers that mention `.dot-red` etc. don't trip
    the check."""
    import re
    return re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)


def _css_rule_exists(class_name: str) -> bool:
    """True iff a CSS rule selector (not a comment) references
    `.<class_name>` followed by a non-name character (so a
    declaration like `.foo {` matches but a substring inside
    `.foo-bar` doesn't)."""
    import re
    text = _strip_css_comments(CSS.read_text())
    pattern = re.compile(
        r"\." + re.escape(class_name) + r"(?![-_a-zA-Z0-9])"
    )
    return bool(pattern.search(text))


def test_dead_css_classes_removed():
    """Every CSS class declaration the audit flagged as dead must
    be gone. List below mirrors the audit's findings exactly so
    the test acts as a regression guard if any of them get
    re-added accidentally."""
    # v1.14.61: removed `coverage-bar-seg-no-tdb` from this list
    # — restored after v1.14.56 over-deletion (the libraries-page
    # per-section bar still emits the class for the unthemed
    # remainder; without the rule the wedge was invisible). Pin
    # of the restoration lives in test_v1_14_61_dead_code_post_audit.
    dead = [
        "dot-amber",
        "dot-red",
        "chip-group",
        "chip-warn",
        "chip-sep",
        "chips-secondary",
        "chip-tiny",
        "fail-glyph",
        "fail-glyph-bad",
        "fail-glyph-warn",
        "fail-glyph-info",
        "coverage-bar-seg-available",
        "coverage-row-legend",
        "coverage-legend-swatch-themed",
        "coverage-legend-swatch-available",
        "coverage-legend-swatch-no-tdb",
        "info-scope-chip-fourk",
        "info-scope-chip-standard",
    ]
    survivors = [c for c in dead if _css_rule_exists(c)]
    assert not survivors, (
        f"CSS rule(s) still defined for classes the audit flagged "
        f"as dead: {survivors}. Either delete the rule or wire up "
        f"a JS / template caller. Same drift shape as v1.14.48 "
        f"`lib-source-cloud` / v1.14.50 `lib-source-manual`."
    )


def test_fail_pulse_keyframes_survives():
    """Sanity: the `@keyframes fail-pulse` block must still exist
    — 5 other rules (state-pill / tdb-pill broken/dropped/await
    variants) reference it; removing it would silently break the
    pulse animation on those pills."""
    css = CSS.read_text()
    assert "@keyframes fail-pulse" in css
    # And there are still consumers using `animation: fail-pulse …`.
    assert css.count("animation: fail-pulse") >= 3


# ── L8: api_events admin gate ────────────────────────────────


def test_api_events_requires_admin():
    """The /api/events endpoint must call _require_admin. Pre-fix
    it was unauthenticated; raw events table (which can carry
    yt-dlp output with cookie-walled URLs / IDs) was readable by
    any forward-auth-disabled session."""
    src = (REPO / "app" / "web" / "api.py").read_text()
    fn_anchor = src.index("async def api_events(")
    body = src[fn_anchor:fn_anchor + 1500]
    # The Request param + the gate.
    assert "request: Request," in body
    assert "_require_admin(request)" in body
    # v1.14.56 marker.
    assert "v1.14.56:" in body


# ── L13: v44 migration timestamp format matches runtime ──────


def test_v44_migration_uses_iso_timestamp_format():
    """The v44 sfa-backfill migration must write timestamps in
    the runtime now_iso() format (`YYYY-MM-DDTHH:MM:SS+00:00`).
    Pre-fix `datetime('now')` emitted `YYYY-MM-DD HH:MM:SS` (no
    T separator, no tz offset) — mixed two timestamp formats in
    section_failure_acks. Lex-sortable either way but breaks any
    future date-range filter / display logic that assumes one
    shape."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    fn_anchor = src.index("def _migrate_v43_to_v44(")
    body = src[fn_anchor:fn_anchor + 5000]
    # The migration uses strftime now.
    assert "strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now')" in body
    # The pre-fix bare datetime('now') is gone from this migration.
    # Strip SQL `-- …` line comments first so the rationale marker
    # that mentions the OLD form (for archaeology) doesn't trip the
    # check.
    body_no_sql_comments = "\n".join(
        line for line in body.splitlines()
        if not line.lstrip().startswith("--")
    )
    assert "datetime('now')" not in body_no_sql_comments
    # v1.14.56 marker.
    assert "v1.14.56:" in body


# ── L14: probe job_type dormant asymmetry documented ─────────


def test_probe_job_type_asymmetry_documented():
    """The schema CHECK constraint at db.py:188+ must carry an
    inline marker explaining why `probe` is allowed in v14-
    migrated DBs but rejected in fresh installs (zero callers
    today; future re-introduction needs explicit alignment)."""
    src = (REPO / "app" / "core" / "db.py").read_text()
    # Anchor on the jobs CREATE TABLE at the top of the schema.
    anchor = src.index("CREATE TABLE IF NOT EXISTS jobs (")
    block = src[max(0, anchor - 1500):anchor]
    assert "v1.14.56:" in block
    assert "dormant asymmetry" in block
    assert "'probe'" in block
