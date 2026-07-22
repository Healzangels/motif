"""v0.51.219 — the per-row deep-links carry their edition, so they skip the picker.

v0.51.218 made the INFO card ASK which cut it should show when a link doesn't say. But two
of the links already KNOW: every row of the loudness-audit report and the canonical-health
report IS one local_files edition (both SELECT edition_key). Making them pass info_edition
means clicking a loudness outlier lands on THAT cut's card directly — the picker is then
only ever the fallback for links that genuinely can't know (a title-level digest, the
/queue reprobe OPEN ROW whose event isn't edition-specific).

Both reports already ship edition_key per row to the client (broken_canonical_report._entry
+ the loudness report's _outliers), so this is purely the three client-side edits: the two
producers append info_edition, and the deep-link parser reads it and threads it through
openInfoDialog's v0.51.218 editionKey argument.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _fn(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


# ── the two producers that KNOW their edition ────────────────────────────────

def test_loudness_audit_rows_carry_their_edition():
    fn = _fn("function outRow(r) {", "function renderPreview()")
    assert "p.set('info_edition', String(r.edition_key))" in fn
    assert "if (r.edition_key != null)" in fn, (
        "'' is a real edition (untagged folder); a truthiness check would drop it and put "
        "the standard cut back into the picker")


def test_canonical_health_rows_carry_their_edition():
    fn = _fn("function link(r) {", "const src = (r)")
    assert "p.set('info_edition', String(r.edition_key))" in fn
    assert "if (r.edition_key != null)" in fn


def test_both_reports_actually_emit_edition_key_to_the_client():
    """Backend→frontend data-flow guard (the v1.18.81 phantom-fix lesson): the producers
    above read r.edition_key, so the endpoints must actually put it there or the links
    silently never scope. Pin both sources."""
    ch = (REPO / "app" / "core" / "canonical_health.py").read_text()
    assert '"edition_key": r["edition_key"]' in ch, "broken_canonical_report._entry"
    la = (REPO / "app" / "core" / "loudness_audit.py").read_text()
    assert '"edition_key": r["edition_key"]' in la, "the loudness report _outliers row"


# ── the parser end of the wire ───────────────────────────────────────────────

def test_deep_link_parser_reads_and_threads_info_edition():
    blk = _fn("v1.14.85: ?info_open=<tmdb_id>", "Auto-refresh on relevant pages")
    assert "sp.has('info_edition')" in blk, (
        "must be .has(), not `.get()||undefined` — '' is a real edition a falsy fallback "
        "would drop back into the v0.51.218 picker")
    assert "openInfoDialog(infoMt, infoOpen, infoSection, undefined, infoEdition)" in blk


def test_a_link_without_an_edition_still_defers_to_the_picker():
    """The /queue reprobe OPEN ROW carries mt/id/section/fourk/title but no edition — its
    event isn't edition-specific — so it must NOT invent info_edition; it correctly lands
    on the v0.51.218 picker for a multi-edition title. Guard that it stayed edition-free."""
    fn = _fn("const tabPath = mt === 'movie' ? '/movies' : '/tv';",
             "window.location.href = `${tabPath}")
    assert "info_edition" not in fn, (
        "the queue OPEN ROW has no edition to carry — it must defer to the picker, not "
        "fabricate a scope")


def test_no_producer_uses_a_truthiness_guard_for_the_edition():
    """Sweep: every info_edition emitter must use `!= null`, since '' is the untagged-
    folder edition and a truthy check silently un-scopes exactly the standard cut."""
    import re
    for m in re.finditer(r"info_edition", APP_JS):
        # look back a little for the guard on the same logical line
        line_start = APP_JS.rfind("\n", 0, m.start())
        prev = APP_JS[max(0, line_start - 120):m.start()]
        if "p.set('info_edition'" in APP_JS[line_start:m.start() + 40] or \
           "params.set('info_edition'" in APP_JS[line_start:m.start() + 40]:
            assert "!= null" in prev or "!= null" in APP_JS[line_start:m.start()], (
                f"info_edition emitter near offset {m.start()} lacks a `!= null` guard")
