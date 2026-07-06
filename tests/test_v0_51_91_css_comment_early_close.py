"""v0.51.91 — a malformed CSS comment silently dropped a live rule.

The dashboard "hide section" toggle stopped hiding sections (RECENT ACTIVITY /
SYNC HISTORY stayed visible after "// done editing"). The JS was correct — it
applied `.dash-user-hidden` and persisted `hidden:true` via /api/dashboard/layout
(the PUT body was right). The break was in app.css: the v0.51.70 dead-Scans note
listed removed selectors as ".kind-*/.scan-filter*/.kpi*/..." — those '/'
separators formed spurious star-slash sequences, and the FIRST one closed the
comment early. The CSS parser then hit invalid tokens and its error-recovery
swallowed the very next rule, `.dash-user-hidden { display: none !important; }`,
so the class had no CSS backing it at runtime.

A second, older instance of the identical bug lived at the glossary-legend
comment ("gc-*/gd-*/gg-*") and had been silently eating
`.library-legend-pill { gap: var(--gap-2); }` — the "unstyled legend box for 3
deploys" symptom described in the adjacent comments.

Reproduced + fixed + confirmed in a real browser (both rules now parse; a hidden
section computes display:none outside customize mode and grid/block @ opacity .45
inside it). This guard walks app.css exactly as a CSS tokenizer would and fails
on ANY comment that closes early — so a future glob-in-a-comment can't silently
drop the rule that follows it.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _scan_comments(css: str):
    """Walk the stylesheet the way a CSS tokenizer does. A comment runs from
    `/*` to the NEXT `*/`; anything else is content. Report every anomaly:
      - STRAY '*/'  : a comment closer with no open comment → an earlier
                      comment closed early (the bug).
      - NESTED '/*' : a '/*' inside a comment body → CSS comments don't nest,
                      so the intended close is being misread.
      - UNCLOSED    : a '/*' with no closing '*/'.
    """
    issues = []
    i, n = 0, len(css)

    def lineno(pos: int) -> int:
        return css.count("\n", 0, pos) + 1

    while i < n:
        if css[i:i + 2] == "/*":
            end = css.find("*/", i + 2)
            if end == -1:
                issues.append((lineno(i), "UNCLOSED"))
                break
            if "/*" in css[i + 2:end]:
                issues.append((lineno(i), "NESTED /*"))
            i = end + 2
        elif css[i:i + 2] == "*/":
            issues.append((lineno(i), "STRAY */"))
            i += 2
        else:
            i += 1
    return issues


def _strip_comments(css: str) -> str:
    out, i, n = [], 0, len(css)
    while i < n:
        if css[i:i + 2] == "/*":
            end = css.find("*/", i + 2)
            i = (end + 2) if end != -1 else n
        else:
            out.append(css[i])
            i += 1
    return "".join(out)


def test_no_css_comment_closes_early():
    issues = _scan_comments(CSS)
    assert issues == [], (
        "app.css has a malformed comment (early close / nesting / unclosed) — a "
        "glob like '.kind-*/.scan-filter*' inside a comment forms a star-slash "
        "that closes it early, and the parser's error-recovery then swallows the "
        f"following rule. Offending line(s): {issues}")


def test_dash_user_hidden_rule_is_real_css():
    # The rule the v0.51.70 comment used to swallow. It MUST survive
    # comment-stripping (i.e. be real top-level CSS, not eaten by a comment).
    cleaned = _strip_comments(CSS)
    assert ".dash-user-hidden { display: none !important; }" in cleaned


def test_library_legend_pill_rule_is_real_css():
    # The rule the glossary-legend comment used to swallow (the older instance).
    cleaned = _strip_comments(CSS)
    assert ".library-legend-pill { gap: var(--gap-2); }" in cleaned
