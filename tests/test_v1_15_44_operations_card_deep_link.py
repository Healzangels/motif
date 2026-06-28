"""v1.15.44 — OPERATIONS row click-through + audit-doc gitignore.

the user (approving the v1.15.42 follow-up flagged in its journal):
"lets do it."

## OPERATIONS row click-through

v1.15.42 wired the ACTIVITY row's ADDED TODAY/WEEK cards as
deep-links to /queue?since=N. Same pattern applies cleanly to
the OPERATIONS row's three cards (QUEUE PENDING / RUNNING NOW /
FAILURES) — the JOBS chip on /queue has honored ?status= since
v1.11.73, so the chip auto-activates on land. The deep-link is
the operator's missing "I see 3 failures → click → here are the
3 jobs" loop without leaving the dashboard.

## Audit-doc gitignore housekeeping

Five agent-written audit docs from the v1.14.20 cycle had been
sitting in `git status` as untracked for 3 days
(AUDIT_API/FRONTEND/WORKER.md, CODEBASE_AUDIT.md,
INFO_CARD_AUDIT.md). Same treatment as SESSION_JOURNAL.md —
local-only working notes, not for the repo. Pattern matches
new audits (AUDIT_*.md) plus the two pre-convention names.

Static-text guards (consistent with v1.15.42 patterns).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
GITIGNORE = REPO / ".gitignore"


# ── 1. OPERATIONS row cards link to /queue?status=N ─────────


OPS_CARD_LINKS = [
    ("queue-pending", "pending"),
    ("queue-running", "running"),
    ("queue-failed", "failed"),
]


def test_operations_cards_have_status_deep_links():
    """Each of the three OPERATIONS cards (QUEUE PENDING /
    RUNNING NOW / FAILURES) must wrap its content in
    `<a href="/queue?status=<value>">` so the JOBS chip on
    /queue auto-activates on land."""
    html = DASH_HTML.read_text()
    failures = []
    for card_id, status in OPS_CARD_LINKS:
        attr = f'data-dash-card="{card_id}"'
        anchor = html.index(attr)
        block = html[anchor:anchor + 800]
        if f'href="/queue?status={status}"' not in block:
            failures.append(
                f"{card_id}: missing href=/queue?status={status} — "
                "JOBS chip on /queue won't auto-activate on land"
            )
        if 'class="stat-card-link"' not in block:
            failures.append(
                f"{card_id}: missing class=stat-card-link — "
                "v1.15.42 hover affordance won't apply"
            )
    assert not failures, "v1.15.44: " + "; ".join(failures)


def test_operations_anchor_wraps_inner_content_not_article():
    """Same structural guard as v1.15.42: the deep-link anchor
    must live INSIDE the article so the per-card customize
    controls (◀ ▶ // HIDE injected at firstChild by
    dashboard-customize.js v1.15.32) stay outside the link's
    click target. Otherwise clicking // HIDE navigates instead
    of toggling the card."""
    html = DASH_HTML.read_text()
    for card_id, _ in OPS_CARD_LINKS:
        attr = f'data-dash-card="{card_id}"'
        article_start = html.index(attr)
        art_open = html.rfind('<article', 0, article_start)
        art_close = html.index('</article>', article_start) + len('</article>')
        article_html = html[art_open:art_close]
        assert '<a href="/queue?status=' in article_html, (
            f"v1.15.44: {card_id} card missing inner anchor — "
            "must live INSIDE the article"
        )
        before_article = html[max(0, art_open - 200):art_open]
        assert not before_article.rstrip().endswith('">'), (
            f"v1.15.44: {card_id} appears wrapped by a sibling "
            "anchor — keep the anchor INSIDE the article so the "
            "customize controls injection point stays clean"
        )


def test_operations_status_values_match_jobs_chip_allowlist():
    """Cross-source mirror-principle guard: the deep-link
    `status=` values on the OPERATIONS cards MUST be in the
    JOBS chip allowlist on /queue (v1.11.73 added the
    `['pending','running','failed','cancelled','done']`
    allowlist in bindQueue()). Otherwise the chip won't
    auto-activate on land."""
    import re
    html = DASH_HTML.read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    card_status_values = set(
        re.findall(r'href="/queue\?status=(\w+)"', html)
    )
    bind_anchor = js.index("function bindQueue()")
    body = js[bind_anchor:bind_anchor + 5000]
    # Jobs chip allowlist literal from v1.11.73.
    allowlist_anchor = body.index(
        "['pending','running','failed','cancelled','done']"
    )
    allowlist_str = body[allowlist_anchor:allowlist_anchor + 80]
    js_status_values = set(re.findall(r"'(\w+)'", allowlist_str))
    missing = card_status_values - js_status_values
    assert not missing, (
        f"v1.15.44: dashboard card status= value(s) {missing} "
        f"not in JOBS chip allowlist — chip won't auto-activate. "
        "Either add to allowlist or change the card href."
    )


# ── 2. Audit docs gitignored ─────────────────────────────────


def test_audit_docs_gitignored():
    """The five audit artifacts left over from the v1.14.20
    audit cycle (AUDIT_API/FRONTEND/WORKER.md + CODEBASE_AUDIT.md
    + INFO_CARD_AUDIT.md) plus any future AUDIT_*.md must match
    a .gitignore entry. They're agent scratch output — local-only
    working notes, like SESSION_JOURNAL.md."""
    src = GITIGNORE.read_text()
    assert "docs/AUDIT_*.md" in src, (
        "v1.15.44: future AUDIT_*.md docs must be gitignored "
        "(catch-all for new audit cycle output)"
    )
    assert "docs/CODEBASE_AUDIT.md" in src, (
        "v1.15.44: pre-convention CODEBASE_AUDIT.md needs explicit "
        "rule (doesn't match AUDIT_*.md prefix)"
    )
    assert "docs/INFO_CARD_AUDIT.md" in src, (
        "v1.15.44: pre-convention INFO_CARD_AUDIT.md needs explicit "
        "rule (suffix-AUDIT, not prefix-AUDIT)"
    )
