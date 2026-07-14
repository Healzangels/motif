"""v0.51.142 — code-review follow-ups on v0.51.141's theme-upload UX.

Three message/UX refinements (no behavior or data change):

  1. The proxy-block error branches on `r.status` instead of always blaming
     request size — a 502 (motif down mid-redeploy), a 401 (SSO timeout), a 403
     (WAF block) and a 413 (body-limit) each get accurate prose. Mislabelling a
     502 as "trim the theme" sent the operator to fix the wrong thing.

  2. The pre-flight >9 MiB proxy-cap warning gets a `.warn` (amber) class so it
     visually reads as a warning, not the same dim tone as the plain info line.

  3. The >50 MiB reject ceils the shown size so the copy can't read
     "50.0 MB exceeds motif's 50 MB limit" at the rounding boundary.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _upload_handler() -> str:
    # the theme-upload fetch flow (the direct fetch to /upload-theme + its error path).
    i = APP_JS.index("/upload-theme`, {")
    return APP_JS[i - 200:i + 2200]


def _size_hint() -> str:
    i = APP_JS.index("function bindUploadDialog")
    return APP_JS[i:i + 2200]


# ── #1: proxy message branches on status ─────────────────────

def test_proxy_message_branches_on_status():
    block = _upload_handler()
    # each distinct proxy cause is diagnosed by status code, not a single
    # "exceeds request-body limit" catch-all.
    assert "r.status === 413" in block
    assert "r.status === 403" in block
    assert "r.status === 401" in block
    assert "r.status >= 502 && r.status <= 504" in block


def test_proxy_message_no_longer_hardblames_size_for_every_html_body():
    block = _upload_handler()
    # the old v0.51.141 wording asserted the body-limit cause unconditionally.
    assert "the theme likely exceeds its request-body limit" not in block
    # the 5xx branch tells the operator to retry (motif restarting), not to shrink.
    assert "retry in a moment" in block
    # the 401 branch points at the session, not the file.
    assert "session expired" in block


def test_proxy_branch_still_detects_html_and_surfaces_json_detail():
    # v0.51.141 invariants preserved: HTML detection + motif JSON `detail` surfacing.
    block = _upload_handler()
    assert "text/html" in block
    assert "JSON.parse(t)" in block
    assert "j.detail" in block


# ── #2: >9 MiB warning gets an amber .warn class ─────────────

def test_warn_class_exists_and_is_amber():
    # a dedicated caution tone, sibling to .form-status.ok / .err.
    css = APP_CSS[APP_CSS.index(".form-status.err"):APP_CSS.index(".form-status.err") + 260]
    assert ".form-status.warn" in css
    assert "var(--amber)" in css


def test_size_hint_marks_the_9mib_tier_as_warn():
    block = _size_hint()
    # the >9 MiB tier adds the warn class; the pick-reset clears it too.
    assert "classList.add('warn')" in block
    assert "remove('err', 'ok', 'warn')" in block


# ── #3: >50 MiB reject ceils the displayed size ──────────────

def test_over_limit_size_is_ceiled_not_toFixed():
    block = _size_hint()
    # the reject branch uses Math.ceil so a 50.04 MiB file shows "~51 MB",
    # never the self-contradictory "50.0 MB exceeds 50 MB".
    assert "Math.ceil(f.size / (1024 * 1024))" in block
    assert "exceeds motif's 50 MB limit" in block
