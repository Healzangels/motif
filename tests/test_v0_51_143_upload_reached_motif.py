"""v0.51.143 — "did it actually reach motif?" guard on the two large-body uploads.

Code-review finding #4 (deferred from v0.51.142): a reverse proxy that intercepts
a POST and answers with a 302→200 HTML page (e.g. an SSO login page) yields
`r.ok === true` though the request never reached motif.

  - Theme upload (a DIRECT fetch) declared "✓ uploaded" on `r.ok` alone → a false
    success for a theme that never landed. Now the success path confirms motif's
    JSON body ({ok:true}) before declaring success.
  - Database restore goes through the api() helper, which does `return r.json()`
    and therefore already THROWS on a non-JSON 200 (no false success) — but the
    thrown SyntaxError surfaced as a cryptic "Unexpected token '<'". Its catch now
    distinguishes motif's own errors (which carry err.status) from a proxy HTML 200
    / network drop (no .status) and shows an actionable message.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _upload_success() -> str:
    # the theme-upload flow, from the fetch through the success branch.
    i = APP_JS.index("/upload-theme`, {")
    return APP_JS[i:i + 3400]


def _restore_upload() -> str:
    i = APP_JS.index("'/api/admin/database-restore/upload'")
    return APP_JS[i - 200:i + 900]


# ── theme upload: confirm motif's JSON body before success ───

def test_upload_confirms_motif_json_before_success():
    block = _upload_success()
    # it inspects the success response content-type + parses the JSON body ...
    assert "application/json" in block
    assert "okBody" in block and "await r.json()" in block
    # ... and requires motif's {ok:true} shape before declaring success.
    assert "okBody.ok !== true" in block


def test_upload_rejects_non_motif_200():
    block = _upload_success()
    # a 200 that isn't motif's JSON throws instead of showing "✓ uploaded".
    assert "not from motif" in block
    # the guard sits BEFORE the success text (so a false 200 can't reach it).
    assert block.index("okBody.ok !== true") < block.index("✓ uploaded · placement queued")


# ── database restore: proxy/network vs motif error in the catch ─

def test_restore_catch_distinguishes_proxy_from_motif_error():
    block = _restore_upload()
    # motif's own errors carry err.status (api() sets it on !r.ok); its absence
    # means the 200 body wasn't motif JSON (proxy HTML) or the network dropped.
    assert "typeof e.status === 'number'" in block
    assert "could not reach motif" in block
