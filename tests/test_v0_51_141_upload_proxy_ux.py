"""v0.51.141 — theme-upload UX for reverse-proxy body limits.

The user's CrowdSec AppSec caps request bodies at 10 MiB and 403s an oversized
theme upload with a full HTML ban page BEFORE it reaches motif. Two client-side
improvements:

  A. The upload fetch handler detects an HTML / non-JSON error body (a proxy/WAF
     block, not motif) and shows a one-line actionable message instead of dumping
     the whole page; for motif's own errors it surfaces the JSON `detail` (e.g.
     413 "file > 50 MiB") rather than the raw JSON.

  B. A pre-flight size hint on file pick: > 50 MiB → will be rejected (motif's
     cap); > 9 MiB → warn it may be proxy-blocked + nudge to trim/re-encode.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _upload_handler() -> str:
    # the theme-upload fetch flow (the direct fetch to /upload-theme + its error path).
    i = APP_JS.index("/upload-theme`, {")
    return APP_JS[i - 200:i + 1400]


def test_upload_detects_proxy_block_and_stops_dumping_html():
    block = _upload_handler()
    # detects an HTML / non-JSON body (a proxy/WAF page, not motif's JSON) ...
    assert "text/html" in block
    assert "reverse proxy" in block and "reaching motif" in block
    # ... and no longer just throws the raw response text as the message.
    assert "${r.status}: ${t || r.statusText}" not in block


def test_upload_surfaces_motif_json_detail():
    block = _upload_handler()
    # motif's own error (413 file > 50 MiB, etc.) is JSON with a `detail`.
    assert "JSON.parse(t)" in block
    assert "j.detail" in block


def test_pick_time_size_warning_present():
    i = APP_JS.index("function bindUploadDialog")
    block = APP_JS[i:i + 2000]
    # the change-listener warns above motif's 50 MiB cap AND near common proxy caps.
    assert "50 * 1024 * 1024" in block
    assert "9 * 1024 * 1024" in block
    assert "reverse proxy" in block or "proxy/WAF" in block
