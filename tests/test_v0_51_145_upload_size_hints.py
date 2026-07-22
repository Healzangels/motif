"""v0.51.145 — reverse-proxy audit, tag 2/3: pre-flight size hints on the other
two uploads (theme upload already had one from v0.51.141).

  - DB restore (server cap 500 MiB; a real motif.db nearly always exceeds a proxy
    body cap): a file-pick hint → >500 MB reject / >9 MB "very likely blocked by a
    proxy, restore on your LAN" amber warn.
  - Import-preview (server cap 5 MB; a direct fetch that previously had zero proxy
    handling): a file-pick size hint (>5 MB reject + disable / >1 MB proxy warn) AND
    its error path now routes through the shared describeProxyOrHttpError decoder plus
    a non-JSON-200 guard, killing the raw-HTML slice / "Unexpected token '<'".
"""
from __future__ import annotations
from _slice_helpers import slice_to_next

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _restore_pick() -> str:
    # the DB-restore file-input change listener.
    return slice_to_next(
        APP_JS, "const uploadBtn = document.getElementById('database-restore-upload-btn')",
        "uploadBtn.addEventListener('click'")


def _import_pick() -> str:
    # the import-csv-file change listener.
    i = APP_JS.index("previewBtn.disabled = !fileInput.files || !fileInput.files.length;")
    return APP_JS[i - 100:i + 900]


def _import_upload() -> str:
    i = APP_JS.index("'/api/import/preview'")
    return APP_JS[i:i + 900]


# ── DB restore pre-flight hint ───────────────────────────────

def test_restore_has_preflight_size_hint():
    block = _restore_pick()
    assert "fileInput.addEventListener('change'" in block
    assert "500 * 1024 * 1024" in block   # motif's cap
    assert "9 * 1024 * 1024" in block      # proxy warn tier
    # the proxy-cap warning steers to the LAN and uses the amber .warn tone.
    assert "reverse proxy/WAF" in block or "reverse proxy / WAF" in block
    assert "form-status warn" in block


# ── import-preview pre-flight hint ───────────────────────────

def test_import_has_preflight_size_hint():
    block = _import_pick()
    assert "5 * 1024 * 1024" in block      # motif's cap → reject + disable
    assert "1 * 1024 * 1024" in block      # proxy warn tier
    assert "previewBtn.disabled = true" in block


# ── import-preview proxy handling on the upload ──────────────

def test_import_upload_uses_shared_decoder_and_guards_non_json_200():
    block = _import_upload()
    # error path no longer dumps a raw HTML slice — it asks the shared decoder first.
    assert "describeProxyOrHttpError(" in block
    # and a proxy 302→200 SSO page is caught before resp.json() throws cryptically.
    assert "includes('application/json')" in block
    assert "proxyStatusHint(resp.status)" in block
    assert block.index("includes('application/json')") < block.index("await resp.json()")
