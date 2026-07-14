"""v0.51.144 — reverse-proxy audit, tag 1/3 (correctness).

A shared decoder + false-success guards. Reverse proxies (CrowdSec / nginx /
Cloudflare / Authentik) can answer an /api/ call with an HTML error page, a 429,
or a 302→200 SSO login page (fetch follows redirects, so r.ok is true). motif
always answers /api/ with JSON, so a non-JSON body means a proxy answered.

  1. Shared `proxyStatusHint(status)` / `describeProxyOrHttpError(status, ct, body)`
     decoder (was inlined in the theme-upload handler in v0.51.141/142), now with a
     429 rate-limit branch.
  2. Three false-success guards: PUSH/REPLACE and SWITCH PLACEMENT set the button to
     "QUEUED" on r.ok alone; PURGE (/forget) passed on !r.ok. A proxy 200 SSO page is
     r.ok, so each claimed success though nothing reached motif. They now require a
     motif JSON content-type (PURGE still allows the legit 204) first.
  3. Fixes the v0.51.143 DB-restore discriminator: api() sets e.status even for a
     proxy 413/403 HTML page (e.detail is null there), so the status-only check dumped
     the raw page. Now: e.detail (motif) → e.status (proxy) → neither (network/SSO).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _slice(anchor: str, before: int = 0, after: int = 700) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i - before:i + after]


# ── 1. shared decoder ────────────────────────────────────────

def test_decoder_defined_once_with_all_status_branches():
    block = _slice("function proxyStatusHint", after=1400)
    for br in ("status === 413", "status === 403", "status === 401",
               "status === 429", "status >= 502 && status <= 504"):
        assert br in block, br
    # the 429 branch is the new rate-limit case this tag adds.
    assert "rate-limited" in block


def test_describe_returns_hint_only_for_html_bodies():
    block = _slice("function describeProxyOrHttpError", after=320)
    # keys off an HTML content-type OR a `<`-leading body; returns null otherwise
    # (so callers keep handling motif's own JSON).
    assert "text/html" in block
    assert "/^\\s*</" in block or "^\\s*<" in block
    assert "proxyStatusHint(status)" in block


# ── 2. false-success guards ──────────────────────────────────

def test_replace_confirms_json_before_queued():
    block = _slice("/replace${_qs}", after=700)
    assert "describeProxyOrHttpError(" in block
    assert "includes('application/json')" in block
    assert "proxyStatusHint(resp.status)" in block
    # the guard must sit before the QUEUED assignment.
    assert block.index("includes('application/json')") < block.index("'QUEUED'")


def test_switch_placement_confirms_json_before_queued():
    block = _slice("/switch-placement${_qs}", after=700)
    assert "includes('application/json')" in block
    assert "proxyStatusHint(r.status)" in block
    assert block.index("includes('application/json')") < block.index("'QUEUED'")


def test_purge_guards_proxy_200_but_keeps_204():
    block = _slice("/forget`", after=700)
    # a proxy 200 HTML page must not pass as a silent no-op ...
    assert "includes('application/json')" in block
    # ... while the legitimate 204 still short-circuits.
    assert "r.status !== 204" in block


# ── 3. DB-restore discriminator (fixes v0.51.143) ────────────

def test_restore_keys_on_detail_then_status_then_neither():
    block = _slice("'/api/admin/database-restore/upload'", after=1500)
    # motif's own error carries e.detail; a proxy HTML page has a numeric e.status
    # but null detail; a 200 SSO/network drop has neither.
    assert "e.detail != null" in block
    assert "proxyStatusHint(e.status)" in block
    assert "could not reach motif" in block
    # detail is checked BEFORE status so a real motif 413 shows its own message,
    # not the generic proxy hint.
    assert block.index("e.detail != null") < block.index("proxyStatusHint(e.status)")
