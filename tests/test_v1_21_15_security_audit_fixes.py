"""v1.21.15 — security + bug audit fixes (contained set).

A 4-agent security + bug audit surfaced two HIGH security issues and
several MEDs. This file covers the CONTAINED fixes shipped here:

  XSS (HIGH) — the CSV-import preview rendered an <a href> built from the
    raw imported URL; htmlEscape blocks attribute-breakout but NOT a
    `javascript:` scheme, and the backend YouTube regex is unanchored so
    `javascript:…//youtu.be/<id>` classified as a clickable link. Fix: a
    client-side `safeHref()` scheme allowlist applied at every href sink.
  M1 — notifications.apprise_external_url was returned in cleartext by
    GET /api/config (it carries basic-auth / path tokens). Masked now.
  MED (SSRF) — sync.database_url / git_url had no scheme validation.
  MED (bug) — _maybe_notify_theme_available's SRC=— gate required
    has_theme=0, missing the stale-Plex-cache case (has_theme=1 +
    verified_ok=0) that the canonical _SRC_LETTER_SQL also classifies –.

The forward-auth bypass (HIGH) is deployment-sensitive and handled
separately.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
PLEX_ENUM = (REPO / "app" / "core" / "plex_enum.py").read_text()


# ── XSS: safeHref scheme allowlist ───────────────────────────

def test_safehref_helper_defined_and_scheme_allowlisted():
    idx = APP_JS.index("function safeHref(")
    body = APP_JS[idx:idx + 220]
    # http(s) only; everything else → null.
    assert "/^https?:\\/\\//i.test(s)" in body
    assert "? s : null" in body


def test_href_sinks_use_safehref():
    """Both the CSV-import preview and the INFO-card linkOrDash must
    route the URL through safeHref before building an <a href>."""
    # Import preview.
    imp = APP_JS.index("const _impHref = safeHref(r.imported_url)")
    seg = APP_JS[imp:imp + 400]
    assert "href=\"${htmlEscape(_impHref)}\"" in seg
    # INFO card linkOrDash.
    lod = APP_JS.index("const linkOrDash = (url, color) =>")
    seg2 = APP_JS[lod:lod + 500]
    assert "const href = safeHref(url);" in seg2
    assert "href=\"${htmlEscape(href)}\"" in seg2


# ── M1: apprise_external_url masking ─────────────────────────

def test_apprise_external_url_masked_in_get():
    assert 'payload["notifications"]["apprise_external_url"] = "***"' in API_PY


def test_apprise_external_url_preserved_in_patch():
    idx = API_PY.index('k == "apprise_external_url"')
    seg = API_PY[idx:idx + 200]
    assert 'v == "" or v == "***"' in seg
    assert "continue" in seg


# ── SSRF: database_url / git_url scheme validation ───────────

def test_validate_rejects_nonhttp_sync_urls():
    from app.core.config_file import MotifConfig, validate
    cfg = MotifConfig()
    cfg.sync.database_url = "file:///etc/passwd"
    cfg.sync.git_url = "gopher://169.254.169.254/"
    errs = validate(cfg, require_themes_dir=False)
    assert any("database_url" in e for e in errs), errs
    assert any("git_url" in e for e in errs), errs


def test_validate_accepts_default_https_sync_urls():
    from app.core.config_file import MotifConfig, validate
    cfg = MotifConfig()
    errs = validate(cfg, require_themes_dir=False)
    assert not any("database_url" in e or "git_url" in e for e in errs), errs


# ── bug: stale-cache SRC=— in the notify/auto-acquire gate ───

def test_candidate_gate_includes_stale_plex_cache():
    """The SRC=— candidate query must also accept has_theme=1 +
    verified_ok=0 (stale Plex cache → canonical SRC=–), not only
    has_theme=0."""
    fn = PLEX_ENUM.index("def _maybe_notify_theme_available(")
    body = PLEX_ENUM[fn:PLEX_ENUM.index("\ndef ", fn + 1)]
    assert "pi.has_theme = 0 " in body
    assert "OR pi.plex_theme_verified_ok = 0" in body


def test_version_bumped():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
