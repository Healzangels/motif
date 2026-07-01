"""v0.50.95 — fonts self-hosted (drop external Google Fonts).

The top row (brand / nav / hero title) FOUT-reflowed 30-85px horizontally on
every navigation whenever the font cache was cold (measured: hero title 242px in
VT323 vs 327px in the fallback), because all three families loaded from external
Google Fonts render-blocking with display=swap. v0.50.95 self-hosts the latin
woff2 (same-origin @font-face + preload) so the branded font paints on the first
frame — no swap reflow — and removes the external dependency for a self-hosted,
Authentik-gated app.

Guards: no Google Fonts refs; local preload matches @font-face src exactly (a
mismatch = double fetch + dead preload); every used weight has a face; the files
exist and are valid woff2; the font-var chains still name VT323 / JetBrains Mono.
"""
from __future__ import annotations

import re
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent
BASE_HTML = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
FONTS_DIR = REPO / "app" / "web" / "static" / "fonts"

# every weight the CSS actually uses (300 was requested from Google but never
# used, so it is intentionally NOT bundled — see the app.css comment).
USED_WEIGHTS = (400, 500, 600, 700)
FONT_FILES = ["vt323-400.woff2"] + [f"jetbrains-mono-{w}.woff2" for w in USED_WEIGHTS]


# ── external dependency gone ────────────────────────────────────────────

def test_no_external_google_fonts():
    assert "fonts.googleapis.com" not in BASE_HTML, "drop the Google Fonts <link>"
    assert "fonts.gstatic.com" not in BASE_HTML, "drop the gstatic preconnect"


# ── @font-face self-hosts every used weight ─────────────────────────────

def test_font_faces_declared_for_every_used_weight():
    assert "@font-face" in APP_CSS
    # VT323 (single weight)
    assert "font-family: 'VT323';" in APP_CSS
    assert "url('/static/fonts/vt323-400.woff2') format('woff2')" in APP_CSS
    # JetBrains Mono — one face per used weight
    for w in USED_WEIGHTS:
        assert f"url('/static/fonts/jetbrains-mono-{w}.woff2') format('woff2')" in APP_CSS, (
            f"missing @font-face for JetBrains Mono {w}"
        )


def test_weight_300_not_bundled():
    # unused → not shipped (no premature assets); its file must not exist either.
    assert "jetbrains-mono-300.woff2" not in APP_CSS
    assert not (FONTS_DIR / "jetbrains-mono-300.woff2").exists()


# ── files present + valid woff2 ─────────────────────────────────────────

def test_font_files_exist_and_are_woff2():
    for name in FONT_FILES:
        p = FONTS_DIR / name
        assert p.exists(), f"missing bundled font {name}"
        assert p.read_bytes()[:4] == b"wOF2", f"{name} is not a valid woff2"


# ── preload correctness (a mismatch double-fetches + kills the preload) ──

def test_preloads_present_and_match_font_face_src_exactly():
    for name in ("jetbrains-mono-400.woff2", "vt323-400.woff2"):
        href = f"/static/fonts/{name}"
        # preloaded, as a font, with crossorigin (required even same-origin)
        m = re.search(
            r'<link rel="preload" as="font" type="font/woff2" '
            rf'href="{re.escape(href)}" crossorigin>',
            BASE_HTML,
        )
        assert m, f"missing/!exact preload for {name}"
        # the preload href carries NO ?v= — it must byte-match the @font-face
        # src so the browser reuses the preloaded response.
        assert f'href="{href}?v=' not in BASE_HTML, (
            f"{name} preload must not be version-qualified (would double-fetch)"
        )
        assert f"url('{href}') format('woff2')" in APP_CSS


# ── the var chains still name the self-hosted families first ────────────

def test_font_var_chains_unchanged():
    assert "--font-mono: 'JetBrains Mono'," in APP_CSS
    assert "--font-display: 'VT323'," in APP_CSS


# ── behavioural: the mount actually serves the woff2 ────────────────────

def _client(tmp: Path) -> TestClient:
    import os
    os.environ["MOTIF_TRUST_FORWARD_AUTH"] = "true"
    os.environ["MOTIF_FORWARD_AUTH_ALLOWED_IPS"] = "127.0.0.1"
    os.environ["MOTIF_CONFIG_DIR"] = str(tmp)
    os.environ["MOTIF_DATA_DIR"] = str(tmp / "data")
    from app.config import Settings
    from app.core.db import init_db
    from app.core.auth import init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp, data_dir=tmp / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)  # auth middleware queries the admin table on every request
    return TestClient(create_app(s), client=("127.0.0.1", 50000))


def test_font_files_served_by_static_mount():
    with tempfile.TemporaryDirectory() as d:
        client = _client(Path(d))
        r = client.get("/static/fonts/vt323-400.woff2")
        assert r.status_code == 200
        assert r.content[:4] == b"wOF2"
