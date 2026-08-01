"""v0.51.207 — the LOUDNESS info-card presentation pass.

Five user-reported polish items, verified where it counts:

1. Clearer "raw" copy — the info-card line no longer says the confusing "raw source
   loudness"; it says the theme "plays at its original recorded level".
2. Clipping is explained — the (clipping) marker carries a title that says what it is
   AND what to do (level it quieter).
3. The squished controls moved into a full-width .loud-controls block.
4. The at-a-glance loudness chip beside the 4K badge.
5. The chip + the library row markers share ONE server-side derivation
   (_loudness_marker) so they can never disagree — the SRC-axis cross-surface rule.

The chip's DATA (local_file.loudness_marker) is exercised through the real endpoint —
that payload IS the chip, so a projection that stops carrying it would silently blank it
(class-9). The copy/markup shape is pinned by source text (cheap regression guards).
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ───────────────────────── the shared marker helper ─────────────────────────

def test_loudness_marker_helper_covers_every_state():
    from app.web.api import _loudness_marker
    thr = -12.0   # target(-18) + margin(6)
    assert _loudness_marker("normalized", True, -5.0, thr) == "leveled"
    assert _loudness_marker("normalized", False, None, thr) == "leveled"  # leveled wins
    assert _loudness_marker(None, True, -5.0, thr) == "outlier"           # above thresh
    assert _loudness_marker(None, True, -25.0, thr) == "raw"              # below thresh
    assert _loudness_marker(None, True, None, thr) == "raw"               # unmeasured raw
    assert _loudness_marker(None, False, -5.0, thr) is None               # no local file


def test_both_surfaces_derive_the_marker_through_the_one_helper():
    """The library row loop AND api_item's local_file payload must both call the shared
    helper — not re-implement the raw/outlier/leveled rule. Mirror-drift guard."""
    calls = API_PY.count("_loudness_marker(")
    # 1 def + ≥2 call sites (library loop + info payload).
    assert "def _loudness_marker(" in API_PY
    assert calls >= 3, f"expected the helper defined + used at both surfaces, saw {calls} refs"


# ───────────────────────── the chip DATA contract ─────────────────────────

@pytest.fixture
def client_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    themes.mkdir(exist_ok=True)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime,"
                  " is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, tmdb_id, loudness_i, normalized=False):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, 'T', '2000', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, NOW, NOW))
        cols = ("media_type, tmdb_id, section_id, edition_key, file_path, file_sha256, "
                "downloaded_at, source_video_id, file_size, loudness_i, loudness_tp, "
                "loudness_lra, loudness_measured_sha256, loudness_measured_at")
        vals = ["movie", tmdb_id, "1", "", f"movies/{tmdb_id}/theme.mp3", "shaX", NOW,
                "vid", 1_000_000, loudness_i, -6.0, 5.0, "shaX", NOW]
        if normalized:
            cols += ", norm_state, norm_gain_db, norm_target, norm_at"
            vals += ["normalized", -13.5, -18.0, NOW]
        c.execute(f"INSERT INTO local_files ({cols}) VALUES "
                  f"({','.join('?' * len(vals))})", vals)
        c.commit()


def _marker(client, tmdb_id):
    r = client.get(f"/api/items/movie/{tmdb_id}", headers=AUTH)
    assert r.status_code == 200
    return r.json()["local_file"]["loudness_marker"]


def test_api_item_stamps_the_marker_for_each_state(client_and_db):
    c, db = client_and_db
    _seed(db, tmdb_id=1, loudness_i=-25.0)                 # quiet raw
    _seed(db, tmdb_id=2, loudness_i=-5.0)                  # loud → outlier (> -18+6)
    _seed(db, tmdb_id=3, loudness_i=-18.0, normalized=True)  # leveled
    assert _marker(c, 1) == "raw"
    assert _marker(c, 2) == "outlier"
    assert _marker(c, 3) == "leveled"


# ───────────────────────── the copy + markup shape ─────────────────────────

def test_raw_copy_is_plain_language_not_jargon():
    assert "plays at its original recorded level" in APP_JS
    assert "no — raw source loudness" not in APP_JS, "the confusing old copy is gone"


def test_clipping_has_an_explainer():
    assert 'class="accent-red loud-clip"' in APP_JS
    # the title must explain BOTH what clipping is and what to do about it.
    m = re.search(r'loud-clip"\s+title="([^"]+)"', APP_JS)
    assert m, "the (clipping) span must carry a title"
    ttl = m.group(1)
    assert "clip" in ttl.lower() and "LEVEL THIS THEME" in ttl


def test_controls_moved_into_their_own_block():
    assert 'class="loud-controls"' in APP_JS
    # v0.51.243: the block is no longer full-width. Spanning both columns is what made the
    # card "look off" — it stranded the <dt> in the label column and shifted the controls
    # ~156px left of every other value. De-squishing is carried by the two rules below,
    # not by the span (measured: 523px of value column vs 366px of buttons at 720px).
    assert ".loud-controls { grid-column: 2;" in APP_CSS
    # the +/- steppers drop the .btn-tiny 72px min-width.
    assert ".loud-stepper .btn-tiny" in APP_CSS
    # and the row wraps rather than overflowing when the column IS narrow (375px).
    assert ".loud-ctl-row { display: flex; flex-wrap: wrap;" in APP_CSS
    # every wiring hook the handlers bind to must survive the re-layout.
    for hook in ('id="loud-target"', 'id="loud-gain-note"', 'id="loud-result"',
                 'id="loud-preview-note"', 'id="loud-preview-audio"',
                 'data-act="loud-step"', 'data-act="loud-preview"',
                 'data-act="loud-normalize"', 'data-act="loud-undo"',
                 'class="loud-preview-player"'):
        assert hook in APP_JS, f"lost wiring hook: {hook}"


def test_loudness_chip_rides_beside_the_4k_badge():
    # the chip is interpolated into the info title, right after the 4K badge.
    assert "${_loudChip}</h3>" in APP_JS
    # it reuses the FIXED loudness-state colours (theme SPLIT — colour encodes meaning).
    for cls in ("tier-badge-lvl", "tier-badge-loud", "tier-badge-raw"):
        assert cls in APP_JS
    for lbl in ("LEVELED", "LOUD", "RAW"):
        assert f"'{lbl}'" in APP_JS


def test_new_css_vars_resolve():
    """Every var(--X) in the new .loud-* rules must be a defined token (v0.51.200 guard)."""
    root = APP_CSS[APP_CSS.index(":root"):APP_CSS.index("}", APP_CSS.index(":root"))]
    defined = set(re.findall(r"(--[\w-]+)\s*:", root))
    # the rules this tag added — bounded to the .loud-gain-note rule's own close, so it
    # doesn't bleed into the adjacent .tier-badge-lvl (which legitimately uses --cyan-rgb).
    _seg_end = APP_CSS.index("}", APP_CSS.index(".loud-gain-note {"))
    seg = APP_CSS[APP_CSS.index(".loud-controls {"):_seg_end]
    used = set(re.findall(r"var\((--[\w-]+)", seg))
    missing = used - defined
    assert not missing, f"new .loud-* CSS references undefined vars: {missing}"
