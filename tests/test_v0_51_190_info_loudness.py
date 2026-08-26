"""v0.51.190 — the INFO card's LOUDNESS block.

Two things are pinned here, and only one of them is about loudness.

1. THE CONTRACT the card reads. The block renders entirely from `data.local_file`,
   so what api_item puts in that payload IS the feature. A projection that quietly
   stops carrying norm_state would blank the block with no error anywhere — the
   class-9 silent failure. Exercised through the real endpoint, not by reading the
   SELECT (v1.18.81: source text alone is a phantom guard).

2. A GENERIC fmt.* lint. Writing this block I called `fmt.date(...)`, which does not
   exist — the helper is `fmt.timeAuto`. `node --check` parses it fine; it throws at
   RUNTIME and blanks the whole card, and only the operator would ever have found it.
   The remedy for a whole bug class is an additive lint, not a fix at the one site
   (docs/PROJECT_HISTORY.md, complexity audit): every `fmt.X(` in app.js must name a
   real key of the fmt object.
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


def _seed(db, *, tmdb_id=42, normalized=True):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, 'Alien', '1979', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, NOW, NOW))
        cols = ("media_type, tmdb_id, section_id, edition_key, file_path, file_sha256, "
                "downloaded_at, source_video_id, file_size, loudness_i, loudness_tp, "
                "loudness_lra, loudness_measured_sha256, loudness_measured_at")
        vals = ["movie", tmdb_id, "1", "", f"movies/{tmdb_id}/theme.mp3", "shaX", NOW,
                "vid", 1_000_000, -18.75, -10.65, 5.0, "shaX", NOW]
        if normalized:
            cols += ", norm_state, norm_gain_db, norm_target, norm_at"
            vals += ["normalized", -13.54, -18.0, NOW]
        c.execute(f"INSERT INTO local_files ({cols}) VALUES "
                  f"({','.join('?' * len(vals))})", vals)
        c.commit()


def test_api_item_carries_every_column_the_loudness_block_renders(client_and_db):
    """The card renders from data.local_file alone — so this payload IS the feature."""
    c, db = _seed_and(client_and_db)
    r = c.get("/api/items/movie/42", headers=AUTH)
    assert r.status_code == 200
    lf = r.json()["local_file"]
    assert lf is not None, "no local_file → the LOUDNESS block renders nothing at all"

    # what the block reads, field by field. A dropped column is a silent blank.
    assert lf["loudness_i"] == -18.75          # "plays at"
    assert lf["loudness_tp"] == -10.65         # "· peak"
    assert lf["norm_state"] == "normalized"    # leveled-vs-raw branch
    assert lf["norm_gain_db"] == -13.54        # "leveled" + derives the raw level
    assert lf["norm_target"] == -18.0
    assert lf["norm_at"]
    assert lf["file_size"] == 1_000_000        # the over-ceiling refusal branch

    # the row identity the LEVEL/UNDO buttons post back. Without these the buttons
    # would target the wrong row (or every edition of it).
    for k in ("media_type", "tmdb_id", "section_id", "edition_key"):
        assert k in lf, f"{k} missing → LEVEL/UNDO cannot address this row"


def test_raw_row_carries_measurement_but_no_norm_state(client_and_db):
    """A raw row must still render a level — it just takes the other branch."""
    c, db = _seed_and(client_and_db, normalized=False)
    lf = c.get("/api/items/movie/42", headers=AUTH).json()["local_file"]
    assert lf["loudness_i"] == -18.75
    assert not lf["norm_state"], "raw row must not claim to be leveled"
    # the card derives the raw level as (loudness_i - norm_gain_db); with no gain
    # there is nothing to derive and it must not print a bogus 'raw source' row.
    assert lf["norm_gain_db"] is None


def _seed_and(client_and_db, **kw):
    c, db = client_and_db
    _seed(db, **kw)
    return c, db


# ─────────────────────────── the generic lint ───────────────────────────

def test_every_fmt_helper_called_in_app_js_actually_exists():
    """`fmt.date(...)` parses, then throws at runtime and blanks the card.

    node --check cannot catch it and no test that pins markup would either. This is
    the cheap general guard: resolve every fmt.X( call against the fmt object.
    """
    i = APP_JS.index("const fmt = {")
    # bound the object by the next top-level close at the same indent — never a
    # fixed byte window (those slide silently as the file grows; bit this repo 4x).
    body = APP_JS[i:APP_JS.index("\n  };", i)]
    keys = set(re.findall(r"^\s{4}(\w+):", body, re.M))
    assert "timeAuto" in keys and "bytes" in keys, f"fmt object not parsed: {keys}"

    called = set(re.findall(r"\bfmt\.(\w+)\s*\(", APP_JS))
    missing = called - keys
    assert not missing, (
        f"app.js calls fmt.{{{', '.join(sorted(missing))}}} which the fmt object does "
        f"not define — this throws at runtime and silently blanks the surface. "
        f"Real keys: {sorted(keys)}"
    )


def test_loudness_block_is_wired_into_the_card():
    """Cheap wiring pin: the group must actually be interpolated into the card.

    Deliberately narrow — it asserts the block is REACHABLE, not what it renders.
    """
    # v0.51.289 (design audit): loudness renders as a reference FOLD now
    # (open only while actionable) — same reachability pin, new shape.
    # v0.51.290 (ultra review): pin the INTERPOLATION — a bare mention
    # would satisfy a call that was never rendered into the template.
    assert "${_fold('loudness', _loudnessRows" in APP_JS
    assert "data-act=\"loud-normalize\"" in APP_JS
    assert "data-act=\"loud-undo\"" in APP_JS
