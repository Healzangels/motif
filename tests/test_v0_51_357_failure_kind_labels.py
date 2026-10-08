"""v0.51.357 — every failure kind has words, on every surface that shows one.

`FailureKind` gained RATE_LIMITED in v0.51.269 (classified on HTTP 429 / "too many requests", and
`worker.py` persists `kind.value` like any other kind). The enum's own `.human` covered it. The four
label maps in the web layer did not — each still listed the pre-.269 seven and fell back to the raw
token, so a throttled row read `rate_limited` in the dashboard's FAILURE BREAKDOWN bar, in the row's
⚠ glyph tooltip, in the TDB pill tooltip and in the INFO card's recovery headline.

The enum's own docstring states the principle that was missed — v0.51.269 hoisted the
probe-inconclusive set onto the enum precisely "so a new kind cannot be added without inheriting an
answer". The label maps never got that treatment. This walks the enum against all of them, so the
ninth kind cannot repeat the eighth's mistake.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.downloader import FailureKind

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
AUTH = {"X-Authentik-Username": "testadmin"}
KINDS = [k.value for k in FailureKind]


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@pytest.fixture
def seeded(tmp_path, monkeypatch):
    """One failed movie per failure kind — the whole enum, on screen at once."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    settings._cfg.paths.themes_dir = str(tmp_path / "themes")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    now = _now()
    with sqlite3.connect(settings.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id,title,type,is_anime,is_4k,themes_subdir,included,"
                  "discovered_at,last_seen_at) VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (now, now))
        for i, kind in enumerate(KINDS):
            tmdb = 900 + i
            tid = c.execute("INSERT INTO themes (media_type,tmdb_id,title,upstream_source,last_seen_sync_at,"
                            "first_seen_sync_at,youtube_url,failure_kind,failure_message,failure_at)"
                            " VALUES ('movie',?,?,'imdb',?,?,?,?,?,?)",
                            (tmdb, f"Film {kind}", now, now, "https://www.youtube.com/watch?v=x0000000001",
                             kind, "yt-dlp said no", now)).lastrowid
            c.execute("INSERT INTO plex_items (rating_key,section_id,media_type,theme_id,guid_tmdb,title,year,"
                      "edition_key,folder_path,has_theme,local_theme_file,plex_independent_theme,"
                      "plex_theme_verified_ok,first_seen_at,last_seen_at)"
                      " VALUES (?,'1','movie',?,?,?,2020,'',?,0,0,0,1,?,?)",
                      (f"rk{tmdb}", tid, tmdb, f"Film {kind}", f"/media/{tmdb}", now, now))
        c.commit()
    return TestClient(create_app(settings)), settings.db_path


# ── the surfaces, driven for real ────────────────────────────


def test_the_dashboard_bar_names_every_kind_in_words(seeded):
    """The FAILURE BREAKDOWN bar's label comes from the server. A missing kind rendered its own
    snake_case value as the caption."""
    client, _ = seeded
    body = client.get("/api/dashboard/insights", headers=AUTH).json()
    labels = {row["kind"]: row["label"] for row in body["failures"]}
    assert set(labels) == set(KINDS), (set(KINDS) - set(labels), "every seeded kind must appear")
    for kind, label in labels.items():
        assert label and label != kind, f"{kind} has no label of its own — the map fell through"
        assert "_" not in label, f"{kind} rendered a raw token: {label!r}"


@pytest.mark.parametrize("kind", KINDS)
def test_the_recovery_headline_names_every_kind_in_words(seeded, kind):
    """The INFO card's "// TRY THIS NEXT" headline. This map used to be hand-maintained AND two
    generations stale in voice ("YouTube cookies…" after v1.14.1 made every other surface
    source-agnostic); it now reads FailureKind.human."""
    client, _ = seeded
    tmdb = 900 + KINDS.index(kind)
    r = client.get(f"/api/items/movie/{tmdb}/recovery-options?section_id=1", headers=AUTH)
    assert r.status_code == 200, r.text
    human = r.json()["human"]
    assert human == FailureKind(kind).human, (kind, human)
    assert "_" not in human and human != kind, (kind, human)


def test_the_recovery_headline_speaks_the_source_agnostic_voice(seeded):
    """v1.14.1/v1.14.4: nothing user-facing says "YouTube" for a failure any more — a SoundCloud or
    AnimeThemes download fails the same ways."""
    client, _ = seeded
    for kind in KINDS:
        tmdb = 900 + KINDS.index(kind)
        human = client.get(f"/api/items/movie/{tmdb}/recovery-options?section_id=1",
                           headers=AUTH).json()["human"]
        assert "youtube" not in human.lower(), (kind, human)


# ── the two client-side maps ─────────────────────────────────


def _js_map_keys(anchor: str) -> set[str]:
    """The quoted keys of the object literal that starts at `anchor`."""
    start = APP_JS.index(anchor)
    end = APP_JS.index("}", start)
    return set(re.findall(r"'([a-z_]+)':", APP_JS[start:end]))


@pytest.mark.parametrize("anchor,what", [
    ("const human = {", "the row ⚠ glyph tooltip"),
    ("const kindHuman = {", "the TDB pill tooltip"),
])
def test_both_client_maps_know_every_kind(anchor, what):
    keys = _js_map_keys(anchor)
    missing = set(KINDS) - keys
    assert not missing, f"{what} has no label for {sorted(missing)} — it will render the raw kind"
