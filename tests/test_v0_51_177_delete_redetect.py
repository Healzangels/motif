"""v0.51.177 — probe the last propagation candidate: delete the selection, then re-detect.

Where this sits. Plex ingests theme.mp3 into its own store at scan time, so mutating the
sidecar changes nothing it plays (v0.51.171, measured). Two ways to tell it otherwise:

  - refresh?force=1 — MEASURED DEAD (v0.51.173: canonical -18.7, Plex still -5.15).
  - re-upload      — works, but Plex 500s over ~10MB = 82 of 2,821 themes (v0.51.176).

This is the third: make Plex LACK a theme (delete the selection), then refresh, because
Local Media Assets ingests assets it lacks. If it works it beats re-upload on every axis
— full coverage, zero bandwidth, entry stays metadata:// rather than flipping upload://.

Two things this tag has to get right, both of which bit earlier tags:

  1. The DELETE also LOCKS the theme field (Plex's own docs, quoted in
     delete_collection_theme). A locked field is exactly what stops an agent writing it,
     so without an unlock the refresh could never work — the probe would "prove" the path
     dead when it only proved the door bolted. Hence set_theme_field_lock.

  2. The verdict must come from a RE-MEASUREMENT, never a status code. v0.51.169's undo
     and v0.51.173's refresh both reported success off the wrong layer. Worse here: the
     DELETE leaves entries in the collection with NOTHING selected, and
     _measure_plex_serving falls back to meta[0] in that state — so the loudness alone
     can't tell "re-ingested + selected" from "nothing selected, here's entry 0". Both
     halves get checked.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db
from app.core.plex import THEME_UPLOAD_CEILING_BYTES

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()


def _endpoint_src() -> str:
    """The plex-redetect handler body, bounded by the NEXT handler — not by a byte count.

    Fixed-offset slices (src[i:i+6000]) slid out of range three times this session as the
    anchored function grew, each time as a silent false pass."""
    start = API.index('@app.post("/api/admin/loudness/plex-redetect")')
    end = API.index('@app.post("/api/admin/loudness/undo-one")', start)
    return API[start:end]


# ── the unlock step exists, and is justified ─────────────────────────────

def test_plex_client_can_unlock_the_theme_field():
    plex = (REPO / "app" / "core" / "plex.py").read_text()
    assert "def set_theme_field_lock(" in plex
    i = plex.index("def set_theme_field_lock(")
    body = plex[i:plex.index("def delete_collection_theme(", i)]
    assert "theme.locked" in body
    # unlock must send "0" — sending "1" both ways would silently bolt it shut harder
    assert '"1" if locked else "0"' in body


def test_redetect_unlocks_after_deleting():
    """Order matters: the DELETE is what sets the lock, so the unlock has to follow it."""
    src = _endpoint_src()
    d = src.index("delete_theme(")
    u = src.index("set_theme_field_lock(")
    r = src.index(".refresh(")
    assert d < u < r, "delete → unlock → refresh; unlocking before the delete is a no-op"
    assert "locked=False" in src


# ── the verdict is a measurement, not a status code ──────────────────────

def test_verdict_requires_both_a_selected_entry_and_the_canonical_loudness():
    """The failure mode this closes: after the DELETE nothing is selected, and
    _measure_plex_serving falls back to meta[0]. If entry 0 were a NEW un-selected
    re-ingest, loudness alone would read as success while Plex actually plays nothing."""
    src = _endpoint_src()
    assert re.search(
        r"worked\s*=\s*bool\(entry_after\)\s*and\s*bool\(after\.get\(\"serving_normalized\"\)\)",
        src,
    ), "the verdict must require a selected entry AND the canonical loudness"


def test_verdict_never_derives_from_the_http_status_or_refresh_bool():
    src = _endpoint_src()
    body = src[src.index("worked ="):]
    for wrong in ("worked = refreshed", "worked = deleted", "worked = bool(refreshed)",
                  "worked = unlock_status"):
        assert wrong not in body
    # `refreshed` may be REPORTED, but must not decide anything.
    assert 'if refreshed' not in body


def test_stranded_is_observed_not_inferred_from_an_error():
    """v0.51.177's first cut inferred "Plex has no theme" from measure ok:False. Wrong:
    "nothing selected" returns ok:TRUE (the meta[0] fallback), and ok:False also covers
    plain transport errors. The selection is checked directly."""
    src = _endpoint_src()
    assert "stranded = entry_after is None" in src
    assert 'stranded = (not worked) and not after.get("ok")' not in src
    assert "def _selection()" in src
    assert 'm.get("selected")' in src


def test_reports_the_selection_state_so_a_failure_is_diagnosable():
    src = _endpoint_src()
    for key in ('"selected_entry_before"', '"selected_entry_after"', '"entries_after"',
                '"redetect_propagates"', '"plex_has_no_theme_now"',
                '"before_plex_loudness_i"'):
        assert key in src, f"{key} missing — a failed probe has to say WHY"


def test_failure_names_the_recovery():
    """If the delete strands the item, the operator needs to be told the way back in the
    same breath — a stranded item silently serving nothing is class-9."""
    src = _endpoint_src()
    assert "PUSH NORMALIZED TO PLEX" in src
    assert "log.warning" in src


# ── the safety net: only gamble where re-upload can recover ──────────────

def test_refuses_an_over_ceiling_theme_because_reupload_could_not_recover_it():
    """Re-upload is the ONLY undo for a stranded selection, and it 500s over ~10MB. The
    first real audition picked a 10.5MB theme — running this probe on that row would
    strand it with no way back."""
    src = _endpoint_src()
    assert "THEME_UPLOAD_CEILING_BYTES" in src
    assert "over_ceiling" in src or "OVER the upload" in src
    # the refusal must come BEFORE any mutating call
    guard = src.index("THEME_UPLOAD_CEILING_BYTES")
    assert guard < src.index("delete_theme("), "check the size before deleting anything"


def test_audition_autopick_skips_un_pushable_themes():
    """The audition picked the LOUDEST row, which was 10.5MB — over the ceiling, so the
    whole propagation half of the test was a dead end before it began."""
    i = API.index('@app.post("/api/admin/loudness/normalize-one")')
    src = API[i:API.index('@app.post("/api/admin/loudness/plex-serving")', i)]
    assert "lf.file_size IS NOT NULL" in src
    assert "lf.file_size <= " in src
    assert "THEME_UPLOAD_CEILING_BYTES" in src


def test_media_type_is_translated_for_plex_items():
    """plex_items stores PLEX's type ('show'); local_files stores motif's ('tv'). The raw
    value silently matches ZERO tv rows — same omission caught in v0.51.171."""
    src = _endpoint_src()
    assert '{"tv": "show", "collection": "collection"}' in src


def test_handler_is_offloaded_to_a_thread():
    """HTTP + ffmpeg + sleeps: blocking the loop freezes every concurrent request
    (class-12). The standing AST lint exempts nested defs, so the work lives in _run."""
    src = _endpoint_src()
    assert "def _run():" in src
    assert "return await run_in_threadpool(_run)" in src


# ── UI ───────────────────────────────────────────────────────────────────

def test_button_and_bind_exist():
    assert 'id="loud-plex-redetect-btn"' in HTML
    assert "// DELETE + RE-DETECT" in HTML
    assert "loud-plex-redetect-btn" in APP_JS
    assert "'/api/admin/loudness/plex-redetect'" in APP_JS


def test_bind_status_follows_the_measurement():
    i = APP_JS.index("const redetectBtn = document.getElementById('loud-plex-redetect-btn')")
    src = APP_JS[i:APP_JS.index("const pushBtn", i)] if "const pushBtn" in APP_JS[i:] \
        else APP_JS[i:i + 2000]
    assert "rep.redetect_propagates" in src
    assert "rep.already_current" in src


# ── behavioral: the auto-pick actually refuses un-pushable rows ──────────
# Source-text assertions alone are phantom guards (CLAUDE.md class-9 / v1.18.81): pipe
# real rows through the real endpoint and watch which one it picks.

def test_autopick_skips_over_ceiling_and_unknown_size_rows(client_and_db, monkeypatch):
    c, db = client_and_db
    ceiling = _ceiling()
    _seed_row(db, tmdb_id=1, loudness_i=-3.0, file_size=ceiling + 1)   # loudest, too big
    _seed_row(db, tmdb_id=2, loudness_i=-5.0, file_size=None)          # loud, size unknown
    _seed_row(db, tmdb_id=3, loudness_i=-9.0, file_size=1_000_000)     # quietest, pushable
    captured = {}

    def _fake(path, target, measured_i, true_peak, *, expect_sha=None):
        captured.update(measured_i=measured_i)
        return {"ok": True, "changed": False, "steps": 0, "applied_db": 0.0,
                "note": "no change", "old_sha": "sha3", "new_sha": "sha3",
                "old_pcm_sha": "pcm3", "new_i": measured_i, "new_tp": true_peak,
                "new_lra": None}

    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fake)
    r = c.post("/api/admin/loudness/normalize-one", headers=AUTH)
    assert r.json()["ok"] is True
    # -3.0 is louder, but a 10.5MB theme is exactly the dead end the first audition hit:
    # Plex 500s on the push, so there is nothing to propagate and nothing to recover with.
    assert captured["measured_i"] == -9.0


def _ceiling() -> int:
    return THEME_UPLOAD_CEILING_BYTES


@pytest.fixture
def client_and_db(tmp_path, monkeypatch):
    """Mirrors v0.51.169's fixture — same shape, same reasons."""
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


def _seed_row(db, *, tmdb_id, loudness_i, file_size):
    """A themed movie with a hardlink placement + a CURRENT measurement."""
    sha = f"sha{tmdb_id}"
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, '1979', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, loudness_measured_sha256, loudness_measured_at, "
                  " file_size) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid', ?, -2.0, ?, ?, ?)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", sha, NOW, loudness_i, sha,
                   NOW, file_size))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', 'hardlink', ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", NOW))
        c.commit()
