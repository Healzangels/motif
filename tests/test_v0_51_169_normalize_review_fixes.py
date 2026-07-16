"""v0.51.169 — code-review follow-ups on the v0.51.168 normalize path.

Six fixes, guarded here. The two that mattered:

  1. STALE MEASUREMENT — normalize-one derived gain from local_files.loudness_i without
     checking it was measured at the CURRENT bytes. A re-download since the audit leaves
     loudness_i stale, so the computed gain is wrong (worst case: a big BOOST onto a file
     that is actually already loud). The staleness key is the same one
     loudness_audit.rows_needing_measure uses: loudness_measured_sha256 == file_sha256.
     Guarded in SQL (auto-pick), in Python (body-named row), and at the leaf
     (normalize_file(expect_sha=...) re-hashes the bytes and refuses on mismatch).

  2. RELOAD STRANDED A NORMALIZED THEME — the undo target lived only in a JS variable, so
     a page reload hid // UNDO while the theme stayed normalized. GET
     /api/admin/loudness/normalized lets the UI re-arm from the DB.

Plus: the race guard on the UPDATE, the non-dict body 500, and normalize_file's
"never raises" contract vs a None measured_i.

Behavioral where it counts — these drive the real endpoint against a real DB, not the
source text (CLAUDE.md's phantom-fix lesson: v1.18.78 pinned a JS conditional that was
fed by an endpoint which never returned the field).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core import loudness_apply as la
from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    # themes_dir is None until configured (config.py); the endpoints guard that, but the
    # happy-path tests need it set — canonical paths are RELATIVE to it (CLAUDE.md).
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
    return TestClient(create_app(s)), s.db_path, s


def _seed(db, *, tmdb_id, loudness_i, file_sha, measured_sha, norm_state=None,
          kind="hardlink", file_size=1_000_000):
    """One themed movie with a hardlink placement + a loudness measurement.

    v0.51.177 added file_size to the auto-pick's eligibility (a NULL size is an UNKNOWN,
    not a small file — same rule as the v0.51.176 cohort count), so the seed has to carry
    one or every row here is ineligible."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?, 'movie', ?, ?, '1979', 'imdb', ?, ?)",
                  (tmdb_id, tmdb_id, f"Movie{tmdb_id}", NOW, NOW))
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, loudness_measured_sha256, loudness_measured_at, "
                  " norm_state, file_size) "
                  "VALUES ('movie', ?, '1', '', ?, ?, ?, 'vid', ?, -2.0, ?, ?, ?, ?)",
                  (tmdb_id, f"movies/{tmdb_id}/theme.mp3", file_sha, NOW, loudness_i,
                   measured_sha, NOW, norm_state, file_size))
        c.execute("INSERT INTO placements (media_type, tmdb_id, section_id, media_folder, "
                  " edition_key, placement_kind, placed_at) "
                  "VALUES ('movie', ?, '1', ?, '', ?, ?)",
                  (tmdb_id, f"/data/movies/{tmdb_id}", kind, NOW))
        c.commit()


# ── fix 1: stale measurement is not normalizable ─────────────────────────────

def test_autopick_skips_row_whose_measurement_is_stale(client, monkeypatch):
    """A theme re-downloaded since the audit (file_sha256 moved on, loudness_i didn't)
    must NOT be auto-picked — its gain would be computed from bytes that no longer exist."""
    c, db, _ = client
    # loudest row, but its measurement was taken at DIFFERENT bytes → stale
    _seed(db, tmdb_id=1, loudness_i=-3.0, file_sha="new", measured_sha="old")
    r = c.post("/api/admin/loudness/normalize-one", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert "no eligible theme" in body["error"]


def test_autopick_prefers_loudest_row_with_a_CURRENT_measurement(client, monkeypatch):
    """The stale-but-louder row is skipped in favour of the quieter CURRENT one."""
    c, db, _ = client
    _seed(db, tmdb_id=1, loudness_i=-3.0, file_sha="new", measured_sha="old")   # stale
    _seed(db, tmdb_id=2, loudness_i=-9.0, file_sha="same", measured_sha="same")  # current
    captured = {}

    def _fake(path, target, measured_i, true_peak, *, expect_sha=None):
        captured.update(measured_i=measured_i, expect_sha=expect_sha)
        return {"ok": True, "changed": False, "steps": 0, "applied_db": 0.0,
                "note": "no change", "old_sha": "same", "new_sha": "same",
                "old_pcm_sha": "pcm-same",
                "new_i": measured_i, "new_tp": true_peak, "new_lra": None}

    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fake)
    r = c.post("/api/admin/loudness/normalize-one", headers=AUTH)
    assert r.json()["ok"] is True
    assert captured["measured_i"] == -9.0            # the current row, not the loud stale one
    assert captured["expect_sha"] == "same"          # sha plumbed to the leaf guard


def test_normalize_file_refuses_when_bytes_do_not_match_expect_sha(tmp_path, monkeypatch):
    """Leaf guard: even if the DB looks current, the bytes on disk must hash to the sha the
    measurement was taken at — else an out-of-band replace drives the gain off a stale read."""
    calls = []
    monkeypatch.setattr(la, "apply_gain", lambda p, s, timeout=None: calls.append(s) or True)
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3audio" * 50)

    res = la.normalize_file(theme, -18.0, -14.5, -2.0, expect_sha="a-stale-sha")
    assert res["ok"] is False
    assert "sha mismatch" in res["error"]
    assert calls == []                                # mp3gain never ran


def test_normalize_file_accepts_matching_expect_sha(tmp_path, monkeypatch):
    monkeypatch.setattr(la, "apply_gain", lambda p, s, timeout=None: True)
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": -18.0, "true_peak": -5.0,
                                            "lra": 6.0})
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3audio" * 50)
    real = la._sha256(theme)

    res = la.normalize_file(theme, -18.0, -14.5, -2.0, expect_sha=real)
    assert res["ok"] is True
    assert res["changed"] is True


# ── fix 2: a normalized theme is re-armable after a reload ───────────────────

def test_normalized_lookup_reports_the_normalized_row(client):
    c, db, _ = client
    _seed(db, tmdb_id=7, loudness_i=-18.0, file_sha="s", measured_sha="s",
          norm_state="normalized")
    with sqlite3.connect(db) as x:
        x.execute("UPDATE local_files SET norm_gain_db=-3.01, norm_target=-18.0, "
                  "norm_at=? WHERE tmdb_id=7", (NOW,))
        x.commit()
    r = c.get("/api/admin/loudness/normalized", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 1
    assert body["normalized"]["row"] == {"media_type": "movie", "tmdb_id": 7,
                                         "section_id": "1", "edition_key": ""}
    assert body["normalized"]["norm_gain_db"] == -3.01


def test_normalized_lookup_is_null_when_nothing_is_normalized(client):
    c, db, _ = client
    _seed(db, tmdb_id=8, loudness_i=-14.0, file_sha="s", measured_sha="s")
    r = c.get("/api/admin/loudness/normalized", headers=AUTH)
    assert r.json() == {"normalized": None, "count": 0}


def test_ui_rearms_undo_from_server_state_on_load():
    """The audition must not depend on an in-memory variable surviving a reload."""
    assert "refreshNormalizedState" in APP_JS
    assert "/api/admin/loudness/normalized'" in APP_JS
    # armUndo is driven by the fetched row, and called at bind time (not only on click)
    i = APP_JS.index("function bindLoudnessAudition")
    block = APP_JS[i:i + 4500]
    assert "refreshNormalizedState();" in block


# ── fix 3: the race guard lives on the WRITE ─────────────────────────────────

def test_normalize_update_is_guarded_by_norm_state_is_null(client, monkeypatch):
    """A second normalize that lost the race must not overwrite norm_orig_sha256 with the
    already-normalized sha (which would make a correct undo report bit_exact=false)."""
    c, db, _ = client
    _seed(db, tmdb_id=3, loudness_i=-14.5, file_sha="orig", measured_sha="orig")

    def _fake(path, target, measured_i, true_peak, *, expect_sha=None):
        return {"ok": True, "changed": True, "steps": -2, "applied_db": -3.01,
                "note": None, "old_sha": "orig", "new_sha": "gained",
                "old_pcm_sha": "pcm-orig",
                "new_i": -18.0, "new_tp": -5.0, "new_lra": 6.0}

    monkeypatch.setattr("app.core.loudness_apply.normalize_file", _fake)
    first = c.post("/api/admin/loudness/normalize-one",
                   json={"media_type": "movie", "tmdb_id": 3,
                         "section_id": "1", "edition_key": ""}, headers=AUTH)
    assert first.json()["ok"] is True
    with sqlite3.connect(db) as x:
        orig_sha = x.execute("SELECT norm_orig_sha256 FROM local_files "
                             "WHERE tmdb_id=3").fetchone()[0]
    assert orig_sha == "orig"     # the TRUE pre-normalize sha is recorded
    with sqlite3.connect(db) as x:
        pcm = x.execute("SELECT norm_orig_pcm_sha256 FROM local_files "
                        "WHERE tmdb_id=3").fetchone()[0]
    assert pcm == "pcm-orig"      # v0.51.170: the AUDIO reference undo verifies against

    # a second call on the same row is refused by the pre-check; the UPDATE's
    # `AND norm_state IS NULL` is the backstop if two ever interleave.
    second = c.post("/api/admin/loudness/normalize-one",
                    json={"media_type": "movie", "tmdb_id": 3,
                          "section_id": "1", "edition_key": ""}, headers=AUTH)
    assert second.json()["ok"] is False
    assert "already normalized" in second.json()["error"]
    with sqlite3.connect(db) as x:
        still = x.execute("SELECT norm_orig_sha256 FROM local_files "
                          "WHERE tmdb_id=3").fetchone()[0]
    assert still == "orig"        # unchanged — the true original survives


def test_update_carries_the_norm_state_guard():
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    i = api_py.index('@app.post("/api/admin/loudness/normalize-one")')
    block = api_py[i:i + 9000]
    assert "AND norm_state IS NULL" in block
    assert "rowcount == 0" in block


# ── fix 4/5/6: body robustness, leaf contract, one timestamp ─────────────────

def test_non_dict_json_body_does_not_500(client, monkeypatch):
    """A list/str body used to AttributeError on body.get → 500 instead of {ok:false}."""
    c, db, _ = client
    _seed(db, tmdb_id=4, loudness_i=-14.5, file_sha="s", measured_sha="s")
    monkeypatch.setattr(
        "app.core.loudness_apply.normalize_file",
        lambda *a, **k: {"ok": True, "changed": False, "steps": 0, "applied_db": 0.0,
                         "note": "no change", "old_sha": "s", "new_sha": "s",
                         "old_pcm_sha": "pcm-s",
                         "new_i": -14.5, "new_tp": -2.0, "new_lra": None})
    for bad in ([1, 2], "nope", 5):
        r = c.post("/api/admin/loudness/normalize-one", json=bad, headers=AUTH)
        assert r.status_code == 200, f"{bad!r} → {r.status_code}: {r.text}"
        assert isinstance(r.json(), dict)
    r = c.post("/api/admin/loudness/undo-one", json=[1, 2], headers=AUTH)
    assert r.status_code == 200
    assert r.json()["ok"] is False


def test_unconfigured_themes_dir_errors_cleanly_instead_of_500(client, monkeypatch):
    """Found while writing these tests: settings.themes_dir is None until configured
    (config.py: "None if not yet set on first run"), and `themes_dir / file_path` raised
    TypeError → 500. A first-run install hitting // AUDITION NORMALIZE should get a clean
    'configure themes_dir' message, not a traceback."""
    from app.config import Settings
    c, db, _ = client
    _seed(db, tmdb_id=9, loudness_i=-14.5, file_sha="s", measured_sha="s")
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: None))
    r = c.post("/api/admin/loudness/normalize-one", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is False
    assert "themes_dir is not configured" in r.json()["error"]


def test_normalize_file_never_raises_on_none_measurement(tmp_path):
    """The docstring promises "never raises"; `target - None` used to raise TypeError
    before any guard ran, so the contract was enforced by the caller, not the leaf."""
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3audio" * 50)
    res = la.normalize_file(theme, -18.0, None, -2.0)
    assert res["ok"] is False
    assert "no loudness measurement" in res["error"]


def test_one_timestamp_per_operation():
    api_py = (REPO / "app" / "web" / "api.py").read_text()
    i = api_py.index('@app.post("/api/admin/loudness/normalize-one")')
    block = api_py[i:i + 9000]
    assert "ts = now_iso()" in block
    # the UPDATE binds the single ts, not two independent now_iso() calls
    assert block.count("now_iso()") == 1
