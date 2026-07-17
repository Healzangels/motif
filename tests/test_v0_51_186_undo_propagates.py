"""v0.51.186 — undo has to put PLEX back, not just the file.

The gap, found in the operator's own library rather than reasoned about. rk 261711 was
sitting at: file −5.2 (undone), motif's DB saying raw, and Plex still serving the −18.75
upload from the earlier push. Undo reverted the bytes and told Plex nothing, so the row
was DIVERGED — and the next `// NORMALIZE LOUDEST THEME` grabbed it as "loudest raw row"
and normalized it straight back onto bytes Plex already had, which is why that audition
read `before_plex_loudness_i: -18.75` and proved nothing.

Why re-select rather than push the restored file: mp3gain leaves an APE tag, so the
restored file's hash differs from the original's. Pushing it mints a THIRD entry (Plex
keys entries by content hash) and every normalize/undo cycle adds another. Fetching the
recorded pre-normalize entry's bytes and re-POSTing them content-dedupes back onto it
(v1.18.36) — Plex ends on the exact entry it started on, no residue.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = datetime.now(timezone.utc).isoformat(timespec="seconds")
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
HTML = (REPO / "app" / "web" / "templates" / "settings.html").read_text()
ORIG_ENTRY = "metadata://themes/original"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    themes = tmp_path / "themes"
    (themes / "movies" / "1").mkdir(parents=True)
    (themes / "movies" / "1" / "theme.mp3").write_bytes(b"ID3RESTORED" * 40)
    monkeypatch.setattr(Settings, "themes_dir", property(lambda self: themes))
    monkeypatch.setattr(Settings, "plex_url", property(lambda self: "http://plex.test"))
    monkeypatch.setattr(Settings, "plex_token", property(lambda self: "tok"))
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    return TestClient(create_app(s)), s.db_path


def _normalized(db, *, entry_uri=ORIG_ENTRY):
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key, "
                  " file_path, file_sha256, downloaded_at, source_video_id, loudness_i, "
                  " loudness_tp, file_size, norm_state, norm_gain_db, norm_at, "
                  " norm_orig_sha256, norm_orig_pcm_sha256, norm_plex_entry_uri) "
                  "VALUES ('movie', 1, '1', '', 'movies/1/theme.mp3', 'newsha', ?, 'vid', "
                  " -18.75, -10.0, 900000, 'normalized', -13.5, ?, 'oldsha', 'oldpcm', ?)",
                  (NOW, NOW, entry_uri))
        c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, title, "
                  " guid_tmdb, edition_key, has_theme, first_seen_at, last_seen_at) "
                  "VALUES ('261711', 'movie', '1', 'Endo', 1, '', 1, ?, ?)", (NOW, NOW))
        c.commit()


def _stub(monkeypatch, *, fetch_ok=True, upload_ok=True, plex_i=-5.2):
    from app.web import api as api_mod
    st = {"posted": None, "fetched": None, "serving": -18.75}

    class _FakePlex:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def fetch_theme_bytes(self, *, item_rating_key, entry_uri):
            st["fetched"] = entry_uri
            if not fetch_ok:
                return {"ok": False, "http_status": 404, "bytes": None, "error": "gone"}
            return {"ok": True, "http_status": 200, "bytes": b"ORIGINALBYTES",
                    "error": None}

        def upload_theme(self, *, rating_key, audio_bytes, content_type="audio/mpeg"):
            st["posted"] = audio_bytes
            if not upload_ok:
                return (False, 500, "nope")
            # what Plex plays follows WHAT WAS POSTED, which is the whole point: posting
            # the recorded entry's bytes lands on `plex_i` (whatever that entry holds —
            # possibly still normalized, as rk 261711's was), while posting the RESTORED
            # FILE lands on the restored loudness. A stub that ignored the difference
            # could not tell the fallback working from the fallback firing.
            st["serving"] = -5.2 if audio_bytes != b"ORIGINALBYTES" else plex_i
            return (True, 200, "")

        def upload_collection_theme(self, **k):
            return self.upload_theme(**k)

        def get_themes(self, *, rating_key):
            return {"ok": True, "http_status": 200, "error": None, "body": {
                "MediaContainer": {"Metadata": [
                    {"ratingKey": ORIG_ENTRY, "selected": True}]}}}

    monkeypatch.setattr(api_mod, "PlexClient", _FakePlex)
    monkeypatch.setattr(api_mod, "_measure_plex_serving",
                        lambda s, *, rk, canonical_i, norm_gain_db=None: {
                            "ok": True, "rating_key": rk, "plex_loudness_i": st["serving"],
                            "canonical_loudness_i": canonical_i,
                            "serving_normalized": abs(st["serving"] - canonical_i) < 0.75})
    monkeypatch.setattr("app.core.loudness_apply.undo_file",
                        lambda p, expect_sha=None, expect_pcm_sha=None: {
                            "ok": True, "audio_restored": True, "file_bit_exact": False,
                            "new_sha": "restoredsha", "new_i": -5.2, "new_tp": 2.9,
                            "new_lra": 5.0, "error": None})
    return st


def _undo(c):
    return c.post("/api/admin/loudness/undo-one", headers=AUTH, json={
        "media_type": "movie", "tmdb_id": 1, "section_id": "1", "edition_key": ""}).json()


# ── the fix ──────────────────────────────────────────────────────────────

def test_undo_re_selects_the_recorded_entry_rather_than_pushing_the_file(client,
                                                                        monkeypatch):
    """The APE tag makes the restored file's hash differ, so pushing it would mint a THIRD
    entry and every cycle would add another. Re-post the ORIGINAL entry's bytes instead —
    content-dedup puts Plex back on the entry it started on."""
    c, db = client
    _normalized(db)
    st = _stub(monkeypatch)

    b = _undo(c)
    assert b["ok"] is True
    assert b["recorded_entry_uri"] == ORIG_ENTRY
    assert st["fetched"] == ORIG_ENTRY, "must fetch the RECORDED entry's bytes"
    assert st["posted"] == b"ORIGINALBYTES", "must re-post those, not the restored file"
    assert b["plex_is_serving_the_restore"] is True


def test_undo_that_leaves_plex_on_the_normalized_copy_is_not_a_success(client,
                                                                      monkeypatch):
    """THE BUG. Reverting the file while Plex keeps serving the normalized upload is the
    diverged state rk 261711 was found in — motif said raw, the file was raw, Plex played
    normalized. A green ✓ there is a lie."""
    c, db = client
    _normalized(db)
    _stub(monkeypatch, upload_ok=False)

    b = _undo(c)
    assert b["audio_restored"] is True          # the FILE came back
    assert b["plex_is_serving_the_restore"] is False   # ...and Plex did not
    assert b["plex_restored"]["plex_matches_restored_file"] is False


def test_a_row_with_no_recorded_entry_falls_back_and_says_so(client, monkeypatch):
    """Rows normalized before v0.51.185 never recorded the entry. Pushing the restored
    file mints a new entry instead of returning to the original — worse, but honest, and
    silently skipping would leave Plex on the loud copy forever."""
    c, db = client
    _normalized(db, entry_uri=None)
    st = _stub(monkeypatch)

    b = _undo(c)
    assert b["recorded_entry_uri"] is None
    assert st["fetched"] is None, "nothing to fetch — there's no recorded entry"
    assert "no pre-normalize entry was recorded" in b["plex_restored"]["method"]
    assert b["plex_is_serving_the_restore"] is True


def test_undo_clears_the_recorded_entry_with_the_rest_of_the_normalize_state(client,
                                                                            monkeypatch):
    c, db = client
    _normalized(db)
    _stub(monkeypatch)
    _undo(c)
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT norm_state, norm_plex_entry_uri FROM local_files"
                           ).fetchone()
    assert row == (None, None), "a raw row must not keep normalize state"


# ── the UI must not promise what Plex isn't doing ────────────────────────

def test_audition_status_leads_with_whether_plex_got_it():
    """v0.51.185 made normalize propagate, but the status line still read '-5.2 → -18.8,
    hear it in Plex' whether or not the push landed — promising something inaudible."""
    i = APP_JS.index("const heard = rep.plex_is_serving_it")
    src = APP_JS[i:i + 900]
    assert "BUT PLEX IS NOT SERVING IT" in src
    assert "form-status-fail" in src


def test_undo_status_reports_plex_too():
    i = APP_JS.index("const plexBack = rep.plex_is_serving_the_restore")
    src = APP_JS[i:i + 700]
    assert "STILL SERVING THE NORMALIZED COPY" in src


# ── answered probes are retired, not left lying around ───────────────────

def test_the_answered_probes_are_gone(client):
    c, _ = client
    for route in ("/api/admin/mp3gain-probe", "/api/admin/plex/unselected-serves-probe"):
        assert route not in API
        assert c.post(route, headers=AUTH).status_code == 404
    assert "// PROBE MP3GAIN" not in HTML
    assert "// DOES AN UNSELECTED ENTRY PLAY?" not in HTML
    assert "mp3gain-probe-btn" not in APP_JS
    assert "loud-unselected-btn" not in APP_JS


def test_the_working_tools_survive():
    """Propagation and its verification are production now, not probes."""
    assert "// PUSH NORMALIZED TO PLEX" in HTML
    assert "// WHAT IS PLEX SERVING?" in HTML
    assert "// NORMALIZE LOUDEST THEME" in HTML
    assert "// UNDO" in HTML


# ── v0.51.187: detecting the divergence isn't enough — fix it ────────────

def test_undo_falls_back_to_pushing_when_the_recorded_entry_does_not_match(client,
                                                                          monkeypatch):
    """THE REAL-LIBRARY CASE. rk 261711's recorded entry was ITSELF a normalized upload —
    Plex was already serving it when that normalize ran — so re-selecting it restored Plex
    to -18.75 while the file went to -5.2. v0.51.186 detected that and stopped, leaving
    the row diverged; the loudest-raw auto-pick then grabbed it straight back and pushed
    bytes Plex already had. That loop is why the propagation test could never run.

    A new entry is a smaller price than a row that never converges."""
    c, db = client
    _normalized(db)
    # the recorded entry serves -18.75: re-selecting it will NOT match the -5.2 restore
    st = _stub(monkeypatch, plex_i=-18.75)

    b = _undo(c)
    assert st["fetched"] == ORIG_ENTRY, "it must TRY the cheap re-select first"
    assert b["plex_restored"]["fell_back_to_push"] is True
    assert "did not match the restored file" in b["plex_restored"]["method"]
    assert b["plex_is_serving_the_restore"] is True, (
        "the fallback must actually converge the row, not just report the problem")


def test_the_cheap_path_is_still_preferred_when_it_works(client, monkeypatch):
    """The fallback must not become the default — re-selecting adds no entry, pushing does."""
    c, db = client
    _normalized(db)
    _stub(monkeypatch, plex_i=-5.2)          # the re-select lands on the restored loudness

    b = _undo(c)
    assert b["plex_restored"].get("fell_back_to_push") is not True
    assert b["plex_restored"]["method"] == "re-selected the pre-normalize entry"
    assert b["plex_is_serving_the_restore"] is True
