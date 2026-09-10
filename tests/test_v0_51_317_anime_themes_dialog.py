"""v0.51.317 — AnimeThemes tag 3: the `// ANIME THEMES` picker dialog.

docs/specs/ANIMETHEMES_SPEC.md §3.5. Decisions taken: a DIALOG (one surface
for the row menu and the INFO card, like SET URL); rows that already have a
theme pre-tick KEEP AS BACKUP so a pick lands as a revision.
"""
from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import animethemes as at  # noqa: E402

AT_LINK = "https://a.animethemes.moe/CowboyBebop-OP1.ogg"


def _res(conf="clean", with_default=True):
    info = at.AnimeInfo(anime_id=521, name="Cowboy Bebop", year=1998, season="Spring", slug="cowboy_bebop")
    au = at.Audio(link=AT_LINK, size=3725835, version=1, source="BD", nc=True, nsfw=False)
    op = at.Theme(slug="OP1", type="OP", sequence=None, audio=(au,))
    ed = at.Theme(slug="ED1", type="ED", sequence=1, audio=(at.Audio(link=AT_LINK.replace("OP1", "ED1"), size=100, version=None, source="WEB", nc=False, nsfw=False),))
    silent = at.Theme(slug="ED2", type="ED", sequence=2, audio=())
    sm = at.SeasonMatch(season=1, anidb=23, info=info, themes=(op, ed, silent))
    return at.Resolution(conf, "tvdb", "season-1 entry, year agrees", [sm], (sm, op, au) if with_default else None)


# ── wire shape ───────────────────────────────────────────────


def test_resolution_to_json_keeps_order_and_points_default_into_seasons():
    j = at.resolution_to_json(_res(), title="Cowboy Bebop", year="1998")
    assert j["confidence"] == "clean" and j["via"] == "tvdb" and j["title"] == "Cowboy Bebop"
    assert [t["slug"] for t in j["seasons"][0]["themes"]] == ["OP1", "ED1"], "themes without audio are not rendered"
    assert j["default"] == {"season_index": 0, "theme": "OP1", "link": AT_LINK, "size": 3725835,
                            "name": "Cowboy Bebop", "year": 1998, "song": None, "artists": []}  # v0.51.318: + song keys
    assert j["seasons"][0]["themes"][0]["audio"][0]["source"] == "BD"


def test_resolution_to_json_without_a_match():
    j = at.resolution_to_json(at.Resolution(None, None, "no bridge entry for this row's guids"))
    assert j["confidence"] is None and j["seasons"] == [] and j["default"] is None
    assert "no bridge entry" in j["reason"]


# ── preview download helper ──────────────────────────────────


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_preview_download_streams_to_dest(tmp_path):
    body = b"OggS" + b"\x00" * 5000
    c = _client(lambda r: httpx.Response(200, headers={"content-length": str(len(body))}, content=body))
    n = at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=c)
    assert n == len(body) and (tmp_path / "x.ogg").read_bytes() == body
    assert not (tmp_path / "x.ogg.part").exists()


def test_preview_download_refuses_non_animethemes_links(tmp_path):
    calls = []
    c = _client(lambda r: (calls.append(r), httpx.Response(200, content=b"x"))[1])
    for bad in ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "https://api.animethemes.moe/anime",
                "https://a.animethemes.moe.evil.com/x.ogg", "https://a.animethemes.moe/x.webm"):
        with pytest.raises(ValueError):
            at.download_preview_audio(bad, tmp_path / "x.ogg", client=c)
    assert calls == [], "belt to the SSRF braces: no request leaves for a non-AnimeThemes link"


def test_preview_download_size_caps_declared_and_actual(tmp_path):
    c = _client(lambda r: httpx.Response(200, headers={"content-length": str(at.PREVIEW_MAX_BYTES + 1)}, content=b"x"))
    with pytest.raises(ValueError, match="too large"):
        at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=c)
    # a chunked body (no content-length) must be capped while streaming
    c2 = _client(lambda r: httpx.Response(200, content=iter([b"\x00" * 1000] * 3)))
    with pytest.raises(ValueError, match="exceeded"):
        at.download_preview_audio(AT_LINK, tmp_path / "y.ogg", client=c2, max_bytes=2000)
    assert not (tmp_path / "y.ogg").exists() and not (tmp_path / "y.ogg.part").exists()


def test_preview_download_errors_and_empty_bodies_raise(tmp_path):
    with pytest.raises(at.AnimeThemesError):
        at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=_client(lambda r: httpx.Response(503)))
    with pytest.raises(at.AnimeThemesError, match="empty"):
        at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=_client(lambda r: httpx.Response(200, content=b"")))
    assert not (tmp_path / "x.ogg").exists()


def test_preview_download_is_one_at_a_time(tmp_path):
    assert at._PREVIEW_LOCK.acquire(blocking=False)
    try:
        with pytest.raises(at.PreviewBusy):
            at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=_client(lambda r: httpx.Response(200, content=b"x")))
    finally:
        at._PREVIEW_LOCK.release()
    # and the lock is released after a failure so the next click works
    with pytest.raises(at.AnimeThemesError):
        at.download_preview_audio(AT_LINK, tmp_path / "x.ogg", client=_client(lambda r: httpx.Response(500)))
    assert at._PREVIEW_LOCK.acquire(blocking=False); at._PREVIEW_LOCK.release()


# ── bridge: the Bleach shape found in the live check ─────────


def test_bridge_falls_back_to_the_tmdb_season_and_prefers_tv():
    # anime-lists carries Bleach 2004 as season {"tmdb": 1} (no tvdb key) and four
    # TYBW cours as {"tvdb": 17}; the movies are {"tvdb": 0}. Pre-fix the 2004 series
    # sorted LAST (season None) and the resolver fell through to season 17.
    data = [
        {"anidb_id": 2369, "tvdb_id": 74796, "type": "TV", "season": {"tmdb": 1}},
        {"anidb_id": 4515, "tvdb_id": 74796, "type": "MOVIE", "season": {"tvdb": 0}},
        {"anidb_id": 15449, "tvdb_id": 74796, "type": "TV", "season": {"tvdb": 17, "tmdb": 2}},
        {"anidb_id": 9999, "tvdb_id": 74796, "type": "OVA", "season": {"tvdb": 1}},
    ]
    b = at.Bridge.from_json(data)
    via, ents = b.entries_for(74796, None)
    assert [(e.anidb, e.season) for e in ents] == [(2369, 1), (9999, 1), (15449, 17), (4515, 0)], (
        "season 1 first (TV before OVA within it), then ascending, specials last")


# ── endpoints (fake bridge + client; no network) ─────────────


AUTH = {"X-Authentik-Username": "testadmin"}
NOW = "2026-09-09T00:00:00+00:00"


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import get_conn, init_db, transaction
    from app.web.api import create_app
    from fastapi.testclient import TestClient
    (tmp_path / "data").mkdir(); (tmp_path / "themes").mkdir()
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {tmp_path / 'themes'}\n")
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path); init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with get_conn(s.db_path) as conn, transaction(conn):
        conn.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, themes_subdir, included, discovered_at, last_seen_at) "
                     "VALUES ('3','Anime','show',1,0,'anime',1,?,?)", (NOW, NOW))
        conn.execute("INSERT INTO plex_items (rating_key, section_id, media_type, title, year, guid_tvdb, guid_tmdb, folder_path, edition_key, has_theme, first_seen_at, last_seen_at) "
                     "VALUES ('777','3','show','Cowboy Bebop','1998',76885,30991,'/data/anime/CB','',0,?,?)", (NOW, NOW))
    return TestClient(create_app(s)), s, tmp_path


def _fake_resolver(monkeypatch, res_factory, *, bridge_exc=None):
    from app.core import animethemes as at
    monkeypatch.setattr(at, "load_bridge", lambda cache_dir, **kw: (_ for _ in ()).throw(bridge_exc) if bridge_exc else object())
    monkeypatch.setattr(at, "resolve", lambda row, bridge, client, **kw: res_factory(row))
    monkeypatch.setattr(at, "AnimeThemesClient", lambda *a, **kw: object())


def test_resolve_endpoint_returns_the_wire_shape(app_env, monkeypatch):
    client, s, _ = app_env
    seen = {}
    def factory(row):
        seen.update(row); return _res()
    _fake_resolver(monkeypatch, factory)
    r = client.get("/api/plex_items/777/anime-themes", headers=AUTH)
    assert r.status_code == 200, r.text
    j = r.json()
    assert seen["guid_tvdb"] == 76885 and seen["guid_tmdb"] == 30991, "the resolver gets the row's guids (they are not on the library wire)"
    assert j["confidence"] == "clean" and j["default"]["theme"] == "OP1" and j["title"] == "Cowboy Bebop"
    assert j["media_type"] == "tv" and j["tmdb_id"] == 30991 and j["is_anime_section"] is True and j["has_theme"] is False
    assert j["rating_key"] == "777"


def test_resolve_endpoint_names_every_failure(app_env, monkeypatch):
    from app.core import animethemes as at
    client, s, _ = app_env
    assert client.get("/api/plex_items/nope/anime-themes", headers=AUTH).status_code == 404
    _fake_resolver(monkeypatch, lambda row: None, bridge_exc=at.BridgeUnavailable("dns"))
    r = client.get("/api/plex_items/777/anime-themes", headers=AUTH)
    assert r.status_code == 503 and "bridge unavailable" in r.json()["detail"]
    def boom(row): raise at.AnimeThemesError(429, "x")
    _fake_resolver(monkeypatch, boom)
    r = client.get("/api/plex_items/777/anime-themes", headers=AUTH)
    assert r.status_code == 502 and "HTTP 429" in r.json()["detail"]
    _fake_resolver(monkeypatch, lambda row: at.Resolution(None, None, "no bridge entry for this row's guids"))
    j = client.get("/api/plex_items/777/anime-themes", headers=AUTH).json()
    assert j["confidence"] is None and j["seasons"] == [] and "no bridge entry" in j["reason"]


def test_resolve_endpoint_is_admin_gated(app_env):
    client, s, _ = app_env
    assert client.get("/api/plex_items/777/anime-themes").status_code in (401, 403)


def test_preview_endpoint_downloads_then_transcodes_into_the_candidate_dir(app_env, monkeypatch):
    from app.core import animethemes as at
    from app.core import audio_edit
    client, s, tmp = app_env
    calls = {}
    def fake_dl(link, dest, **kw):
        calls["link"] = link; dest.write_bytes(b"OggS" + b"\x00" * 100); return 104
    def fake_tc(themes_dir, src, *, quality=0):
        calls["src_exists"] = src.exists(); calls["quality"] = quality; calls["themes_dir"] = themes_dir
        return {"candidate_id": "ab" * 16, "duration_s": 89.9, "file_size": 1234}
    monkeypatch.setattr(at, "download_preview_audio", fake_dl)
    monkeypatch.setattr(audio_edit, "transcode_to_candidate", fake_tc)
    r = client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH)
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["candidate_id"] == "ab" * 16 and j["duration_s"] == 89.9 and j["media_type"] == "tv" and j["tmdb_id"] == 30991
    assert calls["link"] == AT_LINK and calls["src_exists"] and calls["themes_dir"] == s.themes_dir
    assert calls["quality"] == int(s.download_audio_quality), "the preview encodes at the configured quality — what the real download will produce"


def test_preview_endpoint_refuses_bad_links_and_names_failures(app_env, monkeypatch):
    from app.core import animethemes as at
    from app.core import audio_edit
    client, s, tmp = app_env
    r = client.post("/api/plex_items/777/anime-themes/preview", json={"link": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}, headers=AUTH)
    assert r.status_code == 400
    assert client.post("/api/plex_items/777/anime-themes/preview", json={"link": "https://api.animethemes.moe/anime"}, headers=AUTH).status_code == 400
    def busy(link, dest, **kw): raise at.PreviewBusy("busy")
    monkeypatch.setattr(at, "download_preview_audio", busy)
    assert client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH).status_code == 409
    def big(link, dest, **kw): raise ValueError("audio is 60 MB — too large to preview")
    monkeypatch.setattr(at, "download_preview_audio", big)
    r = client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH)
    assert r.status_code == 400 and "too large" in r.json()["detail"]
    def http(link, dest, **kw): raise at.AnimeThemesError(503, link)
    monkeypatch.setattr(at, "download_preview_audio", http)
    assert client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH).status_code == 502
    def ok(link, dest, **kw): dest.write_bytes(b"x" * 10); return 10
    monkeypatch.setattr(at, "download_preview_audio", ok)
    def noff(themes_dir, src, *, quality=0): raise audio_edit.EditError("ffmpeg/ffprobe not available on this install")
    monkeypatch.setattr(audio_edit, "transcode_to_candidate", noff)
    r = client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH)
    assert r.status_code == 503 and "ffmpeg" in r.json()["detail"]
    def fail(themes_dir, src, *, quality=0): raise audio_edit.EditError("ffmpeg failed: boom")
    monkeypatch.setattr(audio_edit, "transcode_to_candidate", fail)
    assert client.post("/api/plex_items/777/anime-themes/preview", json={"link": AT_LINK}, headers=AUTH).status_code == 500


def test_transcode_helper_contract(tmp_path, monkeypatch):
    from app.core import audio_edit
    # tool-availability refusal is an EditError the endpoint maps to 503
    monkeypatch.setattr(audio_edit.shutil, "which", lambda n: None)
    with pytest.raises(audio_edit.EditError, match="not available"):
        audio_edit.transcode_to_candidate(tmp_path, tmp_path / "x.ogg")
    # with tools present: the dest lands in .edit-candidates as <32hex>.mp3 via ffmpeg
    monkeypatch.setattr(audio_edit.shutil, "which", lambda n: "/usr/bin/" + n)
    (tmp_path / "x.ogg").write_bytes(b"OggS")
    seen = {}
    class _P:
        returncode = 0; stderr = ""
    def fake_run(cmd, **kw):
        seen["cmd"] = cmd; Path(cmd[-1]).write_bytes(b"ID3" + b"\x00" * 50); return _P()
    monkeypatch.setattr(audio_edit.subprocess, "run", fake_run)
    monkeypatch.setattr(audio_edit, "probe_duration", lambda p: 89.94)
    out = audio_edit.transcode_to_candidate(tmp_path, tmp_path / "x.ogg", quality=3)
    assert len(out["candidate_id"]) == 32 and out["duration_s"] == 89.9 and out["file_size"] == 53
    assert seen["cmd"][:2] == ["ffmpeg", "-y"] and "-vn" in seen["cmd"]
    assert seen["cmd"][seen["cmd"].index("-codec:a") + 1] == "libmp3lame" and seen["cmd"][seen["cmd"].index("-q:a") + 1] == "3"
    assert (tmp_path / audio_edit._CAND_DIR / f"{out['candidate_id']}.mp3").exists()
    assert audio_edit.candidate_path(tmp_path, out["candidate_id"]).exists(), "the existing stream/cancel routes can serve it"
    # a failed ffmpeg leaves no candidate behind
    class _F:
        returncode = 1; stderr = "boom"
    monkeypatch.setattr(audio_edit.subprocess, "run", lambda cmd, **kw: _F())
    with pytest.raises(audio_edit.EditError, match="ffmpeg failed"):
        audio_edit.transcode_to_candidate(tmp_path, tmp_path / "x.ogg")
    assert len(list((tmp_path / audio_edit._CAND_DIR).glob("*.mp3"))) == 1


def test_manual_url_origin_lands_in_detail_only(app_env, monkeypatch):
    client, s, tmp = app_env
    import json, sqlite3
    r = client.post("/api/plex_items/777/manual-url", headers=AUTH,
                    json={"youtube_url": AT_LINK, "download_only": False,
                          "origin": {"source": "animethemes", "slug": "OP1", "anidb": 23, "confidence": "clean",
                                     "name": "Cowboy Bebop", "ignored": "x" * 500}})
    assert r.status_code == 200, r.text
    # the audit row is written inside the request transaction — read it behaviourally
    with sqlite3.connect(s.db_path) as conn:
        au = conn.execute("SELECT details FROM audit_events WHERE action = 'set_url' ORDER BY id DESC LIMIT 1").fetchone()
    origin = json.loads(au[0])["origin"]
    assert origin == {"source": "animethemes", "slug": "OP1", "anidb": "23", "confidence": "clean", "name": "Cowboy Bebop"}
    assert "ignored" not in origin, "only the known provenance keys are kept"
    # the event goes through the process-global flusher (bound to the first DB it
    # sees — flaky to read across the suite), so pin it at source: the MESSAGE
    # f-string is byte-identical and the origin rides in `detail` only.
    from _slice_helpers import slice_between
    api = (REPO / "app" / "web" / "api.py").read_text()
    call = slice_between(api, 'message=f"Manual URL set by {request.state.user}: {canonical_url}",', "))")
    assert '"origin": _origin' in call, "provenance rides in detail, beside rating_key/title"
    assert api.count('f"Manual URL set by {request.state.user}: ') == 2, "the per-row + CSV-import emitters, unchanged"


def test_manual_url_without_origin_is_unchanged(app_env):
    client, s, tmp = app_env
    r = client.post("/api/plex_items/777/manual-url", headers=AUTH, json={"youtube_url": AT_LINK})
    assert r.status_code == 200, r.text


# ── markup + JS pins (the v0.51.282 template) ────────────────


BASE = (REPO / "app" / "web" / "templates" / "base.html").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _dlg_markup() -> str:
    i = BASE.index('id="anime-themes-dlg"')
    return BASE[i:BASE.index("</dialog>", i)]


def _fn_block(anchor: str, end: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:APP_JS.index(end, i)]


def test_dialog_uses_the_canonical_shell_and_primitives():
    d = _dlg_markup()
    assert '<article class="dlg-body">' in d and '<header class="dlg-head">' in d
    assert '<h2 class="dlg-title">// ANIME THEMES</h2>' in d and 'class="dlg-close"' in d
    assert 'class="form-actions"' in d and "// CANCEL" in d
    assert 'id="anime-themes-download-only"' in d and "KEEP AS BACKUP (Plex keeps serving)" in d
    assert 'class="form-hint form-hint-warn" id="anime-themes-warn"' in d, "GLANCE/NAME warnings use the amber hint primitive"
    assert 'id="anime-themes-player" class="info-audio"' in d, "the EDIT AUDIO player, same class"
    assert 'id="anime-themes-use-default" style="display:none"' in d, (
        "offered only for CLEAN resolutions; inline display because the .btn display rule beats the hidden attribute")
    assert 'id="anime-themes-download-only-row" style="display:none"' in d


def test_binder_is_called_at_startup():
    assert "bindAnimeThemesDialog();" in APP_JS
    assert APP_JS.index("bindManualUrlDialog();") < APP_JS.index("bindAnimeThemesDialog();") < APP_JS.index("bindInfoDialog();")


def test_open_resets_state_and_fetches_the_resolution():
    b = _fn_block("async function openAnimeThemesDialog(", "function closeAnimeThemesDialog(")
    assert "showModalNoFocusRing(dlg)" in b
    assert "/anime-themes`)" in b and "await api('GET'" in b
    assert "const hasTheme = !!srcLetter && srcLetter !== '-';" in b and "dlOnly.checked = hasTheme" in b
    assert "dlOnlyRow.style.display = hasTheme ? '' : 'none'" in b, (
        "live check 2026-09-09: `hidden` on the .form-checkbox row was ignored (flex rule) — KEEP AS BACKUP showed on a theme-less row")
    assert "dlOnly.checked = hasTheme" in b, (
        "decision 2: a row that already has a theme pre-ticks KEEP AS BACKUP")
    assert "player.removeAttribute('src')" in b and "previewRow.hidden = true" in b


def test_preview_and_use_flows():
    b = _fn_block("function bindAnimeThemesDialog(", "\n  // ")
    assert "/anime-themes/preview`" in b and "/edit-candidate/${out.candidate_id}.mp3" in b
    assert b.index("discard();") < b.index("/anime-themes/preview`"), "a new preview discards the previous candidate first"
    assert "dlg.addEventListener('cancel'" in b and b.count("close)") >= 2, "close, cancel and Esc all discard"
    assert "/manual-url`, body)" in b and "source: 'animethemes'" in b and "body.download_only = true" in b
    assert "loadLibrary().catch(() => {});" in b and "libraryRapidPoll();" in b, "mirrors the SET URL submit"
    assert "if (_previewing) return;" in b and "if (_using) return;" in b


def test_confidence_pills_use_existing_tones_only():
    b = _fn_block("const _AT_WORDS = {", "\n  };")  # v0.51.319: the label layer
    assert "pill btn-tone-ok" in b and "pill pill-warn" in b and "pill btn-tone-attn" in b
    assert "chip-warn" not in APP_JS and "chip-ok" not in APP_JS, "the audited-out chip variants stay out"


def test_entry_points():
    assert "'anime-themes', 'ANIME THEMES'" in APP_JS
    gate = APP_JS.index("if (libraryState.tab === 'anime') {")
    assert gate < APP_JS.index("'anime-themes', 'ANIME THEMES'") < APP_JS.index("'upload-theme', 'UPLOAD MP3'"), (
        "the row-menu item is gated on the anime tab and sits in group 3 next to SET URL")
    assert "} else if (act === 'anime-themes') {" in APP_JS, "row-menu dispatch"
    assert "body.querySelector('button[data-act=\"anime-themes\"]')" in APP_JS, "card-side binding"
    assert "sc.is_anime && ratingKey" in APP_JS, "the card button is gated on section_context.is_anime"
    assert "or ANIME THEMES on anime rows" in APP_JS


def test_v0_51_317_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.317: " in init_py
