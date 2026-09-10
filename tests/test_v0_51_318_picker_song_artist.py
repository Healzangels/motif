"""v0.51.318 — AnimeThemes tag 3b: the song title + artist in the picker.

The operator's read of the tag-3 dialog: Bleach's 15 openings and a 13-row
ending "looked like episode or season themes". They are per-arc openings
and per-episode-range VERSIONS of one song; the catalogue's song title and
artist are what a person recognises. So: the step-2 include carries the
song + artists, the wire shape and the rows show "OP1 — Tank! · The
Seatbelts", and each theme is ONE row with its best audio and a versions
pill. Measured first: docs/specs/animethemes_eval/2026-09-09-tdb-agreement.md.
"""
from __future__ import annotations

import sys
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import animethemes as at  # noqa: E402

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


def _anime_json():
    return {"id": 601, "name": "BLEACH", "year": 2004, "season": "Fall", "slug": "bleach",
            "animethemes": [
                {"slug": "OP1", "type": "OP", "sequence": None,
                 "song": {"title": "*~Asterisk", "artists": [{"name": "Orange Range"}]},
                 "animethemeentries": [
                     {"version": 2, "nsfw": False, "spoiler": False,
                      "videos": [{"basename": "b-v2.webm", "nc": True, "source": "WEB", "resolution": 720,
                                  "audio": {"link": "https://a.animethemes.moe/Bleach-OP1v2.ogg", "size": 200}}]},
                     {"version": 1, "nsfw": False, "spoiler": False,
                      "videos": [{"basename": "b-web.webm", "nc": True, "source": "WEB", "resolution": 720,
                                  "audio": {"link": "https://a.animethemes.moe/Bleach-OP1-web.ogg", "size": 300}},
                                 {"basename": "b-bd.webm", "nc": True, "source": "BD", "resolution": 1080,
                                  "audio": {"link": "https://a.animethemes.moe/Bleach-OP1.ogg", "size": 400}}]}]},
                {"slug": "ED1", "type": "ED", "sequence": 1, "song": {"title": "Life is Like a Boat", "artists": []},
                 "animethemeentries": [{"version": None, "nsfw": False, "spoiler": False,
                                        "videos": [{"basename": "e.webm", "nc": False, "source": "BD", "resolution": 1080,
                                                    "audio": {"link": "https://a.animethemes.moe/Bleach-ED1.ogg", "size": 100}}]}]},
                {"slug": "ED2", "type": "ED", "sequence": 2, "song": None,
                 "animethemeentries": [{"version": None, "nsfw": False, "spoiler": False,
                                        "videos": [{"basename": "e2.webm", "nc": False, "source": "BD", "resolution": 1080,
                                                    "audio": {"link": "https://a.animethemes.moe/Bleach-ED2.ogg", "size": 90}}]}]},
            ]}


def test_step2_include_carries_the_song_and_artists():
    seen = {}
    def h(r):
        seen.update(dict(r.url.params.multi_items())); return httpx.Response(200, json={"anime": [_anime_json()]})
    c = at.AnimeThemesClient(client=httpx.Client(transport=httpx.MockTransport(h), base_url=at.API_BASE),
                             sleep=lambda s: None, monotonic=lambda: 0.0)
    th = c.themes_for([601])[601]
    assert "animethemes.song.artists" in seen["include"].split(",") and seen["fields[song]"] == "title" and seen["fields[artist]"] == "name"
    assert th[0].song == "*~Asterisk" and th[0].artists == ("Orange Range",)
    assert th[1].song == "Life is Like a Boat" and th[1].artists == ()
    assert th[2].song is None and th[2].artists == (), "a theme without a song record stays renderable"


def test_wire_shape_orders_best_audio_first_and_carries_the_song():
    th = at._parse_themes(_anime_json())
    info = at.AnimeInfo(anime_id=601, name="BLEACH", year=2004, season="Fall", slug="bleach")
    sm = at.SeasonMatch(season=1, anidb=2369, info=info, themes=th)
    picked = at.pick_default(sm)
    res = at.Resolution("clean", "tvdb", "ok", [sm], (sm, *picked))
    j = at.resolution_to_json(res, title="Bleach", year="2004")
    op1 = j["seasons"][0]["themes"][0]
    assert op1["song"] == "*~Asterisk" and op1["artists"] == ["Orange Range"]
    assert [a["link"] for a in op1["audio"]] == ["https://a.animethemes.moe/Bleach-OP1.ogg",
                                                 "https://a.animethemes.moe/Bleach-OP1-web.ogg",
                                                 "https://a.animethemes.moe/Bleach-OP1v2.ogg"], (
        "version 1 before version 2, BD before WEB — the picker renders ONE row per theme from audio[0]")
    assert j["default"]["song"] == "*~Asterisk" and j["default"]["artists"] == ["Orange Range"]
    assert j["default"]["link"] == "https://a.animethemes.moe/Bleach-OP1.ogg"


def test_rows_show_the_song_and_collapse_versions():
    i = APP_JS.index("function _atThemeRows(seasonIdx, theme) {")
    b = APP_JS[i:APP_JS.index("\n  function ", i + 10)]
    assert "const a = theme.audio[0];" in b, "one row per theme: the server's best audio"
    assert "theme.audio.map(" not in b, "no row per version any more (Bleach ED3 rendered 13 rows)"
    assert "htmlEscape(theme.song)" in b and "theme.artists.join(', ')" in b
    assert "versions</span>" in b and "same song, other episode ranges" in b
    assert "<span class=\"muted\">untitled</span>" in b, "a theme without a song record still renders"


def test_default_button_and_provenance_name_the_song():
    assert "const dsong = data.default.song ? ` — ${htmlEscape(data.default.song)}` : '';" in APP_JS
    i = APP_JS.index("const use = async (link, slug, btn) => {")
    b = APP_JS[i:APP_JS.index("body?.addEventListener('click'", i)]
    assert "song: theme && theme.song ?" in b, "USE THIS carries the song into the manual-url origin"
    assert '"name", "song")' in API_PY, "manual-url keeps the song key of the origin"


def test_harness_records_the_default_song():
    tool = (REPO / "tools" / "animethemes_eval.py").read_text()
    assert '"song": d[1].song, "artists": list(d[1].artists)' in tool


def test_agreement_measurement_is_banked():
    assert (REPO / "docs" / "specs" / "animethemes_eval" / "2026-09-09-tdb-agreement.md").exists()


def test_v0_51_318_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.318: " in init_py
