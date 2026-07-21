"""v0.51.218 — the INFO card must never GUESS which edition it is showing.

A Plex title can hold several cuts in one section (theatrical / extended / 4K / a named
fan edit), and motif tracks a SEPARATE theme file per cut. api_item resolves the cut from
the clicked row's rating_key (v1.21.68) — but only a LIBRARY ROW click carries one. Every
deep-link (inbox click-through, canonical-health, loudness-audit, /queue OPEN ROW) sends
media_type + tmdb_id + section_id and nothing else, so `_info_edition` was None, execution
fell to the section-only branch, and the card rendered `local_payloads[0]` — an arbitrary
cut, with NO ORDER BY behind it.

That made the card's loudness reading, and the file // LEVEL THIS THEME rewrites, belong
to a cut the user never chose. Measured against the operator's live DB (2026-07-21): 32
(media_type, tmdb_id, section_id) groups hold >1 edition, and 23 of those have cuts whose
theme files genuinely differ. Their Fellowship of the Ring carries three at −33.33, −14.75
and −18.75 LUFS, so "whichever came first" is an ~19 dB difference in what gets rewritten.

The fix is to stop guessing: accept an explicit edition_key from callers that know one, and
when nobody does and several exist, report the ambiguity so the card can ask.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = "2026-07-21T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()

# the operator's real Fellowship spread — the point is that these differ a LOT
CUTS = [("extended edition", -14.75), ("sam takes a step", -33.33), ("theatrical", -18.75)]


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_LOUDNESS_TARGET", raising=False)
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.web.api import create_app
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    init_auth_schema(s.db_path)
    create_admin(s.db_path, username="testadmin", password="testpassword")
    with sqlite3.connect(s.db_path) as c:
        c.execute("INSERT INTO plex_sections (section_id, title, type, is_anime, is_4k, "
                  " themes_subdir, included, discovered_at, last_seen_at) "
                  "VALUES ('1','Movies','movie',0,0,'movies',1,?,?)", (NOW, NOW))
        c.commit()
    return TestClient(create_app(s)), s.db_path


def _seed(db, *, tmdb, cuts):
    """One title, N cuts in ONE section — each with its own theme file and loudness."""
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?,'movie',?,'Multi-Cut Title','2001','imdb',?,?)",
                  (tmdb, tmdb, NOW, NOW))
        for i, (edn, lufs) in enumerate(cuts):
            # edition_key_for_rating_key resolves rk -> plex_items.folder_path ->
            # {edition-X} parse, NOT plex_items.edition_key. Seeding the column alone
            # makes every rk resolve to '' and the rating_key test silently passes for
            # the wrong reason — so give each cut its real tagged folder.
            folder = (f"/data/movies/Multi-Cut Title (2001) {{edition-{edn}}}"
                      if edn else "/data/movies/Multi-Cut Title (2001)")
            c.execute("INSERT INTO plex_items (rating_key, media_type, section_id, guid_tmdb,"
                      " theme_id, edition_key, folder_path, title, has_theme,"
                      " first_seen_at, last_seen_at) "
                      "VALUES (?,'movie','1',?,?,?,?,'Multi-Cut Title',1,?,?)",
                      (f"rk{tmdb}{i}", tmdb, tmdb, edn, folder, NOW, NOW))
            c.execute("INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
                      " file_path, file_sha256, downloaded_at, source_video_id, loudness_i) "
                      "VALUES ('movie',?,'1',?,?,?,?,'v',?)",
                      (tmdb, edn, f"movies/{tmdb}/{edn}.mp3", f"sha{tmdb}{i}", NOW, lufs))
        c.commit()


def _get(c, tmdb, **q):
    qs = "".join(f"&{k}={v}" for k, v in q.items())
    return c.get(f"/api/items/movie/{tmdb}?section_id=1{qs}", headers=AUTH).json()


# ── the defect ───────────────────────────────────────────────────────────────

def test_deep_link_without_an_edition_reports_ambiguity(client):
    """The exact deep-link shape: media_type + tmdb + section, no rating_key. The card
    must be TOLD it would be guessing rather than silently rendering payloads[0]."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120)
    assert j["edition_ambiguous"] is True
    assert sorted(j["edition_choices"]) == sorted(e for e, _ in CUTS)
    assert j["edition_key"] is None, "nothing was chosen, so nothing may be claimed"


def test_an_explicit_edition_scopes_the_card(client):
    """The fix's other half — a caller that knows the cut can say so without a rating_key,
    which is all the loudness-audit / canonical-health links have."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120, edition_key="theatrical")
    assert j["edition_ambiguous"] is False
    assert j["edition_key"] == "theatrical"
    assert j["local_file"]["edition_key"] == "theatrical"
    assert j["local_file"]["loudness_i"] == -18.75, (
        "the card must show the NAMED cut's loudness, not a sibling's")
    assert len(j["local_files"]) == 1, "a named cut must not carry its siblings along"


def test_each_cut_resolves_to_its_own_file_and_loudness(client):
    """Guards the thing that actually matters: the file // LEVEL would rewrite. If these
    ever collapse to one row, the mutating buttons are pointed at the wrong audio again."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    seen = {}
    for edn, lufs in CUTS:
        lf = _get(c, 120, edition_key=edn)["local_file"]
        assert lf["loudness_i"] == lufs
        seen[lf["file_path"]] = lf["edition_key"]
    assert len(seen) == len(CUTS), "each cut must map to a DISTINCT theme file"


def test_rating_key_still_resolves_the_edition(client):
    """v1.21.68's path is untouched — library-row clicks were always correct."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120, rating_key="rk1202")          # the 3rd seeded cut = 'theatrical'
    assert j["edition_ambiguous"] is False
    assert j["edition_key"] == "theatrical"


def test_explicit_edition_beats_rating_key(client):
    """A caller naming the cut is more authoritative than one resolved from a rk."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120, rating_key="rk1202", edition_key="extended edition")
    assert j["edition_key"] == "extended edition"
    assert j["local_file"]["loudness_i"] == -14.75


# ── the 97% case must not move ───────────────────────────────────────────────

def test_single_edition_titles_are_untouched(client):
    """2,745 of the operator's 2,822 theme rows are single-edition. They must behave
    exactly as before — no ambiguity, no picker, card fully functional."""
    c, db = client
    _seed(db, tmdb=550, cuts=[("", -18.2)])
    j = _get(c, 550)
    assert j["edition_ambiguous"] is False
    assert j["edition_choices"] == [""]
    assert j["local_file"]["loudness_i"] == -18.2, "the card must still render normally"


def test_ambiguity_is_only_computed_when_no_edition_was_named(client):
    """Cheap-path guard: naming a cut must skip the DISTINCT scan entirely, so the common
    library-row click pays nothing for this feature."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    assert _get(c, 120, edition_key="theatrical")["edition_choices"] == []


def test_the_scan_is_scoped_to_the_section():
    """A title can hold different cuts in the standard vs 4K section; the choice list must
    describe THIS section only, or the picker offers cuts that aren't here (the class-2
    edition-sibling bleed this codebase has been bitten by repeatedly)."""
    start = API_PY.index("_edition_choices: list[str] = []")
    block = API_PY[start:API_PY.index("if section_id and _info_edition is not None:", start)]
    assert "AND section_id = ?" in block
    assert "if _info_edition is None and section_id:" in block


# ── the card surface ─────────────────────────────────────────────────────────

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _loudness_block() -> str:
    i = APP_JS.index("const _loudnessRows = (() => {")
    return APP_JS[i:APP_JS.index("const _grp = (title, rows)", i)]


def test_the_picker_replaces_the_whole_loudness_block():
    """Not just the buttons. Rendering a sibling cut's LUFS next to a picker is what made
    the wrong cut look like the right one — the ambiguous branch must return early, before
    any reading is computed from the arbitrary `lf`."""
    blk = _loudness_block()
    amb = blk.index("if (data.edition_ambiguous)")
    assert amb < blk.index("const li = lf.loudness_i"), (
        "the ambiguity check must short-circuit BEFORE any per-cut reading is derived")
    assert 'data-act="loud-pick-edition"' in blk
    assert "data.edition_choices.map" in blk


def test_the_picker_offers_every_cut_including_the_untagged_one():
    """'' is a real edition (the untagged folder) and must be pickable, labelled rather
    than rendered as an empty button."""
    blk = _loudness_block()
    assert "'STANDARD'" in blk


def test_choosing_a_cut_reopens_scoped_and_busts_the_cache():
    i = APP_JS.index('button[data-act="loud-pick-edition"]')
    h = APP_JS[i:APP_JS.index('data-act="loud-measure"', i)]
    assert "_infoPrefetch.clear()" in h, (
        "v0.51.214: without this the re-open replays the ambiguous payload")
    assert "ev.currentTarget.dataset.edn" in h
    assert h.index("_infoPrefetch.clear()") < h.index("openInfoDialog(")


def test_empty_edition_survives_the_url_builder():
    """'' is falsy — a truthiness check would drop it and silently put the card back to
    guessing on exactly the untagged-folder case."""
    i = APP_JS.index("function _infoUrl(")
    fn = APP_JS[i:APP_JS.index("function _infoFetch(", i)]
    assert "if (edn != null)" in fn, "must be a null check, not truthiness"
    assert "edition_key=" in fn
