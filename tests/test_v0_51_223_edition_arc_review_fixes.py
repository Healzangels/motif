"""v0.51.223 — three correctness fixes the ultra-review found on the edition-exact arc.

The arc (v0.51.218-220) promised that a link claiming to bring you to a specific cut lands
you on THAT cut, and that when nothing names a cut the card asks instead of guessing. The
review found three holes I'd shipped in it:

  #1  enrich_item defaulted edition_key to '' and stamped it into the ItemContext
      unconditionally, so a title-level notice (new_tdb_theme_available /
      plex_item_arrived_themed — callers that pass no edition) recorded '' not NULL. On a
      MULTI-edition title that '' scoped the inbox click-through to the STANDARD cut
      instead of the v0.51.218 picker — a regression on the very "2 new items arrived
      already themed" notice the operator asked about. Fix: stamp only when a cut is named.
      The v0.51.220 tests missed it because both drove record_notification / a hand-built
      ctx directly, never enrich_item with no edition (the v1.18.81 phantom-guard trap) —
      so the discriminator here is the REAL enrich_item -> dispatch -> record -> list path.

  #2  the level/undo + measure success handlers re-opened the card with
      openInfoDialog(mediaType, tmdbId, sectionId, ratingKey) — dropping the closure's
      editionKey. A card reached BY edition (picker / audit-health deep-link) has
      ratingKey undefined, so the re-open went edition-blind and bounced back to the
      picker: a pick -> act -> re-pick loop. Fix: thread editionKey (undefined on a
      library-row click, so that path is unchanged).

  #3  v0.51.218's picker guarded only the LOUDNESS rows. The hero LUFS chip, the file &
      placement block (downloaded path / backup / placements / inline audio), the playback
      headline, and the edition scope-chip all still rendered an arbitrary sibling cut's
      data beside the picker — exactly the "a sibling's numbers made the wrong cut look
      right" failure the picker exists to prevent. Fix: suppress every cut-specific surface
      when the cut is ambiguous (scope-chip label server-side, the rest client-side).
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from app.core.db import init_db

NOW = "2026-07-22T00:00:00"
AUTH = {"X-Authentik-Username": "testadmin"}
REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()

CUTS = [("extended edition", -14.75), ("sam takes a step", -33.33), ("theatrical", -18.75)]


# ── #1 — a title-level notice records NULL, not '' (the real enrich_item seam) ──

@pytest.fixture
def db(tmp_path):
    p = tmp_path / "motif.db"
    init_db(p)
    return p


def _list(db):
    from app.core import notify_inbox
    return notify_inbox.list_notifications(db)


def test_enrich_item_with_no_edition_leaves_the_key_unset(db):
    """The root cause. A caller that names no cut must NOT get edition_key='' stamped —
    dispatch reads it with .get(), so absent -> None -> NULL -> the picker. Pre-fix this
    returned '' and quietly scoped the click-through to the standard cut."""
    from app.core import notify_content
    ctx = notify_content.enrich_item(db, media_type="movie", tmdb_id=120, section_id="1")
    assert ctx.get("edition_key") is None


def test_title_level_dispatch_records_null_edition_through_the_real_path(db):
    """The discriminator the v0.51.220 tests lacked: enrich_item (no edition) -> dispatch
    -> record -> list, end to end. Pre-fix this row's edition_key came back '' and the
    inbox deep-link carried info_edition='' -> standard cut, never the picker."""
    from app.core import notify, notify_content
    ctx = notify_content.enrich_item(db, media_type="movie", tmdb_id=120, section_id="1")
    cfg = SimpleNamespace(
        events={"theme_added": False}, apprise_urls=[], apprise_external_url="",
        inbox_events={"theme_added": True})
    notify.dispatch(db, cfg, event_kind="theme_added",
                    title="🎵 Theme added — X", body=None, item_ctx=ctx)
    rows = _list(db)
    assert rows and rows[0]["edition_key"] is None


def test_a_named_standard_cut_still_records_empty_not_null(db):
    """'' is the untagged standard cut, a REAL scope distinct from NULL 'unknown'. A caller
    that explicitly names the standard cut must still record '' — the fix suppresses only
    the DEFAULT, never an explicitly-passed ''."""
    from app.core import notify_content
    ctx = notify_content.enrich_item(
        db, media_type="movie", tmdb_id=99, section_id="1", edition_key="")
    assert ctx.get("edition_key") == ""


def test_a_named_tagged_cut_is_carried_verbatim(db):
    from app.core import notify_content
    ctx = notify_content.enrich_item(
        db, media_type="movie", tmdb_id=120, section_id="1", edition_key="theatrical")
    assert ctx.get("edition_key") == "theatrical"


# ── #3 (server half) — an ambiguous card surfaces no arbitrary edition label ───

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
    with sqlite3.connect(db) as c:
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute("INSERT OR IGNORE INTO themes (id, media_type, tmdb_id, title, year, "
                  " upstream_source, last_seen_sync_at, first_seen_sync_at) "
                  "VALUES (?,'movie',?,'Multi-Cut Title','2001','imdb',?,?)",
                  (tmdb, tmdb, NOW, NOW))
        for i, (edn, lufs) in enumerate(cuts):
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


def test_ambiguous_card_omits_the_edition_scope_chip(client):
    """The scope-chip label fell back to a section+guid LIMIT 1 with no rk — an ARBITRARY
    sibling's {edition-X}. On an ambiguous card that read as "edition: <some cut>" beside a
    picker that says the cut is unknown. It must be None so the JS omits the chip."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120)
    assert j["edition_ambiguous"] is True
    assert j["section_context"]["edition"] is None


def test_rating_key_card_keeps_its_real_edition_label(client):
    """Suppression is gated on AMBIGUITY only — a card scoped by rating_key resolves the
    exact folder and must still show that cut's label."""
    c, db = client
    _seed(db, tmdb=120, cuts=CUTS)
    j = _get(c, 120, rating_key="rk1202")          # the 3rd seeded cut = 'theatrical'
    assert j["edition_ambiguous"] is False
    assert j["section_context"]["edition"] == "theatrical"


# ── #2 (client) — every re-open threads editionKey ─────────────────────────────

def test_reopen_calls_never_drop_the_edition_key():
    """The two success-handler re-opens (level/undo, measure) resolve the cut from
    ratingKey, which is undefined on an edition deep-link — so they MUST also pass
    editionKey or the card goes edition-blind on re-open. The picker's own re-open uses
    openInfoDialog(..., undefined, edn) and correctly doesn't match this ratingKey shape."""
    calls = re.findall(r'openInfoDialog\(mediaType, tmdbId, sectionId, ratingKey([^)]*)\)',
                       APP_JS)
    assert calls, "expected the ratingKey-based re-open calls to still exist"
    dropped = [c for c in calls if "editionKey" not in c]
    assert not dropped, (
        f"{len(dropped)} re-open call(s) pass ratingKey without editionKey — a card "
        "reached by edition will bounce to the picker on re-open (ultra-review #2)")


# ── #3 (client) — the picker's discipline reaches every cut-specific surface ────

def _slice(start_anchor: str, end_anchor: str) -> str:
    i = APP_JS.index(start_anchor)
    return APP_JS[i:APP_JS.index(end_anchor, i + len(start_anchor))]


def test_the_ambiguity_flag_is_read_once_into_a_guard():
    assert "const _ambiguousCut = !!data.edition_ambiguous;" in APP_JS


def test_hero_loudness_chip_is_blank_when_ambiguous():
    """The chip asserts a cut's LEVELED/LOUD/RAW state; on an ambiguous card that's an
    arbitrary sibling's marker sitting in the title beside the picker."""
    blk = _slice("const _loudChip = (() => {", "body.innerHTML =")
    assert "if (_ambiguousCut) return '';" in blk


def test_file_and_placement_block_is_blank_when_ambiguous():
    """downloaded path / backup / placements / inline audio are ALL an arbitrary cut's
    when unscoped — the audio element would even play that sibling's theme."""
    assert "const _onDiskRows = _ambiguousCut ? '' :" in APP_JS


def test_playback_headline_defers_to_the_picker_when_ambiguous():
    """_derivePlaybackSourceLabel reads the arbitrary `lf`; it must not claim what plays
    when that depends on which cut."""
    fn = _slice("function _derivePlaybackSourceLabel() {", "function _humanSourceKind(")
    assert "_ambiguousCut" in fn
    assert "return '(multiple cuts" in fn


def test_single_edition_cards_keep_every_surface(client):
    """The 97% case must not regress — a single-cut title is never ambiguous, so the chip,
    file block, playback headline and edition label all render exactly as before."""
    c, db = client
    _seed(db, tmdb=550, cuts=[("", -18.2)])
    j = _get(c, 550)
    assert j["edition_ambiguous"] is False
    assert j["local_file"]["loudness_i"] == -18.2
