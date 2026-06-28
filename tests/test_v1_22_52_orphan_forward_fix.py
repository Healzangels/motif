"""v1.22.52 — creation-time forward-fix + upstream display relabel.

the user's prod diagnostic showed 549/555 imdb-bearing orphans were real titles
minted synthetic only because no imdb→tmdb resolver ran at creation time. The
de-orphan walker (v1.22.49-51) cleared the backlog; this closes the source:

1. `adopt._create_orphan_theme` now resolves imdb→tmdb via TMDB BEFORE minting
   a synthetic negative id — new manual/adopted themes are keyed to their real
   identity from birth. Every failure mode (no key / no match / type mismatch /
   resolver exception) falls back to the synthetic mint exactly as before.
2. The cloud-backup orphan mints now stamp the item's guid_imdb on the minted
   row (pre-fix they stored NO imdb → permanently unresolvable).
3. INFO card: `upstream: plex_orphan` (internal jargon) renders as
   "local (manual / adopted — not from themerrdb)" — the user's M*A*S*H confusion.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core.adopt import _create_orphan_theme
from app.core.db import get_conn, init_db

_NOW = "2026-06-09T09:00:00"


def _db():
    d = Path(tempfile.mkdtemp()) / "motif.db"
    init_db(d)
    return d


def _finding(*, imdb=None, tmdb=None, section_type="movie", title="T",
             year="2026"):
    md = {"title": title, "year": year}
    if imdb:
        md["imdb_id"] = imdb
    if tmdb:
        md["tmdb_id"] = tmdb
    return {"resolved_metadata": json.dumps(md), "section_type": section_type}


def _theme_row(d, theme_id):
    with get_conn(d) as c:
        return c.execute(
            "SELECT tmdb_id, imdb_id, upstream_source FROM themes WHERE id=?",
            (theme_id,)).fetchone()


# ── forward-fix: resolve imdb→tmdb before minting ─────────────


def test_resolves_imdb_to_real_tmdb_at_creation(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb",
        lambda self, i: {"tmdb_id": 1430077, "kind": "movie"})
    d = _db()
    theme_id, tmdb = _create_orphan_theme(d, _finding(imdb="tt35672862",
                                                      title="Hokum"), "admin")
    assert tmdb == 1430077  # real identity from birth — no synthetic id
    row = _theme_row(d, theme_id)
    assert row["tmdb_id"] == 1430077
    assert row["imdb_id"] == "tt35672862"
    assert row["upstream_source"] == "plex_orphan"  # provenance still local


def test_resolved_tmdb_reuses_existing_theme(tmp_path, monkeypatch):
    """If the resolved tmdb already has a themes row, the existing-row reuse
    branch fires (same as metadata-provided tmdb) — no duplicate."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb",
        lambda self, i: {"tmdb_id": 500, "kind": "movie"})
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO themes (media_type,tmdb_id,title,upstream_source,"
            " first_seen_sync_at,last_seen_sync_at) "
            "VALUES ('movie',500,'Real','themoviedb',?,?)", (_NOW, _NOW))
        existing_id = c.execute(
            "SELECT id FROM themes WHERE tmdb_id=500").fetchone()[0]
        c.commit()
    theme_id, tmdb = _create_orphan_theme(d, _finding(imdb="tt_x"), "admin")
    assert (theme_id, tmdb) == (existing_id, 500)
    with get_conn(d) as c:
        assert c.execute("SELECT COUNT(*) FROM themes").fetchone()[0] == 1


def test_no_tmdb_key_falls_back_to_synthetic(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("MOTIF_TMDB_API_KEY", raising=False)
    d = _db()
    theme_id, tmdb = _create_orphan_theme(d, _finding(imdb="tt_y"), "admin")
    assert tmdb < 0  # pre-fix behavior preserved when resolution unavailable
    assert _theme_row(d, theme_id)["upstream_source"] == "plex_orphan"


def test_no_match_falls_back_to_synthetic(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb", lambda self, i: None)
    d = _db()
    _, tmdb = _create_orphan_theme(d, _finding(imdb="tt_niche"), "admin")
    assert tmdb < 0


def test_type_mismatch_falls_back_to_synthetic(tmp_path, monkeypatch):
    """A movie finding whose imdb resolves to a TV record must not key into the
    wrong identity space."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")
    monkeypatch.setattr(
        "app.core.tmdb.TMDBClient.lookup_by_imdb",
        lambda self, i: {"tmdb_id": 999, "kind": "tv"})
    d = _db()
    _, tmdb = _create_orphan_theme(
        d, _finding(imdb="tt_z", section_type="movie"), "admin")
    assert tmdb < 0


def test_resolver_exception_falls_back_to_synthetic(tmp_path, monkeypatch):
    """Class-9: a TMDB outage must not break SET URL/adopt — log + fall back."""
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MOTIF_TMDB_API_KEY", "fakekey")

    def _boom(self, i):
        raise RuntimeError("tmdb down")

    monkeypatch.setattr("app.core.tmdb.TMDBClient.lookup_by_imdb", _boom)
    d = _db()
    theme_id, tmdb = _create_orphan_theme(d, _finding(imdb="tt_q"), "admin")
    assert tmdb < 0
    assert _theme_row(d, theme_id)["upstream_source"] == "plex_orphan"


# ── cloud-backup mint stamps guid_imdb ────────────────────────


def test_cloud_mint_stamps_guid_imdb():
    from app.core.cloud_theme_backup import _resolve_or_mint_tmdb_id
    d = _db()
    with get_conn(d) as c:
        c.execute(
            "INSERT INTO plex_sections (section_id,title,type,included,"
            " is_anime,is_4k,themes_subdir,discovered_at,last_seen_at) "
            "VALUES ('s','M','movie',1,0,0,'m',?,?)", (_NOW, _NOW))
        c.execute(
            "INSERT INTO plex_items (rating_key,section_id,media_type,"
            " guid_tmdb,guid_imdb,title,year,first_seen_at,last_seen_at) "
            "VALUES ('rk9','s','movie',NULL,'tt_mint','NoTmdb','2026',?,?)",
            (_NOW, _NOW))
        c.commit()
    with get_conn(d) as c:
        r = c.execute(
            "SELECT rating_key, guid_tmdb, theme_id, title, year "
            "FROM plex_items WHERE rating_key='rk9'").fetchone()
        synth = _resolve_or_mint_tmdb_id(c, r, "movie", mint=True)
        assert synth < 0
        row = c.execute(
            "SELECT imdb_id, upstream_source FROM themes WHERE tmdb_id=?",
            (synth,)).fetchone()
        # pre-fix the mint stored NO imdb — the orphan was unresolvable forever
        assert row["imdb_id"] == "tt_mint"
        assert row["upstream_source"] == "plex_orphan"


# ── INFO card relabel ─────────────────────────────────────────


def test_info_card_relabels_plex_orphan():
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    idx = js.index("<dt>upstream</dt>")
    block = js[idx:idx + 400]
    assert "t.upstream_source === 'plex_orphan'" in block
    assert "not from themerrdb" in block, (
        "v1.22.52: 'plex_orphan' must render as a human label on the INFO card")
