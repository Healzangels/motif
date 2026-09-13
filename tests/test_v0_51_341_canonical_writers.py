"""v0.51.341: the canonical writers — residual review findings from v0.51.338-.340.

  1. UPLOAD MP3 stamps canonical_present = 1 for the canonical it wrote (the .338 worker / .339 adopt rule).
  2. The cloud backup's identical-sha dedup applies only while the recorded canonical is on disk and non-empty.
  3. One placement row is picked present first, then unverified (NULL), then verified-missing (0), then recency.
  4. The INFO card's per-item restore restores each local_files row once, from the bulk's placement pick,
     and stamps an already-present canonical present.
  6. The AnimeThemes provider lane and the downloader's source mirror classify by host, not by substring.
(5, the restore skip wording, lives with the page harness in test_v0_51_339_canonical_health_restore.py.)
"""
from __future__ import annotations

import hashlib
import sqlite3
from unittest.mock import MagicMock

import pytest

from app.core import canonical_health as ch
from test_v0_51_339_canonical_health_restore import (  # noqa: F401 — admin_client is a fixture
    AUTH, NOW, _canonical, _db, _folder, _lf, _lf_cols, _placement, _section, _theme, admin_client,
)

MP3 = b"ID3" + b"\x00" * 64  # passes _looks_like_audio


# ── 1: UPLOAD MP3 ────────────────────────────────────────────────────

def _plex_item(conn, rk, tmdb, folder):
    conn.execute(
        "INSERT INTO plex_items (rating_key, section_id, media_type, theme_id, guid_tmdb, title, year,"
        " edition_key, folder_path, has_theme, first_seen_at, last_seen_at)"
        " VALUES (?, '1', 'movie', ?, ?, ?, '2001', '', ?, 0, ?, ?)",
        (rk, tmdb, tmdb, f"T{tmdb}", folder, NOW, NOW))


@pytest.mark.parametrize("prior", [0, None], ids=["stale-zero", "fresh-insert"])
def test_upload_mp3_stamps_the_canonical_it_wrote_present(admin_client, monkeypatch, prior):
    client, settings, tmp_path = admin_client
    monkeypatch.setattr("app.core.revisions.capture_revision", lambda *a, **k: None)
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 1101)
        _plex_item(conn, "91101", 1101, str(tmp_path / "media" / "T1101 (2001)"))
        if prior is not None:
            _lf(conn, 1101, canonical_present=prior, file_size=5)
        conn.commit()
    r = client.post("/api/plex_items/91101/upload-theme", headers=AUTH,
                    files={"file": ("theme.mp3", MP3, "audio/mpeg")})
    assert r.status_code == 200, r.text
    assert (tmp_path / "themes" / r.json()["file_path"]).read_bytes() == MP3
    assert _lf_cols(settings.db_path, 1101, ("canonical_present", "file_size")) == (1, len(MP3))
    rep = client.get("/api/admin/canonical-health/report", headers=AUTH).json()
    assert rep["counts"]["broken"] == 0, "an uploaded canonical leaves CANONICAL HEALTH now, not at the daily verify"


# ── 2: the cloud backup's dedup ──────────────────────────────────────

def _cloud_plex(body):
    plex = MagicMock()
    plex._rk_path.return_value = "/library/metadata/rk-1201/file"
    plex._headers = {}
    resp = MagicMock()
    resp.status_code, resp.content, resp.text, resp.headers = 200, body, "", {}
    plex._client.get.return_value = resp
    return plex


@pytest.mark.parametrize("on_disk, refetched", [
    (None, True),
    (b"", True),
    ("served", False),
], ids=["missing", "zero-byte-stub", "present"])
def test_cloud_backup_dedups_only_against_a_canonical_that_is_there(tmp_path, monkeypatch, on_disk, refetched):
    from app.core import revisions
    from app.core.cloud_theme_backup import backup_cloud_theme
    monkeypatch.setattr(revisions, "capture_revision", lambda *a, **k: None)
    db, themes, _plexdir = _db(tmp_path)
    themes.mkdir()
    body = b"ID3" + b"cloud-1201" * 16
    recorded = _canonical(themes, 1201)
    if on_disk is not None:
        recorded.parent.mkdir(parents=True)
        recorded.write_bytes(body if on_disk == "served" else on_disk)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        _section(conn)
        _theme(conn, 1201)
        # the recorded sha IS Plex's bytes — only the file on disk says whether the canonical survived
        _lf(conn, 1201, canonical_present=0 if refetched else 1, file_size=len(body),
            file_sha256=hashlib.sha256(body).hexdigest(), extra={"source_kind": "plex_cloud"})
        conn.commit()
        target = {"rating_key": "rk-1201", "guid_tmdb": 1201, "media_type": "movie", "section_id": "1",
                  "title": "Cloud", "year": "2009", "edition_key": "",
                  "entry_uri": "metadata://themes/" + "c" * 40, "sha1": "c" * 40}
        result = backup_cloud_theme(conn, target, themes, _cloud_plex(body))
    finally:
        conn.close()
    assert result["ok"] is True, result
    assert bool(result.get("skipped_identical")) is (not refetched)
    written = themes / result["file_path"] if refetched else recorded
    assert written.read_bytes() == body
    assert _lf_cols(db, 1201, ("canonical_present",)) == (1,)


# ── 3: placement order ───────────────────────────────────────────────

def test_an_unverified_surviving_sidecar_outranks_a_verified_missing_one(tmp_path):
    db, themes, plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 1301)
        _lf(conn, 1301)
        # the stamped-missing row is inserted first, first by media_folder, and the most recent
        _placement(conn, 1301, _folder(plexdir, "A-stamped-missing", b"stale-bytes"),
                   theme_present=0, placed_at="2026-09-10T00:00:00")
        _placement(conn, 1301, _folder(plexdir, "Z-unverified", b"unverified-bytes"),
                   theme_present=None, placed_at="2026-09-01T00:00:00")
        conn.commit()
    res = ch.restore_from_plex(db, themes, None)
    assert (res["restored_sidecar"], res["skipped"]) == (1, [])
    assert _canonical(themes, 1301).read_bytes() == b"unverified-bytes"


@pytest.mark.parametrize("with_present, expected", [(True, "plex_upload"), (False, "hardlink")],
                         ids=["present-first", "unverified-before-verified-missing"])
def test_the_bundle_census_orders_present_then_unverified_then_verified_missing(tmp_path, with_present, expected):
    from app.core.bundle import themes_census
    db, _themes, _plexdir = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        _section(conn)
        _theme(conn, 1302)
        _lf(conn, 1302)
        # the verified-missing row is the newest — only the ORDER BY can put another ahead of it
        _placement(conn, 1302, "/media/A-verified-missing", kind="copy", theme_present=0,
                   placed_at="2026-09-10T00:00:00")
        _placement(conn, 1302, "/media/M-unverified", kind="hardlink", theme_present=None,
                   placed_at="2026-09-05T00:00:00")
        if with_present:
            _placement(conn, 1302, "", kind="plex_upload", rk="91302", theme_present=1,
                       placed_at="2026-09-01T00:00:00")
        conn.commit()
    (row,) = themes_census(db)
    assert row["placement_kind"] == expected


# ── 4: the INFO card's per-item restore ──────────────────────────────

@pytest.mark.parametrize("live_name, dead_name", [("A-live", "Z-dead"), ("Z-live", "A-dead")],
                         ids=["live-first", "dead-first"])
def test_info_card_restore_restores_a_two_placement_item_once(admin_client, live_name, dead_name):
    client, settings, tmp_path = admin_client
    plexdir = tmp_path / "plex"
    live = _folder(plexdir, live_name, b"live-1401")
    dead = _folder(plexdir, dead_name, data=None)
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 1401)
        _lf(conn, 1401)
        # inserted in folder order, so each parametrization puts the other row first
        for folder, present in sorted([(live, None), (dead, 1)], key=lambda fp: fp[0]):
            _placement(conn, 1401, folder, theme_present=present)
        conn.commit()
    r = client.post("/api/items/movie/1401/restore-canonical", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "restored": 1, "skipped": []}, "the INFO card alerts on any skip"
    assert _canonical(tmp_path / "themes", 1401).read_bytes() == b"live-1401"
    assert _lf_cols(settings.db_path, 1401, ("canonical_present",)) == (1,)


def test_info_card_restore_stamps_an_already_present_canonical(admin_client):
    client, settings, tmp_path = admin_client
    with sqlite3.connect(settings.db_path) as conn:
        _section(conn)
        _theme(conn, 1402)
        _lf(conn, 1402, canonical_present=0, file_size=5)
        _placement(conn, 1402, _folder(tmp_path / "plex", "1402", b"sidecar-1402"))
        conn.commit()
    canonical = _canonical(tmp_path / "themes", 1402)
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(b"downloaded-meanwhile")
    r = client.post("/api/items/movie/1402/restore-canonical", headers=AUTH)
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "restored": 0,
                        "skipped": [{"section_id": "1", "reason": "canonical_already_present"}]}
    assert canonical.read_bytes() == b"downloaded-meanwhile"
    assert _lf_cols(settings.db_path, 1402, ("canonical_present", "file_size")) == (1, 5), \
        "present is stamped; the recorded size is left for CHANGED"
    rep = client.get("/api/admin/canonical-health/report", headers=AUTH).json()
    assert rep["counts"]["broken"] == 0, "the row leaves CANONICAL HEALTH now, not at the daily verify"


# ── 6: the AnimeThemes host classifiers ──────────────────────────────

_AT_CASES = [
    ("https://A.ANIMETHEMES.MOE/CowboyBebop-OP1.ogg", True),
    ("https://a.animethemes.moe/CowboyBebop-OP1.ogg", True),
    ("https://cdn.example.com/a.animethemes.moe/CowboyBebop-OP1.ogg", False),
    ("https://cdn.example.com/theme.ogg?mirror=a.animethemes.moe", False),
]
_AT_IDS = ["uppercase-host", "host", "in-a-path", "in-a-query"]


@pytest.mark.parametrize("url, is_at", _AT_CASES, ids=_AT_IDS)
def test_the_provider_lane_is_host_anchored(url, is_at):
    from app.core.provider_health import provider_for_url
    assert provider_for_url(url) == ("animethemes" if is_at else "other")


@pytest.mark.parametrize("url, is_at", _AT_CASES, ids=_AT_IDS)
def test_the_downloader_mirror_is_host_anchored(url, is_at):
    from app.core.downloader import _source_for
    from app.core.sync import url_source
    assert _source_for(url) == ("animethemes" if is_at else "unknown")
    assert (_source_for(url) == "animethemes") is (url_source(url) == "animethemes"), "the mirror agrees with sync"
