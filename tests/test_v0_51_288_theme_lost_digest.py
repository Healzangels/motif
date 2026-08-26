"""v0.51.288 — multi-loss theme-lost runs digest through the coalescer.

the user: "after having multiple items removed that had a theme we get
discord messages that end up being a bit spammy" — each lost row fired its
own full 💔 body, three near-identical paragraphs apiece, back-to-back.
The reaper now routes through dispatch_coalesced: a lone loss keeps the
immediate rich single (bulk=False, byte-identical to before), a multi-loss
run (≥2 candidates) buffers per-kind and flushes as ONE digest — count
title + bulleted labels + the tier's guidance ONCE. Inbox rows stay
per-item (dispatch_coalesced records them before the apprise gate), so
only the Discord/apprise push collapses.

Drives the REAL pipe (v1.18.81 phantom-fix rule): _upsert_items ×2 →
miss-grace → reap → tier classifier → dispatch, with the coalescer spied.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

SEC = "1"
NOW = "2026-08-25T00:00:00+00:00"


def _seed(db, tids_with_rows):
    from app.core.db import get_conn, init_db, transaction
    init_db(db)
    with get_conn(db) as conn, transaction(conn):
        conn.execute(
            """INSERT INTO plex_sections (section_id, title, type, is_anime,
                 is_4k, themes_subdir, included, discovered_at, last_seen_at)
               VALUES (?, 'TV', 'show', 0, 0, 'tv', 1, ?, ?)""",
            (SEC, NOW, NOW))
        for tid, title in tids_with_rows:
            conn.execute(
                """INSERT INTO themes (media_type, tmdb_id, title, year,
                     upstream_source, last_seen_sync_at, first_seen_sync_at)
                   VALUES ('tv', ?, ?, '2010', 'themoviedb', ?, ?)""",
                (tid, title, NOW, NOW))
            # a P-row Plex was serving — no local_files / placements /
            # overrides / sidecar, so its loss classifies no_fallback.
            conn.execute(
                """INSERT INTO plex_items (rating_key, section_id, media_type,
                     title, year, guid_tmdb, folder_path, has_theme,
                     first_seen_at, last_seen_at)
                   VALUES (?, ?, 'show', ?, '2010', ?, ?, 1, ?, ?)""",
                (f"rk-{tid}", SEC, title, tid,
                 f"/data/tv/{title}", NOW, NOW))


def _live_item():
    from app.core.plex import PlexLibraryItem
    return PlexLibraryItem(
        rating_key="rk-live", section_id=SEC, media_type="show",
        title="Still Here", year="2010", guid_imdb=None, guid_tmdb=288999,
        guid_tvdb=None, folder_path="/data/tv/Still Here", has_theme=False)


@pytest.fixture
def env(tmp_path, monkeypatch):
    themes = tmp_path / "data" / "themes"
    themes.mkdir(parents=True)
    (tmp_path / "motif.yaml").write_text(f"paths:\n  themes_dir: {themes}\n")
    db = tmp_path / "motif.db"
    # v0.51.273: Settings' config dir is captured from env at module import —
    # inject the instance, don't setenv.
    import app.config as cfg
    real = cfg.Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    monkeypatch.setattr(cfg, "Settings", lambda *a, **kw: real)
    import app.core.notify as notify_mod
    coalesced: list[dict] = []
    singles: list[dict] = []
    monkeypatch.setattr(notify_mod, "dispatch_coalesced",
                        lambda *a, **kw: coalesced.append(kw))
    monkeypatch.setattr(notify_mod, "dispatch",
                        lambda *a, **kw: singles.append(kw))
    return db, coalesced, singles


def _drive_two_enums(db):
    from app.core.plex_enum import _upsert_items
    # two enums with the seeded rks absent: miss #1 (grace), miss #2 (reap).
    _upsert_items(db, [_live_item()], section_id=SEC)
    _upsert_items(db, [_live_item()], section_id=SEC)


# ── the real pipe: two losses in one run → one per-kind digest ──


def test_two_losses_in_one_run_dispatch_as_bulk(env):
    db, coalesced, singles = env
    _seed(db, [(288001, "Hawaii Five-O"), (288002, "ER")])
    _drive_two_enums(db)
    import app.core.notify_content as _nc
    assert len(coalesced) == 2, coalesced
    for kw in coalesced:
        assert kw["event_kind"] == "plex_theme_lost"
        assert kw["bulk"] is True, (
            "a ≥2-loss run must buffer into the per-kind digest")
        assert kw["batch_title_fn"] is _nc.format_plex_theme_lost_batch_title
        assert kw["batch_body_fn"] is _nc.format_plex_theme_lost_batch_body
        assert kw["single_title"].startswith("💔 Theme lost —")
    labels = sorted(kw["item_label"] for kw in coalesced)
    assert labels[0].startswith("ER")
    assert labels[1].startswith("Hawaii Five-O")
    assert singles == [], "the reaper must not bypass the coalescer"


def test_lone_loss_keeps_the_immediate_rich_single(env):
    db, coalesced, singles = env
    _seed(db, [(288001, "Hawaii Five-O")])
    _drive_two_enums(db)
    assert len(coalesced) == 1
    assert coalesced[0]["bulk"] is False, (
        "a lone loss fires immediately — dispatch_coalesced sends the rich "
        "single with no window when bulk=False (v1.23.46)")
    assert singles == []


# ── digest formatters ────────────────────────────────────────


def test_no_fallback_batch_body_says_the_guidance_once():
    import app.core.notify_content as _nc
    body = _nc.format_plex_theme_lost_batch_body(
        ["Hawaii Five-O (2010)", "ER (1994)"])
    assert "* Hawaii Five-O (2010)" in body
    assert "* ER (1994)" in body
    # the whole point: shared guidance appears ONCE, not per item.
    assert body.count("// REFRESH PLEX") == 1
    assert body.count("// MOTIF INFO") == 1
    title = _nc.format_plex_theme_lost_batch_title(2)
    assert "2" in title and "💔" in title


def test_sidecar_and_backup_batch_bodies_carry_their_ctas():
    import app.core.notify_content as _nc
    side = _nc.format_theme_lost_sidecar_available_batch_body(["A", "B"])
    assert side.count("// ADOPT") == 1
    assert "(M)" in side
    back = _nc.format_theme_lost_backup_ready_batch_body(["A", "B"])
    assert back.count("// PROMOTE TO ACTIVE") == 1
    assert "💔" in _nc.format_theme_lost_sidecar_available_batch_title(3)
    assert "💔" in _nc.format_theme_lost_backup_ready_batch_title(3)


def test_batch_body_caps_the_bullet_list():
    import app.core.notify_content as _nc
    body = _nc.format_plex_theme_lost_batch_body(
        [f"Title {i}" for i in range(20)])
    assert "…and 5 more" in body


def test_v0_51_288_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.288: " in init_py
