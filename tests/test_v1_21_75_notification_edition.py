"""v1.21.75 — edition label in notifications.

the user: notifications for a multi-edition title (LotR Fellowship —
Theatrical / Extended Edition / Sam Takes a Step) all read just "The Lord
of the Rings: The Fellowship of the Ring (2001)" — you couldn't tell WHICH
cut the theme was backed up / placed for.

enrich_item now takes edition_key and resolves the raw {edition-X} label
from the matching plex_items folder (title-cased edition_key fallback),
appending ' · <Edition>' to display_title — so every subject formatter
(added / pushed / backed-up / available / lost, all via
_safe_display_title) names the cut. '' (standard edition) appends nothing.
"""
from __future__ import annotations

import sqlite3

from app.config import Settings
from app.core.db import init_db
from app.core.events import now_iso
from app.core import notify_content as nc


NOW = now_iso()


def _settings(tmp_path):
    s = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(s.db_path)
    return s


def _seed(db):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year,"
            " upstream_source, last_seen_sync_at, first_seen_sync_at,"
            " youtube_url) VALUES ('movie',120,'The Lord of the Rings: The"
            " Fellowship of the Ring','2001','imdb',?,?,'u')", (NOW, NOW))
        conn.execute(
            "INSERT INTO plex_items (rating_key, section_id, media_type,"
            " theme_id, guid_tmdb, title, year, edition_key, folder_path,"
            " has_theme, first_seen_at, last_seen_at) VALUES ('676271','1',"
            "'movie',1,120,'LotR','2001','extended edition',"
            "'/data/media/movies/LotR (2001) {edition-Extended Edition}',1,"
            "?,?)", (NOW, NOW))
        conn.commit()


def test_edition_label_appended_to_subject(tmp_path):
    s = _settings(tmp_path)
    _seed(s.db_path)
    ctx = nc.enrich_item(s.db_path, media_type="movie", tmdb_id=120,
                         section_id="1", edition_key="extended edition")
    # Raw proper-case label from the folder, not the normalized key.
    assert ctx["edition"] == "Extended Edition"
    assert ctx["display_title"].endswith(" · Extended Edition")
    # It flows through to the actual notification subject lines.
    assert nc.format_theme_backed_up_title(ctx).endswith(
        "Fellowship of the Ring (2001) · Extended Edition")
    assert nc.format_theme_added_title(ctx).endswith("· Extended Edition")


def test_standard_edition_appends_nothing(tmp_path):
    s = _settings(tmp_path)
    _seed(s.db_path)
    ctx = nc.enrich_item(s.db_path, media_type="movie", tmdb_id=120,
                         section_id="1", edition_key="")
    assert ctx.get("edition", "") == ""
    assert ctx["display_title"].endswith("(2001)")
    assert "·" not in ctx["display_title"]


def test_unmatched_edition_falls_back_to_titlecased_key(tmp_path):
    """No plex_items folder for this key → title-case the key so the
    notification still names *a* cut rather than nothing."""
    s = _settings(tmp_path)
    _seed(s.db_path)
    ctx = nc.enrich_item(s.db_path, media_type="movie", tmdb_id=120,
                         section_id="1", edition_key="directors cut")
    assert ctx["edition"] == "Directors Cut"
    assert ctx["display_title"].endswith(" · Directors Cut")
