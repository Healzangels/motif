"""v0.51.338 — a downloaded AnimeThemes pick notifies as AnimeThemes, not User URL.

enrich_item step 2 set provenance='animethemes' from the override URL, but step 3
(consulted whenever a local_files row exists) mapped source_kind='url' straight to
'user_url' and overwrote it. Every downloaded AT pick IS such a row (source_kind='url',
source_video_id 'at-<slug>'), so theme_added / pushed / backed_up / auto_restored /
backup_ready_to_deploy / deleted all read "Source: User URL". Step 3 now ranks the at- id
first, like the SRC classifiers; step 2 classifies the override by host, not substring.
"""
from __future__ import annotations

from pathlib import Path

AT_URL = "https://a.animethemes.moe/Bleach-OP1.ogg"
YT_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
TS = "2026-01-01T00:00:00Z"  # a stored stamp only; nothing here reads a now-relative window


def _db(tmp_path: Path) -> Path:
    from app.core.db import get_conn, init_db
    db = tmp_path / "motif.db"
    init_db(db)
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included, discovered_at, last_seen_at) "
            "VALUES ('3', 'Anime', 'show', 1, ?, ?)", (TS, TS))
        conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, title, year, upstream_source, "
            "                    last_seen_sync_at, first_seen_sync_at) "
            "VALUES ('tv', 30991, 'Bleach', '2004', 'themoviedb', ?, ?)", (TS, TS))
    return db


def _override(db: Path, url: str) -> None:
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO user_overrides (media_type, tmdb_id, youtube_url, set_at, section_id) "
            "VALUES ('tv', 30991, ?, ?, '')", (url, TS))


def _local_file(db: Path, *, source_video_id: str, source_kind: str = "url") -> None:
    from app.core.db import get_conn
    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO local_files (media_type, tmdb_id, section_id, file_path, downloaded_at, "
            "                         source_video_id, provenance, source_kind) "
            "VALUES ('tv', 30991, '3', 'tv/Bleach/theme.mp3', ?, ?, 'manual', ?)",
            (TS, source_video_id, source_kind))


def _enrich(db: Path):
    from app.core.notify_content import enrich_item
    return enrich_item(db, media_type="tv", tmdb_id=30991, section_id="3")


def _label_part(line: str) -> str:
    # The platform suffix after " · " names AnimeThemes from the URL alone, so only the part before it proves the provenance.
    assert line.startswith("Source: "), line
    return line.split(" · ")[0]


def test_downloaded_at_pick_notifies_as_animethemes(tmp_path):
    from app.core.notify_content import _format_provenance_line, format_theme_added_body
    db = _db(tmp_path)
    _override(db, AT_URL)
    _local_file(db, source_video_id="at-Bleach-OP1")
    ctx = _enrich(db)
    assert ctx["provenance"] == "animethemes", "the local_files url row overwrote the AT provenance"
    head = _label_part(_format_provenance_line(ctx))
    assert "AnimeThemes" in head and "User URL" not in head, head
    assert "User URL" not in format_theme_added_body(ctx)


def test_at_row_survives_a_cleared_override(tmp_path):
    db = _db(tmp_path)
    _local_file(db, source_video_id="at-Bleach-OP1")
    assert _enrich(db)["provenance"] == "animethemes", "the at- id alone names the AT family, as the SRC letter does"


def test_plain_user_url_row_stays_user_url(tmp_path):
    from app.core.notify_content import _format_provenance_line
    db = _db(tmp_path)
    _override(db, YT_URL)
    _local_file(db, source_video_id="dQw4w9WgXcQ")
    ctx = _enrich(db)
    assert ctx["provenance"] == "user_url"
    assert "User URL" in _label_part(_format_provenance_line(ctx))


def test_url_row_without_override_stays_user_url(tmp_path):
    db = _db(tmp_path)
    _local_file(db, source_video_id="sc-artist-song")
    assert _enrich(db)["provenance"] == "user_url"


def test_at_override_without_local_files_row_is_animethemes(tmp_path):
    db = _db(tmp_path)
    _override(db, AT_URL)
    assert _enrich(db)["provenance"] == "animethemes"


def test_at_host_elsewhere_in_the_url_is_not_animethemes(tmp_path):
    db = _db(tmp_path)
    _override(db, "https://example.com/mirror/a.animethemes.moe/Bleach-OP1.ogg")
    assert _enrich(db)["provenance"] == "user_url", "only the a.animethemes.moe HOST is the AT family"
