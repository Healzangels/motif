"""v1.23.98 (audit) — scanner._classify read the WRONG edition's canonical.

_classify's local_files lookup (reached when a themes match is found but the
scanned sidecar's sha matches no canonical) fetchone()'d an ARBITRARY edition's
row, keyed only on (media_type, tmdb_id). On a multi-edition title, a sidecar in
an edition folder motif does NOT yet track (no own canonical) found a SIBLING
edition's canonical → classified content_mismatch (a false /scans conflict)
instead of hash_match (adoptable as this edition's canonical). The read-side twin
of v1.22.22 #4 (that tag edition-scoped the plex_items UPDATE; this read was
missed).

Fix: scope the read to the scanned folder's edition (from media_folder) OR '',
preferring the exact edition.
"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from app.core.db import init_db
from app.core.scanner import (
    ScanContext, _classify, normalize_title, parse_folder_name)
from app.core.tmdb import TMDBClient

REPO = Path(__file__).resolve().parent.parent
SCANNER_PY = (REPO / "app" / "core" / "scanner.py").read_text()


def _ctx(db, themes_dir):
    return ScanContext(db_path=db, scan_run_id=1, themes_dir=themes_dir,
                       plus_mode="separator", tmdb=TMDBClient(None, db),
                       cancel_check=lambda: False)


def _seed_theme(conn, tmdb=500):
    conn.execute(
        "INSERT INTO themes (id, media_type, tmdb_id, title, upstream_source,"
        " last_seen_sync_at, first_seen_sync_at, youtube_url)"
        " VALUES (1,'movie',?,'Multi','imdb','t','t','https://y')", (tmdb,))


def _seed_local(conn, *, edition, sha, tmdb=500):
    conn.execute(
        "INSERT INTO local_files (media_type, tmdb_id, section_id, edition_key,"
        " file_path, downloaded_at, source_video_id, source_kind, file_sha256)"
        " VALUES ('movie', ?, '1', ?, ?, 't', 'V', 'adopt', ?)",
        (tmdb, edition, f"movie/Multi{edition}/theme.mp3", sha))


def _classify_extended_sidecar(db, themes_dir, tmp_path) -> str:
    """Scan a sidecar in the Extended folder; return the finding_kind."""
    media_folder = tmp_path / "Multi (2012) {edition-Extended}"
    media_folder.mkdir(parents=True, exist_ok=True)
    theme_file = media_folder / "theme.mp3"
    theme_file.write_bytes(b"scanned-extended-bytes")  # sha distinct from seeds
    ctx = _ctx(db, themes_dir)
    parsed = parse_folder_name(media_folder.name)
    title_norm = normalize_title(parsed.title, ctx.plus_mode)
    sha = hashlib.sha256(theme_file.read_bytes()).hexdigest()
    kind, _theme_id, _ = _classify(
        ctx, "movie", parsed, title_norm, theme_file,
        theme_file.stat().st_ino, sha, media_folder)
    return kind


def test_sibling_edition_canonical_does_not_falsely_mismatch(tmp_path):
    """Extended sidecar, motif tracks only a THEATRICAL canonical (a sibling) →
    adoptable (hash_match), NOT a false content_mismatch."""
    db = tmp_path / "motif.db"
    init_db(db)
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    with sqlite3.connect(db) as conn:
        _seed_theme(conn)
        _seed_local(conn, edition="theatrical", sha="aa" * 32)  # sibling only
        conn.commit()
    assert _classify_extended_sidecar(db, themes_dir, tmp_path) == "hash_match", (
        "v1.23.98: a sibling edition's canonical must NOT make the Extended "
        "sidecar read as content_mismatch")


def test_own_edition_canonical_still_flags_mismatch(tmp_path):
    """Control: when the Extended edition DOES have its own (different) canonical,
    the scanned Extended sidecar correctly reads content_mismatch."""
    db = tmp_path / "motif.db"
    init_db(db)
    themes_dir = tmp_path / "themes"
    themes_dir.mkdir()
    with sqlite3.connect(db) as conn:
        _seed_theme(conn)
        _seed_local(conn, edition="extended", sha="bb" * 32)  # own, diff bytes
        conn.commit()
    assert _classify_extended_sidecar(db, themes_dir, tmp_path) == "content_mismatch"


def test_classify_read_is_edition_scoped():
    assert "edition_key IN (?, '')" in SCANNER_PY
    assert "_scan_edition = edition_key_for_folder(str(media_folder))" in SCANNER_PY


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
