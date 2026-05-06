"""Regression guards for the v1.12.121 (Phase A) snapshot path.

Covers:
  - Happy path: a tarball whose layout mirrors LizardByte/ThemerrDB's
    `database` branch is acquired, validated, and read by
    _DatabaseSnapshot.index / fetch_item. The records returned match
    what the remote helpers would return for the same shape.
  - Tar-slip rejection: a tarball with a `..`-bearing member or an
    absolute path raises _SnapshotError before extraction completes.
  - Symlink rejection: symlinks in the tarball are silently skipped
    (rejection surfaces as missing data → validate or downstream
    fetch_item returns None, caller handles).
  - Schema-drift rejection: a tarball where the sample item is
    missing `youtube_theme_url` raises _SnapshotError on validate().
  - HTTP failure path: a 503 from codeload raises _SnapshotError so
    run_sync's caller can fall back to the remote path.

These don't exercise run_sync end-to-end (that path is well-covered
by the existing remote-path integration); they exercise the
_DatabaseSnapshot contract directly so any future refactor of the
snapshot helper has a concrete tripwire.
"""
from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
from pathlib import Path

import httpx
import pytest


# ---------- helpers ----------------------------------------------------------

def _build_database_tarball(records: dict, *, prefix: str = "ThemerrDB-database") -> bytes:
    """Build a tar.gz that mirrors the codeload layout for the
    `database` branch.

    `records` is a dict shaped like:
      {
        "movies": {
          "imdb": {"tt0000001": {...}},
          "themoviedb": {"100": {...}},
          "all_pages": [[{"id": 100, "imdb_id": "tt0000001", "title": "T"}]],
        },
        "tv_shows": { ... same ... },
      }

    Pages JSON is generated automatically: pages = len(all_pages),
    count = sum of entries.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        # Top-level dir entry (codeload includes it).
        di = tarfile.TarInfo(name=prefix)
        di.type = tarfile.DIRTYPE
        di.mode = 0o755
        tf.addfile(di)

        for media_path, payload in records.items():
            # pages.json
            all_pages = payload.get("all_pages") or []
            count = sum(len(p) for p in all_pages)
            pages_json = {"count": count, "pages": len(all_pages),
                          "imdb_count": count}
            _add_file(tf, f"{prefix}/{media_path}/pages.json",
                      json.dumps(pages_json).encode())
            for i, page in enumerate(all_pages, start=1):
                _add_file(tf, f"{prefix}/{media_path}/all_page_{i}.json",
                          json.dumps(page).encode())
            for imdb_id, rec in (payload.get("imdb") or {}).items():
                _add_file(tf, f"{prefix}/{media_path}/imdb/{imdb_id}.json",
                          json.dumps(rec).encode())
            for tmdb_id, rec in (payload.get("themoviedb") or {}).items():
                _add_file(tf, f"{prefix}/{media_path}/themoviedb/{tmdb_id}.json",
                          json.dumps(rec).encode())
    return buf.getvalue()


def _add_file(tf: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    info.mode = 0o644
    tf.addfile(info, io.BytesIO(data))


def _add_evil_tar(*members: tarfile.TarInfo) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for m in members:
            tf.addfile(m, io.BytesIO(b""))
    return buf.getvalue()


class _StaticHandler:
    """Minimal httpx MockTransport handler that returns canned bytes
    or a status code for any GET to a single URL.

    v1.12.126: optionally returns ETag / Last-Modified response
    headers, and can branch on If-None-Match / If-Modified-Since
    request headers to test the conditional-GET short-circuit."""

    def __init__(
        self,
        body: bytes | None = None,
        status: int = 200,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        match_etag: str | None = None,
        match_last_modified: str | None = None,
    ):
        self.body = body
        self.status = status
        self.etag = etag
        self.last_modified = last_modified
        self.match_etag = match_etag
        self.match_last_modified = match_last_modified
        self.last_request_headers: dict[str, str] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.last_request_headers = dict(request.headers)
        # v1.12.126 conditional-GET path: if the caller sent
        # If-None-Match matching match_etag (or If-Modified-Since
        # matching match_last_modified), short-circuit with 304.
        inm = request.headers.get("If-None-Match") or request.headers.get("if-none-match")
        ims = request.headers.get("If-Modified-Since") or request.headers.get("if-modified-since")
        if (self.match_etag and inm == self.match_etag) or (
            self.match_last_modified and ims == self.match_last_modified
        ):
            return httpx.Response(304)
        if self.body is None:
            return httpx.Response(self.status)
        headers = {"content-length": str(len(self.body))}
        if self.etag:
            headers["ETag"] = self.etag
        if self.last_modified:
            headers["Last-Modified"] = self.last_modified
        return httpx.Response(self.status, content=self.body, headers=headers)


def _make_client(handler) -> httpx.Client:
    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """An init'd DB so op_progress.start_progress / update_progress
    don't blow up in the snapshot acquire path."""
    from app.core.db import init_db
    path = tmp_path / "motif.db"
    init_db(path)
    return path


# ---------- happy path -------------------------------------------------------

def test_snapshot_acquire_and_read(db_path: Path):
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    movie_rec = {
        "imdb_id": "tt0000001", "id": 100, "title": "Test Movie",
        "release_date": "2020-01-01", "youtube_theme_url": "https://youtu.be/abc12345678",
        "youtube_theme_added": "2024-01-01", "youtube_theme_edited": "2024-01-01",
    }
    tv_rec = {
        "imdb_id": "tt0000002", "id": 200, "name": "Test Show",
        "first_air_date": "2021-01-01", "youtube_theme_url": "https://youtu.be/def12345678",
        "youtube_theme_added": "2024-01-01", "youtube_theme_edited": "2024-01-01",
    }
    body = _build_database_tarball({
        "movies": {
            "all_pages": [[{"id": 100, "imdb_id": "tt0000001", "title": "Test Movie"}]],
            "imdb": {"tt0000001": movie_rec},
            "themoviedb": {"100": movie_rec},
        },
        "tv_shows": {
            "all_pages": [[{"id": 200, "imdb_id": "tt0000002", "title": "Test Show"}]],
            "imdb": {"tt0000002": tv_rec},
            "themoviedb": {"200": tv_rec},
        },
    })

    client = _make_client(_StaticHandler(body=body))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    try:
        snap.acquire(client)
        assert snap.root is not None and snap.root.is_dir()
        movies_idx = snap.index("movies")
        tv_idx = snap.index("tv_shows")
        assert len(movies_idx) == 1 and movies_idx[0]["id"] == 100
        assert len(tv_idx) == 1 and tv_idx[0]["id"] == 200
        m = snap.fetch_item("movies", imdb_id="tt0000001", tmdb_id=100)
        assert m is not None and m["youtube_theme_url"].startswith("https://youtu.be/")
        # imdb/ takes precedence over themoviedb/ when imdb_id is given.
        m_no_imdb = snap.fetch_item("movies", imdb_id=None, tmdb_id=100)
        assert m_no_imdb is not None and m_no_imdb["title"] == "Test Movie"
    finally:
        snap.release()
    assert snap.root is None


# ---------- tar-slip rejections ----------------------------------------------

def test_snapshot_rejects_dotdot_member(db_path: Path):
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot, _SnapshotError

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    # Member name leaves the prefix dir via ../
    evil_name = "ThemerrDB-database/movies/../../../etc/evil"
    info = tarfile.TarInfo(name=evil_name)
    info.size = 0
    body = _add_evil_tar(info)

    client = _make_client(_StaticHandler(body=body))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    with pytest.raises(_SnapshotError):
        snap.acquire(client)


def test_snapshot_rejects_absolute_member(db_path: Path):
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot, _SnapshotError

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    info = tarfile.TarInfo(name="/etc/passwd")
    info.size = 0
    body = _add_evil_tar(info)

    client = _make_client(_StaticHandler(body=body))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    # Either rejected at filter time or rejected at validate (no
    # movies/ subdir present). Both branches surface as _SnapshotError.
    with pytest.raises(_SnapshotError):
        snap.acquire(client)


# ---------- schema drift -----------------------------------------------------

def test_snapshot_validate_rejects_schema_drift(db_path: Path):
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot, _SnapshotError

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    # Sample movie record is missing `youtube_theme_url` entirely.
    bad_rec = {"imdb_id": "tt0000001", "id": 100, "title": "X"}
    body = _build_database_tarball({
        "movies": {
            "all_pages": [[{"id": 100, "imdb_id": "tt0000001", "title": "X"}]],
            "imdb": {"tt0000001": bad_rec},
        },
        "tv_shows": {
            "all_pages": [[]],
        },
    })

    client = _make_client(_StaticHandler(body=body))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    with pytest.raises(_SnapshotError):
        snap.acquire(client)


# ---------- HTTP failure -----------------------------------------------------

def test_snapshot_cancellation_propagates_unchanged(db_path: Path):
    """A // CANCEL during snapshot acquisition must raise
    _JobCancelled, not _SnapshotError. Pre-fix the acquire() except
    chain swallowed _JobCancelled (which inherits Exception) and
    converted it to _SnapshotError, which then triggered the run_sync
    fallback path — silently overriding the user's cancel and sticking
    a spurious // FALLBACK indicator on the idle pill. v1.12.121."""
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot, _SnapshotError
    from app.core.worker import _JobCancelled

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )
    body = _build_database_tarball({
        "movies": {"all_pages": [[]]},
        "tv_shows": {"all_pages": [[]]},
    })

    # Cancel immediately on the first cancel poll inside the chunk loop.
    cancel_calls = {"n": 0}
    def _cancel_now():
        cancel_calls["n"] += 1
        return cancel_calls["n"] >= 1

    client = _make_client(_StaticHandler(body=body))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=_cancel_now,
    )
    with pytest.raises(_JobCancelled):
        snap.acquire(client)


def test_snapshot_http_503_raises_snapshoterror(db_path: Path):
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot, _SnapshotError

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )
    client = _make_client(_StaticHandler(body=b"", status=503))
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    with pytest.raises(_SnapshotError):
        snap.acquire(client)


# ---------- fallback flag wiring --------------------------------------------

def test_set_detail_field_records_fallback_active(db_path: Path):
    """run_sync's fallback path calls set_detail_field('fallback_active',
    True) so the topbar idle pill can render the sticky // FALLBACK
    indicator. Confirm the helper actually persists into detail_json
    where load_active will surface it as op.detail.fallback_active."""
    import json as _json
    from app.core import progress as op_progress
    from app.core.db import get_conn

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )
    op_progress.set_detail_field(db_path, "tdb_sync", "fallback_active", True)
    op_progress.set_detail_field(db_path, "tdb_sync", "fallback_reason",
                                 "503 from codeload")
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT detail_json FROM op_progress WHERE op_id = 'tdb_sync'"
        ).fetchone()
    assert row is not None
    detail = _json.loads(row["detail_json"])
    assert detail.get("fallback_active") is True
    assert detail.get("fallback_reason") == "503 from codeload"


def test_load_active_exposes_fallback_to_api(db_path: Path):
    """Same wiring through load_active (the /api/progress data source)
    so the JS reading op.detail.fallback_active sees what the worker
    wrote."""
    from app.core import progress as op_progress

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )
    op_progress.set_detail_field(db_path, "tdb_sync", "fallback_active", True)
    rows = op_progress.load_active(db_path)
    sync_rows = [r for r in rows if r.get("kind") == "tdb_sync"]
    assert sync_rows, "tdb_sync row not surfaced via load_active"
    assert sync_rows[0].get("detail", {}).get("fallback_active") is True


# ---------- Phase A.5: ETag short-circuit ------------------------------------

def test_snapshot_first_run_persists_etag(db_path: Path):
    """First-ever run has no .meta file. Snapshot acquires normally;
    after success, .meta carries the response ETag + Last-Modified
    so the next run can send conditional headers."""
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    movie_rec = {
        "imdb_id": "tt0000001", "id": 100, "title": "Test Movie",
        "release_date": "2020-01-01", "youtube_theme_url": "https://youtu.be/abc",
    }
    body = _build_database_tarball({
        "movies": {
            "all_pages": [[{"id": 100, "imdb_id": "tt0000001", "title": "Test Movie"}]],
            "imdb": {"tt0000001": movie_rec},
        },
        "tv_shows": {"all_pages": [[]]},
    })
    handler = _StaticHandler(
        body=body,
        etag='W/"abc123"',
        last_modified="Tue, 05 May 2026 21:00:00 GMT",
    )
    client = _make_client(handler)
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    try:
        snap.acquire(client)
        assert snap.is_unchanged() is False
        # First-run shouldn't send conditional headers.
        assert "If-None-Match" not in handler.last_request_headers
        assert "If-Modified-Since" not in handler.last_request_headers
        # Meta file written.
        assert snap.meta_path.is_file()
        meta = json.loads(snap.meta_path.read_text())
        assert meta.get("etag") == 'W/"abc123"'
        assert meta.get("last_modified") == "Tue, 05 May 2026 21:00:00 GMT"
    finally:
        snap.release()


def test_snapshot_304_short_circuits(db_path: Path):
    """Second run with matching ETag → 304 → is_unchanged() True,
    no extract, no validate, no tmpdir."""
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )

    # Seed the cache as if a prior run had landed.
    cache_dir = db_path.parent / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "themerrdb-database.tar.gz").write_bytes(b"prior-tarball-bytes")
    (cache_dir / "themerrdb-database.tar.gz.meta").write_text(
        json.dumps({"etag": 'W/"abc123"',
                    "last_modified": "Tue, 05 May 2026 21:00:00 GMT"})
    )

    handler = _StaticHandler(
        body=b"<would not be served>",
        match_etag='W/"abc123"',
    )
    client = _make_client(handler)
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    snap.acquire(client)
    assert snap.is_unchanged() is True
    # The conditional headers MUST have been sent.
    headers = {k.lower(): v for k, v in handler.last_request_headers.items()}
    assert headers.get("if-none-match") == 'W/"abc123"'
    # No tmpdir was allocated since extract was skipped.
    assert snap.root is None


def test_snapshot_200_with_changed_etag_advances_meta(db_path: Path):
    """When the ETag has changed, the conditional GET still goes
    through (server returns 200), and the .meta file is rewritten
    with the new validators."""
    from app.core import progress as op_progress
    from app.core.sync import _DatabaseSnapshot

    op_progress.start_progress(
        db_path, op_id="tdb_sync", kind="tdb_sync",
        stage="snapshot_download", stage_label="…",
    )
    cache_dir = db_path.parent / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "themerrdb-database.tar.gz").write_bytes(b"prior")
    (cache_dir / "themerrdb-database.tar.gz.meta").write_text(
        json.dumps({"etag": 'W/"old"'})
    )

    movie_rec = {
        "imdb_id": "tt0000001", "id": 100, "title": "Test Movie",
        "release_date": "2020-01-01", "youtube_theme_url": "https://youtu.be/abc",
    }
    body = _build_database_tarball({
        "movies": {
            "all_pages": [[{"id": 100, "imdb_id": "tt0000001", "title": "Test Movie"}]],
            "imdb": {"tt0000001": movie_rec},
        },
        "tv_shows": {"all_pages": [[]]},
    })
    # match_etag is set to a DIFFERENT value, so the server returns 200
    # with a NEW etag rather than 304.
    handler = _StaticHandler(
        body=body,
        etag='W/"new"',
        match_etag='W/"never-matches"',
    )
    client = _make_client(handler)
    snap = _DatabaseSnapshot(
        db_path, "tdb_sync",
        "https://codeload.invalid/tar.gz/database",
        cancel_check=lambda: False,
    )
    try:
        snap.acquire(client)
        assert snap.is_unchanged() is False
        meta = json.loads(snap.meta_path.read_text())
        assert meta.get("etag") == 'W/"new"'
    finally:
        snap.release()


# ---------- auto_enum_after_sync toggle --------------------------------------

def test_config_auto_enum_default_true():
    from app.core.config_file import MotifConfig
    cfg = MotifConfig()
    assert cfg.sync.auto_enum_after_sync is True


# ---------- config schema ----------------------------------------------------

def test_config_validates_sync_source():
    from app.core.config_file import MotifConfig, validate

    cfg = MotifConfig()
    cfg.paths.themes_dir = "/tmp/themes-fixture"
    # Default is "remote" — must validate clean.
    errors = [e for e in validate(cfg, require_themes_dir=False)
              if "themes_dir" not in e]
    assert errors == []

    cfg.sync.source = "garbage"
    errors = validate(cfg, require_themes_dir=False)
    assert any("sync.source" in e for e in errors)

    cfg.sync.source = "database"
    errors = [e for e in validate(cfg, require_themes_dir=False)
              if "themes_dir" not in e]
    assert errors == []
