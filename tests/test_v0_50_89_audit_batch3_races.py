"""v0.50.89 — holistic-audit Batch 3: remaining race conditions.

1+2. scanner.py's module docstring falsely claimed scan and place/relink
     jobs never run concurrently ("they share the worker") — false since
     the v1.20.40 worker split. `_classify_and_record` now re-stats after
     hashing and discards the finding if the file changed mid-read (covers
     both the stale-docstring HIGH finding and the UNPLACE/PLACE TOCTOU
     MEDIUM finding — same code path, same fix).
3. downloader.py `_cookiefile_snapshot` used shutil.copyfile, which doesn't
   preserve permission bits — the throwaway /tmp copy of a live-session
   cookies file could be world-readable regardless of the source's mode.
4. config.py `Settings.config_write_lock` + api.py's PATCH /api/config now
   serialize the whole read-modify-write span so two concurrent partial
   saves can't silently lose one's change.
"""
from __future__ import annotations

import os
import stat
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCANNER_PY = (REPO / "app" / "core" / "scanner.py").read_text()
CONFIG_PY = (REPO / "app" / "config.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()


# ── 1+2. scanner.py re-stat-after-hash + corrected docstring ────────────

def test_scanner_docstring_no_longer_claims_scan_and_place_are_exclusive():
    assert "no place/relink jobs run" not in SCANNER_PY
    assert "they share the worker" not in SCANNER_PY
    assert "v0.50.89" in SCANNER_PY[:2500], (
        "the corrected concurrency docstring must be near the top of the file"
    )


def test_classify_and_record_rechecks_stat_after_hash():
    i = SCANNER_PY.index("def _classify_and_record(")
    j = SCANNER_PY.index("\ndef ", i + 10)
    body = SCANNER_PY[i:j]
    assert "st2 = theme_file.stat()" in body
    assert "st2.st_size != file_size or st2.st_mtime != st.st_mtime" in body


def test_classify_and_record_discards_finding_when_file_changes_during_hash(
    tmp_path, monkeypatch,
):
    """A file that changes size between the initial stat and the hash read
    (simulating a concurrent place/relink os.replace) must be discarded —
    no scan_findings row recorded."""
    from app.core.db import init_db, get_conn
    from app.core.events import now_iso
    import app.core.scanner as scanner

    db = tmp_path / "m.db"
    init_db(db)
    now = now_iso()
    media_folder = tmp_path / "T (2020)"
    media_folder.mkdir()
    theme_file = media_folder / "theme.mp3"
    theme_file.write_bytes(b"original content")

    with get_conn(db) as conn:
        conn.execute(
            "INSERT INTO plex_sections (section_id, title, type, included,"
            " discovered_at, last_seen_at, themes_subdir)"
            " VALUES ('1','Movies','movie',1,?,?,'movies')", (now, now))

    real_sha256_of = scanner._sha256_of

    def _sha256_of_that_mutates_file(path):
        # Simulate a concurrent place/relink replacing the file's content
        # WHILE the scanner is reading it.
        path.write_bytes(b"replaced by a concurrent place job!!")
        return real_sha256_of(path)

    monkeypatch.setattr(scanner, "_sha256_of", _sha256_of_that_mutates_file)

    ctx = scanner.ScanContext(
        db_path=db, scan_run_id=1, themes_dir=tmp_path, plus_mode=False,
        tmdb=None, cancel_check=lambda: False,
    )

    recorded = scanner._classify_and_record(
        ctx, "1", "movie", media_folder, theme_file,
    )

    assert recorded is False, (
        "v0.50.89: a file that changed size during hashing must be "
        "discarded, not recorded as a hybrid old-stat/new-hash row"
    )
    with get_conn(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM scan_findings"
        ).fetchone()[0]
    assert count == 0


# ── 3. downloader.py cookies snapshot permissions ───────────────────────

def test_cookiefile_snapshot_is_locked_to_owner_only(tmp_path):
    from app.core.downloader import _cookiefile_snapshot

    src = tmp_path / "cookies.txt"
    src.write_text("# Netscape HTTP Cookie File\n")
    os.chmod(src, 0o644)  # deliberately world-readable source

    tmp_str = _cookiefile_snapshot(src)
    mode = stat.S_IMODE(os.stat(tmp_str).st_mode)
    assert mode == 0o600, (
        f"v0.50.89: the cookies snapshot must be locked to 0600 regardless "
        f"of the source file's mode or the process umask, got {oct(mode)}"
    )
    os.unlink(tmp_str)


# ── 4. config.py PATCH /api/config serialization ────────────────────────

def test_settings_exposes_config_write_lock():
    assert "config_write_lock" in CONFIG_PY
    assert "return self._lock" in CONFIG_PY


def test_patch_config_holds_write_lock_across_read_modify_write():
    i = API_PY.index('@app.patch("/api/config")')
    j = API_PY.index("\n    @app.", i + 10)
    body = API_PY[i:j]
    lock_idx = body.index("with settings.config_write_lock:")
    deepcopy_idx = body.index("copy.deepcopy(settings.cfg)")
    save_idx = body.index("settings.save(cfg,")
    assert lock_idx < deepcopy_idx < save_idx, (
        "the lock must be held across the ENTIRE deepcopy-mutate-validate-"
        "save span, not just part of it"
    )


def test_concurrent_patch_config_does_not_lose_a_write(tmp_path, monkeypatch):
    """Two concurrent PATCH /api/config requests touching disjoint sections
    must BOTH survive — pre-fix, the second's full-object save silently
    reverted the first's already-applied change."""
    monkeypatch.setenv("MOTIF_TRUST_FORWARD_AUTH", "true")
    monkeypatch.setenv("MOTIF_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("MOTIF_DATA_DIR", str(tmp_path / "data"))
    from app.config import Settings
    from app.core.auth import create_admin, init_auth_schema
    from app.core.db import init_db
    from app.web.api import create_app
    from fastapi.testclient import TestClient

    settings = Settings(config_dir=tmp_path, data_dir=tmp_path / "data")
    init_db(settings.db_path)
    init_auth_schema(settings.db_path)
    create_admin(settings.db_path, username="testadmin", password="testpassword")
    client = TestClient(create_app(settings))
    auth = {"X-Authentik-Username": "testadmin"}

    # Slow down the read-modify-write window so the two requests are very
    # likely to actually overlap: patch Settings.save to sleep briefly
    # AFTER the deepcopy has already happened in each request (simulates
    # the real-world window between reading cfg and persisting it).
    real_save = Settings.save

    def _slow_save(self, cfg, *, updated_by):
        time.sleep(0.05)
        return real_save(self, cfg, updated_by=updated_by)

    monkeypatch.setattr(Settings, "save", _slow_save)

    results = {}

    def _patch_a():
        r = client.patch("/api/config",
                          json={"matching": {"plus_mode": "literal"}},
                          headers=auth)
        results["a"] = r.status_code

    def _patch_b():
        r = client.patch("/api/config",
                          json={"downloads": {"rate_per_hour": 77}},
                          headers=auth)
        results["b"] = r.status_code

    t1 = threading.Thread(target=_patch_a)
    t2 = threading.Thread(target=_patch_b)
    t1.start()
    time.sleep(0.01)  # ensure t1 is mid-save (past its deepcopy) when t2 starts
    t2.start()
    t1.join()
    t2.join()

    assert results.get("a") == 200
    assert results.get("b") == 200

    final = client.get("/api/config", headers=auth).json()["config"]
    assert final["matching"]["plus_mode"] == "literal", (
        "v0.50.89: request A's change must survive a concurrent PATCH — "
        f"got: {final.get('matching')}"
    )
    assert final["downloads"]["rate_per_hour"] == 77, (
        "v0.50.89: request B's change must survive a concurrent PATCH — "
        f"got: {final.get('downloads')}"
    )
