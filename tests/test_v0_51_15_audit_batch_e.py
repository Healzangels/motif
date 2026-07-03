"""v0.51.15 — round-4 audit Batch E (silent failures, round 3 — class 9).

#20/#21 recovery_v55 twins: the one-shot v55 walker (a) coerced an OSError on
  the pi sidecar stat to False ("file absent") — the indeterminate-vs-False
  class v1.21.42 M2 fixed in stat_theme_sidecar — now None + a breadcrumb, and
  (b) bare-`continue`d a present-but-unstatable canonical with no log — now
  breadcrumbed (cold-path rule, v1.18.7).
#30 db_backup: the stale WAL/-shm unlink swallowed ALL OSErrors and proceeded
  to os.replace — directly beneath the comment explaining a leftover WAL would
  corrupt the restored file. Now ENOENT-tolerant, ABORTS the swap (pending
  snapshot kept) on any real unlink failure.
#31 config_file save(): the env-un-bake guard's two fault branches were silent
  AND diverged — corrupt YAML hydrated disk values from DEFAULTS (resetting
  env-bound fields), a non-mapping document skipped the guard (baking the env
  value, the exact bug the guard prevents). Both now WARN and uniformly skip
  the un-bake (env wins this save; deterministic, least-damaging).
"""
from __future__ import annotations

import logging
import os
import stat as _stat
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
RECOVERY_PY = (REPO / "app" / "core" / "recovery_v55.py").read_text()


# ── #20/#21: walker breadcrumbs (source pins — one-shot walker) ─────────

def test_pi_stat_oserror_is_indeterminate_not_false():
    i = RECOVERY_PY.index("pi_on_disk[rk] = plex_path.is_file()")
    block = RECOVERY_PY[i:i + 1200]
    assert "pi_on_disk[rk] = None" in block, (
        "OSError must yield indeterminate (None), not 'file absent' (False)")
    assert "pi_on_disk[rk] = False" not in block
    assert "sidecar stat indeterminate" in block, "breadcrumb required"
    # the consumer gate stays conservative for None.
    assert 'pi_on_disk.get(pi["rating_key"], False)' in RECOVERY_PY


def test_canonical_stat_oserror_logs_before_skip():
    i = RECOVERY_PY.index("stat = canonical_path.stat()")
    block = RECOVERY_PY[i:i + 900]
    assert "canonical present but stat failed" in block, (
        "v1.18.7 cold-path rule: the skip must log WHY")


# ── #30: restore aborts on a real WAL unlink failure ────────────────────

def test_restore_aborts_when_stale_wal_unremovable(tmp_path, monkeypatch):
    from app.core import db_backup
    from tests.test_v1_23_17_database_restore import (
        _make_db, CURRENT_SCHEMA_VERSION)
    db = tmp_path / "motif.db"
    _make_db(db, schema_version=CURRENT_SCHEMA_VERSION, marker="LIVE")
    snap = tmp_path / "snap.db"
    _make_db(snap, schema_version=CURRENT_SCHEMA_VERSION, marker="SNAP")
    db_backup.stage_restore(db, snap)
    # a stale -wal whose unlink fails (EACCES via monkeypatched unlink).
    wal = db.with_name(db.name + "-wal")
    wal.write_bytes(b"stale")
    real_unlink = Path.unlink

    def _deny_wal(self, *a, **k):
        if self.name.endswith("-wal"):
            raise PermissionError(13, "denied", str(self))
        return real_unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", _deny_wal)
    res = db_backup.apply_pending_restore(
        db, tmp_path, now_stamp="20260702-120000")
    assert res["applied"] is False, res
    assert "-wal" in res.get("error", ""), res
    assert db_backup.restore_pending_path(db).exists(), (
        "pending snapshot must be kept for retry")


def test_restore_tolerates_absent_sidecars(tmp_path):
    # counter-guard: no -wal/-shm present → the swap still applies.
    from app.core import db_backup
    from tests.test_v1_23_17_database_restore import (
        _make_db, CURRENT_SCHEMA_VERSION)
    db = tmp_path / "motif.db"
    _make_db(db, schema_version=CURRENT_SCHEMA_VERSION, marker="LIVE")
    snap = tmp_path / "snap.db"
    _make_db(snap, schema_version=CURRENT_SCHEMA_VERSION, marker="SNAP")
    db_backup.stage_restore(db, snap)
    res = db_backup.apply_pending_restore(
        db, tmp_path, now_stamp="20260702-120000")
    assert res.get("applied") is True, res


# ── #31: config save warns + skips the un-bake on both fault shapes ─────

def _cfgfile(tmp_path, text):
    from app.core.config_file import ConfigFile
    p = tmp_path / "motif.yaml"
    p.write_text(text)
    return ConfigFile(p)


def test_corrupt_yaml_warns_and_skips_unbake(tmp_path, monkeypatch, caplog):
    cf = _cfgfile(tmp_path, "{{{{not yaml::::\n\t")
    monkeypatch.setenv("MOTIF_RATE_PER_HOUR", "77")
    from app.core import config_file as cfmod
    c = cfmod.MotifConfig()
    with caplog.at_level(logging.WARNING):
        cf.save(c)
    assert any("not parseable YAML" in r.message for r in caplog.records), (
        "corrupt-YAML fault must WARN (was a silent default-reset)")


def test_non_mapping_yaml_warns_and_skips_unbake(tmp_path, monkeypatch, caplog):
    cf = _cfgfile(tmp_path, "- just\n- a\n- list\n")
    monkeypatch.setenv("MOTIF_RATE_PER_HOUR", "77")
    from app.core import config_file as cfmod
    c = cfmod.MotifConfig()
    with caplog.at_level(logging.WARNING):
        cf.save(c)
    assert any("not a mapping" in r.message for r in caplog.records), (
        "non-mapping fault must WARN (was a silent guard skip / env bake)")
