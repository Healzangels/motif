"""v0.51.344: the raw-budget refusal says what it measured — true whether the trip lands inside the archive or after it."""
from __future__ import annotations

import gzip
import tarfile

import pytest

from app.core import bundle
from tests.test_v0_51_339_bundle_staging_boot import _bundle, _live
from tests.test_v0_51_342_bundle_tail_bound import _EMPTY_MEMBER, NAME, TAIL_BOUND, _place, _tar_bytes, _within


def test_empty_gzip_members_inside_the_archive_are_refused_in_words_true_there(tmp_path, monkeypatch):
    b = _bundle(tmp_path / "mk")
    db, cd = _live(tmp_path)
    tar = _tar_bytes(b)
    flood = _EMPTY_MEMBER * (bundle._TAIL_RAW_BUDGET * 6 // 5 // len(_EMPTY_MEMBER))
    bad = _place(cd, NAME, gzip.compress(tar[:tarfile.BLOCKSIZE]) + flood + gzip.compress(tar[tarfile.BLOCKSIZE:]))
    judged: list[str] = []
    real = bundle._member_refusal
    monkeypatch.setattr(bundle, "_member_refusal", lambda m, seen: judged.append(m.name) or real(m, seen))
    with _within(TAIL_BOUND):
        c = bundle.inspect_bundle(bad)
    assert judged == [], f"the premise: the trip lands before the archive's first member is judged ({judged})"
    assert c.ok is False and c.error.startswith("not a motif bundle:") and c.error.count("not a motif bundle:") == 1, c.error
    assert str(bundle._TAIL_RAW_BUDGET) in c.error and "padding or empty gzip members" in c.error, c.error
    assert "after the archive's end" not in c.error, "the trip landed inside the archive"
    with _within(TAIL_BOUND), pytest.raises(ValueError) as refused:
        bundle.stage_bundle_restore(db, cd, bad, keep_config=False)
    assert str(refused.value) == c.error
    assert bundle.pending_members(db, cd) == [] and not bundle.STAGING_LOCK.locked()
