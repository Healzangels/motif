"""v1.20.42 — holistic-audit follow-ups (class-9 breadcrumbs + SRC drift).

From the 2026-05-30 multi-agent audit. Four verified findings:
  #1 adopt.py — hardlink→copy fallback swallowed all OSError silently.
  #2 api.py restore — re-hash failure silently wrote stale size/sha.
  #4 db.py — title_norm backfill migration silently used lossy .lower().
  #5 SRC-axis mirror-drift — _SRC_LETTER_SQL had an sc-% branch but no
     ig-%; the JS computeSrcLetter sites had neither.
(Finding #3 — placement.py/worker.py hardlink→copy — already log.warning
with the errno, so it was a phantom and is not touched.)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
ADOPT_PY = (REPO / "app" / "core" / "adopt.py").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
DB_PY = (REPO / "app" / "core" / "db.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


# ── #1 adopt hardlink→copy breadcrumb ────────────────────────


def test_adopt_link_fail_surfaces_non_exdev():
    """The canonical-exists relink branch must log non-EXDEV link
    failures (silent disk-bloat trap otherwise)."""
    anchor = ADOPT_PY.index("canonical_path.unlink(missing_ok=True)")
    block = ADOPT_PY[anchor - 400:anchor + 60]
    assert "e.errno != 18" in block
    assert "log.warning" in block
    assert "fell back to copy" in block


# ── #2 restore re-hash breadcrumb ────────────────────────────


def test_restore_rehash_failure_logged():
    assert "re-hash failed for" in API_PY
    anchor = API_PY.index("re-hash failed for")
    block = API_PY[anchor - 120:anchor + 80]
    assert "log.warning" in block


# ── #4 title_norm migration breadcrumb ───────────────────────


def test_titlenorm_backfill_counts_lossy_fallback():
    anchor = DB_PY.index("from .normalize import normalize_title")
    block = DB_PY[anchor:anchor + 1400]
    assert "_lossy" in block
    assert "_lossy += 1" in block
    assert "if _lossy:" in block
    assert "log.warning" in block


# ── #5 SRC-axis ig-/sc- mirror-drift guard ───────────────────


def test_src_sql_classifies_ig_and_sc_as_url():
    """_SRC_LETTER_SQL's manual-orphan U-branch must recognize BOTH
    sc-% and ig-% source_video_ids (else they mis-tag as 'A')."""
    # v1.21.57: check the rendered _SRC_LETTER_SQL constant (byte-identical
    # default) rather than the source text, which now uses f-string params.
    from app.web.api import _SRC_LETTER_SQL as block
    assert "LIKE 'sc-%'" in block
    assert "LIKE 'ig-%'" in block


def test_src_js_sites_classify_ig_and_sc_as_url():
    """All three JS svid-classification sites (computeSrcLetter, the
    inline-SRC render, sourceKindForActions) must mirror the SQL — treat
    sc-/ig- ids as user URLs, not adopt. Guards the mirror-drift contract."""
    n = APP_JS.count("svid.startsWith('sc-') || svid.startsWith('ig-')")
    assert n >= 3, (
        f"expected sc-/ig- classification at all 3 JS sites, found {n}"
    )


def test_v1_20_42_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
