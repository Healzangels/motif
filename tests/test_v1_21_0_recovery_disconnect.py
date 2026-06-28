"""v1.21.0 — recovery_v55 boot walkers disconnected from startup.

Rollover cut. The 18 one-shot recovery walkers (v1.18.0 FK-cascade
data-loss recovery + the override / adopt / placement / sha256 / linkage
backfills) logged 16 INFO "marker already set — skipping" lines on every
boot once their runtime_settings markers were set — pure noise on every
current install. v1.21.0 REMOVES their 16 call sites from app/main.py
while RETAINING recovery_v55.py + its walker-logic tests as an archived,
callable safety net (re-wire a specific walker if a historical bug class
is ever re-introduced; full file deletion deferred to a real .99
line-close).

This single guard replaces the 17 per-walker `test_main_calls_* /
test_*_wired_into_main` wiring assertions (deleted — they pinned the
now-removed "main.py invokes walker X" contract). It pins the inverse:
the walkers stay DISCONNECTED but RETAINED.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
MAIN_PY = REPO / "app" / "main.py"


def test_main_no_longer_imports_recovery_walkers():
    src = MAIN_PY.read_text()
    # v1.24.34: the ONE sanctioned re-wire. The v1.21.0 retirement note says
    # "Re-wire a specific walker here if a historical bug class is ever
    # re-introduced" — the edition_key='' miskey did (it blocks the v1.24.28-29
    # RP push/auto-recovery, the Two Towers 409). Every OTHER recovery_v55
    # import stays disconnected: the only allowed import is the edition-coverage
    # one.
    import re
    imports = re.findall(r"from \.core\.recovery_v55 import (\w+)", src)
    assert imports in ([], ["maybe_backfill_edition_canonicals"]), (
        "v1.21.0/.24.34: only the v1.24.34 edition-coverage walker may be "
        f"re-wired into main.py; found {imports}")
    # And none of the OTHER (retired) walkers is invoked at boot.
    for call in (
        "maybe_recover_post_v55_data_loss(",
        "maybe_recover_lost_user_overrides(",
        "maybe_backfill_recovered_video_ids(",
        "maybe_recover_lost_bulk_imports(",
        "maybe_recover_lost_adopts(",
        "maybe_repair_adopt_provenance(",
        "maybe_cleanup_duplicate_placements(",
        "maybe_repair_stale_placements(",
        "maybe_backfill_file_sha256(",
        "maybe_relink_orphan_theme_ids(",
        "maybe_clear_plex_upload_indep_flag(",
    ):
        assert call not in src, f"v1.21.0: {call} must not run at boot"


def test_main_documents_the_retirement():
    src = MAIN_PY.read_text()
    assert "v1.21.0: the recovery_v55 boot walkers are RETIRED" in src
    # init_db → retirement comment → init_auth_schema are now adjacent.
    init_db = src.index("init_db(settings.db_path)")
    auth = src.index("init_auth_schema(settings.db_path)")
    assert init_db < src.index("RETIRED from the") < auth


def test_recovery_v55_retained_and_importable():
    """The file + walkers stay as an archived, callable safety net — the
    disconnect is wiring-only, not a deletion."""
    assert (REPO / "app" / "core" / "recovery_v55.py").is_file()
    from app.core import recovery_v55
    for fn in (
        "maybe_recover_post_v55_data_loss",
        "maybe_recover_lost_user_overrides",
        "maybe_recover_lost_adopts",
        "maybe_repair_stale_placements",
        "maybe_backfill_file_sha256",
        "maybe_relink_orphan_theme_ids",
    ):
        assert callable(getattr(recovery_v55, fn)), fn


def test_v1_21_0_version():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
