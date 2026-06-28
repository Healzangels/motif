"""v1.24.74 — menu-actions mirror-drift lint.

`app/web/static/lib/menu-actions.js` is a HAND-MIRRORED, TEST-ONLY copy of
the SOURCE/PLACE/REMOVE gating that lives inline in `renderLibraryRow`
(app.js). It's exercised by `tests/js/test_menu_actions.js` but is NOT
loaded by any template — so when it drifts from the live app.js gate the
node harness silently UNDER-TESTS the real UI.

v1.24.74 found the REPLACE TDB mirror missing three clauses the live gate
has carried since v1.24.71 / v1.20.2 / v1.14.46 (`it.youtube_url`,
`tdbActionPendingOk`, `!lpsHasCanonical`), plus stale `redl` (`!isOrphan`,
v1.17.17), `revert` (the src='M' RESTORE branch, v1.12.81) and `purge`
(`orphanHasPurgeableState`, v1.18.8) gates.

## Why a lint, not delegation

Making app.js DELEGATE to `computeMenuActions` (single source of truth)
would eliminate the drift class outright, but was rejected as high-risk:
the lib is a deliberate SUBSET (no PU FILE/API split, SWITCH PLACEMENT,
DOWNLOAD TDB/PLEX BACKUP, ACK DROP / CONVERT TO MANUAL) and emits plain
`{act,label,tone}` objects, while app.js produces rich HTML with
tooltips/datasets/kind-variants. Reproducing that whole surface in the
mirror is a large rewrite of the most load-bearing UI. This lint instead
diffs the two gates' clause sets so the drift class fails CI cheaply.

Spiritual sibling of `test_v1_19_38_src_axis_sixth_site_drift` (the SRC-axis
mirror lint) and `test_v1_24_72_replace_tdb_after_accept` (the REPLACE TDB
accepted_update dimension).
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
LIB_JS = (REPO / "app" / "web" / "static" / "lib" / "menu-actions.js").read_text()


# ── Normalization ───────────────────────────────────────────────────────────


def _norm(s: str) -> str:
    """Canonicalize a JS gate snippet for cross-file comparison: drop `//`
    line comments, the `flags.` mirror prefix (the lib reads derived flags
    off a `flags` object; app.js has them as bare locals), unify quote style,
    and collapse whitespace."""
    s = re.sub(r"//[^\n]*", "", s)   # strip // line comments
    s = s.replace("flags.", "")      # lib uses flags.X, app.js bare X
    s = s.replace('"', "'")          # lib double-quotes, app.js single
    s = re.sub(r"\s+", "", s)        # collapse all whitespace
    return s


def _const_def(src: str, name: str) -> str:
    """Return the `const NAME = ...;` definition (first `;` terminates — none
    of the mirrored helpers embed a statement separator)."""
    m = re.search(rf"const {name} =.*?;", src, re.DOTALL)
    assert m, f"const {name} not found"
    return m.group(0)


# ── REPLACE TDB gate: clause-set parity ─────────────────────────────────────


def _app_replace_tdb_gate() -> str:
    idx = APP_JS.index("'replace-with-themerrdb', 'REPLACE TDB'")
    return APP_JS[APP_JS.rindex("if (isThemerrDb", 0, idx):idx]


def _lib_replace_tdb_gate() -> str:
    idx = LIB_JS.index('"replace-with-themerrdb"')
    return LIB_JS[LIB_JS.rindex("if (", 0, idx):idx]


# The clause set every REPLACE TDB gate must carry (normalized form). If the
# live gate grows/loses a clause, update BOTH gates and this list together.
REPLACE_TDB_CLAUSES = [
    "isThemerrDb",
    "it.youtube_url",
    "!tdbReplaceBlocked",
    "tdbActionPendingOk",
    "(!it.accepted_update||srcLetter!=='T')",
    "!lpsHasCanonical",
    "(sidecarOnly||isManualPlacement||isPlexAgent)",
]


def test_replace_tdb_gate_clause_set_matches_app_and_lib():
    app = _norm(_app_replace_tdb_gate())
    lib = _norm(_lib_replace_tdb_gate())
    for clause in REPLACE_TDB_CLAUSES:
        assert clause in app, (
            f"app.js REPLACE TDB gate lost clause `{clause}` — update "
            f"REPLACE_TDB_CLAUSES if this is intentional"
        )
        assert clause in lib, (
            f"lib/menu-actions.js REPLACE TDB mirror drifted — missing "
            f"clause `{clause}`. Re-sync with the app.js inline gate (see "
            f"the file header in lib/menu-actions.js)."
        )


# ── Named helpers behind the gate must MEAN the same thing ──────────────────

# The gate references these by name; identical definitions guarantee the
# named clauses (tdbActionPendingOk / !lpsHasCanonical) behave identically.
SHARED_HELPERS = [
    "tdbActionPendingOk",
    "isPlexCloudBackupRow",
    "hasNonCloudCanonical",
    "lpsHasCanonical",
]


def test_replace_tdb_helper_defs_match_between_app_and_lib():
    for name in SHARED_HELPERS:
        app_def = _norm(_const_def(APP_JS, name))
        lib_def = _norm(_const_def(LIB_JS, name))
        assert app_def == lib_def, (
            f"helper `{name}` differs between app.js and the mirror:\n"
            f"  app.js: {app_def}\n  lib   : {lib_def}"
        )


# ── Synced-in-v1.24.74 audit clauses stay pinned in both ────────────────────


def test_redl_gate_excludes_orphans_in_both():
    """RE-DOWNLOAD TDB must skip plex_orphan rows (v1.17.17) — they have no
    TDB URL to re-fetch. Pin `!isOrphan` in both gates."""
    app_idx = APP_JS.index(
        "&& !sidecarOnly && !isPlexAgent && !isManualPlacement"
    )
    # The `&& !isOrphan)` clause sits past the long v1.17.17 comment block
    # that explains it — widen the window to clear the prose.
    assert "&& !isOrphan)" in APP_JS[app_idx:app_idx + 1400], (
        "app.js redl gate must exclude orphans (!isOrphan)"
    )
    lib_idx = LIB_JS.index("!flags.sidecarOnly &&")
    assert "!flags.isOrphan" in LIB_JS[lib_idx:lib_idx + 600], (
        "lib redl mirror missing !flags.isOrphan (v1.17.17) — re-sync"
    )


def test_revert_restore_eligible_kind_has_M_branch_in_both():
    """REVERT/RESTORE eligibility includes the src='-'/'M' recovery branch
    (post-PURGE / post-UNMANAGE), not just previous_youtube_kind==='user'
    (v1.12.81). Pin the src='M' branch in both."""
    for label, src in (("app.js", APP_JS), ("lib", LIB_JS)):
        d = _norm(_const_def(src, "restoreEligibleKind"))
        assert "srcLetter==='M'" in d, (
            f"{label} restoreEligibleKind missing the src='M' RESTORE branch"
        )
        assert "previous_youtube_kind==='user'" in d, (
            f"{label} restoreEligibleKind missing the user-kind branch"
        )


def test_purge_orphan_purgeable_state_matches_between_app_and_lib():
    """PURGE on an orphan gates on `orphanHasPurgeableState`, not a bare
    `isOrphan` (v1.18.8 — a zombie orphan must not show a no-op PURGE). The
    derivation must be identical in both."""
    app_def = _norm(_const_def(APP_JS, "orphanHasPurgeableState"))
    lib_def = _norm(_const_def(LIB_JS, "orphanHasPurgeableState"))
    assert app_def == lib_def, (
        "purge `orphanHasPurgeableState` drifted between app.js and the "
        f"mirror:\n  app.js: {app_def}\n  lib   : {lib_def}"
    )
