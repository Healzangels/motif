"""v0.51.329 — the SRC letter `AT` (AnimeThemes spec §3.8; §6 decision 3).

A placed row whose theme is a url download with an `at-` video id reads AT —
the branch BEFORE U in both classifiers, mirrored in the inline render. The
letter then follows the axis everywhere it renders: the filter allow-lists,
the filter pill + legend row (anime tab only), the dashboard donut and the
general-stats local set, the KEEP CURRENT tone, the INFO headline and history
baseline, the notification provenance. Backups stay UB in LINK.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from app.web.api import _SRC_LETTER_SQL, _src_letter_sql  # noqa: E402

APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()
API_PY = (REPO / "app" / "web" / "api.py").read_text()
LIBRARY = (REPO / "app" / "web" / "templates" / "library.html").read_text()
NOTIFY = (REPO / "app" / "core" / "notify_content.py").read_text()


# ── the SQL classifier, evaluated ─────────────────────────────


def _letter(*, media_folder, source_kind, svid, provenance="manual", upstream="themoviedb",
            local_theme_file=0, has_theme=0):
    """Evaluate the CASE against one synthetic (p, lf, t, pi) row."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE p (media_folder TEXT, provenance TEXT)")
    conn.execute("CREATE TABLE lf (source_kind TEXT, source_video_id TEXT)")
    conn.execute("CREATE TABLE t (upstream_source TEXT)")
    conn.execute("CREATE TABLE pi (local_theme_file INTEGER, has_theme INTEGER, plex_theme_verified_ok INTEGER)")
    conn.execute("INSERT INTO p VALUES (?, ?)", (media_folder, provenance))
    conn.execute("INSERT INTO lf VALUES (?, ?)", (source_kind, svid))
    conn.execute("INSERT INTO t VALUES (?)", (upstream,))
    conn.execute("INSERT INTO pi VALUES (?, ?, NULL)", (local_theme_file, has_theme))
    return conn.execute(f"SELECT ({_SRC_LETTER_SQL}) FROM p, lf, t, pi").fetchone()[0]


def test_sql_classifier_reads_a_placed_animethemes_pick_as_at():
    assert _letter(media_folder="/x", source_kind="url", svid="at-Bleach-OP1") == "AT"
    assert _letter(media_folder="", source_kind="url", svid="at-Bleach-OP1") == "AT", "plex_upload placements too"
    assert _letter(media_folder="/x", source_kind="url", svid="dQw4w9WgXcQ") == "U", "a YouTube url stays U"
    assert _letter(media_folder="/x", source_kind="upload", svid="at-Fake") == "U", "an upload is never AT"
    assert _letter(media_folder=None, source_kind="url", svid="at-Bleach-OP1") == "-", "unplaced: the SRC column says what plays"
    assert _letter(media_folder=None, source_kind="url", svid="at-Bleach-OP1", has_theme=1) == "P"
    assert _letter(media_folder="/x", source_kind="themerrdb", svid="abc") == "T"


def test_at_branch_sits_before_u_in_both_classifiers():
    sql = _src_letter_sql()
    assert sql.index("LIKE 'at-%' THEN 'AT'") < sql.index("IN ('url','upload') THEN 'U'")
    fn = slice_between(APP_JS, "function computeSrcLetter(it) {", "\n  }")
    assert fn.index("svid.startsWith('at-')) return 'AT';") < fn.index("|| sourceKind === 'upload')) return 'U';")
    ed = _src_letter_sql(media_folder="COALESCE(p_e.media_folder, p_g.media_folder)",
                         source_video_id="COALESCE(p_e.svid, p_g.svid)")
    assert "COALESCE(p_e.svid, p_g.svid) LIKE 'at-%' THEN 'AT'" in ed, "the edition-aware form carries it"


# ── the JS mirror, executed ───────────────────────────────────


def _js_letter(it: dict) -> str:
    quickjs = pytest.importorskip("quickjs")
    fn = slice_between(APP_JS, "  function computeSrcLetter(it) {", "\n  }") + "\n  }"
    return quickjs.Context().eval(f"{fn}\ncomputeSrcLetter({json.dumps(it)});")


def test_js_classifier_mirrors_the_sql():
    base = {"media_folder": "/x", "source_kind": "url", "source_video_id": "at-Bleach-OP1"}
    assert _js_letter(base) == "AT"
    assert _js_letter({**base, "media_folder": "", "placement_kind": "plex_upload"}) == "AT"
    assert _js_letter({**base, "source_video_id": "dQw4w9WgXcQ"}) == "U"
    assert _js_letter({**base, "source_kind": "upload"}) == "U"
    assert _js_letter({**base, "media_folder": None}) == "-"
    assert _js_letter({**base, "media_folder": None, "plex_has_theme": 1}) == "P"


def test_inline_render_tone_and_predicates_follow():
    render = slice_between(APP_JS, "    let srcCell;", "    } else if (sidecarOnly) {")
    assert render.index("svid.startsWith('at-')) {") < render.index("link-badge-user")
    assert 'class="link-badge link-badge-animethemes" title="AnimeThemes — an opening picked from AnimeThemes.moe.">AT</span>' in render
    assert "AT: 'animethemes'" in slice_between(APP_JS, "const SRC_LETTER_TONE = {", "};")
    assert "['U', 'AT'].includes(computeSrcLetter(rowItem))" in APP_JS, "an AnimeThemes pick is a user URL to the SET URL dialog"
    assert "{ letter: 'AT', cls: 'AT', name: 'AnimeThemes' }" in APP_JS
    assert "_GS_LOCAL_LETTERS = new Set(['T', 'A', 'U', 'AT', 'M'])" in APP_JS, "else AT rows fold into 'missing'"


def test_card_wording_keys_on_the_at_id():
    assert "? 'AnimeThemes theme' : _heldWord(lf.source_kind || '');" in APP_JS
    assert "? 'Downloaded from AnimeThemes' : _humanSourceKind(lf.source_kind || '')" in APP_JS


# ── the filter, the pills, the tones ──────────────────────────


def test_filter_allow_lists_admit_at():
    assert 'src_set = _pset(src_pills, {"T", "U", "AT", "A", "M", "P", "Pp", "-"})' in API_PY
    assert 'valid_letters = {"T", "U", "AT", "A", "M", "P", "-"}' in API_PY


def test_filter_pill_and_legend_row_are_anime_only_without_a_tab_gate():
    pill = slice_between(LIBRARY, 'data-src-filter="AT"', "</button>")
    assert 'data-tab-only="anime"' in pill and "{{ '' if tab == 'anime' else 'display:none' }}" in pill
    assert LIBRARY.index('data-src-filter="U"') < LIBRARY.index('data-src-filter="AT"') < LIBRARY.index('data-src-filter="A"')
    gloss = slice_between(LIBRARY, '<span class="link-badge link-badge-animethemes">AT</span>', "</div>")
    assert "AnimeThemes.moe opening" in gloss
    assert LIBRARY.count('link-badge-animethemes') == 2


def test_css_tones_are_the_anime_magenta():
    pill = slice_between(APP_CSS, ".link-badge-animethemes {", "}")
    assert "var(--magenta)" in pill and "--magenta-rgb" in pill and "#" not in pill
    assert ".btn.lib-source-animethemes {" in APP_CSS and ".btn.lib-source-animethemes:hover:not(:disabled) {" in APP_CSS
    for sel in (".source-pie-AT { stroke: var(--magenta); }", ".source-pie-AT-text { color: var(--magenta); }",
                ".source-legend-swatch-AT { background: var(--magenta); }"):
        assert sel in APP_CSS, sel


def test_notification_names_animethemes():
    assert '"animethemes": "AnimeThemes",' in NOTIFY
    assert 'ctx["provenance"] = "animethemes"' in NOTIFY


def test_v0_51_329_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.329: the SRC letter `AT`" in init_py
