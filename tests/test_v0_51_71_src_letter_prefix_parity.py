"""v0.51.71 — SRC-letter source-video-id prefix family agrees across SQL + JS.

Complexity/regression audit follow-up (mirror-drift class). The SRC letter (T/A/U/M/
P/-) is derived twice: server-side in _src_letter_sql (api.py) and client-side in
computeSrcLetter (app.js, which carries the predicate in three svid-classification
blocks). The "is this a user URL, not an adopt" test keys on a GROWING family of
source-video-id prefixes — sc- (SoundCloud, v1.14.0), ig- (Instagram, v1.20.42),
fb- (Facebook, v1.22.90) — set by extract_video_id in sync.py. Each new source had to
be added to BOTH sides (and all three JS blocks) by hand; the v1.22.90 Facebook add
"had to touch both sides." A prefix added to one side only makes the row's SRC badge
disagree with server-side SRC filtering and the source-pie donut, silently, with a
green suite.

This guard extracts the prefix family from the stable, motif-specific literals on each
side (SQL `LIKE 'xx-%'`, JS `svid.startsWith('xx-')`) and asserts they all agree —
SQL vs every JS OR-chain. Robust (specific anchors, tokens that appear nowhere else),
NOT a char-window pin. Passes today ({fb, ig, sc} on both); exists to fail loud when a
5th source is added to one side but not the others.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def _sql_prefixes() -> set[str]:
    # scope to the _src_letter_sql function body (the single SQL source of truth).
    i = API_PY.index("def _src_letter_sql(")
    j = API_PY.index("_SRC_LETTER_SQL = _src_letter_sql()")
    return set(re.findall(r"LIKE '([a-z]{2,3})-%'", API_PY[i:j]))


# each JS svid-classification block is an OR-chain of svid.startsWith('xx-') calls.
_CHAIN = re.compile(r"(?:svid\.startsWith\('[a-z]{2,3}-'\)\s*\|\|\s*)*"
                    r"svid\.startsWith\('[a-z]{2,3}-'\)")


def _js_chains() -> list[set[str]]:
    return [set(re.findall(r"startsWith\('([a-z]{2,3})-'\)", c))
            for c in _CHAIN.findall(APP_JS)]


def test_src_letter_prefix_family_agrees_sql_vs_js():
    sql = _sql_prefixes()
    chains = _js_chains()
    # sanity: extractors found real data (a refactor can't make this vacuous).
    assert len(sql) >= 3, f"SQL svid-prefix set parsed too small: {sql}"
    assert len(chains) >= 3, (
        f"expected >=3 JS svid.startsWith OR-chains (computeSrcLetter x2 + the "
        f"source-kind classifier), found {len(chains)} — did the shape change?")
    for idx, js in enumerate(chains):
        assert js == sql, (
            f"SRC-letter source-prefix drift: SQL _src_letter_sql has {sorted(sql)} but "
            f"JS svid.startsWith chain #{idx} has {sorted(js)}. A new theme source (or a "
            f"removed one) must be mirrored in _src_letter_sql AND every computeSrcLetter "
            f"block, or the row badge disagrees with server SRC filtering + the source pie.")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
