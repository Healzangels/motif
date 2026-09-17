"""v0.51.344 (integration review R1-F15): with nothing on disk, the INFO card's headline names Plex as serving exactly when its Plex player shows — a verify 404 (verified_ok 0) says Plex serves no theme."""
from __future__ import annotations

from test_v0_51_343_infocard_builders import _BARE, _HEADLINE, _HELD, _HELPER, _cut, _node, needs_node

# The bare card's hero line and the full card's headline for one row state, beside the row ▶'s rule.
_HARNESS = r"""
const heroLine = (h) => { const m = h.match(/<p class="info-hero-playback muted small">([\s\S]*?)<\/p>/); return m ? m[1].trim() : null; };
const out = payload.cases.map((c) => {
  const row = Object.assign({ plex_title: 'X', theme_media_type: 'tv', theme_tmdb: 777, section_id: '3', rating_key: '1001',
    media_folder: null, placement_kind: null, file_path: null, canonical_missing: false, last_place_attempt_reason: null }, c.row);
  const bareHtml = run(`${payload.bare}\nrenderBareInfoCard(row);`, { row });
  const full = run(`${payload.helper}\n${payload.headline}\n_derivePlaybackSourceLabel();`,
                   { _ambiguousCut: false, lf: null, data: c.data, placements: [] });
  const q = lib.computeQuickPlay(row);
  return { bare: heroLine(bareHtml), barePlayer: /data-plex-theme="1"/.test(bareHtml), full, plays: q ? q.kind : null };
});
process.stdout.write(JSON.stringify(out));
"""


def _cells():
    return [(has, ok, ind) for has in (None, 0, 1) for ok in (None, 0, 1) for ind in (0, 1)]


def _case(cell):
    has, ok, ind = cell
    state = {"plex_has_theme": has, "plex_theme_verified_ok": ok, "plex_independent_theme": ind}
    return {"row": state, "data": {**state, "plex_rating_key": "1001"}}


def _claims_plex_serves(text: str) -> bool:
    return "Plex serves its own theme" in text or "no longer manages" in text


@needs_node
def test_the_no_file_headlines_name_plex_as_serving_exactly_when_the_plex_player_shows():
    cells = _cells()
    out = _node(_HARNESS, {"bare": _cut(_BARE), "helper": _cut(_HELPER, keep_end=True),
                           "headline": _cut(_HELD) + "\n    }\n" + _cut(_HEADLINE) + "\n    }",
                           "cases": [_case(c) for c in cells]})
    seen = set()
    for cell, got in zip(cells, out):
        has, ok, ind = cell
        plays_plex = got["plays"] == "plex"
        seen.add(plays_plex)
        assert got["bare"] and got["bare"].startswith("nothing on disk · "), (cell, got)
        assert got["full"].startswith("nothing on disk · "), (cell, got)
        assert got["barePlayer"] == plays_plex, ("premise: the bare card's player follows the row ▶", cell, got)
        for surface in ("bare", "full"):
            assert _claims_plex_serves(got[surface]) == plays_plex, (surface, cell, got)
            assert ("Plex serves no theme" in got[surface]) == (has == 1 and ok == 0), (surface, cell, got)
        if has == 1 and ok == 0:
            for surface in ("bare", "full"):
                assert "verify" in got[surface], (surface, cell, got)
        if plays_plex:
            assert ("no longer manages" in got["full"]) == (ind == 0), ("the UNMANAGE hint keys on the flag alone", cell, got)
        if has is None:
            assert "Plex metadata only" in got["bare"], (cell, got)
    assert seen == {True, False}, "the grid reaches both sides of the rule"
