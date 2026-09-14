"""v0.51.323 — INFO card restructure, tag A: one statement each.

The operator, after the v0.51.322 Plex-theme preview landed: "info card is
looking a bit cluttered/confusing now with the added plex preview which we
want … let's take a full review of the info cards to optimize". The review
found the same backup state said three times (headline, strip title, PROMOTE
caption), the same URL three times (themerrdb url, backup url, video id) and
two players told apart only by a muted dt. Tag A:

  1. AUDIO group — the players get their own group right under the state
     strip, labelled by what they are ("plex serves" / "motif file") and
     badged by state (SERVING / STANDING BY / PLACED / NOT PLACED). FILE keeps
     only the on-disk facts. Same ambiguous-cut blanking as before (v0.51.223).
  2. SOURCE dedup — the applied-url row renders only when it differs from
     ThemerrDB's; previous url only when set; the video id rides the applied
     row as a muted suffix; probe + anime themes share one actions row.
  3. State said once — the headline is two plain facts (what motif holds ·
     what plays); the strip moved directly under the hero with the intent-flip
     button in its header and one note line.

Pins are invariants (labels, gates, order), not the render text; the headline
matrix is evaluated with quickjs from the card's own helper.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from _slice_helpers import slice_between

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def _rule(sel: str) -> str:
    return slice_between(APP_CSS, sel + " {", "}")


# ── 1. order + the AUDIO group ────────────────────────────────


def test_state_strip_then_audio_lead_the_card():
    order = ["${recoveryPlaceholder}",
             "${_grp('audio', _audioRows)}",
             "${_grp('source', _linksRows)}",
             "${_grp('file', _onDiskRows)}",
             "${diffSection}",
             "_fold('identity'"]
    idxs = [APP_JS.index(s) for s in order]
    assert idxs == sorted(idxs), "strip, audio, source, file, then the reference tail"
    assert "_grp('file & placement'" not in APP_JS


def test_audio_group_blank_under_an_ambiguous_cut_and_plex_first():
    rows = slice_between(APP_JS, "const _audioRows = ", "`;")
    assert rows.startswith("const _audioRows = _ambiguousCut ? '' : `"), (
        "the v0.51.223 ambiguous-cut contract covers the players too")
    assert rows.index("${plexThemeBlock}") < rows.index("${audioBlock}"), "what plays first"
    ondisk = slice_between(APP_JS, "const _onDiskRows = ", "`;")
    assert "${audioBlock}" not in ondisk and "${plexThemeBlock}" not in ondisk, (
        "FILE keeps only the on-disk facts")


def test_plex_row_is_labelled_and_badged_serving():
    b = slice_between(APP_JS, "const plexThemeBlock = ", "      : '';")
    # v0.51.340: the badge moved under the label (the <dt>); the <dd> opens on the player.
    assert '<dt class="info-ctl-label info-ctl-label-play">plex serves' in b
    assert 'class="tier-badge tier-badge-serving"' in b and ">SERVING</span></dt>" in b
    assert "what Plex serves for this item" not in b, "the label says it now (bare card: tag B)"


def test_motif_row_badge_follows_the_file_state():
    b = slice_between(APP_JS, "const audioBlock = lf", "      : '';")
    assert '<dt class="info-ctl-label info-ctl-label-play">motif file' in b  # v0.51.340: badge in the <dt>
    assert "<dt>play</dt>" not in APP_JS
    i_standing = b.index("'tier-badge-standing', 'STANDING BY'")
    i_placed = b.index("'tier-badge-placed', 'PLACED'")
    i_unplaced = b.index("'tier-badge-unplaced', 'NOT PLACED'")
    assert (b.index("lfIsBackupOnly") < i_standing
            < b.index("_placedKinds.length") < i_placed < i_unplaced), (
        "backup intent beats placement; placement beats the quiet default")
    assert 'data-act="edit-audio"' in b, "EDIT AUDIO still rides the motif row"


def test_a_dead_plex_player_takes_its_badge_with_it():
    b = slice_between(APP_JS, "function _bindPlexThemePlayer(body) {", "\n  }")
    assert "querySelector('.tier-badge')" in b and "badge.remove()" in b
    assert b.index("badge.remove()") < b.index("a.remove();")


# ── 2. SOURCE dedup ───────────────────────────────────────────


def test_applied_url_row_only_when_it_differs_from_themerrdb():
    assert ("const _appliedShown = !!currentUrl && (!_tdbRowShown || currentUrl !== tdbUrl);"
            in APP_JS)
    links = slice_between(APP_JS, "const _linksRows = `", "`;\n")
    assert "${_appliedShown ? `<dt>${appliedUrlLabel}</dt>" in links
    assert "<dt>video id</dt>" not in APP_JS
    # the id rides exactly one row: the applied one when shown, else ThemerrDB's
    assert "${_appliedShown ? '' : _vidSuffix}" in links
    assert "${currentUrlLink}${_vidSuffix}" in links
    assert "${previousUrlLink ? `<dt>previous url</dt>" in links
    assert "${_actionsRow}" in links


def test_previous_url_is_omitted_not_dashed_when_hidden():
    b = slice_between(APP_JS, "const previousUrlLink = (previousUrl && !hidePrev)", ";")
    assert "—" not in b


def test_probe_and_anime_themes_share_one_actions_row():
    row = slice_between(APP_JS, "const _actionsRow = ", ";\n")
    assert '<dt class="info-ctl-label">actions</dt><dd class="info-play-row">' in row  # v0.51.340: centres on the button line
    assert (row.index("${probeBtnHtml}") < row.index("${animeThemesBtnHtml}")
            < row.index("${probeMetaHtml}")), "buttons first, then the probe metas"
    assert "<dt>probe</dt>" not in APP_JS and "<dt>anime themes</dt>" not in APP_JS
    meta = slice_between(APP_JS, "const probeMetaHtml = tdbUrl", "      : '';")
    assert 'id="probe-result" class="muted small info-probe-meta"' in meta


# ── 3. the headline: two plain facts ──────────────────────────


def _label(lf, data, placements, ambiguous=False) -> str:
    quickjs = pytest.importorskip("quickjs")
    helper = slice_between(APP_JS, "  function _plexBackupState(data) {", "\n  }\n") + "\n  }\n"  # v0.51.343: shared reading
    held = slice_between(APP_JS, "    function _heldWord(sk) {", "\n    }") + "\n    }"
    fn = slice_between(APP_JS, "    function _derivePlaybackSourceLabel() {", "\n    }") + "\n    }"
    src = f"""
      const _ambiguousCut = {json.dumps(ambiguous)};
      const lf = {json.dumps(lf)};
      const data = {json.dumps(data)};
      const placements = {json.dumps(placements)};
      {helper}
      {held}
      {fn}
      _derivePlaybackSourceLabel();
    """
    return quickjs.Context().eval(src)


@pytest.mark.parametrize("lf, data, placements, expected", [
    (None, {"plex_independent_theme": 1}, [],
     "nothing on disk · Plex serves its own theme"),
    (None, {"plex_has_theme": 1}, [],
     "nothing on disk · Plex is serving a theme motif no longer manages "
     "(RE-DOWNLOAD TDB takes it over, PURGE clears it)"),
    (None, {}, [], "nothing on disk · no theme staged"),
    # v0.51.339: a backup row says Plex serves only when Plex does (has_theme, not a verify 404)
    ({"source_kind": "themerrdb", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": 1}, [],
     "ThemerrDB theme on disk as backup · Plex serves its own theme"),
    ({"source_kind": "plex_cloud", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": 1}, [],
     "copy of Plex's cloud theme on disk as backup · Plex serves its own theme"),
    ({"source_kind": "adopt", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": 1, "plex_theme_verified_ok": 1}, [],
     "adopted sidecar theme on disk as backup · Plex serves its own theme"),
    ({"source_kind": "themerrdb", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": 0}, [],
     "ThemerrDB theme on disk as backup · Plex no longer serves a theme (PROMOTE TO ACTIVE deploys it)"),
    ({"source_kind": "url", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": 1, "plex_theme_verified_ok": 0}, [],
     "user-URL theme on disk as backup · Plex no longer serves a theme (PROMOTE TO ACTIVE deploys it)"),
    # v0.51.341: no plex_items row at all (has_theme is NOT NULL, so null = the item left Plex) — PROMOTE has nowhere to deploy
    ({"source_kind": "themerrdb", "last_place_attempt_reason": "backup_only"}, {"plex_has_theme": None}, [],
     "ThemerrDB theme on disk as backup · this item is not in Plex"),
    ({"source_kind": "plex_cloud", "last_place_attempt_reason": "backup_only"}, {"plex_theme_verified_ok": 0}, [],
     "copy of Plex's cloud theme on disk as backup · this item is not in Plex"),
    ({"source_kind": "upload"}, {}, [{"placement_kind": "plex_upload"}],
     "uploaded MP3 on disk · placed: plex_upload"),
    ({"source_kind": "themerrdb"}, {}, [{"placement_kind": "sidecar"}, {"placement_kind": "hardlink"}],
     "ThemerrDB theme on disk · placed: sidecar, hardlink"),
    ({"source_kind": "url"}, {}, [], "user-URL theme on disk · not placed"),
])
def test_headline_is_what_motif_holds_and_what_plays(lf, data, placements, expected):
    assert _label(lf, data, placements) == expected


def test_headline_never_names_a_badge_or_the_action():
    for lf in (None, {"source_kind": "themerrdb", "last_place_attempt_reason": "backup_only"},
               {"source_kind": "upload"}):
        out = _label(lf, {"plex_independent_theme": 1, "plex_has_theme": 1}, [])  # v0.51.339: a Plex-serving row
        assert "badge" not in out and "PROMOTE" not in out, out


def test_ambiguous_cut_still_defers_to_the_picker():
    out = _label({"source_kind": "themerrdb"}, {}, [{"placement_kind": "sidecar"}], ambiguous=True)
    assert out.startswith("(multiple cuts")


def test_headline_tooltip_still_explains_the_src_letter():
    tag = slice_between(APP_JS, '<p class="info-hero-playback muted small" title="', '">')
    assert "SRC letter" in tag and "what plays" in tag


# ── 4. the state strip ────────────────────────────────────────


def test_strip_header_holds_the_flip_button_and_one_note_line():
    inner = slice_between(
        APP_JS,
        '    section.innerHTML = `\n      <header class="recovery-section-head">',
        "    `;")
    assert (inner.index("${intentFlipBtnsHtml}") < inner.index("</header>")
            < inner.index("${noteLine}") < inner.index("${items}")), (
        "button in the header, the note line under it, options last")
    assert "ackedNoteLine" not in APP_JS
    assert "deploy the backup over Plex's theme" not in APP_JS, "the caption is the note line"
    fn = slice_between(APP_JS, "let intentFlipCaption = '';", "const _noteParts = [];")
    assert fn.count('<span class="recovery-section-flip">') == 2, "PROMOTE and MARK AS BACKUP"
    # v0.51.343: PROMOTE's caption is a ternary on the Plex reading, so count assignments, not string openings
    assert len(re.findall(r"(?<!let )intentFlipCaption = ", fn)) == 2, "each flip sets its caption"
    note = slice_between(APP_JS, "const _noteParts = [];", "section.innerHTML")
    assert "if (data.acked) _noteParts.push(" in note
    assert "if (intentFlipCaption) _noteParts.push(" in note
    assert "recovery-section-note-empty" in note, "empty keeps its height (v1.12.92)"


# ── 5. CSS — tokens only, the primitives it extends ────────────


def test_audio_badge_rules_use_tokens_only():
    for sel, tone in (("tier-badge-serving", "--amber"),
                      ("tier-badge-standing", "--cyan"),
                      ("tier-badge-placed", "--green"),
                      ("tier-badge-unplaced", "--fg-dim")):
        r = _rule("." + sel)
        assert tone in r, sel
        assert "#" not in r, f"{sel}: no hardcoded colours"
        assert "px" not in r.replace("1px solid", ""), f"{sel}: only the 1px border is literal"
        assert f"'{sel}'" in APP_JS or f'"tier-badge {sel}"' in APP_JS, f"{sel} is rendered"
    # v0.51.340: the badge left the play row for the label column, so its flex guard went with it.
    assert ".info-play-row > .tier-badge" not in APP_CSS, "no badge rides a play row any more"
    assert ".dlg-grid dt.info-ctl-label-play {" in APP_CSS, "the badge's label-column rule"


def test_strip_and_first_group_drop_the_divider_under_the_hero():
    assert (".info-hero + .recovery-section { margin-top: var(--gap-4); padding-top: 0; border-top: 0; }"
            in APP_CSS)
    assert (".recovery-section[hidden] + .info-group { margin-top: var(--gap-4); padding-top: 0; border-top: 0; }"
            in APP_CSS), "while the strip is hidden the first group is the hero's neighbour"
    flip = _rule(".recovery-section-flip")
    assert "margin-left: auto" in flip and "margin-top" not in flip, (
        "the flip is pushed right inside the header, not stacked under the options")


def test_downloaded_row_reads_human_bytes():
    dl = slice_between(APP_JS, "const dlBlock = lf", "      : '';")
    assert "fmt.bytes(lf.file_size)" in dl and "fmt.num(lf.file_size)" not in dl


def test_v0_51_323_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.323: INFO card restructure, tag A" in init_py
