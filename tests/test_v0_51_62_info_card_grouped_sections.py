"""v0.51.62 — INFO card detail rows grouped into labeled sections (the user).

The flat ~17-row detail <dl> is split into four .dlg-section.info-group blocks —
IDENTITY / SOURCE / HISTORY / FILE & PLACEMENT (renamed from IDS/LINKS/TIMELINE/ON
DISK in v0.51.64) — reusing the .dlg-section + <h4> primitive (the same one SOURCE
PREVIEW uses) for scannability. The rows are built as consts (same
nesting depth as the old flat grid, so the conditional sub-templates are unchanged)
and wrapped by _grp, which renders a group ONLY when it has >=1 row — so a
metadata-only title (empty ON DISK) doesn't leave a dangling header. Row order
within each group is identical to the old list.

JS has no exec harness; source pins are the app.js / app.css contract.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
APP_CSS = (REPO / "app" / "web" / "static" / "app.css").read_text()


def test_grp_helper_hides_empty_groups():
    # the helper renders a group wrapper only when rows are non-empty (so an empty
    # ON DISK group on a metadata-only row leaves no dangling header).
    assert "const _grp = (title, rows) => rows.trim()" in APP_JS
    # v0.51.64: title htmlEscape'd (the '&' in "file & placement").
    # v0.51.289: `// ` prefix — one header voice across the card.
    assert '<div class="dlg-section info-group"><h4>// ${htmlEscape(title)}</h4><dl class="dlg-grid">${rows}</dl></div>' in APP_JS
    # the fold twin hides empty sections the same way (guard then details).
    fold = APP_JS[APP_JS.index("const _fold = (title, rows,"):
                  APP_JS.index('<details class="history-section info-fold"')]
    assert "rows.trim()" in fold


def test_four_group_row_consts_defined():
    for c in ("_idsRows", "_linksRows", "_timelineRows", "_onDiskRows"):
        assert f"const {c} = " in APP_JS, f"missing group const {c}"


def test_four_groups_rendered_in_order():
    # v0.51.64 (the user): labels reverted to the original demo names
    # IDENTITY/SOURCE/HISTORY/FILE & PLACEMENT (were IDS/LINKS/TIMELINE/ON DISK).
    # The row consts keep their internal names.
    # v0.51.289 (design audit): intent-based order — the expanded actionable
    # groups (SOURCE, FILE & PLACEMENT) render first, then the collapsed
    # reference folds (IDENTITY, TIMELINE — renamed from 'history' to end the
    # // HISTORY collision — LOUDNESS, revisions).
    calls = ["_grp('source', _linksRows)",
             "_grp('file & placement', _onDiskRows)",
             "_fold('identity', _idsRows",
             "_fold('timeline', _timelineRows",
             "_fold('loudness', _loudnessRows"]
    idxs = []
    for c in calls:
        assert c in APP_JS, f"missing group render call {c}"
        idxs.append(APP_JS.index(c))
    assert idxs == sorted(idxs), (
        "card order must be source/file first, then the reference folds")
    # the retired shapes must be gone — a stray expanded identity/history
    # group would resurrect the pre-.289 wall.
    assert "_grp('identity'" not in APP_JS
    assert "_grp('history'" not in APP_JS
    assert "_grp('loudness'" not in APP_JS


def test_group_membership_preserved():
    # spot-check each group const carries its expected rows (order-agnostic).
    ids = APP_JS[APP_JS.index("const _idsRows = "):APP_JS.index("const _linksRows = ")]
    assert "<dt>imdb</dt>" in ids and "<dt>tmdb</dt>" in ids and "<dt>upstream</dt>" in ids
    links = APP_JS[APP_JS.index("const _linksRows = "):APP_JS.index("const _timelineRows = ")]
    assert "${appliedUrlLabel}" in links and "<dt>video id</dt>" in links and "${probeBtnHtml}" in links
    timeline = APP_JS[APP_JS.index("const _timelineRows = "):APP_JS.index("const _onDiskRows = ")]
    assert "<dt>motif added</dt>" in timeline and "${failBlock}" in timeline and "${ovrBlock}" in timeline and "${puBlock}" in timeline
    ondisk = APP_JS[APP_JS.index("const _onDiskRows = "):APP_JS.index("const _onDiskRows = ") + 220]
    assert "${dlBlock}" in ondisk and "${backupBlock}" in ondisk and "${placedBlock}" in ondisk and "${audioBlock}" in ondisk


def test_old_flat_grid_is_gone():
    # the single flat detail grid (a <dl class="dlg-grid"> immediately followed by
    # the imdb row) no longer exists — imdb lives inside the ids group const now.
    assert '<dl class="dlg-grid">\n        <dt>imdb</dt>' not in APP_JS


def test_first_group_drops_redundant_divider_css():
    assert ".info-hero + .info-group {" in APP_CSS, (
        "the group right under the hero must drop its .dlg-section top divider")


def test_v0_51_62_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
