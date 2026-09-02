"""v0.51.307 — post-arc delta audit: six confirmed fixes.

The fresh-eyes audit of v0.51.304–.306 (5 finder angles) confirmed:
  1. Double-clicking the unread dot navigated — click 1's class flip
     display:none'd the dot mid-gesture, the row grid expanded into its
     pixels, and click 2 fell through to the click-through branch. The
     dot branch now stamps the row and the row branch absorbs clicks
     for 400ms.
  2. Keyboard-activating the dot dropped focus to <body> (the focused
     button went display:none) — focus now parks on the row's
     .notif-main so Tab continues from the same row.
  3. The mobile dot tap target was ~28px (comment claimed the 44px
     v0.50.88 floor) and a missed tap navigated — padding now reaches
     8 + 2×18 = 44px, negative margin keeps the row height.
  4. The dot's hover used the THEMEABLE --green-bright on a control
     whose rest state is the FIXED --ok unread signal (the v0.51.108
     fixed-vs-themed split) — hover is --ok-bright now.
  5. The .306 replaceState sat bare inside the gate's silent catch,
     BEFORE the deferred open — a throw (sandboxed iframe) would kill
     every deep-link open with no breadcrumb. Own try + console.warn.
  6. The .302/.304 empty-body guard returned a one-off 'reason' key,
     so set_active_theme_via_reupload logged "step_failed=None" — it
     now returns the sibling step_failed/fetch shape (driven below).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
OPS_CSS = (REPO / "app" / "web" / "static" / "ops.css").read_text()


# ── 1 + 2: the dot's double-click and focus fixes ────────────


def _notif_click_block() -> str:
    i = APP_JS.index("listEl.addEventListener('click'",
                     APP_JS.index("function rowHtml("))
    return APP_JS[i:APP_JS.index("openNotifRow(row);", i)]


def test_dot_branch_stamps_the_row():
    blk = _notif_click_block()
    seg = blk[blk.index("closest('.notif-dot')"):blk.index("const head")]
    assert "dotReadTs" in seg and "Date.now()" in seg, (
        "the dot branch must stamp the row — the class flip hides the dot "
        "mid-gesture, so a double-click's second click hits the row")


def test_row_branch_absorbs_the_double_click_tail():
    i = APP_JS.index("listEl.addEventListener('click'",
                     APP_JS.index("function rowHtml("))
    end = APP_JS.index("openNotifRow(row);", i) + len("openNotifRow(row);")
    row = APP_JS[i:end]
    row = row[row.index("closest('.notif-row.notif-clickable')"):]
    assert "dotReadTs" in row and "return" in row, (
        "the click-through must bail briefly after a dot read — otherwise "
        "double-clicking the dot navigates, the exact anti-goal of v0.51.305")
    assert row.index("dotReadTs") < row.index("openNotifRow(row);"), (
        "the absorb check must run before the navigation")


def test_dot_branch_parks_keyboard_focus():
    blk = _notif_click_block()
    seg = blk[blk.index("closest('.notif-dot')"):blk.index("const head")]
    assert "document.activeElement === dot" in seg and ".focus()" in seg, (
        "the activated button goes display:none, which drops focus to "
        "<body>; park it on .notif-main so Tab continues from this row")


# ── 3 + 4: CSS tap target and the fixed-signal hover ─────────


def test_mobile_dot_reaches_the_tap_floor():
    blk = OPS_CSS[OPS_CSS.index("@media (max-width: 600px)"):]
    blk = blk[:blk.index("}", blk.index(".notif-clear-all"))]
    i = blk.index(".notif-dot")
    rule = blk[blk.index("{", i):blk.index("}", i)]
    assert "padding: 18px" in rule, "8px circle + 2×18 padding = 44px floor"
    assert "margin: -12px" in rule, (
        "the negative margin keeps the 44px hit box from inflating the row")


def test_dot_hover_stays_in_the_fixed_ok_family():
    i = OPS_CSS.index(".notif-dot:hover::before")
    rule = OPS_CSS[OPS_CSS.index("{", i):OPS_CSS.index("}", i)]
    assert "--ok-bright" in rule and "--green-bright" not in rule, (
        "the dot is the FIXED generic unread signal (INBOX pill family); "
        "--green-bright is the themeable accent alias and would flip the "
        "dot's meaning mid-hover under a non-green theme")


# ── 5: the strip's own try + breadcrumb ──────────────────────


def test_deep_link_strip_has_its_own_catch():
    i = APP_JS.index("v1.14.85: ?info_open=<tmdb_id>")
    gate = APP_JS[i:APP_JS.index("URLSearchParams not supported", i)]
    # v0.51.309 (audit r2): scope to the STRIP itself — the gate's outer
    # `try {` satisfied the old `in gate[:r]` check, so unwrapping the inner
    # try survived as long as a console.warn existed anywhere after it.
    strip = gate[gate.index("const qs = sp.toString()"):
                 gate.index("if (infoOpen && infoMt)")]
    # v0.51.311 (review): ORDER, not co-occurrence — a mutant that hoists
    # replaceState above an empty try still had all three tokens present.
    assert strip.index("try {") < strip.index("history.replaceState") \
        < strip.index("} catch"), (
        "the replaceState must sit inside ITS OWN try — a throw before the "
        "deferred open otherwise hits the gate's silent outer catch and "
        "kills every deep-link open with no signal")
    assert "console.warn" in strip[strip.index("} catch"):], (
        "the strip's catch needs its breadcrumb")


# ── 6: the empty-body guard's failure shape, driven ──────────


def test_empty_body_fetch_returns_the_sibling_shape():
    from app.core import plex as plex_mod
    from app.core.plex import PlexConfig
    cfg = PlexConfig(url="http://plex.test:32400", token="t",
                     movie_section="1", tv_section="2", enabled=True)
    client = plex_mod.PlexClient(cfg)
    r = MagicMock()
    r.status_code = 200
    r.content = b""
    fake = MagicMock()
    fake.get = MagicMock(return_value=r)
    client._client = fake
    out = client._fetch_and_reupload_theme(
        item_rating_key="1", theme_rating_key="upload://themes/x")
    assert out["ok"] is False and out["upload"] is None
    assert out["step_failed"] == "fetch", (
        "the one-off 'reason' key made set_active_theme_via_reupload log "
        "'step_failed=None' — an unexplained abort")
    assert out["fetch"]["bytes"] == 0 and "reason" not in out


def test_v0_51_307_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert "0.51.307: " in init_py
