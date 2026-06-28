"""v1.15.42 — dashboard ADDED TODAY/WEEK cards deep-link to /queue.

the user: "I do like the clicking to link to the logs though that
is nice." (Approved as the v1.15.41 follow-up — wire the dashboard
end of the SINCE-chip drill-down so the count→rows trip is one
click from where the operator notices the number.)

Pre-fix the dashboard surfaced ADDED TODAY / ADDED THIS WEEK
counts with no click affordance — the operator had to nav to
LOGS, scroll, eyeball timestamps. v1.15.41 added the SINCE chip
backbone (?since=<seconds> on /api/events + chip row on /queue);
this tag wires the dashboard cards as <a href="/queue?since=N">
deep links so the SINCE chip auto-applies on land.

Static-text guards: card href + structural shape (anchor wraps
inner content, NOT the article — keeps the customize controls
injection at article.firstChild from competing with link clicks).
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"
APP_CSS = REPO / "app" / "web" / "static" / "app.css"


def test_added_today_card_links_to_queue_since_24h():
    """ADDED TODAY card must wrap its content in
    `<a href="/queue?since=86400">` — 86400 mirrors the 1D chip
    value in queue.html and matches the SINCE-chip allowlist in
    bindQueue() (v1.15.41)."""
    html = DASH_HTML.read_text()
    anchor = html.index('data-dash-card="added-today"')
    block = html[anchor:anchor + 800]
    assert 'href="/queue?since=86400"' in block, (
        "v1.15.42: ADDED TODAY must deep-link to /queue?since=86400 "
        "so the SINCE chip lands on 24H"
    )
    assert 'class="stat-card-link"' in block, (
        "v1.15.42: deep-link wrapper must use class stat-card-link "
        "so the hover affordance + border highlight kick in"
    )


def test_added_this_week_card_links_to_queue_since_7d():
    """ADDED THIS WEEK card must wrap its content in
    `<a href="/queue?since=604800">` — 604800 mirrors the 7D chip
    value."""
    html = DASH_HTML.read_text()
    anchor = html.index('data-dash-card="added-this-week"')
    block = html[anchor:anchor + 800]
    assert 'href="/queue?since=604800"' in block
    assert 'class="stat-card-link"' in block


def test_anchor_wraps_inner_content_not_article():
    """Structural guard: the deep-link anchor must live INSIDE the
    <article data-dash-card> — NOT around it. Otherwise the
    per-card customize controls (injected at article.firstChild
    by dashboard-customize.js v1.15.32: ◀ ▶ // HIDE buttons)
    would land inside the link, and clicking the toggle would
    navigate to /queue instead of toggling the card.

    The article must keep `data-dash-card` for the customize
    layout walker (querySelectorAll('[data-dash-card]')) — moving
    it to the anchor would break the layout JSON shape that
    v1.15.32 added."""
    html = DASH_HTML.read_text()
    for cid in ("added-today", "added-this-week"):
        attr = f'data-dash-card="{cid}"'
        article_start = html.index(attr)
        # Walk back to the opening <article> tag for this card.
        art_open = html.rfind('<article', 0, article_start)
        # Walk forward to its </article> close. Anchored on the
        # next </article> (cards don't nest articles).
        art_close = html.index('</article>', article_start) + len('</article>')
        article_html = html[art_open:art_close]
        # Anchor must appear INSIDE the article, AFTER the opening tag.
        assert '<a href="/queue?since=' in article_html, (
            f"v1.15.42: {cid} card missing inner anchor"
        )
        # Anchor must NOT wrap the article — i.e. there should be
        # no `<a ...><article` wrapping pattern.
        before_article = html[max(0, art_open - 200):art_open]
        assert not before_article.rstrip().endswith('">'), (
            f"v1.15.42: {cid} appears to be wrapped by a sibling "
            "anchor — keep the anchor INSIDE the article so the "
            "customize controls (injected at article.firstChild) "
            "stay outside the link's click target"
        )


def test_stat_card_link_css_present():
    """The .stat-card-link rule must exist in app.css with the
    color: inherit + display: block bones — without those the
    card's accent-colored stat-num gets recolored to default link
    blue, and the link wrapper doesn't fill the card's click
    surface."""
    css = APP_CSS.read_text()
    anchor = css.index('a.stat-card-link {')
    block = css[anchor:anchor + 600]
    assert 'color: inherit' in block, (
        "v1.15.42: stat-card-link must inherit color so the card's "
        ".stat-num accent classes (.ok / .accent) keep their colors"
    )
    assert 'display: block' in block, (
        "v1.15.42: stat-card-link must be display:block so the link "
        "fills the card's click surface, not just the inline content"
    )
    # Hover affordance MUST exist — otherwise the card looks
    # static and operators won't realize it's clickable.
    assert 'a.stat-card-link:hover' in css, (
        "v1.15.42: must have a :hover rule on stat-card-link so the "
        "card has a discoverable click affordance (border, shadow, "
        "or 'SHOW IN LOGS' reveal)"
    )


def test_card_since_values_match_queue_chip_allowlist():
    """Cross-source mirror-principle guard: the deep-link `since=`
    values on the dashboard cards MUST be in the queue.html SINCE
    chip allowlist + the JS allowlist in bindQueue() (v1.15.41).
    Otherwise clicking the card lands on /queue with no chip
    activated, which is a confusing user experience."""
    import re
    html_dash = DASH_HTML.read_text()
    queue_html = (REPO / "app" / "web" / "templates" / "queue.html").read_text()
    js = (REPO / "app" / "web" / "static" / "app.js").read_text()
    card_since_values = set(
        re.findall(r'href="/queue\?since=(\d+)"', html_dash)
    )
    chip_values = set(re.findall(r'data-evfilter="(\d+)"', queue_html))
    bind_anchor = js.index("function bindQueue()")
    body = js[bind_anchor:bind_anchor + 5000]
    allowlist_block = body[body.index("new Set(["):body.index("new Set([") + 80]
    js_values = set(re.findall(r"'(\d+)'", allowlist_block))
    missing_in_chips = card_since_values - chip_values
    missing_in_js = card_since_values - js_values
    assert not missing_in_chips, (
        f"v1.15.42: dashboard card since= value(s) {missing_in_chips} "
        f"not in queue.html chip row — chip won't auto-activate on "
        "land. Add the matching chip to queue.html or change the "
        "card href."
    )
    assert not missing_in_js, (
        f"v1.15.42: dashboard card since= value(s) {missing_in_js} "
        f"not in JS allowlist — deep-link parser will reject. Add "
        "the matching value to bindQueue()'s allowedSince Set."
    )
