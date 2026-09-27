"""v0.51.351 — the carousel holds still for a finger, too.

v0.51.347 gave the dashboard carousel a mouse drag and left touch alone, because a finger already scrolls the strip
natively and hijacking that would cost the inertia. What a finger did NOT get is the pause a mouse gets for free from
`:hover`: the auto-scroll loop kept advancing the strip under the finger and took it back the instant the finger
lifted, so a poster could not be held still on a phone.

A touch or pen now holds the loop while it is down, and through the fling after it lifts. The strip's own scrolling
stays the browser's — nothing here assigns scrollLeft for a touch.
"""
from __future__ import annotations

import pytest

from test_v0_51_347_carousel_drag import _run, needs_node_mark  # the v0.51.347 page harness

pytestmark = needs_node_mark

SETTLE_MS = 2000  # app.js TOUCH_SETTLE_MS


@pytest.mark.parametrize("kind", ["touch", "pen"])
def test_the_loop_holds_while_a_finger_is_on_the_strip(tmp_path, kind):
    free, held, still_held = _run(tmp_path, {"scroll": 900, "steps": [
        {"frame": 1000}, {"frame": 1100, "read": True},                 # premise: it advances when nothing holds it
        {"down": {"x": 500, "type": kind}},
        {"frame": 1200}, {"frame": 1300, "read": True},                 # under the finger it must not move
        {"frame": 1400}, {"frame": 9000, "read": True},                 # nor after a long gap while still down
    ]})
    assert free["scroll"] > 900, "premise: the loop advances the strip"
    assert held["scroll"] == free["scroll"], (kind, held)
    assert still_held["scroll"] == free["scroll"], (kind, still_held)


def test_a_touch_never_moves_the_strip_itself(tmp_path):
    # the browser owns touch scrolling; the page must not assign scrollLeft or claim the pointer for a finger
    [read] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "type": "touch"}}, {"move": 300, "read": True},
    ]})
    assert (read["scroll"], read["dragging"], read["grabbing"], read["captured"]) == (900, False, False, None)
    assert "pointermove" not in read["prevented"], "preventing the move would cancel the native scroll"


def test_the_fling_after_the_finger_lifts_is_left_to_settle(tmp_path):
    lifted, during, after = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "type": "touch"}}, {"up": {"type": "touch"}}, {"read": True},
        {"advance": SETTLE_MS - 1}, {"frame": 2000}, {"frame": 2100, "read": True},   # still settling: no move
        {"advance": 2}, {"frame": 2200}, {"frame": 2300, "read": True},               # settled: the loop resumes
    ]})
    assert during["scroll"] == lifted["scroll"], during
    assert after["scroll"] > during["scroll"], after
    assert after["scroll"] < during["scroll"] + 20, "one normal step, not a jump for the held frames"


def test_the_loop_resumes_from_where_the_finger_left_the_strip(tmp_path):
    # the v0.51.286 float re-seed is what carries it on from there; a finger's scroll must not be undone
    [after] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "type": "touch"}},
        {"scrollto": 1500},                      # the browser scrolled it, as it does for a finger
        {"up": {"type": "touch"}}, {"advance": SETTLE_MS + 1},
        {"frame": 3000}, {"frame": 3100, "read": True},
    ]})
    assert 1500 <= after["scroll"] < 1520, after


def test_a_cancelled_touch_also_releases_the_hold(tmp_path):
    [held, after] = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500, "type": "touch"}}, {"frame": 1000}, {"frame": 1100, "read": True},
        {"cancel": True}, {"advance": SETTLE_MS + 1}, {"frame": 1200}, {"frame": 1300, "read": True},
    ]})
    assert held["scroll"] == 900
    assert after["scroll"] > 900, "a cancelled touch must not hold the loop for ever"


def test_the_mouse_drag_is_unchanged(tmp_path):
    # the v0.51.347 behaviour, re-asserted here because both now share the loop's guard
    mid, end = _run(tmp_path, {"scroll": 900, "steps": [
        {"down": {"x": 500}}, {"move": 560, "read": True}, {"move": 380, "read": True}, {"up": True},
    ]})
    assert (mid["scroll"], mid["grabbing"]) == (840, True)
    assert end["scroll"] == 1020
