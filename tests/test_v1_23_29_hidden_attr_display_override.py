"""v1.23.29 — the [hidden]-attribute / author-display footgun, enforced.

the user's repro: the database // RESTORE banner showed "A restore is staged"
permanently — even with NO pending file on disk (he ls'd /config, nothing),
and neither CANCEL nor a hard reload dismissed it. Cause: `.restore-pending-
banner { display: flex }` is an AUTHOR rule, and author origin outranks the UA
`[hidden] { display: none }`, so the `hidden` attribute (set by the SSR, the
cancel handler, and refreshPending) was a complete no-op — the banner was
welded visible since the restore card shipped (v1.23.15).

This footgun has bitten repeatedly (the app.css comments at ~2762 and ~4164
document earlier fixes for .live-indicator and .manual-url-preview). The fix is
always `.<class>[hidden] { display: none }` — a per-class override whose
specificity (0,2,0) beats the base rule (0,1,0). This lint makes forgetting it
impossible: any element toggled via the `hidden` attribute whose class sets an
author `display:` MUST carry a `[hidden]` override.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
CSS = "\n".join(
    (REPO / "app" / "web" / "static" / name).read_text()
    for name in ("app.css", "ops.css"))
TEMPLATES = REPO / "app" / "web" / "templates"

# the boolean `hidden` attribute (NOT aria-hidden / data-hidden / hidden="…").
_HIDDEN_ATTR = re.compile(r"(?<![\w-])hidden(?=[\s/>])")
_OPEN_TAG = re.compile(r"<[a-zA-Z][^>]*>")
_CLASS = re.compile(r'class="([^"]*)"')


# a CSS rule = its selector list + body. (good enough for flat author CSS;
# @media-nested rules are a known blind spot.)
_RULE = re.compile(r"([^{}]+)\{([^}]*)\}")


def _class_sets_display(cls: str) -> bool:
    """True if ANY rule whose selector mentions `.<cls>` as a class token sets
    an author `display:` other than none — covering selector-lists
    (`.a, .cls {…}`), compound (`.cls.active`), pseudo (`.cls:hover`), and
    descendant (`.parent .cls`) forms, not just a bare `.<cls> {` block."""
    tok = re.compile(r"\." + re.escape(cls) + r"(?![\w-])")
    for sel, body in _RULE.findall(CSS):
        if not tok.search(sel):
            continue
        for dm in re.finditer(r"display\s*:\s*([a-z-]+)", body):
            if dm.group(1) != "none":
                return True
    return False


_GLOBAL_HIDDEN_RESET = re.compile(
    r"\[hidden\]\s*\{[^}]*display\s*:\s*none\s*!important")


def _class_has_hidden_override(cls: str) -> bool:
    # a per-class `.cls[hidden]` override, OR a global `[hidden]{display:none
    # !important}` reset that covers every element at once.
    return bool(re.search(r"\." + re.escape(cls) + r"\[hidden\]", CSS)
                or _GLOBAL_HIDDEN_RESET.search(CSS))


def test_hidden_toggled_classes_with_display_have_override():
    offenders = []
    for tpl in sorted(TEMPLATES.rglob("*.html")):
        html = tpl.read_text()
        for tag in _OPEN_TAG.finditer(html):
            t = tag.group(0)
            if not _HIDDEN_ATTR.search(t):
                continue
            cm = _CLASS.search(t)
            if not cm:
                continue
            classes = cm.group(1).split()
            # skip Jinja-dynamic class values (e.g. class="{{ x }}").
            classes = [c for c in classes if re.fullmatch(r"[\w-]+", c)]
            if not any(_class_sets_display(c) for c in classes):
                continue  # no author display rule → UA [hidden] works as-is
            if not any(_class_has_hidden_override(c) for c in classes):
                offenders.append((tpl.name, classes))
    assert not offenders, (
        "elements toggled via the boolean `hidden` attribute whose class sets "
        "an author `display:` (which outranks the UA `[hidden]{display:none}`) "
        "but have NO `.<class>[hidden]` override — they render as a phantom "
        "that `hidden` can't dismiss:\n"
        + "\n".join(f"  {name}: {classes}" for name, classes in offenders))


def test_restore_pending_banner_override_present():
    # the specific v1.23.29 fix (regression-lock for the user's report).
    assert re.search(r"\.restore-pending-banner\[hidden\]\s*\{\s*display:\s*none",
                     CSS)
