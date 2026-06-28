"""v1.19.85 — library/collections/logs design-uniformity audit.

Follow-on to the v1.19.81 settings + v1.19.84 dashboard passes.
The library (movies/tv/anime/collections via library.html) and
LOGS (queue.html) pages were already in excellent shape; three
small drifts found, all approved by the user:

  O1 (chip voice) — within the `.chip` primitive the // prefix
     was on the route/action chips (// STANDARD, // 4K, the
     collections // <SECTION> switch, // NEEDS WORK) but NOT on
     the filter chips (ALL/THEMED/UNTHEMED, plus JOBS/EVENT on
     /queue). the user chose to STRIP // from the outliers so every
     little filter/switch chip is bare — // is reserved for real
     buttons + section titles (DESIGN_SYSTEM §3). Pills already
     work this way.

  L1 (button voice) — the saved-filter popup's "+ Save current"
     was the one button in the libraries surface breaking the //
     convention (its own popup header reads // SAVED FILTERS).
     Renamed to // SAVE CURRENT.

  C1 (dead template) — libraries.html was never rendered (the
     /libraries route 302-redirects to /settings#plex). Deleted.

  (O2 — aligning the two manual-URL dialogs — deferred to its own
   tag: the failure-recovery override-dlg is keyed on
   media_type/tmdb_id while manual-url-dlg is rk-based, so it's a
   feature change, not a style fix.)
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
LIB_HTML = (REPO / "app" / "web" / "templates" / "library.html").read_text()
QUEUE_HTML = (REPO / "app" / "web" / "templates" / "queue.html").read_text()


# ── O1 — switch/sort chips stripped of // ────────────────────


def test_resolution_chips_have_no_slash_prefix():
    assert ">STANDARD</button>" in LIB_HTML
    assert ">4K</button>" in LIB_HTML
    assert ">// STANDARD</button>" not in LIB_HTML, (
        "v1.19.85: the STANDARD toggle chip must be bare (no //)"
    )
    assert ">// 4K</button>" not in LIB_HTML, (
        "v1.19.85: the 4K toggle chip must be bare (no //)"
    )


def test_collections_section_chip_has_no_slash_prefix():
    assert ">{{ s.label|upper }}</button>" in LIB_HTML, (
        "v1.19.85: the collections section chip must render bare"
    )
    assert ">// {{ s.label|upper }}</button>" not in LIB_HTML, (
        "v1.19.85: the collections section chip must not carry //"
    )


def test_needs_work_chip_has_no_slash_prefix():
    assert ">NEEDS WORK</button>" in LIB_HTML
    assert ">// NEEDS WORK</button>" not in LIB_HTML, (
        "v1.19.85: the NEEDS WORK sort chip must be bare (no //)"
    )


def test_status_filter_chips_still_bare():
    """Regression baseline — the filter chips that were always
    bare must stay bare (they're the majority the outliers now
    match)."""
    for label in ("ALL", "THEMED", "UNTHEMED"):
        assert f">{label}</button>" in LIB_HTML


def test_queue_filter_chips_still_bare():
    """LOGS page filter chips were always bare — guard that no //
    crept in."""
    for label in ("ALL", "PENDING", "RUNNING", "FAILED", "1H", "24H", "7D"):
        assert f">{label}</button>" in QUEUE_HTML
        assert f">// {label}</button>" not in QUEUE_HTML


# ── L1 — Save-current button gains // ────────────────────────


def test_save_current_button_uses_slash_prefix():
    assert ">// SAVE CURRENT</button>" in LIB_HTML, (
        "v1.19.85: the presets // SAVE CURRENT button must use the "
        "// convention like every other button"
    )
    assert ">+ Save current</button>" not in LIB_HTML, (
        "v1.19.85: the old '+ Save current' label must be gone"
    )


# ── C1 — dead template removed ───────────────────────────────


def test_libraries_html_template_deleted():
    assert not (REPO / "app" / "web" / "templates" / "libraries.html").exists(), (
        "v1.19.85: the dead libraries.html template must be removed "
        "(the /libraries route 302-redirects; nothing renders it)"
    )


# ── Version pin ──────────────────────────────────────────────


def test_v1_19_85_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
