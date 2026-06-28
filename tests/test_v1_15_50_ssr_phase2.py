"""v1.15.50 — SSR phase 2: PLEX MOVIES/TV/ANIME cards + TDB foots.

the user (screenshot: v1.15.49 dashboard with PLEX MOVIES/TV cards
showing "—" and "— with theme · — ThemerrDB available"):
"On first load of the dashboard seeing a number of the cards
without numbers and take a bit for them to load in, anyway to
make them load instantly."

v1.15.45 SSR'd 11 stat-num values; v1.15.50 closes the rest of
the dashboard so every numeric slot is baked into first paint.

## What's SSR'd in phase 2 (13 new fields)

* PLEX MOVIES card: total + with_theme + motif_avail
* PLEX TV card:     total + with_theme + motif_avail
* PLEX ANIME card:  total + with_theme + motif_avail
                    + visibility gate (display:none only when
                    plex_anime_total === 0)
* TDB MOVIES foot:  in_library + themed
* TDB TV foot:      in_library + themed (combines tv+anime
                    per v1.15.47 — TDB doesn't distinguish)

## Template macro consolidation

24 inline `{{ "{:,}".format(X) if X is not none else "—" }}`
conditionals → single `ssr_num(X)` macro at the top of
dashboard.html. the user: "As a rule let's try to standardize
across sets that are similar to keep style consistent." The
macro is the formatter SoT — future SSR additions stay
one-liners.

## SQL

phase 2 adds a second SELECT (single scan of plex_items
joined to plex_sections + themes) with conditional aggregation
for all 11 plex-side counts. Predicates mirror
/api/coverage/plex line ~13125 exactly so SSR-baked numbers
agree with post-poll render (divergence = visible flicker).

Static-text guards consistent with v1.15.45/47 SSR-test patterns.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
API_PY = REPO / "app" / "web" / "api.py"
DASH_HTML = REPO / "app" / "web" / "templates" / "dashboard.html"


# ── 1. Helper exposes the 13 new fields ─────────────────────


PHASE_2_FIELDS = [
    "plex_movies_total",
    "plex_movies_with_theme",
    "plex_movies_motif_avail",
    "plex_tv_total",
    "plex_tv_with_theme",
    "plex_tv_motif_avail",
    "plex_anime_total",
    "plex_anime_with_theme",
    "plex_anime_motif_avail",
    "tdb_movies_in_library",
    "tdb_movies_themed",
    "tdb_tv_in_library",
    "tdb_tv_themed",
]


def test_ssr_state_declares_phase_2_fields():
    """All 13 phase-2 field names must be declared in the state
    dict's default-None block. Without the declaration the
    template's `_ssr_dash.foo` access falls through to None →
    macro renders '—' → flash bug returns."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    body = src[anchor:anchor + 12000]
    missing = [f for f in PHASE_2_FIELDS if f'"{f}"' not in body]
    assert not missing, (
        f"v1.15.50: _dashboard_ssr_state missing field(s) {missing}"
    )


def test_ssr_state_phase_2_sql_mirrors_coverage_plex_predicates():
    """The phase-2 SQL must mirror /api/coverage/plex predicates
    exactly: motif_available = (t.tmdb_id IS NOT NULL AND
    t.upstream_source != 'plex_orphan'). Predicate drift =
    SSR-baked numbers disagree with post-poll render = visible
    flicker."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    fn_end = src.index('templates.env.globals["dashboard_ssr_state"]',
                        anchor)
    body = src[anchor:fn_end]
    # The phase-2 query is the cov = conn.execute(...) block. v1.15.65
    # consolidated it into the same `with get_conn(...)` block as the
    # phase-1 row query; the marker comment moved into the with body
    # to keep marker-line guard semantics.
    assert "v1.15.50: phase-2 SSR — plex_items aggregates" in body, (
        "v1.15.50: missing phase-2 SQL marker — the plex_items "
        "aggregates query needs its marker comment preserved"
    )
    assert "INNER JOIN plex_sections ps" in body, (
        "v1.15.50: phase-2 query must join plex_sections for the "
        "is_anime + included=1 gates"
    )
    assert "LEFT JOIN themes t ON t.id = pi.theme_id" in body, (
        "v1.15.50: phase-2 query must LEFT JOIN themes via "
        "theme_id (mirrors /api/coverage/plex shape)"
    )
    # motif_available predicate (the v1.15.47 lesson — match
    # /api/coverage/plex's to_item() rule exactly).
    assert "t.tmdb_id IS NOT NULL" in body
    assert "t.upstream_source != 'plex_orphan'" in body


def test_ssr_state_tdb_tv_combines_tv_and_anime():
    """Per v1.15.47, TDB doesn't distinguish anime from TV (both
    are media_type='tv' in themes). TDB TV card's in_library
    count must sum the plex_tv_motif_avail + plex_anime_motif_avail
    aggregates — otherwise anime is invisible in the TDB-perspective
    card (the same v1.15.47 bug)."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    fn_end = src.index('templates.env.globals["dashboard_ssr_state"]',
                        anchor)
    body = src[anchor:fn_end]
    # The combination happens in Python (not SQL) — clearer than
    # a SQL union; preserves the v1.15.47 semantic explicitly.
    assert ('state["tdb_tv_in_library"] = (\n'
            '                state["plex_tv_motif_avail"] '
            '+ state["plex_anime_motif_avail"]\n'
            '            )') in body, (
        "v1.15.50: tdb_tv_in_library must sum plex_tv_motif_avail "
        "+ plex_anime_motif_avail (v1.15.47 cross-section rule)"
    )


def test_ssr_state_themed_uses_motif_available_intersection():
    """tdb_{movies,tv}_themed must count rows where motif_available
    AND has_theme (the v1.15.47 fix — themed must be a strict
    subset of in-library). Otherwise themed > in-library is
    possible (the bug the user screenshotted at v1.15.46)."""
    src = API_PY.read_text()
    anchor = src.index("def _dashboard_ssr_state(")
    body = src[anchor:anchor + 12000]
    # The conditional aggregation must AND has_theme with the
    # motif_available predicate clauses.
    assert ("pi.has_theme=1\n"
            "                                AND t.tmdb_id IS NOT NULL") in body, (
        "v1.15.50: tdb_*_themed aggregates must AND has_theme=1 "
        "with the motif_available clauses — strict subset of "
        "in-library per v1.15.47"
    )


# ── 2. Template uses the macro consistently ─────────────────


def test_template_defines_ssr_num_macro():
    """Single `ssr_num` macro at the top is the formatter SoT.
    Without it, the 24 stat-num cells diverge in format choices
    (comma-grouping yes/no, fallback char, etc)."""
    html = DASH_HTML.read_text()
    assert "{%- macro ssr_num(v) -%}" in html, (
        "v1.15.50: ssr_num macro must be defined at the top of "
        "dashboard.html — single formatter for all SSR cells"
    )
    # Macro body: comma-grouped int or "—" fallback.
    macro_anchor = html.index("{%- macro ssr_num(v) -%}")
    macro_end = html.index("{%- endmacro -%}", macro_anchor)
    macro_body = html[macro_anchor:macro_end]
    assert '"{:,}".format(v)' in macro_body
    assert "—" in macro_body
    assert "is not none" in macro_body


def test_template_uses_macro_for_all_phase_2_placeholders():
    """All 13 phase-2 placeholder values must use the macro. Drift
    here = some cells render formatted, some don't."""
    html = DASH_HTML.read_text()
    placeholder_to_field = [
        ('id="plex-movies-total"', "plex_movies_total"),
        # v1.23.41: PLEX-card foots reframed to ThemerrDB reach —
        # "in ThemerrDB" (motif_avail) + "not in ThemerrDB" (total − avail).
        ('id="plex-movies-motif"', "plex_movies_motif_avail"),
        ('id="plex-movies-not-tdb"', "plex_movies_not_in_tdb"),
        ('id="plex-tv-total"', "plex_tv_total"),
        ('id="plex-tv-motif"', "plex_tv_motif_avail"),
        ('id="plex-tv-not-tdb"', "plex_tv_not_in_tdb"),
        ('id="plex-anime-total"', "plex_anime_total"),
        ('id="plex-anime-motif"', "plex_anime_motif_avail"),
        ('id="plex-anime-not-tdb"', "plex_anime_not_in_tdb"),
        # v1.23.34: top-card foots reframed to themed/total + ready-
        # to-add (the % headline uses ssr_pct, checked separately).
        ('id="cov-movies-themed"', "plex_movies_with_theme"),
        ('id="cov-movies-total"', "plex_movies_total"),
        ('id="cov-movies-ready"', "plex_movies_ready"),
        # v1.23.40: TV is tv-only + ANIME is its own card (was combined).
        ('id="cov-tv-themed"', "plex_tv_with_theme"),
        ('id="cov-tv-total"', "plex_tv_total"),
        ('id="cov-tv-ready"', "plex_tv_ready"),
        ('id="cov-anime-themed"', "plex_anime_with_theme"),
        ('id="cov-anime-total"', "plex_anime_total"),
        ('id="cov-anime-ready"', "plex_anime_ready"),
    ]
    failures = []
    for placeholder, field in placeholder_to_field:
        idx = html.find(placeholder)
        if idx == -1:
            failures.append(f"{placeholder}: element not found in template")
            continue
        block = html[idx:idx + 400]
        if f"ssr_num(_ssr_dash.{field})" not in block:
            failures.append(
                f"{placeholder}: missing ssr_num(_ssr_dash.{field}) call"
            )
    assert not failures, "v1.15.50: " + "; ".join(failures)


def test_template_v1_15_45_calls_migrated_to_macro():
    """Counter-guard: the 11 v1.15.45 inline conditionals must be
    gone (migrated to the macro). The verbose
    `"{:,}".format(_ssr_dash.X) if _ssr_dash.X is not none else "—"`
    pattern leaves the template hard to grep + invites format
    drift on future additions."""
    html = DASH_HTML.read_text()
    # The verbose pattern must not appear anywhere — every call
    # site should use the macro.
    assert '"{:,}".format(_ssr_dash.' not in html, (
        "v1.15.50: v1.15.45 inline conditionals must be migrated "
        "to ssr_num() macro calls — the user: 'standardize across "
        "sets that are similar'"
    )


# ── 3. PLEX ANIME card visibility ─────────────────────────────


def test_plex_anime_card_visibility_ssr_gated():
    """PLEX ANIME card must only be display:none when SSR confirms
    plex_anime_total is 0/None. With ≥1 anime item the card
    renders visible on first paint — no JS reveal → no flash."""
    html = DASH_HTML.read_text()
    anchor = html.index('id="plex-anime-card"')
    # Walk to end of the opening tag.
    end = html.index(">", anchor)
    tag = html[anchor:end + 1]
    assert "{% if not _ssr_dash.plex_anime_total %}" in tag, (
        "v1.15.50: PLEX ANIME card visibility must SSR-gate on "
        "plex_anime_total — `style=display:none` only when 0/None"
    )
    assert 'style="display:none"' in tag, (
        "v1.15.50: PLEX ANIME card must include the conditional "
        "style attr (Jinja `if` wraps it)"
    )
