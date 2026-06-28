"""v1.22.36 — library stuck-row reconciler.

the user: on download "the DL chip flashing cyan ... but I'll never see the PL
chip blinking and once the status bar completes it will be stuck looking like
this until I manually refresh the page. Not always but annoying."

Root cause: both poll paths that would repaint the row — the 2s libraryRapidPoll
AND the 30s background reload — skip when the user has a text selection / an
open dialog (the v1.10.7 interaction guard). A stray selection or an open
row-menu therefore freezes a row whose download/place already finished
indefinitely; the rendered content-hash is correct, so it heals the moment
polling resumes (manual refresh, or clearing the selection).

Fix: a 6s watcher that fires loadLibrary ONLY when a rendered row still claims
job_in_flight while the backend op-queue has gone idle (anyMutatingOpActive
false) — the exact stale-frontend condition — bypassing the interaction guards
(a demonstrably-stale row should win over a stray selection). loadLibrary's
tbody.dataset.lastHash skip makes it a no-op when nothing changed; in normal
operation an idle backend means no row legitimately has a live job, so it never
fires until the pathology occurs.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_reconciler_exists_and_is_gated_on_stale_in_flight():
    assert "stuck-row reconciler" in APP_JS, (
        "v1.22.36: the stuck-row reconciler watcher must exist")
    i = APP_JS.index("stuck-row reconciler")
    block = APP_JS[i:i + 1600]
    # Fires only when a row claims job_in_flight while the backend is idle.
    assert "!q.anyMutatingOpActive" in block, (
        "must gate on the backend queue being idle")
    assert "it.job_in_flight" in block, (
        "must gate on a rendered row still claiming job_in_flight")
    assert "loadLibrary()" in block


def test_reconciler_runs_on_a_short_cadence():
    i = APP_JS.index("stuck-row reconciler")
    block = APP_JS[i:i + 1600]
    # A short cadence (single-digit seconds) so a stuck row heals quickly —
    # not tied to the 30s background reload.
    assert ", 6000)" in block, (
        "v1.22.36: the reconciler should run on a ~6s cadence")


def test_reconciler_skips_hidden_tab():
    i = APP_JS.index("stuck-row reconciler")
    block = APP_JS[i:i + 1600]
    assert "document.visibilityState !== 'visible'" in block, (
        "v1.22.36: the reconciler must skip while the tab is hidden")


def test_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
