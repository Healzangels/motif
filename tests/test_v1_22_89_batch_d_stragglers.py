"""v1.22.89 (audit round 2, Batch D final) — four stragglers.

(1) Bulk ACCEPT ALL captured the previous URL only `if not url_match`,
while per-row ACCEPT made the capture unconditional in v1.12.61
precisely because skipping it left previous_url at the pre-SET-URL
value with the wrong kind (INFO card: "previous url: X themerrdb"
for what was a user override moments earlier). Single-vs-bulk mirror
drift; bulk now captures unconditionally too.

(2) /queue?status=failed deep-links SSR'd the right PANEL (v1.22.56)
but the jobs filter chip row hardcoded ALL active, flashing it for a
frame before app.js flipped to FAILED. The route now passes
initial_jobfilter (same allowlist as the JS) and the template
renders chip-active on the matching chip.

(3) SET URL + UPLOAD MP3 dialogs had no in-flight submit guard
(class 3 double-fire): double-Enter/double-click during the POST
await or the 700ms success-close window fired the request twice —
two download jobs for the same row, or a double upload with two
place jobs racing on one sidecar. Both handlers now hold a
closure-scoped submitting flag + disable the submit button, re-armed
on error and after the success close.

(4) The PROBE PLEX THEMES futs drain re-raised any single title's
unexpected exception, 500ing the whole probe and discarding every
other title's finished work. Failures are now stamped per-row as
probe_error and the partial results survive.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
API_PY = (REPO / "app" / "web" / "api.py").read_text()
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()
QUEUE = (REPO / "app" / "web" / "templates" / "queue.html").read_text()


# ── (1) bulk previous-URL capture unconditional ──────────────


def test_bulk_accept_captures_previous_url_unconditionally():
    i = API_PY.index("v1.22.89: capture unconditionally")
    block = API_PY[i:i + 600]
    assert "_capture_previous_url(" in block
    assert "if not url_match:" not in block, (
        "v1.22.89: the bulk path must mirror per-row ACCEPT's "
        "v1.12.61 unconditional capture"
    )


# ── (2) jobs filter chip SSR'd from ?status= ─────────────────


def test_queue_route_ssrs_initial_jobfilter():
    i = API_PY.index("async def queue_page(")
    body = API_PY[i:i + 2000]
    assert "initial_jobfilter" in body
    assert '"pending", "running", "failed", "cancelled", "done"' in body


def test_queue_template_renders_ssr_jobfilter():
    assert "{% set jf = initial_jobfilter | default('all') %}" in QUEUE
    for st in ("all", "pending", "running", "failed", "cancelled",
               "done"):
        assert (f"{{% if jf == '{st}' %}} chip-active{{% endif %}}\""
                f" data-jobfilter=\"{st}\"") in QUEUE


def test_no_hardcoded_active_jobfilter_chip():
    assert ('class="chip chip-active" data-jobfilter="all"'
            not in QUEUE), (
        "v1.22.89: ALL must take chip-active via the jf conditional, "
        "not a hardcoded class"
    )


# ── (3) dialog double-fire guards ────────────────────────────


def _binding(anchor: str) -> str:
    i = APP_JS.index(anchor)
    return APP_JS[i:i + 3600]


def test_manual_url_dialog_has_submit_guard():
    block = _binding("let urlSubmitting = false;")
    assert "if (urlSubmitting) return;" in block
    assert "urlSubmitting = true;" in block
    assert block.count("urlSubmitting = false;") >= 3, (
        "init + success-close re-arm + catch re-arm"
    )
    assert "submitBtn.disabled = true;" in block


def test_upload_dialog_has_submit_guard():
    block = _binding("let uploadSubmitting = false;")
    assert "if (uploadSubmitting) return;" in block
    assert "uploadSubmitting = true;" in block
    assert block.count("uploadSubmitting = false;") >= 3
    assert "submitBtn.disabled = true;" in block


# ── (4) probe keeps partial work on a title failure ──────────


def test_probe_stamps_per_title_error_instead_of_500():
    i = API_PY.index("def _probe_one_safe(")
    block = API_PY[i:i + 900]
    assert "except Exception" in block
    assert 'r["probe_error"] = str(e)' in block
    assert "pool.submit(_probe_one_safe, r)" in API_PY
