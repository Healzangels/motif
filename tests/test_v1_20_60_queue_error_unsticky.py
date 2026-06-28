"""v1.20.60 — clear the queue hash on a load error so recovery re-renders.

Bug audit (2026-05-31): my v1.20.54 hash-skip introduced a regression.
loadQueue's catch writes the "queue load failed" row to #jobs-body but
the success render is now hash-gated (skip the write when _jobsHtml ===
dataset.lastHash). After a transient /api/jobs error on an idle queue
(job list unchanged across the blip), the next successful poll computes
_jobsHtml === the last-successful lastHash → skips the write → the red
error row sticks until the jobs actually change. Fix: reset
dataset.lastHash to '' on the error path so the next render always writes.
"""
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
APP_JS = (REPO / "app" / "web" / "static" / "app.js").read_text()


def test_queue_error_path_resets_lasthash():
    anchor = APP_JS.index("queue load failed:")
    body = APP_JS[anchor:anchor + 700]
    assert "tb.dataset.lastHash = ''" in body, (
        "the loadQueue error path must clear dataset.lastHash so a "
        "recovered poll re-renders (else the error row sticks)"
    )


def test_success_render_still_hash_gated():
    """The v1.20.54 hash-skip itself stays — we only added the error-path
    reset, not removed the skip."""
    body = APP_JS[APP_JS.index("async function loadQueue()"):
                  APP_JS.index("function bindQueue()")]
    assert "_jobsBody.dataset.lastHash !== _jobsHtml" in body
    assert "_evBody.dataset.lastHash !== _evHtml" in body


def test_v1_20_60_version_pin():
    init_py = (REPO / "app" / "__init__.py").read_text()
    assert '__version__ = "0.' in init_py
