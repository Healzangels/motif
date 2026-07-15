"""v0.51.160 — loudness measure robustness (real-world testing follow-up).

Many prod theme.mp3s (yt-dlp-extracted, with an embedded cover-art / mjpeg stream)
failed ffmpeg loudnorm with rc=254 / no JSON. `-vn` drops the video stream so the
audio filtergraph completes. This pins that the measure command carries `-vn` and
that the parse-failure path still returns None (best-effort) + logs the ffmpeg tail.
"""
from __future__ import annotations

from app.core import loudness


def test_measure_command_has_vn(monkeypatch):
    captured = {}

    class _Proc:
        returncode = 0
        stdout = ""
        stderr = ('[Parsed_loudnorm_0 @ 0x0]\n{\n"input_i" : "-18.0",\n'
                  '"input_tp" : "-2.0",\n"input_lra" : "5.0"\n}\n')

    def _run(cmd, *a, **k):
        captured["cmd"] = cmd
        return _Proc()

    monkeypatch.setattr(loudness.subprocess, "run", _run)
    loudness.measure_loudness("/x/theme.mp3")
    cmd = captured["cmd"]
    assert "-vn" in cmd, "-vn must be present to drop embedded cover-art streams"
    # -vn must come before the -af/-f output opts (an input-scoped option here).
    assert cmd.index("-vn") < cmd.index("-af")


def test_measure_still_parses_with_vn(monkeypatch):
    class _Proc:
        returncode = 0
        stdout = ""
        stderr = ('[Parsed_loudnorm_0 @ 0x0]\n{\n"input_i" : "-18.0",\n'
                  '"input_tp" : "-2.0",\n"input_lra" : "5.0"\n}\n')

    monkeypatch.setattr(loudness.subprocess, "run", lambda *a, **k: _Proc())
    m = loudness.measure_loudness("/x/theme.mp3")
    assert m == {"loudness_i": -18.0, "true_peak": -2.0, "lra": 5.0}


def test_measure_failure_logs_stderr_tail(monkeypatch, caplog):
    import logging

    class _Proc:
        returncode = 254
        stdout = ""
        stderr = "Some ffmpeg noise\nError: Invalid data found when processing input\n"

    monkeypatch.setattr(loudness.subprocess, "run", lambda *a, **k: _Proc())
    with caplog.at_level(logging.DEBUG, logger="motif.loudness"):
        assert loudness.measure_loudness("/x/theme.mp3") is None  # best-effort
    # the actual ffmpeg error surfaces in the breadcrumb (not just rc).
    assert any("Invalid data found" in r.message for r in caplog.records)
