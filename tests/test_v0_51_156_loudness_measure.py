"""v0.51.156 — theme loudness measurement primitive (loudness feature, Phase 0/A).

The read-only measurement leaf: app/core/loudness.py shells out to ffmpeg's EBU
R128 `loudnorm` filter in analysis mode (print_format=json -f null) and parses the
integrated loudness / true peak / loudness range from ffmpeg's stderr JSON, WITHOUT
touching the file. This is the primitive the later LOUDNESS AUDIT op (which stores
per-row measurements) + the eventual normalize/undo build on.

ffmpeg is container-only (ships for yt-dlp), so the parser is unit-tested against a
real loudnorm stderr sample and the subprocess is stubbed; measure_loudness's
best-effort contract (never raises → None) is checked for the missing-ffmpeg and
missing-file paths.
"""
from __future__ import annotations

from app.core import loudness

# A representative ffmpeg loudnorm stderr block (values are strings, as ffmpeg emits).
_SAMPLE = """\
[Parsed_loudnorm_0 @ 0x7f9a1c0]
{
\t"input_i" : "-24.30",
\t"input_tp" : "-5.12",
\t"input_lra" : "7.30",
\t"input_thresh" : "-34.55",
\t"output_i" : "-16.00",
\t"output_tp" : "-1.49",
\t"output_lra" : "7.60",
\t"output_thresh" : "-26.24",
\t"normalization_type" : "dynamic",
\t"target_offset" : "-0.23"
}
"""


def test_parse_real_loudnorm_block():
    m = loudness._parse_loudnorm_json(_SAMPLE)
    assert m == {"loudness_i": -24.30, "true_peak": -5.12, "lra": 7.30}


def test_parse_tolerates_leading_ffmpeg_noise():
    noisy = ("ffmpeg version 6.0 ...\n"
             "Input #0, mp3, from 'theme.mp3':\n"
             "  Duration: 00:00:22.10, bitrate: 128 kb/s\n" + _SAMPLE)
    m = loudness._parse_loudnorm_json(noisy)
    assert m["loudness_i"] == -24.30 and m["true_peak"] == -5.12


def test_parse_no_json_returns_none():
    assert loudness._parse_loudnorm_json("no json here at all") is None
    assert loudness._parse_loudnorm_json("") is None


def test_parse_missing_input_i_returns_none():
    # a JSON block without the input figures we need is not a usable measurement.
    assert loudness._parse_loudnorm_json('{"output_i": "-16.0"}') is None


def test_measure_wires_parse_from_stderr(monkeypatch):
    # stub the ffmpeg subprocess so the measure→parse wiring is exercised without
    # a real ffmpeg (container-only).
    class _Proc:
        returncode = 0
        stdout = ""
        stderr = _SAMPLE

    monkeypatch.setattr(loudness.subprocess, "run", lambda *a, **k: _Proc())
    m = loudness.measure_loudness("/whatever/theme.mp3")
    assert m == {"loudness_i": -24.30, "true_peak": -5.12, "lra": 7.30}


def test_measure_ffmpeg_missing_returns_none(monkeypatch):
    def _raise(*a, **k):
        raise FileNotFoundError("ffmpeg")

    monkeypatch.setattr(loudness.subprocess, "run", _raise)
    assert loudness.measure_loudness("/x.mp3") is None  # best-effort, no raise


def test_measure_unparseable_returns_none(monkeypatch):
    class _Proc:
        returncode = 1
        stdout = ""
        stderr = "Invalid data found when processing input"

    monkeypatch.setattr(loudness.subprocess, "run", lambda *a, **k: _Proc())
    assert loudness.measure_loudness("/x.mp3") is None
