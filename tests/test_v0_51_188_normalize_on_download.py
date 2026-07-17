"""v0.51.188 — condition a theme's loudness as it ARRIVES, before it is placed.

Why this half is cheap. Plex ingests theme.mp3 at scan time and thereafter plays its own
copy, which is why re-normalizing an already-placed theme needs a re-upload to propagate
(the whole v0.51.171-187 arc). A theme normalized BEFORE its first placement has no such
problem: Plex has never seen it, so the only copy it ever ingests is the conditioned one.
No push, no 10MB ceiling, no entry churn. The operator's framing: "we download it and
normalize it during download and pass it over to plex which now has a normalized copy."

Default OFF. This mutates downloaded audio and nobody opts a homelab into that by
accident.

The load-bearing guards here, each earned:
  - a loudness step must NEVER fail a download. A raw theme beats no theme.
  - a SILENT theme measures -inf (v0.51.163, on the real library). gain = target - (-inf)
    = +inf. Never compute a gain from a non-finite measurement.
  - if the file changed, the row must record the POST-gain sha/size, or every staleness
    check keyed on them (the v0.51.169 audit gate, health passes) reads a file that no
    longer exists.
  - "already at target" is NOT normalized: there's no undo tag on it, so calling it
    normalized would make // UNDO promise a restore it cannot perform.
"""
from __future__ import annotations

from pathlib import Path

from app.core.config_file import MotifConfig
from app.core.worker import _cond_columns

REPO = Path(__file__).resolve().parent.parent
WORKER = (REPO / "app" / "core" / "worker.py").read_text()


# ── the toggle ───────────────────────────────────────────────────────────

def test_normalize_on_download_defaults_off():
    """It mutates downloaded audio. Opt in, never inherit."""
    assert MotifConfig().loudness.normalize_on_download is False


def test_default_target_is_minus_18():
    """The operator's audit put the library median at -14.5 LUFS; -18 is the ambient-hover
    lean the target-preview slider was built to settle."""
    assert MotifConfig().loudness.target_lufs == -18.0


def test_a_nonsense_target_is_rejected():
    """target drives `gain = target - measured`, straight onto real audio, so a typo here
    is a typo applied to every download."""
    from app.core.config_file import validate
    for bad in (5.0, -200.0):
        cfg = MotifConfig()
        cfg.loudness.target_lufs = bad
        errs = validate(cfg, require_themes_dir=False)
        assert any("target_lufs" in e for e in errs), f"{bad} should be rejected"


def test_a_sane_target_passes():
    """The guard must not reject the default it ships with."""
    from app.core.config_file import validate
    cfg = MotifConfig()
    assert not any("target_lufs" in e for e in validate(cfg, require_themes_dir=False))


# ── the column mapping (what lands on the row) ───────────────────────────

def test_no_conditioning_stamps_nothing():
    assert _cond_columns(None, "sha") == (None,) * 11


def test_normalized_download_stamps_loudness_and_norm_state():
    cols = _cond_columns(
        {"ok": True, "changed": True, "loudness_i": -18.0, "true_peak": -10.0,
         "lra": 5.0, "applied_db": -13.5, "target": -18.0,
         "orig_sha256": "orig", "orig_pcm_sha256": "pcm"}, "newsha")
    i, tp, lra, at, msha, state, gain, target, nat, osha, opcm = cols
    assert (i, tp, lra) == (-18.0, -10.0, 5.0)      # POST-gain: what was placed
    assert msha == "newsha", "the measurement pins to the bytes it describes"
    assert (state, gain, target) == ("normalized", -13.5, -18.0)
    assert (osha, opcm) == ("orig", "pcm"), "undo needs the pre-gain identity"


def test_already_at_target_is_measured_but_NOT_normalized():
    """No gain applied ⇒ no mp3gain undo tag on the file. Calling it normalized would make
    // UNDO promise a restore it cannot perform."""
    cols = _cond_columns(
        {"ok": True, "changed": False, "note": "no change", "loudness_i": -18.1,
         "true_peak": -9.0, "lra": 4.0, "applied_db": 0.0, "target": -18.0}, "sha")
    assert cols[0] == -18.1          # measured
    assert cols[5] is None, "norm_state must stay NULL — nothing was gained"


def test_a_silent_theme_is_measured_as_nothing_and_left_raw():
    """-inf poison (v0.51.163). condition_new_download refuses to gain from it, and the
    row must not claim a loudness it doesn't have."""
    cols = _cond_columns({"ok": False, "reason": "non-finite measurement (-inf)",
                          "loudness_i": None}, "sha")
    assert cols[0] is None and cols[4] is None, "no loudness, so no measurement pin"
    assert cols[5] is None


def test_a_failed_normalize_still_keeps_the_measurement():
    """The measurement cost real ffmpeg time and is still true of the raw bytes — the
    audit would only have to redo it."""
    cols = _cond_columns({"ok": False, "reason": "mp3gain missing",
                          "loudness_i": -6.0, "true_peak": 1.0}, "rawsha")
    assert (cols[0], cols[1]) == (-6.0, 1.0)
    assert cols[4] == "rawsha"
    assert cols[5] is None, "raw bytes are not normalized"


# ── the download must survive a loudness failure ─────────────────────────

def test_conditioning_never_fails_the_download():
    """A raw theme beats no theme. The hook logs and records the raw download."""
    i = WORKER.index("if self.settings.normalize_on_download:")
    block = WORKER[i:WORKER.index("self._record_local_file(", i)]
    assert "elif cond and not cond.get(\"ok\"):" in block
    assert "recording the raw download" in block
    assert "raise" not in block, "a loudness step must never abort a download"


def test_the_post_gain_sha_is_what_gets_recorded():
    """If the file changed under us, the row must describe the file that now exists —
    every staleness check keys on file_sha256."""
    i = WORKER.index("if self.settings.normalize_on_download:")
    block = WORKER[i:WORKER.index("self._record_local_file(", i)]
    assert 'sha256, size = cond["file_sha256"], result.file_path.stat().st_size' in block
    # and the writer is handed those, not the raw ones
    call = WORKER[WORKER.index("self._record_local_file(", i):]
    assert "rel_path=rel_path, sha256=sha256, size=size," in call[:400]


def test_conditioning_happens_before_the_row_is_recorded():
    """Ordering is the whole point: normalize BEFORE placement, so the only copy Plex ever
    ingests is the conditioned one and no propagation is needed."""
    cond_at = WORKER.index("condition_new_download(")
    record_at = WORKER.index("self._record_local_file(\n            media_type=media_type")
    assert cond_at < record_at


def test_a_redownload_replaces_the_normalize_state():
    """Fresh raw bytes with a stale norm_state='normalized' would tell // UNDO to un-gain
    a file that was never gained."""
    # anchor on the local_files INSERT specifically — worker.py has several ON CONFLICTs
    # and the first one belongs to a different table entirely.
    i = WORKER.index("INSERT INTO local_files")
    block = WORKER[i:WORKER.index('"""', WORKER.index("DO UPDATE SET", i))]
    for col in ("norm_state = excluded.norm_state",
                "norm_gain_db = excluded.norm_gain_db",
                "loudness_i = excluded.loudness_i",
                "norm_orig_sha256 = excluded.norm_orig_sha256"):
        assert col in block, f"{col} must be replaced on re-download"


# ── the engine helper ────────────────────────────────────────────────────

def test_condition_new_download_refuses_a_non_finite_measurement(tmp_path, monkeypatch):
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3" * 50)
    from app.core import loudness_apply as la
    monkeypatch.setattr("app.core.loudness.measure_loudness",
                        lambda p, *a, **k: {"loudness_i": float("-inf"),
                                            "true_peak": -70.0, "lra": 0.0})
    called = []
    monkeypatch.setattr(la, "normalize_file", lambda *a, **k: called.append(1))
    res = la.condition_new_download(theme, target_lufs=-18.0)
    assert res["ok"] is False
    assert "non-finite" in res["reason"]
    assert not called, "must never compute a gain from -inf"


def test_condition_new_download_leaves_the_file_alone_when_unmeasurable(tmp_path,
                                                                       monkeypatch):
    theme = tmp_path / "theme.mp3"
    theme.write_bytes(b"ID3" * 50)
    from app.core import loudness_apply as la
    monkeypatch.setattr("app.core.loudness.measure_loudness", lambda p, *a, **k: None)
    called = []
    monkeypatch.setattr(la, "normalize_file", lambda *a, **k: called.append(1))
    res = la.condition_new_download(theme, target_lufs=-18.0)
    assert res["ok"] is False and not called
