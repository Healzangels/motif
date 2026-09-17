"""v0.51.344: R3-F1 — a database restore is not staged beside a running RESTORE FROM PLEX (the swap would discard
every row the job stamps), the previews stay open, and the stage answer, its event and the settings banner all say
what the boot swap throws away."""
from __future__ import annotations

import re
import shutil

from app.core import bundle, canonical_health as ch, db_backup
from tests.test_v0_51_339_bundle_staging_boot import _bundle
from test_v0_51_342_restore_from_plex_job import (  # noqa: F401 — env is the (client, settings, tmp_path, events) fixture
    AUTH, CHECK, START, HeldRestore, _finish, env)

RESTORE = "/api/admin/database-restore"
UPLOAD = RESTORE + "/upload"


def _legs(client, settings, tmp_path):
    """The three staging legs: a listed snapshot, a bundle confirm, an uploaded snapshot."""
    snap = db_backup.create_backup(settings.db_path, settings.config_dir, now_stamp="20260914-040001")
    b = _bundle(tmp_path / "mk")
    shutil.copyfile(b, db_backup.backups_dir(settings.config_dir) / b.name)
    off_box = (db_backup.backups_dir(settings.config_dir) / snap.name).read_bytes()  # a real snapshot's bytes, as an off-box copy is
    return b, {
        "a listed snapshot": lambda: client.post(RESTORE, json={"name": snap.name}, headers=AUTH),
        "a bundle confirm": lambda: client.post(RESTORE, json={"name": b.name, "confirm": True, "keep_config": True},
                                                headers=AUTH),
        "an uploaded snapshot": lambda: client.post(UPLOAD, headers=AUTH,
                                                    files={"file": ("off-box.db", off_box, "application/octet-stream")}),
    }


def _staged_events(events):
    return [e for e in events if "restore staged" in str(e.get("message", "")).lower()]


def _pending(settings):
    return bundle.pending_members(settings.db_path, settings.config_dir)


def test_every_staging_leg_is_refused_while_restore_from_plex_runs_and_the_previews_stay_open(env, monkeypatch, tmp_path):
    client, settings, _root, events = env
    held = HeldRestore()
    monkeypatch.setattr(ch, "restore_from_plex", held)
    b, legs = _legs(client, settings, tmp_path)
    assert client.post(START, headers=AUTH).json()["started"] is True
    assert held.entered.wait(10)
    check = client.post(CHECK, headers=AUTH)
    assert check.status_code == 409, "premise: the page's own gate on CHECK"
    running = check.json()["detail"].split(" — ", 1)[0]
    for leg, post in legs.items():
        r = post()
        assert r.status_code == 409, (leg, r.text)
        assert r.json()["detail"] == f"{running} — stage the restore when it finishes", (leg, r.json())
        assert _pending(settings) == [], leg
    assert _staged_events(events) == [], "a refusal announced a staging"
    assert not [p.name for p in settings.config_dir.iterdir() if p.name.startswith(".restore-upload.")], \
        "the refused upload left its temp behind"
    r = client.post(RESTORE, json={"name": b.name}, headers=AUTH)
    assert (r.status_code, r.json()["staged"]) == (200, False), ("the bundle preview stages nothing, so it stays open", r.text)
    r = client.post(UPLOAD, headers=AUTH, files={"file": (b.name, b.read_bytes(), "application/gzip")})
    assert (r.status_code, r.json()["staged"]) == (200, False), ("an uploaded bundle only previews", r.text)
    assert _pending(settings) == []
    held.release.set()
    assert _finish(client)["status"] == "done"
    for leg, post in legs.items():
        r = post()
        assert r.status_code == 200, (leg, r.text)
        assert _pending(settings) == ["database"], leg
        assert bundle.cancel_pending(settings.db_path, settings.config_dir), leg


def test_the_answer_the_event_and_the_banner_say_what_the_swap_discards(env, tmp_path):
    client, settings, _root, events = env
    _b, legs = _legs(client, settings, tmp_path)
    note = bundle.STAGED_DISCARDS_NOTE
    assert "discarded" in note and "restart" in note and "RESTORE FROM PLEX" in note, "premise: the sentence names what goes and when"
    for leg, post in legs.items():
        r = post()
        assert r.status_code == 200, (leg, r.text)
        body = r.json()
        assert body["restart_required"] is True and note in body["message"], (leg, body["message"])
        ev = _staged_events(events)[-1]
        assert note in ev["message"] and ev["level"] == "warning", (leg, ev)
        assert bundle.cancel_pending(settings.db_path, settings.config_dir), leg
    html = client.get("/settings", headers=AUTH).text
    m = re.search(r'id="database-restore-pending"[^>]*>(.*?)id="database-restore-cancel-btn"', html, re.S)
    assert m, "no restore-pending banner on the settings page"
    banner = " ".join(re.sub(r"<[^>]+>", " ", m.group(1)).split())
    assert note in banner, banner
