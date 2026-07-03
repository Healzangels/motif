"""v1.19.42: cloud-themes-backup walker + writer.

Plex Pass cloud themes are served only while the user's Plex Pass
is active. If Plex Pass lapses (or Plex's cloud catalog drops the
entry, or the item is removed-and-re-added in Plex with a new
rating_key), every P-row depending on a `metadata://themes/<sha1>`
entry stops playing — instantly, silently, no recovery if motif
never staged a backup.

This module is the staging side. It walks `plex_items.has_theme=1`
rows, asks Plex's `/library/metadata/{rk}/themes` endpoint for
each row's theme catalog, identifies C1 rows (single `metadata://`
entry — Plex cloud is the sole source of the bytes), downloads
the bytes via the v1.18.36 re-upload trick path
(`/library/metadata/{rk}/file?url=<entry_rk>`), and stages them
as `local_files` rows with `source_kind='plex_cloud'` +
`last_place_attempt_reason='backup_only'`.

The `backup_only` stamp reuses the v1.19.21 BK pipe end-to-end:
- BK badge surfaces in the LINK column (v1.19.43 widens to B)
- v1.19.21 retry-sweep skip leaves the backup unplaced
- v1.19.35 PROMOTE TO ACTIVE BK-no-override branch deploys the
  backup on user click (via the v1.18.36 re-upload trick — motif
  uploads its copy back to Plex)
- v1.19.41 theme_lost_backup_ready notification fires when the
  source plex_item's has_theme transitions 1→0

The taxonomy from the user's v1.18.85/.87 probe characterized 18
representative rows into 6 categories:
- C1: single `metadata://` entry, no upload sibling — BACKUP TARGET
- C2: `metadata://` selected, `upload://` siblings exist (motif/
  themerr-plex backup already in /themes) — no backup needed
- C3: `upload://` selected, `metadata://` sibling exists — no
  cloud-backup pressure (selected is upload, not cloud)
- C4/C5/C6: variations where Plex serves a motif-or-user upload,
  not cloud — backup unnecessary

Pre-flight characterization (2026-05-26, n=16 stratified):
- 50% C1 overall, with section variance:
  - Anime: 4/4 = 100% C1 (Plex Pass cloud is de facto source
    for niche libraries)
  - Movies/TV mixed (50% C1, remaining 50% upload-selected)
- ~1,940 C1 rows expected across 3,883 candidate P-rows
- Mean C1 size ~2.2 MB → ~4.2 GB total storage estimate

Rate-limiting + resumability:
- Walker batches in chunks of 50 with 200ms inter-call sleep
  (~6.5 min for full walk, polite to Plex)
- Cursor persisted in runtime_settings as
  `cloud_backup_walker_cursor` — restart picks up where it
  left off, doesn't re-probe already-classified rows
- Cancellable via existing op_progress cancel infrastructure

Writer contract (every column matters per v1.19.18 / v1.19.21 /
v1.19.32 / v1.19.33 / v1.19.37 audit lessons):
- source_kind='plex_cloud' (v58 CHECK addition)
- source_video_id=<sha1> (NOT NULL — SHA-1 is the natural ID)
- file_sha256=<sha256 of buffered body> (v1.19.18 backfill
  walker picks up NULL otherwise)
- file_size=<Content-Length>
- downloaded_at=now_iso()
- provenance='auto' (cloud-backup is automatic, mirrors
  themerrdb writer)
- last_place_attempt_reason='backup_only' — LOAD-BEARING:
  gates BK pipe reuse (v1.19.35 PROMOTE branch + v1.19.21
  retry-sweep skip both filter on this stamp)
- last_place_attempt_at=now_iso()
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from .canonical import canonical_theme_subdir
from .events import now_iso

log = logging.getLogger("motif.cloud_theme_backup")


# v1.19.42: SHA-1 in the metadata://themes/<sha1> entry URI is the
# natural source_video_id (40 hex chars). Group capture so callers
# can extract it without re-parsing.
_SHA1_TAIL_RE = re.compile(r"([0-9a-f]{40})$", re.IGNORECASE)


# v1.19.42: cursor key for resumable walks. Persisted in
# runtime_settings so a worker restart picks up at the last
# processed rating_key rather than re-walking the whole catalog.
CURSOR_KEY = "cloud_backup_walker_cursor"


def _sha1_from_entry_uri(uri: str) -> str | None:
    """Extract the trailing SHA-1 from a metadata://themes/<sha1>
    or upload://themes/<sha1> URI. Returns None for malformed
    URIs (caller treats as 'skip this entry')."""
    if not uri:
        return None
    m = _SHA1_TAIL_RE.search(uri)
    return m.group(1).lower() if m else None


def _classify_themes_response(themes_body: Any) -> dict | None:
    """Return the C1 selection dict, or None if the row isn't C1.

    Backup target = the Plex-SELECTED entry, when it's a
    `metadata://` (cloud-served) entry. Upload-selected rows
    don't need cloud-backup; cloud-selected rows do.

    v1.19.51: classifier rewritten to be SELECTED-aware after
    the user's 2026-05-27 probe on '90 Day Fiancé: Happily Ever
    After?'. The /themes response had THREE entries sharing the
    same SHA-1 — one `metadata://` (selected: true) plus two
    `upload://` siblings (defunct themerr-plex agent +
    tv.plex.agents.series). Pre-fix the C1 classifier rejected
    the row because of the upload siblings, on the assumption
    "motif's upload already covers the bytes." That assumption
    is wrong when motif has NO local_files row for the row
    (which the SQL push-down filter already enforces): those
    uploads aren't motif-managed, the themerr-plex plugin is
    defunct, and they may break alongside Plex Pass anyway.

    New criterion (much simpler):
      1. Find the entry with `selected: true` (Plex emits this
         on the currently-serving entry).
      2. If selected entry is `metadata://` → BACKUP TARGET.
      3. If selected entry is `upload://` → no backup needed
         (Plex is serving an upload; nothing cloud-Pass-dependent
         to lose).
      4. If no entry has `selected` set (rare; legacy Plex
         builds without the flag) → fall back to the original
         v1.19.42 strict-C1 heuristic.

    Returns: `{"entry_uri": "metadata://themes/<sha1>",
              "sha1": "<sha1>"}` for backup targets; None
    otherwise.
    """
    if not isinstance(themes_body, dict):
        return None
    container = themes_body.get("MediaContainer") or {}
    metadata = container.get("Metadata") or []
    if not isinstance(metadata, list) or not metadata:
        return None
    # v1.19.51: honor Plex's `selected: true` flag.
    selected_entry: dict | None = None
    any_selected_flag_seen = False
    for entry in metadata:
        if not isinstance(entry, dict):
            continue
        if "selected" in entry:
            any_selected_flag_seen = True
        if entry.get("selected") is True:
            selected_entry = entry
            break
    if not any_selected_flag_seen:
        # Legacy Plex (no selected flag at all) → fall back to
        # the v1.19.42 strict-C1 heuristic: single metadata://
        # entry, no upload sibling. Production Plex builds
        # (verified 2026-05-27) always emit the flag; this branch
        # exists for builds we haven't seen.
        cloud_entries = [
            e for e in metadata
            if isinstance(e, dict)
            and isinstance(e.get("ratingKey"), str)
            and e["ratingKey"].startswith("metadata://")
        ]
        upload_shas = {
            _sha1_from_entry_uri(e.get("ratingKey") or "")
            for e in metadata
            if isinstance(e, dict)
            and isinstance(e.get("ratingKey"), str)
            and e["ratingKey"].startswith("upload://")
        }
        upload_shas.discard(None)
        if len(cloud_entries) != 1:
            return None
        pick = cloud_entries[0]
        pick_sha = _sha1_from_entry_uri(pick.get("ratingKey") or "")
        if pick_sha is None or pick_sha in upload_shas:
            return None
        return {"entry_uri": pick["ratingKey"], "sha1": pick_sha}
    if selected_entry is None:
        # Flag was seen but nothing was selected — ambiguous,
        # don't back up.
        return None
    uri = selected_entry.get("ratingKey") or ""
    if not isinstance(uri, str) or not uri.startswith("metadata://"):
        # Selected entry is an upload:// or unknown scheme —
        # not cloud-served, no backup pressure.
        return None
    sha = _sha1_from_entry_uri(uri)
    if sha is None:
        return None
    return {"entry_uri": uri, "sha1": sha}


def _classify_themes_response_force(themes_body: Any) -> dict | None:
    """v1.20.18: force variant of `_classify_themes_response`.

    Returns the SELECTED entry's {entry_uri, sha1} regardless of
    scheme — `metadata://` OR `upload://`. The strict classifier
    skips `upload://`-selected rows (Plex is serving an upload, no
    Plex-Pass-cloud loss pressure); force mode instead captures
    WHATEVER Plex is currently serving, so the operator can stage a
    themerr-plex embed (or any served upload) as a Plex Backup on
    deliberate demand.

    the user's call (2026-05-29): on a non-C1 row, DOWNLOAD PLEX BACKUP
    offers a confirm to "capture whatever Plex IS serving." This is
    that path's classifier. The sha256 dedup guard in
    `backup_cloud_theme` prevents a no-op relabel when the served
    bytes are byte-identical to motif's existing canonical (the
    common "Plex echoing motif's own upload back" case).

    Legacy Plex builds with no `selected` flag at all fall back to
    the strict C1 heuristic (we can't know what's serving without
    the flag, so don't guess — production Plex always emits it).
    """
    if not isinstance(themes_body, dict):
        return None
    container = themes_body.get("MediaContainer") or {}
    metadata = container.get("Metadata") or []
    if not isinstance(metadata, list) or not metadata:
        return None
    selected_entry: dict | None = None
    any_selected_flag_seen = False
    for entry in metadata:
        if not isinstance(entry, dict):
            continue
        if "selected" in entry:
            any_selected_flag_seen = True
        if entry.get("selected") is True:
            selected_entry = entry
            break
    if not any_selected_flag_seen:
        # No flag → can't tell what's serving; defer to strict.
        return _classify_themes_response(themes_body)
    if selected_entry is None:
        return None
    uri = selected_entry.get("ratingKey") or ""
    if not isinstance(uri, str) or not (
        uri.startswith("metadata://") or uri.startswith("upload://")
    ):
        return None
    sha = _sha1_from_entry_uri(uri)
    if sha is None:
        return None
    return {"entry_uri": uri, "sha1": sha}


def _get_cursor(conn) -> str | None:
    """Read the resumable-walk cursor (last rating_key processed)
    from runtime_settings. None on fresh start."""
    row = conn.execute(
        "SELECT value FROM runtime_settings WHERE key = ?",
        (CURSOR_KEY,),
    ).fetchone()
    if row is None:
        return None
    val = row["value"] if hasattr(row, "keys") else row[0]
    return str(val) if val else None


def _set_cursor(conn, rating_key: str | None) -> None:
    """Persist the cursor. Pass None to clear (end-of-walk)."""
    if rating_key is None:
        conn.execute(
            "DELETE FROM runtime_settings WHERE key = ?",
            (CURSOR_KEY,),
        )
        return
    conn.execute(
        "INSERT INTO runtime_settings (key, value, updated_at, updated_by) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET "
        "  value = excluded.value, "
        "  updated_at = excluded.updated_at, "
        "  updated_by = excluded.updated_by",
        (CURSOR_KEY, rating_key, now_iso(), "cloud_backup_walker"),
    )


def _resolve_or_mint_tmdb_id(
    conn, r, motif_mt: str, *, mint: bool,
    minted_out: list | None = None,
) -> int | None:
    """v1.21.32: the tmdb_id a backup row gets keyed to.

    A real `guid_tmdb` wins. Else an existing orphan id reachable via
    `theme_id` (the row was already adopted/uploaded/backed-up). Else —
    only on the FORCE path — mint a synthetic NEGATIVE orphan id: allocate
    MIN(tmdb_id)-1, INSERT a plex_orphan themes row, stamp plex_items.
    theme_id. This is exactly what UPLOAD MP3 (api.py) + backup_cloud_theme
    already do for orphans; doing it here, atomically, lets a no-TDB row
    (notably a collection with NULL guid_tmdb — the user's A24 Films repro)
    become backable. Creating the themes row at MINT time (not deferring to
    the writer) means two back-to-back mints in one run can't both grab the
    same MIN-1. Returns None when there's no id and minting isn't allowed
    (the non-force walk, where the candidate query already excludes these).

    v0.51.16 (audit #26): when a mint fires, its (media_type, tmdb_id)
    pair is appended to `minted_out` so the caller can compensate — the
    mint commits at WALK time, minutes before the download stage's
    local_files write, and a fetch failure / cancel in between would
    otherwise strand a linked-but-empty plex_orphan row (the exact state
    the v1.19.79 writer-atomicity fix eliminated inside
    backup_cloud_theme). See unmint_stale_orphans.
    """
    g = r["guid_tmdb"]
    if g is not None:
        return int(g)
    tid = r["theme_id"] if "theme_id" in r.keys() else None
    if tid is not None:
        row = conn.execute(
            "SELECT tmdb_id FROM themes WHERE id = ?", (tid,),
        ).fetchone()
        if row is not None and row["tmdb_id"] is not None:
            return int(row["tmdb_id"])
    if not mint:
        return None
    from .db import transaction
    now = now_iso()
    with transaction(conn):
        lo = conn.execute(
            "SELECT MIN(tmdb_id) AS lo FROM themes "
            "WHERE media_type = ? AND tmdb_id < 0", (motif_mt,),
        ).fetchone()
        min_tmdb = lo["lo"] if lo and lo["lo"] is not None else 0
        synth = min(min_tmdb, 0) - 1
        # v1.21.33: mirror of the backup_cloud_theme writer's orphan-mint
        # block — keep the plex_orphan shape + MIN-1 allocation in sync
        # across both sites if either changes. year as NULL (not '') to
        # match the writer's normalization; '' would miss the year-keyed
        # orphan-promotion match in sync.py (collections carry year='').
        # v1.22.52: stamp the item's guid_imdb on the minted orphan so the
        # de-orphan walker (and sync's orphan-promotion) can resolve it later.
        # Pre-fix the mint stored NO imdb — these rows were permanently
        # unresolvable. Local SELECT only (no network inside the txn).
        _imdb_row = conn.execute(
            "SELECT guid_imdb FROM plex_items WHERE rating_key = ?",
            (str(r["rating_key"]),),
        ).fetchone()
        _mint_imdb = _imdb_row["guid_imdb"] if _imdb_row else None
        cur = conn.execute(
            "INSERT INTO themes (media_type, tmdb_id, imdb_id, title, year, "
            "  upstream_source, last_seen_sync_at, first_seen_sync_at) "
            "VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)",
            (motif_mt, synth, _mint_imdb, r["title"] or "",
             r["year"] or None, now, now),
        )
        conn.execute(
            "UPDATE plex_items SET theme_id = ? "
            "WHERE rating_key = ? AND theme_id IS NULL",
            (cur.lastrowid, str(r["rating_key"])),
        )
    log.info(
        "cloud_backup: minted synthetic tmdb_id=%s for rk=%s (%s) — "
        "Plex serves a theme but TDB doesn't track this title",
        synth, r["rating_key"], motif_mt,
    )
    if minted_out is not None:
        minted_out.append((motif_mt, synth))
    return synth


def unmint_stale_orphans(conn, targets: list[dict]) -> int:
    """v0.51.16 (audit #26): compensating unmint for force-path walk mints.

    The force walk mints plex_orphan themes rows + stamps plex_items.
    theme_id BEFORE the download stage (deliberate — MIN-1 allocation
    safety, see _resolve_or_mint_tmdb_id). If the download then fails or
    the run is cancelled, the mint strands a linked-but-empty orphan:
    theme_id is non-NULL so every resolve pass gated on `theme_id IS
    NULL` skips the row forever. Called at the run's exits with the full
    target list; deletes ONLY rows this run minted (target['minted'])
    that are still plex_orphan, still synthetic (tmdb_id < 0), and got
    NO local_files row — the v1.18.10 fail-safe-sweep rule. The themes
    DELETE fires the plex_items.theme_id ON DELETE SET NULL FK; the
    explicit UPDATE is belt-and-braces so the unstamp never depends on
    FK enforcement being on.
    """
    from .db import transaction
    removed = 0
    for t in targets:
        if not t.get("minted"):
            continue
        mt = t["media_type"]
        tid = int(t["guid_tmdb"])
        with transaction(conn):
            row = conn.execute(
                "SELECT id FROM themes "
                " WHERE media_type = ? AND tmdb_id = ? AND tmdb_id < 0 "
                "   AND upstream_source = 'plex_orphan' "
                "   AND NOT EXISTS (SELECT 1 FROM local_files lf "
                "        WHERE lf.media_type = themes.media_type "
                "          AND lf.tmdb_id = themes.tmdb_id)",
                (mt, tid),
            ).fetchone()
            if row is None:
                continue
            conn.execute(
                "UPDATE plex_items SET theme_id = NULL WHERE theme_id = ?",
                (row["id"],),
            )
            conn.execute("DELETE FROM themes WHERE id = ?", (row["id"],))
            removed += 1
            log.info(
                "cloud_backup: unminted stale orphan tmdb_id=%s (%s) — "
                "backup never completed for rk=%s",
                tid, mt, t.get("rating_key"),
            )
    return removed


def identify_c1_rows(
    conn,
    plex_client,
    *,
    only_anime: bool = False,
    batch_size: int = 50,
    inter_call_sleep_s: float = 0.2,
    on_progress=None,
    cancel_check=None,
    use_cursor: bool = True,
    rks_scope: list[str] | None = None,
    allow_existing_local: bool = False,
    force: bool = False,
) -> list[dict]:
    """Walk plex_items.has_theme=1 P-rows and return the C1 target
    set — rows where Plex's cloud catalog is the SOLE source of
    the bytes.

    P-row predicate: `has_theme=1` AND no `local_files` row
    (no motif-managed backup yet) AND no `placements` row (not
    motif-served). Matches the SRC=P axis the UI surfaces.

    Per-row work:
      1. Call `plex.get_themes(rating_key=rk)` (already a
         best-effort wrapper — never raises, returns
         `{ok, http_status, body}`).
      2. Pass `body` to `_classify_themes_response` — returns
         C1 selection dict or None.
      3. If C1: emit a target descriptor for the writer.

    `only_anime=True` filters to anime sections (probe showed
    100% C1 hit rate there — best ROI cohort).

    Resumable: writes cursor to runtime_settings after each
    batch. Restart picks up at the cursor.

    Cancellable: caller can pass `cancel_check` callable; walker
    breaks out at the next batch boundary if it returns True.
    """
    # v1.20.18: force capture always swaps an existing canonical
    # (or stages onto a pure-P row), so it implies allow_existing_local
    # — the candidate SQL must not filter out rows that already have a
    # non-plex_cloud local_files row (the TB/UB we're replacing).
    if force:
        allow_existing_local = True
    # Resolve anime sections once for the filter check.
    anime_sections: set[str] = set()
    if only_anime:
        for r in conn.execute(
            "SELECT section_id FROM plex_sections WHERE is_anime = 1"
        ).fetchall():
            sid = r["section_id"] if hasattr(r, "keys") else r[0]
            anime_sections.add(str(sid))
    # Cursor handling: pick up where we left off if the prior
    # run was cancelled / crashed.
    cursor = _get_cursor(conn) if use_cursor else None
    # Candidate query: P-rows (has_theme=1 + no local_files + no
    # placement). Sort by rating_key so the cursor is monotonic.
    # plex_items.media_type uses Plex's strings ('movie', 'show',
    # 'collection') — motif's local_files/placements use motif's
    # ('movie', 'tv', 'collection'). The CASE translates 'show'
    # → 'tv' so the NOT EXISTS subqueries hit the right rows.
    # v1.19.62: `allow_existing_local=True` drops the NOT EXISTS
    # local_files clause so PS-with-DL rows (Plex serves + motif
    # has a non-plex_cloud canonical) become C1 candidates.
    # backup_cloud_theme's ON CONFLICT clause REPLACES the existing
    # local_files row with the plex_cloud bytes. the user's
    # "86 EIGHTY-SIX" repro: SRC=P + DL=green TDB download +
    # LINK=BK (post-v1.19.61) — user can DOWNLOAD PLEX BACKUP to
    # swap the TDB canonical for Plex's actual cloud theme bytes.
    # We additionally exclude existing source_kind='plex_cloud'
    # rows (already a Plex backup, nothing to do) so the click is
    # a no-op when the row's already plex_cloud-backed.
    sql_parts = [
        "SELECT pi.rating_key, pi.guid_tmdb, pi.theme_id, pi.media_type, "
        "       pi.section_id, pi.title, pi.year, pi.edition_key "
        "  FROM plex_items pi "
        " WHERE pi.has_theme = 1 "
    ]
    # v1.21.32: the bulk/strict walk still requires a real TMDB guid — we
    # don't mass-mint orphan ids for thousands of rows. The FORCE path
    # (single-row DOWNLOAD PLEX BACKUP) drops the gate so a no-TDB row —
    # notably a collection with NULL guid_tmdb (the user's A24 Films repro) —
    # can be captured; its tmdb_id is resolved-or-minted per row below.
    if not force:
        sql_parts.append("   AND pi.guid_tmdb IS NOT NULL ")
    # v1.21.70: every NOT EXISTS below is scoped to pi.edition_key so a
    # SIBLING edition's local_files / placement doesn't disqualify THIS
    # edition as a backup candidate. Pre-fix the user's Extended edition
    # found 0 C1 targets because the Theatrical/Sam editions of the same
    # (tmdb, section) were already placed — the un-scoped NOT EXISTS saw
    # their placements and excluded the whole title.
    if not allow_existing_local:
        sql_parts.append(
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM local_files lf "
            "      WHERE lf.tmdb_id = pi.guid_tmdb "
            "        AND lf.section_id = pi.section_id "
            "        AND lf.edition_key = pi.edition_key "
            "        AND lf.media_type = CASE pi.media_type "
            "                              WHEN 'show' THEN 'tv' "
            "                              ELSE pi.media_type END"
            "   ) "
        )
    else:
        # Exclude rows that ALREADY have plex_cloud local_files —
        # nothing to do, the row is already cloud-backed.
        sql_parts.append(
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM local_files lf "
            "      WHERE lf.tmdb_id = pi.guid_tmdb "
            "        AND lf.section_id = pi.section_id "
            "        AND lf.edition_key = pi.edition_key "
            "        AND lf.media_type = CASE pi.media_type "
            "                              WHEN 'show' THEN 'tv' "
            "                              ELSE pi.media_type END"
            "        AND lf.source_kind = 'plex_cloud'"
            "   ) "
        )
    sql_parts.append(
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM placements pl "
        "      WHERE pl.tmdb_id = pi.guid_tmdb "
        "        AND pl.section_id = pi.section_id "
        "        AND pl.edition_key = pi.edition_key "
        "        AND pl.media_type = CASE pi.media_type "
        "                              WHEN 'show' THEN 'tv' "
        "                              ELSE pi.media_type END"
        "   )"
    )
    sql = "".join(sql_parts)
    params: list = []
    # v1.19.45: CRITICAL bug fix — push rks_scope into the SQL
    # WHERE clause so single-row "BACKUP THIS THEME" clicks
    # short-circuit instead of walking the full ~3,883 P-row
    # catalog before filtering. Pre-fix the v1.19.42 endpoint
    # walked everything → filtered post-walk; a single-row click
    # took 13+ minutes minimum (200ms × 3,883 inter-call sleep).
    # the user's repro: clicked BACKUP THIS THEME on one TV row →
    # browser timed out → entire UI locked because the async
    # endpoint contained sync work that blocked the event loop.
    if rks_scope is not None:
        if not rks_scope:
            return []  # explicit empty scope = nothing to do
        placeholders = ", ".join("?" * len(rks_scope))
        sql += f" AND pi.rating_key IN ({placeholders})"
        params.extend(rks_scope)
    if cursor is not None:
        sql += " AND pi.rating_key > ?"
        params.append(cursor)
    sql += " ORDER BY pi.rating_key ASC"
    rows = conn.execute(sql, tuple(params)).fetchall()
    if only_anime:
        rows = [r for r in rows if str(r["section_id"]) in anime_sections]
    targets: list[dict] = []
    total = len(rows)
    # v1.19.45: log breadcrumb at walk START so docker logs show
    # something during the multi-minute walk. v1.18.7 cold-path-
    # needs-MORE-logging lesson — pre-fix the walker logged only
    # at end (or on per-row Plex errors), so a stuck walk left
    # `docker logs motif` silent for the entire 13min duration.
    log.info(
        "cloud_backup walker: starting walk over %d candidate row(s) "
        "(only_anime=%s, rks_scope=%s, cursor=%s, batch_size=%d)",
        total, only_anime,
        f"[{len(rks_scope)} rks]" if rks_scope is not None else None,
        cursor, batch_size,
    )
    if on_progress:
        try:
            on_progress({"phase": "start", "total": total})
        except Exception as e:  # noqa: BLE001
            log.debug("on_progress start callback raised: %r", e)
    # v1.19.80 (audit LOW): track cancellation in a local flag instead
    # of re-calling cancel_check() at walk completion. The second call
    # was both redundant (a clean loop exit already means "not
    # cancelled") and a TOCTOU window — a cancel arriving AFTER the
    # last batch finished would wrongly preserve the cursor even though
    # the walk fully completed.
    cancelled = False
    for batch_start in range(0, total, batch_size):
        if cancel_check is not None:
            try:
                if cancel_check():
                    log.info(
                        "identify_c1_rows: cancellation requested "
                        "at batch_start=%d (cursor remains for resume)",
                        batch_start,
                    )
                    cancelled = True
                    break
            except Exception as e:  # noqa: BLE001
                log.debug("cancel_check raised: %r", e)
        batch = rows[batch_start:batch_start + batch_size]
        for r in batch:
            rk = str(r["rating_key"])
            resp = plex_client.get_themes(rating_key=rk)
            if not resp.get("ok"):
                # Plex hiccup on this row — skip but breadcrumb.
                # Class-9 cold-path: rare failures need explicit
                # logging so a future "why didn't this row get
                # backed up?" debug has signal (v1.18.7 lesson).
                log.info(
                    "cloud_backup walker: skipping rk=%s "
                    "(http_status=%s, error=%r)",
                    rk, resp.get("http_status"), resp.get("error"),
                )
            else:
                try:
                    # v1.20.18: force mode captures whatever Plex is
                    # serving (selected entry, any scheme); strict mode
                    # only captures cloud-served (metadata://) entries.
                    pick = (
                        _classify_themes_response_force(resp.get("body"))
                        if force
                        else _classify_themes_response(resp.get("body"))
                    )
                except Exception as e:  # noqa: BLE001
                    # v1.19.79 (Opus-4.8 audit MED): a malformed /themes
                    # body must not abort the whole multi-minute walk —
                    # skip the row with a breadcrumb (cold-path logging,
                    # v1.18.7 lesson).
                    log.warning(
                        "cloud_backup walker: rk=%s classify raised %r "
                        "— skipping", rk, e,
                    )
                    pick = None
                if pick is not None:
                    motif_mt = (
                        "tv" if r["media_type"] == "show"
                        else r["media_type"]
                    )
                    # v1.21.32: resolve (or, force-only, mint) the tmdb_id
                    # this backup keys to. The call site passes
                    # target["guid_tmdb"] to backup_cloud_theme as tmdb_id.
                    # v0.51.16 (audit #26): collect mints so the target can
                    # carry a 'minted' flag for the caller's compensating
                    # unmint_stale_orphans on failure/cancel exits.
                    _minted_pairs: list = []
                    tmdb_id = _resolve_or_mint_tmdb_id(
                        conn, r, motif_mt, mint=force,
                        minted_out=_minted_pairs)
                    if tmdb_id is None:
                        # v1.21.33: defensive — unreachable on current
                        # callers (a NULL-guid row only enters the loop on
                        # the force path, which sets mint=True, so the helper
                        # always mints). Kept as a guard against a future
                        # mint=False/NULL-guid caller; message states the
                        # real condition (theme_id may be set-but-unresolvable
                        # via a dangling FK, not necessarily NULL).
                        log.info(
                            "cloud_backup: skipping rk=%s — no usable tmdb_id "
                            "(guid_tmdb NULL, theme_id absent or unresolvable)",
                            rk,
                        )
                    else:
                        targets.append({
                            "rating_key": rk,
                            "guid_tmdb": tmdb_id,
                            "media_type": motif_mt,
                            "section_id": str(r["section_id"]),
                            # v1.21.70: carry the edition so the writer keys
                            # local_files + the canonical path to it.
                            "edition_key": (r["edition_key"] or ""
                                            if "edition_key" in r.keys()
                                            else ""),
                            "title": r["title"] or "",
                            "year": r["year"] or "",
                            "entry_uri": pick["entry_uri"],
                            "sha1": pick["sha1"],
                            # v0.51.16 (audit #26): True iff THIS resolve
                            # minted — gates unmint_stale_orphans.
                            "minted": (motif_mt, tmdb_id) in _minted_pairs,
                        })
            # Inter-call sleep (politeness to Plex). 200ms ×
            # ~3,883 P-rows = ~13min worst case; with batching
            # the sleep dominates over the HTTP RTT.
            if inter_call_sleep_s > 0:
                time.sleep(inter_call_sleep_s)
        # Advance cursor at batch boundary.
        last_rk = str(batch[-1]["rating_key"])
        if use_cursor:
            _set_cursor(conn, last_rk)
        if on_progress:
            try:
                on_progress({
                    "phase": "batch",
                    "processed": min(batch_start + batch_size, total),
                    "total": total,
                    "c1_count": len(targets),
                })
            except Exception as e:  # noqa: BLE001
                log.debug("on_progress batch callback raised: %r", e)
    # Walk completed — clear cursor so next run starts fresh.
    if use_cursor and not cancelled:
        _set_cursor(conn, None)
    if on_progress:
        try:
            on_progress({
                "phase": "end",
                "total": total,
                "c1_count": len(targets),
            })
        except Exception as e:  # noqa: BLE001
            log.debug("on_progress end callback raised: %r", e)
    log.info(
        "identify_c1_rows: walked %d rows, found %d C1 targets "
        "(anime_only=%s)",
        total, len(targets), only_anime,
    )
    return targets


def _section_themes_subdir(conn, section_id: str) -> str:
    """Resolve the section's themes_subdir slug. Raises if the
    section row is missing or has no slug (caller treats as
    backup-aborted for this row — log + skip)."""
    row = conn.execute(
        "SELECT themes_subdir FROM plex_sections "
        " WHERE section_id = ?",
        (section_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"cloud_backup: no plex_sections row for "
            f"section_id={section_id!r}"
        )
    slug = (row["themes_subdir"] or "").strip()
    if not slug:
        raise RuntimeError(
            f"cloud_backup: plex_sections.themes_subdir empty "
            f"for section_id={section_id!r} — section needs "
            f"re-allocation via sections.reassign_themes_subdir"
        )
    return slug


def backup_cloud_theme(
    conn,
    target: dict,
    themes_dir: Path,
    plex_client,
) -> dict:
    """Download a single C1 cloud theme and stage it as a
    local_files row.

    `target` is one entry from `identify_c1_rows`'s return list.
    `themes_dir` is `settings.themes_dir` (absolute path).
    `plex_client` is a live PlexClient.

    Flow:
      1. GET /library/metadata/{rk}/file?url=<entry_uri> WITHOUT
         Range header. The probe used Range as a safety guard;
         production needs the full file.
      2. Buffer the body (bounded ≤~9MB per the v1.18.85 probe)
         and hash it to compute file_sha256. Full-buffering is
         deliberate — streaming would add complexity for no benefit
         at these sizes (v1.19.80 audit).
      3. Write to themes_dir/<section_themes_subdir>/<Title (Year)>/
         theme.mp3.
      4. INSERT local_files with full v1.19.x writer contract
         (source_kind='plex_cloud', last_place_attempt_reason=
         'backup_only', etc).

    Returns: `{ok, bytes_written, file_path, sha1, sha256,
              error?}`. Never raises — failures land in the
    return dict so the caller can keep iterating.

    Idempotent on retry: ON CONFLICT updates the existing
    local_files row (matches v1.19.x writer pattern from
    worker.py:1761).
    """
    from urllib.parse import quote
    from .db import transaction

    rk = str(target["rating_key"])
    entry_uri = target["entry_uri"]
    sha1 = target["sha1"]
    media_type = target["media_type"]
    tmdb_id = target["guid_tmdb"]
    section_id = target["section_id"]
    title = target.get("title") or "untitled"
    year = str(target.get("year") or "") or None
    # Resolve the section's themes_subdir (mirrors worker.py
    # download path).
    try:
        section_slug = _section_themes_subdir(conn, section_id)
    except Exception as e:  # noqa: BLE001
        # v1.22.16: was `except RuntimeError` only. _section_themes_subdir runs a
        # conn.execute that can raise sqlite3.OperationalError ('database is
        # locked') under a concurrent sync — that escaped and aborted the ENTIRE
        # bulk-backup batch (every remaining C1 target un-backed-up), breaking
        # this function's "never raises — failures land in the return dict"
        # contract (audit #6). Catch ANY resolve error so the caller skips this
        # one row with a breadcrumb and keeps iterating.
        log.warning(
            "backup_cloud_theme: section-subdir resolve failed for "
            "section=%s: %r", section_id, e)
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": None,
            "sha1": sha1,
            "sha256": None,
            "error": str(e),
        }
    # v1.21.70: stage the backup in THIS edition's own canonical folder
    # ({edition-<key>}) so sibling editions don't collide on one theme.mp3,
    # and key the local_files row to the edition (was hardcoded '').
    edition_key = target.get("edition_key", "") or ""
    canonical_folder = canonical_theme_subdir(title, year, edition_key)
    rel_path = f"{section_slug}/{canonical_folder}/theme.mp3"
    abs_path = themes_dir / rel_path
    # Build the URL the same way set_active_theme_via_reupload
    # does (the v1.18.36 production path).
    file_url = (
        f"{plex_client._rk_path(rk, '/file')}"
        f"?url={quote(entry_uri, safe='')}"
    )
    try:
        r = plex_client._client.get(
            file_url, headers=plex_client._headers, timeout=120.0,
        )
    except Exception as e:  # noqa: BLE001
        log.warning(
            "backup_cloud_theme: rk=%s fetch transport error: %r",
            rk, e,
        )
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": None,
            "sha1": sha1,
            "sha256": None,
            "error": f"transport: {e!r}",
        }
    if not (200 <= r.status_code < 300):
        log.warning(
            "backup_cloud_theme: rk=%s fetch HTTP %d (body preview: %r)",
            rk, r.status_code, (r.text[:200] if r.text else ""),
        )
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": None,
            "sha1": sha1,
            "sha256": None,
            "error": f"http {r.status_code}",
        }
    # v1.19.80 (audit LOW): buffer the whole body. Themes are
    # bounded ≤~9MB (v1.18.85 probe) so full-buffering is fine;
    # streaming would be a premature abstraction here.
    audio_bytes = r.content or b""
    if not audio_bytes:
        log.warning(
            "backup_cloud_theme: rk=%s empty body (Content-Length=%s)",
            rk, r.headers.get("content-length"),
        )
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": None,
            "sha1": sha1,
            "sha256": None,
            "error": "empty body",
        }
    # v1.19.79 (Opus-4.8 audit MED): reject a truncated body. The
    # request sends NO Range header, so a 206 Partial Content — or any
    # 2xx whose length disagrees with Content-Length — means Plex
    # served less than the whole theme. Staging a partial file would
    # let a corrupt backup get PROMOTED TO ACTIVE later (silent
    # corruption). Reconcile against Content-Length when present.
    _clen = r.headers.get("content-length")
    if r.status_code == 206 or (
        isinstance(_clen, str) and _clen.isdigit()
        and len(audio_bytes) != int(_clen)
    ):
        log.warning(
            "backup_cloud_theme: rk=%s truncated body — HTTP %d, got "
            "%d bytes, Content-Length=%s; refusing to stage a partial "
            "backup", rk, r.status_code, len(audio_bytes), _clen,
        )
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": None,
            "sha1": sha1,
            "sha256": None,
            "error": "truncated body",
        }
    file_sha256 = hashlib.sha256(audio_bytes).hexdigest()
    file_size = len(audio_bytes)
    # v1.20.18: dedup guard. If the bytes Plex just served are
    # byte-identical to the row's existing canonical, there's nothing
    # to swap — skip the disk write + the REPLACE that would relabel
    # source_kind (e.g. themerrdb→plex_cloud) for no behavioral change.
    # This is the common case on a force capture where Plex is simply
    # echoing motif's OWN uploaded theme back as the selected entry.
    # Harmless no-op for the normal C1 walker path (those candidate
    # rows have no existing local_files row, so this returns nothing).
    # v1.22.3: scope the dedup to THIS edition. Pre-fix the query omitted
    # edition_key + used fetchone(), so on a multi-edition title it compared
    # the served bytes against an ARBITRARY sibling edition's canonical —
    # could falsely "skip identical" (or fail to) on the wrong row.
    existing = conn.execute(
        "SELECT file_sha256 FROM local_files "
        " WHERE media_type = ? AND tmdb_id = ? AND section_id = ? "
        "   AND edition_key = ?",
        (media_type, tmdb_id, section_id, edition_key),
    ).fetchone()
    if existing is not None:
        existing_sha = (
            existing["file_sha256"] if hasattr(existing, "keys")
            else existing[0]
        )
        if existing_sha and existing_sha == file_sha256:
            log.info(
                "backup_cloud_theme: rk=%s served bytes identical to "
                "existing canonical (sha256=%s) — no swap needed",
                rk, file_sha256[:12],
            )
            return {
                "ok": True,
                "bytes_written": 0,
                "file_path": None,
                "sha1": sha1,
                "sha256": file_sha256,
                "skipped_identical": True,
                "error": None,
            }
    # Stage to disk. Parent dirs created on demand — themes_dir
    # itself must already exist (caller's responsibility — the
    # settings validator guarantees it).
    # v1.22.3: write a temp sibling + os.replace, NOT write_bytes in-place.
    # In-place open('wb') TRUNCATES the existing theme.mp3, which needs WRITE
    # permission on THAT FILE — on the user's Unraid the themerrdb canonical is
    # owned via the share's perms and isn't writable by the container user, so
    # the overwrite EACCES'd (rk=417813/417795 PermissionError) even though the
    # worker's download — temp + rename — wrote the same path fine. os.replace
    # needs only DIRECTORY write, mirrors placement.py's atomic pattern, and
    # never leaves a partial file on crash (the v1.19.79 audit's concern).
    tmp_path = abs_path.with_suffix(abs_path.suffix + ".backup-tmp")
    try:
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_bytes(audio_bytes)
        os.replace(tmp_path, abs_path)
    except OSError as e:
        log.warning(
            "backup_cloud_theme: rk=%s disk write failed: %r",
            rk, e,
        )
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError as cleanup_err:
            log.debug("backup_cloud_theme: tmp cleanup failed at %s: %s",
                      tmp_path, cleanup_err)
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": str(abs_path),
            "sha1": sha1,
            "sha256": file_sha256,
            "error": f"disk: {e!r}",
        }
    # v1.19.46: ensure a themes row exists before the local_files
    # INSERT. local_files has FK (media_type, tmdb_id) REFERENCES
    # themes (media_type, tmdb_id) — for cloud-backup rows where
    # ThemerrDB doesn't track the title (the anime cohort + many
    # TV shows), no themes row exists, and the INSERT fails with
    # `IntegrityError: FOREIGN KEY constraint failed`.
    #
    # the user's 2026-05-27 production repro: clicked BACKUP THIS
    # THEME on a TV row → walker found 1 C1 target → INSERT into
    # local_files failed with FK constraint → worker logged
    # "CLOUD THEMES BACKUP failed: IntegrityError" → op_progress
    # marked failed. Pre-fix the v1.19.42 writer assumed the
    # themes row existed; this is wrong for the very cohort
    # cloud-backup exists to serve.
    #
    # Fix mirrors api.py:10808 (the upload-theme precedent for
    # orphan rows): INSERT OR IGNORE into themes with
    # `upstream_source='plex_orphan'` (the canonical value for
    # "Plex tracks it, TDB doesn't"). Then UPDATE plex_items.
    # theme_id so the library JOIN picks up the new linkage
    # immediately (otherwise the row's theme_id stays NULL until
    # the next plex_enum's resolve_theme_ids walk).
    now = now_iso()
    # v1.19.79 (Opus-4.8 audit MED): wrap the 3 writer statements
    # (themes INSERT + plex_items UPDATE + local_files INSERT) in a
    # single transaction. get_conn is autocommit, so pre-fix each
    # statement committed independently — a crash between them could
    # leave a plex_orphan themes row + stamped plex_items.theme_id
    # with NO local_files row (a linked-but-empty theme). Atomic now.
    try:
        with transaction(conn):
            # v1.21.33: mirror of _resolve_or_mint_tmdb_id's mint block — keep
            # the plex_orphan shape in sync across both sites. On the force
            # path the helper has already pre-minted, so this SELECT finds the
            # row and the INSERT below is a no-op; this block still owns the
            # mint for non-force callers (real-tmdb rows TDB doesn't track).
            theme_row = conn.execute(
                "SELECT id FROM themes WHERE media_type = ? AND tmdb_id = ?",
                (media_type, tmdb_id),
            ).fetchone()
            if theme_row is None:
                # v1.22.52: stamp guid_imdb on the minted orphan (mirror of the
                # _resolve_or_mint_tmdb_id mint) so it stays resolvable later.
                _imdb_row = conn.execute(
                    "SELECT guid_imdb FROM plex_items WHERE rating_key = ?",
                    (rk,),
                ).fetchone()
                _mint_imdb = _imdb_row["guid_imdb"] if _imdb_row else None
                cur = conn.execute(
                    """INSERT INTO themes
                         (media_type, tmdb_id, imdb_id, title, year,
                          upstream_source, last_seen_sync_at,
                          first_seen_sync_at)
                       VALUES (?, ?, ?, ?, ?, 'plex_orphan', ?, ?)""",
                    (media_type, tmdb_id, _mint_imdb, title, year, now, now),
                )
                theme_id_pk = cur.lastrowid
                log.info(
                    "backup_cloud_theme: created plex_orphan themes row "
                    "(id=%s) for %s/%s — TDB doesn't track this title",
                    theme_id_pk, media_type, tmdb_id,
                )
                # Stamp plex_items.theme_id so the library JOIN sees the
                # new linkage. Without this the row's theme_id stays NULL
                # until the next plex_enum.resolve_theme_ids — meaning the
                # local_files row written below would exist but the row
                # wouldn't surface as B in the library until then.
                conn.execute(
                    "UPDATE plex_items SET theme_id = ? "
                    "WHERE rating_key = ? AND theme_id IS NULL",
                    (theme_id_pk, rk),
                )
            # INSERT local_files row with the v1.19.x writer contract.
            # Mirrors worker.py:1754 (downloader writer) shape exactly —
            # any column drift between writers is a class-9 contract-drift
            # bug waiting to happen.
            conn.execute(
                """
                INSERT INTO local_files
                    (media_type, tmdb_id, section_id, edition_key, file_path,
                     file_sha256, file_size, downloaded_at,
                     source_video_id, provenance, source_kind,
                     last_place_attempt_reason, last_place_attempt_at,
                     mismatch_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'auto', 'plex_cloud',
                        'backup_only', ?, NULL)
                ON CONFLICT(media_type, tmdb_id, section_id, edition_key) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_sha256 = excluded.file_sha256,
                    file_size = excluded.file_size,
                    downloaded_at = excluded.downloaded_at,
                    source_video_id = excluded.source_video_id,
                    provenance = excluded.provenance,
                    source_kind = excluded.source_kind,
                    last_place_attempt_reason = excluded.last_place_attempt_reason,
                    last_place_attempt_at = excluded.last_place_attempt_at,
                    mismatch_state = excluded.mismatch_state
                """,
                (media_type, tmdb_id, section_id, edition_key, rel_path,
                 file_sha256, file_size, now, sha1, now),
            )
    except Exception as e:
        # v0.50.89 (audit HIGH): this transaction writes DB rows to describe
        # a file that was ALREADY swapped onto disk above (os.replace already
        # ran) — pre-fix an exception here (e.g. a "database is locked"
        # after transaction()'s own retry ladder is exhausted) propagated
        # uncaught, violating this function's documented "Never raises"
        # contract and aborting the caller's ENTIRE batch, abandoning every
        # remaining target. Catch + return an error dict like every other
        # failure branch in this function; the row's on-disk/DB mismatch
        # self-heals on the next run (the local_files INSERT is an
        # idempotent upsert, and abs_path already holds the new bytes).
        log.warning(
            "backup_cloud_theme: rk=%s DB write failed after disk swap "
            "(file at %s now describes NEW content the local_files row "
            "doesn't yet record — will self-heal next run): %r",
            rk, abs_path, e,
        )
        return {
            "ok": False,
            "bytes_written": 0,
            "file_path": str(abs_path),
            "sha1": sha1,
            "sha256": file_sha256,
            "error": f"db: {e!r}",
        }
    log.info(
        "backup_cloud_theme: rk=%s mt=%s tmdb=%s section=%s "
        "wrote %d bytes (sha1=%s, sha256=%s) at %s",
        rk, media_type, tmdb_id, section_id,
        file_size, sha1, file_sha256[:12], rel_path,
    )
    return {
        "ok": True,
        "bytes_written": file_size,
        "file_path": rel_path,
        "sha1": sha1,
        "sha256": file_sha256,
        "error": None,
    }
