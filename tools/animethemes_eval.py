"""AnimeThemes coverage harness (v0.51.314) — docs/specs/ANIMETHEMES_SPEC.md §4.

Runs the PRODUCT resolver (app.core.animethemes) over a motif.db's anime
rows and writes summary.md + results.json. Needs the network (bridge file +
API); not a pytest test. Invocation:

  PYTHONPATH=$PWD .venv/bin/python tools/animethemes_eval.py --db /path/motif.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from app.core import animethemes as at  # noqa: E402

BUCKETS = ("no_theme", "plex_served_unlinked", "tdb_linked", "user_set")
ROWS_SQL = """
SELECT pi.rating_key, pi.media_type, pi.title, pi.year, pi.guid_tvdb, pi.guid_tmdb,
       pi.has_theme, pi.theme_id, pi.section_id,
       (SELECT upstream_source FROM themes t WHERE t.id = pi.theme_id) AS theme_src
FROM plex_items pi JOIN plex_sections ps ON ps.section_id = pi.section_id
WHERE ps.is_anime = 1 AND pi.media_type = 'show' {section}
ORDER BY pi.title
"""


def bucket_for(row: dict) -> str:
    if not row["has_theme"]:
        return "no_theme"
    if row["theme_src"] in ("themoviedb", "imdb"):
        return "tdb_linked"
    if row["theme_src"] == "plex_orphan":
        return "user_set"
    return "plex_served_unlinked"


def load_rows(db: Path, section_id: str | None, limit: int | None) -> list[dict]:
    uri = f"file:{db}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        sql = ROWS_SQL.format(section="AND pi.section_id = ?" if section_id else "")
        rows = [dict(r) for r in conn.execute(sql, (section_id,) if section_id else ())]
    return rows[:limit] if limit else rows


def pct(a: int, n: int) -> str:
    return f"{a} ({100 * a // max(n, 1)}%)"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--section-id")
    ap.add_argument("--out", type=Path, default=REPO / "docs" / "specs" / "animethemes_eval" / date.today().isoformat())
    ap.add_argument("--cache-dir", type=Path, help="bridge cache dir (default: <out>/cache)")
    ap.add_argument("--no-name-search", action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args(argv)

    args.out.mkdir(parents=True, exist_ok=True)
    cache_dir = args.cache_dir or (args.out / "cache")
    rows = load_rows(args.db, args.section_id, args.limit)
    print(f"rows: {len(rows)} from {args.db}", flush=True)
    bridge = at.load_bridge(cache_dir)
    print(f"bridge: {bridge.entries} entries with an AniDB id", flush=True)

    t0 = time.time()
    with at.AnimeThemesClient() as client:
        at.prefetch(rows, bridge, client)
        print(f"prefetch done in {time.time() - t0:.0f}s, {client.requests} requests", flush=True)
        results = []
        for i, r in enumerate(rows, 1):
            b = bucket_for(r)
            res = at.resolve(r, bridge, client, name_search=not args.no_name_search)
            d = res.default
            results.append({
                "rating_key": r["rating_key"], "title": r["title"], "year": r["year"], "bucket": b,
                "bridged": bool(bridge.entries_for(r["guid_tvdb"], r["guid_tmdb"], r["media_type"])[1]),
                "via": res.via, "confidence": res.confidence, "reason": res.reason,
                "has_audio": res.has_audio, "has_op": res.has_op,
                "seasons": [{"season": s.season, "anidb": s.anidb, "name": s.info.name, "year": s.info.year,
                             "themes": [t.slug for t in s.themes if t.audio]} for s in res.seasons],
                "default": ({"name": d[0].info.name, "year": d[0].info.year, "theme": d[1].slug,
                             "song": d[1].song, "artists": list(d[1].artists),  # v0.51.318
                             "link": d[2].link, "size": d[2].size} if d else None),
            })
            if i % 100 == 0:
                print(f"  {i}/{len(rows)} resolved, {client.requests} requests", flush=True)
        print(f"resolved {len(rows)} rows in {time.time() - t0:.0f}s with {client.requests} API requests", flush=True)

    tot, bridged, audio, op, clean, glance, name = (Counter() for _ in range(7))
    misses: dict[str, list] = {b: [] for b in BUCKETS}
    default_bytes = 0
    for x in results:
        b = x["bucket"]; tot[b] += 1
        bridged[b] += x["bridged"]; audio[b] += x["has_audio"]; op[b] += x["has_op"]
        if x["confidence"] == "clean": clean[b] += 1
        elif x["confidence"] == "glance": glance[b] += 1
        elif x["confidence"] == "name": name[b] += 1
        if not x["has_audio"]:
            misses[b].append((x["title"], x["year"], x["reason"]))
        if b == "no_theme" and x["default"]:
            default_bytes += x["default"]["size"] or 0

    lines = [f"# AnimeThemes coverage — {date.today().isoformat()}", "",
             f"Input: `{args.db.name}` (anime shows: {len(rows)}). Bridge: {bridge.entries} entries with an AniDB id. "
             f"API requests: {client.requests}.", "",
             "| bucket | rows | bridged | AT any audio | AT has OP | clean | glance | name |",
             "|---|---|---|---|---|---|---|---|"]
    for b in BUCKETS + ("ALL",):
        if b == "ALL":
            n = sum(tot.values()); f = lambda c: sum(c.values())  # noqa: E731
            lines.append(f"| all | {n} | {pct(f(bridged), n)} | {pct(f(audio), n)} | {pct(f(op), n)} | {f(clean)} | {f(glance)} | {f(name)} |")
        else:
            n = tot[b]
            lines.append(f"| {b} | {n} | {pct(bridged[b], n)} | {pct(audio[b], n)} | {pct(op[b], n)} | {clean[b]} | {glance[b]} | {name[b]} |")
    lines += ["", f"Default-pick audio for the no-theme bucket: ~{default_bytes / 1e6:.0f} MB.", ""]
    for b in BUCKETS:
        lines += [f"## misses — {b} ({len(misses[b])})", ""]
        lines += [f"- {t} ({y}) — {why}" for t, y, why in misses[b][:40]]
        lines.append("")
    (args.out / "summary.md").write_text("\n".join(lines))
    (args.out / "results.json").write_text(json.dumps(results, indent=1))
    print("\n".join(lines[:12]))
    print(f"\nwrote {args.out / 'summary.md'} and results.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
