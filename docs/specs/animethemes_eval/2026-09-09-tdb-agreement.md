# Does AnimeThemes' OP1 agree with a human's pick? — 2026-09-09

Question (the operator's): are the catalogue entries actual show themes, or
episode/season variants? Method: 50 random anime rows the July snapshot
links to ThemerrDB (a contributor chose the YouTube video by hand); resolve
each through the product resolver; compare the AnimeThemes song title of
OP1 (and every OP/ED) with ThemerrDB's video title. Scratch script
`agree/measure.py` (session scratchpad); seed 4242.

| measure (n = 50; 47 resolved, 48 titles fetched) | rows |
|---|---|
| OP1 song title appears verbatim in ThemerrDB's video title | 17 |
| any OP's song title appears | 19 |
| any theme's (OP or ED) song title appears | 20 |
| ThemerrDB's video is titled as an OPENING ("opening", "OP", "intro") | 30 |
| titled as an ending | 0 |
| neither | 18 |

Reading the 25 "disagreements" by eye:
- ~10 are the SAME song under a name the string match can't see — the
  video is titled just "Opening" / "OP 1" / "ノンクレジットOP", or the song
  name is romanised / translated ("輪舞-revolution" = "Rinbu - revolution",
  "Deer Colored Days" = "Shika-iro Days", "息を吸う…" = "Iki wo Suu…", the
  song "01" fell under the 3-char floor).
- ~5 are a LATER season's opening (KONOSUBA "TOMORROW" = S2 OP vs OP1
  "fantastic dreamer"; Blue Exorcist S3; TSUKIMICHI S2; likely JJK).
- ~5 are an instrumental main theme / score cue (Detective Conan, Tomodachi
  Game, Armed Girl's Machiavellism, Yu-Gi-Oh!, Kengan Ashura).
- 1 is the ending / insert song (Cyberpunk: Edgerunners "I Really Want to
  Stay at Your House" vs OP "This Fffire").
- The Legend of Korra is not anime (filed in the anime section).

Conclusion: every AnimeThemes OP1 IS the show's first opening song; the
catalogue never served a non-theme. Where a human picked differently it was
a PREFERENCE among the show's own themes — a later season's OP, the ED, or
the instrumental main theme — not a wrong show. So: the picker must show the
SONG + ARTIST (v0.51.318) so the operator can recognise and choose; for a
bulk default (tag 4), season-1 OP1 agrees with the human pick in roughly
three of four rows and the remainder are legitimate alternatives, not
errors. Multiple "versions" of one theme (Bleach ED3 × 13) are the same song
across episode ranges — collapsed to one row with a versions pill in .318.
