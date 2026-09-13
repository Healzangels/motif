// v0.51.338: which SRC letters a library page can filter on. A dual-export
// module shaped like quick-play.js: base.html loads it before app.js
// (window.motifSrcFilter) and tests/js/test_src_filter.js requires the same
// file, so the rule the page runs is the rule the tests pin.
//
// A letter stays in the SRC filter only while THIS page offers its chip:
//
//   no [data-src-filter] chip for the letter here   → dropped (SRC A / M on
//       /collections, where the template leaves those chips out)
//   its chip is data-tab-only for a different tab    → dropped (SRC AT off
//       /anime: the chip is rendered but hidden, so a kept AT would narrow
//       the table to 0 rows with nothing lit to click off)
//   otherwise                                        → kept
//
// loadLibrary() prunes libraryState.srcFilter through this before it saves
// or sends anything, and the SRC ALL button fills itself through it.
//
// Pure: no DOM, no fetch, no globals.
(function (root, factory) {
  if (typeof module === "object" && typeof module.exports === "object") {
    module.exports = factory();
  } else {
    root.motifSrcFilter = factory();
  }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  // letters: iterable of wire tokens; chips: [{letter, tabOnly}] read off the
  // page's [data-src-filter] buttons; tab: the library tab being shown.
  // Returns the kept letters in input order, without duplicates.
  function keepOfferedLetters(letters, chips, tab) {
    var offered = Object.create(null);
    var list = chips || [];
    for (var i = 0; i < list.length; i++) {
      var chip = list[i];
      if (!chip || !chip.letter) continue;
      if (chip.tabOnly && chip.tabOnly !== tab) continue;
      offered[chip.letter] = true;
    }
    var kept = [];
    var seen = Object.create(null);
    Array.from(letters || []).forEach(function (letter) {
      if (offered[letter] && !seen[letter]) {
        seen[letter] = true;
        kept.push(letter);
      }
    });
    return kept;
  }

  return {
    keepOfferedLetters: keepOfferedLetters,
  };
});
