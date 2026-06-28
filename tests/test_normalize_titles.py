"""Regression tests for title normalization edge cases that affect orphan
promotion (sync._upsert_theme) and scanner classification (scanner._classify).

Run with:
    python -m unittest tests.test_normalize_titles

No pytest dependency; uses stdlib only.
"""
import unittest

from app.core.normalize import normalize_title, titles_equal, parse_folder_name


class TestTitleNormalization(unittest.TestCase):
    def test_leading_paren_stripped_for_match(self):
        # The reported regression: orphan with folder-derived title
        # "(500) Days of Summer" must equal upstream ThemerrDB record
        # "500 Days of Summer".
        self.assertTrue(
            titles_equal("(500) Days of Summer", "500 Days of Summer"),
            "leading paren around digits should not block matching",
        )

    def test_trailing_punctuation(self):
        self.assertTrue(titles_equal("Inception.", "Inception"))

    def test_ampersand_vs_and(self):
        self.assertTrue(titles_equal("Bonnie & Clyde", "Bonnie and Clyde"))

    def test_roman_numerals(self):
        self.assertTrue(titles_equal("Rocky II", "Rocky 2"))

    def test_versus(self):
        self.assertTrue(titles_equal("Alien vs. Predator", "Alien versus Predator"))
        self.assertTrue(titles_equal("Alien v Predator", "Alien versus Predator"))

    def test_year_pure_title_unchanged(self):
        # "2010" shouldn't be munged into anything else
        self.assertEqual(normalize_title("2010"), "2010")
        self.assertTrue(titles_equal("2010", "2010"))

    def test_case_insensitive(self):
        self.assertTrue(titles_equal("THE GODFATHER", "the godfather"))


class TestParseFolderName(unittest.TestCase):
    def test_basic(self):
        p = parse_folder_name("2010 (1984)")
        self.assertEqual(p.title, "2010")
        self.assertEqual(p.year, "1984")

    def test_leading_paren_title(self):
        p = parse_folder_name("(500) Days of Summer (2009)")
        self.assertEqual(p.title, "(500) Days of Summer")
        self.assertEqual(p.year, "2009")

    def test_no_year(self):
        p = parse_folder_name("Some Movie")
        self.assertEqual(p.title, "Some Movie")
        # ParsedFolder.year is "" when absent, NOT None — code that branches
        # on `is None` is buggy.
        self.assertEqual(p.year, "")
        self.assertFalse(p.has_year)

    def test_edition_tags_stripped(self):
        p = parse_folder_name("Blade Runner (1982) {edition-Final Cut}")
        self.assertEqual(p.title, "Blade Runner")
        self.assertEqual(p.year, "1982")


if __name__ == "__main__":
    unittest.main()
