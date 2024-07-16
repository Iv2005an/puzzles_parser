import unittest

from puzzles_parser.models import Puzzle


class TestPuzzle(unittest.TestCase):
    def setUp(self) -> None:
        self.puzzle = Puzzle(
            0, "title", 500, 10, 10, "manufacturer", "country", "path", "url"
        )

    def test_get_csv(self):
        self.assertEqual(
            self.puzzle.get_csv(";"),
            '"0";"title";"500";"10";"10";"manufacturer";"country";"path";"url"',
        )

    def test_get_sql(self):
        self.assertEqual(
            self.puzzle.get_sql("table"),
            "INSERT INTO table VALUES(0,'title',500,10,10,'manufacturer','country','path','url');",
        )


if __name__ == "__main__":
    unittest.main()
