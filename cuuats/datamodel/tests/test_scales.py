import unittest
from cuuats.datamodel.scales import BreaksScale, DictScale


class TestBreaksScale(unittest.TestCase):

    def setUp(self):
        self.breaks_right = BreaksScale([5, 10, 15, 20], [1, 2, 3, 4, 5])
        self.breaks_left = BreaksScale([5, 10, 15, 20], [1, 2, 3, 4, 5], False)

    def test_init(self):
        with self.assertRaises(ValueError):
            BreaksScale([5, 20, 15], [4, 3, 2, 1])

        with self.assertRaises(IndexError):
            BreaksScale([5, 10, 15], [3, 2, 1])

    def test_score_right(self):
        self.assertEqual(self.breaks_right.score(-10), 1)
        self.assertEqual(self.breaks_right.score(5), 1)
        self.assertEqual(self.breaks_right.score(6), 2)
        self.assertEqual(self.breaks_right.score(20), 4)
        self.assertEqual(self.breaks_right.score(100), 5)

    def test_score_left(self):
        self.assertEqual(self.breaks_left.score(-10), 1)
        self.assertEqual(self.breaks_left.score(5), 2)
        self.assertEqual(self.breaks_left.score(6), 2)
        self.assertEqual(self.breaks_left.score(19), 4)
        self.assertEqual(self.breaks_left.score(20), 5)
        self.assertEqual(self.breaks_left.score(100), 5)


class TestDictScale(unittest.TestCase):

    def setUp(self):
        self.scale = DictScale({
            'one': 1,
            'two': 2,
            'three': 3,
        }, 0)

    def test_score(self):
        self.assertEqual(self.scale.score('one'), 1)
        self.assertEqual(self.scale.score('three'), 3)
        self.assertEqual(self.scale.score('notakey'), 0)
