import unittest
from cuuats.datamodel.domains import CodedValue, D


class TestCodedValue(unittest.TestCase):

    def test_coded_value_description(self):
        cv = CodedValue(3, 'Bar')
        self.assertEqual(cv.description, 'Bar')

    def test_int_coded_value(self):
        cv = CodedValue(10, 'Integer')
        self.assertTrue(isinstance(cv, CodedValue))
        self.assertTrue(isinstance(cv, int))

    def test_long_coded_value(self):
        cv = CodedValue(10L, 'Long Integer')
        self.assertTrue(isinstance(cv, CodedValue))
        self.assertTrue(isinstance(cv, long))

    def test_float_coded_value(self):
        cv = CodedValue(10.2, 'Floating Point')
        self.assertTrue(isinstance(cv, CodedValue))
        self.assertTrue(isinstance(cv, float))

    def test_str_coded_value(self):
        cv = CodedValue('string', 'String')
        self.assertTrue(isinstance(cv, CodedValue))
        self.assertTrue(isinstance(cv, str))

    def test_equality(self):
        self.assertEqual(CodedValue(10, 'Integer'), 10)
        self.assertNotEqual(CodedValue(10, 'Integer'), 'Integer')


class TestDescription(unittest.TestCase):

    def test_init(self):
        with self.assertRaises(TypeError):
            D(25)

        with self.assertRaises(TypeError):
            D(None)

    def test_self_equality(self):
        self.assertEqual(D('Test'), D('Test'))
        self.assertNotEqual(D('Test 1'), D('Test 2'))

    def test_coded_value_equality(self):
        self.assertEqual(D('Test'), CodedValue(3, 'Test'))
        self.assertEqual(CodedValue(3, 'Test'), D('Test'))
        self.assertNotEqual(D('Test 1'), CodedValue(3, 'Test 2'))
        self.assertNotEqual(CodedValue(3, 'Test 2'), D('Test 1'))

    def test_coded_value_inequality(self):
        self.assertFalse(D('Test') != CodedValue(3, 'Test'))
        self.assertFalse(CodedValue(3, 'Test') != D('Test'))

    def test_other_equality(self):
        self.assertNotEqual(D('Test'), 'Test')
