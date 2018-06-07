import unittest
from cuuats.datamodel.fields import BaseField, OIDField, GeometryField, \
    StringField, NumericField, MethodField, WeightsField


class TestFields(unittest.TestCase):

    def setUp(self):
        class MyFeatureClass(object):
            base_field = BaseField(
                'Base Field',
                choices=[0, 1, 2],
                required=True)
            oid_field = OIDField('OBJECTID')
            geometry_field = GeometryField('SHAPE')
            string_field = StringField('String Field', required=True)
            numeric_field = NumericField(
                'Numeric Field', min=0, max=10, db_scale=3)
            double_method = MethodField(
                'Numeric Field Doubled', method_name='_double')
            weights_field = WeightsField('Weights Field', weights={
                'numeric_field': 0.25,
                'double_method': 0.75
            }, default=0)

            def __init__(self):
                self.values = {}

            def _double(self, field_name):
                if self.numeric_field is None:
                    return None
                return self.numeric_field * 2

            def get_field(self, field_name):
                return self.__class__.__dict__[field_name]

            def check_condition(self, condition):
                return True

        self.inst_a = MyFeatureClass()
        self.inst_b = MyFeatureClass()

    def test_base_field_get_set(self):
        self.assertTrue(
            isinstance(self.inst_a.get_field('base_field'), BaseField),
            'base field is not an instance of BaseField')

        self.assertEqual(self.inst_a.base_field, None)

        self.inst_a.base_field = 100
        self.assertEqual(self.inst_a.base_field, 100)
        self.assertNotEqual(self.inst_a.base_field, self.inst_b.base_field)

        self.inst_b.base_field = 200
        self.assertEqual(self.inst_a.base_field, 100)
        self.assertEqual(self.inst_b.base_field, 200)

    def test_base_field_validation_required(self):
        field = self.inst_a.get_field('base_field')
        self.assertTrue('Base Field is missing' in field.validate(None),
                        'validation message for required field is missing')
        self.assertEqual(len(field.validate(1)), 0)

    def test_base_field_validation_choices(self):
        field = self.inst_a.get_field('base_field')
        self.assertTrue('Base Field is invalid' in field.validate(10),
                        'no validation message for coded value domain')
        self.assertEqual(len(field.validate(2)), 0)

    def test_string_field_validation(self):
        field = self.inst_a.get_field('string_field')
        self.assertTrue('String Field is missing' in field.validate(''),
                        'no validation message for empty string')

    def test_numeric_field_validation(self):
        field = self.inst_a.get_field('numeric_field')
        self.assertTrue('Numeric Field out of range' in field.validate(25),
                        'no validation message for value above field max')
        self.assertTrue('Numeric Field out of range' in field.validate(-1),
                        'no validation message for value below field min')
        self.assertEqual(len(field.validate(0)), 0)
        self.assertEqual(len(field.validate(5)), 0)
        self.assertEqual(len(field.validate(10)), 0)

    def test_numeric_field_has_changed(self):
        field = self.inst_a.get_field('numeric_field')
        self.assertFalse(field.has_changed(10, 10))
        self.assertTrue(field.has_changed(10, 15))
        self.assertFalse(field.has_changed(1.002, 1.0022))
        self.assertTrue(field.has_changed(1.002, 1.0026))

    def test_method_field(self):
        self.inst_a.numeric_field = 2
        self.inst_b.numeric_field = 3
        self.assertEqual(self.inst_a.double_method, 4)
        self.assertEqual(self.inst_b.double_method, 6)

    def test_weights_field(self):
        self.assertEqual(self.inst_a.weights_field, 0)
        self.inst_a.numeric_field = 2
        self.assertEqual(self.inst_a.weights_field, 3.5)
