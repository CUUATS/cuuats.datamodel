import arcpy
import os
import shutil
import tempfile
import unittest
from cuuats.datamodel.sources import DataSource
from cuuats.datamodel.fields import BaseField, OIDField, GeometryField, \
    StringField, NumericField, ScaleField
from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.scales import BreaksScale


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
            numeric_field = NumericField('Numeric Field', min=0, max=10)

            def __init__(self):
                self.values = {}

            def get_field(self, field_name):
                return self.__class__.__dict__[field_name]

            def _field_value_changed(self, field_name):
                pass

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


class SourceFeatureMixin(object):

    GDB_NAME = 'test.gdb'
    DOMAIN_NAME = 'YesOrNo'
    DOMAIN_DESCRIPTION = 'Domain for yes or no responses'
    DOMAIN_FIELD_TYPE = 'SHORT'
    DOMAIN_TYPE = 'CODED'
    DOMAIN_VALUES = {
        50: 'No',
        100: 'Yes',
        101: 'N/A',
    }
    FEATURE_CLASS_NAME = 'Widget'
    FEATURE_CLASS_TYPE = 'POINT'
    FEATURE_CLASS_FIELDS = (
        ('widget_name', 'TEXT', 100, None),
        ('widget_number', 'LONG', None, None),
        ('widget_available', DOMAIN_FIELD_TYPE, None, DOMAIN_NAME),
        ('widget_number_score', 'DOUBLE', None, None),
    )
    FEATURE_CLASS_DATA = (
        ('Widget A+ Awesome', 12345, 100, None, (2.5, 3.0)),
        ('B-Widgety Widget', None, 50, None, (-2.0, 5.5)),
        ('My Widget C', None, None, None, (0.0, 4.0)),
    )

    def setUp(self):
        # Create a new file geodatabase.
        self.workspace = tempfile.mkdtemp()
        self.gdb_path = os.path.join(self.workspace, self.GDB_NAME)
        arcpy.CreateFileGDB_management(self.workspace, self.GDB_NAME)

        # Add a coded values domain, and populate it with coded values.
        arcpy.CreateDomain_management(
            self.gdb_path, self.DOMAIN_NAME, self.DOMAIN_DESCRIPTION,
            self.DOMAIN_FIELD_TYPE, self.DOMAIN_TYPE)

        for (code, desc) in self.DOMAIN_VALUES.items():
            arcpy.AddCodedValueToDomain_management(
                self.gdb_path, self.DOMAIN_NAME, code, desc)

        # Create a feature class.
        self.fc_path = os.path.join(self.gdb_path, self.FEATURE_CLASS_NAME)
        arcpy.CreateFeatureclass_management(
            self.gdb_path, self.FEATURE_CLASS_NAME, self.FEATURE_CLASS_TYPE)

        # Add some fields to the feature class.
        for (field_name, field_type, field_precision, field_domain) in \
                self.FEATURE_CLASS_FIELDS:
            arcpy.AddField_management(
                self.fc_path, field_name, field_type, field_precision,
                field_domain=field_domain)

        # Populate the feature class with data.
        field_names = [f[0] for f in self.FEATURE_CLASS_FIELDS] + ['SHAPE@XY']
        with arcpy.da.InsertCursor(self.fc_path, field_names) as cursor:
            for row in self.FEATURE_CLASS_DATA:
                cursor.insertRow(row)

        # Create a data source from the GDB.
        self.source = DataSource(self.gdb_path)

        # Create a feature matching the data source.
        class Widget(BaseFeature):
            """
            Test feature class.
            """

            OBJECTID = OIDField('OID')
            widget_name = StringField('Widget Name', required=True)
            widget_number = NumericField('Widget Number', required=True)
            widget_available = NumericField('Is Widget Available?',
                                            required=True)
            widget_number_score = ScaleField(
                'Widget Number Score',
                scale=BreaksScale([100, 500, 1000], [1, 2, 3, 4]),
                value_field='widget_number',
            )
            Shape = GeometryField('Shape')

        self.cls = Widget

    def tearDown(self):
        if self.cls.source is not None:
            del self.cls.source
        del self.cls
        del self.source
        shutil.rmtree(self.workspace)


class TestDataSource(SourceFeatureMixin, unittest.TestCase):

    def test_get_attachment_info(self):
        no_attach = self.source.get_attachment_info(self.FEATURE_CLASS_NAME)
        self.assertEqual(no_attach, None)

        arcpy.EnableAttachments_management(self.fc_path)
        attach = self.source.get_attachment_info(self.FEATURE_CLASS_NAME)
        self.assertEqual(attach.origin, self.FEATURE_CLASS_NAME)

    def test_get_layer_fields(self):
        layer_fields = self.source.get_layer_fields(self.FEATURE_CLASS_NAME)
        for field_def in self.FEATURE_CLASS_FIELDS:
            field_name = field_def[0]
            self.assertTrue(field_name in layer_fields,
                            'missing layer field: %s' % (field_name,))

    def test_count_rows(self):
        count = self.source.count_rows(
            self.FEATURE_CLASS_NAME, 'widget_number = 12345')
        self.assertEqual(count, 1)

    def iter_rows(self):
        rows = list(self.source.iter_rows(
            self.FEATURE_CLASS_NAME,
            ['widget_name', 'widget_available'],
            where_clause='widget_number = 12345'))

        self.assertEqual(
            len(rows), 1, 'where clause does not filter results')

        self.asserEqual(
            ['Widget A+ Awesome', 100], rows[0], 'incorrect row values')

    def test_update_row(self):
        field_names = [f[0] for f in self.FEATURE_CLASS_FIELDS]
        for (row, cursor) in self.source.iter_rows(
                self.FEATURE_CLASS_NAME, field_names, update=True):
            row[0] = row[0].replace('Widget', 'Foo')
            self.source.update_row(cursor, row)

        with arcpy.da.SearchCursor(self.fc_path, ['widget_name']) as cursor:
            for feature in cursor:
                widget_name = feature[0]
                self.assertTrue('Widget' not in widget_name)
                self.assertTrue('Foo' in widget_name)

    def test_domains(self):
        self.assertEqual(len(self.source.domains), 1,
                         'incorrect number of domains in source')
        self.assertTrue(self.DOMAIN_NAME in self.source.domains,
                        'domain is missing from the source domains')

    def test_get_domain(self):
        with self.assertRaises(NameError):
            self.source.get_domain('NotADomain')

        with self.assertRaises(TypeError):
            self.source.get_domain(self.DOMAIN_NAME, 'Range')

        domain = self.source.get_domain(self.DOMAIN_NAME, 'CodedValue')
        self.assertTrue(isinstance(domain, arcpy.da.Domain))

    def test_get_coded_value(self):
        self.assertEqual(
            self.source.get_coded_value(self.DOMAIN_NAME, 'No'), 50,
            'incorrect domain code')

        with self.assertRaises(ValueError):
            self.source.get_coded_value(self.DOMAIN_NAME, 'NotADescription')


class TestRegisterFeature(SourceFeatureMixin, unittest.TestCase):

    def test_register(self):
        with self.assertRaises(AttributeError):
            self.cls([])

        with self.assertRaises(AttributeError):
            self.cls.iter()

        self.cls.register(self.source, self.FEATURE_CLASS_NAME)
        self.assertEqual(self.cls.source.path, self.source.path)
        for (field_name, field) in self.cls.get_fields().items():
            self.assertEqual(field.name, field_name,
                             'field name is not set correctly')
        self.assertEqual(
            len(self.cls.__dict__['widget_available'].choices), 3,
            'choices from domain is not assigned correctly')


class TestFeature(SourceFeatureMixin, unittest.TestCase):

    def setUp(self):
        super(TestFeature, self).setUp()
        self.cls.register(self.source, self.FEATURE_CLASS_NAME)
        self.instance = self.cls(
            OBJECTID=1, widget_name='My Widget', widget_number=300,
            widget_available=50, Shape=None)

    def test_get_fields(self):
        field_names = ['OBJECTID', 'widget_name', 'widget_number',
                       'widget_available', 'widget_number_score', 'Shape']
        self.assertEqual(self.cls.get_fields().keys(), field_names)

        del self.cls.widget_name
        self.cls.widget_name = StringField('Widget Name', required=True)
        self.assertEqual(
            self.cls.get_fields().keys()[-1], 'widget_name',
            'fields are not ordered based on creation_index')

        del self.cls.widget_name
        self.cls.widget_name = StringField(
            'Widget Name', required=True, order=-1)
        self.assertEqual(
            self.cls.get_fields().keys()[0], 'widget_name',
            'fields are not ordered base on order argument')

    def test_iter(self):
        widget_names = [row[0] for row in self.FEATURE_CLASS_DATA]
        for feature in self.cls.iter():
            self.assertTrue(isinstance(feature, self.cls))
            self.assertTrue(feature.cursor is not None)
            self.assertTrue(feature.widget_name in widget_names)

    def test_get_field(self):
        with self.assertRaises(KeyError):
            self.instance.get_field('NotAField')

        self.assertTrue(
            isinstance(self.instance.get_field('widget_name'), StringField))

    def test_set_by_description(self):
        self.instance.set_by_description('widget_available', 'Yes')
        self.assertEqual(self.instance.widget_available, 100)

    def test_get_description_for(self):
        self.assertEqual(
            self.instance.get_description_for('widget_available'), 'No')

    def test_field_value_changed(self):
        self.assertEqual(self.instance.widget_number_score, 2)
        self.instance.widget_number = 800
        self.assertEqual(
            self.instance.widget_number_score, 3,
            'Calculated field not updated')


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

if __name__ == '__main__':
    unittest.main()
