import arcpy
import gc
import os
import shutil
import tempfile
import unittest
from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned
from cuuats.datamodel.workspaces import Workspace
from cuuats.datamodel.fields import BaseField, OIDField, GeometryField, \
    StringField, NumericField, ScaleField, MethodField, WeightsField
from cuuats.datamodel.manytomany import ManyToManyField
from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.scales import BreaksScale, DictScale
from cuuats.datamodel.domains import CodedValue, D
from cuuats.datamodel.workspaces import WorkspaceManager


def setUpModule():
    WorkspaceFixture.setUpModule()


def tearDownModule():
    WorkspaceFixture.tearDownModule()


def hasLicense(*licenses):
    """Is the required license in use?"""
    return arcpy.ProductInfo() in licenses


class WorkspaceFixture(object):

    GDB_NAME = 'test.gdb'
    GDB_BACKUP_NAME = 'test_backup.gdb'
    DOMAIN_NAME = 'YesOrNo'
    DOMAIN_DESCRIPTION = 'Domain for yes or no responses'
    DOMAIN_FIELD_TYPE = 'SHORT'
    DOMAIN_VALUES = {
        50: 'No',
        100: 'Yes',
        101: 'N/A',
    }
    FEATURE_CLASS_NAME = 'Widget'
    FEATURE_CLASS_TYPE = 'POINT'
    FEATURE_CLASS_FIELDS = (
        ('widget_name', 'TEXT', 100, None),
        ('widget_description', 'TEXT', None, None),
        ('widget_number', 'LONG', None, None),
        ('widget_available', DOMAIN_FIELD_TYPE, None, DOMAIN_NAME),
        ('widget_price', 'FLOAT', None, None),
        ('widget_number_score', 'DOUBLE', None, None),
    )
    FEATURE_CLASS_DATA = (
        ('Widget A+ Awesome', None, 12345, 100, 10.50, None, (2.5, 3.0)),
        ('B-Widgety Widget', None, None, 50, None, None, (-2.0, 5.5)),
        ('My Widget C', 'Best widget', None, None, None, None, (0.0, 4.0)),
    )
    RELATED_CLASS_NAME = 'Warehouse'
    RELATED_CLASS_TYPE = 'POINT'
    RELATED_CLASS_FIELDS = (
        ('warehouse_name', 'TEXT', 100, None),
        ('warehouse_address', 'TEXT', None, None),
        ('warehouse_zipcode', 'LONG', 5, None),
        ('warehouse_open', DOMAIN_FIELD_TYPE, None, DOMAIN_NAME),
    )
    RELATED_CLASS_DATA = (
        ('Widget Distribution Center', '123 Main St', 99999, 100, (1.0, 3.3)),
        ('Widgets International', '88 Shipping Dr', 11111, 100, (-6.0, 4.2)),
    )

    @classmethod
    def createCodedValuesDomain(cls, name, description, field_type, values):
        """Add a coded values domain, and populate it with coded values."""

        arcpy.CreateDomain_management(
            cls.gdb_backup_path, name, description, field_type, 'CODED')

        for (code, desc) in values.items():
            arcpy.AddCodedValueToDomain_management(
                cls.gdb_backup_path, name, code, desc)

    @classmethod
    def createFeatureClass(cls, name, fc_type, fields):
        """Create a feature class, and add fields."""

        fc_path = os.path.join(cls.gdb_backup_path, name)
        arcpy.CreateFeatureclass_management(cls.gdb_backup_path, name, fc_type)

        # Add some fields to the feature class.
        for (field_name, field_type, field_precision, field_domain) in fields:
            arcpy.AddField_management(
                fc_path, field_name, field_type, field_precision,
                field_domain=field_domain)

        return fc_path

    @classmethod
    def populateFeatureClass(cls, fc_path, fields, data, shape='SHAPE@XY'):
        """Populate a feature class with data."""

        field_names = [f[0] for f in fields] + [shape]
        with arcpy.da.InsertCursor(fc_path, field_names) as cursor:
            for row in data:
                assert len(row) == len(field_names), \
                    'Row length does not match field length'
                cursor.insertRow(row)

    @classmethod
    def setUpModule(cls):
        # Create a new file geodatabase.
        cls.workspace_dir = tempfile.mkdtemp()
        cls.gdb_backup_path = os.path.join(
            cls.workspace_dir, cls.GDB_BACKUP_NAME)
        arcpy.CreateFileGDB_management(cls.workspace_dir, cls.GDB_BACKUP_NAME)

        # Add YesOrNo domain.
        cls.createCodedValuesDomain(cls.DOMAIN_NAME, cls.DOMAIN_DESCRIPTION,
                                    cls.DOMAIN_FIELD_TYPE, cls.DOMAIN_VALUES)

        # Create the feature classes.
        fc_path = cls.createFeatureClass(
            cls.FEATURE_CLASS_NAME, cls.FEATURE_CLASS_TYPE,
            cls.FEATURE_CLASS_FIELDS)

        rc_path = cls.createFeatureClass(
            cls.RELATED_CLASS_NAME, cls.RELATED_CLASS_TYPE,
            cls.RELATED_CLASS_FIELDS)

        # Populate the feature classes with data.
        cls.populateFeatureClass(
            fc_path, cls.FEATURE_CLASS_FIELDS, cls.FEATURE_CLASS_DATA)

        cls.populateFeatureClass(
            rc_path, cls.RELATED_CLASS_FIELDS, cls.RELATED_CLASS_DATA)

    @classmethod
    def tearDownModule(cls):
        shutil.rmtree(cls.workspace_dir)

    def setUp(self):
        # Restore the geodatabase from backup.
        self.gdb_path = os.path.join(self.workspace_dir, self.GDB_NAME)
        shutil.copytree(self.gdb_backup_path, self.gdb_path)

        # Create a workspace from the GDB.
        self.fc_path = os.path.join(self.gdb_path, self.FEATURE_CLASS_NAME)
        self.rc_path = os.path.join(self.gdb_path, self.RELATED_CLASS_NAME)
        self.workspace = Workspace(self.gdb_path)

        # Create a feature matching the workspace.
        class Widget(BaseFeature):
            """
            Test feature class.
            """

            OBJECTID = OIDField('OID')
            widget_name = StringField('Widget Name', required=True)
            widget_description = StringField('Widget Description')
            widget_number = NumericField('Widget Number', required=True)
            widget_available = NumericField('Is Widget Available?',
                                            required=True)
            widget_price = NumericField('Widget Price',
                                        required_if='self.is_available')
            widget_number_score = ScaleField(
                'Widget Number Score',
                scale=BreaksScale([100, 500, 1000], [1, 2, 3, 4]),
                value_field='widget_number',
                storage={'field_type': 'DOUBLE'}
            )
            Shape = GeometryField('Shape')

            @property
            def is_available(self):
                return self.widget_available == D('Yes')

        self.cls = Widget

    def tearDown(self):
        del self.cls
        del self.workspace
        WorkspaceManager().clear()
        gc.collect()
        shutil.rmtree(self.gdb_path)


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


class TestWorkspace(WorkspaceFixture, unittest.TestCase):

    def test_get_attachment_info(self):
        no_attach = self.workspace.get_attachment_info(self.FEATURE_CLASS_NAME)
        self.assertEqual(no_attach, None)

        arcpy.EnableAttachments_management(self.fc_path)
        attach = self.workspace.get_attachment_info(self.FEATURE_CLASS_NAME)
        self.assertEqual(attach.origin, self.FEATURE_CLASS_NAME)

    def test_get_layer_fields(self):
        layer_fields = self.workspace.get_layer_fields(self.FEATURE_CLASS_NAME)
        for field_def in self.FEATURE_CLASS_FIELDS:
            field_name = field_def[0]
            self.assertTrue(field_name in layer_fields,
                            'missing layer field: %s' % (field_name,))

    def test_count_rows(self):
        count = self.workspace.count_rows(
            self.FEATURE_CLASS_NAME, 'widget_number = 12345')
        self.assertEqual(count, 1)

    def iter_rows(self):
        rows = list(self.workspace.iter_rows(
            self.FEATURE_CLASS_NAME,
            ['widget_name', 'widget_available'],
            where_clause='widget_number = 12345'))

        self.assertEqual(
            len(rows), 1, 'where clause does not filter results')

        self.asserEqual(
            ['Widget A+ Awesome', 100], rows[0], 'incorrect row values')

    def test_update_row(self):
        field_names = [f[0] for f in self.FEATURE_CLASS_FIELDS]

        with self.workspace.edit():
            for (row, cursor) in self.workspace.iter_rows(
                    self.FEATURE_CLASS_NAME, field_names, update=True):
                row[0] = row[0].replace('Widget', 'Foo')
                self.workspace.update_row(cursor, row)

        with arcpy.da.SearchCursor(self.fc_path, ['widget_name']) as cursor:
            for feature in cursor:
                widget_name = feature[0]
                self.assertTrue('Widget' not in widget_name)
                self.assertTrue('Foo' in widget_name)

    def test_insert_row(self):
        field_names = [f[0] for f in self.FEATURE_CLASS_FIELDS]
        values = ('DWIDGET', 'D-Widget', None, None, None, None)
        self.workspace.insert_row(self.FEATURE_CLASS_NAME, field_names, values)

        with arcpy.da.SearchCursor(self.fc_path, ['widget_name']) as cursor:
            widget_names = [row[0] for row in cursor]
        self.assertTrue('DWIDGET' in widget_names)

    def test_domains(self):
        self.assertEqual(len(self.workspace.domains), 1,
                         'incorrect number of domains in workspace')
        self.assertTrue(self.DOMAIN_NAME in self.workspace.domains,
                        'domain is missing from the workspace domains')

    def test_get_domain(self):
        with self.assertRaises(NameError):
            self.workspace.get_domain('NotADomain')

        with self.assertRaises(TypeError):
            self.workspace.get_domain(self.DOMAIN_NAME, 'Range')

        domain = self.workspace.get_domain(self.DOMAIN_NAME, 'CodedValue')
        self.assertTrue(isinstance(domain, arcpy.da.Domain))

    def test_get_coded_value(self):
        self.assertEqual(
            self.workspace.get_coded_value(self.DOMAIN_NAME, 'No'), 50,
            'incorrect domain code')

        with self.assertRaises(ValueError):
            self.workspace.get_coded_value(self.DOMAIN_NAME, 'NotADescription')

    def test_add_field(self):
        num_fields = len(self.workspace.get_layer_fields(
            self.FEATURE_CLASS_NAME))

        with self.assertRaises(KeyError):
            self.workspace.add_field(
                self.FEATURE_CLASS_NAME,
                'widget_color',
                {})

        self.workspace.add_field(
            self.FEATURE_CLASS_NAME,
            'widget_color',
            {'field_type': 'TEXT', 'field_length': 100})

        fields = self.workspace.get_layer_fields(self.FEATURE_CLASS_NAME)
        self.assertEqual(len(fields), num_fields + 1,
                         'Widget color field not added')
        self.assertTrue('widget_color' in fields.keys())


class TestRegisterFeature(WorkspaceFixture, unittest.TestCase):

    def test_register(self):
        with self.assertRaises(AttributeError):
            self.cls([])

        with self.assertRaises(AttributeError):
            self.cls.iter()

        self.cls.register(self.fc_path)
        self.assertEqual(self.cls.workspace.path, self.workspace.path)
        for (field_name, field) in self.cls.fields.items():
            self.assertEqual(field.name, field_name,
                             'field name is not set correctly')
        self.assertEqual(
            len(self.cls.__dict__['widget_available'].choices), 3,
            'choices from domain is not assigned correctly')


class TestFeature(WorkspaceFixture, unittest.TestCase):

    def setUp(self):
        super(TestFeature, self).setUp()
        self.cls.register(self.fc_path)
        self.instance = self.cls(
            OBJECTID=1, widget_name='My Widget', widget_number=300,
            widget_available=50, Shape=None)

    def tearDown(self):
        del self.instance
        super(TestFeature, self).tearDown()

    def test_get_fields(self):
        field_names = ['OBJECTID', 'widget_name', 'widget_description',
                       'widget_number', 'widget_available', 'widget_price',
                       'widget_number_score', 'Shape']
        self.assertEqual(self.cls.fields.keys(), field_names)

        del self.cls.widget_name
        self.cls.widget_name = StringField('Widget Name', required=True)
        self.cls._fields = None
        self.assertEqual(
            self.cls.fields.keys()[-1], 'widget_name',
            'fields are not ordered based on creation_index')

        del self.cls.widget_name
        self.cls.widget_name = StringField(
            'Widget Name', required=True, order=-1)
        self.cls._fields = None
        self.assertEqual(
            self.cls.fields.keys()[0], 'widget_name',
            'fields are not ordered base on order argument')

    def test_save_update(self):
        feature = self.cls.objects.get(OBJECTID=1)
        feature.widget_name = 'Some Widget'
        result = feature.save()

        self.assertTrue(result)
        self.assertEqual(self.cls.objects.first().widget_name, 'Some Widget')

    def test_save_insert(self):
        feature_count = self.cls.objects.count()
        feature = self.cls(widget_name='Newest Widget')
        feature.save()

        self.assertEqual(self.cls.objects.count(), feature_count + 1)
        self.assertEqual(self.cls.objects.last().widget_name, 'Newest Widget')

    def test_save_no_change(self):
        # Update all features to set calculated field values.
        for feature in self.cls.objects.all():
            feature.save()

        # Try updating features without changing anything.
        update_count = 0
        for feature in self.cls.objects.all():
            update_count += int(feature.save())
        self.assertEqual(update_count, 0, 'Features updated unnecessarily')

        # Change a value, and check the update count.
        update_count = 0
        for feature in self.cls.objects.all():
            feature.widget_number = 500
            update_count += int(feature.save())
        self.assertEqual(update_count, 3, 'Features not updated after change')

    def test_get_field(self):
        self.assertEqual(self.instance.fields.get('NotAField'), None)
        self.assertTrue(
            isinstance(self.instance.fields.get('widget_name'), StringField))

    def test_set_by_description(self):
        self.instance.widget_available = D('Yes')
        self.assertEqual(self.instance.widget_available, 100)

    def test_get_description_for(self):
        self.assertEqual(
            self.instance.widget_available, D('No'))

    def test_field_value_changed(self):
        self.assertEqual(self.instance.widget_number_score, 2)
        self.instance.widget_number = 800
        self.assertEqual(
            self.instance.widget_number_score, 3,
            'Calculated field not updated')

    def test_sync_fields(self):
        self.assertTrue('widget_number_score' in
                        self.workspace.get_layer_fields(
                            self.FEATURE_CLASS_NAME))

        arcpy.DeleteField_management(self.fc_path, 'widget_number_score')

        self.assertTrue('widget_number_score' not in
                        self.workspace.get_layer_fields(
                            self.FEATURE_CLASS_NAME))

        self.cls.sync_fields()
        self.assertTrue('widget_number_score' in
                        self.workspace.get_layer_fields(
                            self.FEATURE_CLASS_NAME),
                        'Syncing fields did not create widget_number_score')

    def test_required_if(self):
        price_msg = 'Widget Price is missing'
        description_msg = 'Widget Description is missing'

        self.assertTrue(description_msg not in self.instance.validate(),
                        'Optional field produces missing validation message')

        self.assertTrue(price_msg not in self.instance.validate(),
                        'required_if validation message incorrectly generated')

        self.instance.widget_available = D('Yes')
        self.assertTrue(price_msg in self.instance.validate(),
                        'required_if validation message not generated')

        self.instance.widget_price = 20.00
        self.assertTrue(price_msg not in self.instance.validate(),
                        'required_if validation message incorrectly generated')

    def test_diff(self):
        feature = self.cls.objects.get(OBJECTID=1)
        feature.save()
        original_name = feature.widget_name
        feature.widget_name = 'Some Widget'
        self.assertEqual(
            feature.diff(), {'widget_name': (original_name, 'Some Widget')})

        feature.save()
        self.assertEqual(feature.diff(), {})


class TestQuerySet(WorkspaceFixture, unittest.TestCase):

        def setUp(self):
            super(TestQuerySet, self).setUp()
            self.cls.register(self.fc_path)

        def test_all(self):
            widget_names = [row[0] for row in self.FEATURE_CLASS_DATA]
            for feature in self.cls.objects.all():
                self.assertTrue(isinstance(feature, self.cls))
                self.assertTrue(feature.widget_name in widget_names)

        def test_get(self):
            feature = self.cls.objects.get(OBJECTID=2)
            self.assertEqual(feature.OBJECTID, 2)
            self.assertEqual(feature.widget_name, 'B-Widgety Widget')

            with self.assertRaises(ObjectDoesNotExist):
                self.cls.objects.get(OBJECTID=5)

            with self.assertRaises(MultipleObjectsReturned):
                self.cls.objects.get(widget_description=None)

        def test_get_save(self):
            inst_a = self.cls.objects.get(OBJECTID=1)
            inst_b = self.cls.objects.get(OBJECTID=2)

            self.assertEqual(inst_a.widget_number, 12345)
            self.assertEqual(inst_b.widget_number, None)

            inst_a.widget_number = 10
            inst_a.save()

            inst_b.widget_number = 20
            inst_b.save()

            del inst_a
            del inst_b

            widget_numbers = [r[0] for (r, c) in self.workspace.iter_rows(
                self.FEATURE_CLASS_NAME, ['widget_number'])]
            self.assertTrue(10 in widget_numbers)
            self.assertTrue(20 in widget_numbers)


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


@unittest.skipUnless(hasLicense('ArcInfo', 'ArcEditor'),
                     'required ArcGIS license is not in use')
class TestManyToManyField(WorkspaceFixture, unittest.TestCase):
    REL_NAME = 'Widget_Warehouse'
    ORIGIN_PK = 'OBJECTID'
    ORIGIN_FK = 'WidgetID'
    DESTINATION_PK = 'OBJECTID'
    DESTINATION_FK = 'WarehouseID'

    def setUp(self):
        super(TestManyToManyField, self).setUp()
        self.rel_path = os.path.join(self.gdb_path, self.REL_NAME)
        # Create the relationship class.
        arcpy.CreateRelationshipClass_management(
            origin_table=self.fc_path,
            destination_table=self.rc_path,
            out_relationship_class=self.rel_path,
            relationship_type='SIMPLE',
            forward_label='Warehouse',
            backward_label='Widget',
            message_direction='NONE',
            cardinality='MANY_TO_MANY',
            attributed='NONE',
            origin_primary_key=self.ORIGIN_PK,
            origin_foreign_key=self.ORIGIN_FK,
            destination_primary_key=self.DESTINATION_PK,
            destination_foreign_key=self.DESTINATION_FK)

        # Insert data into the relationship class
        self.rel_fields = ["RID", self.ORIGIN_FK, self.DESTINATION_FK]
        self.data = [(1, 1, 1),
                     (2, 1, 2),
                     (3, 2, 1),
                     (4, 2, 2),
                     (5, 3, 1)]
        with arcpy.da.InsertCursor(self.rel_path, self.rel_fields) as cursor:
            for row in self.data:
                cursor.insertRow(row)
        del cursor

    # Create a Warehouse Python class
        class Warehouse(BaseFeature):
            """
            Test feature class.
            """

            OBJECTID = OIDField('OID')
            warehouse_name = StringField('Warehouse Name', required=True)
            warehouse_address = StringField('Warehouse Address')
            warehouse_zipcode = NumericField('Warehouse Zipcode',
                                             required=True)
            warehouse_open = NumericField('Is Warehouse Open?',
                                          required=True)
            Shape = GeometryField('Shape')

        self.related_cls = self.cls
        self.cls = Warehouse
        self.cls.widgets = ManyToManyField("Widgets",
                                           related_class=self.related_cls,
                                           relationship_class=self.REL_NAME,
                                           foreign_key=self.DESTINATION_FK,
                                           related_foreign_key=self.ORIGIN_FK,
                                           primary_key=self.DESTINATION_PK,
                                           related_primary_key=self.ORIGIN_PK)
        self.cls.register(self.rc_path)
        self.related_cls.register(self.fc_path)

    def test_data_populated(self):
        with arcpy.da.SearchCursor(self.rel_path, self.rel_fields) as cursor:
            test_list = [row[0] for row in cursor]
        self.assertTrue(len(test_list) == 5)

        with arcpy.da.SearchCursor(self.rc_path, "warehouse_name") as cursor:
            test_list = [row[0] for row in cursor]
        self.assertEqual(test_list, ['Widget Distribution Center',
                                     'Widgets International'])

    def test_add_many_to_many_field(self):
        self.assertTrue(isinstance(self.cls.widgets, ManyToManyField))

    def test_get_data(self):
        warehouses = [i for i in self.cls.objects.all()]
        widgets = [i for i in self.related_cls.objects.all()]

        # Testing the len of the data
        self.assertEqual(
            len(warehouses),
            len(super(TestManyToManyField, self).RELATED_CLASS_DATA)
        )
        self.assertEqual(
            len(widgets),
            len(super(TestManyToManyField, self).FEATURE_CLASS_DATA)
        )

        # Test getting correct widgets from warehouse
        testID = 2
        warehouse = self.cls.objects.get(OBJECTID=testID)
        widgetsID = [wid.OBJECTID for wid in warehouse.widgets]
        testWidgets = [d[1] for d in self.data if d[2] == testID]
        self.assertEqual(widgetsID, testWidgets)

        # Test getting correct warehouses from widget
        widget = self.related_cls.objects.get(OBJECTID=testID)
        warehousesID = [ware.OBJECTID for ware in widget.warehouse_set]
        testWarehouses = [w[2] for w in self.data if w[1] == testID]
        self.assertEqual(warehousesID, testWarehouses)

    def test_prefetch_data(self):
        testID = 2
        # Test warehouses prefetch widgets
        warehouses = self.cls.objects.prefetch_related(
                     "widgets").get(OBJECTID=testID)
        widgetsID = [wid.OBJECTID for wid in warehouses.widgets]
        testWidgets = [d[1] for d in self.data if d[2] == testID]
        self.assertEqual(widgetsID, testWidgets)

        widgets = self.related_cls.objects.prefetch_related(
                  "warehouse_set").get(OBJECTID=testID)
        warehousesID = [ware.OBJECTID for ware in widgets.warehouse_set]
        testWarehouses = [w[2] for w in self.data if w[1] == testID]
        self.assertEqual(warehousesID, testWarehouses)

    def tearDown(self):
        del self.related_cls
        super(TestManyToManyField, self).tearDown()


if __name__ == '__main__':
    unittest.main()
