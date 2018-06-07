import arcpy
import unittest
from cuuats.datamodel.tests.base import WorkspaceFixture
from cuuats.datamodel.fields import StringField
from cuuats.datamodel.domains import D


def setUpModule():
    WorkspaceFixture.setUpModule()


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
