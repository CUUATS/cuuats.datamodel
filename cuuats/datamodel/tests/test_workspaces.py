import arcpy
import unittest
from cuuats.datamodel.tests.base import WorkspaceFixture


def setUpModule():
    WorkspaceFixture.setUpModule()


def tearDownModule():
    WorkspaceFixture.tearDownModule()


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
