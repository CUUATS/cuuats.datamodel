import arcpy
import gc
import os
import shutil
import tempfile
from cuuats.datamodel.workspaces import Workspace
from cuuats.datamodel.fields import OIDField, GeometryField, \
    StringField, NumericField, ScaleField
from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.scales import BreaksScale
from cuuats.datamodel.domains import D
from cuuats.datamodel.workspaces import WorkspaceManager


def hasLicense(*licenses):
    """Is the required license in use?"""
    return arcpy.ProductInfo() in licenses


class WorkspaceFixture(object):
    GDB_NAME = 'fixture.gdb'
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
            cls.fixture_path, name, description, field_type, 'CODED')

        for (code, desc) in values.items():
            arcpy.AddCodedValueToDomain_management(
                cls.fixture_path, name, code, desc)

    @classmethod
    def createFeatureClass(cls, name, fc_type, fields):
        """Create a feature class, and add fields."""

        fc_path = os.path.join(cls.fixture_path, name)
        arcpy.CreateFeatureclass_management(cls.fixture_path, name, fc_type)

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
        fixture_dir = os.path.dirname(os.path.realpath(__file__))
        cls.fixture_path = os.path.join(fixture_dir, cls.GDB_NAME)

        if os.path.exists(cls.fixture_path):
            return

        arcpy.CreateFileGDB_management(fixture_dir, cls.GDB_NAME)

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

    def setUp(self):
        # Restore the geodatabase from backup.
        self.workspace_dir = tempfile.mkdtemp()
        self.gdb_path = os.path.join(self.workspace_dir, self.GDB_NAME)
        shutil.copytree(self.fixture_path, self.gdb_path)

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

        self.cls = Widget
        self.related_cls = Warehouse

    def tearDown(self):
        del self.cls
        del self.related_cls
        del self.workspace
        WorkspaceManager().clear()
        gc.collect()
        shutil.rmtree(self.workspace_dir)
