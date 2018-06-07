import arcpy
import os
import unittest
from cuuats.datamodel.tests.base import WorkspaceFixture, hasLicense
from cuuats.datamodel.manytomany import ManyToManyField


def setUpModule():
    WorkspaceFixture.setUpModule()


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

        warehouse = self.related_cls
        self.related_cls = self.cls
        self.cls = warehouse
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
