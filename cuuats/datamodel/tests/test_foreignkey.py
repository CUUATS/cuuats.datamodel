import arcpy
import unittest
from cuuats.datamodel.fields import ForeignKey
from cuuats.datamodel.tests.base import WorkspaceFixture


def setUpModule():
    WorkspaceFixture.setUpModule()


class TestForiegnKey(WorkspaceFixture, unittest.TestCase):
    FK_FIELD = 'warehouse_id'
    FK_VALUES = (1, 1, 2)
    PK_FIELD = 'OBJECTID'

    def setUp(self):
        super(TestForiegnKey, self).setUp()

        arcpy.AddField_management(self.fc_path, self.FK_FIELD, 'LONG')

        with arcpy.da.UpdateCursor(self.fc_path, (self.FK_FIELD,)) as cursor:
            for i, row in enumerate(cursor):
                cursor.updateRow([self.FK_VALUES[i]])

        self.cls.warehouse_id = ForeignKey('Warehouse ID',
                                           origin_class=self.related_cls,
                                           primary_key=self.PK_FIELD)

        self.cls.register(self.fc_path)
        self.related_cls.register(self.rc_path)

    def test_foreign_key_lookup(self):
        for i, fk_value in enumerate(self.FK_VALUES):
            widget = self.cls.objects.get(OBJECTID=(i + 1))
            warehouse = getattr(widget, self.FK_FIELD)
            warehouse_id = getattr(warehouse, self.PK_FIELD)

            self.assertTrue(
                isinstance(warehouse, self.related_cls),
                'foreign key value is not an instance of the related class')

            self.assertEqual(
                warehouse_id, fk_value,
                'foreign key value has the wrong ID')

    def test_foreign_key_prefetch(self):
        for widget in list(self.cls.objects.prefetch_related('warehouse_id')):
            prefetched = widget._prefetch_cache[self.FK_FIELD]
            warehouse_id = getattr(prefetched, self.PK_FIELD)

            self.assertTrue(
                isinstance(prefetched, self.related_cls),
                'prefetched foreign key value is not an instance '
                'of the related class')

            self.assertEqual(
                warehouse_id, self.FK_VALUES[widget.OBJECTID - 1],
                'prefetched foreign key value has the wrong ID')

    def test_foreign_key_query(self):
        widgets = list(self.cls.objects.filter(warehouse_id=2))
        self.assertEqual(
            len(widgets), 1,
            'wrong number of objects returned in foreign key query')

        self.assertEqual(
            widgets[0].OBJECTID, 3,
            'foreign key query returns the wrong object')

    def test_related_manager_lookup(self):
        for pk_value in (1, 2):
            warehouse = self.related_cls.objects.get(OBJECTID=pk_value)

            for widget in warehouse.widget_set.all():
                self.assertTrue(
                    isinstance(widget, self.cls),
                    'related manager value is not an instance of the class')

                self.assertEqual(
                    pk_value, self.FK_VALUES[widget.OBJECTID - 1],
                    'related manager value has the wrong ID')

    def test_related_manager_prefetch(self):
        for warehouse in list(
                self.related_cls.objects.prefetch_related('widget_set')):
            for widget in warehouse._prefetch_cache['widget_set']:
                self.assertTrue(
                    isinstance(widget, self.cls),
                    'prefetched related manager value is not an instance '
                    'of the class')

                self.assertEqual(
                    getattr(warehouse, self.PK_FIELD),
                    self.FK_VALUES[widget.OBJECTID - 1],
                    'prefetched related manager value has the wrong ID')

    def test_related_manager_query(self):
        warehouses = list(
            self.related_cls.objects.filter(widget_set__OBJECTID=1))
        self.assertEqual(
            len(warehouses), 1,
            'wrong number of objects returned in related manager query')

        self.assertEqual(
            warehouses[0].OBJECTID, 1,
            'related manager query returns the wrong object')
