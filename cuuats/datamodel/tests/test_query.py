import unittest
from cuuats.datamodel.tests.base import WorkspaceFixture
from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned


def setUpModule():
    WorkspaceFixture.setUpModule()


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
