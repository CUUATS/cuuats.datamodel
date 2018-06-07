import unittest
from .test_domains import TestCodedValue, TestDescription
from .test_features import TestFeature, TestRegisterFeature
from .test_fields import TestFields
from .test_foreignkey import TestForiegnKey
from .test_manytomany import TestManyToManyField
from .test_query import TestQuerySet
from .test_scales import TestBreaksScale, TestDictScale
from .test_workspaces import TestWorkspace


def test_suite():
    return unittest.TestSuite([
        TestCodedValue(),
        TestDescription(),
        TestFeature(),
        TestRegisterFeature(),
        TestFields(),
        TestForiegnKey(),
        TestManyToManyField(),
        TestQuerySet(),
        TestBreaksScale(),
        TestDictScale(),
        TestWorkspace()
        ])


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
