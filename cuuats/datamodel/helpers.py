import arcpy
import os
from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.fields import BlobField, GeometryField, GlobalIDField, \
    OIDField, StringField, NumericField


class FeatureClassManager(object):
    """
    Helper to manage the relationship between feature classes in the database
    and in Python.
    """

    CLASS_TEMPLATE = "class %(class_name)s(BaseFeature):"

    FIELD_TEMPLATE = "%(field_name)s = %(field_class)s('%(field_label)s')"

    FIELD_TYPE_MAP = {
        'Geometry': 'GeometryField',
        'Integer': 'NumericField',
        'OID': 'OIDField',
        'Single': 'NumericField',
        'SmallInteger': 'NumericField',
        'String': 'StringField',
    }

    def __init__(self, source):
        self.source = source

    def python_stub(self, layer_name):
        """
        Generate stub code for an existing feature class.
        """

        layer_path = os.path.join(self.source.path, layer_name)
        class_string = self.CLASS_TEMPLATE % {
            'class_name': layer_name.split('.')[-1]
        }
        fields_string = '    ' + '\n    '.join([self.FIELD_TEMPLATE % {
            'field_name': f.name,
            'field_class': self.FIELD_TYPE_MAP.get(f.type, 'BaseField'),
            'field_label': f.aliasName.replace(r"'", r"\'"),
        } for f in arcpy.ListFields(layer_path)])

        return '\n\n'.join([class_string, fields_string, ''])


def feature_class_factory(name, source, register=True, exclude=[],
                          follow_relationships=True):
    """
    Create a feature class by introspecting the data source.
    """

    class Feature(BaseFeature):
        pass

    Feature.__name__ = name

    # TODO: Handle Date and Raster field types.
    # TODO: Handle field names that conflict with Feature attributes
    # and methods.
    for (db_name, db_field) in source.get_layer_fields(name).items():
        field = {
            'Blob': BlobField,
            'Geometry': GeometryField,
            'Guid': GlobalIDField,
            'OID': OIDField,
            'String': StringField,
        }.get(db_field.type, NumericField)(
            (db_field.aliasName or db_name),
            required=db_field.required)

        setattr(Feature, db_name, field)

    if follow_relationships:
        # TODO: Implement relationship following.
        pass

    if register:
        Feature.register(source, name)

    return Feature
