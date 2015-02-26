import arcpy
import os


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
