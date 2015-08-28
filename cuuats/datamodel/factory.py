import inspect
from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.fields import BlobField, GeometryField, GlobalIDField, \
    OIDField, StringField, NumericField


def feature_class_factory(fc_name, source, register=True, exclude=[],
                          follow_relationships=True, class_name=None):
    """
    Create a feature class by introspecting the data source.
    """

    class Feature(BaseFeature):
        pass

    # Set the name of the feature class.
    Feature.__name__ = class_name or fc_name

    # Set the module of the feature class to the module of the caller.
    frame = inspect.stack()[1]
    Feature.__module__ = inspect.getmodule(frame[0])

    # Add fields to the feature class.
    # TODO: Handle Date and Raster field types.
    # TODO: Handle field names that conflict with Feature attributes
    # and methods.
    for (db_name, db_field) in source.get_layer_fields(fc_name).items():
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
        Feature.register(source, fc_name)

    return Feature
