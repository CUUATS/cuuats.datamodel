from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.fields import BlobField, GeometryField, GlobalIDField, \
    OIDField, StringField, NumericField


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
