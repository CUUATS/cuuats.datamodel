import inspect
import os
from cuuats.datamodel.features import BaseFeature, relate
from cuuats.datamodel.fields import BlobField, GeometryField, GlobalIDField, \
    OIDField, StringField, NumericField
from cuuats.datamodel.workspaces import WorkspaceManager


def feature_class_factory(path, register=True, exclude=[],
                          follow_relationships=True, class_name=None):
    """
    Create a feature class by introspecting the workspace.
    """

    workspace_path, fc_name = os.path.split(path)
    workspace = WorkspaceManager().get(workspace_path)

    class Feature(BaseFeature):
        pass

    # Set the name of the feature class.
    Feature.__name__ = str(class_name or fc_name)

    # Set the module of the feature class to the module of the caller.
    frame = inspect.stack()[1]
    Feature.__module__ = inspect.getmodule(frame[0])

    # Add fields to the feature class.
    # TODO: Handle Date and Raster field types.
    # TODO: Handle field names that conflict with Feature attributes
    # and methods.
    for (db_name, db_field) in workspace.get_layer_fields(fc_name).items():
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

    # Set up relationships.
    if follow_relationships:
        for rc_info in workspace.list_relationships(fc_name):
            if rc_info.is_attachment:
                continue

            if rc_info.origin == fc_name:
                related_path = os.path.join(
                    workspace_path, rc_info.destination)
                RelatedFeature = feature_class_factory(
                    related_path, register=False, follow_relationships=False)
                relate(Feature, RelatedFeature, rc_info.primary_key,
                       rc_info.foreign_key)
                if register:
                    RelatedFeature.register(related_path)
            else:
                related_path = os.path.join(workspace_path, rc_info.origin)
                RelatedFeature = feature_class_factory(
                    related_path, register=False, follow_relationships=False)
                relate(RelatedFeature, Feature, rc_info.primary_key,
                       rc_info.foreign_key)
                if register:
                    RelatedFeature.register(related_path)

    # Register the new feature class.
    if register:
        Feature.register(path)

    return Feature
