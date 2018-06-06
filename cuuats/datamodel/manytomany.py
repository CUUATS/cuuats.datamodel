from cuuats.datamodel.features import BaseFeature, VirtualField
from cuuats.datamodel.fields import ForeignKey
import inspect
import os


class ManyToManyField(VirtualField):
    def __init__(self, label, **kwargs):
        # Initiate ManyToManyField from Parent BaseField
        super(ManyToManyField, self).__init__()

        # Set variables
        self.label = label
        self.name = kwargs.get('name', None)
        self.related_class = kwargs.get("related_class", None)
        self.related_name = kwargs.get("related_name", None)
        self.relationship_class = kwargs.get("relationship_class", None)
        self.foreign_key = kwargs.get("foreign_key", None)
        self.related_foreign_key = kwargs.get("related_foreign_key", None)
        self.primary_key = kwargs.get("primary_key", None)
        self.related_primary_key = kwargs.get("related_primary_key", None)

    def register(self, workspace, feature_class, field_name):
        """
        Register the ManyToManyField with the workspace
        """
        # register the field
        # create a new name for the method
        if self.related_name is None:
            self.related_name = feature_class.__name__.lower() + '_set'

        if isinstance(self.relationship_class, basestring):
            self.relationship_class = self._create_relationship_class(
                                            self.relationship_class, workspace,
                                            feature_class)

        self.relationship_class.register(os.path.join(workspace.path,
                                         self.relationship_class.__name__))

        self._set_reverse_relationship(feature_class)

    def _create_relationship_class(self, relationship_class_name, workspace,
                                   feature_class):
        class RelationshipFeature(BaseFeature):
            pass

        # Set the name of the feature class.
        RelationshipFeature.__name__ = relationship_class_name

        # Set the module of the feature class to the module of the caller.
        frame = inspect.stack()[1]
        RelationshipFeature.__module__ = inspect.getmodule(frame[0])

        foreign_key = ForeignKey("Foreign Key", origin_class=feature_class,
                                 primary_key=self.primary_key)
        related_foreign_key = ForeignKey("Related Foreign Key",
                                         origin_class=self.related_class,
                                         primary_key=self.related_primary_key)

        setattr(RelationshipFeature, self.foreign_key, foreign_key)
        setattr(RelationshipFeature,
                self.related_foreign_key,
                related_foreign_key)

        return(RelationshipFeature)

    def _set_related(self, feature_class):
        """
        Set references to related feature classes.
        """
        for fc in (self.related_class, feature_class):
            if fc.related_classes is None:
                fc.related_classes = {}

        # set related class for origin to relationship, relationship to
        # destination

        self.related_class.related_classes[
            self.relationship_class.__name__] = self.relationship_class
        feature_class.related_classes[
            self.relationship_class.__name__] = self.relationship_class

    def _set_reverse_relationship(self, feature_class):
        # create a many to many field and assign it to the related class
        setattr(self.related_class, self.related_name,
                ManyToManyField(
                    self.related_name,
                    related_class=feature_class,
                    relationship_class=self.relationship_class,
                    foreign_key=self.related_foreign_key,
                    related_foreign_key=self.foreign_key,
                    primary_key=self.related_primary_key,
                    related_primary_key=self.primary_key
                    )
                )

    def __get__(self, instance, owner):
        """
        Grab the data from the related tables
        """
        if not instance:
            return(self)

        fk_related_name = self.relationship_class.fields.get(
                    self.related_foreign_key).related_name

        return(self.related_class.objects.filter({
                "__".join([fk_related_name, self.foreign_key]):
                getattr(instance, self.primary_key)
            })
        )

    def __set__():
        raise NotImplementedError
