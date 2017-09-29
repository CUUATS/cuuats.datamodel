from cuuats.datamodel.features import BaseFeature
from cuuats.datamodel.fields import BaseField, ForeignKey
import inspect
import os




class ManyToManyField(BaseField):

    def __init__(self, name, **kwargs):
        # Initiate ManyToManyField from Parent BaseField
        super(ManyToManyField, self).__init__(name, **kwargs)

        # Set variables
        self.related_class = kwargs.get("related_class", None)
        self.related_name = kwargs.get("related_name", None)
        self.relationship_class = kwargs.get("relationship_class", None)
        self.foreign_key = kwargs.get("foreign_key", None)
        self.related_foreign_key = kwargs.get("related_foreign_key", None)
        self.primary_key = kwargs.get("primary_key", None)
        self.related_primary_key = kwargs.get("related_primary_key", None)


    def register(self, workspace, feature_class, field_name, layer_name,
                 layer_fields):
        """
        Register the ManyToManyField with the workspace
        """
        # register the field
        super(ManyToManyField, self).register(
            workspace, feature_class, field_name, layer_name, layer_fields)

        # create a new name for the method
        if self.related_name is None:
            self.related_name = feature_class.__name__.lower() + '_set'

        if isinstance(self.relationship_class, basestring): # Base feature?
            self.relationship_class = self._create_relationship_class(
                                            self.relationship_class, workspace,
                                            feature_class)

        self.relationship_class.register(os.path.join(workspace.path,
                                        self.relationship_class.__name__))
        # self._set_related(feature_class)


    def _create_relationship_class(self, relationship_class_name, workspace,
                                    feature_class):
        class RelationshipFeature(BaseFeature):
            pass

        # Set the name of the feature class.
        RelationshipFeature.__name__ = relationship_class_name

        # Set the module of the feature class to the module of the caller.
        frame = inspect.stack()[1]
        RelationshipFeature.__module__ = inspect.getmodule(frame[0])


        foreign_key = ForeignKey("Foreign Key", origin_class = feature_class,
                                    primary_key = self.primary_key,
                                    related_manager = False)
        related_foreign_key = ForeignKey("Related Foreign Key",
                                    origin_class = self.related_class,
                                    primary_key = self.related_primary_key,
                                    related_manager = False)

        setattr(RelationshipFeature, self.foreign_key, foreign_key)
        setattr(RelationshipFeature, self.related_foreign_key, related_foreign_key)

        return(RelationshipFeature)




    def _set_related(self, feature_class):
        """
        Set references to related feature classes.

        Q: Does that mean I have a set relationship between origin_class,
        relationship_class, and destination_class?
        """
        for fc in (self.related_class, feature_class):
            if fc.related_classes is None:
                fc.related_classes = {}

        # set related class for origin to relationship, relationship to
        # destination
        self.related_class.related_classes[self.relationship_class.__name__] = \
            self.relationship_class
        feature_class.related_classes[self.relationship_class.__name__] = \
            self.relationship_class




    def __get__(self, instance, owner):
        """
        Grab the data from the related tables
        """
        value = super(ManyToManyField, self).__get__(instance, owner)
        if value is None:
            return None

        return(self.related_class.objects.get({
                "__".join([self.related_name, self.foreign_key]):
                getattr(instance, self.primary_key)
            })
        )
        # Return the prefetched feature if there is one.
        # prefetched_feature = instance._prefetch_cache.get(self.name, None)
        # if prefetched_feature:
        #     return prefetched_feature

        # # Otherwise, get the related feature from the database.
        # return(self.related.objects.get({
        #     relationship_class.objects.get(instance, self.primary_key) : value
        #     })
        #     )


    def __set__():
        pass