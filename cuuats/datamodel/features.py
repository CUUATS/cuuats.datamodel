import re
from collections import OrderedDict
from cuuats.datamodel.fields import BaseField, OIDField
from cuuats.datamodel.attachments import AttachmentRelationship

IDENTIFIER_RE = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')


def require_source(fn):
    """
    Decorator to check that the class has been registered with a data source.
    """

    def wrapper(*args, **kwargs):
        if getattr(args[0], 'source', None) is None:
            raise AttributeError(
                'Feature class must be registered with a data source')
        return fn(*args, **kwargs)

    return wrapper


class BaseFeature(object):
    """
    Base class used to interact with data stored in a geodatabase feature
    class.
    """

    name = None
    source = None
    attachments = None
    oid_field_name = None

    @classmethod
    def register(cls, source, layer_name):
        """
        Connect this feature class to a layer in the data source.
        """

        cls.source = source
        cls.name = layer_name

        attachment_info = source.get_attachment_info(layer_name)
        if attachment_info is not None:
            cls.attachments = AttachmentRelationship(*attachment_info)

        # Register fields with the source
        layer_fields = source.get_layer_fields(layer_name)
        for (field_name, field) in cls.get_fields().items():
            field.register(source, field_name, cls.name, layer_fields)
            if isinstance(field, OIDField):
                cls.oid_field_name = field_name

    @classmethod
    def get_fields(cls):
        """
        Returns an OrderedDict containing the fields for this feature.
        """

        fields = []
        # Walk the inheritance chain looking for fields.
        for subcls in type.mro(cls):
            fields.extend([(f.order, f.creation_index, n, f) for (n, f) in
                          subcls.__dict__.items()
                          if isinstance(f, BaseField)])
        return OrderedDict([d[2:] for d in sorted(fields)])

    @classmethod
    @require_source
    def iter(cls, update=False, where_clause=None, limit=None):
        """
        Create a generator used to iterate over features in this class.
        """

        field_names = cls.get_fields().keys()
        for (row, cursor) in cls.source.iter_rows(
                cls.name, field_names, update, where_clause, limit):
            feature = cls(**dict(zip(field_names, row)))
            feature.cursor = cursor
            yield feature
            feature.cursor = None

    @classmethod
    @require_source
    def sync_fields(cls, modify=False, remove=False):
        """
        Adds (and optionally, modify or removes) database fields to match
        those defined in this feature class.
        """

        if modify or remove:
            raise NotImplemented('Modification and removal of fields are not '
                                 'yet supported')

        cls_fields = cls.get_fields()
        layer_fields = cls.source.get_layer_fields(cls.name)
        added_fields = []

        for (field_name, field) in cls_fields.items():
            if field_name not in layer_fields and field.storage:
                storage = field.storage
                if 'field_alias' not in storage:
                    storage['field_alias'] = field.label
                cls.source.add_field(cls.name, field_name, storage)
                added_fields.append(field_name)

        # Reregister fields
        layer_fields = cls.source.get_layer_fields(cls.name)
        for (field_name, field) in cls_fields.items():
            field.register(cls.source, field_name, cls.name, layer_fields)

    @classmethod
    @require_source
    def get(cls, oid):
        """
        Get a single feature by OID.
        """

        where_clause = '%s = %i' % (cls.oid_field_name, oid)
        features = list(cls.iter(where_clause=where_clause))
        if len(features) == 1:
            return features[0]
        raise LookupError('A feature with OID %i was not found' % (oid,))

    @require_source
    def __init__(self, **kwargs):
        self.fields = self.__class__.get_fields()
        for field_name in kwargs.keys():
            if field_name not in self.fields.keys():
                raise KeyError('Invalid field name: %s' % (field_name))

        self.values = kwargs
        self.cursor = None

    def __repr__(self):
        name = self.name or '(unregistered)'
        return '%s: %s' % (self.__class__.__name__, name)

    def get_field(self, field_name):
        """
        Get the field with the given name.
        """

        if field_name not in self.fields:
            raise KeyError('Invalid field name: %s' % (field_name,))
        return self.fields[field_name]

    def set_by_description(self, field_name, description):
        """
        Set the given field to the coded value that matches the given
        description.
        """

        field = self.get_field(field_name)
        domain = self.source.get_domain(field.domain_name, 'CodedValue')
        code = self.source.get_coded_value(domain.name, description)
        setattr(self, field_name, code)

    def get_coded_value_for(self, field_name, description):
        """
        Look up the coded value for the given description in the given field's
        domain.
        """

        field = self.get_field(field_name)
        return self.source.get_coded_value(field.domain_name, description)

    def get_description_for(self, field_name, value=None):
        """
        Look up the domain description for the current (or given) value of
        the given field.
        """

        if value is None:
            value = getattr(self, field_name)

        field = self.get_field(field_name)
        domain = self.source.get_domain(field.domain_name, 'CodedValue')
        return domain.codedValues.get(value, None)

    def clean(self):
        """
        Perform cleaning of the raw data.
        """
        # Overridden by subclasses.
        pass

    def validate(self):
        """
        Perform validation on each field in the feature, and return any
        validation error messages.
        """

        messages = []
        for (field_name, field) in self.fields.items():
            value = getattr(self, field_name)
            if value is None and self.check_condition(
                    field.required_if, default=False):
                messages.append('%s is missing' % (field.label,))
            else:
                messages.extend(field.validate(value))
        return messages

    def serialize(self):
        """
        Return a list of values for the fields in this feature.
        """

        return [getattr(self, f) for f in self.fields.keys()]

    def update(self):
        """
        Update the corresponding row in the data source.
        """

        if self.cursor is None:
            # There is not an active cursor.
            cls = self.__class__
            oid_field = cls.oid_field_name
            oid = getattr(self, oid_field)
            field_names = cls.get_fields().keys()
            where_clause = '%s = %i' % (oid_field, oid)
            updated_count = 0
            for (row, cursor) in cls.source.iter_rows(
                    cls.name, field_names, True, where_clause):
                self.source.update_row(cursor, self.serialize())
                updated_count += 1

            if updated_count == 0:
                raise LookupError('A row with OID %i was not found' % (oid,))

        else:
            self.source.update_row(self.cursor, self.serialize())

    def eval(self, expression):
        """
        Evaluate the expression in the context of the feature instance.
        """

        identifiers = IDENTIFIER_RE.findall(expression)
        # Limit retrieval of field values to field names that are found in the
        # expression in order to prevent recursion for calculated fields.
        locals_dict = dict([(f, getattr(self, f)) for f
                            in self.fields.keys() if f in identifiers])
        locals_dict.update({'self': self})
        return eval(expression, {}, locals_dict)

    def check_condition(self, condition, default=True):
        """
        Returns a boolean indicating whether the condition is true for the
        this feature instance.
        """

        if condition is None:
            return default

        return bool(self.eval(condition))
