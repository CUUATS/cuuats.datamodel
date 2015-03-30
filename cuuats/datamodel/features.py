from collections import OrderedDict
from cuuats.datamodel.fields import BaseField, NumericField, CalculatedField
from cuuats.datamodel.attachments import AttachmentRelationship


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

        layer_fields = source.get_layer_fields(layer_name)

        for (field_name, field) in cls.get_fields().items():
            layer_field = layer_fields.get(field_name, None)
            if layer_field is None:
                raise KeyError('%s is not a field of %s' % (
                    field_name, layer_name))

            cls._init_field(field_name, field, layer_field)

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
    def iter(cls, update=True, where_clause=None):
        """
        Create a generator used to iterate over features in this class.
        """

        field_names = cls.get_fields().keys()
        for (row, cursor) in cls.source.iter_rows(
                cls.name, field_names, update, where_clause):
            feature = cls(**dict(zip(field_names, row)))
            feature.cursor = cursor
            yield feature
            feature.cursor = None

    @classmethod
    def _init_field(cls, field_name, field, layer_field):
        """
        Set field properties based on the database schema.
        """

        field.name = field_name
        if layer_field.domain:
            field.domain_name = layer_field.domain
            domain = cls.source.get_domain(layer_field.domain)
            if domain.domainType == 'CodedValue':
                field.choices.extend(domain.codedValues)
            elif isinstance(field, NumericField):
                field.min, field.max = domain.range

    @require_source
    def __init__(self, **kwargs):
        self.fields = self.__class__.get_fields()
        for field_name in kwargs.keys():
            if field_name not in self.fields.keys():
                raise KeyError('Invalid field name: %s' % (field_name))

        self.values = kwargs
        self.cursor = None

        # Trigger the value changed event for all fields that were set
        # in order to update calculated fields.
        for field_name in kwargs.keys():
            self._field_value_changed(field_name)

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
            # Handle conditional requirements outside the field class
            # since they need access to all the field values.
            locals_dict = {'self': self}
            locals_dict.update(self.values)
            if field.required_if is not None and value is None and \
                    eval(field.required_if, {}, locals_dict):
                messages.append('%s is missing' % (field.label,))
            else:
                messages.extend(field.validate(value))
        return messages

    def serialize(self):
        """
        Return a list of values for the fields in this feature.
        """

        return [self.values.get(f, None) for f in self.fields.keys()]

    def update(self):
        """
        Update the corresponding row in the data source. Requires an active
        update cursor.
        """

        if self.source is None:
            raise NotImplementedError(
                'Cannot update features without an active update cursor')
        self.source.update_row(self.cursor, self.serialize())

    def _field_value_changed(self, field_name):
        """
        Event handler triggered when a field value is changed.
        """

        # Update calculated fields that are tied to the changed value.
        for field in self.fields.values():
            if isinstance(field, CalculatedField) and (
                    not field.update_for or field_name in field.update_for):
                field.update(self)
