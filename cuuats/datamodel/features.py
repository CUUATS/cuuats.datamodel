import os
import re
from collections import OrderedDict
from cuuats.datamodel.fields import BaseField, OIDField, BatchField, \
    ForeignKey, NumericField, StringField, BlobField, GeometryField
from cuuats.datamodel.field_values import DeferredValue
from cuuats.datamodel.query import Q, Manager, SQLCompiler

IDENTIFIER_RE = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')


def require_source(fn):
    """
    Decorator to check that the class has been registered with a data source.
    """

    def wrapper(*args, **kwargs):
        if getattr(args[0], 'source', None) is None:
            raise AttributeError(
                'Feature class %s must be registered with a data source' %
                (args[0].__name__,))
        return fn(*args, **kwargs)

    return wrapper


class FieldSet(OrderedDict):

    def __init__(self, *args, **kwargs):
        super(FieldSet, self).__init__(*args, **kwargs)
        self.oid_field = None
        self.geom_field = None
        # Set the name and db_name of fields. This also happens during
        # field registration, but we may need to access these attributes
        # before the field is registered.
        for field_name, field in self.items():
            field.name = field_name
            field.db_name = field.db_name or field_name
            if isinstance(field, OIDField):
                self.oid_field = field
            elif isinstance(field, GeometryField):
                self.geom_field = field

    def get_name(self, db_name):
        """
        Given the database name, get the corresponding field name.
        """

        for (field_name, field) in self.items():
            if (field.db_name or field_name) == db_name:
                return field_name

    def get_db_name(self, field_name):
        """
        Get the database name for a field.
        """

        return self[field_name].db_name


class FieldManager(object):

    def __get__(self, instance, owner):
        if getattr(owner, '_fields', None) is None:
            self._cache_fields(owner)
        return owner._fields

    def _cache_fields(self, owner):
        fields = []

        # Walk the inheritance chain looking for fields.
        for subcls in type.mro(owner):
            fields.extend([(f.order, f.creation_index, n, f) for (n, f) in
                          subcls.__dict__.items()
                          if isinstance(f, BaseField)])
        # Cache the resutling FieldSet.
        owner._fields = FieldSet([d[2:] for d in sorted(fields)])


class BaseFeature(object):
    """
    Base class used to interact with data stored in a geodatabase feature
    class.
    """

    name = None
    source = None
    attachments = None
    db_row = None
    objects = Manager()
    fields = FieldManager()

    @classmethod
    def register(cls, source, layer_name):
        """
        Connect this feature class to a layer in the data source.
        """

        cls.source = source
        cls.name = layer_name

        if not issubclass(cls, BaseAttachment):
            attachment_info = source.get_attachment_info(layer_name)
            if attachment_info is not None:
                Attachment = attachment_class_factory(
                    cls, attachment_info.primary_key,
                    attachment_info.foreign_key)
                Attachment.register(cls.source, attachment_info.destination)
                cls.attachment_class = Attachment

        # Register fields with the source
        layer_fields = source.get_layer_fields(layer_name)
        for (field_name, field) in cls.fields.items():
            field.register(source, cls, field_name, cls.name, layer_fields)

    @classmethod
    @require_source
    def count(cls, where_clause=None):
        """
        Count the number of features matching the given where clause.
        """

        return cls.source.count_rows(cls.name, where_clause)

    @classmethod
    @require_source
    def update_batch_fields(cls, field_names=None):
        """
        Update the given (or all) batch fields for this feature class.
        """

        fields = [f for (n, f) in cls.fields.items()
                  if (isinstance(f, BatchField)
                  and (field_names is None or n in field_names))]

        for field in fields:
            field.update(cls)

    @classmethod
    @require_source
    def sync_fields(cls, modify=False, remove=False):
        """
        Adds (and optionally, modify or removes) database fields to match
        those defined in this feature class.
        """

        if modify or remove:
            raise NotImplementedError('Modification and removal of fields '
                                      'are not yet supported')

        layer_fields = cls.source.get_layer_fields(cls.name)
        added_fields = []

        for (field_name, field) in cls.fields.items():
            if field_name not in layer_fields and field.storage:
                storage = field.storage
                if 'field_alias' not in storage:
                    storage['field_alias'] = field.label
                cls.source.add_field(cls.name, field_name, storage)
                added_fields.append(field_name)

        # Reregister fields
        layer_fields = cls.source.get_layer_fields(cls.name)
        for (field_name, field) in cls.fields.items():
            field.register(cls.source, cls, field_name, cls.name, layer_fields)

    @require_source
    def __init__(self, **kwargs):
        for field_name in kwargs.keys():
            if field_name not in self.fields.keys():
                raise KeyError('Invalid field name: %s' % (field_name))

        self.values = kwargs
        self.db_row = [kwargs[f] for f in self.fields.keys() if f in kwargs
                       and not isinstance(kwargs[f], DeferredValue)]
        self._prefetch_cache = {}

    def __repr__(self):
        name = self.name or '(unregistered)'
        return '<%s: %s>' % (self.__class__.__name__, name)

    @property
    def oid(self):
        """
        Returns the object ID (primary key) for this feature.
        """

        return getattr(self, self.fields.oid_field.name)

    @property
    def oid_where(self):
        """
        Where clause for selecting this feature.
        """

        compiler = SQLCompiler(self.__class__)
        return compiler.compile(Q({self.fields.oid_field.name: self.oid}))

    def get_deferred_values(self, fields=[]):
        """
        Retrieve deferred values from the database.
        """

        if not fields:
            fields = dict([(v.field_name, v.db_name) for v in
                           self.values.values()
                           if isinstance(v, DeferredValue)])

        values = self.source.get_row(
            self.name, fields.values(), self.oid_where)

        self.values.update(dict(zip(fields.keys(), values)))

    def set_by_description(self, field_name, description):
        """
        Set the given field to the coded value that matches the given
        description.
        """

        field = self.fields.get(field_name)
        domain = self.source.get_domain(field.domain_name, 'CodedValue')
        code = self.source.get_coded_value(domain.name, description)
        setattr(self, field_name, code)

    def get_coded_value_for(self, field_name, description):
        """
        Look up the coded value for the given description in the given field's
        domain.
        """

        field = self.fields.get(field_name)
        return self.source.get_coded_value(field.domain_name, description)

    def get_description_for(self, field_name, value=None):
        """
        Look up the domain description for the current (or given) value of
        the given field.
        """

        if value is None:
            value = getattr(self, field_name)

        field = self.fields.get(field_name)
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
        validation error messages. Deferred values that have not been
        retrieved are skipped, as are foreign keys.
        """

        messages = []
        for (field_name, field) in self.fields.items():
            if isinstance(field, ForeignKey):
                continue

            # Skip deferred values that have not been retrieved.
            if isinstance(self.values.get(field_name, None), DeferredValue):
                continue

            value = getattr(self, field_name)
            if value is None and self.check_condition(
                    field.required_if, default=False):
                messages.append('%s is missing' % (field.label,))
            else:
                messages.extend(field.validate(value))
        return messages

    def serialize(self):
        """
        Return a list of values for the fields in this feature. Deferred values
        that have not been retrieved are excluded.
        """

        return [self.values.get(n, None) if isinstance(f, ForeignKey) else
                getattr(self, n) for (n, f) in self.fields.items()
                if not isinstance(self.values.get(n), DeferredValue)]

    def save(self):
        """
        Update the corresponding row in the data source.
        """

        new_row = self.serialize()
        if new_row == self.db_row:
            return False

        oid_field = self.fields.oid_field.name
        oid = getattr(self, oid_field)
        field_names = [f for f in self.fields.keys()
                       if not isinstance(self.values.get(f), DeferredValue)]
        where_clause = '%s = %i' % (oid_field, oid)
        updated_count = 0
        for (row, cursor) in self.source.iter_rows(
                self.name, field_names, True, where_clause):
            self.source.update_row(cursor, new_row)
            updated_count += 1

        if updated_count == 0:
            raise LookupError('A row with OID %i was not found' % (oid,))
        else:
            self.db_row = new_row
            return True

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


class BaseAttachment(BaseFeature):
    """
    A file attached to an ArcGIS feature class.
    """

    attachment_id = OIDField(
        'Attachment ID',
        db_name='ATTACHMENTID')

    file_name = StringField(
        'File Name',
        db_name='ATT_NAME')

    content_type = StringField(
        'Content Type',
        db_name='CONTENT_TYPE')

    file_size = NumericField(
        'File Size',
        db_name='DATA_SIZE')

    data = BlobField(
        'File Data',
        db_name='DATA')

    @property
    def has_data(self):
        """
        Does this attachment have data?
        """

        return self.data is not None

    def save_to(self, path, filename=None, overwrite=False, data=None):
        """
        Save the attached file to the specified directory path.
        """

        if not self.has_data and data is None:
            raise ValueError('Cannot save an attachment with no data')

        if data is None:
            data = self.data

        file_path = os.path.join(path, filename or self.file_name)
        if os.path.isfile(file_path) and not overwrite:
            raise IOError('%s already exists' % (file_path,))

        with open(file_path, 'wb') as file:
            file.write(data)


def attachment_class_factory(
        origin_class, primary_key, foreign_key, related_name='attachments',
        base_class=BaseAttachment):
    """
    Each feature class needs a unique attachment class so that it can be
    registered for the appropriate attachment table in the data source.
    """

    class Attachment(base_class):
        """
        A file attachment.
        """

        feature = ForeignKey(
            'Feature',
            db_name=foreign_key,
            origin_class=origin_class,
            primary_key=primary_key,
            related_name=related_name)

    return Attachment
