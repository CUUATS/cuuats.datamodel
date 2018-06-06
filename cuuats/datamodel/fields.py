import warnings
from numbers import Number
from cuuats.datamodel.domains import CodedValue, D
from cuuats.datamodel.field_values import DeferredValue
from cuuats.datamodel.scales import BaseScale, ScaleLevel
from cuuats.datamodel.query import RelatedManager


class BaseField(object):

    creation_index = 0
    default_storage = {}

    def __init__(self, label, **kwargs):
        self.label = label
        self.name = kwargs.get('name', None)
        self.db_name = kwargs.get('db_name', None)
        self.db_scale = kwargs.get('db_scale', None)
        self.db_precision = kwargs.get('db_precision', None)
        self.deferred = kwargs.get('deferred', False)
        self.required = kwargs.get('required', False)
        self.required_if = kwargs.get('required_if', None)
        self.domain_name = kwargs.get('domain_name', None)
        self.choices = list(kwargs.get('choices', []))
        self.storage = self.default_storage.copy()
        self.storage.update(kwargs.get('storage', {}))

        # Handle field ordering
        self.order = kwargs.get('order', 0)
        self.creation_index = BaseField.creation_index
        BaseField.creation_index += 1

    def __get__(self, instance, owner):
        # Retrieve deferred values if necessary.

        if isinstance(instance.values.get(self.name, None), DeferredValue):
            instance.get_deferred_values()

        # Get the current value from the instance.
        value = instance.values.get(self.name, None)

        # If this field has a coded values domain, set the description
        # for this value.
        if value is not None and self.domain_name:
            domain = instance.workspace.get_domain(self.domain_name)
            if domain.domainType == 'CodedValue':
                description = domain.codedValues.get(value, None)
                return CodedValue(value, description)

        return value

    def __set__(self, instance, value):
        # If this is a coded value, convert it back to a primative before
        # storing it.
        if isinstance(value, CodedValue):
            value = value.value

        # If the value representes a description for a coded values domain,
        # look up the value from the domain.
        elif isinstance(value, D):
            value = instance.workspace.get_coded_value(
                self.domain_name, value.description)

        instance.values[self.name] = value

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self.label)

    def register(self, workspace, feature_class, field_name, layer_name,
                 layer_fields):
        """
        Register this field with the workspace.
        """

        layer_field = layer_fields.get(self.db_name or field_name, None)
        if layer_field is None:
            warnings.warn('%s is not a field of %s' % (
                field_name, layer_name))
            return None

        self.name = field_name
        if self.db_name is None:
            self.db_name = field_name
        if layer_field.domain:
            self.domain_name = layer_field.domain
            domain = workspace.get_domain(layer_field.domain)
            if domain.domainType == 'CodedValue':
                self.choices.extend(domain.codedValues)
            elif hasattr(self, 'min') and hasattr(self, 'max'):
                self.min, self.max = domain.range
        if layer_field.scale:
            self.db_scale = layer_field.scale
        if layer_field.precision:
            self.db_precision = layer_field.precision
        return layer_field

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # Check for missing values in required fields.
        if self.required and value is None:
            return ['%s is missing' % (self.label,)]

        # Check that that value is in the choices, if specified.
        elif len(self.choices) > 0 and value is not None and \
                value not in self.choices:
            return ['%s is invalid' % (self.label,)]

        return []

    def round(self, value):
        """
        Rounds the given value to the number of decimal places it will have
        in the database.
        """

        if self.db_scale is not None and isinstance(value, Number):
            return round(value, self.db_scale)

        return value

    def has_changed(self, old, new):
        """
        Returns true if the new value is different from the old value.
        """

        # When the scale is set, we round to that number of decimal places
        # before comparing in order to determine whether the values will be
        # equal once saved in the database.
        return self.round(old) != self.round(new)

    def summarize(self, instance):
        """
        Returns the summary level for the given instance.
        """

        value = self.__get__(instance, None)
        return SummaryLevel(0, value, str(value))


class OIDField(BaseField):
    """
    Object ID field type.
    """

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # OIDs are always valid.
        return []


class GlobalIDField(BaseField):
    """
    Global ID field type.
    """

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # GlobalIDs are always valid.
        return []


class GeometryField(BaseField):
    """
    Geometry field type.
    """

    def __init__(self, label, **kwargs):
        super(GeometryField, self).__init__(label, **kwargs)
        self.db_name = kwargs.get('db_name', 'SHAPE@')
        self.deferred = kwargs.get('deferred', True)

    def register(self, workspace, feature_class, field_name, layer_name,
                 layer_fields):
        """
        Register this field with the workspace.
        """

        # TODO: Figure out how to check whether the geometry field exists,
        # since the db_name is a special ArcGIS keyword that is not returned
        # by ListFields.

        self.name = field_name
        if self.db_name is None:
            self.db_name = field_name

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # TODO: Add validation for geometries.
        return super(GeometryField, self).validate(value)


class StringField(BaseField):

    default_storage = {
        'field_type': 'TEXT',
        'field_length': 100
    }

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # Convert empty strings to None before performing validation.
        if isinstance(value, str) and value.strip() == '':
            value = None
        return super(StringField, self).validate(value)


class NumericField(BaseField):

    default_storage = {
        'field_type': 'FLOAT',
    }

    def __init__(self, name, **kwargs):
        super(NumericField, self).__init__(name, **kwargs)
        self.min = kwargs.get('min', None)
        self.max = kwargs.get('max', None)

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        messages = super(NumericField, self).validate(value)
        if value is None:
            return messages

        if (self.min is not None and value < self.min) or \
                (self.max is not None and value > self.max):
            messages.append('%s out of range' % (self.label,))

        return messages


class BlobField(BaseField):
    """
    A field for storing blob data.
    """

    def __init__(self, label, **kwargs):
        super(BlobField, self).__init__(label, **kwargs)
        self.deferred = kwargs.get('deferred', True)


class CalculatedField(BaseField):

    def __init__(self, name, **kwargs):
        super(CalculatedField, self).__init__(name, **kwargs)

        # Overridden by subclasses
        self.condition = kwargs.get('condition', None)
        self.default = kwargs.get('default', None)
        self.scale = kwargs.get('scale', None)

    def _as_scale_level(self, level):
        if isinstance(level, ScaleLevel):
            return level
        return ScaleLevel(level, str(level))

    def __get__(self, instance, owner):
        if not instance.check_condition(self.condition):
            return self.default
        return self.calculate(instance)

    def __set__(self, instance, value):
        raise ValueError('Calculated fields cannot be set')

    def calculate(self, instance):
        """
        Calculate the value for this field based on the state of the instance.
        """

        # Overridden by subclasses
        return self.default

    def summarize(self, instance):
        """
        Returns the scale level for the given instance.
        """

        if not self.scale:
            raise AttributeError(
                'Weights fields must have a scale to be summarized')

        value = self.round(self.calculate(instance))
        return self._as_scale_level(self.scale.get_level(value))

    def get_levels(self):
        """
        Returns a sorted list of all possible scale levels.
        """

        if not self.scale:
            raise AttributeError(
                'Weights fields must have a scale to be summarized')

        return sorted(list(set(
            [self._as_scale_level(l) for l in self.scale.get_levels()])))


class MethodField(CalculatedField):

    default_storage = {
        'field_type': 'DOUBLE',
    }

    def __init__(self, name, **kwargs):
        super(MethodField, self).__init__(name, **kwargs)
        self.method_name = kwargs.get('method_name')

    def calculate(self, instance):
        """
        Calculate the value for this field based on the state of the instance.
        """

        return getattr(instance, self.method_name)(self.name)


class WeightsField(CalculatedField):

    default_storage = {
        'field_type': 'DOUBLE',
    }

    def __init__(self, name, **kwargs):
        super(WeightsField, self).__init__(name, **kwargs)
        self.weights = kwargs.get('weights')

    def _get_value(self, instance, field_name):
        return getattr(instance, field_name)

    def calculate(self, instance):
        """
        Calculate the value for this field based on the state of the instance.
        """

        if None in [self._get_value(instance, v) for v in self.weights.keys()]:
            return self.default
        else:
            return sum([self._get_value(instance, v)*w
                        for (v, w) in self.weights.items()])


class ScaleField(CalculatedField):

    default_storage = {
        'field_type': 'SHORT',
    }

    def __init__(self, name, **kwargs):
        super(ScaleField, self).__init__(name, **kwargs)
        self.value_field = kwargs.get('value_field')
        self.use_description = kwargs.get('use_description', False)

        if not isinstance(self.scale, (BaseScale, list, tuple)):
            raise TypeError('Scale must be a subclass of BaseScale or a '
                            'list containing tuples of conditions and scales')

    def _unpack_scales(self):
        if isinstance(self.scale, BaseScale):
            yield (None, self.scale, 0)
        else:
            for (index, scale_info) in enumerate(self.scale):
                if len(scale_info) == 3:
                    yield scale_info
                else:
                    condition, scale = scale_info
                    yield (condition, scale, index)

    def _get_scale_for(self, instance):
        for (condition, scale, weight) in self._unpack_scales():
            if instance.check_condition(condition):
                return (scale, weight)

        return (None, 0)

    def _get_value_for(self, instance):
        value = instance.eval(self.value_field)
        if self.use_description and isinstance(value, CodedValue):
            value = value.description
        return value

    def _as_scale_level(self, scale_weight, level):
        if isinstance(level, ScaleLevel):
            return level.wrap(scale_weight)
        return ScaleLevel(level, str(level), (scale_weight, 0))

    def calculate(self, instance):
        """
        Calculate the value for this field based on the state of the instance.
        """

        value = self._get_value_for(instance)
        scale, weight = self._get_scale_for(instance)
        if not scale:
            return self.default

        return scale.score(value)

    def summarize(self, instance):
        """
        Returns the summary level for the given instance.
        """

        value = self._get_value_for(instance)
        scale, scale_weight = self._get_scale_for(instance)
        if not scale:
            return ScaleLevel(self.default, str(self.default), (0, 0))

        return self._as_scale_level(scale_weight, scale.get_level(value))

    def get_levels(self):
        """
        Returns a sorted list of all possible scale levels.
        """

        levels = []
        for (condition, scale, weight) in self._unpack_scales():
            levels.extend(
                [self._as_scale_level(weight, l) for l in scale.get_levels()])
        return sorted(list(set(levels)))


class ForeignKey(BaseField):
    default_storage = {
        'field_type': 'SHORT',
    }

    def __init__(self, name, **kwargs):
        super(ForeignKey, self).__init__(name, **kwargs)
        # TODO: Allow origin_class to be the name of the class instead
        # of the actual class.
        self.origin_class = kwargs.get('origin_class', None)
        self.primary_key = kwargs.get('primary_key', None)
        self.related_name = kwargs.get('related_name', None)
        self.related_manager = kwargs.get('related_manager', True)

    def register(self, workspace, feature_class, field_name, layer_name,
                 layer_fields):
        """
        Register this field with the workspace.
        """

        super(ForeignKey, self).register(
            workspace, feature_class, field_name, layer_name, layer_fields)

        if self.primary_key is None:
            self.primary_key = self.origin_class.fields.oid_field.name

        if self.related_name is None:
            self.related_name = feature_class.__name__.lower() + '_set'

        if self.related_manager:
            setattr(self.origin_class, self.related_name,
                    RelatedManager(self.related_name, feature_class,
                                   self.name, self.primary_key))

        self._set_related(feature_class)

    def _set_related(self, destination_class):
        """
        Set references to related feature classes.
        """

        for feature_class in (self.origin_class, destination_class):
            if feature_class.related_classes is None:
                feature_class.related_classes = {}

        self.origin_class.related_classes[destination_class.__name__] = \
            destination_class
        destination_class.related_classes[self.origin_class.__name__] = \
            self.origin_class

    def __get__(self, instance, owner):
        value = super(ForeignKey, self).__get__(instance, owner)
        if value is None:
            return None

        # Return the prefetched feature if there is one.
        prefetched_feature = instance._prefetch_cache.get(self.name, None)
        if prefetched_feature:
            return prefetched_feature

        # Otherwise, get the related feature from the database.
        return self.origin_class.objects.get({
            self.primary_key: value
        })

    def __set__(self, instance, value):
        # Clear prefetched feature for this relationship.
        if self.name in instance._prefetch_cache:
            del instance._prefetch_cache[self.name]

        # Allow setting using the primary key or the feature itself.
        if isinstance(value, self.origin_class):
            super(ForeignKey, self).__set__(getattr(value, self.primary_key))
        else:
            super(ForeignKey, self).__set__(value)
