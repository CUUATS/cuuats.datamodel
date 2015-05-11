import warnings
from cuuats.datamodel.scales import BaseScale


class BaseField(object):

    creation_index = 0
    default_storage = {}

    def __init__(self, label, **kwargs):
        self.label = label
        self.name = kwargs.get('name', None)
        self.db_name = kwargs.get('db_name', None)
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
        return instance.values.get(self.name, None)

    def __set__(self, instance, value):
        instance.values[self.name] = value

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self.label)

    def register(self, source, field_name, layer_name, layer_fields):
        """
        Register this field with the data source.
        """

        layer_field = layer_fields.get(field_name, None)
        if layer_field is None:
            warnings.warn('%s is not a field of %s' % (
                field_name, layer_name))
        else:
            self.name = field_name
            if self.db_name is None:
                self.db_name = field_name
            if layer_field.domain:
                self.domain_name = layer_field.domain
                domain = source.get_domain(layer_field.domain)
                if domain.domainType == 'CodedValue':
                    self.choices.extend(domain.codedValues)
                elif hasattr(self, 'min') and hasattr(self, 'max'):
                    self.min, self.max = domain.range

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


class CalculatedField(BaseField):

    def __init__(self, name, **kwargs):
        super(CalculatedField, self).__init__(name, **kwargs)

        # Overridden by subclasses
        self.condition = kwargs.get('condition', None)
        self.default = kwargs.get('default', None)

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
        self.scale = kwargs.get('scale')
        self.value_field = kwargs.get('value_field')
        self.use_description = kwargs.get('use_description', False)

        if not isinstance(self.scale, (BaseScale, list, tuple)):
            raise TypeError('Scale must be a subclass of BaseScale or a '
                            'list containing tuples of conditions and scales')

    def _get_scale_for(self, instance):
        if isinstance(self.scale, BaseScale):
            return self.scale

        for (condition, scale) in self.scale:
            if instance.check_condition(condition):
                return scale

        return None

    def calculate(self, instance):
        """
        Calculate the value for this field based on the state of the instance.
        """

        if self.use_description:
            value = instance.get_description_for(self.value_field)
        else:
            value = instance.eval(self.value_field)

        scale = self._get_scale_for(instance)
        if not scale:
            return self.default

        return scale.score(value)


class BatchField(BaseField):

    def __init__(self, name, **kwargs):
        super(BatchField, self).__init__(name, **kwargs)

        # Overridden by subclasses
        self.default = kwargs.get('default', None)

    def __set__(self, instance, value):
        raise ValueError('Batch fields cannot be set')

    def update(self, cls):
        """
        Update the field values for this feature class.
        """

        # Overridden by subclasses
        return self.default


class RelationshipSummaryField(BatchField):

    def __init__(self, name, **kwargs):
        super(RelationshipSummaryField, self).__init__(name, **kwargs)

        # Overridden by subclasses
        self.rel = kwargs.get('relationship', None)
        self.summary_field = kwargs.get('summary_field', None)
        self.statistic = kwargs.get('statistic', None)
        self.where_clause = kwargs.get('where_clause', None)

    def update(self, cls):
        """
        Update the field values for this feature class.
        """

        cls.source.update_relationship_statistics(
            self.rel,
            {self.name: [self.summary_field, self.statistic]},
            self.where_clause,
            self.default)
