from cuuats.datamodel.scales import BaseScale


class BaseField(object):

    creation_index = 0

    def __init__(self, label, **kwargs):
        self.label = label
        self.name = kwargs.get('name', False)
        self.required = kwargs.get('required', False)
        self.required_if = kwargs.get('required_if', None)
        self.domain_name = kwargs.get('domain_name', None)
        self.choices = list(kwargs.get('choices', []))

        # Handle field ordering
        self.order = kwargs.get('order', 0)
        self.creation_index = BaseField.creation_index
        BaseField.creation_index += 1

    def __get__(self, instance, owner):
        return instance.values.get(self.name, None)

    def __set__(self, instance, value):
        instance.values[self.name] = value
        instance._field_value_changed(self.name)

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self.label)

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

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # TODO: Add validation for geometries.
        return super(GeometryField, self).validate(value)


class StringField(BaseField):

    def validate(self, value):
        """
        Performs validation on the given value, and returns any error messages.
        """

        # Convert empty strings to None before performing validation.
        if isinstance(value, str) and value.strip() == '':
            value = None
        return super(StringField, self).validate(value)


class NumericField(BaseField):

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
        self.update_for = []

    def __set__(self, instance, value):
        raise ValueError('Calculated fields cannot be set')

    def update(self, instance):
        """
        Update the value of the calculated field for the given instance.
        """

        # Overridden by subclasses
        pass


class ScaleField(CalculatedField):

    def __init__(self, name, **kwargs):
        super(ScaleField, self).__init__(name, **kwargs)
        self.scale = kwargs.get('scale', None)
        self.value_field = kwargs.get('value_field', None)
        self.update_for = [self.value_field]

        if not isinstance(self.scale, BaseScale):
            raise TypeError('Scale must be a subclass of BaseScale')

    def update(self, instance):
        """
        Update the value of the calculated field for the given instance.
        """

        value = getattr(instance, self.value_field)
        instance.values[self.name] = self.scale.score(value)
