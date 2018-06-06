"""
Classes for working with domains.
"""


class Description(object):
    """
    A description associated with a coded value used in a coded values domain.
    """

    def __init__(self, description):
        if not isinstance(description, basestring):
            raise TypeError('Description must be a string')

        self.description = description

    def __eq__(self, other):
        if isinstance(other, Description):
            return self.description == other.description
        elif isinstance(other, CodedValue):
            return self.description == other.description
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


D = Description


class CodedValue(object):
    """
    A coded value with a description.
    """

    def __new__(cls, value, description):
        if isinstance(value, int):
            obj = int.__new__(IntCodedValue, value)
        elif isinstance(value, long):
            obj = long.__new__(LongCodedValue, value)
        elif isinstance(value, float):
            obj = float.__new__(FloatCodedValue, value)
        elif isinstance(value, str):
            obj = str.__new__(StringCodedValue, value)
        elif isinstance(value, unicode):
            obj = unicode.__new__(UnicodeCodedValue, value)
        else:
            raise TypeError(
                'Value must be of type int, long, float, str, or unicode')
        obj.description = description
        return obj

    @property
    def value(self):
        """
        Returns the value as a primative.
        """

        return type.mro(self.__class__)[2](self)


class IntCodedValue(CodedValue, int):
    """
    An integer coded value.
    """


class LongCodedValue(CodedValue, long):
    """
    A long integer coded value.
    """


class FloatCodedValue(CodedValue, float):
    """
    A floating point coded value.
    """


class StringCodedValue(CodedValue, str):
    """
    A string coded value.
    """


class UnicodeCodedValue(CodedValue, unicode):
    """
    A unicode coded value.
    """
