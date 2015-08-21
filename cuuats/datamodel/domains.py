"""
Classes for working with domains.
"""


class Description(object):
    """
    A description associated with a coded value used in a coded values domain.
    """

    def __init__(self, description):
        self.description = description

    def __eq__(self, other):
        if isinstance(other, basestring):
            return self.description == other
        else:
            return self.description == getattr(other, '_description', None)


D = Description
