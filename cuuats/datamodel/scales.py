class ScaleLevel(object):
    """
    A value level used in scales.
    """

    def __init__(self, value, label, weight=0, **kwargs):
        self.value = value
        self.label = label
        self.weight = weight
        self.meta = kwargs

    def _as_tuple(self):
        return (self.weight, self.value, self.label)

    def __cmp__(self, other):
        return cmp(self._as_tuple(), other._as_tuple())

    def __hash__(self):
        return hash(self._as_tuple())

    def wrap(self, weight):
        return ScaleLevel(
            self.value, self.label, (weight, self.weight), **self.meta)


class BaseScale(object):
    """
    Base scale used to score raw values.
    """

    def get_levels(self):
        """
        Returns a list of all possible scale levels.
        """

        raise NotImplemented

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the
        input value.
        """

        raise NotImplemented

    def score(self, value):
        """
        Returns the score for the given value.
        """

        level = self.get_level(value)
        if isinstance(level, ScaleLevel):
            return level.value
        return level


class StaticScale(BaseScale):
    """
    Scale that returns a static score regardless of the value.
    """

    def __init__(self, level):
        self.level = level

    def get_levels(self):
        """
        Returns a list of all possible scale levels.
        """

        return [self.level]

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the
        input value.
        """

        return self.level


class BreaksScale(BaseScale):
    """
    A scale with predefined breaks.
    """

    def __init__(self, breaks, levels, right=True):
        if not breaks == sorted(breaks):
            raise ValueError('Breaks must be provided in increasing order')

        if len(breaks) + 1 != len(levels):
            raise IndexError('The number of levels must be one greater than '
                             'the number of breaks')

        self.breaks = breaks
        self.levels = levels
        self.right = right

    def get_levels(self):
        """
        Returns a list of all possible scale levels.
        """

        return self.levels

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the
        input value.
        """

        breaks = self.breaks + [float('Inf')]
        for (break_value, level) in zip(breaks, self.levels):
            if (value < break_value) or (self.right and value == break_value):
                return level


class DictScale(BaseScale):
    """
    A scale based on a levels dictionary.
    """

    def __init__(self, levels, default=0):
        self.levels = levels
        self.default = default

    def get_levels(self):
        """
        Returns a list of all possible scale levels.
        """

        return self.levels.values()

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the
        input value.
        """

        if value in self.levels:
            return self.levels[value]
        return self.default
