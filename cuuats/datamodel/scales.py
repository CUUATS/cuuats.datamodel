from collections import namedtuple

ScaleLevel = namedtuple('ScaleLevel', ['score', 'label', 'weight'])


class BaseScale(object):
    """
    Base scale used to score raw values.
    """

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the input value.
        """

        raise NotImplemented

    def score(self, value):
        """
        Returns the score for the given value.
        """

        level = self.get_level(value)
        if isinstance(level, ScaleLevel):
            return level.score
        return level


class StaticScale(BaseScale):
    """
    Scale that returns a static score regardless of the value.
    """

    def __init__(self, level):
        self.level = level

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the input value.
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

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the input value.
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

    def get_level(self, value):
        """
        Retuns a ScaleLevel object or a number corresponding to the input value.
        """

        if value in self.levels:
            return self.levels[value]
        return self.default
