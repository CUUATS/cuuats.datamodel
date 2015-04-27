class BaseScale(object):
    """
    Base scale used to score raw values.
    """

    def score(self, value):
        """
        Returns the score for the given value.
        """

        # Overriden by subclasses
        return value


class StaticScale(BaseScale):
    """
    Scale that returns a static score regardless of the value.
    """

    def __init__(self, score_value):
        self.score_value = score_value

    def score(self, value):
        """
        Returns the score for the given value.
        """

        return self.score_value


class BreaksScale(BaseScale):
    """
    A scale with predefined breaks.
    """

    def __init__(self, breaks, scores, right=True):
        if not breaks == sorted(breaks):
            raise ValueError('Breaks must be provided in increasing order')

        if len(breaks) + 1 != len(scores):
            raise IndexError('The number of scores must be one greater than '
                             'the number of breaks')

        self.breaks = breaks
        self.scores = scores
        self.right = right

    def score(self, value):
        """
        Returns the score for the given value.
        """

        breaks = self.breaks + [float('Inf')]
        for (break_value, score) in zip(breaks, self.scores):
            if (value < break_value) or (self.right and value == break_value):
                return score


class DictScale(BaseScale):
    """
    A scale based on a scores dictionary.
    """

    def __init__(self, scores, default=0):
        self.scores = scores
        self.default = default

    def score(self, value):
        """
        Returns the score for the given value.
        """

        if value in self.scores:
            return self.scores[value]
        return self.default
