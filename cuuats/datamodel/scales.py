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
