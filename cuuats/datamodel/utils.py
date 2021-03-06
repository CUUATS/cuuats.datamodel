"""
Utility functions and classes.
"""


class Singleton(type):
    """
    Metaclass for creating a singleton class. Source:
    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs)
        return cls._instances[cls]


def batches(items, batch_size):
    for i in xrange(0, len(items), batch_size):
        yield items[i:i + batch_size]
