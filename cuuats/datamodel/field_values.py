class DeferredValue(object):
    """
    A field value that is only retrieved from the database when needed.
    """

    def __init__(self, field_name, db_name):
        self.field_name = field_name
        self.db_name = db_name
