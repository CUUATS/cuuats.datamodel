
class Attachment(object):
    """
    A file attached to an ArcGIS feature class.
    """

    def __init__(self, attachment_id, file_name, content_type,
                 file_size, data=None):
        self.attachment_id = attachment_id
        self.file_name = file_name
        self.content_type = content_type
        self.file_size = file_size
        self.data = data


class AttachmentRelationship(object):
    """
    An attachemnt relationship.
    """

    def __init__(self, origin, destination, primary, foreign):
        self.origin = origin
        self.destination = destination
        self.primary_key = primary
        self.foreign_key = foreign

    def __get__(self, instance, owner):
        """
        Return an attachment manager for this feature instance.
        """

        return AttachmentManager(self, instance)


class AttachmentManager(object):
    """
    Helper class to manage file attachments for a feature.
    """

    def __init__(self, relationship, feature):
        self.rel = relationship
        self.feature = feature

    def iter(self):
        raise NotImplementedError(
            'Attachemnt iteration is not yet implemented')

    def count(self):
        id_value = getattr(self.feature, self.rel.primary_key)
        if isinstance(id_value, basestring):
            id_value = "'%s'" % (id_value,)
        where_clause = '%s = %s' % (self.rel.foreign_key, str(id_value))
        return self.feature.source.count_rows(
            self.rel.destination, where_clause)
