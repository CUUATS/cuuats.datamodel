import os


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

    @property
    def has_data(self):
        """
        Does this attachment have data?
        """

        return self.data is not None

    def save_to(self, path, filename=None, overwrite=False, data=None):
        """
        Save the attached file to the specified directory path.
        """

        if not self.has_data and data is None:
            raise ValueError('Cannot save an attachment with no data')

        if data is None:
            data = self.data.tobytes()

        file_path = os.path.join(path, filename or self.file_name)
        if os.path.isfile(file_path) and not overwrite:
            raise IOError('%s already exists' % (file_path,))

        with open(file_path, 'wb') as file:
            file.write(data)


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

    @property
    def _where_clause(self):
        """
        Generate the where clause used to select attachments for this feature.
        """

        id_value = getattr(self.feature, self.rel.primary_key)
        if isinstance(id_value, basestring):
            id_value = "'%s'" % (id_value,)
        return '%s = %s' % (self.rel.foreign_key, str(id_value))

    def iter(self, update=False, get_data=True):
        """
        Iterate over the attachments for the feature.
        """

        if update:
            raise NotImplementedError(
                'Attachemnt updating is not yet implemented')

        fields = ['ATTACHMENTID', 'ATT_NAME', 'CONTENT_TYPE', 'DATA_SIZE']
        if get_data:
            fields += ['DATA']

        for (row, cursor) in self.feature.source.iter_rows(
                self.rel.destination, fields, update, self._where_clause):
            yield Attachment(*row)

    def count(self):
        """
        Get the number of attachments for the feature.
        """

        return self.feature.source.count_rows(
            self.rel.destination, self._where_clause)
