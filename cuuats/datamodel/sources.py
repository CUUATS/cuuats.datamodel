import arcpy
import os
from collections import namedtuple


class DataSource(object):
    """
    A data source for manipulating data in a file geodatabase or SDE.
    """

    AttachmentInfo = namedtuple(
        'AttachmentInfo',
        ['origin', 'destination', 'primary_key', 'foreign_key'])

    def __init__(self, path):
        self.path = path
        self.domains = \
            dict([(d.name, d) for d in arcpy.da.ListDomains(self.path)])
        self.editor = arcpy.da.Editor(self.path)

    def get_attachment_info(self, layer_name):
        """
        If this layer has attachments, return a named tuple describing the
        attachment relationship.
        """

        layer = arcpy.Describe(os.path.join(self.path, layer_name))
        for rc_name in layer.relationshipClassNames:
            rc = arcpy.Describe(os.path.join(self.path, rc_name))
            if rc.isAttachmentRelationship:
                origin = rc.originClassNames[0]
                destination = rc.destinationClassNames[0]
                keys = rc.OriginClassKeys
                primary = [k[0] for k in keys if k[1] == 'OriginPrimary'][0]
                foreign = [k[0] for k in keys if k[1] == 'OriginForeign'][0]
                return self.AttachmentInfo(
                    origin, destination, primary, foreign)
        return None

    def get_layer_fields(self, layer_name):
        """
        Returns a dictionary of fields for the given layer.
        """

        layer_path = os.path.join(self.path, layer_name)
        return dict([(f.name, f) for f in arcpy.ListFields(layer_path)])

    def count_rows(self, layer_name, where_clause=None):
        """
        Count the number of rows meeting the given criteria.
        """

        # TODO: Investigate whether using select by attributes would be more
        # efficient.

        return len(list(self.iter_rows(
            layer_name, ['OID@'], where_clause=where_clause)))

    def iter_rows(self, layer_name, field_names, update=False,
                  where_clause=None):
        """
        Iterate over rows of the specified layer.
        """

        layer_path = os.path.join(self.path, layer_name)
        cursor_factory = arcpy.da.SearchCursor

        if update:
            cursor_factory = arcpy.da.UpdateCursor
            self.editor.startEditing(False, False)

        with cursor_factory(layer_path, field_names, where_clause) as cursor:
            for row in cursor:
                yield (row, cursor)

        if update:
            self.editor.stopEditing(True)

    def update_row(self, cursor, values):
        """
        Update the active row in the current cursor with the given values.
        """

        if not isinstance(cursor, arcpy.da.UpdateCursor):
            raise TypeError('Invalid cursor')
        cursor.updateRow(values)

    def get_domain(self, domain_name, domain_type=None):
        """
        Get the named domain if it exists.
        """

        domain = self.domains.get(domain_name, None)

        if domain is None:
            raise NameError('Invalid domain name: %s', (domain_name,))

        if domain_type is not None and domain.domainType != domain_type:
            raise TypeError(
                '%s is not a domain of type %s' % (domain_name, domain_type))

        return domain

    def get_coded_value(self, domain_name, description):
        """
        Get the coded value from a domain using the description.
        """

        domain = self.get_domain(domain_name, 'CodedValue')

        for (code, desc) in domain.codedValues.items():
            if desc == description:
                return code

        raise ValueError('Domain %s has no code for description %s' %
                         (domain_name, description))

    def add_field(self, layer_name, field_name, storage):
        if 'field_type' not in storage:
            raise KeyError('The storage dictionary must contain the '
                           'key field_type.')

        layer_path = os.path.join(self.path, layer_name)
        arcpy.AddField_management(
            layer_path,
            field_name,
            **storage)
