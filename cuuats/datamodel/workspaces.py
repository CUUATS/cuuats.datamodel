import arcpy
import logging
import os
import re
from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from time import time
from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned
from cuuats.datamodel.utils import Singleton


class WorkspaceManager(object):
    """
    Workspace manager for manipulating workspaces.
    """

    __metaclass__ = Singleton

    def __init__(self):
        self.workspaces = {}

    def get(self, path):
        """
        Get a workspace corresponding to the given path.
        """

        if path not in self.workspaces:
            self.workspaces[path] = Workspace(path)
        return self.workspaces[path]

    def clear(self):
        """
        Clear all cached workspaces.
        """

        self.workspaces = {}


class Workspace(object):
    """
    A workspace representing a file geodatabase or SDE.
    """

    RelationshipInfo = namedtuple(
        'RelationshipInfo', ['origin', 'destination', 'primary_key',
                             'foreign_key', 'is_attachment'])

    def __init__(self, path):
        self.path = path
        self.domains = \
            dict([(d.name, d) for d in arcpy.da.ListDomains(self.path)])
        self.editor = arcpy.da.Editor(self.path)

    def list_relationships(self, layer_name):
        """
        List the relationships that this feature class participates in.
        Relationship information is returned as a RelationshipInfo tuple.
        """

        layer = arcpy.Describe(os.path.join(self.path, layer_name))
        if not hasattr(layer, "relationshipClassNames"):
            return([])

        return [self.get_relationship_info(rc_name)
                for rc_name in layer.relationshipClassNames]

    def get_relationship_info(self, rc_name):
        """
        Extract the origin, destination, primary and foreign keys from a
        RelationshipClass Describe result or the name of the relationship
        class.
        """

        rc = arcpy.Describe(os.path.join(self.path, rc_name))
        origin = rc.originClassNames[0]
        destination = rc.destinationClassNames[0]
        keys = rc.OriginClassKeys
        primary = [k[0] for k in keys if k[1] == 'OriginPrimary'][0]
        foreign = [k[0] for k in keys if k[1] == 'OriginForeign'][0]
        is_attachment = rc.isAttachmentRelationship
        return self.RelationshipInfo(
            origin, destination, primary, foreign, is_attachment)

    def get_attachment_info(self, layer_name):
        """
        If this layer has attachments, return a named tuple describing the
        attachment relationship.
        """

        for rc_info in self.list_relationships(layer_name):
            if rc_info.is_attachment:
                return rc_info
        return None

    def get_layer_fields(self, layer_name):
        """
        Returns a dictionary of fields for the given layer.
        """

        layer_path = os.path.join(self.path, layer_name)
        return OrderedDict([(f.name, f) for f in arcpy.ListFields(layer_path)])

    def count_rows(self, layer_name, where_clause=None):
        """
        Count the number of rows meeting the given criteria.
        """

        # TODO: Investigate whether using select by attributes would be more
        # efficient.

        return len(list(self.iter_rows(
            layer_name, ['OID@'], where_clause=where_clause)))

    def get_row(self, layer_name, field_names, where_clause=None):
        """
        Retrieve a row from the specified feature class.
        """

        rows_cursors = self.iter_rows(
            layer_name, field_names, where_clause=where_clause, limit=2)
        rows = [r for (r, c) in rows_cursors]

        if len(rows) == 1:
            return rows[0]
        elif len(rows) == 0:
            raise ObjectDoesNotExist(where_clause)
        raise MultipleObjectsReturned(where_clause)

    def iter_rows(self, layer_name, field_names, update=False,
                  where_clause=None, limit=None, prefix=None, postfix=None):
        """
        Iterate over rows of the specified layer.
        """

        layer_path = os.path.join(self.path, layer_name)
        cursor_factory = arcpy.da.SearchCursor

        if update:
            cursor_factory = arcpy.da.UpdateCursor

        logging.debug(
            '{cursor}: SELECT {prefix}{fields} FROM '
            '{table}{where}{postfix}'.format(
                cursor=cursor_factory.__name__,
                prefix=prefix + ' ' if prefix else '',
                fields=', '.join(field_names),
                table=layer_name,
                where=' WHERE ' + where_clause if where_clause else '',
                postfix=' ' + postfix if postfix else ''))

        with cursor_factory(layer_path, field_names, where_clause,
                            sql_clause=(prefix, postfix)) as cursor:
            for row in cursor:

                # Load memoryview data into memory so that we don't lose access
                # to it in the next iteration.
                # TODO: Is there a better way to deal with this issue?
                values = [v.tobytes() if isinstance(v, memoryview) else v
                          for v in row]

                if limit is None or limit > 0:
                    yield (values, cursor)
                    if limit is not None:
                        limit -= 1
                else:
                    break

    def update_row(self, cursor, values):
        """
        Update the active row in the current cursor with the given values.
        """

        if not isinstance(cursor, arcpy.da.UpdateCursor):
            raise TypeError('Invalid cursor')
        cursor.updateRow(values)

    def insert_row(self, layer_name, field_names, values):
        """
        Insert a row in the table.
        """

        layer_path = os.path.join(self.path, layer_name)
        self.editor.startEditing(False, False)

        with arcpy.da.InsertCursor(layer_path, field_names) as cursor:
            oid = cursor.insertRow(values)

        self.editor.stopEditing(True)
        return oid

    @contextmanager
    def edit(self, versioned=True):
        self.editor.startEditing(False, versioned)
        if versioned:
            self.editor.startOperation()
        try:
            yield
            if versioned:
                self.editor.stopOperation()
            self.editor.stopEditing(True)
        except Exception, e:
            if versioned:
                self.editor.abortOperation()
            self.editor.stopEditing(False)
            raise e

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
        """
        Add a field to a feature class.
        """

        if 'field_type' not in storage:
            raise KeyError('The storage dictionary must contain the '
                           'key field_type.')

        layer_path = os.path.join(self.path, layer_name)
        arcpy.AddField_management(
            layer_path,
            field_name,
            **storage)

    def _make_layer_name(self):
        """
        Returns a unique layer name.
        """

        return 'layer_%s' % (str(time()).replace('.', ''),)

    @contextmanager
    def make_layer(self, fc_name, where_clause=None):
        """
        Temporarily make a feature class into a layer so that it can be
        used with geoprocessing tools that require a layer.
        """

        layer_name = self._make_layer_name()
        fc_path = os.path.join(self.path, fc_name)

        arcpy.MakeFeatureLayer_management(fc_path, layer_name)
        yield layer_name

        arcpy.Delete_management(layer_name)

    def summarize(self, fc_name, fields, where_clause=None):
        """
        Generate summary statistics for a feature class.
        """

        summary_name = self._make_layer_name()
        summary_path = 'in_memory/%s' % (summary_name,)

        with self.make_layer(fc_name) as layer_name:
            if where_clause:
                arcpy.SelectLayerByAttribute_management(
                    layer_name, where_clause=where_clause)

            arcpy.Statistics_analysis(
                layer_name, summary_path, fields)

        summary_fields = [re.sub(r'[^A-Za-z0-9]', '_', '%s_%s' % (s, f))
                          for (f, s) in fields]

        for row in arcpy.da.SearchCursor(summary_path, summary_fields):
            return dict(zip(summary_fields, row))

    def update_spatial_relationship(self, rc_name, rel_type='INTERSECT',
                                    search_radius=None):
        """
        Assign or update values for a foreign key based on the spatial
        relationship of the features.
        """

        # TODO: Move this method to a separate class dedicated to dealing
        # with spatial relationships.

        rc_info = self.get_relationship_info(rc_name)
        dest_path = os.path.join(self.path, rc_info.destination)
        origin_path = os.path.join(self.path, rc_info.origin)

        # Create field mappings. We really don't want any fields
        # included, so we add the origin primary key, which will
        # be removed automatically.
        field_mappings = arcpy.FieldMappings()
        field_map = arcpy.FieldMap()
        field_map.addInputField(origin_path, rc_info.primary_key)
        field_mappings.addFieldMap(field_map)

        # Execute the spatial join.
        join_name = self._make_layer_name()
        join_path = 'in_memory/%s' % (join_name,)
        arcpy.SpatialJoin_analysis(
            dest_path, origin_path, join_path, 'JOIN_ONE_TO_MANY',
            'KEEP_COMMON', field_mappings, rel_type, search_radius)

        # Create a mapping: destination -> origin.
        with arcpy.da.SearchCursor(
                join_path, ['TARGET_FID', 'JOIN_FID']) as cursor:
            oid_map = dict([(str(row[0]), row[1]) for row in cursor])

        # Delete the join layer.
        arcpy.Delete_management(join_path)
        if not oid_map:
            return

        # Update the forien key.
        for (row, cursor) in self.iter_rows(
                rc_info.destination, ['OID@', rc_info.foreign_key], True):
            oid, fk = row
            new_fk = oid_map.get(str(oid), None)
            if fk != new_fk:
                self.update_row(cursor, [oid, new_fk])
