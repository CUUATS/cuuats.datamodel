from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned


class DeferredValue(object):

    def __init__(self, field_name, db_name):
        self.field_name = field_name
        self.db_name = db_name


class Q(object):

    OPERATORS = {
        'contains': 'LIKE',
        'eq': '=',
        'exact': 'IS',
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
    }

    def __init__(self, filters={}, **kwargs):
        print filters
        if isinstance(filters, basestring):
            self.sql = filters
        elif isinstance(filters, dict):
            all_filters = filters.copy()
            all_filters.update(kwargs)
            self.sql = self._to_sql(all_filters)

    def __and__(self, other):
        return Q('(%s) AND (%s)' % (self.sql, other.sql))

    def __or__(self, other):
        return Q('(%s) OR (%s)' % (self.sql, other.sql))

    def __invert__(self):
        return Q('NOT (%s)' % (self.sql,))

    def __repr__(self):
        return '<Q: %s>' % self.sql

    def _to_sql(self, filters):
        parts = []
        for (field_info, value) in filters.items():
            field_name, sep, op_name = field_info.rpartition('__')
            if op_name not in self.OPERATORS:
                field_name = field_info
                if value is None:
                    op_name = 'exact'
                else:
                    op_name = 'eq'

            operator = self.OPERATORS.get(op_name, None)
            parts.append(' '.join([field_name, operator,
                         self._to_string(value, operator)]))

        return ' AND '.join(parts)

    def _to_string(self, value, operator):
        if value is None:
            return 'NULL'

        elif isinstance(value, basestring):
            if operator == 'LIKE':
                return "'%%%s%%'" % (value,)
            return "'%s'" % (value,)

        # TODO: Deal with dates and other common types.
        return str(value)


class Query(object):

    def __init__(self, fields=[], where=None, order_by=None, group_by=None):
        self.fields = fields
        self.where = where
        self.order_by = order_by
        self.group_by = group_by

    def _clone(self):
        return self.__class__(
            self.fields, self.where, self.order_by, self.group_by)

    def _add_q(self, q):
        if self.where is None:
            self.where = q
        else:
            self.where = self.where & q

    def _set_order(self, fields):
        self.order_by = []
        for field in fields:
            if isinstance(field, basestring):
                self.order_by.append([field, 'ASC'])
            else:
                self.order_by.append(field)


class QuerySet(object):

    def __init__(self, feature_class, query=None):
        self.feature_class = feature_class
        self._field_names =
        self.query = query or self._make_query()
        self._field_name_cache = None
        self._db_name_cache = None
        self._row_cache = []

    def __len__(self):
        return self.count()

    def __iter__(self):
        self._fetch_all()
        for row in self._row_cache:
            return self._feature(row)

    def _make_query(self, feature_class):
        fields = [f.db_name for f in feature_class.get_fields().values()
                  if not f.deferred]
        return Query(fields)

    def _fetch_all(self):
        if self._row_cache is None:
            self._row_cache = list(self.iterator())

    def _clone(self):
        clone = self.__class__(self.feature_class, self.query._clone())
        clone._field_name_cache = self._field_name_cache
        clone._db_name_cache = self._db_name_cache

    @property
    def _field_names(self):
        if self._field_name_cache is None:
            self._field_name_cache = self.feature_class.get_fields().keys()
        return self._field_name_cache

    @property
    def _db_names(self):
        if self._db_name_cache is None:
            self._db_name_cache = [
                f.db_name for f in self.feature_class.get_fields().values()]
        return self._db_name_cache

    def _feature(self, row):
        row_map = dict(zip(self.query.fields, row))
        # FIXME: Need to get DB name for deferred fields
        values = [row_map.get(n, DeferredValue(n)) for n in self._field_names]
        return self.feature_class(**dict(zip(self._field_names, row)))

    # Methods that return QuerySets
    def all(self):
        return self._clone()

    def filter(self, *args, **kwargs):
        clone = self._clone()
        clone.query._add_q(Q(*args, **kwargs))
        return clone

    def exclude(self, *args, **kwargs):
        clone = self._clone()
        clone.query._add_q(~Q(*args, **kwargs))
        return clone

    def order_by(self, fields):
        clone = self._clone()
        clone.query._set_order(fields)
        return clone

    # Methods that do not return QuerySets
    def get(self, *args, **kwargs):
        clone = self._clone()
        clone.query._add_q(Q(*args, **kwargs))
        clone_length = len(clone)
        if clone_length == 1:
            return self._feature(clone._row_cache[0])
        elif clone_length == 0:
            raise ObjectDoesNotExist(clone.query)
        raise MultipleObjectsReturned(clone.query)

    def count(self):
        # TODO: Investigate whether using selection would be faster.
        self._fetch_all()
        return len(self._row_cache)

    def iterator(self):
        for (row, cursor) in self.feature_class.source.iter_rows(
                self.feature_class.name, self._db_names, False,
                self._where_clause, None):
            yield self._feature(row)

    def latest(self, field_name):
        pass

    def earliest(self, field_name):
        pass

    def first(self, field_name):
        pass

    def last(self, field_name):
        pass

    def aggregate(self, *args, **kwargs):
        pass

    def exists(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class Manager(object):

    def __init__(self, queryset_class=QuerySet):
        self.queryset_class = queryset_class

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError('Manager is not accessible '
                                 'from feature instances')

        return self.queryset_class(owner)
