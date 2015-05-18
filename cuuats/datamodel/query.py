from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned
from cuuats.datamodel.fields import DeferredValue


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
        if isinstance(filters, Q):
            self.sql = filters.sql
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

    def __init__(self, fields=[]):
        self.fields = fields
        self._where = None
        self._order_by = None
        self._group_by = None

    def clone(self):
        clone = self.__class__(self.fields)
        clone._where = self._where
        clone._order_by = self._order_by
        clone._group_by = self._group_by
        return clone

    def add_q(self, q):
        if self._where is None:
            self._where = q
        else:
            self._where = self._where & q

    def set_order(self, fields):
        self._order_by = []
        for field in fields:
            if isinstance(field, basestring):
                self.order_by.append([field, 'ASC'])
            else:
                self.order_by.append(field)

    @property
    def where(self):
        if self._where is None:
            return None
        return self._where.sql

    @property
    def prefix(self):
            return None

    @property
    def postfix(self):
        if not self._order_by:
            return None

        return 'ORDER BY %s' % (
            ', '.join(['%s %s' % (c, o) for (c, o) in self._order_by]))


class QuerySet(object):

    def __init__(self, feature_class, query=None):
        self.feature_class = feature_class
        self.query = query or self._make_query(feature_class)
        self._field_name_cache = None
        self._db_name_cache = None
        self._cache = []

    def __len__(self):
        return self.count()

    def __iter__(self):
        self._fetch_all()
        return iter(self._cache)

    def _make_query(self, feature_class):
        fields = [f.db_name for f in feature_class.get_fields().values()
                  if not f.deferred]
        return Query(fields)

    def _fetch_all(self):
        if not self._cache:
            self._cache = list(self.iterator())

    def _clone(self):
        clone = self.__class__(self.feature_class, self.query.clone())
        clone._field_name_cache = self._field_name_cache
        clone._db_name_cache = self._db_name_cache
        return clone

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
        fields = zip(self._field_names, self._db_names)
        row_map = dict(zip(self.query.fields, row))
        values = [row_map.get(d, DeferredValue(f, d)) for (f, d) in fields]
        return self.feature_class(**dict(zip(self._field_names, values)))

    # Methods that return QuerySets
    def all(self):
        return self._clone()

    def filter(self, *args, **kwargs):
        clone = self._clone()
        clone.query.add_q(Q(*args, **kwargs))
        return clone

    def exclude(self, *args, **kwargs):
        clone = self._clone()
        clone.query.add_q(~Q(*args, **kwargs))
        return clone

    def order_by(self, fields):
        clone = self._clone()
        clone.query.set_order(fields)
        return clone

    # Methods that do not return QuerySets
    def get(self, *args, **kwargs):
        clone = self._clone()
        clone.query.add_q(Q(*args, **kwargs))
        clone_length = len(clone)
        if clone_length == 1:
            return clone._cache[0]
        elif clone_length == 0:
            raise ObjectDoesNotExist(clone.query.where)
        raise MultipleObjectsReturned(clone.query.where)

    def count(self):
        # TODO: Investigate whether using selection would be faster in cases
        # where the results are not already cached.
        if self._cache:
            return len(self._cache)

        return self.feature_class.source.count_rows(
            self.feature_class.name,
            self.query.where)

    def iterator(self):
        for (row, cursor) in self.feature_class.source.iter_rows(
                self.feature_class.name, self._db_names, False,
                self.query.where, None):
            yield self._feature(row)

    def latest(self, field_name):
        pass

    def earliest(self, field_name):
        pass

    def first(self, field_name):
        pass

    def last(self, field_name):
        pass

    def aggregate(self, fields):
        return self.feature_class.source.summarize(
            self.feature_class.name,
            fields,
            self.query.where)

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
