from collections import defaultdict
from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned
from cuuats.datamodel.field_values import DeferredValue


class SQLCondition(object):

    OPERATORS = {
        'contains': 'LIKE',
        'eq': '=',
        'exact': 'IS',
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
    }

    def __init__(self, field_info, value):
        self.value = value
        self.rel_name = None
        self.field_name, sep, op_name = field_info.rpartition('__')
        if op_name not in self.OPERATORS:
            self.field_name = field_info
            if value is None:
                op_name = 'exact'
            else:
                op_name = 'eq'

        self.operator = self.OPERATORS.get(op_name, None)

        if '__' in self.field_name:
            self.rel_name, sep, self.field_name = field_info.rpartition('__')


class Q(object):

    def __init__(self, filters={}, **kwargs):
        self.operator = 'AND'
        self.negated = False
        self.children = [
            SQLCondition(*f) for f in filters.items() + kwargs.items()]

    def __and__(self, other):
        return self._combine(other, 'AND')

    def __or__(self, other):
        return self._combine(other, 'OR')

    def __invert__(self):
        q = self._clone()
        q.negated = not self.negated
        return q

    def __len__(self):
        return len(self.children)

    def __repr__(self):
        child_str = ', '.join([str(c) for c in self.children])
        return '<Q: %s(%s: %s)>' % (
            self.negated and 'NOT ' or '', self.operator, child_str)

    def _clone(self):
        q = self.__class__()
        q.operator = self.operator
        q.negated = self.negated
        q.children = self.children[:]
        return q

    def _rels(self):
        return set([f.rel_name for f in self.children if
                    isinstance(f, SQLCondition) and f.rel_name is not None])

    def _op_match(self, op):
        return op == self.operator or len(self) == 1

    def _can_merge(self, other, op):
        # Don't merge Q objects that traverse the same relationships.
        # TODO: This determintation should be tied to which QuerySet.filter()
        # call the Q objects came from.
        if self._rels() & other._rels():
            return False
        return self._op_match(op) and other._op_match(op) and \
            self.negated == other.negated

    def _can_append(self, other, op):
        return self._op_match(op) and not self.negated

    def _combine(self, other, op):
        if self._can_merge(other, op):
            q = self._clone()
            q.children.extend(other.children)
        elif self._can_append(other, op):
            q = self._clone()
            q.children.append(other._clone())
        elif other._can_append(self, op):
            q = other._clone()
            q.children.append(self._clone())
        else:
            q = self.__class__()
            q.children = [self._clone(), other._clone()]
        q.operator = op
        return q


class SQLCompiler(object):

    def __init__(self, feature_class=None):
        self.feature_class = feature_class

    def compile(self, q, inner=False):
        sep = ' %s ' % (q.operator)
        subquery_map = defaultdict(list)
        sql_parts = []

        for child in q.children:
            if isinstance(child, Q):
                sql_parts.append(self.compile(child))
            else:
                value_str = self._to_string(child.value, child.operator)
                if child.rel_name is None:
                    sql_parts.append(' '.join([child.field_name,
                                               child.operator, value_str]))
                else:
                    subquery_map[child.rel_name].append(
                        (child.field_name, child.operator, value_str))

        # Compile subqueries
        sql_parts.extend([self._subquery(rel, fields, sep)
                          for (rel, fields) in subquery_map.items()])

        sql = sep.join(sql_parts)
        if len(sql_parts) > 1 and (q.negated or inner):
            sql = '(%s)' % (sql,)
        if q.negated:
            sql = 'NOT %s' % (sql,)
        return sql

    def _to_string(self, value, operator):
        if value is None:
            return 'NULL'

        elif isinstance(value, basestring):
            if operator == 'LIKE':
                return "'%%%s%%'" % (value,)
            return "'%s'" % (value,)

        # TODO: Deal with dates and other common types.
        return str(value)

    def _resolve_rel(self, rel_name):
        field = self.feature_class.__dict__.get(rel_name)
        if isinstance(field, RelatedManager):
            destination = field.destination_class
            return [self.feature_class.oid_field_name, field.foreign_key,
                    destination.name]
        # Field is a ForeignKey.
        origin = field.origin_class
        return [field.db_name, origin.oid_field_name, origin.name]

    def _subquery(self, rel_name, fields, sep):
        where_clause = sep.join([' '.join(f) for f in fields])
        return '%s IN (SELECT %s FROM %s WHERE %s)' % \
            tuple(self._resolve_rel(rel_name) + [where_clause])


class Query(object):

    def __init__(self, fields, compiler):
        self.fields = fields
        self.compiler = compiler
        self._where = None
        self._order_by = None
        self._group_by = None

    def clone(self):
        clone = self.__class__(self.fields, self.compiler)
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
                self._order_by.append([field, 'ASC'])
            else:
                self._order_by.append(field)

    def reverse_order(self):
        new_order = []
        for (field, direction) in self._order_by:
            if direction == 'ASC':
                new_order.append([field, 'DESC'])
            else:
                new_order.append([field, 'ASC'])
        self._order_by = new_order

    @property
    def where(self):
        if self._where is None:
            return None
        return self.compiler.compile(self._where)

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
        compiler = SQLCompiler(feature_class)
        query = Query(fields, compiler)
        query.set_order([(feature_class.oid_field_name, 'ASC')])
        return query

    def _make_q(self, *args, **kwargs):
        if args and isinstance(args[0], Q):
            return args[0] & Q(*args[1:], **kwargs)
        return Q(*args, **kwargs)

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
        clone.query.add_q(self._make_q(*args, **kwargs))
        return clone

    def exclude(self, *args, **kwargs):
        clone = self._clone()
        clone.query.add_q(~self._make_q(*args, **kwargs))
        return clone

    def order_by(self, fields):
        clone = self._clone()
        clone.query.set_order(fields)
        return clone

    # Methods that do not return QuerySets
    def get(self, *args, **kwargs):
        clone = self._clone()
        clone.query.add_q(self._make_q(*args, **kwargs))
        clone._fetch_all()
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

    def iterator(self, limit=None):
        for (row, cursor) in self.feature_class.source.iter_rows(
                self.feature_class.name, self._db_names, False,
                self.query.where, limit, self.query.prefix,
                self.query.postfix):
            yield self._feature(row)

    def first(self):
        if self._cache:
            return self._cache[0]

        results = list(self.iterator(limit=1))
        if results:
            return results[0]
        return None

    def last(self):
        if self._cache:
            return self._cache[-1]

        clone = self._clone()
        clone.query.reverse_order()
        return clone.first()

    def aggregate(self, fields):
        return self.feature_class.source.summarize(
            self.feature_class.name,
            fields,
            self.query.where)

    def exists(self):
        if self._cache:
            return True
        return self.first() is not None

    def update(self):
        raise NotImplementedError('QuerySet updates are not yet supported')

    def delete(self):
        raise NotImplementedError('QuerySet deletions are not yet supported')


class Manager(object):

    def __init__(self, queryset_class=QuerySet):
        self.queryset_class = queryset_class

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError('Manager is not accessible '
                                 'from feature instances')

        if owner.source is None:
            raise AttributeError(
                'Feature class %s must be registered with a data source '
                'before it can be queried' % (owner.__name__,))

        return self.queryset_class(owner)


class RelatedManager(Manager):

    def __init__(self, destination_class, foreign_key,
                 queryset_class=QuerySet):
        super(RelatedManager, self).__init__(queryset_class)
        self.destination_class = destination_class
        self.foreign_key = foreign_key

    def __get__(self, instance, owner):
        if instance is None:
            raise AttributeError('Related Manager is only accessible '
                                 'from feature instances')

        if self.destination_class.source is None:
            raise AttributeError(
                'Related class must be registered with a data source before '
                'it can be queried')

        return self.queryset_class(self.destination_class).filter({
            self.foreign_key: instance.oid
        })
