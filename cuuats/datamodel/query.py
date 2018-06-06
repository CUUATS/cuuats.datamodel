import itertools
from collections import defaultdict
from cuuats.datamodel.exceptions import ObjectDoesNotExist, \
    MultipleObjectsReturned
from cuuats.datamodel.field_values import DeferredValue
from cuuats.datamodel.domains import D


class SQLCondition(object):

    OPERATORS = {
        'contains': 'LIKE',
        'eq': '=',
        'exact': 'IS',
        'gt': '>',
        'in': 'IN',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
    }

    def __init__(self, field_name, value, op_name=None):
        self.field_name = field_name
        self.value = value

        # Extract or intuit the operator.
        if op_name is None:
            if value is None:
                op_name = 'exact'
            else:
                op_name = 'eq'
        self.operator = self.OPERATORS[op_name]

    def __repr__(self):
        return '<SQLCondition: %s>' % (
            ' '.join([self.field_name, self.operator, str(self.value)]),)


class Q(object):

    def __init__(self, filters={}, **kwargs):
        self.operator = 'AND'
        self.negated = False
        self.rel_name = None
        self.children = []
        self._parse_children(filters.items() + kwargs.items())

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
        result = '%s[%s]: %s' % (self.rel_name or '', self.operator, child_str)
        if self.negated:
            result = 'NOT(%s)' % (result,)
        return '<Q:%s>' % (result,)

    def _parse_children(self, filters):
        for (field_info, value) in filters:
            rels, field_name, op_name = self._parse_field_info(field_info)
            parent = self
            for rel in rels:
                child = self._create()
                child.rel_name = rel
                parent.children.append(child)
                parent = child
            parent.children.append(SQLCondition(field_name, value, op_name))

    def _parse_field_info(self, field_info):
        op_name = None
        parts = field_info.split('__')
        if parts[-1] in SQLCondition.OPERATORS:
            op_name = parts.pop()
        field_name = parts.pop()
        return (parts, field_name, op_name)

    def _create(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def _clone(self, children=True):
        q = self._create()
        q.operator = self.operator
        q.negated = self.negated
        q.rel_name = self.rel_name
        if children:
            q.children = self.children[:]
        return q

    def _combine(self, other, op):
        q = self._create()
        q.children = [self._clone(), other._clone()]
        q.operator = op
        return q

    def _op_match(self, op):
        return op == self.operator or len(self) == 1

    def _can_merge(self, other, op):
        return self._op_match(op) and other._op_match(op) and \
            self.negated == other.negated and self.rel_name == other.rel_name

    def _can_append(self, other, op):
        return self._op_match(op) and not self.negated and \
            (self.rel_name is None or self.rel_name == other.rel_name)

    def _merge(self, other, op):
        q = self._clone()
        q.children.extend(other.children)
        q.operator = op
        return q

    def _append(self, other, op):
        q = self._clone()
        child = other._clone()
        if q.rel_name is not None:
            child.rel_name = None
        q.children.append(child)
        q.operator = op
        return q

    def _merge_or_append(self, other, op):
        if self._can_merge(other, op):
            return self._merge(other, op)
        elif self._can_append(other, op):
            return self._append(other, op)
        elif other._can_append(self, op):
            return other._append(self, op)
        return None

    def simplify(self):
        q = self._clone()
        q.children = [c.simplify() if isinstance(c, Q) else c
                      for c in q.children]

        # Merge/append children with/to each other.
        num_children = 0
        while num_children < len(q.children):
            num_children = len(q.children)
            for (a, b) in itertools.combinations(q.children, 2):
                if not isinstance(a, Q) or not isinstance(b, Q):
                    continue

                new_child = a._merge_or_append(b, q.operator)
                if new_child:
                    q.children = [new_child] + \
                        [c for c in q.children if c not in (a, b)]
                    break

        # Simplify the partent.
        parent_dirty = True
        while parent_dirty:
            parent_dirty = False
            # Merge children with the parent.
            for child in q.children[:]:
                if isinstance(child, Q) and q._can_merge(child, q.operator):
                    q.children = [c for c in q.children if c is not child]
                    q = q._merge(child, q.operator)
                    parent_dirty = True

            # Remove unnecessary parents.
            if len(q.children) == 1 and not q.negated and q.rel_name is None \
                    and isinstance(q.children[0], Q):
                q = q.children[0]
                parent_dirty = True

        return q


class SQLCompiler(object):

    def __init__(self, feature_class=None):
        self.feature_class = feature_class

    def compile(self, q, inner=False):
        q = q.simplify()
        where_parts = []
        feature_class, other_key, self_key = self._resolve_rel(q.rel_name)

        for child in q.children:
            if isinstance(child, Q):
                compiler = self.__class__(feature_class)
                where_parts.append(compiler.compile(child))
            else:
                where_parts.append(
                    self._compile_sql_condition(child, feature_class))

        sep = ' %s ' % (q.operator)
        where = sep.join(where_parts)

        if len(where_parts) > 1 and (q.negated or inner):
            where = '(%s)' % (where,)
        if q.negated:
            where = 'NOT %s' % (where,)
        if self_key is not None:
            where = '%s IN (SELECT %s FROM %s WHERE %s)' % (
                other_key, self_key, feature_class.name, where)

        return where

    def _compile_sql_condition(self, cond, feature_class):
        return ' '.join([
            self._resolve_field_name(cond.field_name, feature_class),
            cond.operator,
            self._to_string(cond.field_name, cond.value, cond.operator)])

    def _to_string(self, field_name, value, operator):
        if value is None:
            return 'NULL'

        elif isinstance(value, D):
            return self._to_string(
                field_name,
                self._resolve_coded_value(field_name, value), operator)

        elif isinstance(value, basestring):
            if operator == 'LIKE':
                return "'%%%s%%'" % (value,)
            return "'%s'" % (value,)

        elif isinstance(value, (list, tuple)):
            return '(%s)' % (', '.join(
                [self._to_string(field_name, v, operator) for v in value]), )

        # TODO: Deal with dates and other common types.
        return str(value)

    def _resolve_coded_value(self, field_name, d):
        field = self.feature_class.fields.get(field_name)
        return self.feature_class.workspace.get_coded_value(
            field.domain_name, d.description)

    def _resolve_field_name(self, field_name, feature_class):
        return feature_class.fields.get_db_name(field_name)

    def _resolve_rel(self, rel_name):
        if rel_name is None:
            return [self.feature_class, None, None]

        relation = self.feature_class.__dict__.get(rel_name)
        if isinstance(relation, RelatedManager):
            return [relation.destination_class,
                    self.feature_class.fields.get_db_name(
                        relation.primary_key),
                    relation.destination_class.fields.get_db_name(
                        relation.foreign_key)]

        # Field is a ForeignKey.
        return [relation.origin_class,
                relation.db_name,
                relation.origin_class.fields.get_db_name(relation.primary_key)]


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
        self._cache = None
        self._prefetch_rel = []
        self._prefetch_deferred = []

    def __len__(self):
        return self.count()

    def __iter__(self):
        self._fetch_all()
        return iter(self._cache)

    def _make_query(self, feature_class):
        fields = [f.db_name for f in feature_class.fields.values()
                  if not f.deferred]
        compiler = SQLCompiler(feature_class)
        query = Query(fields, compiler)
        oid_field = feature_class.fields.oid_field
        if oid_field:
            query.set_order([(oid_field.db_name, 'ASC')])

        return query

    def _make_q(self, *args, **kwargs):
        if args and isinstance(args[0], Q):
            return args[0] & Q(*args[1:], **kwargs)
        return Q(*args, **kwargs)

    def _fetch_all(self):
        if self._cache is None:
            self._cache = list(self.iterator())
            if self._cache:
                self._prefetch()

    def _prefetch(self):
        # Prefetch related features.
        for rel_name in self._prefetch_rel:
            rel = self.feature_class.__dict__.get(rel_name, None)
            if isinstance(rel, RelatedManager):
                self._prefetch_related_manager(rel_name, rel)
            elif rel.__class__.__name__ == 'ForeignKey':
                self._prefetch_foreign_key(rel_name, rel)
            elif rel.__class__.__name__ == 'ManyToManyField':
                self._prefetch_many_to_many(rel_name, rel)
            else:
                raise AttributeError(
                    'Relationship %s does not exist.' % (rel_name,))

    def _prefetch_related_manager(self, rel_name, rel):
        # rel is a RelatedManager.
        destination = rel.destination_class

        pk_filter = '%s__in' % (rel.foreign_key,)
        pks = [getattr(f, rel.primary_key) for f in self._cache]
        dest_features = destination.objects.filter({pk_filter: pks})

        dest_map = defaultdict(list)
        for feature in dest_features:
            dest_map[feature.values.get(rel.foreign_key)].append(feature)

        for feature in self._cache:
            feature._prefetch_cache[rel_name] = dest_map[
                getattr(feature, rel.primary_key)]

    def _prefetch_foreign_key(self, rel_name, rel):
        # rel is a ForeignKey.
        origin = rel.origin_class

        fk_filter = '%s__in' % (rel.primary_key,)
        fks = [f.values.get(rel_name, None) for f in self._cache]
        fks = [fk for fk in fks if fk is not None]
        origin_features = origin.objects.filter({fk_filter: fks})

        origin_map = dict(
            [(getattr(f, rel.primary_key), f) for f in origin_features])

        for feature in self._cache:
            feature._prefetch_cache[rel_name] = origin_map.get(
                feature.values.get(rel_name), None)

    def _prefetch_many_to_many(self, rel_name, rel):
        # rel is a ManyToManyField.
        # - Query rel's relationship class to get instances where the
        #   the foreign key is in the primary keys from this QuerySet's cache.
        pk_filter = "%s__in" % (rel.foreign_key,)
        pks = [getattr(f, rel.primary_key) for f in self._cache]
        relationship_class_features = rel.relationship_class.objects.filter(
            {pk_filter: pks})

        # - Create a unique set of related foreign keys from the instances
        #   of the relationship class.
        # - Query rel's related class to get instances where the primary key is
        #   in the set of related foreign keys.
        pk_filter = "%s__in" % (rel.related_primary_key,)
        pks = [f.values.get(rel.related_foreign_key) for f in
               relationship_class_features]
        related_class_features =\
            rel.related_class.objects.filter({pk_filter: pks})

        # - Create a dictionary mapping foreign key to related foreign key for
        #   the instances of the relationship class.
        relationship_class_dict = defaultdict(list)
        for fc in relationship_class_features:
            relationship_class_dict[fc.values.get(rel.foreign_key)].append(
                fc.values.get(rel.related_foreign_key))

        related_class_dict = dict([(getattr(rc, rel.related_primary_key), rc)
                                   for rc in related_class_features])

        # - Iterate over the objects in this QuerySet's cache, and populate
        #   their prefectch caches by using the dictionary to find the related
        #   class instances that are related to the object.
        for feature in self._cache:
            related_class_pks = relationship_class_dict.get(
                getattr(feature, rel.primary_key)
            )
            feature._prefetch_cache[rel_name] = [
                related_class_dict.get(pk) for pk in related_class_pks]

    def _clone(self, preserve_cache=False):
        clone = self.__class__(self.feature_class, self.query.clone())
        clone._field_name_cache = self._field_name_cache
        clone._db_name_cache = self._db_name_cache
        clone._prefetch_rel = self._prefetch_rel

        if preserve_cache:
            clone._cache = self._cache

        return clone

    @property
    def _field_names(self):
        if self._field_name_cache is None:
            self._field_name_cache = self.feature_class.fields.keys()
        return self._field_name_cache

    @property
    def _db_names(self):
        if self._db_name_cache is None:
            self._db_name_cache = [
                f.db_name for f in self.feature_class.fields.values()]
        return self._db_name_cache

    def _feature(self, row):
        fields = zip(self._field_names, self._db_names)
        row_map = dict(zip(self.query.fields, row))
        values = [row_map.get(d, DeferredValue(f, d)) for (f, d) in fields]
        return self.feature_class(**dict(zip(self._field_names, values)))

    # Methods that return QuerySets
    def all(self):
        return self._clone(preserve_cache=True)

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

    def get_or_create(self, *args, **kwargs):
        try:
            return self.get(*args, **kwargs)
        except ObjectDoesNotExist:
            return self.feature_class(*args, **kwargs)

    def count(self):
        # TODO: Investigate whether using selection would be faster in cases
        # where the results are not already cached.
        if self._cache is not None:
            return len(self._cache)

        return self.feature_class.workspace.count_rows(
            self.feature_class.name,
            self.query.where)

    def iterator(self, limit=None):
        for (row, cursor) in self.feature_class.workspace.iter_rows(
                self.feature_class.name, self.query.fields, False,
                self.query.where, limit, self.query.prefix,
                self.query.postfix):
            yield self._feature(row)

    def first(self):
        if self._cache is not None:
            if self._cache:
                return self._cache[0]
            return None

        results = list(self.iterator(limit=1))
        if results:
            return results[0]
        return None

    def last(self):
        if self._cache is not None:
            if self._cache:
                return self._cache[-1]
            return None

        clone = self._clone()
        clone.query.reverse_order()
        return clone.first()

    def summarize(self, summary_field_name, **kwargs):
        summary_field = self.feature_class.fields.get(summary_field_name, None)
        if not summary_field:
            raise KeyError('Invalid summary field name')

        levels = summary_field.get_levels()
        results = {}

        for level in levels:
            level_key = hash(level)
            results[level_key] = {}
            results[level_key].update(level.meta)
            results[level_key].update(
                dict(zip(kwargs.keys(), [0]*len(kwargs))))
            results[level_key]['count'] = 0
            results[level_key]['value'] = level.value
            results[level_key]['label'] = level.label

        for feature in self:
            level = summary_field.summarize(feature)
            level_key = hash(level)

            results[level_key]['count'] += 1
            for value_key, value_expr in kwargs.items():
                results[level_key][value_key] += feature.eval(value_expr)

        return [results[hash(l)] for l in levels]

    def aggregate(self, fields):
        return self.feature_class.workspace.summarize(
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

    def prefetch_related(self, *rels):
        for rel in rels:
            if rel not in self._prefetch_rel:
                self._prefetch_rel.append(rel)
        return self

    def prefetch_deferred(self, *field_names):
        for field_name in field_names:
            field = self.feature_class.fields.get(field_name, None)
            if field is None:
                raise AttributeError('%s does not have field "%s"' % (
                    self.feature_class.__name__, field_name))
            if field.db_name not in self.query.fields:
                self.query.fields.append(field.db_name)
        return self


class Manager(object):

    def __init__(self, queryset_class=QuerySet):
        self.queryset_class = queryset_class

    def __get__(self, instance, owner):
        if instance is not None:
            raise AttributeError('Manager is not accessible '
                                 'from feature instances')

        if owner.workspace is None:
            raise AttributeError(
                'Feature class %s must be registered '
                'before it can be queried' % (owner.__name__,))

        return self.queryset_class(owner)


class RelatedManager(Manager):

    def __init__(self, name, destination_class, foreign_key, primary_key,
                 queryset_class=QuerySet):
        super(RelatedManager, self).__init__(queryset_class)
        self.name = name
        self.destination_class = destination_class
        self.foreign_key = foreign_key
        self.primary_key = primary_key

    def __get__(self, instance, owner):
        if instance is None:
            raise AttributeError('Related Manager is only accessible '
                                 'from feature instances')

        if self.destination_class.workspace is None:
            raise AttributeError(
                'Related class must be registered before '
                'it can be queried')

        qs = self.queryset_class(self.destination_class).filter({
            self.foreign_key: getattr(instance, self.primary_key)
        })

        # If we have prefetched related features, populate the QuerySet cache.
        qs._cache = instance._prefetch_cache.get(self.name, None)

        return qs
