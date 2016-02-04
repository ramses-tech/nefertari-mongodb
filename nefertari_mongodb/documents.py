import copy
import logging

import six
import mongoengine as mongo

from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from nefertari.utils import (
    process_fields, process_limit, _split, dictset, drop_reserved_params)
from .metaclasses import ESMetaclass, DocumentMetaclass
from .signals import on_bulk_update
from .fields import (
    DateTimeField, IntegerField, ForeignKeyField, RelationshipField,
    DictField, ListField, ChoiceField, ReferenceField, StringField,
    TextField, UnicodeField, UnicodeTextField,
    IdField, BooleanField, BinaryField, DecimalField, FloatField,
    BigIntegerField, SmallIntegerField, IntervalField, DateField,
    TimeField, BaseFieldMixin
)


log = logging.getLogger(__name__)


def get_document_cls(name):
    try:
        return mongo.document.get_document(name)
    except:
        raise ValueError('`%s` does not exist in mongo db' % name)


def get_document_classes():
    """ Get all defined not abstract document classes

    Class is assumed to be non-abstract if its `_meta['abstract']` is
    defined and False.
    """
    document_classes = {}
    registry = mongo.base.common._document_registry.copy()
    for model_name, model_cls in registry.items():
        _meta = getattr(model_cls, '_meta', {})
        abstract = _meta.get('abstract', True)
        if not abstract:
            document_classes[model_name] = model_cls
    return document_classes


def process_lists(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'in' or _t == 'all':
            _dict[k] = _dict.aslist(k)
    return _dict


def process_bools(_dict):
    for k in _dict:
        new_k, _, _t = k.partition('__')
        if _t == 'bool':
            _dict[new_k] = _dict.pop_bool_param(k)

    return _dict


TYPES_MAP = {
    StringField: {'type': 'string'},
    TextField: {'type': 'string'},
    UnicodeField: {'type': 'string'},
    UnicodeTextField: {'type': 'string'},
    mongo.fields.ObjectIdField: {'type': 'string'},
    ForeignKeyField: {'type': 'string'},
    IdField: {'type': 'string'},

    BooleanField: {'type': 'boolean'},
    BinaryField: {'type': 'object'},
    DictField: {'type': 'object', 'enabled': False},

    DecimalField: {'type': 'double'},
    FloatField: {'type': 'double'},

    IntegerField: {'type': 'long'},
    BigIntegerField: {'type': 'long'},
    SmallIntegerField: {'type': 'long'},
    IntervalField: {'type': 'long'},

    DateTimeField: {'type': 'date', 'format': 'dateOptionalTime'},
    DateField: {'type': 'date', 'format': 'dateOptionalTime'},
    TimeField: {'type': 'date', 'format': 'HH:mm:ss'},
}


class BaseMixin(object):
    """ Represents mixin class for models.

    Attributes:
        _auth_fields: String names of fields meant to be displayed to
            authenticated users.
        _public_fields: String names of fields meant to be displayed to
            non-authenticated users.
        _hidden_fields: String names of fields meant to be hidden but editable.
        _nested_relationships: String names of reference/relationship fields
            that should be included in JSON data of an object as full
            included documents. If reference/relationship field is not
            present in this list, this field's value in JSON will be an
            object's ID or list of IDs.
        _nesting_depth: Depth of relationship field nesting in JSON.
            Defaults to 1(one) which makes only one level of relationship
            nested.
    """
    _public_fields = None
    _auth_fields = None
    _hidden_fields = None
    _nested_relationships = ()
    _backref_hooks = ()
    _nesting_depth = 1

    _type = property(lambda self: self.__class__.__name__)
    Q = mongo.Q

    @classmethod
    def get_es_mapping(cls, _depth=None, types_map=None):
        """ Generate ES mapping from model schema. """
        from nefertari.elasticsearch import ES
        if types_map is None:
            types_map = TYPES_MAP
        if _depth is None:
            _depth = cls._nesting_depth
        depth_reached = _depth <= 0

        properties = {}
        mapping = {
            ES.src2type(cls.__name__): {
                'properties': properties
            }
        }
        fields = cls._fields.copy()
        for name, field in fields.items():
            if isinstance(field, RelationshipField):
                field = field.field
            if isinstance(field, (ReferenceField, RelationshipField)):
                if name in cls._nested_relationships and not depth_reached:
                    field_mapping = {'type': 'nested'}
                    submapping = field.document_type.get_es_mapping(
                        _depth=_depth-1)
                    field_mapping.update(list(submapping.values())[0])
                else:
                    field_mapping = types_map[
                        field.document_type.pk_field_type()]
                properties[name] = field_mapping
                continue

            if isinstance(field, ChoiceField):
                field = field._real_field
            field_type = type(field)
            if field_type is ListField:
                field_type = field.item_type
            if field_type not in types_map:
                continue
            properties[name] = types_map[field_type]

        properties['_pk'] = {'type': 'string'}
        return mapping

    @classmethod
    def autogenerate_for(cls, model, set_to):
        """ Setup `post_save` event handler.

        Event handler is registered for class :model: and creates a new
        instance of :cls: with a field :set_to: set to an instance on
        which the event occured.

        The handler is set up as class method because mongoengine refuses
        to call signal handlers if they aren't importable.
        """
        from mongoengine import signals

        def generate(cls, sender, document, *args, **kw):
            if kw.get('created', False):
                cls(**{set_to: document}).save()

        cls._generate_on_creation = classmethod(generate)
        signals.post_save.connect(cls._generate_on_creation, sender=model)

    @classmethod
    def pk_field(cls):
        return cls._meta['id_field']

    @classmethod
    def pk_field_type(cls):
        return getattr(cls, cls.pk_field()).__class__

    @classmethod
    def check_fields_allowed(cls, fields):
        if issubclass(cls, mongo.DynamicDocument):
            # Dont check if its dynamic doc
            return
        fields = [f.split('__')[0] for f in fields]
        fields_to_query = set(cls.fields_to_query())
        if not set(fields).issubset(fields_to_query):
            not_allowed = set(fields) - fields_to_query
            raise JHTTPBadRequest(
                "'%s' object does not have fields: %s" % (
                    cls.__name__, ', '.join(not_allowed)))

    @classmethod
    def filter_fields(cls, params):
        """ Filter out fields with invalid names. """
        fields = cls.fields_to_query()
        return dictset({
            name: val for name, val in params.items()
            if name.split('__')[0] in fields
        })

    @classmethod
    def apply_fields(cls, query_set, _fields):
        fields_only, fields_exclude = process_fields(_fields)

        try:
            if fields_only:
                query_set = query_set.only(*fields_only)

            if fields_exclude:
                query_set = query_set.exclude(*fields_exclude)

        except mongo.InvalidQueryError as e:
            raise JHTTPBadRequest('Bad _fields param: %s ' % e)

        return query_set

    @classmethod
    def apply_sort(cls, query_set, _sort):
        if not _sort:
            return query_set
        return query_set.order_by(*_sort)

    @classmethod
    def count(cls, query_set):
        return query_set.count(with_limit_and_skip=True)

    @classmethod
    def filter_objects(cls, objects, first=False, **params):
        """ Perform query with :params: on instances sequence :objects:

        Arguments:
            :object: Sequence of :cls: instances on which query should be run.
            :params: Query parameters to filter :objects:.
        """
        id_name = cls.pk_field()
        key = '{}__in'.format(id_name)
        ids = [getattr(obj, id_name, None) for obj in objects]
        ids = [str(id_) for id_ in ids if id_ is not None]
        params[key] = ids

        if first:
            return cls.get_item(**params)
        else:
            return cls.get_collection(**params)

    @classmethod
    def get_collection(cls, **params):
        """
        Params may include '_limit', '_page', '_sort', '_fields'.
        Returns paginated and sorted query set.
        Raises JHTTPBadRequest for bad values in params.
        """
        log.debug('Get collection: {}, {}'.format(cls.__name__, params))
        params.pop('__confirmation', False)
        _strict = params.pop('_strict', True)
        _item_request = params.pop('_item_request', False)

        _sort = _split(params.pop('_sort', []))
        _fields = _split(params.pop('_fields', []))
        _limit = params.pop('_limit', None)
        _page = params.pop('_page', None)
        _start = params.pop('_start', None)
        query_set = params.pop('query_set', None)

        _count = '_count' in params
        params.pop('_count', None)
        _explain = '_explain' in params
        params.pop('_explain', None)
        _raise_on_empty = params.pop('_raise_on_empty', False)

        if query_set is None:
            query_set = cls.objects

        # Remove any __ legacy instructions from this point on
        params = dictset({
            key: val for key, val in params.items()
            if not key.startswith('__')
        })

        params = drop_reserved_params(params)
        if _strict:
            _check_fields = [
                f.strip('-+') for f in list(params.keys()) + _fields + _sort]
            cls.check_fields_allowed(_check_fields)
        else:
            params = cls.filter_fields(params)

        process_lists(params)
        process_bools(params)

        # If param is _all then remove it
        params.pop_by_values('_all')

        try:
            query_set = query_set(**params)
            _total = query_set.count()
            if _count:
                return _total

            # Filtering by fields has to be the first thing to do on the
            # query_set!
            query_set = cls.apply_fields(query_set, _fields)
            query_set = cls.apply_sort(query_set, _sort)

            if _limit is not None:
                _start, _limit = process_limit(_start, _page, _limit)
                query_set = query_set[_start:_start+_limit]

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if _raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)

        except mongo.ValidationError as ex:
            if _item_request:
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                raise JHTTPNotFound(msg, explanation=ex.message)
            else:
                raise JHTTPBadRequest(str(ex), extra={'data': ex})
        except mongo.InvalidQueryError as ex:
            raise JHTTPBadRequest(str(ex), extra={'data': ex})

        if _explain:
            return query_set.explain()

        log.debug('get_collection.query_set: %s(%s)',
                  cls.__name__, query_set._query)

        query_set._nefertari_meta = dict(
            total=_total,
            start=_start,
            fields=_fields)

        return query_set

    @classmethod
    def has_field(cls, field):
        return field in cls._fields

    @classmethod
    def fields_to_query(cls):
        query_fields = [
            'id', '_limit', '_page', '_sort', '_fields', '_count', '_start']
        return query_fields + list(cls._fields.keys())

    @classmethod
    def get_item(cls, **params):
        params.setdefault('_raise_on_empty', True)
        params['_limit'] = 1
        params['_item_request'] = True
        query_set = cls.get_collection(**params)
        return query_set.first()

    def unique_fields(self):
        pk_field = [self.pk_field()]
        uniques = [e['fields'][0][0] for e in self._unique_with_indexes()]
        return uniques + pk_field

    @classmethod
    def get_or_create(cls, **params):
        defaults = params.pop('defaults', {})
        try:
            return cls.objects.get(**params), False
        except mongo.queryset.DoesNotExist:
            defaults.update(params)
            return cls(**defaults).save(), True
        except mongo.queryset.MultipleObjectsReturned:
            raise JHTTPBadRequest('Bad or Insufficient Params')

    def _update(self, params, **kw):
        process_bools(params)
        self.check_fields_allowed(list(params.keys()))
        iter_fields = set(
            k for k, v in type(self)._fields.items()
            if isinstance(v, (DictField, ListField)) and
            not isinstance(v, RelationshipField))
        pk_field = self.pk_field()
        for key, value in params.items():
            if key == pk_field:  # can't change the primary key
                continue
            if key in iter_fields:
                self.update_iterables(value, key, unique=True, save=False)
            else:
                setattr(self, key, value)
        return self.save(**kw)

    @classmethod
    def _delete_many(cls, items, request=None):
        """ Delete objects from :items: """
        items_count = len(items)
        for item in items:
            item.delete(request)
        return items_count

    @classmethod
    def _update_many(cls, items, params, request=None):
        """ Update objects from :items:

        If :items: is an instance of `mongoengine.queryset.queryset.QuerySet`
        items.update() is called. Otherwise update is performed per-object.

        'on_bulk_update' is called explicitly, because mongoengine does not
        trigger any signals on QuerySet.update() call.
        """
        if isinstance(items, mongo.queryset.queryset.QuerySet):
            items.update(**params)
            on_bulk_update(cls, items, request)
            return cls.count(items)
        items_count = len(items)
        for item in items:
            item.update(params, request)
        return items_count

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        pk_field = self.pk_field()
        parts.append('{}={}'.format(pk_field, getattr(self, pk_field)))
        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        pk_field = '{}__in'.format(cls.pk_field())
        params.update({
            pk_field: ids,
        })
        return cls.get_collection(**params)

    @classmethod
    def get_null_values(cls):
        """ Get null values of :cls: fields. """
        skip_fields = set(['_acl'])
        null_values = {}
        for name in cls._fields.keys():
            if name in skip_fields:
                continue
            field = getattr(cls, name)
            if isinstance(field, RelationshipField):
                value = []
            else:
                value = None
            null_values[name] = value
        null_values.pop('id', None)
        return null_values

    def to_dict(self, **kwargs):
        _depth = kwargs.get('_depth')
        if _depth is None:
            _depth = self._nesting_depth
        depth_reached = _depth is not None and _depth <= 0

        _data = dictset()
        for field, field_type in self._fields.items():
            # Ignore ForeignKeyField fields
            if isinstance(field_type, ForeignKeyField):
                continue
            value = getattr(self, field, None)

            if value is not None:
                include = field in self._nested_relationships
                if not include or depth_reached:
                    encoder = lambda v: getattr(v, v.pk_field(), None)
                else:
                    encoder = lambda v: v.to_dict(_depth=_depth-1)

                if isinstance(field_type, ReferenceField):
                    value = encoder(value)
                elif isinstance(field_type, RelationshipField):
                    value = [encoder(val) for val in value]
                elif hasattr(value, 'to_dict'):
                    value = value.to_dict(_depth=_depth-1)

            _data[field] = value
        _data['_type'] = self._type
        _data['_pk'] = str(getattr(self, self.pk_field()))
        return _data

    def get_related_documents(self, nested_only=False):
        """ Return pairs of (Model, istances) of relationship fields.

        Pair contains of two elements:
          :Model: Model class object(s) contained in field.
          :instances: Model class instance(s) contained in field

        :param nested_only: Boolean, defaults to False. When True, return
            results only contain data for models on which current model
            and field are nested.
        """
        relationship_fields = {
            name: field for name, field in self._fields.items()
            if isinstance(field, (ReferenceField, RelationshipField))}

        for name, field in relationship_fields.items():
            value = getattr(self, name)
            if not value:
                continue
            if not isinstance(value, list):
                value = [value]
            model_cls = value[0].__class__

            if nested_only:
                backref = getattr(field, 'reverse_rel_field', None)
                if backref and backref not in model_cls._nested_relationships:
                    continue

            yield (model_cls, value)

    def update_iterables(self, params, attr, unique=False,
                         value_type=None, save=True,
                         request=None):
        is_dict = isinstance(type(self)._fields[attr], mongo.DictField)
        is_list = isinstance(type(self)._fields[attr], mongo.ListField)

        def split_keys(keys):
            neg_keys = []
            pos_keys = []

            for key in keys:
                if key.startswith('__'):
                    continue
                if key.startswith('-'):
                    neg_keys.append(key[1:])
                else:
                    pos_keys.append(key.strip())
            return pos_keys, neg_keys

        def update_dict(update_params):
            final_value = getattr(self, attr, {}) or {}
            final_value = final_value.copy()
            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = {
                    '-' + key: val for key, val in final_value.items()}
            positive, negative = split_keys(list(update_params.keys()))

            # Pop negative keys
            for key in negative:
                final_value.pop(key, None)

            # Set positive keys
            for key in positive:
                final_value[str(key)] = update_params[key]

            setattr(self, attr, final_value)
            if save:
                self.save(request)

        def update_list(update_params):
            final_value = getattr(self, attr, []) or []
            final_value = copy.deepcopy(final_value)
            if update_params is None or update_params == '':
                if not final_value:
                    return
                update_params = ['-' + val for val in final_value]
            if isinstance(update_params, dict):
                keys = list(update_params.keys())
            else:
                keys = update_params

            positive, negative = split_keys(keys)

            if not (positive + negative):
                raise JHTTPBadRequest('Missing params')

            if positive:
                if unique:
                    positive = [v for v in positive if v not in final_value]
                final_value += positive

            if negative:
                final_value = list(set(final_value) - set(negative))

            setattr(self, attr, final_value)
            if save:
                self.save(request)

        if is_dict:
            update_dict(params)

        elif is_list:
            update_list(params)

    @classmethod
    def expand_with(cls, with_cls, join_on=None, attr_name=None, params={},
                    with_params={}):
        """ Acts like "join" and inserts the with_cls objects in
        the result as "attr_name".
        """
        if join_on is None:
            join_on = with_cls.__name__.lower()
        if attr_name is None:
            attr_name = with_cls.__name__.lower()

        with_params.pop('_fields', [])
        with_objs = with_cls.get_by_ids(
            cls.objects.scalar(join_on),
            **with_params)
        with_objs = dict([[str(wth.id), wth] for wth in with_objs])

        params['%s__in' % join_on] = list(with_objs.keys())
        objs = cls.get_collection(**params)

        for ob in objs:
            ob._data[attr_name] = with_objs[getattr(ob, join_on)]
            setattr(ob, attr_name, ob._data[attr_name])

        return objs

    def _is_modified(self):
        """ Determine if instance is modified.

        For instance to be marked as 'modified', it should:
          * Have PK field set (not newly created)
          * Have changed fields
        """
        modified = bool(self._get_changed_fields())
        return modified

    def _is_created(self):
        return self._created

    def _to_python_fields(self):
        """ Call to_python on non-relation fields. """
        from .utils import relationship_fields
        for name, field in self._fields.items():
            rel_field = isinstance(field, relationship_fields)
            if name not in self._data or rel_field:
                continue
            value = self._data[name]
            try:
                self._data[name] = field.to_python(value)
            except:
                continue


class BaseDocument(six.with_metaclass(DocumentMetaclass,
                                      BaseMixin, mongo.Document)):
    meta = {
        'abstract': True,
    }

    def __init__(self, *args, **values):
        """ Override init to filter out invalid fields from :values:.

        Fields are filtered out to make mongoengine less strict when
        loading objects from database.
        :internal_fields: are the fields pop'ed from :values: before
        performing fields presence validation in the original mongoengine
        init code:
        https://github.com/MongoEngine/mongoengine/blob/v0.9.0/mongoengine/base/document.py#L41

        PS. This issue is fixed in mongoengine master and not released after
        0.9.0 yet.
        https://github.com/MongoEngine/mongoengine/blob/master/mongoengine/base/document.py#L75
        """
        _created = values.get('_created')
        if _created is not None and not _created:
            internal_fields = [
                'id', 'pk', '_cls', '_text_score',
                '__auto_convert', '__only_fields', '_created',
            ]
            valid_fields = list(self._fields.keys()) + internal_fields
            values = {key: val for key, val in values.items()
                      if key in valid_fields}
        super(BaseDocument, self).__init__(*args, **values)

    def save(self, request=None, *arg, **kw):
        """
        Force insert document in creation so that unique constraits are
        respected.
        This makes each POST to a collection act as a 'create' operation
        (as opposed to an 'update' for example).
        """
        kw['force_insert'] = self._created
        self._request = request
        try:
            super(BaseDocument, self).save(*arg, **kw)
        except (mongo.NotUniqueError, mongo.OperationError) as e:
            if (e.__class__ is mongo.OperationError
                    and 'E11000' not in e.message):
                raise  # Other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})
        else:
            self.run_backref_hooks()
            self._backref_hooks = ()
            return self

    def run_backref_hooks(self):
        """ Runs post-save backref hooks.

        Includes backref hooks which are used one time only
        to sync the backrefs.
        """
        for hook in self._backref_hooks:
            hook(document=self)

    def update(self, params, request=None, **kw):
        kw['request'] = request
        # request are passed to _update and then to save
        try:
            self._update(params, **kw)
            self._to_python_fields()
            return self
        except (mongo.NotUniqueError, mongo.OperationError) as e:
            if (e.__class__ is mongo.OperationError
                    and 'E11000' not in e.message):
                raise  # other error, not duplicate

            raise JHTTPConflict(
                detail='Resource `{}` already exists.'.format(
                    self.__class__.__name__),
                extra={'data': e})

    def validate(self, *arg, **kw):
        try:
            super(BaseDocument, self).validate(*arg, **kw)
        except mongo.ValidationError as e:
            raise JHTTPBadRequest(
                'Resource `%s`: %s' % (self.__class__.__name__, e),
                extra={'data': e})

    def delete(self, request=None, **kw):
        self._request = request
        super(BaseDocument, self).delete(**kw)

    @classmethod
    def get_field_params(cls, field_name):
        """ Get init params of field named :field_name:. """
        field = cls._fields.get(field_name)
        return getattr(field, '_init_kwargs', None)

    def clean(self):
        """ Clean fields which are instances of BaseFieldMixin """
        for field_name, field_obj in self._fields.items():
            if isinstance(field_obj, BaseFieldMixin):
                field_obj.clean(self)


class ESBaseDocument(six.with_metaclass(ESMetaclass, BaseDocument)):
    """ Base for document classes which should be indexed by ES. """
    meta = {
        'abstract': True,
    }
