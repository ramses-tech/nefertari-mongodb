import copy
import logging
from datetime import datetime

import mongoengine as mongo

from nefertari.json_httpexceptions import (
    JHTTPBadRequest, JHTTPNotFound, JHTTPConflict)
from nefertari.utils import (
    process_fields, process_limit, _split, dictset, DataProxy,
    to_dicts)
from .metaclasses import ESMetaclass, DocumentMetaclass
from .fields import (
    DateTimeField, IntegerField, ForeignKeyField, RelationshipField,
    DictField, ListField, ChoiceField, ReferenceField, StringField,
    TextField, UnicodeField, UnicodeTextField,
    IdField, BooleanField, BinaryField, DecimalField, FloatField,
    BigIntegerField, SmallIntegerField, IntervalField, DateField,
    TimeField
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
    DictField: {'type': 'object'},

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
        _nested_relationships: String names of reference/relationship fields
            that should be included in JSON data of an object as full
            included documents. If reference/relationship field is not
            present in this list, this field's value in JSON will be an
            object's ID or list of IDs.
    """
    _public_fields = None
    _auth_fields = None
    _nested_relationships = ()
    _backref_hooks = ()

    _type = property(lambda self: self.__class__.__name__)
    Q = mongo.Q

    @classmethod
    def get_es_mapping(cls):
        """ Generate ES mapping from model schema. """
        from nefertari.elasticsearch import ES
        ignored_types = set([
            ReferenceField,
            RelationshipField,
        ])
        properties = {}
        mapping = {
            ES.src2type(cls.__name__): {
                'properties': properties
            }
        }
        fields = cls._fields.copy()
        fields['id'] = fields.get(cls.pk_field())

        for name, field in fields.items():
            if isinstance(field, ChoiceField):
                field = field._real_field
            field_type = type(field)
            if field_type is ListField:
                field_type = field.item_type
            if field_type in ignored_types:
                continue
            if field_type not in TYPES_MAP:
                continue
            properties[name] = TYPES_MAP[field_type]

        properties['_type'] = {'type': 'string'}
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
            :params: Query parameters.
        """
        id_name = cls.pk_field()
        key = '{}__in'.format(id_name)
        ids = [getattr(obj, id_name, None) for obj in objects]
        ids = [str(id_) for id_ in ids if id_ is not None]
        params[key] = ids

        if first:
            return cls.get_resource(**params)
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
        __strict = params.pop('__strict', True)

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
        __raise_on_empty = params.pop('__raise_on_empty', False)

        if query_set is None:
            query_set = cls.objects

        # Remove any __ legacy instructions from this point on
        params = dictset(filter(
            lambda item: not item[0].startswith('__'), params.items()))

        if __strict:
            _check_fields = [
                f.strip('-+') for f in params.keys() + _fields + _sort]
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

            if _limit is None:
                raise JHTTPBadRequest('Missing _limit')

            _start, _limit = process_limit(_start, _page, _limit)

            # Filtering by fields has to be the first thing to do on the
            # query_set!
            query_set = cls.apply_fields(query_set, _fields)
            query_set = cls.apply_sort(query_set, _sort)
            query_set = query_set[_start:_start+_limit]

            if not query_set.count():
                msg = "'%s(%s)' resource not found" % (cls.__name__, params)
                if __raise_on_empty:
                    raise JHTTPNotFound(msg)
                else:
                    log.debug(msg)

        except (mongo.ValidationError, mongo.InvalidQueryError) as e:
            raise JHTTPBadRequest(str(e), extra={'data': e})

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
        return query_fields + cls._fields.keys()

    @classmethod
    def get_resource(cls, **params):
        params.setdefault('__raise_on_empty', True)
        params['_limit'] = 1
        query_set = cls.get_collection(**params)
        return query_set.first()

    @classmethod
    def get(cls, **kw):
        return cls.get_resource(
            __raise_on_empty=kw.pop('__raise', False), **kw)

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
        self.check_fields_allowed(params.keys())
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
    def _delete(cls, **params):
        cls.objects(**params).delete()

    @classmethod
    def _delete_many(cls, items):
        for item in items:
            item.delete()

    @classmethod
    def _update_many(cls, items, **params):
        for item in items:
            item.update(params)

    def __repr__(self):
        parts = ['%s:' % self.__class__.__name__]

        if hasattr(self, 'id'):
            parts.append('id=%s' % self.id)

        if hasattr(self, '_version'):
            parts.append('v=%s' % self._version)

        return '<%s>' % ', '.join(parts)

    @classmethod
    def get_by_ids(cls, ids, **params):
        pk_field = '{}__in'.format(cls.pk_field())
        params.update({
            pk_field: ids,
            '_limit': len(ids),
        })
        return cls.get_collection(**params)

    @classmethod
    def get_null_values(cls):
        """ Get null values of :cls: fields. """
        null_values = {}
        field_names = cls._fields.keys()
        for name in field_names:
            field = getattr(cls, name)
            if isinstance(field, RelationshipField):
                value = []
            else:
                value = None
            null_values[name] = value
        null_values.pop('id', None)
        return null_values

    def to_dict(self, **kwargs):
        def _process(key, val):
            is_doc = isinstance(val, mongo.Document)
            include = key in self._nested_relationships
            if is_doc and not include:
                val = getattr(val, val.pk_field(), None)
            return val

        _data = {}
        for attr in self._data:
            # Ignore ForeignKeyField fields
            if isinstance(self._fields.get(attr), ForeignKeyField):
                continue
            value = getattr(self, attr, None)
            if isinstance(value, list):
                value = [_process(attr, v) for v in value]
            else:
                value = _process(attr, value)
            _data[attr] = value
        _dict = DataProxy(_data).to_dict(**kwargs)
        _dict['_type'] = self._type
        _dict['id'] = getattr(self, self.pk_field())
        return _dict

    def get_reference_documents(self):
        # TODO: Make lazy load of documents
        models = self.__class__._meta['delete_rules'] or {}
        for model_cls, key in models:
            documents = to_dicts(model_cls.objects(**{key: self}))
            yield model_cls, documents

    def update_iterables(self, params, attr, unique=False,
                         value_type=None, save=True):
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
            if update_params is None:
                update_params = {
                    '-' + key: val for key, val in final_value.items()}
            positive, negative = split_keys(update_params.keys())

            # Pop negative keys
            for key in negative:
                final_value.pop(key, None)

            # Set positive keys
            for key in positive:
                final_value[unicode(key)] = update_params[key]

            setattr(self, attr, final_value)
            if save:
                self.save()

        def update_list(update_params):
            final_value = getattr(self, attr, []) or []
            final_value = copy.deepcopy(final_value)
            if update_params is None:
                update_params = ['-' + val for val in final_value]
            keys = (update_params.keys() if isinstance(update_params, dict)
                    else update_params)
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
                self.save()

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

        params['%s__in' % join_on] = with_objs.keys()
        objs = cls.get_collection(**params)

        for ob in objs:
            ob._data[attr_name] = with_objs[getattr(ob, join_on)]
            setattr(ob, attr_name, ob._data[attr_name])

        return objs


class BaseDocument(BaseMixin, mongo.Document):
    __metaclass__ = DocumentMetaclass

    updated_at = DateTimeField()
    _version = IntegerField(default=0)

    meta = {
        'abstract': True,
    }

    def save(self, *arg, **kw):
        """
        Force insert document in creation so that unique constraits are
        respected.
        This makes each POST to a collection act as a 'create' operation
        (as opposed to an 'update' for example).
        """
        kw['force_insert'] = self._created

        sync_backref = kw.pop('sync_backref', True)
        if self._get_changed_fields():
            self.updated_at = datetime.utcnow()
            self._version += 1
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
            if sync_backref:
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

    def update(self, params, **kw):
        try:
            return self._update(params, **kw)
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
            return super(BaseDocument, self).validate(*arg, **kw)
        except mongo.ValidationError as e:
            raise JHTTPBadRequest(
                'Resource `%s`: %s' % (self.__class__.__name__, e),
                extra={'data': e})

    def clean(self):
        """ Override `clean` method to apply each field's processors
        before running validation.
        """
        for name, field in self._fields.items():
            if hasattr(field, 'apply_processors'):
                value = getattr(self, name)
                value = field.apply_processors(value)
                setattr(self, name, value)


class ESBaseDocument(BaseDocument):
    __metaclass__ = ESMetaclass
    meta = {
        'abstract': True,
    }
