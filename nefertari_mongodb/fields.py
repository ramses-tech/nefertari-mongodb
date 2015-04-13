import datetime
import pickle
from functools import partial

import mongoengine as mongo
from mongoengine import fields
from mongoengine.queryset import DO_NOTHING, NULLIFY, CASCADE, DENY, PULL
import dateutil.parser


class BaseFieldMixin(object):
    """ Base mixin to implement a common interface for all mongo fields.

    Is responsible for droping invalid field kwargs.
    Subclasses must define `translate_kwargs` if some fields from
    input kwargs have different name but same meaning.

    Attributes:
        _valid_kwargs: sequence of strings that represent valid kwargs
            names that a current field may accept. Kwargs not present in
            this sequence are dropped by `self.drop_invalid_kwargs`.
        _common_valid_kwargs: sequence of strings that represent valid kwargs
            names that a `mongoengine.base.fields.BaseField` accepts. These
            kwargs are common to all the fields.
    """
    _valid_kwargs = ()
    _common_valid_kwargs = (
        'db_field', 'name', 'required', 'default', 'unique',
        'unique_with', 'primary_key', 'validation', 'choices',
        'verbose_name', 'help_text', 'sparse',
        # 'null'  # In development branch of mongoengine
    )

    def __init__(self, *args, **kwargs):
        """ Translate kwargs and drop invalid kwargs. """
        kwargs = self.translate_kwargs(kwargs)
        kwargs = self.drop_invalid_kwargs(kwargs)
        super(BaseFieldMixin, self).__init__(*args, **kwargs)

    def translate_kwargs(self, kwargs):
        """ Translate kwargs from one key to another.

        E.g. your field is an instance of a StringField and accepts kwarg
        'maximum_length', but the StringField expects the same kwargs with
        a key `max_length`. Extend this method to perform such translations.

        This particular implementation translates:
            name -> db_field
        """
        kwargs = kwargs.copy()
        kwargs['db_field'] = kwargs.pop('name', None)
        # In development branch of mongoengine
        # if kwargs.get('null') is None:
        #     kwargs['null'] = not kwargs.get('required', False)
        return kwargs

    def drop_invalid_kwargs(self, kwargs):
        """ Drop kwargs not present in `self._common_valid_kwargs` and
        `self._valid_kwargs` (thus invalid).
        """
        valid_kwargs = self._common_valid_kwargs + self._valid_kwargs
        return {k: v for k, v in kwargs.items() if k in valid_kwargs}


class ProcessableMixin(object):
    """ Mixin that allows running callables on a value that
    is being set to a field.
    """
    def __init__(self, *args, **kwargs):
        self.processors = kwargs.pop('processors', ())
        super(ProcessableMixin, self).__init__(*args, **kwargs)

    def apply_processors(self, value):
        for proc in self.processors:
            value = proc(value)
        return value


class IntegerField(ProcessableMixin, BaseFieldMixin, fields.IntField):
    _valid_kwargs = ('min_value', 'max_value')


class BigIntegerField(ProcessableMixin, BaseFieldMixin, fields.LongField):
    _valid_kwargs = ('min_value', 'max_value')


class SmallIntegerField(ProcessableMixin, BaseFieldMixin, fields.IntField):
    """ As mongoengine does not provide SmallInt, this is just a
    copy of `IntegerField`.
    """
    _valid_kwargs = ('min_value', 'max_value')


class BooleanField(ProcessableMixin, BaseFieldMixin, fields.BooleanField):
    _valid_kwargs = ()


class DateField(ProcessableMixin, BaseFieldMixin, fields.DateTimeField):
    """ Custom field that stores `datetime.date` instances.

    This is basically mongoengine's `DateTimeField` which gets
    datetime's `.date()` before storing to mongo.
    """
    _valid_kwargs = ()

    def to_mongo(self, *args, **kwargs):
        """ Override mongo's DateTimeField value conversion to get
        date instead of datetime.
        """
        value = super(DateField, self).to_mongo(*args, **kwargs)
        if isinstance(value, datetime.datetime):
            value = value.date()
        return value


class DateTimeField(ProcessableMixin, BaseFieldMixin, fields.DateTimeField):
    _valid_kwargs = ()


class FloatField(ProcessableMixin, BaseFieldMixin, fields.FloatField):
    _valid_kwargs = ('min_value', 'max_value')


class StringField(ProcessableMixin, BaseFieldMixin, fields.StringField):
    """ Custom version of mongoengine StringField.

    Converts values to str type on output. Unicode strings
    are returned by a separate field class.
    """
    _valid_kwargs = ('regex', 'min_length', 'max_length')

    def to_python(self, value):
        return str(value)


class TextField(StringField):
    pass


class UnicodeField(ProcessableMixin, BaseFieldMixin, fields.StringField):
    _valid_kwargs = ('regex', 'min_length', 'max_length')


class UnicodeTextField(UnicodeField):
    pass


class ChoiceField(ProcessableMixin, fields.BaseField):
    """
    As mongoengine does not have explicit ChoiceField, but all mongoengine
    fields accept `choices` kwarg, this kind of proxy is defined.
    It uses naive aproach: check the type of first choice and instantiate
    a field of an appropriate type under the hood. Then translate all the
    attribute access to the underlying field.
    """
    def __init__(self, *args, **kwargs):
        first_choice = kwargs['choices'][0]
        if isinstance(first_choice, int):
            self._real_field = IntegerField(*args, **kwargs)
        elif isinstance(first_choice, str):
            self._real_field = StringField(*args, **kwargs)
        elif isinstance(first_choice, unicode):
            self._real_field = UnicodeField(*args, **kwargs)
        elif isinstance(first_choice, float):
            self._real_field = FloatField(*args, **kwargs)
        else:
            raise ValueError(
                'Choices must be one of following types: int, str, float, '
                'unicode. All choices must be of one type.')
        self.__dict__.update(self._real_field.__dict__)

    def __getattribute__(self, attr):
        methods = ('to_python', 'to_mongo', 'prepare_query_value',
                   'validate', 'lookup_member')
        if attr in methods:
            return self._real_field.__getattribute__(attr)
        return super(ChoiceField, self).__getattribute__(attr)


class BinaryField(ProcessableMixin, BaseFieldMixin, fields.BinaryField):
    _valid_kwargs = ('max_bytes',)

    def translate_kwargs(self, kwargs):
        """ Translate kwargs from one key to another.

        Translates:
            length -> max_bytes
        """
        kwargs = super(BinaryField, self).translate_kwargs(kwargs)
        kwargs['max_bytes'] = kwargs.pop('length', None)
        return kwargs


class DecimalField(ProcessableMixin, BaseFieldMixin, fields.DecimalField):
    """ This is basically a DecimalField with a fixed name of
    `precision` kwarg.
    """
    _valid_kwargs = ('min_value', 'max_value', 'force_string',
                     'precision', 'rounding')

    def translate_kwargs(self, kwargs):
        """ Translate kwargs from one key to another.

        Translates:
            scale -> precision
        """
        kwargs = super(DecimalField, self).translate_kwargs(kwargs)
        kwargs['precision'] = kwargs.pop('scale', None)
        return kwargs


class TimeField(ProcessableMixin, BaseFieldMixin, fields.BaseField):
    """ Custom field that stores `datetime.date` instances. """
    _valid_kwargs = ()

    def validate(self, value):
        new_value = self.to_mongo(value)
        if not isinstance(new_value, datetime.time):
            self.error("Can't parse time `%s`" % value)

    def to_mongo(self, value):
        value = super(TimeField, self).to_mongo(value)
        if value is None:
            return value
        if isinstance(value, datetime.time):
            return value
        if isinstance(value, datetime.datetime):
            return value.time()
        if callable(value):
            return value()

        if not isinstance(value, basestring):
            return None

        try:
            return dateutil.parser.parse(value).time()
        except (TypeError, ValueError):
            return None

    def to_python(self, value):
        value = super(TimeField, self).to_python(value)
        if not isinstance(value, datetime.time):
            value = value.time()
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)


class PickleField(BinaryField):
    """ Custom field that stores pickled data as a BinaryField.

    Data is pickled when saving to mongo and unpickled when retrieving
    from mongo.
    `pickler` kwarg may be provided that may be any object with
    pickle-compatible `dumps` and `loads` methods. Defaults to python's
    built-in `pickle`.
    """
    pickler = None

    def translate_kwargs(self, kwargs):
        self.pickler = kwargs.pop('pickler', pickle)
        return super(PickleField, self).translate_kwargs(kwargs)

    def validate(self, value):
        value = self.pickler.dumps(value)
        return super(PickleField, self).validate(value)

    def to_mongo(self, value):
        value = self.pickler.dumps(value)
        return super(PickleField, self).to_mongo(value)

    def to_python(self, value):
        value = super(PickleField, self).to_python(value)
        return self.pickler.loads(value)


class IntervalField(IntegerField):
    """ Custom field that stores `datetime.timedelta` instances.

    Values are stored as seconds in mongo and loaded by
    `datetime.timedelta(seconds=<value>) when restoring from mongo.
    """
    _valid_kwargs = ()

    def validate(self, value):
        value = self.to_mongo(value)
        return super(IntervalField, self).validate(value)

    def to_mongo(self, value):
        if not isinstance(value, datetime.timedelta):
            self.error('IntervalField value must be an instance of '
                       '`datetime.timedelta`.')
        return int(value.total_seconds())

    def to_python(self, value):
        return datetime.timedelta(seconds=value)

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)


class ListField(ProcessableMixin, BaseFieldMixin, fields.ListField):
    """ Custom ListField.

    Custom part is the value validation. Choices are stored in a separate
    attribute :self.list_choices: and validation checks if value (which is
    a sequence) has values other than those specified in :self.list_choices:.

    Original mongoengine ListField validation requires value to be a sequence
    but checks for value(sequence) inclusion into choices.
    """
    _valid_kwargs = ('field',)

    def __init__(self, *args, **kwargs):
        self.list_choices = kwargs.pop('choices', None)
        super(ListField, self).__init__(*args, **kwargs)

    def validate(self, value, **kwargs):
        super(ListField, self).validate(value, **kwargs)
        if self.list_choices and value is not None:
            choice_list = self.list_choices
            if isinstance(self.list_choices[0], (list, tuple)):
                choice_list = [k for k, v in self.list_choices]
            if set(value) - set(choice_list):
                self.error('Value must be one of {}. Got: {}'.format(
                    unicode(choice_list), unicode(value)))
        return value


class DictField(ProcessableMixin, BaseFieldMixin, fields.DictField):
    _valid_kwargs = ('basecls', 'field')


class IdField(ProcessableMixin, BaseFieldMixin, fields.ObjectIdField):
    """ Just a subclass of ObjectIdField that must be used for fields
    that represent database-specific 'id' field.
    """
    _valid_kwargs = ('primary_key',)

    def __init__(self, *args, **kwargs):
        kwargs.pop('primary_key', None)
        super(IdField, self).__init__(*args, **kwargs)


class ForeignKeyField(BaseFieldMixin, fields.StringField):
    """ Field that references other document.

    It is not meant to be used inside mongodb engine. It is added for
    compatibility with sqla engine and is not displayed in JSON output.
    """
    _valid_kwargs = ()


class ReferenceField(BaseFieldMixin, fields.ReferenceField):
    """ Field that references other document.

    Isn't meant to be used explicitly by user. To create relationship
    fields, use `Relationship` constructor function.

    When this field is not added to model's `_nested_relationships` this
    field returns an ID of document that is being referenced. Otherwise
    full document is included in JSON response.

    This class is used in a `Relationship` function to generate kind of
    one-to-one relationship. It is also used to create backreferences.

    `reverse_rel_field`: string name of a field of the relationship on the
        other side of relationship. Used in backreferences' generation so
        fields on each side know the name of the field on the other side.
    """
    _valid_kwargs = ('document_type', 'dbref', 'reverse_delete_rule')
    _kwargs_prefix = 'ref_'
    _backref_prefix = 'backref_'
    reverse_rel_field = None

    def __init__(self, *args, **kwargs):
        """ Init the field.

        Also saves backref kwargs for future creation of backref.

        Expects:
            `document` or `<_kwargs_prefix>document`: mongoengine model name.
        """
        key = 'document'
        pref_key = self._kwargs_prefix + key
        document_type = kwargs.pop(pref_key, None) or kwargs.pop(key, None)
        args = (document_type,) + args

        # Filter out backreference kwargs
        self.backref_kwargs = {
            k[len(self._backref_prefix):]: v for k, v in kwargs.items()
            if k.startswith(self._backref_prefix)}

        super(ReferenceField, self).__init__(*args, **kwargs)

    def _register_deletion_hook(self, old_object, instance):
        """ Register backref hook to delete `instance` from the `old_object`s
        field to which `instance` was related before by the backref.

        `instance` is either deleted from `old_object` field's collection or
        `old_object`s field, responsible for relationship is set to None.
        This depends on type of the field at `old_object`.

        `old_object.update` is run with `sync_backref=False` because
        original relationship side is already updated and it is the one
        who calls the hook. This also prevents recursion in One2One.

        `instance` is not actually used in hook - up-to-date value of
        `instance` is passed to hook when it is run.
        """
        def _delete_from_old(old_obj, document, field_name):
            from mongoengine.fields import ListField
            field_value = getattr(old_obj, field_name, None)
            if field_value:
                field_object = old_obj._fields[field_name]
                if isinstance(field_object, ListField):
                    field_value = field_value or []
                    if document in field_value:
                        field_value.remove(document)
                else:
                    field_value = None
                old_obj.update({field_name: field_value}, sync_backref=False)

        old_object_hook = partial(
            _delete_from_old,
            old_obj=old_object,
            field_name=self.reverse_rel_field)
        instance._backref_hooks += (old_object_hook,)

    def _register_addition_hook(self, new_object, instance):
        """ Register backref hook to add `instance` to the `old_object`s
        field to which `instance` became related by the backref.

        `instance` is either added to `old_object` field's collection or
        `old_object`s field, responsible for relationship is set to `instance`.
        This depends on type of the field at `old_object`.

        `new_object.update` is run with `sync_backref=False` because
        original relationship side is already updated and it is the one
        who calls the hook. This also prevents recursion in One2One.

        `instance` is not actually used in hook - up-to-date value of
        `instance` is passed to hook when it is run.
        """
        def _add_to_new(new_obj, document, field_name):
            from mongoengine.fields import ListField
            field_value = getattr(new_obj, field_name, None)
            field_object = new_obj._fields[field_name]
            if isinstance(field_object, ListField):
                field_value = field_value or []
                if document not in field_value:
                    field_value.append(document)
            else:
                field_value = document
            new_obj.update({field_name: field_value}, sync_backref=False)

        new_object_hook = partial(
            _add_to_new,
            new_obj=new_object,
            field_name=self.reverse_rel_field)
        instance._backref_hooks += (new_object_hook,)

    def __set__(self, instance, value):
        """ Custom __set__ method that updates linked relationships.

        Updates linked relationship fields, if current field has
        `reverse_rel_field` property set. By default this property is
        only set when backreference is created.

        If value is changed, `instance` is deleted from object it was related
        to before and is added to object it will be related to now - `value`.
        """
        super_set = super(ReferenceField, self).__set__

        # Object has no backref / is not backref
        if not self.reverse_rel_field:
            return super_set(instance, value)

        old_object = getattr(instance, self.name)
        new_object = value
        super_set(instance, value)

        # The same object is being set - no changes needed
        if new_object == old_object:
            return

        # Old object is being changed to new one, thus
        # old should forget about the `instance`
        if old_object is not None and isinstance(old_object, mongo.Document):
            self._register_deletion_hook(old_object, instance)

        # New object is being set, thus se need to set `instance` to it
        if new_object is not None and isinstance(new_object, mongo.Document):
            self._register_addition_hook(new_object, instance)

    def _get_referential_action(self, kwargs):
        """ Determine/translate generic rule name to mongoengine-specific rule.

        Custom rules names are used here to make them look SQL-ish and
        pretty at the same time.

        Mongoengine rules are:
            DO_NOTHING   Don't do anything (default)
            NULLIFY      Updates the reference to null
            CASCADE      Deletes the documents associated with the reference
            DENY         Prevent the deletion of the reference object
            PULL         Pull the reference from a ListField of references
        """
        key = 'ondelete'
        ondelete = kwargs.pop(key, 'DO_NOTHING')
        rules = {
            'DO_NOTHING': DO_NOTHING,
            'NULLIFY': NULLIFY,
            'CASCADE': CASCADE,
            'RESTRICT': DENY,
            'PULL': PULL,
        }
        ondelete = ondelete.upper()
        if ondelete not in rules:
            raise KeyError('Invalid `{}` argument value. Must be '
                           'one of: {}'.format(key, ', '.join(rules.keys())))
        return rules[ondelete]

    def translate_kwargs(self, kwargs):
        kwargs = super(ReferenceField, self).translate_kwargs(kwargs)
        # Remove prefixes
        for key in self._valid_kwargs:
            pre_key = self._kwargs_prefix + key
            if pre_key in kwargs:
                kwargs[key] = kwargs.pop(pre_key)
        kwargs['reverse_delete_rule'] = self._get_referential_action(kwargs)
        return kwargs


class RelationshipField(ListField):
    """ Relationship field meant to be used to create one-to-many relationships.

    Is used in `Relationship` function to generate one-to-many relationships.
    Under the hood it is just a ListField containing ReferenceFields.

    `reverse_rel_field`: string name of the field of the relationship on the
        other side of relationship. Used in backreferences' generation so
        fields on each side know the name of the field on the other side.
    """
    _common_valid_kwargs = (
        'db_field', 'required', 'default', 'unique',
        'unique_with', 'primary_key', 'validation', 'choices',
        'verbose_name', 'help_text', 'sparse',
    )
    _backref_prefix = 'backref_'
    reverse_rel_field = None

    def __init__(self, *args, **kwargs):
        """ Save backref kwargs for future creation of backref. """
        # Filter out backreference kwargs
        self.backref_kwargs = {
            k[len(self._backref_prefix):]: v for k, v in kwargs.items()
            if k.startswith(self._backref_prefix)}
        super(RelationshipField, self).__init__(*args, **kwargs)

    def _register_addition_hook(self, new_object, instance):
        """ Define and register addition hook.

        Hook sets `instance` at `new_object` using `self.reverse_rel_field` as
        a field which should be set. `instance` is not actually used in hook -
        up-to-date value of `instance` is passed to hook when it is run.
        """
        def _add_to_new(new_obj, document, field_name):
            field_value = getattr(new_obj, field_name, None)
            if field_value != document:
                new_obj.update({field_name: document})

        new_object_hook = partial(
            _add_to_new,
            new_obj=new_object,
            field_name=self.reverse_rel_field)
        instance._backref_hooks += (new_object_hook,)

    def _register_deletion_hook(self, old_object, instance):
        """ Define and register deletion hook.

        Hook removes `instance` from `new_object` using `self.reverse_rel_field`
        as a field from which value should be removed.
        `instance` is not actually used in hook - up-to-date value of `instance`
        is passed to hook when it is run.
        """
        def _delete_from_old(old_obj, document, field_name):
            field_value = getattr(old_obj, field_name, None)
            if field_value == document:
                old_obj.update({field_name: None})

        old_object_hook = partial(
            _delete_from_old,
            old_obj=old_object,
            field_name=self.reverse_rel_field)
        instance._backref_hooks += (old_object_hook,)

    def __set__(self, instance, value):
        """ Custom __set__ method that updates linked relationships.

        Updates linked relationship fields, if current field has
        `reverse_rel_field` property set. By default this property is
        only set when backreference is created.

        If value is changed, `instance` is deleted from object it was related
        to before and is added to object it will be related to now - `value`.
        """
        super_set = super(RelationshipField, self).__set__
        if not self.reverse_rel_field:
            return super_set(instance, value)

        current_value = getattr(instance, self.name, []) or []
        value = value or []
        super_set(instance, value)

        if value == current_value:
            return

        added_values = set(value) - set(current_value)
        deleted_values = set(current_value) - set(value)
        is_doc = lambda x: isinstance(x, mongo.Document)

        if added_values and all(is_doc(v) for v in added_values):
            for val in added_values:
                self._register_addition_hook(val, instance)

        if deleted_values and all(is_doc(v) for v in deleted_values):
            for val in deleted_values:
                self._register_deletion_hook(val, instance)

    def translate_kwargs(self, kwargs):
        """ For RelationshipField use ReferenceField keys without prefix. """
        kwargs = super(RelationshipField, self).translate_kwargs(kwargs)
        reference_kwargs = {}
        # Extract reference kwargs withour prefix
        fields = ReferenceField._valid_kwargs + ('document', 'ondelete')
        for key in fields:
            if key in kwargs:
                reference_kwargs[key] = kwargs.pop(key)
        reference_field = ReferenceField(**reference_kwargs)
        kwargs['field'] = reference_field
        return kwargs


def Relationship(**kwargs):
    """ Relationship fields' generator.

    Should be used to generate one-to-many and one-to-one relationships.
    Provide `uselist` do say what kind of relation you expect to get.
    If `uselist` is True, then RelationshipField is used and one-to-many
    is created. Otherwise ReferenceField is used and one-to-one is created.

    This is the place where `ondelete` rules kwargs should be passed.
    If you switched from SQLA engine, copy here the same `ondelete` rules
    you passed to SQLA's `ForeignKeyField`.
    `ondelete` kwargs may be kept in both fields with no side-effect when
    switching between sqla-mongo engines.
    """
    uselist = kwargs.pop('uselist', True)
    # many-to-one
    if uselist:
        return RelationshipField(**kwargs)
    # one-to-one
    else:
        fields = ReferenceField._valid_kwargs + (
            'document', 'ondelete')
        fields += tuple(ReferenceField._backref_prefix + f for f in fields)
        fields += (ReferenceField._backref_prefix + 'name',)
        ref_kwargs = {f: kwargs.get(f) for f in fields if f in kwargs}
        return ReferenceField(**ref_kwargs)
