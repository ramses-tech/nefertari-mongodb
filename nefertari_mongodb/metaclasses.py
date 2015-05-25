from mongoengine import Document
from mongoengine.queryset import DO_NOTHING

from .signals import setup_es_signals_for
from .fields import ReferenceField, RelationshipField


class DocumentMetaclass(Document.my_metaclass):
    """ Custom metaclass that supports backreferences generation.

    The feature of this metaclass is that it creates a backreference
    for `ReferenceField` and `RelationshipField`(which is a ListField of
    ReferenceFields internally). Backrefs are created for the 'other'(backref)
    side the of relationship when the document class at the 'origin side'
    (the side which defines the original relationship) is being created.

    A backreference is created if the field defining a relationship provides
    an additional set of arguments for backreference that are prefixed with
    `ReferenceField._backref_prefix`. Arguments are the same as for
    `Relationship`, except:
        1. `document` is not a valid argument for backref;
        2. The `name` backref argument must be provided and should be the name
            of the backreference field that will be created on the other side
            of relationship.

    Follow inline comments in the code to understand how the process of backref
    creation works. Check `mongoengine/base/metaclasses.py` for the original
    code of this metaclass.
    """

    def __init__(self, name, bases, attrs):
        """ Override new class initialization to create backreferences.

        """
        super(DocumentMetaclass, self).__init__(name, bases, attrs)
        for field_name, field in self._fields.items():

            # Field is not a relationship field
            if not isinstance(field, (ReferenceField, RelationshipField)):
                continue

            # Field has no backreference kwargs, thus does not use
            # backreference
            if not (hasattr(field, 'backref_kwargs') and field.backref_kwargs):
                continue

            # Prepare kwargs for new ReferenceField
            backref_kw = field.backref_kwargs.copy()
            backref_name = backref_kw.pop('name')
            backref_kw['document'] = self.__name__

            # Create backref ReferenceField. Set its name and `db_field` prop
            backref_field = ReferenceField(**backref_kw)
            backref_field.name = backref_name
            if not backref_field.db_field:
                backref_field.db_field = backref_name

            # Set field's `owner_document`
            if isinstance(field, RelationshipField):
                target_cls = field.field.document_type
            else:
                target_cls = field.document_type
            backref_field.owner_document = target_cls

            # Set `reverse_rel_field` for both fields, so they know name of the
            # field that is used on the other side of relationship
            backref_field.reverse_rel_field = field_name
            field.reverse_rel_field = backref_name

            # Add field to target class
            target_cls._fields[backref_name] = backref_field

            # Add new field to `_fields_ordered`
            if (backref_name in target_cls._fields and
                    backref_name not in target_cls._fields_ordered):
                fields = list(target_cls._fields_ordered) + [backref_name]
                target_cls._fields_ordered = sorted(fields)

            # Set new field as an attribute of target class
            setattr(target_cls, backref_name, backref_field)

            # Register reverse deletion rules
            delete_rule = getattr(backref_field, 'reverse_delete_rule',
                                  DO_NOTHING)
            if delete_rule != DO_NOTHING:
                self.register_delete_rule(
                    target_cls,
                    backref_name,
                    delete_rule)


class ESMetaclass(DocumentMetaclass):
    def __init__(self, name, bases, attrs):
        self._index_enabled = True
        setup_es_signals_for(self)
        return super(ESMetaclass, self).__init__(name, bases, attrs)
