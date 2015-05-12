from .fields import (RelationshipField, ReferenceField)

relationship_fields = (RelationshipField, ReferenceField)


def is_relationship_field(field, model_cls):
    """ Determine if `field` of the `model_cls` is a relational
    field.
    """
    if not model_cls.has_field(field):
        return False
    field_obj = model_cls._fields[field]
    return isinstance(field_obj, relationship_fields)


def relationship_cls(field, model_cls):
    """ Return class which is pointed to by relationship field
    `field` from model `model_cls`.

    You have to make sure field exists and is a relationship
    field by yourself. Use `is_relationship_field` for these purposes.
    """
    field_obj = model_cls._fields[field]
    if isinstance(field_obj, RelationshipField):
        field_obj = getattr(field_obj, 'field')
    return getattr(field_obj, 'document_type')
