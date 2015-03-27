MongoDB Engine
==============

Common API
==========

**BaseMixin**
    Mixin with a most of the API of *BaseDocument*. *BaseDocument* subclasses from this mixin.

**BaseDocument**
    Base for regular models defined in your application. Just subclass it to define your model's fields. Relevant attributes:
        * **_auth_fields**: String names of fields meant to be displayed to authenticated users.
        * **_public_fields**: String names of fields meant to be displayed to non-authenticated users.
        * **_nested_relationships**: String names of relationship fields that should be included in JSON data of an object as full included documents. If relationship field is not present in this list, this field's value in JSON will be an object's ID or list of IDs.

**ESBaseDocument**
    Subclass of *BaseDocument* instances of which are indexed on create/update/delete.

**ESMetaclass**
    Document metaclass which is used in *ESBaseDocument* to enable automatic indexation to Elasticsearch of documents.

**get_document_cls(name)**
    Helper function used to get the class of document by the name of the class.

**JSONEncoder**
    JSON encoder that should be used to encode output of views.

**ESJSONSerializer**
    JSON encoder used to encode documents prior indexing them in Elasticsearch.

**relationship_fields**
    Tuple of classes that represent relationship fields in specific engine.

**is_relationship_field(field, model_cls)**
    Helper function to determine whether *field* is a relationship field at *model_cls* class.

**relationship_cls(field, model_cls)**
    Return class which is pointed to by relationship field *field* from model *model_cls*.

Fields abstractions
===================

* BigIntegerField
* BooleanField
* DateField
* DateTimeField
* ChoiceField
* FloatField
* IntegerField
* IntervalField
* BinaryField
* DecimalField
* PickleField
* SmallIntegerField
* StringField
* TextField
* TimeField
* UnicodeField
* UnicodeTextField
* Relationship
* PrimaryKeyField
* ForeignKeyField


Documents
---------

.. autoclass:: nefertari_mongodb.documents.BaseMixin
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.documents.BaseDocument
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.documents.ESBaseDocument
    :members:
    :special-members:
    :private-members:


Serializers
-----------

.. autoclass:: nefertari_mongodb.serializers.JSONEncoder
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.serializers.ESJSONSerializer
    :members:
    :special-members:
    :private-members:


Fields
------


.. autoclass:: nefertari_mongodb.fields.IntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.BigIntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.SmallIntegerField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.BooleanField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.DateField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.DateTimeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.FloatField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.StringField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.TextField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.UnicodeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.UnicodeTextField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.ChoiceField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.BinaryField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.DecimalField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.TimeField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.PickleField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.IntervalField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.ListField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.DictField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.PrimaryKeyField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.ForeignKeyField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.ReferenceField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.RelationshipField
    :members:
    :special-members:
    :private-members:

.. autoclass:: nefertari_mongodb.fields.Relationship
    :members:
    :special-members:
    :private-members:
