import mongoengine as mongo

from .. import documents as docs
from .. import fields


class TestDocumentMetaclass(object):
    def test_id_field_set_to_meta(self):
        class Foo(docs.BaseDocument):
            id = fields.IdField()

        field = Foo._fields['id']
        assert isinstance(field, fields.IdField)
        assert Foo._meta['id_field'] == 'id'

    def test_id_field_not_set_to_meta(self):
        class Foo(docs.BaseDocument):
            id = fields.IdField()
            name = fields.StringField(primary_key=True)

        field = Foo._fields['id']
        assert isinstance(field, mongo.fields.ObjectIdField)
        assert Foo._meta['id_field'] == 'name'
