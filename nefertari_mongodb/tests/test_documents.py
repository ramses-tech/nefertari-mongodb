import pytest
from mock import patch, Mock

import mongoengine as mongo
from nefertari.utils.dictset import dictset
from nefertari.json_httpexceptions import JHTTPBadRequest

from .. import documents as docs
from .. import fields


class TestDocumentHelpers(object):

    @patch.object(docs.mongo.document, 'get_document')
    def test_get_document_cls(self, mock_get):
        mock_get.return_value = 'foo'
        assert docs.get_document_cls('MyModel') == 'foo'
        mock_get.assert_called_once_with('MyModel')

    @patch.object(docs.mongo.base, 'common')
    def test_get_document_classes(self, mock_common):
        bar_mock = Mock(_meta={'abstract': False})
        mock_common._document_registry = {
            'Foo': Mock(_meta={'abstract': True}),
            'Bar': bar_mock,
            'Zoo': Mock(_meta={}),
        }
        document_classes = docs.get_document_classes()
        assert document_classes == {'Bar': bar_mock}

    @patch.object(docs.mongo.document, 'get_document')
    def test_get_document_cls_error(self, mock_get):
        mock_get.side_effect = Exception()
        with pytest.raises(ValueError) as ex:
            docs.get_document_cls('MyModel')
        mock_get.assert_called_once_with('MyModel')
        assert str(ex.value) == '`MyModel` does not exist in mongo db'

    def test_process_lists(self):
        test_dict = dictset(
            id__in='1,   2, 3',
            name__all='foo',
            other__arg='4',
            yet_other_arg=5,
        )
        result_dict = docs.process_lists(test_dict)
        expected = dictset(
            id__in=['1', '2', '3'],
            name__all=['foo'],
            other__arg='4',
            yet_other_arg=5,
        )
        assert result_dict == expected

    def test_process_bools(self):
        test_dict = dictset(
            complete__bool='false',
            other_arg=5,
        )
        result_dict = docs.process_bools(test_dict)
        assert result_dict == dictset(complete=False, other_arg=5)


class TestBaseMixin(object):

    @patch('nefertari.elasticsearch.engine')
    def test_get_es_mapping(self, mock_conv):
        class MyModel(docs.BaseDocument):
            my_id = fields.IdField()
            name = fields.StringField(primary_key=True)
            status = fields.ChoiceField(choices=['active'])
            groups = fields.ListField(item_type=fields.IntegerField)
        mapping = MyModel.get_es_mapping()
        assert mapping == {
            'mymodel': {
                'properties': {
                    '_type': {'type': 'string'},
                    '_version': {'type': 'long'},
                    'groups': {'type': 'long'},
                    'id': {'type': 'string'},
                    'my_id': {'type': 'string'},
                    'name': {'type': 'string'},
                    'status': {'type': 'string'},
                    'updated_at': {'format': 'dateOptionalTime',
                                   'type': 'date'}
                }
            }
        }

    def test_pk_field(self):
        class MyModel(docs.BaseDocument):
            my_id = fields.IdField()
            name = fields.StringField(primary_key=True)

        assert MyModel.pk_field() == 'name'

    def test_check_fields_allowed_not_existing_field(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()

        with pytest.raises(JHTTPBadRequest) as ex:
            MyModel.check_fields_allowed(('id__in', 'name', 'description'))
        assert "'MyModel' object does not have fields" in str(ex.value)
        assert 'description' in str(ex.value)
        assert 'name' not in str(ex.value)

    def test_check_fields_allowed(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()
        try:
            MyModel.check_fields_allowed(('id__in', 'name'))
        except JHTTPBadRequest:
            raise Exception('Unexpected JHTTPBadRequest exception raised')

    def test_check_fields_allowed_dymanic_doc(self):
        class MyModel(docs.BaseMixin, mongo.DynamicDocument):
            name = fields.StringField()
        try:
            MyModel.check_fields_allowed(('id__in', 'name', 'description'))
        except JHTTPBadRequest:
            raise Exception('Unexpected JHTTPBadRequest exception raised')

    def test_filter_fields(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()
        params = MyModel.filter_fields(dictset(
            description='nice',
            name='regular name',
            id__in__here=[1, 2, 3],
        ))
        assert params == dictset(
            name='regular name',
            id__in__here=[1, 2, 3],
        )

    def test_apply_fields(self):
        query_set = Mock()
        _fields = ['name', 'id', '-title']
        docs.BaseDocument.apply_fields(query_set, _fields)
        query_set.only.assert_called_once_with('name', 'id')
        query_set.only().exclude.assert_called_once_with('title')

    def test_apply_sort(self):
        query_set = Mock()
        docs.BaseDocument.apply_sort(query_set, ['name', 'id'])
        query_set.order_by.assert_called_once_with('name', 'id')

    def test_apply_sort_no_sort(self):
        query_set = Mock()
        docs.BaseDocument.apply_sort(query_set, [])
        assert not query_set.order_by.called

    def test_count(self):
        query_set = Mock()
        docs.BaseDocument.count(query_set)
        query_set.count.assert_called_once_with(
            with_limit_and_skip=True)

    def test_get_null_values(self):
        class MyModel1(docs.BaseDocument):
            name = fields.StringField(primary_key=True)

        class MyModel2(docs.BaseDocument):
            name = fields.StringField(primary_key=True)
            models1 = fields.Relationship(
                document='MyModel1', backref_name='model2')

        assert MyModel1.get_null_values() == {
            '_version': None,
            'name': None,
            'model2': None,
            'updated_at': None,
        }

        assert MyModel2.get_null_values() == {
            '_version': None,
            'models1': [],
            'name': None,
            'updated_at': None,
        }
