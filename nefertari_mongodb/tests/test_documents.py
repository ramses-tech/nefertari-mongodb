import pytest
from mock import patch, Mock, call

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

        class MyModel2(docs.BaseDocument):
            _nested_relationships = ['child']
            __tablename__ = 'mymodel2'
            name = fields.StringField(primary_key=True)
            child = fields.Relationship(
                document='MyModel', backref_name='parent',
                uselist=False, backref_uselist=False)

        assert MyModel.get_es_mapping() == {
            'mymodel': {
                'properties': {
                    '_type': {'type': 'string'},
                    '_version': {'type': 'long'},
                    'groups': {'type': 'long'},
                    'id': {'type': 'string'},
                    'my_id': {'type': 'string'},
                    'name': {'type': 'string'},
                    'parent': {'type': 'string'},
                    'status': {'type': 'string'},
                    'updated_at': {'format': 'dateOptionalTime',
                                   'type': 'date'}
                }
            }
        }

        assert MyModel2.get_es_mapping() == {
            'mymodel2': {
                'properties': {
                    '_type': {'type': 'string'},
                    '_version': {'type': 'long'},
                    'id': {'type': 'string'},
                    'name': {'type': 'string'},
                    'child': {'type': 'object'},
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

    def test_is_modified_no_changed_fields(self):
        obj = docs.BaseMixin()
        obj.pk_field = Mock(return_value='id')
        obj._get_changed_fields = Mock(return_value=[])
        obj.id = 1
        assert not obj._is_modified()

    def test_is_modified(self):
        obj = docs.BaseMixin()
        obj.pk_field = Mock(return_value='id')
        obj._get_changed_fields = Mock(return_value=['name'])
        obj.id = 1
        assert obj._is_modified()


class TestBaseDocument(object):

    def test_apply_before_validation_new_object(self):
        processor = Mock(return_value='Foo')
        processor2 = Mock(return_value='BarFoo')

        class MyModel(docs.BaseDocument):
            name = fields.StringField(
                before_validation=[processor],
                after_validation=[processor2])
            email = fields.StringField(before_validation=[processor])

        obj = MyModel(name='a', email='b')
        obj.apply_before_validation()
        processor.assert_has_calls([
            call(instance=obj, new_value='b'),
            call(instance=obj, new_value='a'),
        ], any_order=True)
        assert obj.name == 'Foo'
        assert obj.email == 'Foo'

    def test_apply_before_validation_updated_object(self):
        processor = Mock(return_value='Foo')
        processor2 = Mock(return_value='BarFoo')

        class MyModel(docs.BaseDocument):
            name = fields.StringField(
                before_validation=[processor],
                after_validation=[processor2])
            email = fields.StringField(before_validation=[processor])

        obj = MyModel(name='a', email='b')

        obj.name = 'asdasd'
        obj._get_changed_fields = Mock(return_value=['name'])
        obj._created = False
        obj.apply_before_validation()
        processor.assert_has_calls([
            call(instance=obj, new_value='asdasd'),
        ], any_order=True)
        assert obj.name == 'Foo'

    def test_apply_after_validation(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()

        obj = MyModel()
        obj._fields_to_process = ['id', 'name']
        obj.apply_processors = Mock()
        obj.apply_after_validation()
        obj.apply_processors.assert_called_once_with(
            ['id', 'name'], after=True)

    def test_apply_before_validation(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()

        obj = MyModel()
        obj._get_changed_fields = Mock(return_value=['name'])
        obj._created = False
        obj.apply_processors = Mock()
        obj.apply_before_validation()
        obj.apply_processors.assert_called_once_with(
            ['name'], before=True)

    def test_apply_processors(self):
        def processor1(instance, new_value):
            return new_value + '-'

        def processor2(instance, new_value):
            return new_value + '+'

        class MyModel(docs.BaseDocument):
            name = fields.StringField(
                before_validation=[processor1],
                after_validation=[processor2])

        obj = MyModel(name='foo')
        obj.apply_processors(before=True)
        assert obj.name == 'foo-'

        obj.apply_processors(after=True)
        assert obj.name == 'foo-+'

        obj.apply_processors(field_names=['name'], before=True, after=True)
        assert obj.name == 'foo-+-+'

    def test_validate(self):
        class MyModel(docs.BaseDocument):
            name = fields.StringField()

        obj = MyModel(name='asdasd')
        obj.apply_before_validation = Mock()
        obj.apply_after_validation = Mock()
        obj.validate()
        obj.apply_before_validation.assert_called_once_with()
        obj.apply_after_validation.assert_called_once_with()

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
