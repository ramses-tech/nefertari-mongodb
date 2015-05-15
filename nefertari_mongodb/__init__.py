import logging

import mongoengine

from .documents import (
    BaseDocument, ESBaseDocument, BaseMixin,
    get_document_cls)
from .serializers import JSONEncoder, ESJSONSerializer
from .metaclasses import ESMetaclass
from .utils import (
    relationship_fields, is_relationship_field,
    get_relationship_cls)
from .fields import (
    BigIntegerField,
    BooleanField,
    DateField,
    DateTimeField,
    ChoiceField,
    FloatField,
    IntegerField,
    IntervalField,
    BinaryField,
    DecimalField,
    PickleField,
    SmallIntegerField,
    StringField,
    TextField,
    TimeField,
    UnicodeField,
    UnicodeTextField,
    Relationship,
    IdField,
    ForeignKeyField,

    ListField,
    DictField,
)


log = logging.getLogger(__name__)


def includeme(config):
    """ Include required packages. """
    pass


def setup_database(config):
    """ Setup db engine, db itself. Create db if not exists. """
    settings = config.registry.settings
    mongoengine.connect(settings['mongodb.db'],
                        host=settings['mongodb.host'],
                        port=int(settings['mongodb.port']))
