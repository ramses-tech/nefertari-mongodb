import logging
from bson import ObjectId, DBRef

from nefertari.engine.common import (
    JSONEncoder as NefEncoder,
    ESJSONSerializer as NefESEncoder,
)

log = logging.getLogger(__name__)


class JSONEncoderMixin(object):
    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)
        return super(JSONEncoderMixin, self).default(obj)


class JSONEncoder(JSONEncoderMixin, NefEncoder):
    pass


class ESJSONSerializer(JSONEncoderMixin, NefESEncoder):
    pass
