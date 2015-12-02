import logging
from bson import ObjectId, DBRef

from nefertari.engine.common import (
    JSONEncoder as NefEncoder,
)

log = logging.getLogger(__name__)


class JSONEncoder(NefEncoder):
    def default(self, obj):
        if isinstance(obj, (ObjectId, DBRef)):
            return str(obj)
        return super(JSONEncoder, self).default(obj)
