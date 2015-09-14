import logging

from mongoengine import signals
from nefertari.utils import to_dicts


log = logging.getLogger(__name__)


def on_post_save(sender, document, **kw):
    """ Add new document to index or update existing. """
    from nefertari.elasticsearch import ES
    common_kw = {'request': getattr(document, '_request', None)}
    created = kw.get('created', False)
    if created:
        es = ES(document.__class__.__name__)
        es.index(document.to_dict(), **common_kw)
    elif not created and document._get_changed_fields():
        es = ES(document.__class__.__name__)
        es.index(document.to_dict(), **common_kw)
        es.index_relations(document, nested_only=True, **common_kw)


def on_post_delete(sender, document, **kw):
    from nefertari.elasticsearch import ES
    request = getattr(document, '_request', None)
    ES(document.__class__.__name__).delete(
        document.id, request=request)


def on_bulk_update(model_cls, objects, request):
    if not getattr(model_cls, '_index_enabled', False):
        return

    if not objects:
        return

    from nefertari.elasticsearch import ES
    es = ES(source=model_cls.__name__)
    documents = to_dicts(objects)
    es.index(documents, request=request)

    # Reindex relationships
    es.bulk_index_relations(objects, request=request, nested_only=True)


def setup_es_signals_for(source_cls):
    signals.post_save.connect(on_post_save, sender=source_cls)
    signals.post_delete.connect(on_post_delete, sender=source_cls)
    log.info('setup_es_signals_for: %r' % source_cls)
