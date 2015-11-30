import logging

from mongoengine import signals
from nefertari.engine import sync_events


log = logging.getLogger(__name__)


def on_post_save(sender, document, **kw):
    """ Add new document to index or update existing. """
    request = getattr(document, '_request', None)
    created = kw.get('created', False)
    if created:
        event_cls = sync_events.ItemCreated
    elif not created and document._get_changed_fields():
        event_cls = sync_events.ItemUpdated
    if request is not None:
        request.registry.notify(event_cls(item=document))


def on_post_delete(sender, document, **kw):
    request = getattr(document, '_request', None)
    if request is not None:
        event = sync_events.ItemDeleted(item=document)
        request.registry.notify(event)


def on_bulk_update(model_cls, objects, request):
    if not getattr(model_cls, '_index_enabled', False):
        return

    if not objects:
        return

    if request is not None:
        event = sync_events.BulkUpdated(items=list(objects))
        request.registry.notify(event)


def setup_signals_for(source_cls):
    signals.post_save.connect(on_post_save, sender=source_cls)
    signals.post_delete.connect(on_post_delete, sender=source_cls)
    log.info('setup_signals_for: %r' % source_cls)
