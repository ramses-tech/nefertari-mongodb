import logging

from mongoengine import signals


log = logging.getLogger(__name__)


def on_pre_save(sender, document, **kw):
    from nefertari.elasticsearch import ES
    if not kw.get('created', False) and document._get_changed_fields():
        ES(document.__class__.__name__).index(document.to_dict())


def on_post_save(sender, document, **kw):
    from nefertari.elasticsearch import ES
    if kw.get('created', False):
        ES(document.__class__.__name__).index(document.to_dict())


def on_delete(sender, document, **kw):
    from nefertari.elasticsearch import ES
    ES(document.__class__.__name__).delete(document.id)


def setup_es_signals_for(source_cls):
    signals.post_save.connect(on_post_save, sender=source_cls)
    signals.pre_save_post_validation.connect(on_pre_save, sender=source_cls)
    signals.post_delete.connect(on_delete, sender=source_cls)
    log.info('setup_es_signals_for: %r' % source_cls)
