import boto
from scrapy.exceptions import NotConfigured


# signal
event_signal = object()


class EventNotify(object):

    def __init__(self, settings):
        self._ses_credentials = (settings.get('EVENTNOTIFY_SESID'),
                settings.get('EVENTNOTIFY_SESKEY'))
        self._from = settings.get('EVENTNOTIFY_FROM', '')
        self._to = settings.getlist('EVENTNOTIFY_TO', [])
        self._subject = settings.get('EVENTNOTIFY_SUBJECT', '')
        self._body = settings.get('EVENTNOTIFY_BODY', '')
        if not (all(self._ses_credentials) and self._to and self._from):
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool('EVENTNOTIFY_ENABLED'):
            raise NotConfigured
        o = cls(settings)
        crawler.signals.connect(o.notify, signal=event_signal)
        return o

    def notify(self, **kwargs):
        ses = boto.connect_ses(*self._ses_credentials)
        ses.send_email(self._from, self._subject.format(**kwargs),
                self._body.format(**kwargs), self._to)
