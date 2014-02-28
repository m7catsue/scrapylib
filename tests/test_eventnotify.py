import unittest

from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured

from scrapylib.eventnotify import EventNotify, event_signal


class EventNotifyTestCase(unittest.TestCase):

    ext_cls = EventNotify

    def _mock_crawler(self, settings=None):
        class MockedDownloader(object):
            slots = {}

        class MockedEngine(object):
            downloader = MockedDownloader()
            fake_spider_closed_result = None

            def close_spider(self, spider, reason):
                self.fake_spider_closed_result = (spider, reason)

        crawler = get_crawler(settings)
        crawler.engine = MockedEngine()
        return crawler

    def test_enabled(self):
        settings = {'EVENTNOTIFY_ENABLED': True}
        crawler = self._mock_crawler(settings)
        self.assertRaises(NotConfigured, self.ext_cls.from_crawler, crawler)
        settings['EVENTNOTIFY_SESID'] = 'id_123'
        settings['EVENTNOTIFY_SESKEY'] = 'key_123'
        crawler = self._mock_crawler(settings)
        self.assertRaises(NotConfigured, self.ext_cls.from_crawler, crawler)
        settings['EVENTNOTIFY_FROM'] = 'info@scrapinghub.com'
        settings['EVENTNOTIFY_TO'] = 'someone@domain.com'
        crawler = self._mock_crawler(settings)
        self.ext_cls.from_crawler(crawler)

    def test_event_signal(self):
        settings = {
                'EVENTNOTIFY_TO': 'someone@domain.com',
                'EVENTNOTIFY_FROM': 'info@scrapinghub.com',
                'EVENTNOTIFY_SESID': 'id_123',
                'EVENTNOTIFY_SESKEY': 'key_123',
                'EVENTNOTIFY_ENABLED': True,
                }

        class MockedEventNotify(self.ext_cls):

            def __init__(self, *args, **kwargs):
                super(MockedEventNotify, self).__init__(*args, **kwargs)
                self.last_event = None

            def notify(self, **kwargs):
                self.last_event = kwargs

        crawler = self._mock_crawler(settings)
        ext = MockedEventNotify.from_crawler(crawler)
        crawler.signals.send_catch_log(signal=event_signal, message='scrapy')
        self.assertEqual('scrapy', ext.last_event.get('message'))
