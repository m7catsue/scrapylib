import unittest

from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured
from scrapy.spider import Spider

from scrapylib.autoschedule import AutoSchedule


class AutoScheduleTestCase(unittest.TestCase):

    ext_cls = AutoSchedule

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
        settings = {'AUTOSCHEDULE_ENABLED': True}
        crawler = self._mock_crawler(settings)
        self.assertRaises(NotConfigured, self.ext_cls.from_crawler, crawler)
        settings['AUTOSCHEDULE_APIKEY'] = '123'
        crawler = self._mock_crawler(settings)
        self.assertRaises(NotConfigured, self.ext_cls.from_crawler, crawler)
        settings['AUTOSCHEDULE_SETTINGS'] = {}
        crawler = self._mock_crawler(settings)
        self.assertRaises(NotConfigured, self.ext_cls.from_crawler, crawler)
        # finally enabled
        settings['AUTOSCHEDULE_SETTINGS'] = {'finished': {'foo': []}}
        crawler = self._mock_crawler(settings)
        self.ext_cls.from_crawler(crawler)

    def test_compile_params(self):
        spider = Spider('foo', pstr='string', pint=1, pcall=iter, plist=[])
        jobs = [{
                'spider': 'bar',
                'string': 'pstr',
                'int': 'pint',
                'call': 'pcall',
                'constant': 'constant1',
                }, {
                'spider': 'foo_bar',
                'string': 'pstr',
                'call': 'pcall',
                'list': 'plist',
                'constant': 'constant2',
                }]
        settings = {
                'AUTOSCHEDULE_APIKEY': '123',
                'AUTOSCHEDULE_ENABLED': True,
                'AUTOSCHEDULE_SETTINGS': {
                    'finished': {
                        'foo': jobs
                        }
                    }
                }
        crawler = self._mock_crawler(settings)
        ext = self.ext_cls.from_crawler(crawler)
        params= []
        ext._schedule = lambda y, x: params.append(x)
        ext.spider_closed(spider, 'finished')
        for j, p in zip(jobs, params):
            for k, v in p.items():
                if k == 'parent_job_key':
                    continue

                sv = getattr(spider, j[k], v)
                if not hasattr(sv, '__call__'):
                    self.assertEqual(sv, v)
                else:
                    self.assertNotEqual(sv, k)
