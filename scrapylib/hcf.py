
"""
HCF Middleware

This SpiderMiddleware uses the HCF backend from hubstorage to retrieve the new
urls to crawl and store back the links extracted.

To activate this middleware it needs to be added to the SPIDER_MIDDLEWARES
list, i.e:

SPIDER_MIDDLEWARES = {
    'scrapylib.hcf.HcfMiddleware': 543,
}

And the next settings need to be defined:

    HS_AUTH, SH_APIKEY - API key
    HS_FRONTIER_ENABLED - True or False
    HS_FRONTIER_PROJECTID, *SCRAPY_PROJECT_ID - Project ID in the panel.
    HS_FRONTIER_ID - Frontier name.
    HS_FRONTIER_SLOT - Slot from where the spider will read new URLs.

*OS environ variable
Note that HS_FRONTIER and HS_SLOT can be overriden from inside a spider using
the spider attributes: "hs_frontier" and "hs_consume_from_slot" respectively.

The next optional settings can be defined:

    HS_ENDPOINT - URL to the API endpoint, i.e: http://localhost:8003.
                  The default value is provided by the python-hubstorage
                  package.

    HS_START_JOB_ENABLED - Enable whether to start a new job when the spider
                           finishes. The default is False

    HS_START_JOB_ON_REASON - This is a list of closing reasons, if the spider ends
                             with any of these reasons a new job will be started
                             for the same slot. The default is ['finished']

    HS_START_JOB_NEW_PANEL - If True the jobs will be started in the new panel.
                             The default is False.

    HS_NUMBER_OF_SLOTS - This is the number of slots that the middleware will
                         use to store the new links. The default is 8.

The next keys can be defined in a Request meta in order to control the behavior
of the HCF middleware:

    use_hcf - If set to True the request will be stored in the HCF.
    hcf_params - Dictionary of parameters to be stored in the HCF with the request
                 fingerprint

        qdata    data to be stored along with the fingerprint in the request queue
        fdata    data to be stored along with the fingerprint in the fingerprint set
        p    Priority - lower priority numbers are returned first. The default is 0

The value of 'qdata' parameter could be retrieved later using
``response.meta['hcf_params']['qdata']``.

The spider can override the default slot assignation function by setting the
spider slot_callback method to a function with the following signature:

   def slot_callback(request):
       ...
       return slot

"""
import hashlib, os
from collections import defaultdict
from datetime import datetime
from collections import deque
import cPickle as pickle
from scrapy import signals, log
from scrapy.exceptions import NotConfigured, DontCloseSpider
from scrapy.http import Request
from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint
from hubstorage import HubstorageClient
from threading import Thread, Event
from Queue import Queue, Empty


class BaseQueue(object):
    """Queue Interface for Middleware"""

    def task_done(self):
        """Should return None, is just to notify the Queue a
        batch had been processed"""
        pass

    def get(self):
        """It should return an iterable of requests"""
        pass

    def put(self, request):
        """It recieve a request and returns None"""
        pass

    def close(self):
        """Returns None just to do closing stuff like flush data"""
        pass


class FrontierQueue(BaseQueue):

    def __init__(self, settings, spider):
        client, frontier, slot, maxsize = self._get_settings(settings, spider)

        self.spider = spider
        self.client = client
        self.frontier = frontier
        self.slot = slot
        self.batch_ids = deque()
        self.already_seen = set()
        self.counter = {'in':0,'out':0}

        self.queue = Queue(maxsize=maxsize)
        self._start_download = Event()
        self._thread = Thread(target=self._download_batches)
        self._thread.daemon = True
        self._thread.start()

    def _get_settings(self, settings, spider):
        endpoint = settings.get('HS_ENDPOINT')
        auth = settings.get('HS_AUTH', settings.get('SH_APIKEY'))
        self.hubstorage = HubstorageClient(auth=auth, endpoint=endpoint)
        project_id = settings.get('HS_FRONTIER_PROJECTID',
                os.environ.get('SCRAPY_PROJECT_ID'))
        project = self.hubstorage.get_project(project_id)
        client = project.frontier
        maxsize = 2
        frontier = getattr(spider, 'hs_frontier', settings.get('HS_FRONTIER'))
        slot = getattr(spider, 'hs_slot', settings.get('HS_SLOT'))
        return client, frontier, slot, maxsize

    def _download_batches(self):
        self._start_download.wait()
        while True:
            for batch in self.client.read(self.frontier, self.slot, mincount=1000):
                print "requests downloaded"
                self.queue.put(batch, block=True)
            self.queue.join()

    def task_done(self):
        self.client.delete(self.frontier, self.slot, self.batch_ids.popleft())
        self.client.flush()
        self.queue.task_done()

    def get(self):
        self._start_download.set()
        try:
            batch = self.queue.get(timeout=3.0)
        except Empty:
            print "w8ing for flush"
            writer = self.client._get_writer(self.frontier, self.slot)
            writer._waitforq()
            batch = self.queue.get(timeout=3.0)
        self.batch_ids.append(batch['id'])
        return (deserializer(request, self.spider) for _, request in batch['requests'])

    def put(self, request):
        fp = request_fingerprint(request)
        if fp in self.already_seen:
            return
        self.counter['in'] += 1
        self.already_seen.add(fp)
        request = {'fp': fp, 'qdata': serializer(request, self.spider)}
        self.client.add(self.frontier, self.slot, [request])

    def close(self):
        print self.counter
        self.client.close()
        self.hubstorage.close()


def deserializer(request, spider):
    return request_from_dict(request, spider)


def serializer(request, spider):
    return request_to_dict(request, spider)


import json

import boto


BATCHSIZE = 400
class SQSQueue(BaseQueue):

    class MockedMessage(object):

        def __init__(self, receipt_handle):
            self.receipt_handle = receipt_handle

    def __init__(self, settings, spider):
        client, maxsize = self._get_settings(settings, spider)

        self.spider = spider
        self.client = client
        self.batch_ids = deque()
        self.already_seen = set()
        self.closed = False
        self.flushme = False
        self.counter = 0

        self.queue = Queue(maxsize=maxsize)
        self._start_download = Event()
        self._downloader = Thread(target=self._download_batches)
        self._downloader.daemon = True
        self._downloader.start()

        self.qup = Queue(maxsize=BATCHSIZE * 2)
        self._upload_batch = Event()
        self._uploader = Thread(target=self._upload_batches)
        self._uploader.daemon = True
        self._uploader.start()

    def _get_settings(self, settings, spider):
        accesskey = settings.get('SQS_ACCESSKEY')
        secretkey = settings.get('SQS_SECRETKEY')
        queuename = getattr(spider, 'sqs_queuename',
                settings.get('SQS_QUEUENAME'))
        client = boto.connect_sqs(accesskey, secretkey).create_queue(queuename, 0)
        maxsize = 2
        return client, maxsize

    def _download_batches(self):
        self._start_download.wait()
        while True:
            if self.closed:
                break
            message = self.client.read(0)
            if message:
                self.queue.put(message, block=True)
            self.queue.join()

    def _upload_batches(self):
        while True:
            if self.closed:
                break
            self._upload_batch.wait()
            self._upload_batch.clear()
            while self.counter >= BATCHSIZE or self.flushme:
                requests = []
                for _ in range(BATCHSIZE):
                    try:
                        requests.append(self.qup.get_nowait())
                        self.counter -= 1
                    except Empty:
                        pass
                if requests:
                    message = self.client.new_message(json.dumps(requests))
                    l = len(message)
                    if l > 1024 * 200:
                        print l
                    self.client.write(message, 0)
                self.flushme = False

    def task_done(self):
        self.client.delete_message(self.MockedMessage(self.batch_ids.popleft()))
        self.queue.task_done()

    def get(self):
        self._start_download.set()
        try:
            message = self.queue.get(timeout=3.0)
        except Empty:
            print "w8ing for flush"
            self.flush()
            message = self.queue.get(timeout=3.0)
        self.batch_ids.append(message.receipt_handle)
        return (deserializer(request, self.spider) for request in json.loads(message.get_body()))

    def put(self, request):
        fp = request_fingerprint(request)
        if fp in self.already_seen:
            return
        self.already_seen.add(fp)
        request = serializer(request, self.spider)
        self.qup.put(request, block=True)
        self.counter += 1
        if self.counter >= BATCHSIZE:
            self._upload_batch.set()

    def flush(self):
        from time import sleep
        self.flushme = True
        self._upload_batch.set()
        while self.counter >= BATCHSIZE or self.flushme:
            sleep(1.0)

    def close(self):
        self.closed = True
        self._upload_batch.set()


class HcfMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('HS_FRONTIER_ENABLED'):
            raise NotConfigured()
        o = cls()
        o.crawler = crawler
        crawler.signals.connect(o.spider_idle, signal=signals.spider_idle)
        return o

    def _get_queue(self, spider):
        settings = spider.settings
        return SQSQueue(settings, spider)

    def process_start_requests(self, start_requests, spider):
        print "START"
        self.queue = self._get_queue(spider)
        self.flag = False
        if getattr(spider, 'use_start_requests', False):
            for request in start_requests:
                yield request
        else:
            for request in self._get_requests():
                yield request

    def _get_requests(self):
        try:
            for request in self.queue.get():
                yield request
        except Empty:
            pass

    def spider_idle(self, spider):
        print "idle"
        if hasattr(self, 'queue'):
            if self.flag:
                self.queue.task_done()
            self.flag = True
            request = None
            for request in self._get_requests():
                self.crawler.engine.crawl(request, spider)
            print request
            if request:
                raise DontCloseSpider
            else:
                print "Queue closed"
                self.queue.close()

    def _process_spider_request(self, response, request, spider):
        if 'dont_hcf' in request.meta:
            return

        self.queue.put(request)

    def process_spider_output(self, response, result, spider):
        results = result
        for result in results:
            if isinstance(result, Request):
                self._process_spider_request(response, result, spider)
            else:
                yield result
