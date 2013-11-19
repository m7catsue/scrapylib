
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
import time
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


class BaseRequestEncoder(object):

    def encode(self, request):
        """Returns string object representing the request"""
        raise NotImplementedError

    def decode(self, request):
        """Returns scrapy.http.Request object from a string"""
        raise NotImplementedError


class JsonRequestEncoder(BaseRequestEncoder):

    def __init__(self, spider):
        self.spider = spider

    def encode(self, request):
        return json.dumps(request_to_dict(request, self.spider))

    def decode(self, request):
        return request_from_dict(json.loads(request), self.spider)


class BaseQueue(object):
    """Queue Interface for Middleware"""

    def __init__(self, settings, spider):
        pass

    def task_done(self):
        """Should return None, is just to notify the Queue a
        batch had been processed"""
        raise NotImplementedError

    def get(self):
        """It should return an iterable of requests"""
        raise NotImplementedError

    def put(self, request):
        """It recieve a request and returns None"""
        raise NotImplementedError

    def flush(self):
        """Returns None. Blocking call that force to upload
        requests on the queue"""
        raise NotImplementedError

    def close(self):
        """Returns None, just to do closing stuff like flush data"""
        raise NotImplementedError

    def delete(self):
        """Returns None. Blocking call that reset the queue"""
        raise NotImplementedError


class FrontierQueue(BaseQueue):

    def __init__(self, settings, spider):
        endpoint, auth, project_id, frontier, slots, maxsize = \
                self._get_settings(settings, spider)

        self.encoder = JsonRequestEncoder(spider)
        self.hubstorage = HubstorageClient(auth=auth, endpoint=endpoint)
        self.project = self.hubstorage.get_project(project_id)
        self.client = self._get_client(settings, spider)
        self.frontier = frontier
        self.all_slots = deque(range(slots))
        self.read_slot = self._get_read_slot(spider)
        self.batch_ids = deque()
        self.all_slots.rotate(-self.read_slot)
        self.counter = 0

        self.queue = Queue(maxsize=maxsize)
        self._start_download = Event()
        self._thread = Thread(target=self._download_batches)
        self._thread.daemon = True
        self._thread.start()

    def _get_settings(self, settings, spider):
        endpoint = settings.get('HS_ENDPOINT')
        auth = settings.get('HS_AUTH')
        project_id = settings.get('HS_FRONTIER_PROJECTID',
                os.environ.get('SCRAPY_PROJECT_ID'))
        maxsize = 2
        frontier = getattr(spider, 'hs_frontier',
                settings.get('HS_FRONTIER', spider.name))
        slots = getattr(spider, 'hs_totalslots',
                settings.getint('HS_TOTALSLOTS', 1))
        return endpoint, auth, project_id, frontier, slots, maxsize

    def _get_client(self, settings, spider):
        client = self.project.frontier
        client.batch_size = 100
        return client

    def _get_read_slot(self, spider):
        project = self.project
        spider_id = project.ids.spider(spider.name)
        job_key = os.environ.get('SCRAPY_JOB')
        slot = None
        while slot is None:
            jobq = project.jobq.summary('running', spider_id)
            slots = []
            for summary in jobq['summary']:
                if summary['key'] == job_key:
                    continue
                tag = None
                for tag in summary['tags']:
                    if tag.startswith('slot='):
                        slots.append(tag.replace('slot=', ''))
                if not tag:
                    break
            else:
                slot = deque(x for x in self.all_slots
                        if x not in slots).popleft()
        if job_key:
            jobmeta = project.get_job(job_key).jobmetadata
            jobmeta['tags'].append('slot={}'.format(slot))
            jobmeta.save()
        return slot

    def _download_batches(self):
        self._start_download.wait()
        while True:
            for batch in self.client.read(self.frontier, self.read_slot, mincount=400):
                self.queue.put(batch, block=True)
            self.queue.join()

    def task_done(self):
        self.client.delete(self.frontier, self.read_slot, self.batch_ids.popleft())
        self.queue.task_done()

    def get(self):
        self._start_download.set()
        try:
            batch = self.queue.get(timeout=3.0)
        except Empty:
            self.flush()
            batch = self.queue.get(timeout=3.0)
        self.batch_ids.append(batch['id'])
        decode = self.encoder.decode
        return (decode(request) for _, request in batch['requests'])

    def put(self, request):
        request = {'fp': request_fingerprint(request),
                   'qdata': self.encoder.encode(request)}
        self.client.add(self.frontier, self.all_slots[0], [request])
        self.counter += 1
        if self.counter % 100 == 0:
            self.all_slots.rotate(-1)

    def flush(self):
        self.client.flush()

    def close(self):
        self.client.close()
        self.hubstorage.close()

    def delete(self):
        for slot in self.all_slots:
            self.client.delete_slot(self.frontier, slot)


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
            self.flush()
            message = self.queue.get(timeout=3.0)
        self.batch_ids.append(message.receipt_handle)
        return json.loads(message.get_body())

    def put(self, request):
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


class ExternalRequestTrackingMiddleware(object):

    def __init__(self, settings):
        self.debug = settings.getbool('ERT_DEBUG')
        if settings.getbool('ERT_FILTER_DUPLICATES'):
            self.already_seen = []

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('ERT_ENABLED'):
            raise NotConfigured
        o = cls(crawler.settings)
        o.crawler = crawler
        o.task_scheduled = False
        crawler.signals.connect(o.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(o.engine_stopped, signal=signals.engine_stopped)
        return o

    def _get_queue(self, spider):
        settings = spider.settings
        return FrontierQueue(settings, spider)

    def process_start_requests(self, start_requests, spider):
        self.queue = self._get_queue(spider)
        if getattr(spider, 'ert_start', False):
            self.queue.delete()
            for request in start_requests:
                yield request
        else:
            for request in self._get_requests():
                yield request

    def _get_requests(self):
        start = time.time()
        try:
            for idx, request in enumerate(self.queue.get(), start=1):
                yield request
        except Empty:
            return
        self.task_scheduled = True
        if self.debug:
            log.msg('Got {} requests in {} seconds'.format(idx,
                time.time() - start), level=log.INFO)

    def spider_idle(self, spider):
        if self.task_scheduled:
            self.queue.task_done()
            self.task_scheduled = False
        request = None
        for request in self._get_requests():
            self.crawler.engine.crawl(request, spider)
        if request:
            raise DontCloseSpider

    def _process_spider_request(self, response, request, spider):
        if 'dont_ert' in request.meta:
            return request

        if hasattr(self, 'already_seen'):
            fp = request_fingerprint(request)
            if fp in self.already_seen:
                return
            self.already_seen.append(fp)

        self.queue.put(request)

    def process_spider_output(self, response, result, spider):
        for x in result:
            if isinstance(x, Request):
                yield self._process_spider_request(response, x, spider)
            else:
                yield x

    def engine_stopped(self):
        self.queue.close()
