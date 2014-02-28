import os

import scrapinghub
from scrapy import log
from scrapy import signals
from scrapy.exceptions import NotConfigured


class AutoSchedule(object):
    '''
    AUTOSCHEDULE_SETTINGS:
        {reason: {spider_name: [{**params}, {...}]}}
    '''

    def __init__(self, settings):
        self.settings = settings.getdict('AUTOSCHEDULE_SETTINGS')
        self.apikey = settings.get('AUTOSCHEDULE_APIKEY')
        if not self.settings.values() or not self.apikey:
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool('AUTOSCHEDULE_ENABLED'):
            raise NotConfigured
        o = cls(settings)
        crawler.signals.connect(o.spider_closed,
                signal=signals.spider_closed)
        return o

    def _compile_params(self, settings_params, spider):
        params = {}
        for k, v in settings_params.items():
            if isinstance(v, basestring):
                new_v = getattr(spider, v, v)
                params[k] = v if hasattr(new_v, '__call__') else new_v
            else:
                params[k] = v
        return params

    def spider_closed(self, spider, reason):
        settings = self.settings.get(reason).get(spider.name)
        if not settings:
            return

        _compiled_params_list = []
        for params in settings:
            params = self._compile_params(params, spider)
            _compiled_params_list.append(params)

        conn = scrapinghub.Connection(self.apikey)
        project = conn[os.environ.get('SCRAPY_PROJECT_ID')]
        current_job_key = os.environ.get('SCRAPY_JOB')
        for settings_params in _compiled_params_list:
            params = {'parent_job_key': current_job_key}
            params.update(settings_params)
            self._schedule(project, params)

    def _schedule(self, project, params):
        job = project.schedule(**params)
        log.msg('Scheduled {spider} spider, job {job}, params {params}'\
                .format(job=job, params=params, **params), level=log.INFO)
