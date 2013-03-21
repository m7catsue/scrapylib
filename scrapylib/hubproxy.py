import re

from w3lib.http import basic_auth_header
from scrapy.utils.httpobj import urlparse_cached
from scrapy import log, signals

class HubProxyMiddleware(object):

    url = 'http://proxy.scrapinghub.com:8010'
    maxbans = 20
    ban_code = 503
    download_timeout = 1800
    allowed_domains = []

    @classmethod
    def from_crawler(cls, crawler):
        o = cls()
        o.crawler = crawler
        o._bans = 0
        crawler.signals.connect(o.open_spider, signals.spider_opened)
        return o

    def open_spider(self, spider):
        self.enabled = self.is_enabled(spider)
        if not self.enabled:
            return

        for k in ('user', 'pass', 'url', 'maxbans', 'download_timeout',
                'allowed_domains'):
            o = getattr(self, k, None)
            s = self.crawler.settings.get('HUBPROXY_' + k.upper(), o)
            v = getattr(spider, 'hubproxy_' + k, s)
            setattr(self, k, v)

        self._proxyauth = self.get_proxyauth(spider)

        self.host_regexes = self.get_host_regex(self.allowed_domains)
        self.domains_seen = set()
        log.msg("Using hubproxy at %s (user: %s)" % (self.url, self.user), spider=spider)

    def is_enabled(self, spider):
        """Hook to enable middleware by custom rules"""
        return getattr(spider, 'use_hubproxy', False) \
                or self.crawler.settings.getbool("HUBPROXY_ENABLED")

    def get_proxyauth(self, spider):
        """Hook to compute Proxy-Authorization header by custom rules"""
        return basic_auth_header(self.user, getattr(self, 'pass'))

    def process_request(self, request, spider):
        if self.enabled:
            if (self.should_follow(request) \
                    and not request.meta.get('dont_proxy'))\
                    or request.dont_filter:
                request.meta['proxy'] = self.url
                request.meta['download_timeout'] = self.download_timeout
                request.headers['Proxy-Authorization'] = self._proxyauth
            else:
                if request.meta.get('proxy') == self.url:
                    del request.meta['proxy']
                    del request.meta['download_timeout']
                    del request.headers['Proxy-Authorization']

    def process_response(self, request, response, spider):
        if self.enabled:
            if request.meta.get('proxy') == self.url:
                if response.status == self.ban_code:
                    self._bans += 1
                    if self._bans > self.maxbans:
                        self.crawler.engine.close_spider(spider, 'banned')
                else:
                    self._bans = 0
                response.flags.append('proxied')
        return response

    def get_host_regex(self, allowed_domains):
        """Override this method to implement a different offsite policy"""
        if not allowed_domains:
            return re.compile('') # allow all by default
        domains = [d.replace('.', r'\.') for d in allowed_domains]
        regex = r'^(.*\.)?(%s)$' % '|'.join(domains)
        return re.compile(regex)

    def should_follow(self, request):
        regex = self.host_regexes
        # hostanme can be None for wrong urls (like javascript links)
        host = urlparse_cached(request).hostname or ''
        return bool(regex.search(host))
