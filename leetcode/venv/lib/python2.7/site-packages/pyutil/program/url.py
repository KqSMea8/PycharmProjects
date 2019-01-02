import urllib2, cookielib, logging

def get_redirect_url(url, http_proxy=None, https_proxy=None, timeout=30, header=None):
    try:
        request = urllib2.Request(url)
        if header:
            for key, value in header.items():
                request.add_header(key, value)
        cj = cookielib.CookieJar()
        if http_proxy or https_proxy:
            proxy_ip = {}
            if http_proxy:
                proxy_ip['http'] = http_proxy
            if https_proxy:
                proxy_ip['https'] = https_proxy

            proxy = urllib2.ProxyHandler(proxy_ip)
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj), proxy)
        else:
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        urllib2.install_opener(opener)
        response = urllib2.urlopen(request, None, timeout=timeout)
        return 'success', response.geturl(), None
    except urllib2.HTTPError, e:
        redirect_url = e.url or None
        return 'fail: HTTPError', redirect_url, e.code
    except UnicodeEncodeError:
        return 'fail: UnicodeEncodeError', None, None
    except Exception, e:
        logging.info('get_redirect_url Exception, e=%s', e)
        return 'fail: Exception', None, None

