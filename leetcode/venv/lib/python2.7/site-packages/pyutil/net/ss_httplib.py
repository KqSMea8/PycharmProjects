#!coding=utf8
# Wrapper module for httplib, providing some additional facilities

import sys
import time
from httplib import HTTPConnection
from _socket import getaddrinfo, SOCK_STREAM, error
from socket import _socketobject, _GLOBAL_DEFAULT_TIMEOUT

def ss_get_sa(address, skfamily, sktype):
    '''get socket address info, the address should be
    a tuple of (domain, port)'''
    host, port = address
    return getaddrinfo(host, port, skfamily, sktype)

def ss_create_connection(skinfos, timeout=_GLOBAL_DEFAULT_TIMEOUT, source_address=None):
    err = None
    for skinfo in skinfos:
        af, socktype, proto, canonname, sa = skinfo
        sock = None
        try:
            sock = _socketobject(af, socktype, proto)
            if timeout is not _GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)
            if source_address:
                sock.bind(source_address)
            sock.connect(sa)
            return sock

        except error as _:
            err = _
            if sock is not None:
                sock.close()
    if err is not None:
        raise err
    else:
        raise error("getaddrinfo returns an empty list")
    if err is not None:
        raise err
    else:
        raise error("getaddrinfo returns an empty list")

class SS_HTTPConnection(HTTPConnection):

    def __init__(self, host, port=None, strict=None,
                 timeout=_GLOBAL_DEFAULT_TIMEOUT, source_address=None,
                 server_address=None):
        self.server_address=server_address
        try:
            super(SS_HTTPConnection, self).__init__(host, port, strict, timeout, source_address)
        except TypeError:
            HTTPConnection.__init__(self, host, port, strict, timeout, source_address)

    def get_sa(self):
        if self.server_address:
            address = (self.server_address, self.port)
        else:
            address = (self.host, self.port)
        self.skinfos = ss_get_sa(address, 0, SOCK_STREAM)

    def ss_connect(self):
        """Connect to the host and port specified in __init__."""
        self.sock = ss_create_connection(self.skinfos, self.timeout, self.source_address)

        if self._tunnel_host:
            self._tunnel()

#thi is a simple test for ss_httplib
def http_download(url=""):
    import urllib2
    req = urllib2.Request(url)
    if req.get_type() != 'http':
        return {'exception': Exception('unsupported scheme: %s' % req.get_type())}
    result = {}
    c = SS_HTTPConnection(host=req.get_host(), server_address='192.168.20.44')
    try:
        bt = time.time()
        c.get_sa()
    except:
        result['exception'] = sys.exc_info()
        return result
    finally:
        result['socket_info_time'] = time.time() - bt
    try:
        bt = time.time()
        c.ss_connect()
    except:
        result['exception'] = sys.exc_info()
        return result
    finally:
        result['connect_time'] = time.time() - bt
    try:
        bt = time.time()
        c.request(req.get_method(), req.get_selector())
        r = c.getresponse()
        result['body'] = r.read()
        result['http_status'] = r.status
    except:
        result['exception'] = sys.exc_info()
    finally:
        result['download_time'] = time.time() - bt
        c.close()
    return result

if __name__ == '__main__':
    res = http_download('http://i.snssdk.com/2/article/recent/?tag=news&count=20&uuid=lazy123456789&device_platform=iphone&channel=App%20Store&app_name=news_article&device_type=iPhone%204&os_version=5.1.1&version_code=1.1&_self=monitor')
    print 'socket info time:', res.get('socket_info_time')
    print 'connect time:', res.get('connect_time')
    print 'download time:', res.get('download_time')
    print res
