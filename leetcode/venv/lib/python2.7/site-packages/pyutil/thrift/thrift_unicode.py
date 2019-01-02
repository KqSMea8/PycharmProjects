#coding=utf8

import logging
from functools import wraps
from .thrift_utils import thrift_methods, thrift_type_convert
from .thrift_int import thrift_to_uint64, thrift_to_int64

__all__ = ['thrift_to_str', 'thrift_to_unicode', 'thrift_unicode_client', 'thrift_unicode_handler']


def atom_thrift_to_unicode(text, types):
    if not isinstance(text, types):
        return text
    if isinstance(text, str):
        return text.decode('utf8')
    else:
        return unicode(text)

def atom_thrift_to_str(text):
    from datetime import datetime
    if isinstance(text, unicode):
        return text.encode('utf8')
    elif isinstance(text, datetime):
        return text.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return text

def thrift_to_str(struct):
    return thrift_type_convert(struct, atom_thrift_to_str)

def thrift_to_unicode(struct, types=str):
    '''
    types - list of types or 'basic', for example: (int, long).
    '''
    if types == 'basic':
        types = (int, long, float, bool, str)
    return thrift_type_convert(struct, lambda x: atom_thrift_to_unicode(x, types))

def thrift_unicode_handler_method(fn, use_unicode=True, use_uint64=False):
    '''
    decorator for unicode thrift processor(service) handler method
    convert req to unicode/uint64 and rsp to str/int64
    '''
    @wraps(fn)
    def wrapped(self, *args, **kws):
        if use_unicode:
            args = thrift_to_unicode(args)
            kws = thrift_to_unicode(kws)
        if use_uint64:
            args = thrift_to_uint64(args)
            kws = thrift_to_uint64(kws)
        rsp = fn(self, *args, **kws)
        if use_unicode:
            rsp = thrift_to_str(rsp)
        if use_uint64:
            rsp = thrift_to_int64(rsp)
        return rsp

    return wrapped

def thrift_unicode_client_method(fn, use_unicode=True, use_uint64=False):
    '''
    decorator for unicode thrift client method
    convert req to str/int64 and rsp to unicode/uint64
    '''
    @wraps(fn)
    def wrapped(self, *args, **kws):
        if use_unicode:
            args = thrift_to_str(args)
            kws = thrift_to_str(kws)
        if use_uint64:
            args = thrift_to_int64(args)
            kws = thrift_to_int64(kws)
        rsp = fn(self, *args, **kws)
        if use_unicode:
            rsp = thrift_to_unicode(rsp)
        if use_uint64:
            rsp = thrift_to_uint64(rsp)
        return rsp

    return wrapped

def thrift_unicode_client(ClientClass, use_unicode=True, use_uint64=False):
    '''
    decorator for unicode thrift client class
    convert req to str and rsp to unicode
    '''
    UnicodeClient = type('Unicode%s' % ClientClass.__name__, (ClientClass, object), {'__module__': ClientClass.__module__})
    for name in thrift_methods(ClientClass):
        method = getattr(UnicodeClient, name)
        setattr(UnicodeClient, name, thrift_unicode_client_method(method, use_unicode, use_uint64))

    return UnicodeClient

def thrift_unicode_handler(IfaceClass, use_unicode=True, use_uint64=False):
    '''
    decorator for unicode thrift processor(service) handler
    convert req to unicode/uint64 and rsp to str/int64
    '''
    def thrift_unicode_handler_decorator(HandlerClass):
        UnicodeHandlerClass = type('Unicode%s' % HandlerClass.__name__, (HandlerClass, object), {})
        for name in thrift_methods(IfaceClass):
            if hasattr(UnicodeHandlerClass, name):
                method = getattr(UnicodeHandlerClass, name)
                setattr(UnicodeHandlerClass, name, thrift_unicode_handler_method(method, use_unicode, use_uint64))
        return UnicodeHandlerClass

    return thrift_unicode_handler_decorator

if __name__ == '__main__':
    import sys
    from ss_thrift_gen.ss.article.article_extract import ArticleExtract
    from ss_thrift_gen.ss.article.article_extract.ttypes import ExtractReq
    UClient = thrift_unicode_client(ArticleExtract.Client)
    req = ExtractReq(url=u'中')
    uc = UClient(None, None)
    uc.extract(req)
