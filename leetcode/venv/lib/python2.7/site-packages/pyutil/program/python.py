#coding=utf-8

import json

def unique(list_, key=lambda x: x):
    """efficient function to uniquify a list preserving item order"""
    seen = {}
    result = []
    for item in list_:
        seenkey = key(item)
        if seenkey in seen:
            continue
        seen[seenkey] = 1
        result.append(item)
    return result

def flatten(x):
    """flatten(sequence) -> list

    Returns a single, flat list which contains all elements retrieved
    from the sequence and all recursively contained sub-sequences
    (iterables).

    Examples:
    >>> [1, 2, [3,4], (5,6)]
    [1, 2, [3, 4], (5, 6)]
    >>> flatten([[[1,2,3], (42,None)], [4,5], [6], 7, (8,9,10)])
    [1, 2, 3, 42, None, 4, 5, 6, 7, 8, 9, 10]
    >>> flatten([dict(a=1), 1, []])
    [{'a': 1}, 1]
    """

    result = []
    for el in x:
        if not isinstance(el, dict) and hasattr(el, "__iter__"):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result

def compact(lst):
    return [x for x in lst if x]

def first(lst, default=None):
    '''
    获取iterator的第一个元素, 没有则返回default
    >>> first([1,2])
    1
    >>> first([])
    >>> first([], 1)
    1
    >>> first((x for x in [1,2]))
    1
    '''
    for k in lst:
        return k
    return default

def find(lst, pred, default=None):
    '''
    >>> find([1,2], lambda e: e == 1)
    1
    >>> find([1,2], lambda e: e == 3, default=0)
    0
    '''
    for e in lst:
        if pred(e):
            return e
    return default

def merge_dicts(*dicts):
    '''
    >>> merge_dicts(dict(a=1), dict(b=2)) == dict(a=1, b=2)
    True
    '''
    m = {}
    for d in dicts:
        m.update(d)
    return m

def merge_dict(d1, d2, op, op_type=0, recursive=False, inplace=False, pkeys=[]):
    '''
    merge two dict, for key in both dict, use op function to compute new value
    recursive - if True, merge dict values for the same key recursively
    inplace - if True, modify d1 in place
    op_type -
        0 - op is a function with signature op(v1, v2)
        1 - op is a function with signature op(v1, v2, k, pkeys)
    pkeys - the keys leading to current value, only used internally

    >>> import operator
    >>> merge_dict(dict(a=1, b=2), dict(a=1, c=1), operator.add)
    {'a': 2, 'c': 1, 'b': 2}
    >>> merge_dict(dict(a=1, b=2), dict(a=1, c=1), op=lambda v1, v2, k, pkeys: (v1 if k == 'a' else v1 + v2), op_type=1)
    {'a': 1, 'c': 1, 'b': 2}
    >>> merge_dict(dict(a=dict(b=1,c=2), x=5), dict(a=dict(b=2), d=1), lambda x, y: y, recursive=True)
    {'a': {'c': 2, 'b': 2}, 'x': 5, 'd': 1}
    >>> merge_dict(dict(a=dict(b=1,c=2), x=5), dict(a=dict(b=2), d=1), lambda x, y: y, recursive=True, inplace=True)
    {'a': {'c': 2, 'b': 2}, 'x': 5, 'd': 1}
    >>> merge_dict(dict(a=dict(b=1,c=2), x=5), dict(a=dict(b=2), d=1), lambda x, y: y)
    {'a': {'b': 2}, 'x': 5, 'd': 1}
    '''
    nd = d1 if inplace else d1.copy()
    for k, v in d2.iteritems():
        if k in nd:
            if recursive and isinstance(v, dict) and isinstance(nd[k], dict):
                nd[k] = merge_dict(nd[k], v, op, op_type=op_type, recursive=recursive, inplace=inplace, pkeys=pkeys + [k])
            else:
                if op_type == 0:
                    nd[k] = op(nd[k], v)
                else:
                    nd[k] = op(nd[k], v, k, pkeys)
        else:
            nd[k] = v
    return nd

def map_dict(d, func, pkeys=[], atom_op=None):
    '''
    >>> map_dict(dict(a=1, b=dict(x=1)), lambda k, v, pkeys: (k + 'x', v))
    {'ax': 1, 'bx': {'xx': 1}}
    >>> map_dict([dict(a=1, b=dict(x=1))], lambda k, v, pkeys: (k + 'x', v))
    [{'ax': 1, 'bx': {'xx': 1}}]
    >>> map_dict(dict(a=1, b=dict(x=1)), lambda k, v, pkeys: None)
    {}
    >>> map_dict(dict(a=[1, 2]), lambda k, v, pkeys: (k, v), atom_op=lambda v, pkeys: v + 1)
    {'a': [2, 3]}
    '''
    if isinstance(d, (list, tuple)):
        return type(d)([map_dict(x, func, pkeys, atom_op=atom_op) for x in d])
    elif isinstance(d, dict):
        nd = {}
        for k, v in d.iteritems():
            nv = map_dict(v, func, pkeys + [k], atom_op=atom_op)
            res = func(k, nv, pkeys)
            if res is not None:
                nk, nv = func(k, nv, pkeys)
                nd[nk] = nv
        return nd
    else:
        return atom_op(d, pkeys) if atom_op else d


def walk_dict(d, func, parent=None, pkeys=[], skip_keys=[]):
    '''
    func - lambda k, v, parent, pkeys
    '''
    if isinstance(d, (list, tuple)):
        for x in d:
            walk_dict(x, func, parent, pkeys=pkeys, skip_keys=skip_keys)
    elif isinstance(d, dict):
        for k, v in d.items():
            if k not in skip_keys:
                walk_dict(v, func, d, pkeys=pkeys + [k], skip_keys=skip_keys)
                func(k, v, d, pkeys)

def copy_dict(d, required_keys=None, optional_keys=None, skip_none=False):
    '''
    d - dict or object
    optional_keys - don't copy empty value
    required_keys - copy empty value, don't copy inexistent value (not in)
    key can be one of the following forms:
        (k_src, k_dst, conv_v)
        (k_src, k_dst)
        (k_src, conv_v)
        k
    skip_none - do not copy key with value None (for required_keys)
    optional_keys and required_keys should be given at least one
    return - new dict

    >>> copy_dict(dict(a=None, b=2, c=0), ['a', ('b', 'b1'), 'd'], ['c'])
    {'a': None, 'b1': 2}
    >>> copy_dict(dict(a=None, b=0), ['a', 'b'], skip_none=True)
    {'b': 0}
    >>> copy_dict(dict(a=1), [('a', lambda x: x+1)])
    {'a': 2}
    >>> class C(object): pass
    >>> o = C()
    >>> o.a = 1
    >>> copy_dict(o, ['a', 'b'])
    {'a': 1}
    '''
    def conv_k(k):
        conv_v = lambda x: x
        if isinstance(k, (list, tuple)):
            if len(k) == 2:
                if callable(k[1]):
                    k_src, conv_v = k
                    k_dst = k_src
                else:
                    k_src, k_dst = k
            else:
                k_src, k_dst, conv_v = k
        else:
            k_src, k_dst = k, k
        return k_src, k_dst, conv_v

    def _get(k, defv=None):
        if isinstance(d, dict):
            return d.get(k, defv)
        else:
            return getattr(d, k, defv)
    def _has(k):
        if isinstance(d, dict):
            return k in d
        else:
            return hasattr(d, k)

    nd = {}
    for k in required_keys or []:
        k_src, k_dst, conv_v = conv_k(k)
        if _has(k_src):
            if not skip_none or _get(k_src) is not None:
                nd[k_dst] = conv_v(_get(k_src))
    for k in optional_keys or []:
        k_src, k_dst, conv_v = conv_k(k)
        if _get(k_src):
            nd[k_dst] = conv_v(_get(k_src))

    return nd


def unicode_to_str(text, encoding=None, strict=True):
    """Return the str representation of text in the given encoding. Unlike
    .encode(encoding) this function can be applied directly to a str
    object without the risk of double-decoding problems (which can happen if
    you don't use the default 'ascii' encoding)

    strict - True/False. when text is not basestring, if True, raise exception, else return text
    """

    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, unicode):
        return text.encode(encoding)
    elif isinstance(text, str):
        return text
    else:
        if strict:
            raise TypeError('in strict mode, unicode_to_str must receive a unicode or str object, got %s' % type(text).__name__)
        else:
            return text

def str_to_unicode(text, encoding=None, strict=True):
    """Return the unicode representation of text in the given encoding. Unlike
    .encode(encoding) this function can be applied directly to a unicode
    object without the risk of double-decoding problems (which can happen if
    you don't use the default 'ascii' encoding)

    strict - True/False. when text is not basestring, if True, raise exception, else return text
    """

    if encoding is None:
        encoding = 'utf-8'
    if isinstance(text, str):
        return text.decode(encoding)
    elif isinstance(text, unicode):
        return text
    else:
        if strict:
            raise TypeError('in strict mode, str_to_unicode must receive a str or unicode object, got %s' % type(text).__name__)
        else:
            return text

def mystrip(s):
    u'''
    return - unicode with space stripped
    >>> mystrip(u'　 \x0ba\ufeff ')
    u'a'
    >>> print mystrip(u'\u200b\u4eba\u6c11\u7f51')
    人民网
    >>> mystrip(None)
    '''

    if s is None:
        return None

    import re
    s = str_to_unicode(s)
    spaces = u'[ \t\n\r\x00-\x1F\x7F\xA0\xAD\u2000-\u200F\u201F\u202F\u3000\uFEFF]+'
    return re.sub(u'^%s|%s$' % (spaces, spaces), '', s)

def arg_to_iter(arg):
    """Convert an argument to an iterable. The argument can be a None, single
    value, or an iterable.

    Exception: if arg is a dict, [arg] will be returned

    >>> arg_to_iter(1)
    [1]
    >>> arg_to_iter(dict(a=1))
    [{'a': 1}]
    >>> arg_to_iter([1])
    [1]
    """
    if arg is None:
        return []
    elif not isinstance(arg, dict) and hasattr(arg, '__iter__'):
        return arg
    else:
        return [arg]

def split_chunks(n, lst):
    '''
    n - chunk size
    >>> split_chunks(3, [1,2,3,4])
    [[1, 2, 3], [4]]
    >>> split_chunks(2, [1,2,3,4])
    [[1, 2], [3, 4]]
    >>> split_chunks(3, [])
    []
    '''
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def split_chunks_evenly(n, lst, padding=False):
    '''
    n - number of chunks
    >>> split_chunks_evenly(3, [1,2,3,4])
    [[1, 2], [3], [4]]
    >>> split_chunks_evenly(2, [1,2,3,4])
    [[1, 2], [3, 4]]
    >>> split_chunks_evenly(2, [1,2,3,4], padding=True)
    [[1, 2], [3, 4]]
    >>> split_chunks_evenly(3, [1])
    [[1]]
    >>> split_chunks_evenly(3, [1], padding=True)
    [[1], [], []]
    >>> split_chunks_evenly(3, [])
    []
    >>> split_chunks_evenly(3, [], padding=True)
    [[], [], []]
    '''
    chunk_size = len(lst) / n
    remainder = len(lst) % n
    result = []
    if remainder:
        parts = [
            (chunk_size + 1, lst[:(chunk_size + 1) * remainder]),
            (chunk_size, lst[(chunk_size + 1) * remainder:]),
            ]
    else:
        parts = [(chunk_size, lst)]
    for chunk_size_, lst_ in parts:
        if lst_:
            result += split_chunks(chunk_size_, lst_)
    if padding and len(result) < n:
        result += [[] for x in range(n - len(result))]
    return result

DictProxyType = type(object.__dict__)
def make_hash(o, obj_as_dict=False):

    """
    see http://stackoverflow.com/questions/5884066/hashing-a-python-dictionary
    Makes a hash from a dictionary, list, tuple or set to any level, that
    contains only other hashable types (including any lists, tuples, sets, and
    dictionaries). In the case where other kinds of objects (like classes) need
    to be hashed, pass in a collection of object attributes that are pertinent.
    For example, a class can be hashed in this fashion:

        make_hash([cls.__dict__, cls.__name__])

    A function can be hashed like so:

        make_hash([fn.__dict__, fn.__code__])
    """

    import copy
    if isinstance(o, DictProxyType):
        o = {k: v for k, v in o.items() if not k.startswith('__')}
    if obj_as_dict and hasattr(o, '__dict__'):
        return make_hash(o.__dict__, obj_as_dict)
    elif isinstance(o, set):
        return hash(frozenset(o))
    elif isinstance(o, (tuple, list)):
        return hash(tuple([make_hash(e, obj_as_dict) for e in o]))
    elif isinstance(o, dict):
        new_o = {k: make_hash(v, obj_as_dict) for k, v in o.items()}
        return hash(frozenset(new_o.items()))
    else:
        try:
            return hash(o)
        except:
            import logging
            logging.exception(o)
            raise

def obj_unicode_to_str(obj):
    '''
    >>> obj_unicode_to_str(2)
    2
    >>> obj_unicode_to_str({1:1})
    {'1': 1}
    '''
    if isinstance(obj, dict):
        v = {unicode_to_str(k) if isinstance(k, basestring) else str(k): obj_unicode_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        v = [obj_unicode_to_str(x) for x in obj]
        if isinstance(obj, tuple):
            v = tuple(v)
    else:
        v = unicode_to_str(obj, strict=False)
    return v

def unicode_json_dump(extra):
    u'''
    dump '中' as '中' instead of u'\u4e2d' to save space
    extra中字符串必须为unicode或utf8 encoded
    return - unicode
    >>> unicode_json_dump(dict(a=1, b=2))
    u'{"a":1,"b":2}'
    >>> print unicode_json_dump([u'中'])
    ["中"]
    >>> print unicode_json_dump(dict(a='中'))
    {"a":"中"}
    >>> print unicode_json_dump({u'中': '中'})
    {"中":"中"}
    >>> len(unicode_json_dump(dict(a='中')))
    9
    >>> len(unicode_json_dump({u'中': '中'}))
    9
    >>> json.loads(unicode_json_dump({u'中': '中'})) == {u'中': u'中'}
    True
    >>> print json.dumps(dict(a='中'))
    {"a": "\\u4e2d"}
    >>> len(json.dumps(dict(a='中')))
    15
    >>> unicode_json_dump(u'\U0001f631') == json.dumps(u'\U0001f631') # Non-BMP unicode char转换成utf8时是4个字节，mysql utf8最多只支持3字节, 因此此种情况转为ascii以确保存储正确
    True
    '''

    extra = obj_unicode_to_str(extra)
    jsonu = json.dumps(extra, ensure_ascii=False, separators=(',', ':')).decode('utf8')
    for u in jsonu:
        if len(u.encode('utf8')) > 3:
            jsonu = json.dumps(extra, ensure_ascii=True, separators=(',', ':')).decode('utf8')
            break
    return jsonu

def to_int64(i):
    """
    >>> to_int64(-100)
    -100
    >>> to_int64(2**64 - 1)
    -1
    >>> to_int64(1)
    1
    """
    from ctypes import c_longlong
    return c_longlong(i).value

def to_uint64(i):
    """
    >>> to_uint64(-1) == 2**64 - 1
    True
    >>> to_uint64(2**64 - 1) == 2**64 - 1
    True
    >>> to_uint64(100)
    100L
    """
    from ctypes import c_ulonglong
    return c_ulonglong(i).value

if __name__ == '__main__':
    import sys
    sys.path = sys.path[1:]
    import doctest
    doctest.testmod()
