#coding=utf-8
import hashlib

def hash_uint64(raw_val):
    '''
    >>> hash_uint64('')
    15284527576400310788L
    '''
    if isinstance(raw_val, unicode):
        raw_val = raw_val.encode('utf-8')
    val = hashlib.md5(raw_val).hexdigest()
    return long(val[:16], 16)
