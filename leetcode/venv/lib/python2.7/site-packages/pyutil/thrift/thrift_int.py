#coding=utf-8
from ctypes import c_longlong, c_ulonglong
from pyutil.thrift.thrift_utils import thrift_type_convert

'''
thrift不支持uint64, 提供函数将req转为int64, 将rsp转为uint64
'''

__all__ = ['thrift_to_uint64', 'thrift_to_int64']

def atom_to_uint64(v):
    if isinstance(v, (int, long)) and v < 0:
        return c_ulonglong(v).value
    else:
        return v

def atom_to_int64(v):
    if isinstance(v, long):
        return c_longlong(v).value
    else:
        return v

def thrift_to_uint64(struct):
    return thrift_type_convert(struct, atom_to_uint64)

def thrift_to_int64(struct):
    '''
    >>> thrift_to_int64([2**64 - 1])
    [-1]
    >>> thrift_to_int64(dict(seed_ids=[2**64 - 1]))
    {'seed_ids': [-1]}
    '''
    return thrift_type_convert(struct, atom_to_int64)
