#coding=utf8
import logging, threading


_buffer = [] 
_combine_enable = True

def push_info_end():
    global _buffer
    global _combine_enable
    if _combine_enable:
        _buffer.append('===========END')
        logging.info("%s", ''.join(_buffer))
        _buffer = []

def push_info_begin(combine_enable=True):
    global _buffer
    global _combine_enable
    _combine_enable = combine_enable
    if not _combine_enable:
        if _buffer:
            push_info_end()
        _buffer.append('BEGIN=========')

def push_info_log(key, value):
    global _buffer
    global _combine_enable
    try:
        threadid = threading.local().threadid
    except:
        import ctypes
        libc = ctypes.cdll.LoadLibrary('libc.so.6')
        SYS_gettid = 186
        threadid = threading.local().threadid = libc.syscall(SYS_gettid)
    if _combine_enable:
        _buffer.append('[%s;%s;t:%s]'%(key, value, threadid))
    else:
        logging.info('%s, %s, t:%s', key, value, threadid)
