#!/usr/bin/env python
# coding: utf-8
__author__ = 'zhenghuabin'

import sys


def set_thread_name(ident, name):
    """
    :param ident:
    :param name:
    :return:
    """
    if not sys.platform.startswith('linux'):
        return
    import ctypes
    from ctypes.util import find_library

    libpthread_path = find_library("pthread")
    if not libpthread_path:
        return
    libpthread = ctypes.CDLL(libpthread_path)
    if not hasattr(libpthread, "pthread_setname_np"):
        return

    pthread_setname_np = libpthread.pthread_setname_np
    pthread_setname_np.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    pthread_setname_np.restype = ctypes.c_int
    try:
        pthread_setname_np(ident, name)
    except:
        pass
