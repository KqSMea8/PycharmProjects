# coding: utf8
"""
Lock类
"""

import threading


class KeyLock(object):
    """Lock类
    @usage:
    用法一:
    with KeyLock('xxx') as x:
        pass

    用法二:
    lock = KeyLock('xxx')
    lock.acquire()
    lock.release()
    """

    _LOCK_DICT = {}

    def __init__(self, key):
        self._key = key

    def __enter__(self):
        self.acquire()

    def __exit__(self, _type, value, traceback):
        self.release()

    def acquire(self):
        lock = KeyLock.getLock(self._key)
        if not lock:
            lock = KeyLock.setLock(self._key)

        lock.acquire()

    def release(self):
        lock = KeyLock.getLock(self._key)
        if lock:
            lock.release()
            KeyLock.delLock(self._key)

    @classmethod
    def getLock(cls, key):
        return cls._LOCK_DICT.get(key)

    @classmethod
    def setLock(cls, key):
        if key not in cls._LOCK_DICT:
            cls._LOCK_DICT[key] = threading.Lock()

        return cls._LOCK_DICT[key]

    @classmethod
    def delLock(cls, key):
        del cls._LOCK_DICT[key]

