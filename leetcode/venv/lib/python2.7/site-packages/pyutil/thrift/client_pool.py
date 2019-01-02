# coding: utf-8
"""

a simple thrift client pool

@usage:

>> pool = ThriftClientPool(100, client_factory)
>> with pool() as client:
>>     client.ping()

@note: use gevent.queue.LifoQueue in gevent ioloop

@create: 15-1-7
"""
from Queue import LifoQueue

from contextlib import contextmanager


class ThriftClientPool(object):

    def __init__(self, maxsize, client_factory, queue_cls=LifoQueue):
        self.__pool = queue_cls(maxsize)
        self.__client_factory = client_factory
        for _ in xrange(maxsize):
            self.__pool.put(None)

    def get(self):
        client = self.__pool.get()
        if client is None:
            client = self.__client_factory()

        return client

    def close(self, client):
        self.__pool.put(client)

    def remove(self):
        self.__pool.put(None)

    @contextmanager
    def __call__(self):
        client = self.get()
        try:
            yield client
        finally:
            self.close(client)

