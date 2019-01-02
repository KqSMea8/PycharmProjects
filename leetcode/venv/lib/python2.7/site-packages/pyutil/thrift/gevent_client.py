# coding: utf-8
"""

a thrift client using gevent socket

@usage:

>> with ThriftClient(client_class, [(xxxx, 9999)]) as client:
>>     client.ping()

@create: 15-1-7
"""
from pyutil.thrift.transport import TGeventSocketPool
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from pyutil.thrift.thrift_unicode import thrift_unicode_client


class ThriftClient(object):

    def __init__(self, servers, client_class, use_unicode=False, timeout=None, conn_timeout=None, use_framed=False, retry_interval=5):
        if use_unicode:
            client_class = thrift_unicode_client(client_class)

        if timeout:
            timeout = float(timeout) / 1000

        if conn_timeout:
            conn_timeout = float(conn_timeout) / 1000
        else:
            conn_timeout = timeout

        self.socket = TGeventSocketPool.TGeventSocketPool(
            servers,
            timeout=timeout,
            conn_timeout=conn_timeout,
            retry_interval=retry_interval
        )

        if use_framed:
            self.transport = TTransport.TFramedTransport(self.socket)
        else:
            self.transport = TTransport.TBufferedTransport(self.socket)

        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = client_class(protocol)

    def open(self):
        self.transport.open()

    def close(self):
        self.transport.close()

    def __enter__(self):
        self.transport.open()
        return self.client

    def __exit__(self, type, value, traceback):
        return self.transport.close()

    def __getattr__(self, name):
        return getattr(self.client, name)
