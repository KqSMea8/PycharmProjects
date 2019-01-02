# coding: utf-8
"""

a TSocket driven by gevent

@create: 15-1-7
"""
from functools import wraps

import time
import random
import socket
import errno
import sys
import gevent.socket

from thrift.transport.TTransport import TTransportException, TTransportBase
from thrift.Thrift import TException


def _resolve_address(host, port):
    return socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE | socket.AI_ADDRCONFIG)


def check_retry_interval(func):

    @wraps(func)
    def _func(self, *args, **kwargs):

        if not self.retry_interval:
            return True

        return func(self, *args, **kwargs)

    return _func


class TGeventSocketPool(TTransportBase):
    """
    """

    serverStates = {}

    def __init__(self, servers, timeout=None, conn_timeout=None, retry_interval=5):

        self.timeout = timeout
        self.conn_timeout = conn_timeout
        self.servers = servers
        self.retry_interval = retry_interval
        self.max_failures = 2
        self.handle = None
        self.host = None
        self.port = None

    @check_retry_interval
    def _check_server_states(self, fail_time_key):

        last_fail_time = self.__class__.serverStates.get(fail_time_key, 0)
        if not last_fail_time:
            return True

        elapsed = int(time.time()) - last_fail_time
        return elapsed > self.retry_interval

    def connect(self, host, port):

        try:
            res0 = _resolve_address(host, port)
            for res in res0:
                self.handle = gevent.socket.socket(res[0], res[1])
                self.handle.settimeout(self.conn_timeout)
                try:
                    self.handle.connect(res[4])
                    self.handle.settimeout(self.timeout)
                except socket.error, e:
                    if res is not res0[-1]:
                        continue
                    else:
                        raise e
                break
        except socket.error:
            message = 'Could not connect to %s:%d' % (host, port)
            raise TTransportException(type=TTransportException.NOT_OPEN, message=message)

    @check_retry_interval
    def _update_server_states(self, fail_time_key, fail_cnt_key):
        fail_cnt = self.__class__.serverStates.get(fail_cnt_key, 0)
        # Increment by one
        fail_cnt += 1
        # Log and cache this failure
        if fail_cnt >= self.max_failures:
            # Store the failure time
            self.__class__.serverStates[fail_time_key] = int(time.time())
            # Clear the count of consecutive failures
            del self.__class__.serverStates[fail_cnt_key]
        else:
            self.__class__.serverStates[fail_cnt_key] = fail_cnt

    @check_retry_interval
    def _clear_server_states(self, fail_cnt_key, fail_time_key):
        if fail_time_key in self.__class__.serverStates:
            del self.__class__.serverStates[fail_time_key]
        if fail_cnt_key in self.__class__.serverStates:
            del self.__class__.serverStates[fail_cnt_key]

    def open(self):

        servers = range(len(self.servers))

        while servers:

            i = random.randint(0, len(servers) - 1)
            self.host, self.port = self.servers[servers.pop(i)]

            # Check APC cache for a record of this server being down
            fail_time_key = 'last_fail_time:%s:%d' % (self.host, self.port)
            fail_cnt_key = 'fail_cnt:%s:%d' % (self.host, self.port)
            if not self._check_server_states(fail_time_key):
                continue

            try:
                self.connect(self.host, self.port)
                self._clear_server_states(fail_cnt_key, fail_time_key)

                return
            except:

                self._update_server_states(fail_time_key, fail_cnt_key)

        error = u"All hosts in pool are down."
        raise TException(error)

    def isOpen(self):
        return self.handle is not None

    def close(self):
        if self.handle:
            self.handle.close()
            self.handle = None

    def read(self, sz):
        try:
            buff = self.handle.recv(sz)
        except socket.error, e:
            if (e.args[0] == errno.ECONNRESET and
                    (sys.platform == 'darwin' or sys.platform.startswith('freebsd'))):
                # freebsd and Mach don't follow POSIX semantic of recv
                # and fail with ECONNRESET if peer performed shutdown.
                # See corresponding comment and code in TSocket::read()
                # in lib/cpp/src/transport/TSocket.cpp.
                self.close()
                # Trigger the check to raise the END_OF_FILE exception below.
                buff = ''
            elif e.args[0] == errno.EINTR:
                buff = self.handle.recv(sz)
                if len(buff) > 0:
                    return buff
            else:
                raise
        if len(buff) == 0:
            raise TTransportException(type=TTransportException.END_OF_FILE, message='TSocket read 0 bytes')
        return buff

    def write(self, buff):
        if not self.handle:
            raise TTransportException(type=TTransportException.NOT_OPEN, message='Transport not open')
        sent = 0
        have = len(buff)
        while sent < have:
            plus = self.handle.send(buff)
            if plus == 0:
                raise TTransportException(type=TTransportException.END_OF_FILE, message='TSocket sent 0 bytes')
            sent += plus
            buff = buff[plus:]

    def flush(self):
        pass
