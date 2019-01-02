# -*- coding: utf-8 -*-

import os
import random
import redis
import time
import threading
import logging

rand = random.SystemRandom()

_DEAD_RETRY = 10 # seconds


class ThreadLocalConnectionPool(redis.ConnectionPool, threading.local):
    MAX_RETRY = 3

    def __init__(self, servers, max_connection=None,
                 find_connection_timeout=0.1, **connection_kwargs):
        self.last_connection = None
        self.servers = servers
        from copy import deepcopy
        if 'socket_timeout' not in connection_kwargs:
            connection_kwargs['socket_timeout'] = 0.25
        if 'socket_connect_timeout' not in connection_kwargs:
            connection_kwargs['socket_connect_timeout'] = 0.05
        self.connection_kwargs = deepcopy(connection_kwargs)
        self.find_connection_timeout = find_connection_timeout
        super(ThreadLocalConnectionPool, self).__init__(**connection_kwargs)

    def build_connection(self):
        for server in self.servers:
            host, port = server.split(':')
            self.connection_kwargs['host'] = host
            self.connection_kwargs['port'] = port
            if 'db' not in self.connection_kwargs:
                self.connection_kwargs['db'] = 0
            conn = self.make_connection()
            self._available_connections.append(conn)

    def get_connection(self, command_name, *keys, **options):
        if self.last_connection and not self.last_connection.is_dead():
            return self.last_connection
        if not self._available_connections:
            self.build_connection()
        max_idx = len(self.servers) - 1
        black_set = set()
        max_util = time.time() + self.find_connection_timeout
        for i in range(ThreadLocalConnectionPool.MAX_RETRY):
            idx = random.randint(0, max_idx)
            if max_util < time.time():
                return self._available_connections[idx]
            if not self._available_connections[idx].is_dead():
                self.last_connection = self._available_connections[idx]
                return self.last_connection
            black_set.add(self._available_connections[idx])
        random.shuffle(self._available_connections)
        for conn in self._available_connections:
            if max_util < time.time():
                return conn
            if conn in black_set:
                continue
            if not conn.is_dead():
                self.last_connection = conn
                return self.last_connection
        return self._available_connections[0]

    def release(self, connection):
        pass


class RandomRedisConnectionPool(redis.ConnectionPool):
    def __init__(self, servers, connection_class=redis.Connection,
                 max_connection=None, **connection_kwargs):
        if not isinstance(servers, list):
            raise TypeError("argument servers '%r' should be list" % servers)
        self.servers = servers or ["127.0.0.1:6379"]
        self._lock = threading.Lock()
        self._dead_connections = []
        if 'socket_timeout' not in connection_kwargs:
            connection_kwargs['socket_timeout'] = 0.25
        if 'socket_connect_timeout' not in connection_kwargs:
            connection_kwargs['socket_connect_timeout'] = 0.1
        super(RandomRedisConnectionPool, self).__init__(**connection_kwargs)

    def fill_connections(self):
        "fill available connections with all servers"
        for server in self.servers:
            host, port = server.split(':')
            self.connection_kwargs['host'] = host
            self.connection_kwargs['port'] = int(port)
            if 'db' not in self.connection_kwargs:
                self.connection_kwargs['db'] = 0
            conn = self.make_connection()
            self.add2available_queue(conn)

    def add2available_queue(self, conn):
        self._available_connections.append(conn)

    def get_connection(self, *args, **kwargs):
        "Get a random connection from the pool"
        # make sure we haven't changed process.
        self._checkpid()
        # retry dead servers if needed
        now = time.time()
        _dead_connections = []
        with self._lock:
            for conn in self._dead_connections:
                if now - conn._dead_time > _DEAD_RETRY:
                    self.add2available_queue(conn)
                else:
                    _dead_connections.append(conn)
            self._dead_connections = _dead_connections

            # fill the pool if needed
            if len(self._available_connections) == 0:
                self.fill_connections()

            # random pop a connection
            random.shuffle(self._available_connections)
            conn = self._available_connections.pop()
            self._in_use_connections.add(conn)
        return conn

    def release(self, connection):
        if connection._sock is None:
            # mark the dead time for retrying
            connection._dead_time = time.time()
            with self._lock:
                self._dead_connections.append(connection)
                self._in_use_connections.remove(connection)
            return # never put the connection back into the pool on err
        with self._lock:
            return super(RandomRedisConnectionPool, self).release(connection)


class AutoConfRedisConnectionPool(RandomRedisConnectionPool):
    def __init__(self, cluster_name, interval_time=60, connection_class=redis.Connection,
                 max_connection=None, conf_file='/opt/tiger/ss_conf/ss/redis.conf', **connection_kwargs):
        self.valid_connection = set()
        self.cluster_name = cluster_name
        self.interval_time = interval_time
        servers = AutoConfRedisConnectionPool.get_cluster_servers(cluster=cluster_name, conf_file=conf_file)
        self.conf_file = conf_file
        super(AutoConfRedisConnectionPool, self).__init__(servers, connection_class,
                                                          max_connection, **connection_kwargs)
        self.update_conf_thread = threading.Thread(target=self.update_server_conf)
        self.update_conf_thread.setDaemon(True)
        self.update_conf_thread.start()

    @staticmethod
    def get_cluster_servers(cluster, conf_file='/opt/tiger/ss_conf/ss/redis.conf', bypass_cache=False):
        from pyutil.program.conf import Conf
        conf = Conf(conf_file, bypass_cache=bypass_cache)
        return conf.get_values(cluster)

    def release(self, connection):
        if connection in self.valid_connection:
            return super(AutoConfRedisConnectionPool, self).release(connection)
        connection.disconnect()

    def add2available_queue(self, conn):
        self.valid_connection.add(conn)
        self._available_connections.append(conn)

    def update_server_conf(self):
        while True:
            servers = AutoConfRedisConnectionPool.get_cluster_servers(cluster=self.cluster_name,
                                                                      conf_file=self.conf_file,
                                                                      bypass_cache=True)
            if servers != self.servers and servers:
                with self._lock:
                    st = time.time()
                    self.servers = servers
                    logging.debug("Find diff, start update server conf.")
                    for conn in self._dead_connections:
                        conn.disconnect()
                    for conn in self._available_connections:
                        conn.disconnect()
                    self.valid_connection = set()
                    end = time.time()
                    logging.debug("Update conf occupy lock %s seconds", (end - st))
            time.sleep(self.interval_time)
