#!/usr/bin/python

import sys
import json
import redis
import string
import socket
from httplib import HTTPConnection

REDIS_HOST = "10.4.16.197"
REDIS_PORT = 6379


class GangliaClient(object):
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT):
        self.host = host
        self.port = port
        self.redis = redis.Redis(host=host, port=port)

    def ip2host(self, host):
        ret = socket.gethostbyaddr(host)
        ip = ret[0]
        return ip


    def get(self, host):
        host = self.ip2host(host)
        metrics = self.redis.hgetall('ganglia:%s' % host)
        return metrics
