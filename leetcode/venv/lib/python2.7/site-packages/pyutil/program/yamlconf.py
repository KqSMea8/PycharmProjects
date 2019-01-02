#!/usr/bin/env python
# coding: utf-8
__author__ = 'zhenghuabin'

import time
from functools import wraps

import yaml


def auto_reload(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        need_reload = False
        if self.options is None:
            need_reload = True
        elif self.reload_interval > 0 and time.time() > self.last_load_time + self.reload_interval:
            need_reload = True
        if need_reload:
            with open(self.filename, 'r') as fp:
                self.options = yaml.safe_load(fp)
                self.last_load_time = time.time()
        return method(self, *args, **kwargs)

    return wrapper


class Conf(object):
    def __init__(self, filename, reload_interval=0):
        self.filename = filename
        self.reload_interval = reload_interval
        self.options = None
        self.last_load_time = 0

    @auto_reload
    def get(self, key, val=None):
        return self.options.get(key, val)

    @auto_reload
    def get_all(self):
        return self.options


if __name__ == '__main__':
    options = {
        "aa": "bb",
    }
    filename = '/tmp/xxyml.yml'
    with open(filename, 'w') as fp:
        yaml.safe_dump(options, fp)
    conf = Conf(filename, 2)
    assert conf.get("aa") == "bb"
    options['aa'] = 'cc'
    with open(filename, 'w') as fp:
        yaml.safe_dump(options, fp)
    assert conf.get("aa") == "bb"
    time.sleep(3)
    assert conf.get("aa") == "cc"


