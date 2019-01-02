# -*- coding: utf8 -*-

import logging
import memcache
import logging.handlers
from conf import Conf

class LogMonitorHandler(logging.Handler):

    def __init__(self, host='10.4.16.37', port=11211, name='test'):
        logging.Handler.__init__(self)
        self.host = host
        self.port = port
        self.name = name
        self.init_counter()

    # init the counter
    def init_counter(self):
        conf = Conf('/opt/tiger/ss_conf/ss/memcache.conf')
        server = conf.get('memcache_monitor')
        self.mc = memcache.Client([server])
        if not self.mc.get(self.name):
            self.mc.set(self.name, 0)

    def set_host(self, host):
        self.host = host

    def get_host(self):
        return getattr(self, 'host', '127.0.0.1')

    category = property(get_host, set_host)

    def emit(self, record):
        try:
            self.mc.incr(self.name)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.init_counter()
            #self.handleError(record)

    def flush(self):
        pass
