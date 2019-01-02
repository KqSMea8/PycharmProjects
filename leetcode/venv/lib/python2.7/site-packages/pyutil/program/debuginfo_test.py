# -*- coding: utf-8 -*- 

import multiprocessing
import threading
import random
import logging

from pyutil.program.conf import Conf
from pyutil.kafka_proxy.kafka_proxy import KafkaProxy
from pyutil.program.log import logging_config

import debuginfo

conf = Conf('debuginfo_test.conf')

class TestBase(object):
    def __init__(self, msg):
        self.msg = msg
        self.i = 0
        self.max_iter = 2 

    def run(self):
        while self.i < self.max_iter:
            req_id = random.randint(1, 10)
            writer = debuginfo.get_writer(req_id, 'debuginfo_czm')
            writer.record_by_request('uid', 8)
            writer.record_by_group(123, 'score', 1)
            self.foo(req_id)
            writer.record_by_request('msg', self.msg)

            writer2 = debuginfo.get_writer(req_id, 'debuginfo_czm2')
            writer2.record_by_request('uid', 8)
            writer2.record_by_group(123, 'score', 1)
            self.foo(req_id)
            writer.emit()
            writer2.emit()
            self.i += 1

    def foo(self, req_id):
        writer = debuginfo.get_writer(req_id, 'debuginfo_czm')
        writer.record_by_group(456, 'score', 2)
        writer2 = debuginfo.get_writer(req_id, 'debuginfo_czm2')
        writer2.record_by_group(456, 'score', 2)

class TestMultiProcess(TestBase, multiprocessing.Process):
    def __init__(self, msg):
        TestBase.__init__(self, msg)
        multiprocessing.Process.__init__(self)

class TestMultiThread(TestBase, threading.Thread):
    def __init__(self, msg):
        TestBase.__init__(self, msg)
        threading.Thread.__init__(self)

def test_class(class_type):
    print 'test_multi_process:'
    logging_config(conf.local_log_file, log_level=logging.DEBUG)
    debuginfo.init(conf)
    debuginfo.start()

    kafka_proxy_1 = KafkaProxy(conf=conf, topic='debuginfo_czm', consumer_group='debuginfo_test')
    kafka_proxy_2 = KafkaProxy(conf=conf, topic='debuginfo_czm2', consumer_group='debuginfo_test')
    kafka_proxy_1.set_consumer_offset(0, 2)
    kafka_proxy_2.set_consumer_offset(0, 2)

    plist = [class_type('%s #%s' % (repr(class_type), i)) for i in range(10)]
    for p in plist:
        p.start()
    for p in plist:
        p.join()

    debuginfo.stop()

    msgs = kafka_proxy_1.fetch_msgs(count=10*2)
    if msgs:
        for msg in msgs:
            print msg
    
    msg2 = kafka_proxy_2.fetch_msgs(count=10*2)
    if msg2:
        for msg in msg2:
            print msg

test_class(TestMultiProcess)
test_class(TestMultiThread)
