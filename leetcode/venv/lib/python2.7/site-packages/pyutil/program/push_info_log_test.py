# -*- coding: utf-8 -*- 

import multiprocessing
import threading
import random
import logging
import time

from pyutil.program.log import logging_config
from pyutil.program.push_info_log import push_info_begin, push_info_log, push_info_end


class ThreadRun(object):
    def __init__(self, processindex, msg):
        self.msg = msg
        self.processindex = processindex
        self.i = 0
        self.max_iter = 10 

    def run(self):
        for i in range(1000):
            push_info_log('process_%s_%s'%(self.processindex, self.msg), self.msg)
            push_info_log('p_%s_%s'%(self.processindex, self.msg), self.msg)
            push_info_log('r_%s_%s'%(self.processindex, self.msg), self.msg)
            push_info_log('o_%s_%s'%(self.processindex, self.msg), self.msg)


class TestMultiThread(ThreadRun, threading.Thread):
    def __init__(self, processindex, msg):
        ThreadRun.__init__(self,processindex,msg)
        threading.Thread.__init__(self)

class ProcessRun(object):
    def __init__(self, msg):
        self.msg = msg
        self.i = 0
        self.max_iter = 10 

    def run(self):

        for i in range(self.max_iter):
            t0 = time.time()
            push_info_begin()
            plist = [TestMultiThread(self.msg,i) for i in range(10)]
            for p in plist:
                p.start()
            for p in plist:
                p.join()
            push_info_end() 
            logging.info('cost_time:%f', time.time() - t0)

class TestMultiProcess(ProcessRun, multiprocessing.Process):
    def __init__(self, msg):
        ProcessRun.__init__(self, msg)
        multiprocessing.Process.__init__(self)


def test():
    logging_config('./push_info_log_test.log', log_level=logging.DEBUG)
    
    plist = [TestMultiProcess(i) for i in range(10)]
    for p in plist:
        p.start()
    for p in plist:
        p.join()

test()
