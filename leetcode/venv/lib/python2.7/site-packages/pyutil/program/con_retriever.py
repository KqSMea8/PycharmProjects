#!/usr/bin/env python
#coding=utf8

import time, logging, Queue
from pyutil.program.thread import ThreadPool

class ConRetriever(object):

    def __init__(self, timeout, thread_num=10):
        self.timeout = timeout
        self.thread_num = thread_num
        self.rec_thread_pool = ThreadPool(self.thread_num)
        self.last_groupid = 0
        self.reset_stat()

    def submit_task(self, groupid, key, handler, *args, **kargs):
        if groupid != self.last_groupid:
            self.queue = Queue.Queue(maxsize=self.thread_num + 1)
            self.last_groupid = groupid
        self.rec_thread_pool.queue_task(self.__handler_wrapper__, (key, handler, args, kargs))
        self.task_keys.append(key)

    def __handler_wrapper__(self, key, handler, args, kargs):
        res = None
        try:
            res = handler(*args, **kargs)
        except Exception as e:
            logging.exception(e)
        finally:
            if self.queue:
                self.queue.put((key, res))

    def reset_stat(self):
        self.task_keys = []
        self.queue = None

    def wait_for_res(self):
        ret = {}
        if len(self.task_keys) == 0:
            return ret, set([])
        ret_count = 0
        ts = time.time()
        while 1:
            try:
                key, res = self.queue.get(timeout=0.05)
                ret_count += 1
                if res is not None:
                    ret[key] = res
            except Queue.Empty:
                pass
            if  time.time() - ts > self.timeout:
                logging.warn("got %s tasks , but just %s success:%s", len(self.task_keys), len(ret), ret.keys())
                break
            if ret_count == len(self.task_keys):
                break
        failed_key = set(self.task_keys) - set(ret.keys())
        self.reset_stat()
        return ret, failed_key

if __name__ == "__main__":
    import time
    def handler(arg1, arg2, arg3=None, arg4={}):
        time.sleep(1)
        return arg1, arg2, arg3, arg4
    con_recall = ConRetriever(0.12, thread_num=3)
    groupid = time.time()
    con_recall.submit_task(groupid, "foo", handler, 1,2,arg3=3,arg4={1:3})
    con_recall.submit_task(groupid, "foo1", handler, 1,2,arg3=3,arg4={1:3})
    res = con_recall.wait_for_res()
    print res

