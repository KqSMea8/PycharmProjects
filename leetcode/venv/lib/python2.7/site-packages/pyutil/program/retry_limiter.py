#!/usr/bin/env python
#! -*- coding: utf8 -*-

import time, threading

'''
RetryLimiter: 基本想法是不要过多重试。目前的实现很简单：
  1. 最近10s的流量作为基础流量
  2. 配额是10%（默认值），被所有请求共享。如果配额被用光，就不再重试。

注意：
  1. 每次访问都需要实例化一个RetryLimiter，它会维持状态信息
  2. 后端名称需要合理选择，同一个后端使用一个，同一个服务名称的配额会被共享
  3. 重试次数一般为2，包含第一次重试。同样的，第一次调用can_retry()永远会返回true

Example:

retry_limiter = RetryLimiter("backend_service_name", 2)
while retry_limiter.can_retry():
    client.query()

    if succeed:
       break
'''

class RetryManager(object):
    THROUGHPUT_WIN = 10 # in seconds
    QUOTA_WIN = 10 # in seconds

    # static data
    _data = {}
    _lock = threading.Lock()

    @staticmethod
    def incr_throughput(name):
        with RetryManager._lock:
            now = int(time.time())

            if name not in RetryManager._data:
                # create new item
                di = {}
                di['throughput_win'] = int(now / RetryManager.THROUGHPUT_WIN)
                di['cur_throughput'] = 1
                di['last_throughput'] = 0
                di['quota_win'] = int(now / RetryManager.QUOTA_WIN)
                di['extra_retried'] = 0
                RetryManager._data[name] = di
            else:
                di = RetryManager._data[name]
                if int(now / RetryManager.THROUGHPUT_WIN) > int(di['throughput_win']):
                    # switch window data
                    di['throughput_win'] = int(now / RetryManager.THROUGHPUT_WIN)
                    di['last_throughput'] = di['cur_throughput']
                    di['cur_throughput'] = 0
                if int(now / RetryManager.QUOTA_WIN) > int(di['quota_win']):
                    di['quota_win'] = int(now / RetryManager.QUOTA_WIN)
                    di['extra_retried'] = 0

                di['cur_throughput'] += 1

    @staticmethod
    def _quota(last_throughput, retry_ratio):
        quota = int(last_throughput * RetryManager.QUOTA_WIN
                    * retry_ratio / RetryManager.THROUGHPUT_WIN)
        if last_throughput > 0 and quota < 1:
            quota = 1 # at least 1 if we have throughput in last window
        return quota

    @staticmethod
    def can_retry(name, retry_ratio):
        # retry quota is based on the throughput of last window

        with RetryManager._lock:
            if name not in RetryManager._data:
                return True # should not happen, but we let it retry

            di = RetryManager._data[name]
            quota = RetryManager._quota(di['last_throughput'], retry_ratio)
            if di['extra_retried'] < quota:
                di['extra_retried'] += 1
                return True
            else:
                return False

    @staticmethod
    def debug_string(name, retry_ratio):
        with RetryManager._lock:
            if name not in RetryManager._data:
                return 'not found'

            di = RetryManager._data[name]
            quota = RetryManager._quota(di['last_throughput'], retry_ratio)
            return (('%s. global data [throughput_win: %s cur_throughput: %s ' +
                    'last_throughput: %s quota_win: %s extra_retried: %s quota: %s]') % (
                    name, di['throughput_win'], di['cur_throughput'], di['last_throughput'],
                    di['quota_win'], di['extra_retried'], quota))

class RetryLimiter(object):

    def __init__(self, name, retry_times=2, retry_ratio=0.1):
        self.name = name
        self._retry_times = retry_times
        self._retry_ratio = retry_ratio
        self._retried = 0

        if self._retry_times < 1:
            self._retry_times = 1 # at least 1, for the first query

        RetryManager.incr_throughput(self.name)

    def can_retry(self):
        # always true for the first time
        if self._retried == 0:
            self._retried += 1
            return True

        # check my own retry quota first
        # then check the global quota
        if self._retried < self._retry_times and RetryManager.can_retry(self.name, self._retry_ratio):
            self._retried += 1
            return True
        else:
            return False

    def retried(self):
        return self._retried

    def debug_string(self):
        return (RetryManager.debug_string(self.name, self._retry_ratio) + 
                ' local data [retried: %s]' % self._retried)

# unit test
import unittest
class TestBasic(unittest.TestCase):
    def setUp(self):
        RetryManager.THROUGHPUT_WIN = 4
        RetryManager.QUOTA_WIN = 2

    def wait_to_second(self, ts):
        while True:
            if time.time() >= ts:
                break
            time.sleep(0.01)

    def test_basic(self):
        rl = RetryLimiter('a', 2)
        self.assertTrue(rl.can_retry()) # always true for the first time
        self.assertFalse(rl.can_retry()) # throughput data has not been filled

        window = int(time.time() / RetryManager.THROUGHPUT_WIN)
        window += 1
        self.wait_to_second(window * RetryManager.THROUGHPUT_WIN)

        # -- fill throughput --

        for i in xrange(10 * RetryManager.THROUGHPUT_WIN):
            rl = RetryLimiter('a', 2)
            while (rl.can_retry()):
                break # assume access ok
        for i in xrange(100 * RetryManager.THROUGHPUT_WIN):
            rl = RetryLimiter('b', 2)
            while (rl.can_retry()):
                break # assume access ok

        # wait

        window += 1
        self.wait_to_second(window * RetryManager.THROUGHPUT_WIN)

        # -- case 1 --

        for i in xrange(RetryManager.QUOTA_WIN):
            rl = RetryLimiter('a', 2)
            print 'case 1. pre. ' + rl.debug_string()

            self.assertTrue(rl.can_retry())
            print 'case 1. point 1. ' + rl.debug_string()
            self.assertTrue(rl.can_retry()) # can retry again
            print 'case 1. point 2. ' + rl.debug_string()

            self.assertFalse(rl.can_retry()) # limited to 2
            self.assertFalse(rl.can_retry())

            print 'case 1. post. ' + rl.debug_string()

        # -- case 2 --

        rl = RetryLimiter('a', 2)
        print 'case 2. pre. ' + rl.debug_string()

        self.assertTrue(rl.can_retry())

        self.assertFalse(rl.can_retry())
        self.assertFalse(rl.can_retry())

        print 'case 2. post. ' + rl.debug_string()

        # -- case 3 --

        rl = RetryLimiter('b', 4)
        print 'case 3. pre. ' + rl.debug_string()

        self.assertTrue(rl.can_retry())
        self.assertTrue(rl.can_retry())
        self.assertTrue(rl.can_retry()) # 3rd retry
        self.assertTrue(rl.can_retry()) # 4th retry

        self.assertFalse(rl.can_retry())
        self.assertFalse(rl.can_retry())

        print 'case 3. post. ' + rl.debug_string()

        if RetryManager.THROUGHPUT_WIN > RetryManager.QUOTA_WIN:
            # wait
            self.wait_to_second(window * RetryManager.THROUGHPUT_WIN + RetryManager.QUOTA_WIN)

            for i in xrange(RetryManager.QUOTA_WIN):
                # -- case 4 --

                rl = RetryLimiter('a', 2)
                print 'case 4. pre. ' + rl.debug_string()

                self.assertTrue(rl.can_retry())
                self.assertTrue(rl.can_retry())

                self.assertFalse(rl.can_retry())

                print 'case 4. post. ' + rl.debug_string()

            # -- case 5 --

            rl = RetryLimiter('a', 2)
            print 'case 5. pre. ' + rl.debug_string()

            self.assertTrue(rl.can_retry())

            self.assertFalse(rl.can_retry())
            self.assertFalse(rl.can_retry())

            print 'case 5. post. ' + rl.debug_string()

if __name__ == '__main__':
    unittest.main()

