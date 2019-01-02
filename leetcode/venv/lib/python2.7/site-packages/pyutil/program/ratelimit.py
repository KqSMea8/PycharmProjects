# coding: utf-8
"""

@create: 3/24/15
"""

from __future__ import absolute_import
import time
import math
import functools
try:
    import ujson as json
except:
    import json


def as_key(func):

    @functools.wraps(func)
    def _func(self, subject, *args, **kwargs):
        subject = self.make_key(subject)
        return func(self, subject, *args, **kwargs)

    return _func

class LeakyBucket(object):
    """
    基于漏桶算法的频控组件，暂未考虑并发写的问题
    """

    def __init__(self, mc_client, capacity, leak_rate):
        self.mc_client = mc_client
        self.capacity = float(capacity)
        self.leak_rate = float(leak_rate)


    def add(self, key, amount=1):
        """
        stored data:{"ts": 1232313131, "remaining":123}
        """
        data = self.mc_client.get(key)
        data = json.loads(data) if data else {}
        ts = data.get("ts", time.time())
        remaining = data.get("remaining", 0)
        delta = self.leak_rate * (time.time() - ts)
        remaining = max(0, remaining - delta)
        if remaining + amount > self.capacity:
            return False
        remaining += amount
        data["ts"] = time.time()
        data["remaining"] = remaining
        self.mc_client.set(key, json.dumps(data))
        return True


class SimpleLeakyBucket(LeakyBucket):
    def __init__(self, mc_client, count, time_span):
        capacity = float(count)
        leak_rate = float(count) / time_span
        super(SimpleLeakyBucket, self).__init__(mc_client, capacity, leak_rate)


class RateLimit(object):

    def __init__(self, client, bucket_span=600, bucket_interval=5, subject_expiry=1200):
        """
        @type client: redis.StrictRedis
        @param bucket_span: 最长检测时间区间，默认600秒
        @type bucket_span:
        @param bucket_interval: 桶时间区间，默认值5秒
        @type bucket_interval:
        @param subject_expiry:
        @type subject_expiry:
        @return:
        @rtype:
        """
        self.client = client
        self.bucket_span = bucket_span
        self.bucket_interval = bucket_interval
        self.subject_expiry = subject_expiry
        self.bucket_count = round(self.bucket_span / self.bucket_interval)

    def get_bucket(self, timestamp=None):

        if timestamp is None:
            timestamp = long(time.time())

        bucket = int(math.floor((timestamp % self.bucket_span) / self.bucket_interval))
        return bucket

    @staticmethod
    def make_key(subject):
        return 'rate_limit:%s' % subject

    @as_key
    def add(self, subject, interval=None, timestamp=None):

        bucket = self.get_bucket(timestamp)
        if interval is None:
            bucket_count = 2
        else:
            bucket_count = int(math.floor(interval / self.bucket_interval))

        pipeline = self.client.pipeline(transaction=False)

        # increment the current bucket
        pipeline.hincrby(subject, bucket, 1)

        # clear the buckets ahead
        keys = map(lambda x: int((bucket + x + 1) % self.bucket_count), range(bucket_count))
        pipeline.hdel(subject, *keys)

        # renew the key ttl
        pipeline.expire(subject, self.subject_expiry)

        pipeline.execute()

        return self

    @as_key
    def get_count(self, subject, interval):
        bucket = self.get_bucket()
        bucket_count = int(math.floor(interval / self.bucket_interval))

        # get the counts from the previous `count` buckets
        keys = map(lambda x: int((bucket - x) % self.bucket_count), range(bucket_count, 0, -1))
        result = self.client.hmget(subject, keys)

        # sum the counts
        count = sum(map(int, filter(lambda x: x, result)))
        return count

    @as_key
    def delete(self, subject):
        self.client.delete(subject)

def test_leaky_bucket():
    import memcache
    client = memcache.Client(['10.4.16.189:11311'], pickleProtocol=2)
    leaky_bucket = SimpleLeakyBucket(client, 30, 100)
    for _ in range(19):
        print leaky_bucket.add("test", 1)

def test():
    import redis

    client = redis.StrictRedis('10.4.23.47', 6284)
    rl = RateLimit(client)
    subject = 'test'
    rl.delete(subject)
    now = long(time.time())
    for x in range(10):
        rl.add(subject, 60, now - 30 + x * 3)

    print rl.get_count(subject, 30)
    print rl.get_count(subject, 60)


if __name__ == '__main__':
    #  test()
    test_leaky_bucket()

