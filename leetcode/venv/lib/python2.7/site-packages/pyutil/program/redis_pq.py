#coding=utf-8
import pickle, time, logging
from redis import WatchError


class RedisPriorityQueue(object):
    def __init__(self, redis_client, key, max_size):
        self._redis_client = redis_client
        self._key = key
        self._pipe = redis_client.pipeline()
        self._maxsize = max_size
        logging.info('Start RedisPriorityQueue for %s, max_size = %s', key, max_size)

    def put(self, score, item, check_maxsize=True):
        while check_maxsize and self.qsize() > self._maxsize:
            logging.info('redis_pq full: %s > %s', self.qsize(), self._maxsize)
            time.sleep(1)
        data = pickle.dumps(item)
        self._redis_client.zadd(self._key, score, data)

    def free_slot(self):
        return self._maxsize - self.qsize()

    def get(self):
        while True:
            try:
                item = None
                self._pipe.watch(self._key)
                data_list = self._pipe.zrevrange(self._key, 0, 0, withscores=False)
                if data_list:
                    item = pickle.loads(data_list[0])
                    self._pipe.multi()
                    self._pipe.zrem(self._key, data_list[0])
                self._pipe.execute()
                if item:
                    return item
                else:
                    time.sleep(1)
            except WatchError:
                time.sleep(1)
            finally:
                self._pipe.reset()

    def qsize(self):
        return self._redis_client.zcard(self._key)

    def clear(self):
        return self._redis_client.zremrangebyrank(self._key, 0, -1)
