#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import sys, os, subprocess, time, string, random, logging

from redis import Redis

from pyutil.redis.redis_proxy import make_redis_proxy_cli, make_redis_proxy_cli2

MANUAL_MASTERS = ['127.0.0.1:5690']
MANUAL_SLAVES = ['127.0.0.1:5690']
AUTO_BACKEND_ERROR = ['127.0.0.1:5696']
AUTO_BACKEND_TIMEOUT = ['127.0.0.1:5697']
HOST = '127.0.0.1'
PORT = 5698
AUTO_MASTERS = ['%s:%s' % (HOST, PORT)]
#AUTO_SLAVES = ['127.0.0.1:5699']

AUTO = True # setup environment automaticly
BACKEND_ERROR_SERVERS = AUTO_BACKEND_ERROR
BACKEND_TIMEOUT_SERVERS = AUTO_BACKEND_TIMEOUT
MASTER_SERVERS = AUTO_MASTERS
#SLAVE_SERVERS = AUTO_SLAVES

def rand_str(n=10, prefix=''):
    if prefix != '':
        prefix = str(prefix) + '_'
    return str(prefix) + ''.join(random.choice(string.ascii_uppercase) for _ in range(n))

seq_i = 0
def gen_seq():
    global seq_i
    seq_i += 1
    return seq_i

def gen_kv_pair(num=16, rand=True):
    if rand:
        return {rand_str(16, i) : rand_str(64, i) for i in range(0, num)}
    else:
        return {str(i) : str(i) for i in range(0, num)}

def gen_member_pair(num=16, rand=True):
    if rand:
        return {rand_str(16, i) : random.randint(-1024, 1024) for i in range(0, num)}
    else:
        return {str(i) : i for i in range(0, num)}

class TestBasic(unittest.TestCase):
    def setUp(self):
        self._db = Redis(HOST, PORT)

    def test_expire(self):
        self.assertEqual(self._db.set('a', 'a'), True)
        self.assertEqual(self._db.get('a'), 'a')

    def test_expire(self):
        pass
        print 'result: %r' %self._db.set('a', 'a')
        #print self._db.expire('a', -3)
        #print self._db.get('a')

    def test_pipeline(self):
        pipe = self._db.pipeline(transaction=False)

        #pipe.set('a', 'a')
        #print pipe.execute()

        pipe.set('a', 'a').set('b', 'b').get('a')
        self.assertEqual(pipe.execute(), [True, True, 'a'])

    def test_large_pipeline(self):
        return

        pipe = self._db.pipeline(transaction=False)
        key_num = 10
        field_num = 20

        count = 0
        for i in xrange(key_num):
            count += 1
            pipe.setex('unittest:%d' % i, 'value', 300)
        ts = time.time()
        pipe.execute()
        #print 'pipe setex count:', count, ', time:', time.time() - ts

        field_pair = gen_member_pair(field_num, rand=False)
        count = 0
        for i in xrange(key_num):
            key = 'unittest:hash:%d' % i
            count += 1
            pipe.delete(key)
            for field, value in field_pair.items():
                count += 1
                pipe.hset(key, field, value)
            count += 1
            pipe.expire(key, 30000)
        ts = time.time()
        pipe.execute()
        print 'pipe hset count:', count, ', time:', time.time() - ts

        count = 0
        for i in xrange(key_num):
            key = 'unittest:hash:%d' % i
            count += 1
            pipe.hgetall(key)
        ts = time.time()
        pipe.execute()
        print 'pipe hgetall count:', count, ', time:', time.time() - ts

class TestFailure(unittest.TestCase):

    def test_connect_timeout(self):
        def do_test_connect_timeout(timeout):
            # pyredis retry one more time if failed.
            # refer:
            #   redis/client.py: StrictRedis.execute_command
            #   redis/connection.py: Connection.__init__
            expected_timeout = timeout * 2

            ts = time.time()
            with self.assertRaises(Exception): db.set('a', 'a')
            cost = time.time() - ts
            #self.assertGreater(cost, expected_timeout) # TODO
            self.assertLess(cost, expected_timeout * 1.2)

        # should use default timeout
        db = make_redis_proxy_cli(['10.100.100.100:1000'])
        do_test_connect_timeout(0.1) # our default connect timeout

        timeouts = [0.01, 0.1, 0.8]
        for timeout in timeouts:
            db = make_redis_proxy_cli(['10.100.100.100:1000'],
                                      socket_connect_timeout=timeout)
            do_test_connect_timeout(timeout)

    def test_read_timeout(self):
        def do_test(timeout):
            expected_timeout = timeout

            ts = time.time()
            with self.assertRaises(Exception): db.set('a', 'a')
            cost = time.time() - ts
            #self.assertGreater(cost, expected_timeout) # TODO
            self.assertLess(cost, expected_timeout * 1.2)

        # should use default timeout
        db = make_redis_proxy_cli(BACKEND_TIMEOUT_SERVERS)
        do_test(0.25) # our default timeout

        timeouts = [0.01, 0.1, 0.8]
        for timeout in timeouts:
            db = make_redis_proxy_cli(BACKEND_TIMEOUT_SERVERS,
                                      socket_timeout=timeout)
            do_test(timeout)

    def test_read_error(self):
        def do_test():
            ts = time.time()
            with self.assertRaises(Exception): db.set('a', 'a')
            cost = time.time() - ts
            self.assertLess(cost, 0.01)

        db = make_redis_proxy_cli(BACKEND_ERROR_SERVERS)
        do_test()

        timeouts = [0.01, 0.1, 0.8]
        for timeout in timeouts:
            db = make_redis_proxy_cli(BACKEND_ERROR_SERVERS,
                                      socket_timeout=timeout)
            do_test()

    def test_mget_empty(self):
        db = make_redis_proxy_cli(MASTER_SERVERS, socket_connect_timeout=3, socket_timeout=3)
        try:
            ts = time.time()
            db.mget([])
        except:
            pass
        finally:
            self.assertLess(time.time() - ts, 1.0)

    def test_get_empty(self):
        db = make_redis_proxy_cli(MASTER_SERVERS, socket_connect_timeout=3, socket_timeout=3)
        try:
            ts = time.time()
            db.get('')
            db.get()
        except:
            pass
        finally:
            self.assertLess(time.time() - ts, 1.0)

    def test_setex_empty(self):
        db = make_redis_proxy_cli(MASTER_SERVERS, socket_connect_timeout=3, socket_timeout=3)
        try:
            ts = time.time()
            db.setex('k', 'v', '')
            db.setex('k', '')
            db.setex('')
            db.setex('k', 'v')
            db.setex('k')
            db.setex()
        except:
            pass
        finally:
            self.assertLess(time.time() - ts, 1.0)

class TestProxy(unittest.TestCase):
    def setUp(self):
        servers = MASTER_SERVERS
        self._db = make_redis_proxy_cli(servers)

    def test_set(self):
        self.assertTrue(self._db.set('a', 'a'))

    def test_strict_redis(self):
        db1 = make_redis_proxy_cli(MASTER_SERVERS, socket_timeout=0.1)
        db1.delete('z_key1')
        db1.zadd('z_key1', 100.1, 'name1')
        self.assertEqual(db1.zrank('z_key1', 'name1'), 0)

        db2 = make_redis_proxy_cli(MASTER_SERVERS, socket_timeout=0.2, strict_redis=True)
        db2.delete('z_key1')
        db2.zadd('z_key1', 100.1, 'name1')
        self.assertEqual(db2.zrank('z_key1', 'name1'), 0)

        db3 = make_redis_proxy_cli(MASTER_SERVERS, socket_timeout=0.3, strict_redis=False)
        db3.delete('z_key1')
        db3.zadd('z_key1', 'name1', 100.1)
        self.assertEqual(db3.zrank('z_key1', 'name1'), 0)

        db3 = make_redis_proxy_cli(MASTER_SERVERS, socket_timeout=0.3, strict_redis=True)
        db3.delete('z_key1')
        with self.assertRaises(Exception): db3.zadd('z_key1', 'name1', 100.1)
        self.assertEqual(db3.zrank('z_key1', 'name1'), None)

    def test_cli2(self):
        db1 = make_redis_proxy_cli2(MASTER_SERVERS, connection_kwargs={'socket_timeout':0.1},
                                    redis_kwargs={'decode_responses':True})
        self.assertTrue(db1.set('a', 'a'))

        db1 = make_redis_proxy_cli2(MASTER_SERVERS, connection_kwargs={'socket_timeout':0.1})
        self.assertTrue(db1.set('a', 'a'))

# TODO, add test of multiple processes

class TestServer(unittest.TestCase):
    def test_set(self):                                                            
        db = Redis(' 192.168.20.41', 5800)                                         
        self.assertTrue(db.set('a', 'a'))                                          
                                                                                   
        servers = [' 192.168.20.41:5800']                                          
        db = make_redis_proxy_cli(servers)                                         
        self.assertTrue(db.set('a', 'a'))

def run_cmd(cmd):
    scripts_dir = os.path.dirname(__file__) + '/test_env'
    #print cmd
    p = subprocess.Popen(cmd, shell=True, cwd=scripts_dir, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    #print p.stdout.readlines()
    #print p.stderr.readlines()
    p.wait()

def check_env():
    k = rand_str()
    v = rand_str()
    try:
        db = make_redis_proxy_cli(MASTER_SERVERS)
        result = db.setex(k, 10, v)
        if not result:
            logging.error('check master %s failed' % MASTER_SERVERS)
            return False
    except Exception, ex:
        logging.exception('check master %s failed' % MASTER_SERVERS)
        return False

    #try:
        #db = SpringDBClient(SLAVE_SERVERS, TABLE)
        #result = db.get(k)
    #except Exception, ex:
        #logging.exception('check slave %s failed' % SLAVE_SERVERS)
        #return False

    return True

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.DEBUG)

    # manual: set up redis and twemproxy manually
    # auto: test script will start redis and twemproxy automaticly

    if 'manual' in sys.argv:
        AUTO = False
        sys.argv.remove('manual')

    if AUTO:
        logging.info('setup environment ...')
        run_cmd('./redis-ctrl restart')
        run_cmd('./twemproxy-ctrl restart')
        logging.info('sleep a while before check environment ...')
        time.sleep(0.3)
        logging.info('check environment ...')
        if not check_env():
            logging.error('check environment failed')
            sys.exit(-1)
    unittest.main(exit=False)
    if AUTO:
        logging.info('clean environment ...')
        run_cmd('./twemproxy-ctrl stop')
        run_cmd('./redis-ctrl stop')

